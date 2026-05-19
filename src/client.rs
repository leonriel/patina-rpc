//! Client-side multiplexer.
//!
//! Many concurrent callers share one TCP connection. `Client::call()`
//! allocates a unique `u64` id, parks a `oneshot::Sender<Envelope>` in a
//! shared `PendingMap`, and pushes the request envelope into an mpsc channel
//! that a background writer task drains to the socket. A background reader
//! task decodes incoming envelopes, looks up the id in `PendingMap`, and
//! fires the matching `oneshot` to wake the parked caller.
//!
//! Cancellation: `Client::call` returns a future that owns a [`PendingGuard`].
//! If the caller drops the future (e.g. wrapped in `tokio::time::timeout`),
//! the guard removes its id from `PendingMap` so a late response from the
//! server doesn't leak the entry. See DESIGN.md §4 "Client Timeouts".
//!
//! Connection drop: when the reader task exits (stream EOF or decode error),
//! it sets the `closed` flag and drains the `PendingMap`. Dropping the
//! contained `oneshot::Sender`s causes every parked caller's `rx.await` to
//! resolve to `Err(RecvError)`, which `call()` maps to `RpcError::Closed`.
//! The design doc (§4 "Connection Drops") describes sending an explicit
//! `Envelope::Error`; dropping the sender has the same wake-up effect without
//! requiring a sentinel error code that callers would have to discriminate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures::sink::SinkExt;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tracing::{debug, error, trace, warn};

use crate::codec::WireCodec;
use crate::envelope::{Envelope, RequestData};
use crate::error::RpcError;

/// Bound on the writer task's mpsc buffer. Backpressures callers when the
/// socket can't drain outbound requests fast enough.
const CLIENT_WRITER_CHANNEL_CAPACITY: usize = 128;

type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<Envelope>>>>;

/// RPC client over a single multiplexed TCP connection. Cheap to share via
/// `Arc<Client>` since `call(&self, ...)` only needs a shared reference.
pub struct Client {
    next_id: AtomicU64,
    pending: PendingMap,
    outbound: mpsc::Sender<Envelope>,
    closed: Arc<AtomicBool>,
}

impl Client {
    /// Open a TCP connection and start the background reader/writer tasks.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self, RpcError> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self::from_stream(stream))
    }

    /// Wrap an already-connected `TcpStream`. Exposed so tests and bespoke
    /// transports (e.g. TLS-wrapped streams plumbed in later) can hand us a
    /// pre-built socket.
    pub fn from_stream(stream: TcpStream) -> Self {
        let framed = Framed::new(stream, WireCodec::new());
        let (sink, stream) = framed.split();
        let (tx, rx) = mpsc::channel::<Envelope>(CLIENT_WRITER_CHANNEL_CAPACITY);
        let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));

        tokio::spawn(writer_loop(sink, rx));
        tokio::spawn(reader_loop(stream, pending.clone(), closed.clone()));

        Client {
            next_id: AtomicU64::new(0),
            pending,
            outbound: tx,
            closed,
        }
    }

    /// Send a request and wait for its matching response.
    ///
    /// Cancellation-safe: dropping the returned future (e.g. via timeout)
    /// removes the in-flight entry from `PendingMap`, so the server's
    /// eventual reply is silently dropped instead of leaking memory.
    pub async fn call(
        &self,
        method: impl Into<String>,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(RpcError::Closed);
        }

        // DESIGN.md §4: AtomicU64 with Relaxed is sufficient — we only need
        // uniqueness, not cross-thread ordering on other state.
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        {
            let mut map = self.pending.lock().expect("PendingMap poisoned");
            map.insert(id, tx);
        }
        // Drop guard cleans up `id` from PendingMap if this future is dropped
        // before `rx.await` completes (cancellation / timeout path).
        let _guard = PendingGuard { map: self.pending.clone(), id };

        let request = Envelope::Request(RequestData {
            id,
            method: method.into(),
            payload,
        });

        if self.outbound.send(request).await.is_err() {
            // Writer task is gone — connection effectively closed.
            return Err(RpcError::Closed);
        }

        match rx.await {
            Ok(Envelope::Response(r)) => Ok(r.payload),
            Ok(Envelope::Error(e)) => Err(RpcError::from(e)),
            Ok(other) => {
                warn!(?other, "non-response/error envelope received through oneshot");
                Err(RpcError::Closed)
            }
            // Sender dropped without sending — reader_loop exited and drained
            // the PendingMap. Surface as a connection close.
            Err(_) => Err(RpcError::Closed),
        }
    }

    /// Number of in-flight calls currently parked in the `PendingMap`.
    ///
    /// Primarily useful for tests asserting that cancelled/timed-out calls
    /// don't leak entries, and for runtime observability of in-flight load.
    pub fn pending_count(&self) -> usize {
        self.pending.lock().map(|m| m.len()).unwrap_or(0)
    }

    /// Whether the background reader has observed connection closure.
    /// `call()` short-circuits to `RpcError::Closed` once this is true.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

/// RAII guard that removes its `id` from the `PendingMap` on drop. Held by
/// the `call()` future so cancellation triggers cleanup without requiring
/// cooperation from the reader task.
struct PendingGuard {
    map: PendingMap,
    id: u64,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        // If the reader already removed the entry (normal completion), this
        // is a harmless no-op. If the future was cancelled, this is what
        // prevents the leak described in DESIGN.md §4.
        if let Ok(mut m) = self.map.lock() {
            m.remove(&self.id);
        }
    }
}

async fn writer_loop(
    mut sink: SplitSink<Framed<TcpStream, WireCodec>, Envelope>,
    mut rx: mpsc::Receiver<Envelope>,
) {
    while let Some(envelope) = rx.recv().await {
        if let Err(e) = sink.send(envelope).await {
            error!(error = %e, "client writer: send failed");
            break;
        }
    }
    trace!("client writer task exiting");
}

async fn reader_loop(
    mut stream: SplitStream<Framed<TcpStream, WireCodec>>,
    pending: PendingMap,
    closed: Arc<AtomicBool>,
) {
    while let Some(frame) = stream.next().await {
        match frame {
            Ok(envelope) => {
                let id = match &envelope {
                    Envelope::Response(r) => Some(r.id),
                    Envelope::Error(e) => Some(e.id),
                    Envelope::Heartbeat => None,
                    Envelope::Request(_) => {
                        warn!("client received Request envelope; ignoring");
                        None
                    }
                };
                if let Some(id) = id {
                    let sender = {
                        let mut map = pending.lock().expect("PendingMap poisoned");
                        map.remove(&id)
                    };
                    match sender {
                        Some(tx) => {
                            let _ = tx.send(envelope);
                        }
                        None => debug!(id, "response for unknown id (cancelled?)"),
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "client reader: decode error, closing");
                break;
            }
        }
    }

    // Connection died. Mark closed and drain the PendingMap; dropping the
    // contained senders wakes every parked caller with RecvError, which
    // `call()` maps to `RpcError::Closed`.
    closed.store(true, Ordering::Relaxed);
    if let Ok(mut map) = pending.lock() {
        let drained = std::mem::take(&mut *map);
        trace!(count = drained.len(), "draining pending map after disconnect");
    }
}
