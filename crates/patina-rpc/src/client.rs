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
//! server doesn't leak the entry. See DESIGN.md §4 "Client Timeouts". Once
//! `rx.await` resolves the guard is disarmed, so the common success path never
//! re-locks the map to remove an id the reader already took.
//!
//! Connection lifecycle: the reader and writer tasks share a
//! `CancellationToken`. When either half exits — the reader on stream EOF or
//! decode error, the writer on a send failure or after all callers drop — it
//! cancels the token, waking the other so one dead half tears down the whole
//! connection instead of leaving a task parked forever on a half-open socket.
//! The reader owns teardown: on exit it drains the `PendingMap`, and dropping
//! the contained `oneshot::Sender`s causes every parked caller's `rx.await` to
//! resolve to `Err(RecvError)`, which `call()` maps to `PatinaError::Closed`.
//! `is_closed()` reports `token.is_cancelled()`.
//!
//! The design doc (§4 "Connection Drops") describes sending an explicit
//! `Envelope::Error`; dropping the sender has the same wake-up effect without
//! requiring a sentinel error code that callers would have to discriminate.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use futures::sink::SinkExt;
use futures::stream::{SplitSink, SplitStream, StreamExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};

use crate::codec::WireCodec;
use crate::envelope::{Envelope, RequestData};
use crate::error::PatinaError;

/// Bound on the writer task's mpsc buffer. Backpressures callers when the
/// socket can't drain outbound requests fast enough.
const CLIENT_WRITER_CHANNEL_CAPACITY: usize = 128;

/// Concurrent registry of in-flight requests: `id -> oneshot::Sender` for the
/// parked caller awaiting that id's response.
///
/// Wraps `Arc<Mutex<HashMap<..>>>` (per DESIGN.md §3 / CLAUDE.md rule 3) but
/// exposes *only* ownership-returning operations — `insert`, `take`, `remove`,
/// `drain`. No accessor hands out a borrow into the map, which keeps the
/// locking discipline in one place and leaves the backing store free to swap
/// later (e.g. a sharded concurrent map) without touching call sites.
#[derive(Clone, Default)]
struct PendingMap {
    inner: Arc<Mutex<HashMap<u64, oneshot::Sender<Envelope>>>>,
}

impl PendingMap {
    /// Park a sender under `id`. Overwrites any prior entry for that id (ids
    /// come from a monotonic `AtomicU64`, so collisions don't happen in practice).
    fn insert(&self, id: u64, tx: oneshot::Sender<Envelope>) {
        self.guard().insert(id, tx);
    }

    /// Remove `id` and return its sender if present. Used by the reader to
    /// claim ownership of the sender before firing it — no borrow is held
    /// across the subsequent `send`.
    fn take(&self, id: u64) -> Option<oneshot::Sender<Envelope>> {
        self.guard().remove(&id)
    }

    /// Drop `id`'s entry if present. Used by `PendingGuard` on the
    /// cancellation path.
    fn remove(&self, id: u64) {
        self.guard().remove(&id);
    }

    /// Remove and return every parked sender (disconnect teardown). Dropping
    /// the returned senders wakes each parked caller with `RecvError`, which
    /// `call()` maps to `PatinaError::Closed`.
    fn drain(&self) -> Vec<oneshot::Sender<Envelope>> {
        std::mem::take(&mut *self.guard()).into_values().collect()
    }

    /// Number of in-flight entries.
    fn len(&self) -> usize {
        self.guard().len()
    }

    /// Lock the inner map. Our critical sections only touch the `HashMap` and
    /// never panic, so the mutex cannot actually be poisoned; recover the
    /// guard defensively rather than `.expect()` (project no-panic rule).
    fn guard(&self) -> MutexGuard<'_, HashMap<u64, oneshot::Sender<Envelope>>> {
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// RPC client over a single multiplexed TCP connection. Cheap to share via
/// `Arc<Client>` since `call(&self, ...)` only needs a shared reference.
pub struct Client {
    next_id: AtomicU64,
    pending: PendingMap,
    outbound: mpsc::Sender<Envelope>,
    /// Cancelled when either background task exits. Doubles as the "connection
    /// closed" flag observed by `is_closed()` and `call()`'s fast path.
    token: CancellationToken,
}

impl Client {
    /// Open a TCP connection and start the background reader/writer tasks.
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self, PatinaError> {
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
        let pending = PendingMap::default();
        let token = CancellationToken::new();

        tokio::spawn(writer_loop(sink, rx, token.clone()));
        tokio::spawn(reader_loop(stream, pending.clone(), token.clone()));

        Client {
            next_id: AtomicU64::new(0),
            pending,
            outbound: tx,
            token,
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
    ) -> Result<Vec<u8>, PatinaError> {
        if self.token.is_cancelled() {
            return Err(PatinaError::Closed);
        }

        // DESIGN.md §4: AtomicU64 with Relaxed is sufficient — we only need
        // uniqueness, not cross-thread ordering on other state.
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(id, tx);

        // Drop guard cleans up `id` from PendingMap if this future is dropped
        // before `rx.await` completes (cancellation / timeout path). It stays
        // armed only across the send + await window below, then is disarmed.
        let mut guard = PendingGuard { map: self.pending.clone(), id, armed: true };

        let request = Envelope::Request(RequestData {
            id,
            method: method.into(),
            payload,
        });

        if self.outbound.send(request).await.is_err() {
            // Writer task is gone — connection effectively closed. The guard
            // stays armed and removes the now-orphaned entry on drop.
            return Err(PatinaError::Closed);
        }

        let result = match rx.await {
            Ok(Envelope::Response(r)) => Ok(r.payload),
            Ok(Envelope::Error(e)) => Err(PatinaError::from(e)),
            Ok(other) => {
                warn!(?other, "non-response/error envelope received through oneshot");
                Err(PatinaError::Closed)
            }
            // Sender dropped without sending — reader_loop exited and drained
            // the PendingMap. Surface as a connection close.
            Err(_) => Err(PatinaError::Closed),
        };

        // `rx.await` resolved, so the reader has already removed our entry (on
        // dispatch) or drained it (on disconnect). Disarm so the guard's drop
        // doesn't re-lock the map to remove an id that's already gone.
        guard.disarm();
        result
    }

    /// Number of in-flight calls currently parked in the `PendingMap`.
    ///
    /// Primarily useful for tests asserting that cancelled/timed-out calls
    /// don't leak entries, and for runtime observability of in-flight load.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Whether either background task has observed connection closure.
    /// `call()` short-circuits to `PatinaError::Closed` once this is true.
    pub fn is_closed(&self) -> bool {
        self.token.is_cancelled()
    }
}

/// RAII guard that removes its `id` from the `PendingMap` on drop. Held by
/// the `call()` future so cancellation triggers cleanup without requiring
/// cooperation from the reader task.
struct PendingGuard {
    map: PendingMap,
    id: u64,
    armed: bool,
}

impl PendingGuard {
    /// Mark the guard inert so its `Drop` is a no-op. Called once `rx.await`
    /// resolves, because by then the reader task has already removed the entry
    /// (normal completion) or drained the whole map (disconnect).
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        // Armed only across the in-flight window (send + await). If the future
        // was cancelled there, this removes the entry the reader never will,
        // preventing the leak described in DESIGN.md §4. Once disarmed (normal
        // completion), it is skipped entirely — no redundant lock acquisition.
        if self.armed {
            self.map.remove(self.id);
        }
    }
}

async fn writer_loop(
    mut sink: SplitSink<Framed<TcpStream, WireCodec>, Envelope>,
    mut rx: mpsc::Receiver<Envelope>,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            maybe = rx.recv() => match maybe {
                Some(envelope) => {
                    if let Err(e) = sink.send(envelope).await {
                        error!(error = %e, "client writer: send failed");
                        break;
                    }
                }
                // All `outbound` senders dropped (Client gone): nothing more
                // to write.
                None => break,
            }
        }
    }
    // However we exited, the connection is finished. Wake the reader so it
    // tears down too rather than parking on `stream.next()` indefinitely.
    token.cancel();
    trace!("client writer task exiting");
}

async fn reader_loop(
    mut stream: SplitStream<Framed<TcpStream, WireCodec>>,
    pending: PendingMap,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            frame = stream.next() => match frame {
                Some(Ok(envelope)) => {
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
                        match pending.take(id) {
                            Some(tx) => {
                                let _ = tx.send(envelope);
                            }
                            None => debug!(id, "response for unknown id (cancelled?)"),
                        }
                    }
                }
                Some(Err(e)) => {
                    error!(error = %e, "client reader: decode error, closing");
                    break;
                }
                // EOF — the peer closed the connection.
                None => break,
            }
        }
    }

    // Connection died (or the writer signalled us via the token). Cancel so
    // the writer stops too, then drain the PendingMap; dropping the contained
    // senders wakes every parked caller with RecvError, which `call()` maps to
    // PatinaError::Closed. `is_closed()` observes the cancelled token.
    token.cancel();
    let drained = pending.drain();
    trace!(count = drained.len(), "draining pending map after disconnect");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `oneshot::Sender<Envelope>` whose receiver we immediately drop — fine
    /// for these tests, which only exercise map/guard bookkeeping and never
    /// actually send through the sender.
    fn dummy_sender() -> oneshot::Sender<Envelope> {
        let (tx, _rx) = oneshot::channel();
        tx
    }

    #[test]
    fn pending_map_insert_take_round_trips() {
        let map = PendingMap::default();
        assert_eq!(map.len(), 0);

        map.insert(7, dummy_sender());
        assert_eq!(map.len(), 1);

        assert!(map.take(7).is_some(), "take returns the inserted sender");
        assert_eq!(map.len(), 0, "take removes the entry");
        assert!(map.take(7).is_none(), "second take is empty");
    }

    #[test]
    fn pending_map_drain_returns_all_and_empties() {
        let map = PendingMap::default();
        map.insert(1, dummy_sender());
        map.insert(2, dummy_sender());
        map.insert(3, dummy_sender());

        let drained = map.drain();
        assert_eq!(drained.len(), 3, "drain returns every sender");
        assert_eq!(map.len(), 0, "drain empties the map");
        assert!(map.drain().is_empty(), "second drain is empty");
    }

    #[test]
    fn armed_guard_removes_entry_on_drop() {
        let map = PendingMap::default();
        map.insert(42, dummy_sender());
        {
            let _guard = PendingGuard { map: map.clone(), id: 42, armed: true };
        } // guard drops here
        assert_eq!(map.len(), 0, "armed guard removes its id on drop");
    }

    #[test]
    fn disarmed_guard_leaves_entry_on_drop() {
        let map = PendingMap::default();
        map.insert(42, dummy_sender());
        {
            let mut guard = PendingGuard { map: map.clone(), id: 42, armed: true };
            guard.disarm();
        } // guard drops here, but disarmed
        assert_eq!(
            map.len(),
            1,
            "disarmed guard leaves the entry (reader owns removal)"
        );
    }
}
