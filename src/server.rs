//! Server-side multiplexer.
//!
//! Decouples ingest from response: a dispatcher loop reads requests off the
//! wire and spawns one Tokio task per request (DESIGN.md §3 "Worker Spawn").
//! Each worker runs a registered [`Handler`], wraps its result in a
//! `Envelope::Response` or `Envelope::Error`, and pushes it into an mpsc
//! channel that a dedicated writer task drains to the socket.
//!
//! Public surface:
//!   * [`Handler`] — the canonical contract handlers implement.
//!   * [`handler_fn`] — convenience adapter for closure-style registration.
//!   * [`Server`] + [`ServerBuilder`] — register handlers and run an accept loop.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::SinkExt;
use futures::stream::{SplitSink, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;
use tracing::{debug, error, trace, warn};

use crate::codec::WireCodec;
use crate::envelope::{Envelope, ErrorData, RequestData, ResponseData};
use crate::error::RpcError;

/// Bound on the writer task's mpsc buffer. Backpressures dispatchers when the
/// socket can't drain responses fast enough.
const WRITER_CHANNEL_CAPACITY: usize = 128;

/// Wire-level status code used when a request names a method the server
/// hasn't registered. Mirrors HTTP 404 by convention.
const UNKNOWN_METHOD_CODE: u16 = 404;

/// Error returned by a [`Handler`] when business logic fails.
///
/// Distinct from [`ErrorData`] because handlers shouldn't have to know
/// (or set) the per-request `id` — the dispatcher pairs the error with the
/// request id when building the outbound `Envelope::Error`.
#[derive(Debug, Clone)]
pub struct HandlerError {
    /// HTTP-like status code surfaced to the caller as `RpcError::Remote.code`.
    pub code: u16,
    /// Human-readable message surfaced to the caller as `RpcError::Remote.message`.
    pub message: String,
}

impl HandlerError {
    pub fn new(code: u16, message: impl Into<String>) -> Self {
        Self { code, message: message.into() }
    }
}

/// Implemented by anything the server can dispatch a request to.
///
/// Implementors hold any state they need via `&self` (DB pools, configs,
/// metrics handles) and route on `&self` data. For closure-style registration
/// in tests/demos, see [`handler_fn`].
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    async fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>, HandlerError>;
}

/// Adapt an `Fn(Vec<u8>) -> impl Future<Output = Result<Vec<u8>, HandlerError>>`
/// closure into a [`Handler`]. Convenience for tests and small demos; real
/// services should implement [`Handler`] on a struct to keep state on `&self`.
pub fn handler_fn<F, Fut>(f: F) -> impl Handler
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u8>, HandlerError>> + Send + 'static,
{
    FnHandler(f)
}

struct FnHandler<F>(F);

#[async_trait]
impl<F, Fut> Handler for FnHandler<F>
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u8>, HandlerError>> + Send + 'static,
{
    async fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>, HandlerError> {
        (self.0)(payload).await
    }
}

/// Frozen registry of `method -> handler` pairs. Cheap to clone (handlers are
/// shared via `Arc`), so each accepted connection gets its own handle.
#[derive(Clone)]
pub struct Server {
    handlers: Arc<HashMap<String, Arc<dyn Handler>>>,
}

impl Server {
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Run until `listener.accept()` errors. Spawns one connection task per
    /// accepted socket; connection errors are logged but do not stop the loop.
    pub async fn serve(self, listener: TcpListener) -> Result<(), RpcError> {
        loop {
            let (stream, peer) = listener.accept().await?;
            debug!(%peer, "accepted connection");
            let me = self.clone();
            tokio::spawn(async move {
                if let Err(e) = me.serve_connection(stream).await {
                    error!(error = %e, %peer, "connection ended with error");
                }
            });
        }
    }

    /// Drive a single accepted connection until the stream ends or hits a
    /// wire-layer error. Exposed for tests and bespoke server harnesses that
    /// want their own accept logic or shutdown semantics.
    pub async fn serve_connection(&self, stream: TcpStream) -> Result<(), RpcError> {
        serve_connection_inner(self.handlers.clone(), stream).await
    }
}

/// Mutable counterpart to [`Server`]; collects handler registrations before
/// freezing them into an `Arc`-shared map.
#[derive(Default)]
pub struct ServerBuilder {
    handlers: HashMap<String, Arc<dyn Handler>>,
}

impl ServerBuilder {
    /// Register a handler under `method`. Later registrations for the same
    /// method overwrite earlier ones.
    pub fn register<H: Handler>(mut self, method: impl Into<String>, handler: H) -> Self {
        self.handlers.insert(method.into(), Arc::new(handler));
        self
    }

    pub fn build(self) -> Server {
        Server { handlers: Arc::new(self.handlers) }
    }
}

async fn serve_connection_inner(
    handlers: Arc<HashMap<String, Arc<dyn Handler>>>,
    stream: TcpStream,
) -> Result<(), RpcError> {
    let framed = Framed::new(stream, WireCodec::new());
    let (sink, mut read) = framed.split();
    let (tx, rx) = mpsc::channel::<Envelope>(WRITER_CHANNEL_CAPACITY);

    let writer = tokio::spawn(writer_loop(sink, rx));
    // If this future is aborted (e.g. a caller drops the JoinHandle for the
    // accept task) before reaching the normal `writer.await` below, we still
    // need to tear down the writer task so it drops its `SplitSink` half of
    // the Framed and the underlying TCP socket actually closes — otherwise
    // the peer hangs forever waiting on a half-alive connection.
    let _writer_guard = AbortOnDrop(writer.abort_handle());

    while let Some(frame) = read.next().await {
        match frame {
            Ok(Envelope::Request(req)) => {
                let handlers = handlers.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    let response = dispatch(handlers, req).await;
                    if tx.send(response).await.is_err() {
                        debug!("writer mpsc closed; dropping response");
                    }
                });
            }
            Ok(Envelope::Heartbeat) => trace!("heartbeat consumed"),
            Ok(other @ (Envelope::Response(_) | Envelope::Error(_))) => {
                warn!(?other, "server received non-request envelope; ignoring");
            }
            Err(e) => {
                error!(error = %e, "decode error; closing connection");
                break;
            }
        }
    }

    // Signal the writer to drain remaining responses and exit.
    drop(tx);
    let _ = writer.await;
    Ok(())
}

async fn dispatch(
    handlers: Arc<HashMap<String, Arc<dyn Handler>>>,
    req: RequestData,
) -> Envelope {
    match handlers.get(&req.method) {
        Some(handler) => match handler.handle(req.payload).await {
            Ok(bytes) => Envelope::Response(ResponseData { id: req.id, payload: bytes }),
            Err(err) => Envelope::Error(ErrorData {
                id: req.id,
                code: err.code,
                message: err.message,
            }),
        },
        None => Envelope::Error(ErrorData {
            id: req.id,
            code: UNKNOWN_METHOD_CODE,
            message: format!("unknown method: {}", req.method),
        }),
    }
}

/// RAII helper: aborts a spawned task when dropped. Used to tear down the
/// per-connection writer task if the owning `serve_connection_inner` future
/// is cancelled mid-await rather than exiting through its normal cleanup.
struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn writer_loop(
    mut sink: SplitSink<Framed<TcpStream, WireCodec>, Envelope>,
    mut rx: mpsc::Receiver<Envelope>,
) {
    while let Some(envelope) = rx.recv().await {
        if let Err(e) = sink.send(envelope).await {
            error!(error = %e, "server writer: send failed");
            break;
        }
    }
    trace!("server writer task exiting");
}
