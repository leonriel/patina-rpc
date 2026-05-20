//! End-to-end demo for the Phase 2 multiplexer.
//!
//! Stands up a `Server` registered with two handlers (one via `handler_fn`,
//! one via a trait impl on a stateful struct), then fires a burst of
//! concurrent calls from one `Client` — proving the connection multiplexer
//! correctly routes responses back to the right waiters over a single TCP
//! connection.
//!
//! Run with:
//! ```text
//! cargo run --example ping_pong
//! RUST_LOG=patina_rpc=debug cargo run --example ping_pong
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use patina_rpc::{Client, Handler, HandlerError, Server, handler_fn};
use tokio::net::TcpListener;
use tracing::info;

/// Stateful handler — counts how many times it's been invoked. Demonstrates
/// the value of the trait API over closure-only registration: shared state
/// lives on `&self` instead of in captured `Arc`s.
struct CountingPing {
    invocations: AtomicU64,
}

#[async_trait::async_trait]
impl Handler for CountingPing {
    async fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>, HandlerError> {
        let n = self.invocations.fetch_add(1, Ordering::Relaxed) + 1;
        info!(call_number = n, payload_len = payload.len(), "ping handled");
        Ok(payload)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let counter = Arc::new(AtomicU64::new(0));
    let server = Server::builder()
        .register(
            "ping",
            CountingPing { invocations: AtomicU64::new(0) },
        )
        .register(
            "upper",
            handler_fn(|payload: Vec<u8>| async move {
                Ok(payload.into_iter().map(|b| b.to_ascii_uppercase()).collect())
            }),
        )
        .build();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    info!(%addr, "server listening");

    tokio::spawn(async move {
        if let Err(e) = server.serve(listener).await {
            tracing::error!(error = %e, "server stopped");
        }
    });

    let client = Arc::new(Client::connect(addr).await?);

    // Fire ten concurrent "ping" calls plus one "upper" call to show that
    // different methods multiplex through the same connection.
    let mut handles = Vec::new();
    for i in 0..10u32 {
        let client = client.clone();
        let counter = counter.clone();
        handles.push(tokio::spawn(async move {
            let payload = format!("ping-{i}").into_bytes();
            let response = client.call("ping", payload.clone()).await?;
            assert_eq!(response, payload);
            counter.fetch_add(1, Ordering::Relaxed);
            Ok::<_, patina_rpc::RpcError>(())
        }));
    }
    let upper = client.call("upper", b"shout".to_vec()).await?;
    info!(response = ?String::from_utf8_lossy(&upper), "upper round-trip");

    for h in handles {
        h.await??;
    }

    info!(
        completed = counter.load(Ordering::Relaxed),
        pending = client.pending_count(),
        "all pings completed"
    );
    Ok(())
}
