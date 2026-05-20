//! `#[patina_rpc::service]` end-to-end demo (DESIGN.md Phase 3 §3 workflow).
//!
//! Defines a `Calc` service trait, implements it on a struct, serves it with
//! the generated `CalcServer` via `add_service`, then drives the generated
//! `CalcClient` with a burst of concurrent calls — no hand-written
//! serialization or method-name strings anywhere.
//!
//! Run with:
//! ```text
//! cargo run -p patina-rpc --example calc_service
//! RUST_LOG=patina_rpc=debug cargo run -p patina-rpc --example calc_service
//! ```

use std::sync::Arc;

use patina_rpc::{PatinaError, Server};
use tokio::net::TcpListener;
use tracing::info;

#[patina_rpc::service]
pub trait Calc: Send + Sync + 'static {
    async fn add(&self, a: i64, b: i64) -> Result<i64, PatinaError>;
    async fn greet(&self, name: String) -> Result<String, PatinaError>;
}

struct MyCalc;

impl Calc for MyCalc {
    async fn add(&self, a: i64, b: i64) -> Result<i64, PatinaError> {
        Ok(a + b)
    }

    async fn greet(&self, name: String) -> Result<String, PatinaError> {
        Ok(format!("hello, {name}"))
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

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    info!(%addr, "calc server listening");

    let server = Server::builder().add_service(CalcServer::new(MyCalc)).build();
    tokio::spawn(async move {
        if let Err(e) = server.serve(listener).await {
            tracing::error!(error = %e, "server stopped");
        }
    });

    let client = Arc::new(CalcClient::connect(addr).await?);

    let mut handles = Vec::new();
    for i in 0..5i64 {
        let client = client.clone();
        handles.push(tokio::spawn(async move { client.add(i, i * 10).await }));
    }
    for handle in handles {
        info!(sum = handle.await??, "add result");
    }

    let greeting = client.greet("patina".to_string()).await?;
    info!(%greeting, "greet result");

    Ok(())
}
