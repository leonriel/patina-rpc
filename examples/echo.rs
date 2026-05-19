//! Tiny TCP loopback demo for the wire layer.
//!
//! Spins up a listener on an ephemeral port, has it echo every received
//! `Envelope` back to the sender, then connects a client that sends one of
//! each variant and verifies the echo. This is a smoke test for the codec
//! over a real `TcpStream` — the multiplexer phase will replace it with a
//! proper client/server.
//!
//! Run with:
//!
//! ```text
//! cargo run --example echo
//! RUST_LOG=patina_rpc=trace cargo run --example echo
//! ```

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use patina_rpc::{Envelope, ErrorData, RequestData, ResponseData, WireCodec};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::info;

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
    info!(%addr, "listening");

    // Server: accept one connection and echo every envelope back.
    let server = tokio::spawn(async move {
        let (socket, peer) = listener.accept().await.expect("accept");
        info!(%peer, "accepted");
        let mut framed = Framed::new(socket, WireCodec::new());
        while let Some(frame) = framed.next().await {
            let envelope = frame.expect("decode");
            framed.send(envelope).await.expect("send back");
        }
        info!("server done");
    });

    // Client: send each variant, assert each echoes back.
    let stream = TcpStream::connect(addr).await?;
    let mut client = Framed::new(stream, WireCodec::new());

    let envelopes = vec![
        Envelope::Request(RequestData {
            id: 1,
            method: "ping".to_string(),
            payload: b"hi".to_vec(),
        }),
        Envelope::Response(ResponseData {
            id: 1,
            payload: b"pong".to_vec(),
        }),
        Envelope::Error(ErrorData {
            id: 2,
            code: 418,
            message: "i am a teapot".to_string(),
        }),
        Envelope::Heartbeat,
    ];

    for envelope in envelopes {
        client.send(envelope.clone()).await?;
        let echoed = client
            .next()
            .await
            .ok_or("connection closed before echo")??;
        assert_eq!(echoed, envelope);
        info!(?echoed, "echo received");
    }

    // Closing the client lets the server's read loop exit cleanly.
    drop(client);
    server.await?;
    Ok(())
}
