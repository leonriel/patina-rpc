//! End-to-end tests for the connection multiplexer.
//!
//! Each test stands up a one-shot server (accepts exactly one connection,
//! runs `Server::serve_connection` on it) on an ephemeral 127.0.0.1 port, then
//! connects a real `Client` and exercises the multiplexer behaviors specified
//! in DESIGN.md §2-§4: id-based response routing, per-request worker spawn,
//! cancellation cleanup via the `PendingGuard` drop guard, and waiter wake-up
//! on connection drop.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use patina_rpc::{handler_fn, Client, Handler, HandlerError, RpcError, Server};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Bind on 127.0.0.1:0, accept one connection, drive it via
/// `Server::serve_connection`. Returns the bound address and the server
/// JoinHandle (kept by the caller so the task can be `abort()`-ed mid-test).
async fn start_one_shot_server(server: Server) -> (SocketAddr, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let handle = tokio::spawn(async move {
        let (stream, _peer) = listener.accept().await.expect("accept");
        let _ = server.serve_connection(stream).await;
    });
    (addr, handle)
}

#[tokio::test]
async fn concurrent_calls_multiplex_correctly() {
    let server = Server::builder()
        .register(
            "echo",
            handler_fn(|bytes: Vec<u8>| async move { Ok(bytes) }),
        )
        .build();
    let (addr, _server) = start_one_shot_server(server).await;

    let client = Arc::new(Client::connect(addr).await.expect("connect"));

    let handles: Vec<_> = (0..100u32)
        .map(|i| {
            let client = client.clone();
            tokio::spawn(async move {
                let bytes = i.to_le_bytes().to_vec();
                let result = client.call("echo", bytes.clone()).await.expect("call");
                assert_eq!(result, bytes, "response payload for {i} mismatched");
                i
            })
        })
        .collect();

    let mut seen = std::collections::HashSet::new();
    for h in handles {
        seen.insert(h.await.expect("task ok"));
    }
    assert_eq!(seen.len(), 100, "all 100 calls completed independently");
    assert_eq!(client.pending_count(), 0, "no entries leaked after success");
}

#[tokio::test]
async fn unknown_method_returns_404_remote_error() {
    let server = Server::builder().build();
    let (addr, _server) = start_one_shot_server(server).await;
    let client = Client::connect(addr).await.expect("connect");

    match client.call("nonexistent", vec![]).await {
        Err(RpcError::Remote { code: 404, message }) => {
            assert!(
                message.contains("nonexistent"),
                "expected method name in message, got: {message}"
            );
        }
        other => panic!("expected RpcError::Remote {{ code: 404 }}, got {other:?}"),
    }
}

#[tokio::test]
async fn handler_error_propagates_to_caller() {
    let server = Server::builder()
        .register(
            "boom",
            handler_fn(|_: Vec<u8>| async move { Err(HandlerError::new(500, "boom")) }),
        )
        .build();
    let (addr, _server) = start_one_shot_server(server).await;
    let client = Client::connect(addr).await.expect("connect");

    match client.call("boom", vec![]).await {
        Err(RpcError::Remote { code: 500, message }) => assert_eq!(message, "boom"),
        other => panic!("expected RpcError::Remote {{ code: 500 }}, got {other:?}"),
    }
}

#[tokio::test]
async fn timed_out_call_does_not_leak_pending_entry() {
    let server = Server::builder()
        .register(
            "slow",
            handler_fn(|_: Vec<u8>| async move {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok(vec![])
            }),
        )
        .build();
    let (addr, _server) = start_one_shot_server(server).await;
    let client = Client::connect(addr).await.expect("connect");

    assert_eq!(client.pending_count(), 0);

    let outcome =
        tokio::time::timeout(Duration::from_millis(50), client.call("slow", vec![])).await;
    assert!(outcome.is_err(), "expected timeout to fire");

    // Drop guard ran when the timed-out future was dropped; entry should be gone.
    assert_eq!(
        client.pending_count(),
        0,
        "PendingGuard must remove cancelled entries (DESIGN.md §4)"
    );
}

#[tokio::test]
async fn connection_drop_wakes_pending_callers() {
    let server = Server::builder()
        .register(
            "slow",
            handler_fn(|_: Vec<u8>| async move {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(vec![])
            }),
        )
        .build();
    let (addr, server_handle) = start_one_shot_server(server).await;

    let client = Arc::new(Client::connect(addr).await.expect("connect"));

    let call = tokio::spawn({
        let c = client.clone();
        async move { c.call("slow", vec![]).await }
    });

    // Give the call a beat to enter PendingMap.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(client.pending_count(), 1, "call should be parked");

    // Aborting the server task drops the Framed<TcpStream> mid-await, closing
    // the connection. The client's reader hits EOF and drains PendingMap.
    server_handle.abort();

    let result = tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .expect("client should wake within 2s of disconnect")
        .expect("join");

    assert!(
        matches!(result, Err(RpcError::Closed)),
        "expected RpcError::Closed, got {result:?}"
    );
    assert!(client.is_closed(), "is_closed reflects reader exit");
}

#[tokio::test]
async fn handlers_run_concurrently() {
    let server = Server::builder()
        .register(
            "slow",
            handler_fn(|bytes: Vec<u8>| async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(bytes)
            }),
        )
        .build();
    let (addr, _server) = start_one_shot_server(server).await;
    let client = Arc::new(Client::connect(addr).await.expect("connect"));

    let start = Instant::now();
    let h1 = tokio::spawn({
        let c = client.clone();
        async move { c.call("slow", vec![1]).await.expect("call 1") }
    });
    let h2 = tokio::spawn({
        let c = client.clone();
        async move { c.call("slow", vec![2]).await.expect("call 2") }
    });
    h1.await.expect("join 1");
    h2.await.expect("join 2");
    let elapsed = start.elapsed();

    // Serial execution would take >= 400ms; parallel ~200ms + scheduling slack.
    assert!(
        elapsed < Duration::from_millis(350),
        "expected parallel execution, took {elapsed:?}"
    );
}

#[tokio::test]
async fn handler_fn_and_trait_impl_behave_identically() {
    struct UpperHandler;

    #[async_trait::async_trait]
    impl Handler for UpperHandler {
        async fn handle(&self, payload: Vec<u8>) -> Result<Vec<u8>, HandlerError> {
            Ok(payload.into_iter().map(|b| b.to_ascii_uppercase()).collect())
        }
    }

    let server = Server::builder()
        .register("trait_upper", UpperHandler)
        .register(
            "fn_upper",
            handler_fn(|payload: Vec<u8>| async move {
                Ok(payload.into_iter().map(|b| b.to_ascii_uppercase()).collect())
            }),
        )
        .build();
    let (addr, _server) = start_one_shot_server(server).await;
    let client = Client::connect(addr).await.expect("connect");

    let input = b"hello".to_vec();
    let trait_result = client
        .call("trait_upper", input.clone())
        .await
        .expect("trait call");
    let fn_result = client
        .call("fn_upper", input.clone())
        .await
        .expect("fn call");

    assert_eq!(trait_result, b"HELLO");
    assert_eq!(fn_result, b"HELLO");
    assert_eq!(trait_result, fn_result);
}
