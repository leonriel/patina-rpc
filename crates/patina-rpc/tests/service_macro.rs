//! End-to-end test for the `#[patina_rpc::service]` macro.
//!
//! Defines a service trait, implements it, serves it via the generated
//! `CalcServer` + `add_service`, then drives the generated `CalcClient` over a
//! real TCP socket. Exercises multi-arg, zero-arg, error propagation, and
//! generic-over-trait usage (proving the client implements the trait).

use std::net::SocketAddr;

use patina_rpc::{Client, PatinaError, Server};
use tokio::net::TcpListener;

#[patina_rpc::service]
pub trait Calc: Send + Sync + 'static {
    async fn add(&self, a: i32, b: i32) -> Result<i32, PatinaError>;
    async fn ping(&self) -> Result<String, PatinaError>;
    async fn boom(&self) -> Result<(), PatinaError>;
}

struct CalcImpl;

impl Calc for CalcImpl {
    async fn add(&self, a: i32, b: i32) -> Result<i32, PatinaError> {
        Ok(a + b)
    }

    async fn ping(&self) -> Result<String, PatinaError> {
        Ok("pong".to_string())
    }

    async fn boom(&self) -> Result<(), PatinaError> {
        Err(PatinaError::application(503, "service down"))
    }
}

/// Bind, serve `CalcImpl`, return the address callers should connect to.
async fn start_calc_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = Server::builder().add_service(CalcServer::new(CalcImpl)).build();
    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });
    addr
}

#[tokio::test]
async fn multi_and_zero_arg_round_trip() {
    let addr = start_calc_server().await;
    let client = CalcClient::connect(addr).await.expect("connect");

    assert_eq!(client.add(2, 3).await.expect("add"), 5);
    assert_eq!(client.add(-10, 4).await.expect("add"), -6);
    assert_eq!(client.ping().await.expect("ping"), "pong");
}

#[tokio::test]
async fn handler_error_propagates_through_macro() {
    let addr = start_calc_server().await;
    let client = CalcClient::connect(addr).await.expect("connect");

    match client.boom().await {
        Err(PatinaError::Remote { code: 503, message }) => assert_eq!(message, "service down"),
        other => panic!("expected Remote {{ code: 503 }}, got {other:?}"),
    }
}

#[tokio::test]
async fn unknown_method_is_404() {
    let addr = start_calc_server().await;
    // Use the raw client to call a method the service doesn't expose.
    let client = Client::connect(addr).await.expect("connect");
    match client.call("Calc::nonexistent", vec![]).await {
        Err(PatinaError::Remote { code: 404, .. }) => {}
        other => panic!("expected Remote {{ code: 404 }}, got {other:?}"),
    }
}

#[tokio::test]
async fn client_and_impl_are_interchangeable_behind_trait() {
    // Generic over the trait — works for both the network client and a local
    // impl, which only compiles because CalcClient implements Calc.
    async fn sum_via(calc: &impl Calc) -> i32 {
        calc.add(10, 20).await.expect("add")
    }

    let addr = start_calc_server().await;
    let client = CalcClient::connect(addr).await.expect("connect");

    assert_eq!(sum_via(&client).await, 30);
    assert_eq!(sum_via(&CalcImpl).await, 30);
}
