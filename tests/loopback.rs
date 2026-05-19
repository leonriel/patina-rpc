//! End-to-end framing tests over an in-memory duplex stream.
//!
//! These tests exercise the same code path that the future client/server will
//! use — `Framed<TcpStream, WireCodec>` — but swap the socket for
//! `tokio::io::duplex` so they run in-process without binding ports.

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use patina_rpc::{Envelope, ErrorData, RequestData, ResponseData, WireCodec};
use tokio_util::codec::Framed;

fn sample_envelopes() -> Vec<Envelope> {
    vec![
        Envelope::Request(RequestData {
            id: 1,
            method: "store.get".to_string(),
            payload: vec![0xde, 0xad, 0xbe, 0xef],
        }),
        Envelope::Response(ResponseData {
            id: 1,
            payload: b"hello world".to_vec(),
        }),
        Envelope::Error(ErrorData {
            id: 1,
            code: 500,
            message: "internal".to_string(),
        }),
        Envelope::Heartbeat,
    ]
}

#[tokio::test]
async fn each_variant_round_trips_over_duplex() {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let mut client = Framed::new(client, WireCodec::new());
    let mut server = Framed::new(server, WireCodec::new());

    for envelope in sample_envelopes() {
        client.send(envelope.clone()).await.expect("send");
        let received = server
            .next()
            .await
            .expect("frame arrives")
            .expect("decode ok");
        assert_eq!(received, envelope);
    }
}

#[tokio::test]
async fn burst_of_envelopes_preserves_order_and_content() {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let mut client = Framed::new(client, WireCodec::new());
    let mut server = Framed::new(server, WireCodec::new());

    let burst: Vec<Envelope> = (0..50)
        .map(|i| {
            Envelope::Request(RequestData {
                id: i,
                method: format!("m{i}"),
                payload: vec![i as u8; (i as usize) % 17],
            })
        })
        .collect();

    // Drain the writer separately so we exercise the back-pressure path:
    // the sender keeps producing while the receiver consumes.
    let send_task = {
        let burst = burst.clone();
        tokio::spawn(async move {
            for envelope in burst {
                client.send(envelope).await.expect("send");
            }
            client.close().await.expect("close");
        })
    };

    for expected in &burst {
        let actual = server
            .next()
            .await
            .expect("frame arrives")
            .expect("decode ok");
        assert_eq!(&actual, expected);
    }

    send_task.await.expect("sender finished");
    // After the writer closes, the reader should see end-of-stream.
    assert!(server.next().await.is_none());
}
