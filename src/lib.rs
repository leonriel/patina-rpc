//! Patina RPC — wire layer.
//!
//! This crate implements Phase 1 of the Patina RPC stack: the serialization
//! format, the on-the-wire envelope, and the length-delimited codec that turns
//! a TCP byte stream into a sequence of typed messages. Higher-level concerns
//! (request multiplexing, connection lifecycle, retries) live in later phases.
//!
//! The two public entry points are [`Envelope`] (the message type) and
//! [`WireCodec`] (a `tokio_util::codec` that round-trips envelopes through
//! bincode + a 4-byte length prefix). A typical usage looks like:
//!
//! ```no_run
//! use patina_rpc::{Envelope, WireCodec};
//! use tokio::net::TcpStream;
//! use tokio_util::codec::Framed;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let stream = TcpStream::connect("127.0.0.1:9000").await?;
//! let framed: Framed<TcpStream, WireCodec> = Framed::new(stream, WireCodec::new());
//! // `framed` is both a `Sink<Envelope>` and a `Stream<Item = Result<Envelope, _>>`.
//! # let _ = framed;
//! # Ok(())
//! # }
//! ```

pub mod codec;
pub mod envelope;
pub mod error;

pub use codec::WireCodec;
pub use envelope::{Envelope, ErrorData, RequestData, ResponseData};
pub use error::WireError;
