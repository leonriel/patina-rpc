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

pub mod client;
pub mod codec;
pub mod envelope;
pub mod error;
pub mod server;

pub use client::Client;
pub use codec::WireCodec;
pub use envelope::{Envelope, ErrorData, RequestData, ResponseData};
pub use error::{PatinaError, RpcError, WireError};
pub use server::{handler_fn, Handler, HandlerError, PatinaService, Server, ServerBuilder};

/// Attribute macro that turns an async trait into an RPC client + server.
/// See the `patina-macros` crate docs and `DESIGN.md` "Phase 3".
pub use patina_macros::service;

/// Re-exported so macro-generated `connect` functions can name the bound
/// without the user crate depending on `tokio` paths directly.
pub use tokio::net::ToSocketAddrs;

/// Implementation details the `#[service]` macro expands into. Not part of the
/// stable public API — do not depend on these paths directly.
#[doc(hidden)]
pub mod __private {
    pub use async_trait::async_trait;
    pub use tokio::net::ToSocketAddrs;

    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::error::PatinaError;

    /// Serialize RPC arguments / return values with the same bincode config the
    /// wire codec uses. A failure here means the value isn't serializable —
    /// an internal bug — so it surfaces as a 500.
    pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, PatinaError> {
        bincode::serde::encode_to_vec(value, bincode::config::standard())
            .map_err(|e| PatinaError::Remote { code: 500, message: format!("encode: {e}") })
    }

    /// Deserialize an RPC payload. A failure means the peer sent bytes that
    /// don't match the expected type — surfaced as a 400 (bad request).
    pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, PatinaError> {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map(|(value, _)| value)
            .map_err(|e| PatinaError::Remote { code: 400, message: format!("decode: {e}") })
    }
}
