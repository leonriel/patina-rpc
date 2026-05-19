//! Wire-level message types.
//!
//! Every byte that travels across a Patina connection is a [`Envelope`].
//! The variants partition the protocol into request/response/error flows plus
//! a lightweight keep-alive (`Heartbeat`). The inner `payload: Vec<u8>` fields
//! are intentionally opaque to this layer — generated RPC stubs serialize the
//! method arguments into those bytes before the envelope reaches the wire.

use serde::{Deserialize, Serialize};

/// Top-level message wrapper. See `DESIGN.md` §3.1.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Envelope {
    Request(RequestData),
    Response(ResponseData),
    Error(ErrorData),
    Heartbeat,
}

/// Initiates an RPC call. `id` is unique per in-flight request on a connection,
/// `method` selects the server-side handler, and `payload` carries the
/// pre-serialized arguments.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct RequestData {
    pub id: u64,
    pub method: String,
    pub payload: Vec<u8>,
}

/// Successful reply to a `RequestData`. `id` must equal the request's `id`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ResponseData {
    pub id: u64,
    pub payload: Vec<u8>,
}

/// Failure reply. `code` follows the HTTP-like convention from `DESIGN.md` §3.2.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ErrorData {
    pub id: u64,
    pub code: u16,
    pub message: String,
}
