//! Error types for the wire and RPC layers.
//!
//! `WireError` covers byte-level concerns (framing, serialization, socket I/O).
//! `PatinaError` is the higher-level error surface exposed to RPC callers —
//! transport failure, remote-reported errors, and connection lifecycle events.
//! `RpcError` is retained as a backward-compatible alias of `PatinaError`.

use crate::envelope::ErrorData;

/// Errors produced while encoding or decoding `Envelope` values.
///
/// `LengthDelimitedCodec` surfaces framing problems (oversized frames,
/// malformed length prefixes, underlying socket failures) as `io::Error`,
/// so a single `Io` variant covers both transport and framing faults.
///
/// Per `DESIGN.md` §5.3 any of these variants taints the byte stream and the
/// caller is expected to drop the connection rather than attempt recovery.
#[derive(Debug, thiserror::Error)]
pub enum WireError {
    /// Transport or framing failure (TCP read/write, oversized frame, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Bincode failed to serialize an outgoing envelope.
    #[error("bincode encode: {0}")]
    Encode(#[from] bincode::error::EncodeError),

    /// Bincode failed to deserialize an incoming frame.
    #[error("bincode decode: {0}")]
    Decode(#[from] bincode::error::DecodeError),
}

/// Errors observable by RPC callers.
///
/// Distinct from [`WireError`] so the wire layer keeps its narrow vocabulary
/// (bytes in, bytes out) and the RPC layer surfaces higher-level concerns
/// (remote errors, connection lifecycle, dispatch failures). This is also the
/// error type the `#[patina_rpc::service]` macro uses in generated trait
/// method signatures (`Result<T, PatinaError>`).
#[derive(Debug, thiserror::Error)]
pub enum PatinaError {
    /// Transport or codec failure (read/write/decode/encode).
    #[error("wire error: {0}")]
    Wire(#[from] WireError),

    /// The remote peer returned an `Envelope::Error` for this call.
    #[error("remote error {code}: {message}")]
    Remote {
        /// HTTP-like status code chosen by the remote handler.
        code: u16,
        /// Human-readable message from the remote handler.
        message: String,
    },

    /// The connection was closed before a response arrived, or after the
    /// `Client` was marked closed by its reader task.
    #[error("connection closed")]
    Closed,

    /// The server received a request for a method it has no handler for.
    /// Surfaced server-side when constructing the outbound error envelope;
    /// the corresponding client-side reception is a `Remote { code: 404, .. }`.
    #[error("unknown method: {0}")]
    UnknownMethod(String),
}

/// Backward-compatible alias. Phase 1–2 code referred to this type as
/// `RpcError`; it was renamed to [`PatinaError`] in Phase 3 to match the
/// service-macro vocabulary.
pub type RpcError = PatinaError;

impl PatinaError {
    /// Construct an application-level failure for a handler to return. Surfaces
    /// to the caller as [`PatinaError::Remote`] with the given code and message.
    pub fn application(code: u16, message: impl Into<String>) -> Self {
        PatinaError::Remote { code, message: message.into() }
    }
}

impl From<ErrorData> for PatinaError {
    fn from(e: ErrorData) -> Self {
        PatinaError::Remote { code: e.code, message: e.message }
    }
}

impl From<std::io::Error> for PatinaError {
    fn from(e: std::io::Error) -> Self {
        PatinaError::Wire(WireError::Io(e))
    }
}
