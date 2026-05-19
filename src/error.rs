//! Error type for the wire layer.
//!
//! `LengthDelimitedCodec` surfaces framing problems (oversized frames,
//! malformed length prefixes, underlying socket failures) as `io::Error`,
//! so a single `Io` variant covers both transport and framing faults.

/// Errors produced while encoding or decoding `Envelope` values.
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
