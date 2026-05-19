//! `tokio_util::codec` adapter for [`Envelope`].
//!
//! Composes two layers:
//!
//!   1. `LengthDelimitedCodec` for the on-the-wire framing (4-byte big-endian
//!      `u32` length prefix, as specified in `DESIGN.md` §4).
//!   2. `bincode` (via `serde`) for the body, using `bincode::config::standard()`
//!      which is little-endian + varint — the densest stable bincode setting.
//!
//! The length prefix endianness (BE) and the payload endianness (LE) are
//! intentionally different: the prefix matches `LengthDelimitedCodec`'s default
//! and the byte-layout diagram in `DESIGN.md` §4.1, while the body matches the
//! "little-endian" requirement in §2.

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::envelope::Envelope;
use crate::error::WireError;

/// Default cap on a single envelope's serialized size (64 MiB), per
/// `DESIGN.md` §5.1. Prevents an adversary from triggering an OOM via a
/// fabricated multi-gigabyte length prefix.
pub const DEFAULT_MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// Codec that turns a byte stream into a stream of [`Envelope`] values.
///
/// Drop this into `tokio_util::codec::Framed` to get a `Sink<Envelope>` +
/// `Stream<Item = Result<Envelope, WireError>>` over any `AsyncRead + AsyncWrite`.
pub struct WireCodec {
    inner: LengthDelimitedCodec,
}

impl WireCodec {
    /// Build a codec with the default 64 MiB frame cap.
    pub fn new() -> Self {
        Self::with_max_frame_length(DEFAULT_MAX_FRAME_LEN)
    }

    /// Build a codec with a custom frame cap. Frames larger than `max` (either
    /// produced by the local encoder or signalled by a remote length prefix)
    /// surface as `WireError::Io`.
    pub fn with_max_frame_length(max: usize) -> Self {
        let inner = LengthDelimitedCodec::builder()
            .length_field_length(4)
            .max_frame_length(max)
            .new_codec();
        Self { inner }
    }
}

impl Default for WireCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for WireCodec {
    type Item = Envelope;
    type Error = WireError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some(frame) = self.inner.decode(src)? else {
            return Ok(None);
        };
        let (envelope, _read) =
            bincode::serde::decode_from_slice(&frame, bincode::config::standard())?;
        tracing::trace!(?envelope, "decoded envelope");
        Ok(Some(envelope))
    }
}

impl Encoder<Envelope> for WireCodec {
    type Error = WireError;

    fn encode(&mut self, item: Envelope, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = bincode::serde::encode_to_vec(&item, bincode::config::standard())?;
        tracing::trace!(len = bytes.len(), "encoded envelope");
        self.inner.encode(Bytes::from(bytes), dst)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{ErrorData, RequestData, ResponseData};

    fn roundtrip(envelope: Envelope) {
        let mut codec = WireCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(envelope.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().expect("frame present");
        assert_eq!(decoded, envelope);
        assert!(buf.is_empty(), "buffer drained after decode");
    }

    #[test]
    fn roundtrip_request() {
        roundtrip(Envelope::Request(RequestData {
            id: 42,
            method: "store.put".to_string(),
            payload: vec![1, 2, 3, 4, 5],
        }));
    }

    #[test]
    fn roundtrip_response() {
        roundtrip(Envelope::Response(ResponseData {
            id: 42,
            payload: vec![9, 8, 7],
        }));
    }

    #[test]
    fn roundtrip_error() {
        roundtrip(Envelope::Error(ErrorData {
            id: 42,
            code: 404,
            message: "not found".to_string(),
        }));
    }

    #[test]
    fn roundtrip_heartbeat() {
        roundtrip(Envelope::Heartbeat);
    }

    #[test]
    fn heartbeat_is_tiny() {
        // Sanity check: an empty variant should encode to a near-minimal
        // frame — 4 bytes of length prefix + 1 byte of varint discriminant.
        let mut codec = WireCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(Envelope::Heartbeat, &mut buf).unwrap();
        assert_eq!(buf.len(), 5, "got {:?}", &buf[..]);
    }

    #[test]
    fn partial_frame_yields_none_until_complete() {
        let mut codec = WireCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(
                Envelope::Request(RequestData {
                    id: 1,
                    method: "m".to_string(),
                    payload: vec![0; 16],
                }),
                &mut buf,
            )
            .unwrap();

        let full = buf.split().freeze();
        let mid = full.len() / 2;
        let (head, tail) = full.split_at(mid);

        let mut staging = BytesMut::from(head);
        assert!(codec.decode(&mut staging).unwrap().is_none(), "head only");

        staging.extend_from_slice(tail);
        let decoded = codec.decode(&mut staging).unwrap().expect("frame complete");
        assert!(matches!(decoded, Envelope::Request(_)));
        assert!(staging.is_empty());
    }

    #[test]
    fn back_to_back_frames_decode_independently() {
        let mut codec = WireCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(Envelope::Heartbeat, &mut buf).unwrap();
        codec
            .encode(
                Envelope::Response(ResponseData {
                    id: 7,
                    payload: vec![0xab, 0xcd],
                }),
                &mut buf,
            )
            .unwrap();

        let first = codec.decode(&mut buf).unwrap().expect("first frame");
        assert_eq!(first, Envelope::Heartbeat);

        let second = codec.decode(&mut buf).unwrap().expect("second frame");
        match second {
            Envelope::Response(r) => {
                assert_eq!(r.id, 7);
                assert_eq!(r.payload, vec![0xab, 0xcd]);
            }
            other => panic!("expected Response, got {other:?}"),
        }
        assert!(buf.is_empty());
    }

    #[test]
    fn oversized_frame_prefix_is_rejected() {
        // Build a buffer that claims a 1000-byte body, hand it to a codec
        // capped at 100 bytes. `LengthDelimitedCodec` raises io::ErrorKind::InvalidData.
        let mut codec = WireCodec::with_max_frame_length(100);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&1000u32.to_be_bytes());

        match codec.decode(&mut buf) {
            Err(WireError::Io(e)) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
            }
            other => panic!("expected Io(InvalidData), got {other:?}"),
        }
    }

    #[test]
    fn corrupted_body_surfaces_decode_error() {
        // Length prefix says 3 bytes, body is three varint-continuation bytes.
        // Framing succeeds, bincode fails on the malformed discriminant.
        let mut codec = WireCodec::new();
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&3u32.to_be_bytes());
        buf.extend_from_slice(&[0xff, 0xff, 0xff]);

        match codec.decode(&mut buf) {
            Err(WireError::Decode(_)) => {}
            other => panic!("expected Decode error, got {other:?}"),
        }
    }
}
