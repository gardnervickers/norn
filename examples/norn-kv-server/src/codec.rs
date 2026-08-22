//! Incremental Memcached-binary framing and response encoding.

use thiserror::Error;

use crate::protocol::{
    append_response, EncodeError, ProtocolError, Request, RequestHeader, Status, HEADER_LEN,
};

/// One decoded request frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedFrame<'a> {
    /// A valid request.
    Request(Request<'a>),
    /// A framed request with invalid command arguments.
    Invalid {
        /// The validated header used to correlate the error response.
        header: RequestHeader,
        /// The command validation failure.
        error: ProtocolError,
    },
}

/// Result of attempting to decode one frame from a byte slice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeStatus<'a> {
    /// More bytes are required for a complete frame.
    NeedMore,
    /// One complete frame was decoded.
    Frame {
        /// The decoded request or command validation error.
        frame: DecodedFrame<'a>,
        /// Number of input bytes consumed by this frame.
        consumed: usize,
    },
}

/// Stateless decoder for one Memcached-binary frame at a time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameDecoder {
    max_body_len: usize,
}

impl FrameDecoder {
    /// Construct a decoder with a maximum accepted request body length.
    pub fn new(max_body_len: usize) -> Self {
        assert!(max_body_len != 0);
        Self { max_body_len }
    }

    /// Decode the first frame in `bytes` without copying its key or value.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError`] when a complete header is malformed or declares
    /// a body larger than the configured limit. Command-specific validation
    /// errors are returned as [`DecodedFrame::Invalid`] because the frame
    /// boundary remains trustworthy and the server can send an error response.
    pub fn decode<'a>(&self, bytes: &'a [u8]) -> Result<DecodeStatus<'a>, DecodeError> {
        if bytes.len() < HEADER_LEN {
            return Ok(DecodeStatus::NeedMore);
        }

        let header = RequestHeader::decode(&bytes[..HEADER_LEN]).map_err(DecodeError::Header)?;
        if header.body_len() > self.max_body_len {
            return Err(DecodeError::BodyTooLarge {
                declared: header.body_len(),
                maximum: self.max_body_len,
            });
        }

        let frame_len = HEADER_LEN + header.body_len();
        if bytes.len() < frame_len {
            return Ok(DecodeStatus::NeedMore);
        }

        let body = &bytes[HEADER_LEN..frame_len];
        let frame = decode_body(header, body);
        Ok(DecodeStatus::Frame {
            frame,
            consumed: frame_len,
        })
    }
}

pub(crate) fn decode_body(header: RequestHeader, body: &[u8]) -> DecodedFrame<'_> {
    match Request::decode(header, body) {
        Ok(request) => DecodedFrame::Request(request),
        Err(error) => DecodedFrame::Invalid { header, error },
    }
}

/// Fatal frame-decoding failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The fixed request header is malformed.
    #[error("invalid request header: {0}")]
    Header(ProtocolError),
    /// The declared body exceeds the configured server limit.
    #[error("request body length {declared} exceeds configured maximum {maximum}")]
    BodyTooLarge {
        /// Body length declared by the request.
        declared: usize,
        /// Maximum body length accepted by the server.
        maximum: usize,
    },
}

/// Encoder which appends ordered response frames to one connection batch.
pub struct ResponseEncoder<'a> {
    out: &'a mut Vec<u8>,
}

impl<'a> ResponseEncoder<'a> {
    /// Wrap an output buffer for response encoding.
    pub fn new(out: &'a mut Vec<u8>) -> Self {
        Self { out }
    }

    /// Return the number of encoded bytes currently in the batch.
    pub fn len(&self) -> usize {
        self.out.len()
    }

    /// Return whether the response batch is empty.
    pub fn is_empty(&self) -> bool {
        self.out.is_empty()
    }

    /// Append a response correlated with `header`.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError`] if a response field cannot be represented by
    /// the Memcached binary header.
    pub fn append(
        &mut self,
        header: RequestHeader,
        status: Status,
        cas: u64,
        extras: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), EncodeError> {
        append_response(
            self.out,
            header.opcode(),
            status,
            header.opaque(),
            cas,
            extras,
            key,
            value,
        )
    }

    /// Append a response with an empty body and zero CAS value.
    ///
    /// # Errors
    ///
    /// Returns [`EncodeError`] if the response cannot be represented by the
    /// Memcached binary header.
    pub fn append_empty(
        &mut self,
        header: RequestHeader,
        status: Status,
    ) -> Result<(), EncodeError> {
        self.append(header, status, 0, &[], &[], &[])
    }
}

impl std::fmt::Debug for ResponseEncoder<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResponseEncoder")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{OP_GET, OP_SET, REQUEST_MAGIC, RESPONSE_MAGIC};

    fn request(opcode: u8, key: &[u8], extras: &[u8], value: &[u8]) -> Vec<u8> {
        let body_len = extras.len() + key.len() + value.len();
        let mut bytes = Vec::with_capacity(HEADER_LEN + body_len);
        bytes.push(REQUEST_MAGIC);
        bytes.push(opcode);
        bytes.extend_from_slice(&(key.len() as u16).to_be_bytes());
        bytes.push(extras.len() as u8);
        bytes.push(0);
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&(body_len as u32).to_be_bytes());
        bytes.extend_from_slice(&7_u32.to_be_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes());
        bytes.extend_from_slice(extras);
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        bytes
    }

    #[test]
    fn waits_at_every_partial_frame_boundary() {
        let frame = request(OP_GET, b"key", &[], &[]);
        let decoder = FrameDecoder::new(1024);
        for prefix in 0..frame.len() {
            assert_eq!(
                decoder.decode(&frame[..prefix]).unwrap(),
                DecodeStatus::NeedMore
            );
        }
        let DecodeStatus::Frame { frame, consumed } = decoder.decode(&frame).unwrap() else {
            panic!("complete frame remained incomplete");
        };
        assert_eq!(consumed, HEADER_LEN + 3);
        assert!(matches!(frame, DecodedFrame::Request(_)));
    }

    #[test]
    fn decodes_coalesced_frames_one_at_a_time() {
        let first = request(OP_GET, b"first", &[], &[]);
        let second = request(OP_GET, b"second", &[], &[]);
        let mut bytes = first.clone();
        bytes.extend_from_slice(&second);
        let decoder = FrameDecoder::new(1024);

        let DecodeStatus::Frame { consumed, .. } = decoder.decode(&bytes).unwrap() else {
            panic!("first frame remained incomplete");
        };
        assert_eq!(consumed, first.len());
        let DecodeStatus::Frame {
            consumed: second_consumed,
            ..
        } = decoder.decode(&bytes[consumed..]).unwrap()
        else {
            panic!("second frame remained incomplete");
        };
        assert_eq!(second_consumed, second.len());
    }

    #[test]
    fn preserves_framed_command_errors() {
        let mut extras = Vec::new();
        extras.extend_from_slice(&0_u32.to_be_bytes());
        extras.extend_from_slice(&1_u32.to_be_bytes());
        let frame = request(OP_SET, b"key", &extras, b"value");
        let decoder = FrameDecoder::new(1024);
        let DecodeStatus::Frame { frame, .. } = decoder.decode(&frame).unwrap() else {
            panic!("complete frame remained incomplete");
        };
        assert!(matches!(
            frame,
            DecodedFrame::Invalid {
                error: ProtocolError::ExpirationUnsupported(1),
                ..
            }
        ));
    }

    #[test]
    fn rejects_oversized_body_before_waiting_for_it() {
        let frame = request(OP_GET, b"key", &[], &[]);
        let decoder = FrameDecoder::new(2);
        assert_eq!(
            decoder.decode(&frame[..HEADER_LEN]),
            Err(DecodeError::BodyTooLarge {
                declared: 3,
                maximum: 2,
            })
        );
    }

    #[test]
    fn response_encoder_preserves_request_correlation() {
        let frame = request(OP_GET, b"key", &[], &[]);
        let decoder = FrameDecoder::new(1024);
        let DecodeStatus::Frame {
            frame: DecodedFrame::Request(request),
            ..
        } = decoder.decode(&frame).unwrap()
        else {
            panic!("GET did not decode");
        };
        let mut out = Vec::new();
        let mut encoder = ResponseEncoder::new(&mut out);
        encoder
            .append(request.header(), Status::Success, 0, &[], &[], b"value")
            .unwrap();
        assert_eq!(out[0], RESPONSE_MAGIC);
        assert_eq!(&out[12..16], &7_u32.to_be_bytes());
        assert_eq!(&out[HEADER_LEN..], b"value");
    }
}
