//! Memcached binary framing and command validation.

use thiserror::Error;

/// Length of a Memcached binary request or response header.
pub const HEADER_LEN: usize = 24;
/// Maximum key length accepted by the server.
pub const MAX_KEY_LEN: usize = 250;

/// Request packet magic.
pub const REQUEST_MAGIC: u8 = 0x80;
/// Response packet magic.
pub const RESPONSE_MAGIC: u8 = 0x81;

/// GET opcode.
pub const OP_GET: u8 = 0x00;
/// SET opcode.
pub const OP_SET: u8 = 0x01;
/// DELETE opcode.
pub const OP_DELETE: u8 = 0x04;
/// QUIT opcode.
pub const OP_QUIT: u8 = 0x07;
/// GETQ opcode.
pub const OP_GETQ: u8 = 0x09;
/// NOOP opcode.
pub const OP_NOOP: u8 = 0x0a;
/// VERSION opcode.
pub const OP_VERSION: u8 = 0x0b;
/// STAT opcode.
pub const OP_STAT: u8 = 0x10;
/// SETQ opcode.
pub const OP_SETQ: u8 = 0x11;
/// DELETEQ opcode.
pub const OP_DELETEQ: u8 = 0x14;

/// A validated request header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestHeader {
    opcode: u8,
    key_len: usize,
    extras_len: usize,
    body_len: usize,
    opaque: u32,
    cas: u64,
}

impl RequestHeader {
    /// Decode a 24-byte request header.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] when the header length, magic, data type, or
    /// declared key/extras lengths are invalid.
    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() != HEADER_LEN {
            return Err(ProtocolError::HeaderLength(bytes.len()));
        }
        if bytes[0] != REQUEST_MAGIC {
            return Err(ProtocolError::Magic(bytes[0]));
        }
        if bytes[5] != 0 {
            return Err(ProtocolError::DataType(bytes[5]));
        }

        let key_len = u16::from_be_bytes([bytes[2], bytes[3]]) as usize;
        let extras_len = bytes[4] as usize;
        let body_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let prefix_len = extras_len
            .checked_add(key_len)
            .ok_or(ProtocolError::BodyLayout)?;
        if prefix_len > body_len {
            return Err(ProtocolError::BodyLayout);
        }
        if key_len > MAX_KEY_LEN {
            return Err(ProtocolError::KeyTooLong(key_len));
        }

        Ok(Self {
            opcode: bytes[1],
            key_len,
            extras_len,
            body_len,
            opaque: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
            cas: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        })
    }

    /// Return the raw opcode.
    pub fn opcode(self) -> u8 {
        self.opcode
    }

    /// Return the declared body length.
    pub fn body_len(self) -> usize {
        self.body_len
    }

    /// Return the request correlation token.
    pub fn opaque(self) -> u32 {
        self.opaque
    }

    /// Return the request CAS value.
    pub fn cas(self) -> u64 {
        self.cas
    }
}

/// A decoded command borrowing its request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command<'a> {
    /// Read one key.
    Get {
        /// Key to read.
        key: &'a [u8],
        /// Suppress a miss response.
        quiet: bool,
    },
    /// Insert or replace one key.
    Set {
        /// Key to update.
        key: &'a [u8],
        /// Application-defined flags.
        flags: u32,
        /// Value bytes.
        value: &'a [u8],
        /// Suppress a successful response.
        quiet: bool,
    },
    /// Delete one key.
    Delete {
        /// Key to delete.
        key: &'a [u8],
        /// Suppress a successful response.
        quiet: bool,
    },
    /// Return an empty success response.
    Noop,
    /// Return the server version.
    Version,
    /// Return server statistics. The optional key selects a statistics group.
    Stat {
        /// Optional statistics group selector.
        key: &'a [u8],
    },
    /// Return success and close the connection.
    Quit,
    /// An unsupported opcode whose body was framed successfully.
    Unknown,
}

/// A decoded request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Request<'a> {
    header: RequestHeader,
    command: Command<'a>,
}

impl<'a> Request<'a> {
    /// Decode and validate a request body against its header.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] when the body length or command-specific
    /// extras/key/value layout is invalid. Expiration and CAS are unsupported.
    pub fn decode(header: RequestHeader, body: &'a [u8]) -> Result<Self, ProtocolError> {
        if body.len() != header.body_len {
            return Err(ProtocolError::BodyLength {
                declared: header.body_len,
                actual: body.len(),
            });
        }

        let key_start = header.extras_len;
        let key_end = key_start + header.key_len;
        let key = &body[key_start..key_end];
        let value = &body[key_end..];

        let command = match header.opcode {
            OP_GET | OP_GETQ => {
                require_layout(header, 0, true, true)?;
                Command::Get {
                    key,
                    quiet: header.opcode == OP_GETQ,
                }
            }
            OP_SET | OP_SETQ => {
                require_layout(header, 8, true, false)?;
                if header.cas != 0 {
                    return Err(ProtocolError::CasUnsupported);
                }
                let flags = u32::from_be_bytes(body[0..4].try_into().unwrap());
                let expiration = u32::from_be_bytes(body[4..8].try_into().unwrap());
                if expiration != 0 {
                    return Err(ProtocolError::ExpirationUnsupported(expiration));
                }
                Command::Set {
                    key,
                    flags,
                    value,
                    quiet: header.opcode == OP_SETQ,
                }
            }
            OP_DELETE | OP_DELETEQ => {
                require_layout(header, 0, true, true)?;
                if header.cas != 0 {
                    return Err(ProtocolError::CasUnsupported);
                }
                Command::Delete {
                    key,
                    quiet: header.opcode == OP_DELETEQ,
                }
            }
            OP_NOOP => {
                require_layout(header, 0, false, true)?;
                Command::Noop
            }
            OP_VERSION => {
                require_layout(header, 0, false, true)?;
                Command::Version
            }
            OP_STAT => {
                if header.extras_len != 0 || header.key_len != header.body_len {
                    return Err(ProtocolError::CommandLayout);
                }
                Command::Stat { key }
            }
            OP_QUIT => {
                require_layout(header, 0, false, true)?;
                Command::Quit
            }
            _ => Command::Unknown,
        };

        Ok(Self { header, command })
    }

    /// Return the validated header.
    pub fn header(self) -> RequestHeader {
        self.header
    }

    /// Return the decoded command.
    pub fn command(self) -> Command<'a> {
        self.command
    }
}

fn require_layout(
    header: RequestHeader,
    extras_len: usize,
    key_required: bool,
    no_value: bool,
) -> Result<(), ProtocolError> {
    if header.extras_len != extras_len {
        return Err(ProtocolError::CommandLayout);
    }
    if key_required && header.key_len == 0 {
        return Err(ProtocolError::EmptyKey);
    }
    if !key_required && header.key_len != 0 {
        return Err(ProtocolError::CommandLayout);
    }
    if no_value && header.body_len != extras_len + header.key_len {
        return Err(ProtocolError::CommandLayout);
    }
    Ok(())
}

/// Memcached response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum Status {
    /// Operation completed successfully.
    Success = 0x0000,
    /// The requested key does not exist.
    KeyNotFound = 0x0001,
    /// The request arguments are invalid or unsupported.
    InvalidArguments = 0x0004,
    /// The opcode is unsupported.
    UnknownCommand = 0x0081,
    /// The server encountered an internal failure.
    InternalError = 0x0084,
}

/// Append one complete response frame to `out`.
///
/// # Errors
///
/// Returns [`EncodeError`] when the key or total body cannot be represented by
/// the binary protocol header.
#[allow(clippy::too_many_arguments)]
pub fn append_response(
    out: &mut Vec<u8>,
    opcode: u8,
    status: Status,
    opaque: u32,
    cas: u64,
    extras: &[u8],
    key: &[u8],
    value: &[u8],
) -> Result<(), EncodeError> {
    let key_len = u16::try_from(key.len()).map_err(|_| EncodeError::KeyLength(key.len()))?;
    let extras_len =
        u8::try_from(extras.len()).map_err(|_| EncodeError::ExtrasLength(extras.len()))?;
    let body_len = extras
        .len()
        .checked_add(key.len())
        .and_then(|len| len.checked_add(value.len()))
        .ok_or(EncodeError::BodyLength(usize::MAX))?;
    let body_len_u32 = u32::try_from(body_len).map_err(|_| EncodeError::BodyLength(body_len))?;

    out.reserve(HEADER_LEN + body_len);
    out.push(RESPONSE_MAGIC);
    out.push(opcode);
    out.extend_from_slice(&key_len.to_be_bytes());
    out.push(extras_len);
    out.push(0);
    out.extend_from_slice(&(status as u16).to_be_bytes());
    out.extend_from_slice(&body_len_u32.to_be_bytes());
    out.extend_from_slice(&opaque.to_be_bytes());
    out.extend_from_slice(&cas.to_be_bytes());
    out.extend_from_slice(extras);
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    Ok(())
}

/// Protocol framing or command-validation failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// The header was not exactly 24 bytes.
    #[error("request header length is {0}, expected {HEADER_LEN}")]
    HeaderLength(usize),
    /// The packet magic is not a request magic byte.
    #[error("invalid request magic 0x{0:02x}")]
    Magic(u8),
    /// The packet data type is unsupported.
    #[error("unsupported data type {0}")]
    DataType(u8),
    /// The key exceeds the configured protocol limit.
    #[error("key length {0} exceeds {MAX_KEY_LEN}")]
    KeyTooLong(usize),
    /// Extras and key lengths exceed the declared body.
    #[error("extras and key lengths exceed the declared body")]
    BodyLayout,
    /// The received body length differs from the declaration.
    #[error("request body length is {actual}, expected {declared}")]
    BodyLength {
        /// Declared body length.
        declared: usize,
        /// Received body length.
        actual: usize,
    },
    /// A command has an invalid extras/key/value layout.
    #[error("invalid command body layout")]
    CommandLayout,
    /// A command that requires a key supplied an empty key.
    #[error("key must not be empty")]
    EmptyKey,
    /// Expiration is not implemented by the in-memory server.
    #[error("expiration {0} is unsupported; use zero")]
    ExpirationUnsupported(u32),
    /// Compare-and-swap is not implemented by the in-memory server.
    #[error("CAS is unsupported; use zero")]
    CasUnsupported,
}

/// Response-encoding failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum EncodeError {
    /// Response key length exceeds the header field.
    #[error("response key length {0} exceeds u16")]
    KeyLength(usize),
    /// Response extras length exceeds the header field.
    #[error("response extras length {0} exceeds u8")]
    ExtrasLength(usize),
    /// Response body length exceeds the header field.
    #[error("response body length {0} exceeds u32")]
    BodyLength(usize),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(opcode: u8, key_len: u16, extras_len: u8, body_len: u32) -> [u8; HEADER_LEN] {
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[0] = REQUEST_MAGIC;
        bytes[1] = opcode;
        bytes[2..4].copy_from_slice(&key_len.to_be_bytes());
        bytes[4] = extras_len;
        bytes[8..12].copy_from_slice(&body_len.to_be_bytes());
        bytes[12..16].copy_from_slice(&0x1234_5678_u32.to_be_bytes());
        bytes
    }

    #[test]
    fn decodes_get_and_preserves_opaque() {
        let header = RequestHeader::decode(&header(OP_GET, 3, 0, 3)).unwrap();
        let request = Request::decode(header, b"key").unwrap();
        assert_eq!(request.header().opaque(), 0x1234_5678);
        assert_eq!(
            request.command(),
            Command::Get {
                key: b"key",
                quiet: false
            }
        );
    }

    #[test]
    fn decodes_set_without_copying_body() {
        let mut body = Vec::new();
        body.extend_from_slice(&7_u32.to_be_bytes());
        body.extend_from_slice(&0_u32.to_be_bytes());
        body.extend_from_slice(b"keyvalue");
        let header = RequestHeader::decode(&header(OP_SET, 3, 8, body.len() as u32)).unwrap();
        let request = Request::decode(header, &body).unwrap();
        assert_eq!(
            request.command(),
            Command::Set {
                key: b"key",
                flags: 7,
                value: b"value",
                quiet: false,
            }
        );
    }

    #[test]
    fn rejects_expiration_and_command_layout_errors() {
        let mut body = Vec::new();
        body.extend_from_slice(&0_u32.to_be_bytes());
        body.extend_from_slice(&1_u32.to_be_bytes());
        body.extend_from_slice(b"kv");
        let set_header = RequestHeader::decode(&header(OP_SET, 1, 8, 10)).unwrap();
        assert_eq!(
            Request::decode(set_header, &body),
            Err(ProtocolError::ExpirationUnsupported(1))
        );

        let get_header = RequestHeader::decode(&header(OP_GET, 1, 1, 2)).unwrap();
        assert_eq!(
            Request::decode(get_header, b"xk"),
            Err(ProtocolError::CommandLayout)
        );
    }

    #[test]
    fn unknown_opcode_remains_framed() {
        let header = RequestHeader::decode(&header(0xfe, 0, 0, 3)).unwrap();
        let request = Request::decode(header, b"abc").unwrap();
        assert_eq!(request.command(), Command::Unknown);
    }

    #[test]
    fn encodes_binary_response() {
        let mut response = Vec::new();
        append_response(
            &mut response,
            OP_GET,
            Status::Success,
            0x1234_5678,
            0,
            &9_u32.to_be_bytes(),
            b"",
            b"value",
        )
        .unwrap();
        assert_eq!(response.len(), HEADER_LEN + 4 + 5);
        assert_eq!(response[0], RESPONSE_MAGIC);
        assert_eq!(response[1], OP_GET);
        assert_eq!(&response[6..8], &0_u16.to_be_bytes());
        assert_eq!(&response[8..12], &9_u32.to_be_bytes());
        assert_eq!(&response[12..16], &0x1234_5678_u32.to_be_bytes());
        assert_eq!(&response[HEADER_LEN..HEADER_LEN + 4], &9_u32.to_be_bytes());
        assert_eq!(&response[HEADER_LEN + 4..], b"value");
    }
}
