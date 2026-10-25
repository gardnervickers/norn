//! Memcached binary protocol framing used by the client.
//!
//! This is intentionally a separate implementation from `norn-kv-server` so
//! that server changes cannot silently change what the client sends or
//! accepts.

pub const HEADER_LEN: usize = 24;

const REQUEST_MAGIC: u8 = 0x80;
const RESPONSE_MAGIC: u8 = 0x81;
pub const OP_GET: u8 = 0x00;
pub const OP_SET: u8 = 0x01;

pub const STATUS_SUCCESS: u16 = 0x0000;
pub const STATUS_KEY_NOT_FOUND: u16 = 0x0001;

fn header(
    out: &mut Vec<u8>,
    opcode: u8,
    key_len: usize,
    extras_len: u8,
    body_len: usize,
    opaque: u32,
) {
    out.push(REQUEST_MAGIC);
    out.push(opcode);
    out.extend_from_slice(&(key_len as u16).to_be_bytes());
    out.push(extras_len);
    out.push(0); // data type
    out.extend_from_slice(&0_u16.to_be_bytes()); // vbucket
    out.extend_from_slice(&(body_len as u32).to_be_bytes());
    out.extend_from_slice(&opaque.to_be_bytes());
    out.extend_from_slice(&0_u64.to_be_bytes()); // cas
}

/// Append a GET request frame.
pub fn encode_get(out: &mut Vec<u8>, key: &[u8], opaque: u32) {
    header(out, OP_GET, key.len(), 0, key.len(), opaque);
    out.extend_from_slice(key);
}

/// Append a SET request frame with zero flags and no expiration.
pub fn encode_set(out: &mut Vec<u8>, key: &[u8], value: &[u8], opaque: u32) {
    header(
        out,
        OP_SET,
        key.len(),
        8,
        8 + key.len() + value.len(),
        opaque,
    );
    out.extend_from_slice(&0_u32.to_be_bytes()); // flags
    out.extend_from_slice(&0_u32.to_be_bytes()); // expiration
    out.extend_from_slice(key);
    out.extend_from_slice(value);
}

/// Encoded length of a GET request.
#[cfg(test)]
pub fn get_len(key_len: usize) -> usize {
    HEADER_LEN + key_len
}

/// Encoded length of a SET request.
#[cfg(test)]
pub fn set_len(key_len: usize, value_len: usize) -> usize {
    HEADER_LEN + 8 + key_len + value_len
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResponseHeader {
    pub opcode: u8,
    pub key_len: usize,
    pub extras_len: usize,
    pub status: u16,
    pub body_len: usize,
    pub opaque: u32,
}

/// Decode a response header from the first [`HEADER_LEN`] bytes of `bytes`.
pub fn decode_response(bytes: &[u8]) -> Result<ResponseHeader, String> {
    debug_assert!(bytes.len() >= HEADER_LEN);
    if bytes[0] != RESPONSE_MAGIC {
        return Err(format!("bad response magic {:#04x}", bytes[0]));
    }
    let key_len = u16::from_be_bytes([bytes[2], bytes[3]]) as usize;
    let extras_len = bytes[4] as usize;
    let status = u16::from_be_bytes([bytes[6], bytes[7]]);
    let body_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let opaque = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
    if key_len + extras_len > body_len {
        return Err(format!(
            "response body {body_len} shorter than key {key_len} + extras {extras_len}"
        ));
    }
    Ok(ResponseHeader {
        opcode: bytes[1],
        key_len,
        extras_len,
        status,
        body_len,
        opaque,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_lengths_match_encoding() {
        let mut out = Vec::new();
        encode_get(&mut out, b"key", 7);
        assert_eq!(out.len(), get_len(3));
        out.clear();
        encode_set(&mut out, b"key", b"value", 7);
        assert_eq!(out.len(), set_len(3, 5));
        assert_eq!(&out[12..16], &7_u32.to_be_bytes());
    }

    #[test]
    fn decodes_response_header() {
        let mut frame = vec![0_u8; HEADER_LEN];
        frame[0] = RESPONSE_MAGIC;
        frame[1] = OP_GET;
        frame[4] = 4;
        frame[6..8].copy_from_slice(&STATUS_SUCCESS.to_be_bytes());
        frame[8..12].copy_from_slice(&10_u32.to_be_bytes());
        frame[12..16].copy_from_slice(&99_u32.to_be_bytes());
        let header = decode_response(&frame).unwrap();
        assert_eq!(header.extras_len, 4);
        assert_eq!(header.body_len, 10);
        assert_eq!(header.opaque, 99);
        frame[0] = 0x80;
        assert!(decode_response(&frame).is_err());
    }
}
