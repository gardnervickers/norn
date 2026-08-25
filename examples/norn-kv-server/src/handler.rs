//! In-memory command execution independent of socket framing and scheduling.

use std::cell::RefCell;
use std::rc::Rc;

use crate::codec::{DecodedFrame, ResponseEncoder};
use crate::memory::MemoryStore;
use crate::protocol::{Command, EncodeError, Request, Status};

const VERSION: &[u8] = b"norn-kv-server 0.1.0";

/// Backend selected for command execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HandlerConfig {
    /// Execute commands against a worker-local in-memory key/value store.
    #[default]
    Memory,
    /// Return a fixed value for every GET without owning a key/value store.
    FixedResponse {
        /// Number of value bytes appended after the four-byte GET flags field.
        value_len: usize,
    },
}

impl HandlerConfig {
    /// Construct the configured command handler.
    pub fn build(self) -> MemoryHandler {
        match self {
            Self::Memory => MemoryHandler::new(MemoryStore::new()),
            Self::FixedResponse { value_len } => MemoryHandler::fixed_response(value_len),
        }
    }
}

#[derive(Debug, Clone)]
enum Backend {
    Memory(Rc<RefCell<MemoryStore>>),
    FixedResponse(Rc<[u8]>),
}

/// Command handler shared by local connection tasks.
#[derive(Debug, Clone)]
pub struct MemoryHandler {
    backend: Backend,
}

impl MemoryHandler {
    /// Construct a handler backed by `store`.
    pub fn new(store: MemoryStore) -> Self {
        Self {
            backend: Backend::Memory(Rc::new(RefCell::new(store))),
        }
    }

    /// Construct a network-only handler which returns a fixed GET value.
    pub fn fixed_response(value_len: usize) -> Self {
        Self {
            backend: Backend::FixedResponse(vec![0x5a; value_len].into()),
        }
    }

    pub(crate) fn handle(
        &self,
        frame: DecodedFrame<'_>,
        out: &mut ResponseEncoder<'_>,
    ) -> Result<ConnectionAction, EncodeError> {
        match frame {
            DecodedFrame::Request(request) => self.execute(request, out),
            DecodedFrame::Invalid { header, error } => {
                let message = error.to_string();
                out.append(
                    header,
                    Status::InvalidArguments,
                    0,
                    &[],
                    &[],
                    message.as_bytes(),
                )?;
                Ok(ConnectionAction::Continue)
            }
        }
    }

    fn execute(
        &self,
        request: Request<'_>,
        out: &mut ResponseEncoder<'_>,
    ) -> Result<ConnectionAction, EncodeError> {
        self.execute_command(request.header(), request.command(), out)
    }

    pub(crate) fn execute_command(
        &self,
        header: crate::protocol::RequestHeader,
        command: Command<'_>,
        out: &mut ResponseEncoder<'_>,
    ) -> Result<ConnectionAction, EncodeError> {
        match command {
            Command::Get { key, quiet } => {
                match &self.backend {
                    Backend::Memory(store) => {
                        let store = store.borrow();
                        if let Some(entry) = store.get(key) {
                            out.append(
                                header,
                                Status::Success,
                                0,
                                &entry.flags().to_be_bytes(),
                                &[],
                                entry.value(),
                            )?;
                        } else if !quiet {
                            out.append_empty(header, Status::KeyNotFound)?;
                        }
                    }
                    Backend::FixedResponse(value) => {
                        out.append(header, Status::Success, 0, &0_u32.to_be_bytes(), &[], value)?;
                    }
                }
                Ok(ConnectionAction::Continue)
            }
            Command::Set {
                key,
                flags,
                value,
                quiet,
            } => {
                if let Backend::Memory(store) = &self.backend {
                    store.borrow_mut().set(key, flags, value);
                }
                if !quiet {
                    out.append_empty(header, Status::Success)?;
                }
                Ok(ConnectionAction::Continue)
            }
            Command::Delete { key, quiet } => {
                let deleted = match &self.backend {
                    Backend::Memory(store) => store.borrow_mut().delete(key),
                    Backend::FixedResponse(_) => true,
                };
                if deleted {
                    if !quiet {
                        out.append_empty(header, Status::Success)?;
                    }
                } else {
                    out.append_empty(header, Status::KeyNotFound)?;
                }
                Ok(ConnectionAction::Continue)
            }
            Command::Noop => {
                out.append_empty(header, Status::Success)?;
                Ok(ConnectionAction::Continue)
            }
            Command::Version => {
                out.append(header, Status::Success, 0, &[], &[], VERSION)?;
                Ok(ConnectionAction::Continue)
            }
            Command::Stat { key } => {
                if !key.is_empty() {
                    out.append_empty(header, Status::InvalidArguments)?;
                    return Ok(ConnectionAction::Continue);
                }
                let (items, bytes) = match &self.backend {
                    Backend::Memory(store) => {
                        let stats = store.borrow().stats();
                        (stats.items.to_string(), stats.value_bytes.to_string())
                    }
                    Backend::FixedResponse(value) => ("0".to_owned(), value.len().to_string()),
                };
                out.append(
                    header,
                    Status::Success,
                    0,
                    &[],
                    b"curr_items",
                    items.as_bytes(),
                )?;
                out.append(header, Status::Success, 0, &[], b"bytes", bytes.as_bytes())?;
                out.append_empty(header, Status::Success)?;
                Ok(ConnectionAction::Continue)
            }
            Command::Quit => {
                out.append_empty(header, Status::Success)?;
                Ok(ConnectionAction::Close)
            }
            Command::Unknown => {
                out.append_empty(header, Status::UnknownCommand)?;
                Ok(ConnectionAction::Continue)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConnectionAction {
    Continue,
    Close,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{DecodeStatus, FrameDecoder};
    use crate::protocol::{
        OP_DELETE, OP_DELETEQ, OP_GET, OP_GETQ, OP_NOOP, OP_QUIT, OP_SET, OP_SETQ, OP_STAT,
        OP_VERSION, REQUEST_MAGIC, RESPONSE_MAGIC,
    };

    fn request(opcode: u8, key: &[u8], extras: &[u8], value: &[u8], opaque: u32) -> Vec<u8> {
        let body_len = extras.len() + key.len() + value.len();
        let mut bytes = Vec::with_capacity(crate::protocol::HEADER_LEN + body_len);
        bytes.push(REQUEST_MAGIC);
        bytes.push(opcode);
        bytes.extend_from_slice(&(key.len() as u16).to_be_bytes());
        bytes.push(extras.len() as u8);
        bytes.push(0);
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&(body_len as u32).to_be_bytes());
        bytes.extend_from_slice(&opaque.to_be_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes());
        bytes.extend_from_slice(extras);
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        bytes
    }

    fn handle(handler: &MemoryHandler, bytes: &[u8]) -> (ConnectionAction, Vec<u8>) {
        let decoder = FrameDecoder::new(64 * 1024);
        let DecodeStatus::Frame { frame, consumed } = decoder.decode(bytes).unwrap() else {
            panic!("complete request remained incomplete");
        };
        assert_eq!(consumed, bytes.len());
        let mut out = Vec::new();
        let action = handler
            .handle(frame, &mut ResponseEncoder::new(&mut out))
            .unwrap();
        (action, out)
    }

    fn status(response: &[u8]) -> u16 {
        u16::from_be_bytes(response[6..8].try_into().unwrap())
    }

    fn response_count(mut responses: &[u8]) -> usize {
        let mut count = 0;
        while !responses.is_empty() {
            assert!(responses.len() >= crate::protocol::HEADER_LEN);
            assert_eq!(responses[0], RESPONSE_MAGIC);
            let body_len = u32::from_be_bytes(responses[8..12].try_into().unwrap()) as usize;
            responses = &responses[crate::protocol::HEADER_LEN + body_len..];
            count += 1;
        }
        count
    }

    #[test]
    fn set_get_replace_and_delete() {
        let handler = MemoryHandler::new(MemoryStore::new());
        let mut extras = Vec::new();
        extras.extend_from_slice(&3_u32.to_be_bytes());
        extras.extend_from_slice(&0_u32.to_be_bytes());

        let (_, set) = handle(&handler, &request(OP_SET, b"key", &extras, b"first", 1));
        assert_eq!(status(&set), Status::Success as u16);

        let (_, get) = handle(&handler, &request(OP_GET, b"key", &[], &[], 2));
        assert_eq!(status(&get), Status::Success as u16);
        assert_eq!(&get[24..28], &3_u32.to_be_bytes());
        assert_eq!(&get[28..], b"first");

        let (_, replace) = handle(
            &handler,
            &request(OP_SET, b"key", &extras, b"replacement", 3),
        );
        assert_eq!(status(&replace), Status::Success as u16);
        let (_, delete) = handle(&handler, &request(OP_DELETE, b"key", &[], &[], 4));
        assert_eq!(status(&delete), Status::Success as u16);
        let (_, missing) = handle(&handler, &request(OP_GET, b"key", &[], &[], 5));
        assert_eq!(status(&missing), Status::KeyNotFound as u16);
    }

    #[test]
    fn quiet_commands_suppress_only_successes_and_get_misses() {
        let handler = MemoryHandler::new(MemoryStore::new());
        let mut extras = Vec::new();
        extras.extend_from_slice(&0_u32.to_be_bytes());
        extras.extend_from_slice(&0_u32.to_be_bytes());

        let (_, set) = handle(&handler, &request(OP_SETQ, b"key", &extras, b"value", 1));
        assert!(set.is_empty());
        let (_, get_hit) = handle(&handler, &request(OP_GETQ, b"key", &[], &[], 2));
        assert_eq!(status(&get_hit), Status::Success as u16);
        let (_, get_miss) = handle(&handler, &request(OP_GETQ, b"missing", &[], &[], 3));
        assert!(get_miss.is_empty());
        let (_, delete_hit) = handle(&handler, &request(OP_DELETEQ, b"key", &[], &[], 4));
        assert!(delete_hit.is_empty());
        let (_, delete_miss) = handle(&handler, &request(OP_DELETEQ, b"missing", &[], &[], 5));
        assert_eq!(status(&delete_miss), Status::KeyNotFound as u16);
    }

    #[test]
    fn control_commands_and_invalid_requests_are_encoded() {
        let handler = MemoryHandler::new(MemoryStore::new());
        let (_, noop) = handle(&handler, &request(OP_NOOP, &[], &[], &[], 1));
        assert_eq!(status(&noop), Status::Success as u16);

        let (_, version) = handle(&handler, &request(OP_VERSION, &[], &[], &[], 2));
        assert_eq!(&version[24..], VERSION);

        let (_, stats) = handle(&handler, &request(OP_STAT, &[], &[], &[], 3));
        assert_eq!(response_count(&stats), 3);

        let (quit, response) = handle(&handler, &request(OP_QUIT, &[], &[], &[], 4));
        assert_eq!(quit, ConnectionAction::Close);
        assert_eq!(status(&response), Status::Success as u16);

        let (_, unknown) = handle(&handler, &request(0xfe, &[], &[], &[], 5));
        assert_eq!(status(&unknown), Status::UnknownCommand as u16);

        let mut invalid_extras = Vec::new();
        invalid_extras.extend_from_slice(&0_u32.to_be_bytes());
        invalid_extras.extend_from_slice(&1_u32.to_be_bytes());
        let (_, invalid) = handle(
            &handler,
            &request(OP_SET, b"key", &invalid_extras, b"value", 6),
        );
        assert_eq!(status(&invalid), Status::InvalidArguments as u16);
        assert!(!invalid[24..].is_empty());
    }

    #[test]
    fn fixed_response_mode_has_no_key_value_state() {
        let handler = HandlerConfig::FixedResponse { value_len: 64 }.build();
        let mut extras = Vec::new();
        extras.extend_from_slice(&9_u32.to_be_bytes());
        extras.extend_from_slice(&0_u32.to_be_bytes());

        let (_, set) = handle(&handler, &request(OP_SET, b"key", &extras, b"ignored", 1));
        assert_eq!(status(&set), Status::Success as u16);

        let (_, get) = handle(&handler, &request(OP_GET, b"another-key", &[], &[], 2));
        assert_eq!(status(&get), Status::Success as u16);
        assert_eq!(get.len(), crate::protocol::HEADER_LEN + 4 + 64);
        assert_eq!(&get[24..28], &0_u32.to_be_bytes());
        assert!(get[28..].iter().all(|&byte| byte == 0x5a));

        let (_, delete) = handle(&handler, &request(OP_DELETE, b"missing", &[], &[], 3));
        assert_eq!(status(&delete), Status::Success as u16);
    }
}
