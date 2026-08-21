//! Device-specific commands submitted through `IORING_OP_URING_CMD`.

use std::io;

use io_uring::opcode;

use crate::fd::{NornFd, UringFd};
use crate::operation::{CQEResult, Operation, Singleshot};

/// The inline payload for a 16-byte `io_uring` device command.
///
/// The meaning of the command operation and payload is defined by the target
/// device's UAPI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Command16 {
    command_op: u32,
    data: [u8; 16],
    address: Option<u64>,
    fixed_buffer: Option<u16>,
}

impl Command16 {
    /// Create a command encoding from a device-specific operation and payload.
    pub const fn new(command_op: u32, data: [u8; 16]) -> Self {
        Self {
            command_op,
            data,
            address: None,
            fixed_buffer: None,
        }
    }

    /// Set the SQE address field used by the command.
    pub const fn with_address(mut self, address: u64) -> Self {
        self.address = Some(address);
        self
    }

    /// Select a registered fixed buffer by index.
    ///
    /// The command implementation must retain the corresponding registration
    /// through the terminal completion.
    pub const fn with_fixed_buffer(mut self, index: u16) -> Self {
        self.fixed_buffer = Some(index);
        self
    }

    /// Return the device-specific command operation.
    pub const fn command_op(&self) -> u32 {
        self.command_op
    }

    /// Return the inline command payload.
    pub const fn data(&self) -> &[u8; 16] {
        &self.data
    }

    /// Return the configured SQE address field.
    pub const fn address(&self) -> Option<u64> {
        self.address
    }

    /// Return the selected fixed-buffer index.
    pub const fn fixed_buffer(&self) -> Option<u16> {
        self.fixed_buffer
    }
}

/// A single-completion device command with a 16-byte inline payload.
///
/// The command value is stored at a stable address before [`UringCommand16::encode`]
/// is called and remains there through the terminal completion. This permits an
/// implementation to own buffers or other resources referenced by its encoded
/// payload.
///
/// # Safety
///
/// Implementing this trait asserts that:
///
/// - every pointer, registered-resource index, and other resource encoded by
///   [`UringCommand16::encode`] remains valid for every access permitted by the
///   device UAPI through the terminal CQE;
/// - kernel access to encoded memory obeys Rust's aliasing rules;
/// - the command produces exactly one terminal completion and does not enable a
///   multishot mode;
/// - [`UringCommand16::reap`] returns an owned value that safely releases every
///   resource represented by the CQE when dropped, including for synthetic
///   configuration or submission failures; and
/// - [`UringCommand16::reap`] does not unwind.
pub unsafe trait UringCommand16: Sized + 'static {
    /// The owned interpretation of the command's terminal CQE.
    type Completion: 'static;

    /// The result produced from the command's terminal completion.
    type Output;

    /// Encode the device command after `self` has been placed at a stable address.
    ///
    /// # Errors
    ///
    /// Returns an error if a valid command payload cannot be constructed.
    fn encode(&mut self) -> io::Result<Command16>;

    /// Convert the raw terminal CQE into an owned completion. A panic from this method aborts the
    /// process because the dequeued CQE cannot be replayed.
    ///
    /// # Safety
    ///
    /// `result` must be the unique terminal completion produced for this exact command and must
    /// not have been passed to another reaper.
    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion;

    /// Convert the owned completion into this command's output.
    fn complete(self, completion: Self::Completion) -> Self::Output;
}

struct CommandOp<C> {
    fd: NornFd,
    command: C,
}

impl<C> CommandOp<C> {
    fn new(fd: NornFd, command: C) -> Self {
        Self { fd, command }
    }
}

// Safety: `NornFd` retains the target descriptor and `UringCommand16` supplies
// the lifetime, single-completion, and reap contract for the encoded payload.
unsafe impl<C> Operation for CommandOp<C>
where
    C: UringCommand16,
{
    type Completion = C::Completion;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let command = self.command.encode()?;
        let mut builder =
            opcode::UringCmd16::new(self.fd.fd(), command.command_op).cmd(command.data);
        if let Some(address) = command.address {
            builder = builder.addr(Some(address));
        }
        if let Some(index) = command.fixed_buffer {
            builder = builder.buf_index(Some(index));
        }
        Ok(builder.build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        // Safety: CommandOp receives the completion for this exact encoded command once.
        unsafe { self.command.reap(result) }
    }
}

impl<C> Singleshot for CommandOp<C>
where
    C: UringCommand16,
{
    type Output = C::Output;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        self.command.complete(completion)
    }
}

impl UringFd {
    /// Submit a device-specific command on this descriptor.
    ///
    /// The returned request is lazy and retains the descriptor and command
    /// value until the operation reaches its terminal completion.
    pub fn submit_command<C>(&self, command: C) -> impl crate::Request<Output = C::Output>
    where
        C: UringCommand16,
    {
        self.submit(CommandOp::new(self.lease(), command))
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::RawFd;

    use super::*;

    struct TestCommand;

    unsafe impl UringCommand16 for TestCommand {
        type Completion = CQEResult;
        type Output = io::Result<u32>;

        fn encode(&mut self) -> io::Result<Command16> {
            Ok(Command16::new(7, [0x5a; 16])
                .with_address(0x0102_0304_0506_0708)
                .with_fixed_buffer(11))
        }

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }

        fn complete(self, completion: Self::Completion) -> Self::Output {
            completion.into_result()
        }
    }

    #[test]
    fn configures_uring_command_opcode() {
        let mut fds: [RawFd; 2] = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let mut operation = CommandOp::new(NornFd::from_fd(fds[0]), TestCommand);

        let entry = operation.configure().unwrap();

        assert_eq!(entry.get_opcode(), opcode::UringCmd16::CODE.into());
        let bytes: [u8; 64] = unsafe { std::mem::transmute_copy(&entry) };
        assert_eq!(u32::from_ne_bytes(bytes[8..12].try_into().unwrap()), 7);
        assert_eq!(
            u64::from_ne_bytes(bytes[16..24].try_into().unwrap()),
            0x0102_0304_0506_0708
        );
        assert_eq!(u32::from_ne_bytes(bytes[28..32].try_into().unwrap()), 1);
        assert_eq!(u16::from_ne_bytes(bytes[40..42].try_into().unwrap()), 11);
        assert_eq!(&bytes[48..64], &[0x5a; 16]);
        drop(operation);
        assert_eq!(unsafe { libc::close(fds[1]) }, 0);
    }
}
