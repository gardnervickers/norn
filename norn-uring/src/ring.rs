//! Kernel opcode support.
//!
//! [`Handle::capabilities`] reports which `io_uring` opcodes the running kernel
//! implements. The driver probes once, with `IORING_REGISTER_PROBE`, when it
//! is created, so the call costs nothing and works in any driver state while a
//! [`Handle`] lives, including during and after shutdown.

use std::fmt;
use std::io;

use io_uring::{IoUring, Probe};

use crate::Handle;

/// The `io_uring` opcodes the running kernel implements.
///
/// Returned by [`Handle::capabilities`]. Support is a property of the kernel,
/// not of the ring: the kernel reports every opcode it implements, including
/// ones that need a ring setup the driver never creates. For example,
/// zero-copy receive (`IORING_OP_RECV_ZC`) needs 32-byte completions, so it
/// cannot work on a Norn ring even where it is reported. A supported opcode
/// also does not prove that every flag or argument of it is accepted.
#[derive(Clone, PartialEq, Eq)]
pub struct Capabilities {
    opcodes: [u64; 4],
}

impl Capabilities {
    /// Probe the kernel through `ring`.
    pub(crate) fn probe(ring: &IoUring) -> io::Result<Self> {
        let mut probe = Probe::new();
        ring.submitter().register_probe(&mut probe)?;
        let mut opcodes = [0u64; 4];
        for opcode in 0..=u8::MAX {
            if probe.is_supported(opcode) {
                opcodes[usize::from(opcode / 64)] |= 1 << (opcode % 64);
            }
        }
        Ok(Self { opcodes })
    }

    /// Return whether the kernel implements `opcode`.
    ///
    /// Pass an opcode code such as `io_uring::opcode::Read::CODE`.
    pub fn supports_opcode(&self, opcode: u8) -> bool {
        self.opcodes[usize::from(opcode / 64)] & (1 << (opcode % 64)) != 0
    }

    /// Iterate over every supported opcode code in ascending order.
    pub fn supported_opcodes(&self) -> impl Iterator<Item = u8> + '_ {
        (0..=u8::MAX).filter(|&opcode| self.supports_opcode(opcode))
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Capabilities")
            .field("opcodes", &self.supported_opcodes().collect::<Vec<_>>())
            .finish()
    }
}

impl Handle {
    /// Report which `io_uring` opcodes the running kernel implements.
    ///
    /// The driver probed the kernel when it was created; see [`Capabilities`]
    /// for what the result does and does not promise.
    pub fn capabilities(&self) -> &Capabilities {
        self.kernel_capabilities()
    }
}
