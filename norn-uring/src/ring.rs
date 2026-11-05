//! Ring-wide kernel capabilities.
//!
//! [`Handle::capabilities`] reports which opcodes the kernel supports, along
//! with the ring's setup parameters and kernel feature flags. It is one
//! synchronous `IORING_REGISTER_PROBE` call, which gives the kernel no
//! reference to caller memory and creates no submission. It may therefore be
//! called in any driver state while a [`Handle`] keeps the ring alive. It
//! returns [`io::ErrorKind::WouldBlock`] if the ring is momentarily borrowed
//! by the driver, and the kernel's error otherwise.

use std::fmt;
use std::io;

use io_uring::{Parameters, Probe};

use crate::Handle;

/// Opcode support and setup parameters reported by one ring.
///
/// Returned by [`Handle::capabilities`]. The snapshot does not change while
/// the ring is alive.
#[derive(Clone)]
pub struct Capabilities {
    opcodes: [u64; 4],
    params: Parameters,
}

impl Capabilities {
    fn new(probe: &Probe, params: Parameters) -> Self {
        let mut opcodes = [0u64; 4];
        for opcode in 0..=u8::MAX {
            if probe.is_supported(opcode) {
                opcodes[usize::from(opcode / 64)] |= 1 << (opcode % 64);
            }
        }
        Self { opcodes, params }
    }

    /// Return whether the kernel reports `opcode` as supported.
    ///
    /// Pass an opcode code such as `io_uring::opcode::Read::CODE`. Support
    /// reflects what the kernel implements; it does not prove that every flag
    /// or argument of that opcode is accepted.
    pub fn supports_opcode(&self, opcode: u8) -> bool {
        self.opcodes[usize::from(opcode / 64)] & (1 << (opcode % 64)) != 0
    }

    /// Iterate over every supported opcode code in ascending order.
    pub fn supported_opcodes(&self) -> impl Iterator<Item = u8> + '_ {
        (0..=u8::MAX).filter(|&opcode| self.supports_opcode(opcode))
    }

    /// Return the parameters the kernel reported when the ring was created.
    ///
    /// These include setup flags such as SQPOLL and kernel feature flags such
    /// as `IORING_FEAT_EXT_ARG`.
    pub fn params(&self) -> &Parameters {
        &self.params
    }
}

impl fmt::Debug for Capabilities {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Capabilities")
            .field("opcodes", &self.supported_opcodes().collect::<Vec<_>>())
            .field("params", &self.params)
            .finish()
    }
}

impl Handle {
    /// Probe opcode support and report the ring's setup parameters.
    ///
    /// Each call performs one `IORING_REGISTER_PROBE` syscall; cache the
    /// result when checking support on a hot path. Requires Linux 5.6.
    ///
    /// # Errors
    ///
    /// Returns the probe syscall's error, or
    /// [`io::ErrorKind::WouldBlock`] if the ring is momentarily borrowed.
    pub fn capabilities(&self) -> io::Result<Capabilities> {
        self.try_with_ring(|ring| {
            let mut probe = Probe::new();
            ring.submitter().register_probe(&mut probe)?;
            Ok(Capabilities::new(&probe, ring.params().clone()))
        })
    }
}
