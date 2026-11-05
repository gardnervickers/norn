//! Ring-wide kernel capabilities and tuning.
//!
//! These [`Handle`] methods inspect or configure the driver's `io_uring`
//! instance as a whole:
//!
//! - [`Handle::capabilities`] probes supported opcodes and reports the ring's
//!   setup parameters and kernel feature flags.
//! - [`Handle::io_wq_max_workers`] and [`Handle::set_io_wq_max_workers`] query
//!   and bound the kernel's async worker pool.
//! - [`Handle::register_napi`], [`Handle::unregister_napi`],
//!   [`Handle::add_napi_id`], and [`Handle::remove_napi_id`] control NAPI
//!   busy polling while the driver waits for completions.
//!
//! None of these calls gives the kernel a reference to caller memory or
//! creates a submission. Each is one synchronous `io_uring_register(2)` call
//! which the kernel applies or rejects as a unit. They therefore neither extend
//! nor depend on the driver's operation, registration, or shutdown lifecycles,
//! and may be called in any driver state while a [`Handle`] keeps the ring
//! alive. NAPI settings belong to the ring and end with it. Worker-pool limits
//! do not: they apply to the pool of each thread that uses the ring, and that
//! pool can outlive the ring (see [`Handle::set_io_wq_max_workers`]).
//!
//! Every method returns [`io::ErrorKind::WouldBlock`] if the ring is
//! momentarily borrowed by the driver, and the kernel's error otherwise.
//! Kernels which predate a registration opcode generally report
//! [`io::ErrorKind::InvalidInput`] (`EINVAL`).
//!
//! # Registered ring descriptors
//!
//! `IORING_REGISTER_RING_FDS` is deliberately not exposed. `io-uring` 0.7.15
//! stores the registered index in the [`io_uring::Submitter`] value that
//! performed the registration, while [`io_uring::IoUring::submitter`] returns a
//! fresh, unregistered submitter for every call. The driver creates a
//! submitter per syscall, so registering would leak one of the thread's 16
//! registered-ring slots, each pinning a reference to the ring file, without
//! any later enter using the registered index.

use std::fmt;
use std::io;
use std::num::NonZeroU32;
use std::time::Duration;

use io_uring::types::{Napi, NapiTracking as RawNapiTracking};
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

/// Worker-count limits for the kernel's async I/O worker pool (io-wq).
///
/// When setting limits, `None` leaves that limit unchanged. When reporting
/// limits, `None` means the kernel reported zero, for example because no
/// worker pool exists yet for the affected task. A reported `None` therefore
/// cannot be passed back to restore a limit.
///
/// Since Linux 6.5 the limits apply to the whole pool; earlier kernels apply
/// them to each NUMA node separately. Values above `i32::MAX` are rejected by
/// the kernel.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoWqMaxWorkers {
    /// Limit for bounded workers, which run operations expected to finish in
    /// bounded time, such as regular file and block-device I/O.
    pub bounded: Option<NonZeroU32>,
    /// Limit for unbounded workers, which run operations that may wait
    /// indefinitely, such as socket I/O.
    pub unbounded: Option<NonZeroU32>,
}

impl IoWqMaxWorkers {
    fn to_raw(self) -> [u32; 2] {
        [
            self.bounded.map_or(0, NonZeroU32::get),
            self.unbounded.map_or(0, NonZeroU32::get),
        ]
    }

    fn from_raw(raw: [u32; 2]) -> Self {
        Self {
            bounded: NonZeroU32::new(raw[0]),
            unbounded: NonZeroU32::new(raw[1]),
        }
    }
}

/// How the kernel selects the NAPI instances it busy polls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NapiTracking {
    /// Track NAPI instances of sockets used by operations on the ring.
    Dynamic,
    /// Poll only ids added with [`Handle::add_napi_id`].
    ///
    /// Requires Linux 6.13. Socket NAPI ids are available from the
    /// `SO_INCOMING_NAPI_ID` socket option.
    Static,
}

/// NAPI busy-poll configuration for [`Handle::register_napi`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NapiConfig {
    /// How long a completion wait busy polls before sleeping.
    ///
    /// The kernel uses microsecond granularity; sub-microsecond remainders
    /// are truncated, and timeouts of `u32::MAX` microseconds or more are
    /// rejected.
    pub busy_poll_timeout: Duration,
    /// Whether to prefer busy polling over interrupt-driven processing.
    pub prefer_busy_poll: bool,
    /// How the polled NAPI instances are selected.
    pub tracking: NapiTracking,
}

impl NapiConfig {
    /// Create a dynamically tracked configuration with the given timeout.
    pub fn new(busy_poll_timeout: Duration) -> Self {
        Self {
            busy_poll_timeout,
            prefer_busy_poll: false,
            tracking: NapiTracking::Dynamic,
        }
    }

    fn to_raw(self) -> io::Result<Napi> {
        let micros = u32::try_from(self.busy_poll_timeout.as_micros())
            .ok()
            .filter(|&micros| micros != u32::MAX)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "NAPI busy-poll timeout must be below u32::MAX microseconds",
                )
            })?;
        let tracking = match self.tracking {
            NapiTracking::Dynamic => RawNapiTracking::Dynamic,
            NapiTracking::Static => RawNapiTracking::Static,
        };
        Ok(Napi::new()
            .set_busy_poll_timeout(micros)
            .set_prefer_busy_poll(self.prefer_busy_poll)
            .set_tracking(tracking))
    }
}

/// NAPI busy-poll settings the kernel reported as previously active.
///
/// `io-uring` 0.7.15 does not expose the reported tracking mode, so it is
/// omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NapiSettings {
    /// Previous busy-poll timeout. Zero when NAPI was not registered.
    pub busy_poll_timeout: Duration,
    /// Previous busy-poll preference.
    pub prefer_busy_poll: bool,
}

impl NapiSettings {
    fn from_raw(napi: &Napi) -> Self {
        Self {
            busy_poll_timeout: Duration::from_micros(napi.busy_poll_timeout().into()),
            prefer_busy_poll: napi.prefer_busy_poll(),
        }
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

    /// Report the current async worker-pool limits without changing them.
    ///
    /// Requires Linux 5.15.
    ///
    /// # Errors
    ///
    /// Returns the registration syscall's error, or
    /// [`io::ErrorKind::WouldBlock`] if the ring is momentarily borrowed.
    pub fn io_wq_max_workers(&self) -> io::Result<IoWqMaxWorkers> {
        self.set_io_wq_max_workers(IoWqMaxWorkers::default())
    }

    /// Set async worker-pool limits and return the previous limits.
    ///
    /// The kernel records the limits on the ring and applies them to the
    /// worker pool of every task using it. Without SQPOLL that pool belongs to
    /// the submitting thread and is shared with every other ring that thread
    /// uses; with SQPOLL it belongs to the polling thread. The previous limits
    /// are those of the calling thread's pool, or of the SQPOLL thread's pool.
    /// Requires Linux 5.15.
    ///
    /// Without SQPOLL the limits outlive the ring: the thread's pool keeps
    /// them until the thread exits or they are set again, so a later ring on
    /// the same thread starts with them. To confine limits to one driver, set
    /// the returned previous limits again before dropping it. A previous limit
    /// reported as `None` cannot be restored this way.
    ///
    /// # Errors
    ///
    /// Returns the registration syscall's error, including `EINVAL` for a
    /// limit above `i32::MAX`, or [`io::ErrorKind::WouldBlock`] if the ring is
    /// momentarily borrowed.
    pub fn set_io_wq_max_workers(&self, limits: IoWqMaxWorkers) -> io::Result<IoWqMaxWorkers> {
        let mut raw = limits.to_raw();
        self.try_with_submitter(|submitter| submitter.register_iowq_max_workers(&mut raw))?;
        Ok(IoWqMaxWorkers::from_raw(raw))
    }

    /// Enable NAPI busy polling for completion waits on this ring.
    ///
    /// Registration replaces any earlier configuration and clears the set of
    /// tracked NAPI ids. While enabled, a driver park that waits for
    /// completions may spin for up to the configured timeout before sleeping.
    /// Remote unparks still wake it. With SQPOLL, the polling thread busy
    /// polls instead. Requires Linux 6.9.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an unrepresentable timeout,
    /// the registration syscall's error, or [`io::ErrorKind::WouldBlock`] if
    /// the ring is momentarily borrowed.
    pub fn register_napi(&self, config: NapiConfig) -> io::Result<NapiSettings> {
        let mut napi = config.to_raw()?;
        self.try_with_submitter(|submitter| submitter.register_napi(&mut napi))?;
        Ok(NapiSettings::from_raw(&napi))
    }

    /// Disable NAPI busy polling and return the settings that were active.
    ///
    /// Disabling an inactive ring succeeds. Requires Linux 6.9.
    ///
    /// # Errors
    ///
    /// Returns the registration syscall's error, or
    /// [`io::ErrorKind::WouldBlock`] if the ring is momentarily borrowed.
    pub fn unregister_napi(&self) -> io::Result<NapiSettings> {
        let mut napi = Napi::new();
        self.try_with_submitter(|submitter| submitter.unregister_napi(&mut napi))?;
        Ok(NapiSettings::from_raw(&napi))
    }

    /// Add a NAPI id to a ring registered with [`NapiTracking::Static`].
    ///
    /// Requires Linux 6.13.
    ///
    /// # Errors
    ///
    /// Returns `EINVAL` when the ring does not use static tracking or the id
    /// is invalid, other registration syscall errors, or
    /// [`io::ErrorKind::WouldBlock`] if the ring is momentarily borrowed.
    pub fn add_napi_id(&self, napi_id: u32) -> io::Result<()> {
        self.try_with_submitter(|submitter| submitter.register_napi_add_id(napi_id))
    }

    /// Remove a NAPI id added with [`Handle::add_napi_id`].
    ///
    /// Requires Linux 6.13.
    ///
    /// # Errors
    ///
    /// Returns `EINVAL` when the ring does not use static tracking, `ENOENT`
    /// when the id is not tracked, other registration syscall errors, or
    /// [`io::ErrorKind::WouldBlock`] if the ring is momentarily borrowed.
    pub fn remove_napi_id(&self, napi_id: u32) -> io::Result<()> {
        self.try_with_submitter(|submitter| submitter.register_napi_del_id(napi_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_report_probed_opcodes_and_params() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let capabilities = driver.handle().capabilities()?;
        assert!(capabilities.supports_opcode(io_uring::opcode::Nop::CODE));
        assert!(capabilities.supports_opcode(io_uring::opcode::Read::CODE));
        assert!(!capabilities.supports_opcode(u8::MAX));
        assert!(capabilities
            .supported_opcodes()
            .any(|opcode| opcode == io_uring::opcode::Nop::CODE));
        assert!(!capabilities.params().is_setup_sqpoll());
        Ok(())
    }

    #[test]
    fn ring_controls_fail_instead_of_panicking_while_ring_is_borrowed() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        handle.test_with_ring_borrowed_mut(|| {
            let kinds = [
                handle.capabilities().map(drop),
                handle.io_wq_max_workers().map(drop),
                handle.unregister_napi().map(drop),
                handle.add_napi_id(1),
            ]
            .map(|result| result.unwrap_err().kind());
            assert_eq!(kinds, [io::ErrorKind::WouldBlock; 4]);
        });
        Ok(())
    }

    #[test]
    fn napi_timeout_must_fit_in_kernel_microseconds() {
        let config = NapiConfig::new(Duration::from_micros(u32::MAX.into()));
        assert_eq!(
            config.to_raw().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        let napi = NapiConfig::new(Duration::from_nanos(2_999))
            .to_raw()
            .unwrap();
        assert_eq!(napi.busy_poll_timeout(), 2);
    }

    #[test]
    fn worker_limits_round_trip_through_kernel_encoding() {
        let limits = IoWqMaxWorkers {
            bounded: NonZeroU32::new(3),
            unbounded: None,
        };
        assert_eq!(limits.to_raw(), [3, 0]);
        assert_eq!(IoWqMaxWorkers::from_raw(limits.to_raw()), limits);
    }
}
