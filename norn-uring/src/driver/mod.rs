use std::any::Any;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;
use std::{io, mem};

use io_uring::squeue::{Flags, PushError};
use io_uring::types::{self, CancelBuilder, SubmitArgs, Timespec};
use io_uring::{cqueue, opcode, IoUring, Submitter};
use log::{debug, error, trace, warn};
use norn_executor::park::{Park, ParkMode};
use smallvec::SmallVec;

use crate::error::SubmitError;
use crate::fd;
use crate::operation::{complete_operation, ConfiguredEntry, Op, Operation};
use crate::registered_buffers::Registry as RegisteredBuffers;
pub(crate) use crate::registered_buffers::{
    Generation as FixedBufGeneration, Release as FixedBufRelease,
    ReleaseError as FixedBufReleaseError, ReserveError as ReserveFixedBufError,
    Retention as FixedBufRetention,
};
use crate::util::notify::Notify;
pub(crate) use futures::PushFuture;

#[cfg(test)]
use crate::registered_buffers::State as FixedBufState;

mod context;
mod futures;
mod unpark;

const LOG: &str = "norn_uring::driver";

/// True if the needs_park check should check the submission and completion queues.
///
/// This will have a perf impact on each poll, but may ensure better overall performance.
const NEEDS_PARK_CHECK_RINGS: bool = true;

/// Number of CQEs copied out of the ring at once while draining.
const COMPLETION_DRAIN_BATCH: usize = 32;

/// Maximum time each shutdown cancellation attempt waits for the kernel.
///
/// `IORING_REGISTER_SYNC_CANCEL` accepts a timeout, so teardown can retry without
/// entering one unbounded syscall when a request cannot be cancelled.
const SHUTDOWN_CANCEL_TIMEOUT: Duration = Duration::from_millis(100);

/// Options controlling normal [`Driver`] operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DriverOptions {
    /// Maximum number of completion queue entries handled by one normal park.
    ///
    /// The default, `None`, drains all ready completions and preserves the
    /// driver's existing behavior. Setting a nonzero limit can improve
    /// cooperative scheduling fairness when completions arrive faster than
    /// application tasks consume them.
    ///
    /// Shutdown always drains completions exhaustively, regardless of this
    /// option.
    pub completion_drain_budget: Option<NonZeroUsize>,
}

/// [`Driver`] provides a [`Park`] implementation which will drive
/// a [`IoUring`] instance, submitting new requests and waiting
/// for completions.
///
/// Interaction with the driver is done via [`Handle`]. The handle
/// can be used to submit new requests to the driver.
///
/// Shutdown stops accepting new requests, cancels outstanding work, and waits
/// for every kernel-owned operation to reach a terminal completion. Teardown
/// retries transient and fatal submission errors indefinitely; consequently,
/// dropping a driver can block forever if its ring can no longer make progress.
pub struct Driver {
    options: DriverOptions,
    shared: Rc<Shared>,
    unparker: Arc<unpark::Unparker>,
    /// Storage passed to the kernel for the eventfd read.
    ///
    /// [`Driver::drop`] releases the buffer only after the drain sentinel proves
    /// every earlier request, including the eventfd read, is terminal.
    unparker_buf: mem::ManuallyDrop<UnparkerBuffer>,
}

struct UnparkerBuffer {
    storage: Box<UnsafeCell<[u8; 8]>>,
    #[cfg(test)]
    drop_probe: Option<DropProbe>,
}

#[cfg(test)]
struct DropProbe(Arc<std::sync::atomic::AtomicUsize>);

#[cfg(test)]
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Release);
    }
}

/// [`Handle`] is used to interact with the [`Driver`] and
/// the backing [`IoUring`] instance.
#[derive(Clone)]
pub struct Handle {
    shared: Rc<Shared>,
}

struct Shared {
    ring: RefCell<IoUring>,
    backpressure: Notify,
    status: Cell<Status>,
    submit_error: RefCell<Option<(io::ErrorKind, String)>>,
    shutdown_outcome: Cell<Option<ShutdownOutcome>>,
    #[cfg(test)]
    cancel_all_error: Cell<Option<i32>>,
    #[cfg(test)]
    submit_failures: RefCell<std::collections::VecDeque<i32>>,
    #[cfg(test)]
    submit_limits: RefCell<std::collections::VecDeque<(usize, usize)>>,
    // This field must be declared after `ring`: fields are dropped in declaration
    // order, so retained registered storage is released only after the ring.
    registered_buffers: RegisteredBuffers,
}

pub(crate) struct FixedBufReservation {
    shared: Rc<Shared>,
    generation: FixedBufGeneration,
    committed: bool,
}

#[derive(Clone)]
pub(crate) struct FixedBufDriver {
    shared: Weak<Shared>,
}

/// The status of the driver.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(super) enum Status {
    /// The driver is running and accepting new requests.
    Running,
    /// The driver is closing and will not accept new requests.
    Closing,
    /// The driver is waiting for all admitted operations to become terminal.
    DrainingOperations,
    /// Operation completions have been reaped and cleanup-generated work is being flushed.
    ClosingResources,
    /// The driver is waiting for cleanup-generated work to become terminal.
    DrainingResources,
    /// The driver has shutdown and will not accept new requests.
    Shutdown,
}

/// Describes whether shutdown reached the normal reclamation boundary.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ShutdownOutcome {
    /// The drain sentinel completed, so every earlier request reached a terminal CQE.
    CleanDrained,
}

pub(crate) enum TryPush {
    Submitted,
    Full(ConfiguredEntry),
    Failed(SubmitError),
}

#[derive(Debug)]
pub(crate) enum CloseFdError {
    NeverQueued(io::Error),
    Queued(io::Error),
}

impl std::fmt::Display for CloseFdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeverQueued(err) => write!(f, "close SQE was never queued: {err}"),
            Self::Queued(err) => write!(f, "close SQE was queued before submit failed: {err}"),
        }
    }
}

impl std::error::Error for CloseFdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NeverQueued(err) | Self::Queued(err) => Some(err),
        }
    }
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle").finish()
    }
}

impl std::fmt::Debug for Driver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver")
            .field("options", &self.options)
            .finish()
    }
}

impl UnparkerBuffer {
    fn new() -> Self {
        Self {
            storage: Box::new(UnsafeCell::new([0; 8])),
            #[cfg(test)]
            drop_probe: None,
        }
    }

    fn get(&self) -> *mut [u8; 8] {
        self.storage.get()
    }

    #[cfg(test)]
    fn track_drop(&mut self, counter: Arc<std::sync::atomic::AtomicUsize>) {
        self.drop_probe = Some(DropProbe(counter));
    }
}

impl FixedBufReservation {
    pub(crate) fn generation(&self) -> FixedBufGeneration {
        self.generation
    }

    pub(crate) fn with_submitter<U>(&self, f: impl FnOnce(&Submitter<'_>) -> U) -> U {
        self.shared.with_submitter(f)
    }

    pub(crate) fn arm_kernel_call(&self) {
        self.shared
            .registered_buffers
            .arm_kernel_call(self.generation);
    }

    pub(crate) fn kernel_call_failed(&self) {
        self.shared
            .registered_buffers
            .kernel_call_failed(self.generation);
    }

    pub(crate) fn commit(mut self) -> FixedBufDriver {
        let driver = FixedBufDriver {
            shared: Rc::downgrade(&self.shared),
        };
        self.shared.registered_buffers.commit(self.generation);
        self.committed = true;
        driver
    }
}

impl Drop for FixedBufReservation {
    fn drop(&mut self) {
        if !self.committed {
            self.shared.registered_buffers.rollback(self.generation);
        }
    }
}

impl FixedBufDriver {
    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.shared.as_ptr() == Rc::as_ptr(&handle.shared)
    }

    pub(crate) fn unregister(
        &self,
        generation: FixedBufGeneration,
    ) -> Result<FixedBufRelease, FixedBufReleaseError> {
        let Some(shared) = self.shared.upgrade() else {
            return Ok(FixedBufRelease::RingGone);
        };
        shared
            .registered_buffers
            .unregister(&shared.ring, generation)
    }

    pub(crate) fn retain(
        &self,
        generation: FixedBufGeneration,
        storage: Box<dyn Any>,
    ) -> FixedBufRetention {
        let Some(shared) = self.shared.upgrade() else {
            drop(storage);
            return FixedBufRetention::RingGone;
        };
        shared.registered_buffers.retain(generation, storage)
    }
}

impl Handle {
    /// Returns a handle to the current driver if one is set in TLS context.
    pub(crate) fn try_current() -> Option<Self> {
        context::DriverContext::handle()
    }

    /// Returns a handle to the current driver.
    ///
    /// If the current thread is not in a driver context, this will panic.
    #[track_caller]
    pub fn current() -> Self {
        Self::try_current().expect("not in driver context")
    }

    /// Prepare an operation for submission.
    ///
    /// The returned [`Op`] is lazy: it does not touch the submission queue until
    /// it is first polled.
    pub fn submit<T>(&self, op: T) -> Op<T>
    where
        T: Operation + 'static,
    {
        Op::new(op, self.clone())
    }

    /// Issue a cancellation request.
    ///
    /// Setting `sync` to true will cause the cancellation to
    /// be performed synchronously. If `sync` is false, async
    /// cancellation will be attempted first followed by sync
    /// cancellation if the async cancellation fails.
    pub(crate) fn cancel(&self, criteria: CancelBuilder, sync: bool) -> io::Result<()> {
        self.shared.cancel(criteria, sync)
    }

    /// Attempt to push a new entry into the submission queue.
    ///
    /// If the submission queue is full, this will block until there
    /// is space or the driver has shutdown.
    pub(crate) fn push(&self, entry: ConfiguredEntry) -> PushFuture {
        PushFuture::new(Rc::clone(&self.shared), entry)
    }

    pub(crate) fn try_push(&self, entry: ConfiguredEntry) -> TryPush {
        if self.shared.status() != Status::Running {
            let err = if let Some(err) = self.shared.health_error() {
                SubmitError::broken(err)
            } else {
                SubmitError::shutting_down()
            };
            return TryPush::Failed(err);
        }
        match self.shared.try_push(entry) {
            Ok(()) => TryPush::Submitted,
            Err(entry) => TryPush::Full(entry),
        }
    }

    /// Attempt to push a new batch of entries into the submission queue.
    ///
    /// If the submission queue is full, this will block until there
    /// is space or the driver has shutdown.
    pub(crate) fn push_batch(&self, entries: SmallVec<[ConfiguredEntry; 4]>) -> PushFuture {
        PushFuture::new_batch(Rc::clone(&self.shared), entries)
    }

    pub(crate) fn close_fd(&self, kind: &fd::FdKind) -> Result<(), CloseFdError> {
        self.shared.close_fd(kind)
    }

    pub(crate) fn with_submitter<U>(&self, f: impl FnOnce(&Submitter<'_>) -> U) -> U {
        self.shared.with_submitter(f)
    }

    pub(crate) fn reserve_fixed_buffers(
        &self,
    ) -> Result<FixedBufReservation, ReserveFixedBufError> {
        if self.shared.status() != Status::Running {
            return Err(ReserveFixedBufError::DriverStopped);
        }
        let generation = self.shared.registered_buffers.reserve(&self.shared.ring)?;

        Ok(FixedBufReservation {
            shared: Rc::clone(&self.shared),
            generation,
            committed: false,
        })
    }

    pub(crate) fn fixed_buf_driver(&self) -> FixedBufDriver {
        FixedBufDriver {
            shared: Rc::downgrade(&self.shared),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_forget_fixed_buffers(&self) {
        self.shared.registered_buffers.test_forget();
    }

    #[cfg(test)]
    pub(crate) fn test_retained_fixed_buffers(&self) -> usize {
        self.shared.registered_buffers.test_retained_len()
    }

    #[cfg(test)]
    pub(crate) fn test_with_ring_borrowed_mut(&self, f: impl FnOnce()) {
        let _ring = self.shared.ring.borrow_mut();
        f();
    }

    /// Returns the first recorded fatal driver submit error, if any.
    pub fn health_error(&self) -> Option<io::Error> {
        self.shared.health_error()
    }

    pub(crate) fn same_driver(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.shared, &other.shared)
    }
}

impl Driver {
    /// Signals that all operations admitted before shutdown are terminal.
    const OPERATIONS_DRAIN_TOKEN: usize = 0x01;

    /// [Driver::UNPARKER_WAKE_TOKEN] is a special token which is used to signal unparker wake events.
    const UNPARKER_WAKE_TOKEN: usize = 0x02;

    /// [Driver::CANCELLATION_TOKEN] is a special token which is used to signal cancellation events.
    const CANCELLATION_TOKEN: usize = 0x03;

    /// [Driver::CLOSE_FD_TOKEN] is a special token which is used to signal close fd events.
    const CLOSE_FD_TOKEN: usize = 0x04;

    /// Signals that all work generated while reaping operation completions is terminal.
    const RESOURCES_DRAIN_TOKEN: usize = 0x05;

    /// Create a new [`Driver`] with the provided size from the provided [`io_uring::Builder`].
    ///
    /// This uses [`DriverOptions::default`]. Use
    /// [`Driver::new_with_options`] to customize driver behavior.
    pub fn new(builder: io_uring::Builder, size: u32) -> io::Result<Self> {
        Self::new_with_options(builder, size, DriverOptions::default())
    }

    /// Create a new [`Driver`] with the provided builder, size, and options.
    pub fn new_with_options(
        mut builder: io_uring::Builder,
        size: u32,
        options: DriverOptions,
    ) -> io::Result<Self> {
        let ring = builder.dontfork().build(size)?;
        Ok(Self {
            options,
            shared: Rc::new(Shared {
                ring: RefCell::new(ring),
                backpressure: Notify::default(),
                status: Cell::new(Status::Running),
                submit_error: RefCell::new(None),
                shutdown_outcome: Cell::new(None),
                #[cfg(test)]
                cancel_all_error: Cell::new(None),
                #[cfg(test)]
                submit_failures: RefCell::new(std::collections::VecDeque::new()),
                #[cfg(test)]
                submit_limits: RefCell::new(std::collections::VecDeque::new()),
                registered_buffers: RegisteredBuffers::new(),
            }),
            unparker: Arc::new(unpark::Unparker::new()?),
            unparker_buf: mem::ManuallyDrop::new(UnparkerBuffer::new()),
        })
    }

    /// Returns a handle to the driver.
    ///
    /// The handle can be used to submit new requests to the driver.
    pub fn handle(&self) -> Handle {
        Handle {
            shared: Rc::clone(&self.shared),
        }
    }

    /// Prepare the ring for parking.
    ///
    /// Returns true if the ring is ready for parking.
    fn prepare_park(&self) -> bool {
        if self.shared.status() != Status::Running {
            return true;
        }
        let action = self.unparker.park();
        if action == unpark::ParkAction::Notified {
            return false;
        }
        if action == unpark::ParkAction::Arm {
            let fd = self.unparker.raw_fd();
            let fd = io_uring::types::Fd(fd);
            // Safety: We use the unparker to track the outstanding requests which use the unparker_buf, preventing
            //         any two requests from running at the same time.
            let unparker_ptr = self.unparker_buf.get();
            let opcode = io_uring::opcode::Read::new(fd, unparker_ptr as _, 8)
                .build()
                .user_data(Self::UNPARKER_WAKE_TOKEN as u64);
            // Safety: The request relies on some shared state which is marked as ManuallyDrop. The shared state
            //         is only ever dropped once the reactor has shutdown. Additionally, we're leaning on the safety
            //         requirements from prepare_unparker to ensure another [io_uring::SubmissionQueue] does not exist.

            if unsafe { self.shared.try_push_raw(&opcode) }.is_err() {
                // We were not able to arm the eventfd read, so we must not leave the
                // reactor marked as parked.
                self.unparker.clear_parked();
                return false;
            }
        }
        true
    }

    /// Submits all pending entries to the ring.
    ///
    /// This will block the calling thread based on the provided `ParkMode`. It can return
    /// EBUSY, in which case the caller should retry.
    ///
    /// Returns the number of entries which were submitted.
    fn submit(&self, mut mode: ParkMode) -> io::Result<usize> {
        // If we're going to park, then prepare the unparker.
        if matches!(mode, ParkMode::Timeout(_) | ParkMode::NextCompletion) && !self.prepare_park() {
            // Preparing the unparker failed, don't park!
            mode = ParkMode::NoPark;
        }
        trace!(target: LOG, "submit.mode {:?}", mode);
        let submitted = self.shared.submit(mode)?;
        log::trace!(target: LOG, "submit.submitted {}", submitted);
        Ok(submitted)
    }

    /// Drain up to `max` entries from the ring.
    ///
    /// This will continuously drain entries from the ring until there are either
    /// no more entries left, or `max` entries have been drained. Once either of
    /// these conditions is true, this method will return the number of entries
    /// drained.
    ///
    /// `N` is the number of entries to drain at a time. This is used to allocate
    /// storage for copying the entries out of the ring. This should be a small value.
    fn drain<const N: usize>(&self, max: usize) -> usize {
        assert!(N > 0, "completion drain batch must be nonzero");
        if max == 0 {
            return 0;
        }
        let mut entries: [mem::MaybeUninit<cqueue::Entry>; N] =
            unsafe { mem::MaybeUninit::uninit().assume_init() };
        let mut total_drained = 0;
        loop {
            let remaining = max - total_drained;
            let batch_len = remaining.min(N);
            let (entries, has_more) = self.shared.drain_fill(&mut entries[..batch_len]);
            let nr_drained = entries.len();
            for cqe in entries {
                let user_data = cqe.user_data() as usize;
                if user_data == Self::OPERATIONS_DRAIN_TOKEN {
                    trace!(target: LOG, "drain.operations.token");
                    debug_assert_eq!(self.shared.status(), Status::DrainingOperations);
                    self.shared.set_status(Status::ClosingResources);
                    continue;
                }
                if user_data == Self::RESOURCES_DRAIN_TOKEN {
                    trace!(target: LOG, "drain.resources.token");
                    debug_assert_eq!(self.shared.status(), Status::DrainingResources);
                    self.shared.finish_shutdown(ShutdownOutcome::CleanDrained);
                    continue;
                }
                if user_data == Self::UNPARKER_WAKE_TOKEN {
                    trace!(target: LOG, "drain.token");
                    self.unparker.reset();
                    continue;
                }

                if user_data == Self::CANCELLATION_TOKEN {
                    trace!(target: LOG, "cancellation.token");
                    continue;
                }

                if user_data == Self::CLOSE_FD_TOKEN {
                    trace!(target: LOG, "close_fd.token");
                    continue;
                }

                if user_data <= 1024 {
                    let result = cqe.result();
                    let result = if result >= 0 {
                        Ok(result as u32)
                    } else {
                        Err(io::Error::from_raw_os_error(-result))
                    };
                    warn!(target: LOG, "drain.invalid_user_data {result:?}");
                    // Surely nothing in our heap is going to be allocated at < 1024!
                    // We are keeping this space reserved for additional operations.
                    continue;
                }
                // Safety: This is being called on a completion queue entry which has been generated
                // by a prior submission.
                unsafe { complete_operation(cqe) }
            }
            total_drained += nr_drained;
            if !has_more || total_drained >= max {
                break;
            }
        }
        total_drained
    }

    fn retry_shutdown(&self, stage: &'static str, err: &io::Error) {
        warn!(target: LOG, "shutdown.{stage}.retry {err:?}");
        let drained = self.drain_exhaustive();
        trace!(target: LOG, "shutdown.{stage}.retry drained={drained}");
        if self.shared.status() != Status::Shutdown {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn drain_normal(&self, remaining: &mut Option<usize>) -> usize {
        let drained = self.drain::<COMPLETION_DRAIN_BATCH>(remaining.unwrap_or(usize::MAX));
        if let Some(remaining) = remaining {
            *remaining -= drained;
        }
        drained
    }

    fn drain_exhaustive(&self) -> usize {
        self.drain::<COMPLETION_DRAIN_BATCH>(usize::MAX)
    }

    fn park_with_completion_budget(
        &mut self,
        mut mode: ParkMode,
        mut remaining: Option<usize>,
    ) -> Result<(), io::Error> {
        let drained = self.drain_normal(&mut remaining);
        if drained > 0 {
            trace!(target: LOG, "park.drained {}", drained);
            mode = ParkMode::NoPark;
        }

        loop {
            match self.submit(mode) {
                Ok(_) => return Ok(()),
                Err(err) if err.raw_os_error() == Some(libc::EBUSY) => {
                    trace!(target: LOG, "park.ebusy");
                    let drained = self.drain_normal(&mut remaining);
                    trace!(target: LOG, "park.drained {}", drained);
                    if remaining.is_some() && (drained == 0 || remaining == Some(0)) {
                        // Yield to runnable work. The next park call will retry
                        // any submission that remains queued.
                        return Ok(());
                    }
                    mode = ParkMode::NoPark;
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {
                    error!(target: LOG, "park.eintr");
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl Park for Driver {
    type Unparker = Arc<unpark::Unparker>;

    type Guard = context::DriverContextGuard;

    fn park(&mut self, mode: ParkMode) -> Result<(), io::Error> {
        let budget = self.options.completion_drain_budget.map(NonZeroUsize::get);
        self.park_with_completion_budget(mode, budget)
    }

    fn enter(&self) -> Self::Guard {
        context::DriverContext::enter(self.handle())
    }

    fn unparker(&self) -> Self::Unparker {
        Arc::clone(&self.unparker)
    }

    fn needs_park(&self) -> bool {
        self.shared
            .needs_park(self.options.completion_drain_budget.is_some())
    }

    fn shutdown(&mut self) {
        let _ = self.shutdown_with_outcome();
    }
}

impl Driver {
    fn shutdown_with_outcome(&mut self) -> ShutdownOutcome {
        if let Some(outcome) = self.shared.shutdown_outcome() {
            return outcome;
        }
        if matches!(self.shared.status(), Status::Running | Status::Shutdown) {
            self.shared.set_status(Status::Closing);
        }

        // Once an SQE may have reached the kernel, returning before its terminal CQE
        // is unsafe: the operation can retain buffers, descriptors, and this driver.
        // Teardown therefore retries indefinitely. A permanently broken ring can
        // make shutdown block forever, but it cannot make shutdown abandon memory
        // still owned by the kernel.
        loop {
            if let Some(outcome) = self.shared.shutdown_outcome() {
                return outcome;
            }
            match self.shared.status() {
                Status::Shutdown => self.shared.set_status(Status::Closing),
                Status::Running => self.shared.set_status(Status::Closing),
                Status::Closing => {
                    self.unparker.wake();
                    if let Err(err) = self.shared.submit_all_pending() {
                        self.retry_shutdown("submit", &err);
                        continue;
                    }

                    if let Err(err) = self.shared.cancel_all() {
                        if err.raw_os_error() != Some(libc::ENOENT) {
                            self.retry_shutdown("cancel_all", &err);
                            continue;
                        }
                    }

                    let opcode = io_uring::opcode::Nop::new()
                        .build()
                        .flags(io_uring::squeue::Flags::IO_DRAIN)
                        .user_data(Self::OPERATIONS_DRAIN_TOKEN as u64);
                    if unsafe { self.shared.try_push_raw(&opcode) }.is_ok() {
                        self.shared.set_status(Status::DrainingOperations);
                    } else {
                        let drained = self.drain_exhaustive();
                        trace!(target: LOG, "shutdown.push_operations_drain.retry drained={drained}");
                        if drained == 0 {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
                Status::DrainingOperations => {
                    if let Err(err) =
                        self.park_with_completion_budget(ParkMode::NextCompletion, None)
                    {
                        self.retry_shutdown("park", &err);
                    }
                }
                Status::ClosingResources => {
                    // Reaping the first barrier can destroy RawOps. Their destructors may
                    // enqueue close SQEs or other cleanup work after that barrier, so flush
                    // the complete userspace SQ before ordering a final drain behind it.
                    if let Err(err) = self.shared.submit_all_pending() {
                        self.retry_shutdown("submit_cleanup", &err);
                        continue;
                    }

                    let opcode = io_uring::opcode::Nop::new()
                        .build()
                        .flags(io_uring::squeue::Flags::IO_DRAIN)
                        .user_data(Self::RESOURCES_DRAIN_TOKEN as u64);
                    if unsafe { self.shared.try_push_raw(&opcode) }.is_ok() {
                        self.shared.set_status(Status::DrainingResources);
                    } else {
                        let drained = self.drain::<32>(usize::MAX);
                        trace!(target: LOG, "shutdown.push_resources_drain.retry drained={drained}");
                        if drained == 0 {
                            std::thread::sleep(Duration::from_millis(1));
                        }
                    }
                }
                Status::DrainingResources => {
                    if let Err(err) = self.park(ParkMode::NextCompletion) {
                        self.retry_shutdown("park_cleanup", &err);
                    }
                }
            }
        }
    }
}

impl Shared {
    fn validate_batch_len(&self, batch_len: usize) -> Result<(), SubmitError> {
        if batch_len <= 1 {
            return Ok(());
        }
        let mut ring = self.ring.borrow_mut();
        let sq = ring.submission();
        let capacity = sq.capacity();
        if batch_len > capacity {
            return Err(SubmitError::batch_too_large(batch_len, capacity));
        }
        Ok(())
    }

    fn should_record_submit_error(err: &io::Error) -> bool {
        !matches!(err.raw_os_error(), Some(libc::EBUSY | libc::EINTR))
    }

    /// Get the current status of the driver.
    fn status(&self) -> Status {
        self.status.get()
    }

    fn shutdown_outcome(&self) -> Option<ShutdownOutcome> {
        self.shutdown_outcome.get()
    }

    fn finish_shutdown(&self, outcome: ShutdownOutcome) {
        if self.shutdown_outcome.get().is_none() {
            self.shutdown_outcome.set(Some(outcome));
        }
        self.set_status(Status::Shutdown);
    }

    fn health_error(&self) -> Option<io::Error> {
        self.submit_error
            .borrow()
            .as_ref()
            .map(|(kind, message)| io::Error::new(*kind, message.clone()))
    }

    fn record_submit_error(&self, err: &io::Error) {
        let mut slot = self.submit_error.borrow_mut();
        if slot.is_none() {
            *slot = Some((err.kind(), err.to_string()));
        }
        drop(slot);
        if self.status() == Status::Running {
            self.set_status(Status::Closing);
        }
    }

    /// Set the status of the driver.
    ///
    /// All waiters will be notified of the status change.
    fn set_status(&self, status: Status) {
        debug!(target: LOG, "status.change {:?} => {:?}", self.status.get(), status);
        if status != self.status.get() {
            // On status change, notify all waiters.
            self.backpressure.notify(usize::MAX);
        }
        self.status.set(status);
    }

    /// Attempt to push a single entry into the submission queue.
    ///
    /// If the submission queue is full, this returns the original entry.
    fn try_push(&self, entry: ConfiguredEntry) -> Result<(), ConfiguredEntry> {
        let mut ring = self.ring.borrow_mut();
        let mut sq = ring.submission();
        if sq.is_full() {
            Err(entry)
        } else {
            let entry = entry.into_entry_with_flags(Flags::empty());
            unsafe { sq.push(&entry) }.unwrap();
            Ok(())
        }
    }

    /// Attempt to push a batch of entries into the submission queue.
    ///
    /// If the submission queue does not have enough free capacity for the full
    /// batch, returns `false` and leaves the entries unchanged.
    fn try_push_batch(&self, entries: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        let mut ring = self.ring.borrow_mut();
        let mut sq = ring.submission();
        if sq.capacity() - sq.len() < entries.len() {
            return false;
        }

        if entries.len() == 1 {
            let entry = entries.pop().expect("singleton batch missing entry");
            let entry = entry.into_entry_with_flags(Flags::empty());
            unsafe { sq.push(&entry) }.unwrap();
            return true;
        }

        let len = entries.len();
        let mut raw_entries = SmallVec::<[io_uring::squeue::Entry; 4]>::with_capacity(len);
        for (idx, entry) in std::mem::take(entries).into_iter().enumerate() {
            let flags = if idx + 1 == len {
                Flags::empty()
            } else {
                Flags::IO_LINK
            };
            raw_entries.push(entry.into_entry_with_flags(flags));
        }
        unsafe { sq.push_multiple(raw_entries.as_slice()) }.unwrap();
        true
    }

    /// Attempt to push a new raw entry into the submission queue.
    ///
    /// If the submission queue is full, this will return an error.
    unsafe fn try_push_raw(&self, entry: &io_uring::squeue::Entry) -> Result<(), PushError> {
        let mut ring = self.ring.borrow_mut();
        let mut sq = ring.submission();
        sq.push(entry)
    }

    fn with_submitter<U>(&self, f: impl FnOnce(&Submitter<'_>) -> U) -> U {
        let ring = self.ring.borrow();
        let sq = ring.submitter();
        f(&sq)
    }

    fn submit_once(&self, mode: ParkMode) -> io::Result<usize> {
        #[cfg(test)]
        if let Some(errno) = self.submit_failures.borrow_mut().pop_front() {
            return Err(io::Error::from_raw_os_error(errno));
        }

        #[cfg(test)]
        if let Some((limit, expected_remaining)) = self.submit_limits.borrow_mut().pop_front() {
            assert_eq!(mode, ParkMode::NoPark);
            let mut ring = self.ring.borrow_mut();
            let pending = {
                let mut sq = ring.submission();
                sq.sync();
                sq.len()
            };
            if pending == 0 {
                return Ok(0);
            }
            let to_submit = pending.min(limit);
            // Safety: this is the same io_uring_enter operation used by Submitter::submit,
            // deliberately capped to emulate a successful partial kernel consumption.
            let submitted = unsafe {
                ring.submitter()
                    .enter::<libc::sigset_t>(to_submit as u32, 0, 0, None)
            }?;
            let remaining = {
                let mut sq = ring.submission();
                sq.sync();
                sq.len()
            };
            assert_eq!(
                remaining, expected_remaining,
                "capped shutdown submit did not leave the expected userspace SQ remainder"
            );
            return Ok(submitted);
        }

        let mut ring = self.ring.borrow_mut();
        Ok(match mode {
            ParkMode::Timeout(duration) => {
                let ts = Timespec::new()
                    .sec(duration.as_secs())
                    .nsec(duration.subsec_nanos());
                let args = SubmitArgs::new().timespec(&ts);
                ring.submitter().submit_with_args(1, &args)?
            }
            ParkMode::NextCompletion => {
                let args = SubmitArgs::new();
                ring.submitter().submit_with_args(1, &args)?
            }
            ParkMode::NoPark => {
                let sq = ring.submission();
                if sq.is_empty() {
                    0
                } else {
                    drop(sq);
                    ring.submitter().submit()?
                }
            }
        })
    }

    /// Submit all entries in the submission queue.
    ///
    /// The provided `ParkMode` is used to determine if the
    /// submission should block on new completions or not.
    ///
    /// Returns the number of entries which were submitted.
    fn submit(&self, mode: ParkMode) -> io::Result<usize> {
        let mut ebusy_retries = 0usize;
        loop {
            match self.submit_once(mode) {
                Ok(submitted) => {
                    if submitted > 0 {
                        self.backpressure.notify(submitted);
                    }
                    return Ok(submitted);
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {
                    trace!(target: LOG, "submit.eintr");
                    continue;
                }
                Err(err) if err.raw_os_error() == Some(libc::EBUSY) && ebusy_retries < 8 => {
                    ebusy_retries += 1;
                    trace!(target: LOG, "submit.ebusy retry={}", ebusy_retries);
                    continue;
                }
                Err(err) if err.raw_os_error() == Some(libc::EBUSY) => {
                    trace!(target: LOG, "submit.ebusy retries_exhausted");
                    return Err(err);
                }
                Err(err) => {
                    if Self::should_record_submit_error(&err) {
                        self.record_submit_error(&err);
                    }
                    return Err(err);
                }
            }
        }
    }

    /// Submit every SQE currently visible in the userspace submission queue.
    ///
    /// A successful `io_uring_enter` may consume fewer entries than requested. Shutdown
    /// must drive that remainder into the kernel before synchronous cancellation, or the
    /// cancel registration cannot observe all admitted operations.
    fn submit_all_pending(&self) -> io::Result<usize> {
        let mut total_submitted = 0;
        loop {
            let pending_before = self.pending_submissions();
            if pending_before == 0 {
                return Ok(total_submitted);
            }

            let submitted = self.submit(ParkMode::NoPark)?;
            total_submitted += submitted;

            let pending_after = self.pending_submissions();
            if pending_after == 0 {
                return Ok(total_submitted);
            }
            if pending_after >= pending_before {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "submission queue made no progress",
                ));
            }
        }
    }

    fn pending_submissions(&self) -> usize {
        let mut ring = self.ring.borrow_mut();
        let mut sq = ring.submission();
        sq.sync();
        sq.len()
    }

    /// Cancel a specific request synchronously.
    ///
    /// Returns an error if the request could not be cancelled.
    fn cancel(&self, criteria: CancelBuilder, sync: bool) -> io::Result<()> {
        // Submit all unsubmitted entries to the ring so that we can cancel them.
        self.submit(ParkMode::NoPark)?;

        // First try to submit an async cancel request, this avoids a syscall.
        let mut ring = self.ring.borrow_mut();
        if !sync {
            let mut sq = ring.submission();
            if !sq.is_full() {
                let cancel = opcode::AsyncCancel2::new(criteria)
                    .build()
                    .flags(Flags::SKIP_SUCCESS)
                    .user_data(Driver::CANCELLATION_TOKEN as u64);
                unsafe { sq.push(&cancel) }.unwrap();
                return Ok(());
            }
        }
        let submitter = ring.submitter();
        submitter.register_sync_cancel(None, criteria)?;
        Ok(())
    }

    fn close_fd(&self, kind: &fd::FdKind) -> Result<(), CloseFdError> {
        let entry = match kind {
            fd::FdKind::Fd(fd) => opcode::Close::new(types::Fd(fd.0)).build(),
            fd::FdKind::Fixed(fd) => opcode::Close::new(types::Fixed(fd.0)).build(),
        }
        .flags(Flags::SKIP_SUCCESS)
        .user_data(Driver::CLOSE_FD_TOKEN as u64);

        if unsafe { self.try_push_raw(&entry) }.is_err() {
            // The close entry is still caller-owned, so a submit failure here proves
            // that the kernel has never seen it.
            self.submit(ParkMode::NoPark)
                .map_err(CloseFdError::NeverQueued)?;
            unsafe { self.try_push_raw(&entry) }.map_err(|err| {
                CloseFdError::NeverQueued(io::Error::other(format!(
                    "failed to push close entry: {err:?}"
                )))
            })?;
        }

        // Ensure close requests are not stranded in SQ if no further park cycle happens.
        self.submit(ParkMode::NoPark)
            .map_err(CloseFdError::Queued)?;
        Ok(())
    }

    /// Cancel all outstanding requests synchronously.
    pub(crate) fn cancel_all(&self) -> io::Result<()> {
        #[cfg(test)]
        if let Some(errno) = self.cancel_all_error.take() {
            return Err(io::Error::from_raw_os_error(errno));
        }

        let ring = self.ring.borrow();
        let criteria = CancelBuilder::any();
        let timeout = Timespec::from(SHUTDOWN_CANCEL_TIMEOUT);
        match ring
            .submitter()
            .register_sync_cancel(Some(timeout), criteria)
        {
            Ok(()) => Ok(()),
            // No matching requests means there is nothing left to cancel.
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err),
        }
    }

    #[cfg(test)]
    fn fail_next_submit(&self, errno: i32) {
        self.submit_failures.borrow_mut().push_back(errno);
    }

    #[cfg(test)]
    fn limit_next_submit(&self, limit: usize, expected_remaining: usize) {
        assert!(limit > 0);
        self.submit_limits
            .borrow_mut()
            .push_back((limit, expected_remaining));
    }

    fn needs_park(&self, completion_drain_is_bounded: bool) -> bool {
        // First check if there are any waiters, this is a cheap check
        // compared to checking the ring.
        if self.backpressure.waiters() > 0 {
            return true;
        }
        if NEEDS_PARK_CHECK_RINGS {
            let mut ring = self.ring.borrow_mut();
            let (_, sq, cq) = ring.split();
            sq.is_full()
                || if completion_drain_is_bounded {
                    !cq.is_empty()
                } else {
                    cq.is_full()
                }
        } else {
            false
        }
    }

    /// Drain the completion queue into the provided buffer.
    ///
    /// Returns the filled buffer, and a flag indicating if there are more entries after
    /// this buffer.
    fn drain_fill<'a>(
        &'a self,
        entries: &'a mut [mem::MaybeUninit<cqueue::Entry>],
    ) -> (&'a mut [cqueue::Entry], bool) {
        let mut ring = self.ring.borrow_mut();
        let mut cq = ring.completion();
        let has_more = cq.len() > entries.len();
        (cq.fill(entries), has_more)
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let outcome = self.shutdown_with_outcome();
        if outcome == ShutdownOutcome::CleanDrained {
            // Safety: observing the IO_DRAIN sentinel proves the kernel has
            // finished every earlier SQE. In particular, no eventfd read can
            // still reference this buffer. Driver is dropping, so this is the
            // buffer's only explicit release.
            unsafe { mem::ManuallyDrop::drop(&mut self.unparker_buf) };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::TcpListener;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::task::Poll;
    use std::time::Duration;

    use smallvec::SmallVec;

    use super::*;
    use crate::operation::{CQEResult, Operation, Singleshot};
    use crate::Request;

    fn preload_nop_completions(driver: &Driver, total: usize) {
        let nop = opcode::Nop::new()
            .build()
            .user_data(Driver::CANCELLATION_TOKEN as u64);
        let mut ring = driver.shared.ring.borrow_mut();
        {
            let mut sq = ring.submission();
            assert!(sq.capacity() - sq.len() >= total);
            for _ in 0..total {
                // Safety: the test-only filler NOP references no external
                // storage and uses a reserved completion token.
                unsafe { sq.push(&nop) }.unwrap();
            }
        }
        assert!(ring.submitter().submit_and_wait(total).unwrap() >= total);
        assert_eq!(ring.completion().len(), total);
    }

    enum LongLivedIo {
        Read {
            reader: UnixStream,
            _writer: UnixStream,
            buf: Box<[u8; 8]>,
        },
        Accept(TcpListener),
    }

    struct LongLivedOp {
        io: LongLivedIo,
        _driver: Handle,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for LongLivedOp {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    unsafe impl Operation for LongLivedOp {
        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(match &mut self.io {
                LongLivedIo::Read { reader, buf, .. } => opcode::Read::new(
                    types::Fd(reader.as_raw_fd()),
                    buf.as_mut_ptr(),
                    buf.len() as u32,
                )
                .build(),
                LongLivedIo::Accept(listener) => opcode::Accept::new(
                    types::Fd(listener.as_raw_fd()),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                )
                .build(),
            })
        }

        fn cleanup(&mut self, result: CQEResult) {
            if matches!(self.io, LongLivedIo::Accept(_)) {
                if let Ok(fd) = result.result {
                    // Safety: a successful accept completion returns a newly owned fd.
                    unsafe { libc::close(fd as i32) };
                }
            }
        }
    }

    impl Singleshot for LongLivedOp {
        type Output = io::Result<u32>;

        fn complete(self, result: CQEResult) -> Self::Output {
            result.result
        }
    }

    fn assert_cancel_failure_retries(io: LongLivedIo, dropped: Arc<AtomicBool>) {
        let thread_dropped = Arc::clone(&dropped);
        let unparker_buf_drops = Arc::new(AtomicUsize::new(0));
        let thread_unparker_buf_drops = Arc::clone(&unparker_buf_drops);
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
            driver.unparker_buf.track_drop(thread_unparker_buf_drops);
            let handle = driver.handle();
            let op = LongLivedOp {
                io,
                _driver: handle.clone(),
                dropped: thread_dropped,
            };
            let mut op = Box::pin(handle.submit(op));
            let waker = futures_test::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);

            assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
            driver
                .park(ParkMode::NoPark)
                .expect("long-lived operation submission failed");
            assert!(driver.prepare_park(), "eventfd read should be queued");
            driver
                .shared
                .submit(ParkMode::NoPark)
                .expect("eventfd read submission failed");

            drop(op);
            drop(handle);
            driver.shared.cancel_all_error.set(Some(libc::EIO));
            let outcome = driver.shutdown_with_outcome();
            let shared_refs = Rc::strong_count(&driver.shared);
            drop(driver);
            tx.send((outcome, shared_refs)).unwrap();
        });

        let (outcome, shared_refs) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("shutdown did not recover from the injected cancellation failure");
        assert_eq!(outcome, ShutdownOutcome::CleanDrained);
        assert_eq!(
            shared_refs, 1,
            "clean drain must release the operation-owned driver reference"
        );
        assert!(
            dropped.load(Ordering::Acquire),
            "the cancelled operation must be reclaimed after its terminal CQE"
        );
        assert_eq!(
            unparker_buf_drops.load(Ordering::Acquire),
            1,
            "clean shutdown must release the kernel-referenced unparker buffer once"
        );
    }

    #[test]
    fn cancel_all_failure_retries_long_lived_read() {
        let (reader, writer) = UnixStream::pair().unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        assert_cancel_failure_retries(
            LongLivedIo::Read {
                reader,
                _writer: writer,
                buf: Box::new([0; 8]),
            },
            dropped,
        );
    }

    #[test]
    fn cancel_all_failure_retries_long_lived_accept() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        assert_cancel_failure_retries(LongLivedIo::Accept(listener), dropped);
    }

    #[test]
    fn shutdown_without_pending_requests_is_clean_drained() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();

        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
    }

    #[test]
    fn completion_drain_budget_is_opt_in() {
        const BUDGET: usize = 2;
        const TOTAL: usize = BUDGET + 1;

        let mut unbounded = Driver::new(io_uring::IoUring::builder(), 4).unwrap();
        preload_nop_completions(&unbounded, TOTAL);
        unbounded.park(ParkMode::NoPark).unwrap();
        assert_eq!(unbounded.shared.ring.borrow_mut().completion().len(), 0);

        let options = DriverOptions {
            completion_drain_budget: NonZeroUsize::new(BUDGET),
        };
        let mut bounded =
            Driver::new_with_options(io_uring::IoUring::builder(), 4, options).unwrap();
        preload_nop_completions(&bounded, TOTAL);
        bounded.park(ParkMode::NoPark).unwrap();
        assert_eq!(bounded.shared.ring.borrow_mut().completion().len(), 1);
        assert!(bounded.needs_park());
        bounded.park(ParkMode::NoPark).unwrap();
        assert_eq!(bounded.shared.ring.borrow_mut().completion().len(), 0);
    }

    #[test]
    fn shutdown_drain_is_exhaustive_past_normal_budget() {
        let options = DriverOptions {
            completion_drain_budget: NonZeroUsize::new(2),
        };
        let mut driver =
            Driver::new_with_options(io_uring::IoUring::builder(), 4, options).unwrap();
        preload_nop_completions(&driver, 3);

        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        assert_eq!(driver.shared.ring.borrow_mut().completion().len(), 0);
    }

    #[test]
    fn clean_shutdown_releases_unparker_buffer_once() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let unparker_buf_drops = Arc::new(AtomicUsize::new(0));
        driver
            .unparker_buf
            .track_drop(Arc::clone(&unparker_buf_drops));

        assert!(driver.prepare_park(), "eventfd read should be queued");
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        assert_eq!(unparker_buf_drops.load(Ordering::Acquire), 0);

        Park::shutdown(&mut driver);
        assert_eq!(unparker_buf_drops.load(Ordering::Acquire), 0);

        drop(driver);
        assert_eq!(unparker_buf_drops.load(Ordering::Acquire), 1);
    }

    #[test]
    fn prepare_park_consumes_a_latched_wake_before_arming() {
        let driver = Driver::new(io_uring::IoUring::builder(), 2).unwrap();

        driver.unparker.wake_inner();
        assert!(driver.unparker.state().woken());
        assert!(!driver.prepare_park());
        assert!(!driver.unparker.state().woken());
        assert!(!driver.unparker.state().is_parked());
        {
            let mut ring = driver.shared.ring.borrow_mut();
            assert_eq!(ring.submission().len(), 0);
        }

        assert!(driver.prepare_park());
        assert!(driver.unparker.state().is_parked());
        assert!(!driver.unparker.state().woken());
        {
            let mut ring = driver.shared.ring.borrow_mut();
            assert_eq!(ring.submission().len(), 1);
        }
    }

    #[test]
    fn prepare_park_sq_full_clears_parked_state() {
        let driver = Driver::new(io_uring::IoUring::builder(), 2).unwrap();
        let entry = io_uring::opcode::Nop::new()
            .build()
            .user_data(Driver::CANCELLATION_TOKEN as u64);

        loop {
            // Safety: test-only queue filling with a trivially valid NOP entry.
            if unsafe { driver.shared.try_push_raw(&entry) }.is_err() {
                break;
            }
        }

        assert!(!driver.unparker.state().is_parked());

        let should_park = driver.prepare_park();
        assert!(!should_park);
        assert!(
            !driver.unparker.state().is_parked(),
            "prepare_park must not leave the unparker parked when enqueue fails"
        );

        driver.unparker.wake_inner();
        assert!(
            driver.unparker.state().woken(),
            "remote wake should still be observable after prepare_park failure"
        );
    }

    #[test]
    fn health_error_is_exposed_and_submit_fails_fast() {
        #[derive(Debug)]
        struct NopOp;

        unsafe impl Operation for NopOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for NopOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        driver
            .shared
            .record_submit_error(&io::Error::from_raw_os_error(libc::EIO));
        assert_eq!(driver.shared.status(), Status::Closing);

        let health = handle
            .health_error()
            .expect("driver should expose health error");
        assert!(
            health.to_string().contains("Input/output error"),
            "unexpected health error message: {}",
            health
        );

        let mut op = std::pin::pin!(handle.submit(NopOp));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let poll = Future::poll(op.as_mut(), &mut cx);
        let err = match poll {
            std::task::Poll::Ready(Err(err)) => err,
            other => panic!("expected ready error, got: {other:?}"),
        };
        assert!(
            err.to_string().contains("submit path failed"),
            "unexpected submit error: {}",
            err
        );
    }

    #[test]
    fn shutdown_retries_submit_failure_and_releases_fd_operations() {
        #[derive(Debug)]
        struct PendingFdOp {
            fd: fd::NornFd,
            timeout: Timespec,
            dropped: Rc<Cell<usize>>,
        }

        impl Drop for PendingFdOp {
            fn drop(&mut self) {
                self.dropped.set(self.dropped.get() + 1);
            }
        }

        // Safety: RawOp keeps the timeout storage stable until the terminal CQE.
        unsafe impl Operation for PendingFdOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                let _ = self.fd.kind();
                Ok(opcode::Timeout::new(&self.timeout).build())
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for PendingFdOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        #[derive(Debug)]
        struct PendingFixedFdOp {
            fd: fd::NornFd,
            buf: crate::fixedbuf::FixedBuf<Vec<u8>>,
            timeout: Timespec,
            dropped: Rc<Cell<usize>>,
        }

        impl Drop for PendingFixedFdOp {
            fn drop(&mut self) {
                self.dropped.set(self.dropped.get() + 1);
            }
        }

        // Safety: RawOp keeps the timeout storage stable until the terminal CQE.
        // The operation also retains a registered fixed buffer during that period.
        unsafe impl Operation for PendingFixedFdOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                let _ = self.fd.kind();
                let _ = self.buf.index();
                Ok(opcode::Timeout::new(&self.timeout).build())
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for PendingFixedFdOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let weak_shared = Rc::downgrade(&driver.shared);
        let dropped = Rc::new(Cell::new(0));
        let guard = driver.enter();
        let handle = driver.handle();
        let pool = handle.register_fixed_buffers(vec![vec![0u8; 32]]).unwrap();
        let fixed_buf = pool.try_acquire().unwrap();
        let fd = fd::NornFd::from_fd(-1);
        let mut op = Box::pin(handle.submit(PendingFdOp {
            fd: fd.clone(),
            timeout: Timespec::new().sec(3_600),
            dropped: Rc::clone(&dropped),
        }));
        let mut fixed_op = Box::pin(handle.submit(PendingFixedFdOp {
            fd,
            buf: fixed_buf,
            timeout: Timespec::new().sec(3_600),
            dropped: Rc::clone(&dropped),
        }));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
        assert!(Future::poll(fixed_op.as_mut(), &mut cx).is_pending());
        driver.shared.submit(ParkMode::NoPark).unwrap();
        drop(op);
        drop(fixed_op);
        driver.shared.fail_next_submit(libc::EIO);
        drop(handle);
        drop(guard);

        Park::shutdown(&mut driver);

        assert_eq!(driver.shared.status(), Status::Shutdown);
        assert_eq!(
            dropped.get(),
            2,
            "both cancelled operations must be reclaimed"
        );
        drop(driver);
        assert!(
            weak_shared.upgrade().is_none(),
            "shutdown must break the operation/descriptor/driver ownership cycle"
        );
        assert_eq!(pool.unregister().unwrap().len(), 1);
    }

    #[test]
    fn shutdown_waits_for_cleanup_generated_close_before_returning() {
        #[derive(Debug)]
        struct PendingFdOp {
            _fd: fd::NornFd,
            timeout: Timespec,
            shared: Weak<Shared>,
        }

        impl Drop for PendingFdOp {
            fn drop(&mut self) {
                // Force the NornFd field's subsequent close submission to remain in
                // userspace until the shutdown finalization pass retries it.
                self.shared
                    .upgrade()
                    .expect("driver shared state dropped before operation cleanup")
                    .fail_next_submit(libc::EIO);
            }
        }

        // Safety: RawOp keeps the timeout storage stable until the terminal CQE.
        unsafe impl Operation for PendingFdOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(opcode::Timeout::new(&self.timeout).build())
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for PendingFdOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        fn assert_open(raw_fd: libc::c_int) {
            assert_ne!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) }, -1);
        }

        fn assert_closed(raw_fd: libc::c_int) {
            assert_eq!(unsafe { libc::fcntl(raw_fd, libc::F_GETFD) }, -1);
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));
        }

        let mut pipe_fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0);
        let owned_fd = pipe_fds[0];
        let write_fd = pipe_fds[1];
        let duplicate_fd = unsafe { libc::dup(owned_fd) };
        assert!(duplicate_fd >= 0);

        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let weak_shared = Rc::downgrade(&driver.shared);
        let guard = driver.enter();
        let handle = driver.handle();
        let mut op = Box::pin(handle.submit(PendingFdOp {
            _fd: fd::NornFd::from_fd(owned_fd),
            timeout: Timespec::new().sec(3_600),
            shared: weak_shared,
        }));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
        driver.shared.submit(ParkMode::NoPark).unwrap();
        drop(op);
        drop(handle);
        drop(guard);

        Park::shutdown(&mut driver);

        assert!(driver.shared.submit_failures.borrow().is_empty());
        assert_closed(owned_fd);
        assert_open(duplicate_fd);

        // Reuse the exact integer descriptor before destroying the ring. A close SQE
        // left behind the first drain would close this unrelated replacement later.
        assert_eq!(unsafe { libc::dup2(duplicate_fd, owned_fd) }, owned_fd);
        assert_open(owned_fd);
        drop(driver);
        assert_open(owned_fd);

        unsafe {
            libc::close(owned_fd);
            libc::close(duplicate_fd);
            libc::close(write_fd);
        }
    }

    #[test]
    fn shutdown_submits_partial_success_remainder_before_cancel_all() {
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            #[derive(Debug)]
            struct NopOp;

            unsafe impl Operation for NopOp {
                fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                    Ok(opcode::Nop::new().build())
                }

                fn cleanup(&mut self, _: CQEResult) {}
            }

            impl Singleshot for NopOp {
                type Output = io::Result<()>;

                fn complete(self, result: CQEResult) -> Self::Output {
                    result.result.map(drop)
                }
            }

            let (reader, writer) = UnixStream::pair().unwrap();
            let dropped = Arc::new(AtomicBool::new(false));
            let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
            let handle = driver.handle();
            let mut nop = Box::pin(handle.submit(NopOp));
            let mut long_lived = Box::pin(handle.submit(LongLivedOp {
                io: LongLivedIo::Read {
                    reader,
                    _writer: writer,
                    buf: Box::new([0; 8]),
                },
                _driver: handle.clone(),
                dropped: Arc::clone(&dropped),
            }));
            let waker = futures_test::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);

            assert!(Future::poll(nop.as_mut(), &mut cx).is_pending());
            assert!(Future::poll(long_lived.as_mut(), &mut cx).is_pending());
            assert_eq!(driver.shared.pending_submissions(), 2);
            // The test hook asserts inside the capped io_uring_enter path, before
            // submit_all_pending can loop and before cancel_all can run, that one
            // long-lived operation still remains in the userspace SQ.
            driver.shared.limit_next_submit(1, 1);

            let outcome = driver.shutdown_with_outcome();
            assert!(
                !dropped.load(Ordering::Acquire),
                "operation future must remain alive throughout shutdown"
            );
            drop(nop);
            drop(long_lived);
            drop(handle);
            tx.send((outcome, dropped.load(Ordering::Acquire))).unwrap();
        });

        let (outcome, dropped) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("shutdown stranded the long-lived userspace SQ remainder");
        assert_eq!(outcome, ShutdownOutcome::CleanDrained);
        assert!(
            dropped,
            "the partially submitted operation must be reclaimed"
        );
    }

    #[test]
    fn submit_construction_is_lazy() {
        #[derive(Debug)]
        struct NopOp;

        unsafe impl Operation for NopOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for NopOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let driver = Driver::new(io_uring::IoUring::builder(), 2).unwrap();
        let _op = driver.handle().submit(NopOp);

        let mut ring = driver.shared.ring.borrow_mut();
        let sq = ring.submission();
        assert_eq!(sq.len(), 0, "request construction must not enqueue");
    }

    #[test]
    fn try_push_batch_needs_full_capacity() {
        #[derive(Debug)]
        struct NopOp;

        unsafe impl Operation for NopOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for NopOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let driver = Driver::new(io_uring::IoUring::builder(), 2).unwrap();
        let handle = driver.handle();

        let mut first = std::pin::pin!(handle.submit(NopOp));
        let mut first_batch = SmallVec::new();
        first.as_mut().prepare_batch(&mut first_batch);
        assert!(driver.shared.try_push_batch(&mut first_batch));
        assert!(first_batch.is_empty());

        let mut second = std::pin::pin!(handle.submit(NopOp));
        let mut third = std::pin::pin!(handle.submit(NopOp));
        let mut batch = SmallVec::new();
        second.as_mut().prepare_batch(&mut batch);
        third.as_mut().prepare_batch(&mut batch);

        assert!(!driver.shared.try_push_batch(&mut batch));
        assert_eq!(batch.len(), 2);

        let mut ring = driver.shared.ring.borrow_mut();
        let sq = ring.submission();
        assert_eq!(sq.len(), 1, "failed batch enqueue must not partially push");
    }

    #[test]
    fn oversized_batch_fails_instead_of_waiting_forever() {
        #[derive(Debug)]
        struct NopOp;

        unsafe impl Operation for NopOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for NopOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let driver = Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        let mut chain = std::pin::pin!(handle.submit(NopOp).then(handle.submit(NopOp)));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        match Future::poll(chain.as_mut(), &mut cx) {
            Poll::Ready((Err(left), Err(right))) => {
                assert_eq!(left.kind(), io::ErrorKind::InvalidInput);
                assert_eq!(right.kind(), io::ErrorKind::InvalidInput);
            }
            other => panic!("expected immediate invalid-input errors, got {other:?}"),
        }
    }

    #[test]
    fn submit_error_classification_keeps_ebusy_nonfatal() {
        let ebusy = io::Error::from_raw_os_error(libc::EBUSY);
        assert!(
            !Shared::should_record_submit_error(&ebusy),
            "EBUSY should remain a transient submit condition"
        );

        let eintr = io::Error::from_raw_os_error(libc::EINTR);
        assert!(
            !Shared::should_record_submit_error(&eintr),
            "EINTR should remain retryable"
        );

        let eio = io::Error::from_raw_os_error(libc::EIO);
        assert!(
            Shared::should_record_submit_error(&eio),
            "hard submit failures should still transition driver health"
        );
    }

    #[test]
    fn fixed_buffer_reservation_is_exclusive_and_rolls_back() {
        let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();

        let reservation = handle.reserve_fixed_buffers().unwrap();
        assert_eq!(reservation.generation(), 1);
        assert_eq!(
            handle.reserve_fixed_buffers().err(),
            Some(ReserveFixedBufError::TableInUse)
        );
        drop(reservation);
        assert_eq!(
            driver.shared.registered_buffers.test_state(),
            FixedBufState::Empty
        );

        let reservation = handle.reserve_fixed_buffers().unwrap();
        assert_eq!(reservation.generation(), 2);
        reservation.arm_kernel_call();
        let token = reservation.commit();
        assert!(token.same_driver(&handle));
        assert_eq!(
            driver.shared.registered_buffers.test_state(),
            FixedBufState::Registered(2)
        );
        assert_eq!(
            handle.reserve_fixed_buffers().err(),
            Some(ReserveFixedBufError::TableInUse)
        );
    }

    #[test]
    fn fixed_buffer_reservation_rejects_stopped_driver() {
        let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        driver.shared.set_status(Status::Shutdown);

        assert_eq!(
            handle.reserve_fixed_buffers().err(),
            Some(ReserveFixedBufError::DriverStopped)
        );
        assert_eq!(
            driver.shared.registered_buffers.test_state(),
            FixedBufState::Empty
        );
    }
}
