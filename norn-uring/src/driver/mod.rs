use std::any::Any;
use std::cell::{Cell, RefCell, UnsafeCell};
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

/// Maximum time shutdown waits for the kernel to cancel all submitted requests.
///
/// `IORING_REGISTER_SYNC_CANCEL` accepts a timeout, so teardown does not need to
/// enter an unbounded syscall when a request cannot be cancelled.
const SHUTDOWN_CANCEL_TIMEOUT: Duration = Duration::from_millis(100);

/// [`Driver`] provides a [`Park`] implementation which will drive
/// a [`IoUring`] instance, submitting new requests and waiting
/// for completions.
///
/// Interaction with the driver is done via [`Handle`]. The handle
/// can be used to submit new requests to the driver.
pub struct Driver {
    shared: Rc<Shared>,
    unparker: Arc<unpark::Unparker>,
    /// Storage passed to the kernel for the eventfd read.
    ///
    /// The buffer is manually dropped because an abandoned ring may still hold
    /// its address. [`Driver::drop`] releases it only after the drain sentinel
    /// proves every earlier request, including the eventfd read, is terminal.
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
    /// The driver is draining and will not accept new requests.
    Draining,
    /// The driver has shutdown and will not accept new requests.
    Shutdown,
}

/// Describes whether shutdown reached the normal reclamation boundary.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ShutdownOutcome {
    /// The drain sentinel completed, so every earlier request reached a terminal CQE.
    CleanDrained,
    /// Teardown stopped waiting and quarantined the ring and kernel-owned allocations.
    Abandoned,
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
        f.debug_struct("Driver").finish()
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
    /// [Driver::DRAIN_TOKEN] is a special token which is used to signal the driver has drained all requests.
    const DRAIN_TOKEN: usize = 0x01;

    /// [Driver::UNPARKER_WAKE_TOKEN] is a special token which is used to signal unparker wake events.
    const UNPARKER_WAKE_TOKEN: usize = 0x02;

    /// [Driver::CANCELLATION_TOKEN] is a special token which is used to signal cancellation events.
    const CANCELLATION_TOKEN: usize = 0x03;

    /// [Driver::CLOSE_FD_TOKEN] is a special token which is used to signal close fd events.
    const CLOSE_FD_TOKEN: usize = 0x04;

    /// Create a new [`Driver`] with the provided size from the provided [`io_uring::Builder`].
    pub fn new(mut builder: io_uring::Builder, size: u32) -> io::Result<Self> {
        let ring = builder.dontfork().build(size)?;
        Ok(Self {
            shared: Rc::new(Shared {
                ring: RefCell::new(ring),
                backpressure: Notify::default(),
                status: Cell::new(Status::Running),
                submit_error: RefCell::new(None),
                shutdown_outcome: Cell::new(None),
                #[cfg(test)]
                cancel_all_error: Cell::new(None),
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
        let mut entries: [mem::MaybeUninit<cqueue::Entry>; N] =
            unsafe { mem::MaybeUninit::uninit().assume_init() };
        let mut total_drained = 0;
        loop {
            let (entries, has_more) = self.shared.drain_fill(&mut entries);
            let nr_drained = entries.len();
            for cqe in entries {
                let user_data = cqe.user_data() as usize;
                if user_data == Self::DRAIN_TOKEN {
                    trace!(target: LOG, "drain.token");
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
}

impl Park for Driver {
    type Unparker = Arc<unpark::Unparker>;

    type Guard = context::DriverContextGuard;

    fn park(&mut self, mut mode: ParkMode) -> Result<(), io::Error> {
        let drained = self.drain::<32>(usize::MAX);
        if drained > 0 {
            trace!(target: LOG, "park.drained {}", drained);
            mode = ParkMode::NoPark;
        }

        loop {
            match self.submit(mode) {
                Ok(_) => return Ok(()),
                Err(err) if err.raw_os_error() == Some(libc::EBUSY) => {
                    trace!(target: LOG, "park.ebusy");
                    let drained = self.drain::<32>(usize::MAX);
                    trace!(target: LOG, "park.drained {}", drained);
                    mode = ParkMode::NoPark;
                    continue;
                }
                Err(err) if err.raw_os_error() == Some(libc::EINTR) => {
                    error!(target: LOG, "park.eintr");
                    continue;
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn enter(&self) -> Self::Guard {
        context::DriverContext::enter(self.handle())
    }

    fn unparker(&self) -> Self::Unparker {
        Arc::clone(&self.unparker)
    }

    fn needs_park(&self) -> bool {
        self.shared.needs_park()
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
        if self.shared.status() == Status::Shutdown {
            return self.abandon();
        }
        loop {
            if let Some(outcome) = self.shared.shutdown_outcome() {
                return outcome;
            }
            if self.shared.status() == Status::Running {
                self.unparker.wake();
                if let Err(err) = self.shared.submit(ParkMode::NoPark) {
                    // Fail-soft shutdown path: if we can't submit during teardown,
                    // stop driving the ring instead of panicking in Drop.
                    // This may abandon in-flight work, but avoids use-after-free style
                    // teardown hazards by not forcing partially-failed transitions.
                    warn!(target: LOG, "shutdown.submit.failed {:?}", err);
                    return self.abandon();
                }
                if let Err(err) = self.shared.cancel_all() {
                    warn!(target: LOG, "shutdown.cancel_all.failed {:?}", err);
                    return self.abandon();
                }
                let opcode = io_uring::opcode::Nop::new()
                    .build()
                    .flags(io_uring::squeue::Flags::IO_DRAIN)
                    .user_data(Self::DRAIN_TOKEN as u64);
                if unsafe { self.shared.try_push_raw(&opcode) }.is_ok() {
                    self.shared.set_status(Status::Draining);
                } else {
                    let drained = self.drain::<32>(usize::MAX);
                    trace!(target: LOG, "shutdown.push_drain.retry drained={}", drained);
                }
            }
            if self.shared.status() == Status::Draining {
                if let Err(err) = self.park(ParkMode::NextCompletion) {
                    // Same fail-soft policy as above: prefer an explicit shutdown stop
                    // over panic while dropping the driver.
                    warn!(target: LOG, "shutdown.park.failed {:?}", err);
                    return self.abandon();
                }
            }
        }
    }

    /// Stop driving the ring without releasing memory the kernel may still access.
    ///
    /// The extra strong reference intentionally quarantines `Shared`, including the
    /// ring and registered storage. This is the bounded fallback for cancellation
    /// failure. A future ownership/reaper design (tracked separately) can replace
    /// this quarantine with deferred ring destruction and reclamation.
    fn abandon(&self) -> ShutdownOutcome {
        if self.shared.shutdown_outcome() == Some(ShutdownOutcome::Abandoned) {
            return ShutdownOutcome::Abandoned;
        }
        self.shared.finish_shutdown(ShutdownOutcome::Abandoned);
        mem::forget(Rc::clone(&self.shared));
        ShutdownOutcome::Abandoned
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
        self.set_status(Status::Shutdown);
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

    fn needs_park(&self) -> bool {
        // First check if there are any waiters, this is a cheap check
        // compared to checking the ring.
        if self.backpressure.waiters() > 0 {
            return true;
        }
        if NEEDS_PARK_CHECK_RINGS {
            let mut ring = self.ring.borrow_mut();
            let (_, sq, cq) = ring.split();
            sq.is_full() || cq.is_full()
        } else {
            false
        }
    }

    /// Drain the completion queue into the provided buffer.
    ///
    /// Returns the filled buffer, and a flag indicating if there are more entries after
    /// this buffer.
    fn drain_fill<'a, const N: usize>(
        &'a self,
        entries: &'a mut [mem::MaybeUninit<cqueue::Entry>; N],
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::task::Poll;
    use std::time::Duration;

    use smallvec::SmallVec;

    use super::*;
    use crate::operation::{CQEResult, Operation, Singleshot};
    use crate::Request;

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

    fn assert_cancel_failure_abandons(op: LongLivedOp) {
        let dropped = Arc::clone(&op.dropped);
        let unparker_buf_drops = Arc::new(AtomicUsize::new(0));
        let thread_unparker_buf_drops = Arc::clone(&unparker_buf_drops);
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
            driver.unparker_buf.track_drop(thread_unparker_buf_drops);
            let handle = driver.handle();
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

            driver.shared.cancel_all_error.set(Some(libc::EIO));
            let outcome = driver.shutdown_with_outcome();
            drop(op);
            drop(handle);

            let shared_refs = Rc::strong_count(&driver.shared);
            drop(driver);
            tx.send((outcome, shared_refs)).unwrap();
        });

        let (outcome, shared_refs) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("shutdown did not take the bounded abandonment path");
        assert_eq!(outcome, ShutdownOutcome::Abandoned);
        assert_eq!(
            shared_refs, 2,
            "abandonment must add exactly one quarantine reference"
        );
        assert!(
            !dropped.load(Ordering::Acquire),
            "kernel-referenced operation storage was released before ring destruction"
        );
        assert_eq!(
            unparker_buf_drops.load(Ordering::Acquire),
            0,
            "abandoned shutdown must quarantine the kernel-referenced unparker buffer"
        );
    }

    #[test]
    fn cancel_all_failure_abandons_long_lived_read() {
        let (reader, writer) = UnixStream::pair().unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        assert_cancel_failure_abandons(LongLivedOp {
            io: LongLivedIo::Read {
                reader,
                _writer: writer,
                buf: Box::new([0; 8]),
            },
            dropped,
        });
    }

    #[test]
    fn cancel_all_failure_abandons_long_lived_accept() {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        assert_cancel_failure_abandons(LongLivedOp {
            io: LongLivedIo::Accept(listener),
            dropped,
        });
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
