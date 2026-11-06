use std::any::Any;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;
use std::{io, mem};

use io_uring::squeue::{Flags, PushError};
use io_uring::types::{self, CancelBuilder, SubmitArgs, Timespec};
use io_uring::{cqueue, opcode, IoUring, Submitter};
use log::{debug, trace, warn};
use norn_executor::park::{Park, ParkMode};
use smallvec::SmallVec;

use crate::bufring::{
    BufRingStorage, Registration as RegisteredBufRing, Registry as RegisteredBufRings,
    ReserveError as ReserveBufRingError,
};
use crate::error::SubmitError;
use crate::operation::{reap_operation, AsyncCancel, ConfiguredEntry, Op, OpTarget, Operation};
use crate::registered_buffers::Registry as RegisteredBuffers;
pub(crate) use crate::registered_buffers::{
    Generation as FixedBufGeneration, Release as FixedBufRelease,
    ReleaseError as FixedBufReleaseError, ReserveError as ReserveFixedBufError,
    Retention as FixedBufRetention, UpdateError as FixedBufUpdateError,
};
use crate::util::notify::Notify;

#[cfg(test)]
use crate::registered_buffers::State as FixedBufState;

mod context;
mod unpark;

const LOG: &str = "norn_uring::driver";

/// Number of CQEs copied out of the ring at once while draining.
const COMPLETION_DRAIN_BATCH: usize = 32;

const DEFAULT_COMPLETION_DRAIN_BUDGET: NonZeroUsize = NonZeroUsize::new(32).unwrap();

/// Maximum time each shutdown cancellation attempt, and each wait for
/// outstanding requests to complete, waits for the kernel.
///
/// `IORING_REGISTER_SYNC_CANCEL` accepts a timeout, so teardown can retry without
/// entering one unbounded syscall when a request cannot be cancelled.
const SHUTDOWN_CANCEL_TIMEOUT: Duration = Duration::from_millis(100);

/// Options controlling normal [`Driver`] operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DriverOptions {
    /// Maximum number of completion queue entries handled by one normal park.
    ///
    /// The default is 32. Setting this to `None` drains all ready completions
    /// and can starve application tasks when completions arrive continuously.
    ///
    /// Shutdown always drains completions exhaustively, regardless of this
    /// option.
    pub completion_drain_budget: Option<NonZeroUsize>,
}

impl Default for DriverOptions {
    fn default() -> Self {
        Self {
            completion_drain_budget: Some(DEFAULT_COMPLETION_DRAIN_BUDGET),
        }
    }
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
/// retries unclassified cancellation errors and submission errors indefinitely.
/// While it waits, it cancels again after every bounded wait that completes
/// nothing, so a request that the kernel issues only after the first
/// cancellation, such as a linked dependent, is reclaimed too.
/// Kernels without synchronous cancellation proceed directly to the drain
/// barriers, which can still block forever behind an operation that cannot
/// complete without cancellation.
pub struct Driver {
    options: DriverOptions,
    shared: Rc<Shared>,
    unparker: Arc<unpark::Unparker>,
    /// Storage passed to the kernel for the eventfd read.
    ///
    /// [`Driver::drop`] releases the buffer only after shutdown proves every
    /// request, including the eventfd read, is terminal.
    unparker_buf: mem::ManuallyDrop<UnparkerBuffer>,
    /// CQEs copied out of the kernel ring but not dispatched because a wake
    /// callback unwound. This is empty on the normal path.
    deferred_cqes: RefCell<VecDeque<cqueue::Entry>>,
}

/// Tracks one batch copied from the kernel CQ.
///
/// Advancing the cursor before dispatch gives an unwinding destructor an exact
/// tail to preserve. The current entry has either reached its operation or is
/// a special token whose state transition was committed before any callback.
struct CqeBatch<'a> {
    entries: &'a [cqueue::Entry],
    next: usize,
    deferred: &'a RefCell<VecDeque<cqueue::Entry>>,
}

impl<'a> CqeBatch<'a> {
    fn new(entries: &'a [cqueue::Entry], deferred: &'a RefCell<VecDeque<cqueue::Entry>>) -> Self {
        Self {
            entries,
            next: 0,
            deferred,
        }
    }

    fn next(&mut self) -> Option<&'a cqueue::Entry> {
        let entry = self.entries.get(self.next)?;
        self.next += 1;
        Some(entry)
    }
}

impl Drop for CqeBatch<'_> {
    fn drop(&mut self) {
        if self.next < self.entries.len() {
            self.deferred
                .borrow_mut()
                .extend(self.entries[self.next..].iter().cloned());
        }
    }
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
    /// Entries that did not fit in the SQ, in submission order.
    ///
    /// The driver owns each entry once it is queued, so its operation counts as
    /// submitted. Parks move entries into the SQ before entering the kernel,
    /// and a linked group moves only whole. While this is non-empty, new
    /// entries queue behind it. Shutdown completes what remains with the
    /// shutdown error.
    overflow: RefCell<VecDeque<Queued>>,
    receive_shutdown: Notify,
    status: Cell<Status>,
    /// Cancellations of dropped operations that did not fit in the SQ.
    ///
    /// Each target retains its operation's identity until the cancellation is
    /// queued. Parks retry these after draining completions.
    deferred_cancels: RefCell<VecDeque<PendingCancel>>,
    /// Set once shutdown has requested cancellation of every outstanding request.
    cancel_all_requested: Cell<bool>,
    /// Linked requests whose first cancellation found nothing (ENOENT).
    ///
    /// A request linked behind another is invisible to cancellation until the
    /// kernel issues it, which happens in task work after its predecessor
    /// completes. Each entry is cancelled once more, after that point. Unlike
    /// `deferred_cancels`, these do not represent SQ demand.
    cancel_retries: RefCell<Vec<CancelRetry>>,
    submit_error: RefCell<Option<(io::ErrorKind, String)>>,
    shutdown_outcome: Cell<Option<ShutdownOutcome>>,
    /// Entries pushed to the SQ whose terminal CQE has not been dispatched.
    ///
    /// Operations (including cancellations), the eventfd read, and descriptor
    /// closes are counted. Each posts exactly one terminal CQE: custom
    /// operations cannot skip theirs (see `RESERVED_SQE_FLAGS`). Shutdown is
    /// complete once this reaches zero.
    in_flight: Cell<usize>,
    /// Set once shutdown finds the count untrusted: it wrapped, or the kernel
    /// dropped CQEs. Shutdown then proves reclamation with an `IO_DRAIN`
    /// barrier instead.
    accounting_broken: Cell<bool>,
    /// Whether that fallback barrier is in the SQ or the kernel, or has
    /// completed. Cleared only when it fails.
    drain_barrier_pushed: Cell<bool>,
    #[cfg(test)]
    shutdown_test: ShutdownTest,
    // These fields must be declared after `ring`: fields are dropped in declaration
    // order, so retained registered storage is released only after the ring.
    registered_buf_rings: RegisteredBufRings,
    registered_buffers: RegisteredBuffers,
}

#[cfg(test)]
#[derive(Default)]
struct ShutdownTest {
    cancel_all_failures: RefCell<std::collections::VecDeque<i32>>,
    cancel_all_attempts: Cell<usize>,
    drain_failures: Cell<usize>,
    drain_attempts: Cell<usize>,
    submit_failures: RefCell<std::collections::VecDeque<i32>>,
    submit_limits: RefCell<std::collections::VecDeque<(usize, usize)>>,
}

pub(crate) struct FixedBufReservation {
    shared: Rc<Shared>,
    generation: FixedBufGeneration,
    committed: bool,
}

pub(crate) struct BufRingReservation {
    shared: Rc<Shared>,
    registration: Rc<RegisteredBufRing>,
    committed: bool,
}

pub(crate) struct BufRingRegistration {
    shared: Rc<Shared>,
    registration: Rc<RegisteredBufRing>,
}

/// A cancellation waiting for SQ space.
struct PendingCancel {
    target: OpTarget,
    /// The request linked immediately before `target`, if any. Only a
    /// first cancellation of a linked request carries it; it allows one retry.
    predecessor: Option<OpTarget>,
}

/// One allowed retry of a linked request's cancellation that found nothing.
pub(crate) struct CancelRetry {
    shared: Weak<Shared>,
    target: Option<OpTarget>,
    predecessor: OpTarget,
}

impl CancelRetry {
    /// Queue the retry for `target` with a later park.
    pub(crate) fn schedule(mut self, target: OpTarget) {
        self.target = Some(target);
        if let Some(shared) = self.shared.upgrade() {
            shared.cancel_retries.borrow_mut().push(self);
        }
    }
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
    /// Every outstanding request has been cancelled, and the driver is waiting
    /// for each counted entry's terminal completion.
    Draining,
    /// The driver has shutdown and will not accept new requests.
    Shutdown,
}

/// Describes whether shutdown reached the normal reclamation boundary.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ShutdownOutcome {
    /// Every request the driver queued reached its terminal CQE.
    CleanDrained,
}

/// An entry waiting in [`Shared::overflow`] because the SQ was full.
struct Queued {
    entry: ConfiguredEntry,
    /// Whether the next queued entry is linked behind this one.
    linked: bool,
}

/// The length of the linked group at the front of the overflow queue.
fn overflow_group_len(overflow: &VecDeque<Queued>) -> Option<usize> {
    overflow
        .iter()
        .position(|queued| !queued.linked)
        .map(|last| last + 1)
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

impl BufRingReservation {
    pub(crate) fn register(
        &self,
        register: impl FnOnce(&Submitter<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        let ring = self.shared.ring.try_borrow().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring is already borrowed during buffer-ring registration",
            )
        })?;
        self.registration.arm_kernel_call();
        let result = register(&ring.submitter());
        if result.is_err() {
            self.registration.kernel_call_failed();
        }
        result
    }

    pub(crate) fn commit(mut self) -> BufRingRegistration {
        self.registration.commit();
        self.committed = true;
        BufRingRegistration {
            shared: Rc::clone(&self.shared),
            registration: Rc::clone(&self.registration),
        }
    }
}

impl Drop for BufRingReservation {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if let Err(err) = self
            .shared
            .registered_buf_rings
            .rollback(&self.shared.ring, &self.registration)
        {
            warn!(target: LOG, "buf_ring.registration_rollback_deferred {err:?}");
        }
    }
}

impl BufRingRegistration {
    pub(crate) fn is_running(&self) -> bool {
        self.shared.status() == Status::Running
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        use std::future::{poll_fn, Future};
        use std::pin::pin;
        let mut notified = pin!(self.shared.receive_shutdown.wait());
        poll_fn(|cx| {
            // Register before checking the condition, including reentrant wakeups.
            let _ = notified.as_mut().poll(cx);
            if self.is_running() {
                std::task::Poll::Pending
            } else {
                std::task::Poll::Ready(())
            }
        })
        .await
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        Rc::ptr_eq(&self.shared, &handle.shared)
    }

    /// Request cancellation of a receive on this ring without entering the
    /// kernel; see [`Handle::cancel_op`]. A receive is never a later member of
    /// a linked batch, so it has no predecessor.
    pub(crate) fn cancel_op(&self, target: OpTarget) {
        self.shared.cancel_op(target, None)
    }
}

impl Drop for BufRingRegistration {
    fn drop(&mut self) {
        if let Err(err) = self
            .shared
            .registered_buf_rings
            .release(&self.shared.ring, &self.registration)
        {
            warn!(target: LOG, "buf_ring.release_deferred {err:?}");
        }
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

    pub(crate) fn is_running(&self) -> bool {
        self.shared
            .upgrade()
            .is_some_and(|shared| shared.status() == Status::Running)
    }

    /// Replace one slot of this generation's fixed-buffer table.
    ///
    /// # Safety
    ///
    /// See [`RegisteredBuffers::update`].
    pub(crate) unsafe fn update_slot(
        &self,
        generation: FixedBufGeneration,
        index: u32,
        iovec: &libc::iovec,
    ) -> Result<(), FixedBufUpdateError> {
        let Some(shared) = self.shared.upgrade() else {
            return Err(FixedBufUpdateError::RingGone);
        };
        // Safety: forwarded to the caller.
        unsafe {
            shared
                .registered_buffers
                .update(&shared.ring, generation, index, iovec)
        }
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
    /// # Panics
    ///
    /// Panics if the current thread is not inside a [`Driver`] context.
    #[track_caller]
    pub fn current() -> Self {
        Self::try_current().expect("not in driver context")
    }

    /// Prepare an operation for submission.
    ///
    /// The returned [`Op`] is lazy: it is not configured and does not touch the
    /// submission queue until it is first polled.
    pub fn submit<T>(&self, op: T) -> Op<T>
    where
        T: Operation + 'static,
    {
        Op::new(op, self.clone())
    }

    /// Request asynchronous cancellation of a submitted operation.
    ///
    /// This never enters the kernel. The cancellation is queued in the SQ, or
    /// deferred in driver-owned state when the SQ is full, and is submitted
    /// by a later park.
    ///
    /// `predecessor` is the request linked immediately before `target`, if
    /// `target` was submitted as a later member of a linked batch.
    pub(crate) fn cancel_op(&self, target: OpTarget, predecessor: Option<OpTarget>) {
        self.shared.cancel_op(target, predecessor)
    }

    /// Hand an entry to the driver, which pushes it into the SQ or, when the
    /// SQ is full, queues it for parks to drain. Either way it owns the entry.
    pub(crate) fn submit_entry(&self, entry: ConfiguredEntry) -> Result<(), SubmitError> {
        if self.shared.status() != Status::Running {
            return Err(self.shared.closed_error());
        }
        self.shared.push_or_queue(entry);
        Ok(())
    }

    /// Hand a linked batch to the driver, which pushes or queues it whole.
    pub(crate) fn submit_batch(
        &self,
        entries: SmallVec<[ConfiguredEntry; 4]>,
    ) -> Result<(), SubmitError> {
        if self.shared.status() != Status::Running {
            return Err(self.shared.closed_error());
        }
        self.shared.validate_batch_len(entries.len())?;
        self.shared.push_or_queue_batch(entries);
        Ok(())
    }

    pub(crate) fn close_fd(&self, fd: types::Fd) -> Result<(), CloseFdError> {
        self.shared.close_fd(fd)
    }

    /// The number of entries waiting in the overflow queue.
    #[cfg(test)]
    pub(crate) fn queued_entries(&self) -> usize {
        self.shared.overflow.borrow().len()
    }

    /// Whether shutdown may have cancelled any submitted request.
    ///
    /// `cancel_all` cancels with `IORING_ASYNC_CANCEL_ANY`, which matches every
    /// submitted request and does not report which ones it hit, so this cannot
    /// say whether a particular request was cancelled. It is set before the
    /// cancel is issued and never cleared. After it is set, an `-ECANCELED`
    /// completion therefore cannot be told apart from one the operation
    /// produced on its own, and callers must treat it as "possibly cancelled".
    pub(crate) fn cancel_all_requested(&self) -> bool {
        self.shared.cancel_all_requested.get()
    }

    /// Whether the driver still accepts work.
    pub(crate) fn is_running(&self) -> bool {
        self.shared.status() == Status::Running
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

    pub(crate) fn reserve_buf_ring(
        &self,
        storage: Rc<BufRingStorage>,
    ) -> io::Result<BufRingReservation> {
        if self.shared.status() != Status::Running {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring driver is not accepting buffer-ring registrations",
            ));
        }
        let registration = self
            .shared
            .registered_buf_rings
            .reserve(&self.shared.ring, storage)
            .map_err(map_reserve_buf_ring_error)?;
        Ok(BufRingReservation {
            shared: Rc::clone(&self.shared),
            registration,
            committed: false,
        })
    }

    /// Run a synchronous call on the ring.
    ///
    /// Fails with [`io::ErrorKind::WouldBlock`] instead of panicking when the
    /// ring is already mutably borrowed by the driver.
    pub(crate) fn try_with_ring<U>(
        &self,
        f: impl FnOnce(&IoUring) -> io::Result<U>,
    ) -> io::Result<U> {
        let ring = self.shared.ring.try_borrow().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring is already borrowed by the driver",
            )
        })?;
        f(&ring)
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
    pub(crate) fn test_with_submitter<U>(&self, f: impl FnOnce(&Submitter<'_>) -> U) -> U {
        self.shared.with_submitter(f)
    }

    #[cfg(test)]
    pub(crate) fn test_fail_next_fixed_buffer_update(&self, errno: i32) {
        self.shared.registered_buffers.test_fail_next_update(errno);
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

    #[cfg(test)]
    pub(crate) fn test_with_buf_ring_registry_borrowed(&self, f: impl FnOnce()) {
        self.shared.registered_buf_rings.test_with_borrowed(f);
    }

    #[cfg(test)]
    pub(crate) fn test_registered_buf_rings(&self) -> usize {
        self.shared.registered_buf_rings.test_len()
    }

    #[cfg(test)]
    pub(crate) fn test_fail_next_buf_ring_release(&self, errno: i32) {
        self.shared
            .registered_buf_rings
            .test_fail_next_release(errno);
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
    /// Marks the `IO_DRAIN` barrier that proves reclamation when the in-flight
    /// count cannot be trusted.
    const DRAIN_TOKEN: usize = 0x01;

    /// [`Driver::UNPARKER_WAKE_TOKEN`] is a special token which is used to signal unparker wake events.
    const UNPARKER_WAKE_TOKEN: usize = 0x02;

    /// [`Driver::CANCELLATION_TOKEN`] is a special token which is used to signal cancellation events.
    const CANCELLATION_TOKEN: usize = 0x03;

    /// [`Driver::CLOSE_FD_TOKEN`] is a special token which is used to signal close fd events.
    const CLOSE_FD_TOKEN: usize = 0x04;

    /// Create a new [`Driver`] with the provided size from the provided [`io_uring::Builder`].
    ///
    /// This uses [`DriverOptions::default`]. Use
    /// [`Driver::new_with_options`] to customize driver behavior.
    ///
    /// # Errors
    ///
    /// Returns an error if the `io_uring` instance or its eventfd-based
    /// unparker cannot be created.
    pub fn new(builder: io_uring::Builder, size: u32) -> io::Result<Self> {
        Self::new_with_options(builder, size, DriverOptions::default())
    }

    /// Create a new [`Driver`] with the provided builder, size, and options.
    ///
    /// When the builder enables `COOP_TASKRUN` or `DEFER_TASKRUN`, the driver
    /// also enables `IORING_SETUP_TASKRUN_FLAG` so it can tell when deferred
    /// completions need an enter to be posted. The driver first tries the
    /// setup with that flag; the kernel rejects it with `EINVAL` for any other
    /// configuration (including the default builder and SQPOLL), and the ring
    /// is then built as configured. Such builders therefore make one extra,
    /// failing `io_uring_setup` call.
    ///
    /// A ring set up disabled (`IORING_SETUP_R_DISABLED`) is enabled, because
    /// nothing else can reach it to do so.
    ///
    /// # Errors
    ///
    /// Returns an error if the `io_uring` instance or its eventfd-based
    /// unparker cannot be created. It also returns `InvalidInput` if the
    /// builder sets `IORING_SETUP_IOPOLL` (such a ring runs only polled block
    /// I/O, while the driver needs timers, sockets, and an eventfd read to
    /// unpark), and `Unsupported` if the kernel lacks `IORING_FEAT_EXT_ARG`
    /// (Linux 5.11), which every waiting park uses.
    pub fn new_with_options(
        mut builder: io_uring::Builder,
        size: u32,
        options: DriverOptions,
    ) -> io::Result<Self> {
        builder.dontfork();
        // Request `IORING_SQ_TASKRUN` so a park can see deferred task work
        // (see `Shared::submit_once`). The kernel accepts the flag only with
        // `COOP_TASKRUN` or `DEFER_TASKRUN` and without `SQPOLL`; otherwise
        // this probe fails with EINVAL and the ring is built as configured.
        let ring = match builder.clone().setup_taskrun_flag().build(size) {
            Err(err) if err.raw_os_error() == Some(libc::EINVAL) => builder.build(size)?,
            ring => ring?,
        };
        Self::check_setup(&ring)?;
        Ok(Self {
            options,
            shared: Rc::new(Shared {
                ring: RefCell::new(ring),
                overflow: RefCell::new(VecDeque::new()),
                receive_shutdown: Notify::default(),
                status: Cell::new(Status::Running),
                deferred_cancels: RefCell::new(VecDeque::new()),
                cancel_all_requested: Cell::new(false),
                cancel_retries: RefCell::new(Vec::new()),
                submit_error: RefCell::new(None),
                shutdown_outcome: Cell::new(None),
                in_flight: Cell::new(0),
                accounting_broken: Cell::new(false),
                drain_barrier_pushed: Cell::new(false),
                #[cfg(test)]
                shutdown_test: ShutdownTest::default(),
                registered_buf_rings: RegisteredBufRings::new(),
                registered_buffers: RegisteredBuffers::new(),
            }),
            unparker: Arc::new(unpark::Unparker::new()?),
            unparker_buf: mem::ManuallyDrop::new(UnparkerBuffer::new()),
            deferred_cqes: RefCell::new(VecDeque::new()),
        })
    }

    /// Reject ring setups the driver cannot run with, and enable a disabled ring.
    fn check_setup(ring: &IoUring) -> io::Result<()> {
        if ring.params().is_setup_iopoll() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IORING_SETUP_IOPOLL rings run only polled block I/O; \
                 the driver needs timers, sockets, and an eventfd read",
            ));
        }
        if !ring.params().is_feature_ext_arg() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the kernel lacks IORING_FEAT_EXT_ARG (Linux 5.11), which parks use",
            ));
        }
        match ring.submitter().register_enable_rings() {
            // EBADFD: the ring was not set up disabled.
            Err(err) if err.raw_os_error() != Some(libc::EBADFD) => Err(err),
            _ => Ok(()),
        }
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
        let waiting = matches!(mode, ParkMode::Timeout(_) | ParkMode::NextCompletion);
        if waiting && self.shared.has_sq_demand() {
            // Work waiting for SQ space needs capacity, not a completion. Blocking
            // for a CQE could strand it behind long-lived operations, so submit,
            // wait only for SQ capacity, and refill the SQ from the overflow queue.
            let submitted = self.shared.submit(ParkMode::NoPark)?;
            self.shared.wait_for_sq_capacity()?;
            self.shared.flush_overflow();
            return Ok(submitted);
        }
        let mut submitted = 0;
        if waiting && self.shared.sqpoll_sq_full() {
            // The SQPOLL kernel thread has not yet consumed queued entries, so
            // there is no room to arm the unparker and the park would return
            // at once, turning the caller's wait into a spin. Wake the thread
            // and wait for a free slot first.
            submitted += self.shared.submit(ParkMode::NoPark)?;
            self.shared.wait_for_sq_capacity()?;
        }
        // If we're going to park, then prepare the unparker.
        if waiting && !self.prepare_park() {
            // Preparing the unparker failed, don't park!
            mode = ParkMode::NoPark;
        }
        trace!(target: LOG, "submit.mode {:?}", mode);
        submitted += self.shared.submit(mode)?;
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
            while total_drained < max {
                let entry = self.deferred_cqes.borrow_mut().pop_front();
                let Some(entry) = entry else {
                    break;
                };
                self.dispatch_cqe(&entry);
                total_drained += 1;
            }
            if total_drained >= max {
                break;
            }

            let remaining = max - total_drained;
            let batch_len = remaining.min(N);
            let (entries, has_more) = self.shared.drain_fill(&mut entries[..batch_len]);
            let nr_drained = entries.len();
            let mut batch = CqeBatch::new(entries, &self.deferred_cqes);
            while let Some(cqe) = batch.next() {
                self.dispatch_cqe(cqe);
            }
            total_drained += nr_drained;
            if !has_more || total_drained >= max {
                break;
            }
        }
        total_drained
    }

    fn dispatch_cqe(&self, cqe: &cqueue::Entry) {
        if cqe.user_data() <= 1024 {
            self.dispatch_token_cqe(cqe);
            return;
        }
        // Account before reaping: the CQE is consumed even if a callback unwinds.
        if !cqueue::more(cqe.flags()) {
            self.shared.complete_in_flight();
        }
        // Safety: This is being called on a completion queue entry which has been generated
        // by a prior submission.
        unsafe { reap_operation(cqe) }
    }

    /// Dispatch a CQE carrying one of the driver's reserved tokens.
    ///
    /// Kept out of line so that the operation path stays small.
    #[inline(never)]
    fn dispatch_token_cqe(&self, cqe: &cqueue::Entry) {
        let user_data = cqe.user_data() as usize;
        if user_data == Self::DRAIN_TOKEN {
            debug_assert_eq!(self.shared.status(), Status::Draining);
            if cqe.result() < 0 {
                let err = io::Error::from_raw_os_error(-cqe.result());
                warn!(target: LOG, "drain.failed {err:?}");
                self.shared.drain_barrier_pushed.set(false);
            } else {
                trace!(target: LOG, "drain.token");
                // Every earlier request is terminal, so the remaining deferred
                // cancellations and retries are moot.
                drop(self.shared.take_deferred_cancels());
                drop(self.shared.cancel_retries.take());
                self.shared.finish_shutdown(ShutdownOutcome::CleanDrained);
            }
            return;
        }
        if user_data == Self::UNPARKER_WAKE_TOKEN {
            trace!(target: LOG, "unparker.token");
            self.shared.complete_in_flight();
            self.unparker.reset();
            return;
        }

        if user_data == Self::CANCELLATION_TOKEN {
            // Raw filler entries pushed by tests; never counted.
            trace!(target: LOG, "cancellation.token");
            return;
        }

        if user_data == Self::CLOSE_FD_TOKEN {
            trace!(target: LOG, "close_fd.token");
            self.shared.complete_in_flight();
            return;
        }

        let result = cqe.result();
        let result = if result >= 0 {
            Ok(result as u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        };
        warn!(target: LOG, "drain.invalid_user_data {result:?}");
        // Surely nothing in our heap is going to be allocated at < 1024!
        // We are keeping this space reserved for additional operations.
    }

    fn retry_shutdown(&self, stage: &'static str, err: &io::Error) {
        warn!(target: LOG, "shutdown.{stage}.retry {err:?}");
        let drained = self.drain_exhaustive();
        trace!(target: LOG, "shutdown.{stage}.retry drained={drained}");
        if self.shared.status() != Status::Shutdown {
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    /// Push the `IO_DRAIN` barrier, uncounted: its CQE is not a request's.
    fn try_push_drain_barrier(&self) -> bool {
        let barrier = self.drain_barrier();
        let mut ring = self.shared.ring.borrow_mut();
        let mut sq = ring.submission();
        // Safety: the barrier references no memory.
        unsafe { sq.push(&barrier) }.is_ok()
    }

    fn drain_barrier(&self) -> io_uring::squeue::Entry {
        #[cfg(test)]
        let opcode = if self.shared.fail_drain_barrier() {
            opcode::Close::new(types::Fd(-1)).build()
        } else {
            opcode::Nop::new().build()
        };
        #[cfg(not(test))]
        let opcode = opcode::Nop::new().build();

        opcode
            .flags(io_uring::squeue::Flags::IO_DRAIN)
            .user_data(Self::DRAIN_TOKEN as u64)
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
        self.shared.retry_buf_ring_releases();
        let drained = self.drain_normal(&mut remaining);
        if drained > 0 {
            trace!(target: LOG, "park.drained {}", drained);
            mode = ParkMode::NoPark;
        }
        self.shared.retry_cancels();
        self.shared.flush_deferred_cancels();
        self.shared.flush_overflow();

        loop {
            match self.submit(mode) {
                Ok(_) => return Ok(()),
                Err(err) if Shared::is_transient_submit_error(&err) => {
                    trace!(target: LOG, "park.transient {err}");
                    let drained = self.drain_normal(&mut remaining);
                    trace!(target: LOG, "park.drained {}", drained);
                    let ebusy = err.raw_os_error() == Some(libc::EBUSY);
                    if (remaining.is_some() || !ebusy) && (drained == 0 || remaining == Some(0)) {
                        // Yield to runnable work. The next park call will retry
                        // any submission that remains queued. Allocation
                        // failures yield here too rather than spin on enter.
                        return Ok(());
                    }
                    mode = ParkMode::NoPark;
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

    fn enter_for_shutdown(&self) -> Self::Guard {
        context::DriverContext::enter_for_shutdown(self.handle())
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
        if matches!(self.shared.status(), Status::Running | Status::Shutdown) {
            self.shared.set_status(Status::Closing);
        }

        // Once an SQE may have reached the kernel, returning before its terminal CQE
        // is unsafe: the operation can retain buffers, descriptors, and this driver.
        // Teardown therefore retries every cancellation error that does not prove
        // the operation unsupported, then waits until no counted entry is in
        // flight. If synchronous cancellation is unsupported, that wait can block
        // forever, but shutdown cannot abandon memory still owned by the kernel.
        loop {
            if let Some(outcome) = self.shared.shutdown_outcome() {
                return outcome;
            }
            match self.shared.status() {
                Status::Shutdown => self.shared.set_status(Status::Closing),
                Status::Running => self.shared.set_status(Status::Closing),
                Status::Closing => {
                    self.unparker.wake();
                    // Queued entries never reached the kernel; fail them as before.
                    self.shared.fail_overflow();
                    // Queue deferred cancellations before the synchronous cancel so
                    // they still apply on kernels without synchronous cancellation.
                    self.shared.flush_deferred_cancels();
                    if let Err(err) = self.shared.submit_all_pending() {
                        self.retry_shutdown("submit", &err);
                        continue;
                    }
                    if self.shared.has_deferred_cancels() {
                        continue;
                    }
                    // Issue linked dependents whose heads have already completed, so
                    // the synchronous cancel can find them. Under DEFER_TASKRUN only a
                    // GETEVENTS enter runs that task work.
                    self.shared.run_all_task_work();

                    if let Err(err) = self.shared.cancel_all() {
                        if Self::sync_cancel_unsupported(&err) {
                            // Without synchronous cancellation, draining waits for each
                            // outstanding request to finish on its own, and the
                            // asynchronous cancellations queued by drops still apply.
                            warn!(target: LOG, "shutdown.cancel_all.unsupported {err:?}");
                        } else if err.raw_os_error() != Some(libc::ENOENT) {
                            self.retry_shutdown("cancel_all", &err);
                            continue;
                        }
                    }
                    self.shared.set_status(Status::Draining);
                }
                Status::Draining => self.drain_for_shutdown(),
            }
        }
    }

    /// One step of waiting for every counted entry's terminal completion.
    fn drain_for_shutdown(&mut self) {
        if self.shared.accounting_broken() {
            self.drain_behind_barrier();
            return;
        }
        if self.drain_exhaustive() > 0 {
            return;
        }
        if self.shared.in_flight() == 0 {
            // Every admitted operation is terminal, so remaining deferred
            // cancellations and retries are moot. Dropping their targets may
            // destroy operations, and that may queue more counted work, such
            // as a cancellation, so look again before finishing.
            drop(self.shared.take_deferred_cancels());
            drop(self.shared.cancel_retries.take());
            if self.shared.in_flight() == 0 {
                trace!(target: LOG, "shutdown.drained");
                self.shared.finish_shutdown(ShutdownOutcome::CleanDrained);
            }
            return;
        }
        // A head can still complete after the synchronous cancel, and its linked
        // dependent is issued only then. Wait for a bounded time, and cancel again
        // if nothing completed.
        if let Err(err) =
            self.park_with_completion_budget(ParkMode::Timeout(SHUTDOWN_CANCEL_TIMEOUT), None)
        {
            self.retry_shutdown("park", &err);
            return;
        }
        if self.drain_exhaustive() == 0 && self.shared.in_flight() > 0 {
            // Like the first cancel, this matches every request. That includes a
            // drop-close (`CLOSE_FD_TOKEN`) still queued in io-wq, which only
            // happens for files whose close flushes (FUSE, NFS); cancelling one
            // leaks its descriptor. This runs only after a wait that completed
            // nothing, and a signal can cut that wait short: a timed park
            // returns early on `EINTR`.
            debug!(
                target: LOG,
                "shutdown.recancel in_flight={}",
                self.shared.in_flight()
            );
            if let Err(err) = self.shared.cancel_all() {
                trace!(target: LOG, "shutdown.recancel {err:?}");
            }
        }
    }

    /// Wait for an `IO_DRAIN` barrier instead of the count.
    ///
    /// The barrier completes once every earlier SQE has completed in the
    /// kernel, whether or not its CQE reached userspace, and no later SQE starts
    /// before it completes. Cancellations are not queued behind it (see
    /// `Shared::try_push_cancel`), and nothing else is pushed during shutdown,
    /// so its CQE proves the kernel holds nothing. An operation whose CQE was
    /// lost is never reaped, so its allocation leaks rather than being freed
    /// while the kernel might still use it.
    fn drain_behind_barrier(&mut self) {
        if !self.shared.drain_barrier_pushed.get() {
            if let Err(err) = self.shared.submit_all_pending() {
                self.retry_shutdown("submit_barrier", &err);
                return;
            }
            if self.try_push_drain_barrier() {
                warn!(target: LOG, "shutdown.drain_barrier");
                self.shared.drain_barrier_pushed.set(true);
            } else if self.drain_exhaustive() == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
            return;
        }
        if let Err(err) =
            self.park_with_completion_budget(ParkMode::Timeout(SHUTDOWN_CANCEL_TIMEOUT), None)
        {
            self.retry_shutdown("park_barrier", &err);
            return;
        }
        // The barrier also waits behind requests issued after the synchronous
        // cancel, so cancel again whenever a bounded wait completes nothing, as
        // `drain_for_shutdown` does.
        if self.drain_exhaustive() == 0 && self.shared.status() == Status::Draining {
            if let Err(err) = self.shared.cancel_all() {
                trace!(target: LOG, "shutdown.recancel_barrier {err:?}");
            }
        }
    }

    fn sync_cancel_unsupported(err: &io::Error) -> bool {
        matches!(
            err.raw_os_error(),
            Some(libc::EINVAL) | Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS)
        )
    }
}

impl Shared {
    fn retry_buf_ring_releases(&self) {
        if let Err(err) = self.registered_buf_rings.retry_deferred(&self.ring) {
            warn!(target: LOG, "buf_ring.release_retry_failed {err:?}");
        }
    }

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
        !matches!(err.raw_os_error(), Some(libc::EINTR)) && !Self::is_transient_submit_error(err)
    }

    /// Returns true for `io_uring_enter` failures that leave the ring usable.
    ///
    /// EBUSY reports a full CQ; EAGAIN and ENOMEM report that the kernel could
    /// not allocate request memory. In each case the caller should reap
    /// completions and retry rather than retire the driver.
    fn is_transient_submit_error(err: &io::Error) -> bool {
        matches!(
            err.raw_os_error(),
            Some(libc::EBUSY | libc::EAGAIN | libc::ENOMEM)
        )
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
        let previous = self.status.replace(status);
        if status != previous {
            // Commit the state before callbacks. A waker may panic, but the
            // CQE that caused this transition has already been consumed and
            // subsequent driver work must observe its new stage.
            self.receive_shutdown.notify(usize::MAX);
        }
    }

    /// The error for a request the driver can no longer accept.
    #[cold]
    fn closed_error(&self) -> SubmitError {
        match self.health_error() {
            Some(err) => SubmitError::broken(err),
            None => SubmitError::shutting_down(),
        }
    }

    /// Push `entry` into the SQ, or queue it behind the entries waiting for room.
    #[inline]
    fn push_or_queue(&self, entry: ConfiguredEntry) {
        if self.overflow.borrow().is_empty() {
            let mut ring = self.ring.borrow_mut();
            let mut sq = ring.submission();
            if !sq.is_full() {
                let entry = entry.into_entry_with_flags(Flags::empty());
                // Safety: the entry carries its operation's kernel reference,
                // which keeps what it points to alive until its terminal CQE.
                unsafe { sq.push(&entry) }.unwrap();
                self.in_flight.set(self.in_flight.get() + 1);
                return;
            }
        }
        self.queue(entry);
    }

    #[cold]
    #[inline(never)]
    fn queue(&self, entry: ConfiguredEntry) {
        entry.mark_queued();
        self.overflow.borrow_mut().push_back(Queued {
            entry,
            linked: false,
        });
    }

    /// Push a linked batch into the SQ whole, or queue it whole.
    fn push_or_queue_batch(&self, mut entries: SmallVec<[ConfiguredEntry; 4]>) {
        if self.overflow.borrow().is_empty() && self.try_push_batch(&mut entries) {
            return;
        }
        let len = entries.len();
        let mut overflow = self.overflow.borrow_mut();
        for (idx, entry) in entries.into_iter().enumerate() {
            entry.mark_queued();
            overflow.push_back(Queued {
                entry,
                linked: idx + 1 < len,
            });
        }
    }

    /// Move queued entries into the SQ, in order, while they fit.
    ///
    /// A group in which an operation was cancelled while it waited completes
    /// with `ECANCELED` without reaching the kernel. A group's members are
    /// dropped together, so all of them were cancelled, including any behind
    /// a hard link. Once the driver stops accepting work, queued entries fail
    /// with the shutdown error instead.
    fn flush_overflow(&self) {
        loop {
            if self.overflow.borrow().is_empty() {
                return;
            }
            if self.status() != Status::Running {
                self.fail_overflow();
                return;
            }
            let mut cancelled = SmallVec::<[ConfiguredEntry; 4]>::new();
            {
                let mut overflow = self.overflow.borrow_mut();
                let mut ring = self.ring.borrow_mut();
                let mut sq = ring.submission();
                while let Some(len) = overflow_group_len(&overflow) {
                    if overflow
                        .iter()
                        .take(len)
                        .any(|queued| queued.entry.cancelled_while_queued())
                    {
                        cancelled.extend(overflow.drain(..len).map(|queued| queued.entry));
                        continue;
                    }
                    if sq.capacity() - sq.len() < len {
                        break;
                    }
                    for queued in overflow.drain(..len) {
                        queued.entry.mark_dequeued();
                        let flags = if queued.linked {
                            queued.entry.link_flags()
                        } else {
                            Flags::empty()
                        };
                        let entry = queued.entry.into_entry_with_flags(flags);
                        // Safety: as for `push_or_queue`.
                        unsafe { sq.push(&entry) }.unwrap();
                    }
                    self.in_flight.set(self.in_flight.get() + len);
                }
            }
            if cancelled.is_empty() {
                return;
            }
            // Completing runs the operations' callbacks and destructors, so do it
            // without holding the queue or the ring.
            for entry in cancelled {
                entry.complete_unsubmitted(io::Error::from_raw_os_error(libc::ECANCELED));
            }
        }
    }

    /// Complete every queued entry with the shutdown error. None of them
    /// reached the kernel.
    fn fail_overflow(&self) {
        loop {
            let Some(queued) = self.overflow.borrow_mut().pop_front() else {
                return;
            };
            queued
                .entry
                .complete_unsubmitted(self.closed_error().to_io_error());
        }
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
            self.in_flight.set(self.in_flight.get() + 1);
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
            self.in_flight.set(self.in_flight.get() + 1);
            return true;
        }

        let len = entries.len();
        let mut raw_entries = SmallVec::<[io_uring::squeue::Entry; 4]>::with_capacity(len);
        for (idx, entry) in std::mem::take(entries).into_iter().enumerate() {
            let flags = if idx + 1 == len {
                Flags::empty()
            } else {
                entry.link_flags()
            };
            raw_entries.push(entry.into_entry_with_flags(flags));
        }
        unsafe { sq.push_multiple(raw_entries.as_slice()) }.unwrap();
        self.in_flight.set(self.in_flight.get() + len);
        true
    }

    /// Attempt to push a new raw entry into the submission queue, and count it
    /// in flight.
    ///
    /// The entry must post exactly one terminal CQE. Only the drain barrier is
    /// pushed uncounted. If the submission queue is full, this will return an
    /// error.
    unsafe fn try_push_raw(&self, entry: &io_uring::squeue::Entry) -> Result<(), PushError> {
        let mut ring = self.ring.borrow_mut();
        let mut sq = ring.submission();
        sq.push(entry)?;
        self.in_flight.set(self.in_flight.get() + 1);
        Ok(())
    }

    fn in_flight(&self) -> usize {
        self.in_flight.get()
    }

    /// Account for one counted entry's terminal CQE.
    ///
    /// This runs for every completion, so release builds do not check for
    /// underflow. A terminal CQE the count never covered wraps it, which
    /// [`Shared::accounting_broken`] reports unless later pushes wrap it back.
    #[inline]
    fn complete_in_flight(&self) {
        let count = self.in_flight.get();
        debug_assert!(
            count > 0,
            "a terminal CQE the in-flight count never covered"
        );
        self.in_flight.set(count.wrapping_sub(1));
    }

    /// Whether the in-flight count can no longer prove reclamation.
    ///
    /// The count wraps if it missed an entry, and an entry whose CQE the kernel
    /// dropped (it failed to allocate an overflow entry) stays counted forever.
    fn accounting_broken(&self) -> bool {
        if !self.accounting_broken.get() {
            let wrapped = self.in_flight.get() > isize::MAX as usize;
            let dropped = self.ring.borrow_mut().completion().overflow();
            if wrapped || dropped > 0 {
                warn!(target: LOG, "in_flight.untrusted wrapped={wrapped} cqes_dropped={dropped}");
                self.accounting_broken.set(true);
            }
        }
        self.accounting_broken.get()
    }

    fn with_submitter<U>(&self, f: impl FnOnce(&Submitter<'_>) -> U) -> U {
        let ring = self.ring.borrow();
        let sq = ring.submitter();
        f(&sq)
    }

    fn submit_once(&self, mode: ParkMode) -> io::Result<usize> {
        #[cfg(test)]
        if let Some(errno) = self.shutdown_test.submit_failures.borrow_mut().pop_front() {
            return Err(io::Error::from_raw_os_error(errno));
        }

        #[cfg(test)]
        if let Some((limit, expected_remaining)) =
            self.shutdown_test.submit_limits.borrow_mut().pop_front()
        {
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
                let ts = crate::util::timespec(duration);
                let args = SubmitArgs::new().timespec(&ts);
                ring.submitter().submit_with_args(1, &args)?
            }
            ParkMode::NextCompletion => {
                let args = SubmitArgs::new();
                ring.submitter().submit_with_args(1, &args)?
            }
            ParkMode::NoPark => {
                let sq = ring.submission();
                // The kernel may hold completions outside the CQ ring: in its
                // overflow list, or in task work that has not run yet. Only an
                // enter with GETEVENTS posts them, so a park that skips waiting
                // must still make one; `Submitter::submit` adds GETEVENTS when
                // either flag is set. Without either flag, an empty SQ needs no
                // syscall.
                let kernel_pending = sq.cq_overflow() || sq.taskrun();
                let empty = sq.is_empty();
                drop(sq);
                if kernel_pending || !empty {
                    ring.submitter().submit()?
                } else {
                    0
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
                Ok(submitted) => return Ok(submitted),
                Err(err)
                    if matches!(mode, ParkMode::Timeout(_))
                        && err.raw_os_error() == Some(libc::ETIME) =>
                {
                    // A timed CQ wait expired normally. ETIME in other submit
                    // modes remains an unexpected driver failure.
                    return Ok(0);
                }
                Err(err)
                    if matches!(mode, ParkMode::Timeout(_))
                        && err.raw_os_error() == Some(libc::EINTR) =>
                {
                    // A signal interrupted the timed wait. Retrying would
                    // restart the full timeout on every signal, so a steady
                    // signal stream could postpone the park indefinitely. A
                    // timed park may return early; the caller re-checks its
                    // deadlines and parks again with the remaining time.
                    trace!(target: LOG, "submit.eintr timed park returns early");
                    return Ok(0);
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

    /// With SQPOLL, wait until the kernel thread frees SQ space.
    ///
    /// The caller must already have submitted, which wakes an idle kernel
    /// thread. Without SQPOLL a submit consumes the SQ synchronously, so this
    /// returns immediately.
    fn wait_for_sq_capacity(&self) -> io::Result<()> {
        if !self.sqpoll_sq_full() {
            return Ok(());
        }
        let ring = self.ring.borrow_mut();
        match ring.submitter().squeue_wait() {
            Ok(_) => Ok(()),
            Err(err) if err.raw_os_error() == Some(libc::EINTR) => Ok(()),
            Err(err) => Err(err),
        }
    }

    /// Returns true when an SQPOLL ring's SQ has no free slot, because the
    /// kernel thread has not yet consumed the queued entries.
    fn sqpoll_sq_full(&self) -> bool {
        let mut ring = self.ring.borrow_mut();
        ring.params().is_setup_sqpoll() && ring.submission().is_full()
    }

    /// Returns true when queued work is waiting for SQ space.
    fn has_sq_demand(&self) -> bool {
        !self.overflow.borrow().is_empty() || self.has_deferred_cancels()
    }

    fn has_deferred_cancels(&self) -> bool {
        !self.deferred_cancels.borrow().is_empty()
    }

    fn take_deferred_cancels(&self) -> VecDeque<PendingCancel> {
        mem::take(&mut *self.deferred_cancels.borrow_mut())
    }

    /// Request asynchronous cancellation of `target` without entering the kernel.
    ///
    /// The cancellation SQE follows the target's SQE in the queue, so it cannot
    /// reach the kernel first. If the SQ is full, the target is retained in
    /// driver-owned state and retried by later parks.
    ///
    /// `predecessor` is the request linked immediately before `target`. If the
    /// cancellation finds nothing because the kernel has not yet issued
    /// `target`, it is retried once after the issue must have happened.
    fn cancel_op(self: &Rc<Self>, target: OpTarget, predecessor: Option<OpTarget>) {
        // A target still in the overflow queue is cancelled there, so it never
        // reaches the kernel.
        if target.is_complete() || target.cancel_if_queued() {
            return;
        }
        let cancel = PendingCancel {
            target,
            predecessor,
        };
        if self.has_deferred_cancels() {
            self.deferred_cancels.borrow_mut().push_back(cancel);
            return;
        }
        if let Err(cancel) = self.try_push_cancel(cancel) {
            trace!(target: LOG, "cancel.deferred");
            self.deferred_cancels.borrow_mut().push_back(cancel);
        }
    }

    fn try_push_cancel(self: &Rc<Self>, cancel: PendingCancel) -> Result<(), PendingCancel> {
        if self.drain_barrier_pushed.get() {
            // A cancellation behind the drain barrier could not start until
            // every request it could target is terminal. Dropping it keeps the
            // barrier the last entry the kernel holds.
            return Ok(());
        }
        if self.ring.borrow_mut().submission().is_full() {
            return Err(cancel);
        }
        let retry = cancel.predecessor.clone().map(|predecessor| CancelRetry {
            shared: Rc::downgrade(self),
            target: None,
            predecessor,
        });
        let entry = AsyncCancel::entry(cancel.target.clone(), retry);
        match self.try_push(entry) {
            Ok(()) => Ok(()),
            Err(entry) => {
                drop(entry);
                Err(cancel)
            }
        }
    }

    /// Queue as many deferred cancellations as the SQ can hold.
    fn flush_deferred_cancels(self: &Rc<Self>) {
        loop {
            let Some(cancel) = self.deferred_cancels.borrow_mut().pop_front() else {
                return;
            };
            if cancel.target.is_complete() {
                // Dropping a completed target may destroy its operation, so do it
                // without holding the deferred queue.
                drop(cancel);
                continue;
            }
            if let Err(cancel) = self.try_push_cancel(cancel) {
                self.deferred_cancels.borrow_mut().push_front(cancel);
                return;
            }
        }
    }

    /// Issue each due retry of a linked request's cancellation.
    ///
    /// A linked request is issued from task work queued when its predecessor
    /// completes. A retry is therefore due once the predecessor's terminal CQE
    /// has been reaped. Before sending it, the driver runs pending task work
    /// with a non-blocking GETEVENTS enter, so the retry reaches the kernel
    /// after the target was issued; a second ENOENT then means the target is
    /// issued but not cancellable, and there is no further retry.
    ///
    /// Under SQPOLL, the kernel's SQ thread may run that task work instead of
    /// the enter, so the retry can still arrive before the target is issued.
    /// The target then runs until it completes on its own or shutdown cancels
    /// it. This is suspected from the kernel source, not reproduced.
    ///
    /// Retries run only while the driver accepts work. Shutdown reclaims the
    /// remaining targets through synchronous cancellation and its re-cancels
    /// instead.
    #[inline]
    fn retry_cancels(self: &Rc<Self>) {
        if self.cancel_retries.borrow().is_empty() {
            return;
        }
        self.retry_cancels_slow();
    }

    #[cold]
    #[inline(never)]
    fn retry_cancels_slow(self: &Rc<Self>) {
        let retries = self.cancel_retries.take();
        if self.status() != Status::Running {
            // Dropping a target may destroy its operation, so do it without
            // holding the retry queue.
            drop(retries);
            return;
        }
        let mut waiting = Vec::new();
        let mut due = Vec::new();
        for retry in retries {
            let target = retry.target.as_ref().expect("scheduled retry has a target");
            if target.is_complete() {
                continue;
            }
            if retry.predecessor.is_complete() {
                due.push(retry);
            } else {
                waiting.push(retry);
            }
        }
        if !due.is_empty() {
            if self.run_task_work() {
                for mut retry in due {
                    let target = retry.target.take().expect("scheduled retry has a target");
                    self.cancel_op(target, None);
                }
            } else {
                // Task work is still pending; the next park tries again.
                waiting.extend(due);
            }
        }
        if !waiting.is_empty() {
            self.cancel_retries.borrow_mut().extend(waiting);
        }
    }

    /// Run pending task work without waiting for completions.
    ///
    /// Returns true when the enter succeeded and the kernel reports no task
    /// work still pending. The kernel reports pending work (`IORING_SQ_TASKRUN`)
    /// only for rings set up with `IORING_SETUP_TASKRUN_FLAG`; on other rings a
    /// successful enter is taken to have run it all.
    fn run_task_work(&self) -> bool {
        const IORING_ENTER_GETEVENTS: u32 = 1;
        let mut ring = self.ring.borrow_mut();
        // Safety: the enter submits nothing, waits for nothing and passes no
        // argument; with GETEVENTS it only runs task work and flushes the CQ.
        let entered = unsafe {
            ring.submitter()
                .enter::<libc::sigset_t>(0, 0, IORING_ENTER_GETEVENTS, None)
        };
        match entered {
            Ok(_) => !ring.submission().taskrun(),
            Err(err) => {
                trace!(target: LOG, "cancel.retry.run_task_work {err}");
                false
            }
        }
    }

    /// Queue a close of `fd` without waiting for its completion.
    ///
    /// Once the driver has begun shutting down this refuses with
    /// [`CloseFdError::NeverQueued`], so the caller closes the descriptor
    /// directly. A close queued during shutdown is exposed to three hazards:
    /// the fallback `IO_DRAIN` barrier can make the kernel defer it until ring
    /// teardown, which cancels it; the synchronous `ANY` cancel and the
    /// re-cancels that follow it can cancel a close that io-wq has not started,
    /// and there is no "all except closes" form; and nothing reaps it. Any of
    /// these leaves the descriptor open forever.
    ///
    /// A direct close is safe in every phase: this runs when the last
    /// reference to the descriptor is dropped, and every operation that uses it
    /// holds a reference until its terminal completion is reaped, so no request
    /// can still be using it. Closes queued before shutdown began are not
    /// covered. Any other code that submits to a shutting-down ring without
    /// waiting for the result has the same hazards.
    fn close_fd(&self, fd: types::Fd) -> Result<(), CloseFdError> {
        if self.shutdown_outcome().is_some() || self.status() != Status::Running {
            return Err(CloseFdError::NeverQueued(io::Error::other(
                "driver is shutting down",
            )));
        }
        let entry = opcode::Close::new(fd)
            .build()
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

    /// Run deferred task work until the kernel reports none left.
    ///
    /// The kernel issues a linked dependent from task work once its head
    /// completes, and an enter that waits for nothing runs a bounded batch of
    /// that work (20 items in 6.18). Without `IORING_SETUP_TASKRUN_FLAG` one
    /// enter is taken to have run it all, and the bounded re-cancel while
    /// draining covers what is left.
    fn run_all_task_work(&self) {
        const MAX_ENTERS: usize = 64;
        for _ in 0..MAX_ENTERS {
            if self.run_task_work() {
                return;
            }
        }
    }

    /// Cancel all outstanding requests synchronously.
    ///
    /// This matches with `CancelBuilder::any()`, not by descriptor or
    /// `user_data`: every submitted request is a candidate. It sets
    /// `cancel_all_requested`; see `Handle::cancel_all_requested` for what that
    /// does and does not establish.
    pub(crate) fn cancel_all(&self) -> io::Result<()> {
        self.cancel_all_requested.set(true);
        #[cfg(test)]
        {
            self.shutdown_test
                .cancel_all_attempts
                .set(self.shutdown_test.cancel_all_attempts.get() + 1);
            if let Some(errno) = self
                .shutdown_test
                .cancel_all_failures
                .borrow_mut()
                .pop_front()
            {
                return Err(io::Error::from_raw_os_error(errno));
            }
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
    fn fail_next_cancel_all(&self, errno: i32) {
        self.shutdown_test
            .cancel_all_failures
            .borrow_mut()
            .push_back(errno);
    }

    #[cfg(test)]
    fn fail_next_drain_barrier(&self) {
        let failures = &self.shutdown_test.drain_failures;
        failures.set(failures.get() + 1);
    }

    #[cfg(test)]
    fn fail_drain_barrier(&self) -> bool {
        let attempts = &self.shutdown_test.drain_attempts;
        attempts.set(attempts.get() + 1);
        let failures = &self.shutdown_test.drain_failures;
        let fail = failures.get() > 0;
        if fail {
            failures.set(failures.get() - 1);
        }
        fail
    }

    #[cfg(test)]
    fn fail_next_submit(&self, errno: i32) {
        self.shutdown_test
            .submit_failures
            .borrow_mut()
            .push_back(errno);
    }

    #[cfg(test)]
    fn limit_next_submit(&self, limit: usize, expected_remaining: usize) {
        assert!(limit > 0);
        self.shutdown_test
            .submit_limits
            .borrow_mut()
            .push_back((limit, expected_remaining));
    }

    fn needs_park(&self) -> bool {
        // A queued entry or deferred cancellation proves that work observed a
        // full SQ and needs the driver to submit queued work. The executor task quantum
        // bounds all other driver work without inspecting the shared SQ/CQ
        // indices after every task poll.
        self.has_sq_demand()
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

fn map_reserve_buf_ring_error(err: ReserveBufRingError) -> io::Error {
    match err {
        ReserveBufRingError::RegistryBorrowed => io::Error::new(
            io::ErrorKind::WouldBlock,
            "buffer-ring registration registry is already borrowed",
        ),
        ReserveBufRingError::DuplicateBgid => io::Error::new(
            io::ErrorKind::AlreadyExists,
            "buffer group id is already registered with this driver",
        ),
        ReserveBufRingError::OutOfMemory => io::Error::new(
            io::ErrorKind::OutOfMemory,
            "unable to retain buffer-ring registration storage",
        ),
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        let outcome = self.shutdown_with_outcome();
        if outcome == ShutdownOutcome::CleanDrained {
            // Safety: a clean shutdown proves the kernel has finished every
            // request: each counted entry reached its terminal CQE, or the
            // fallback drain barrier completed behind them. Eventfd reads are
            // counted and are not armed during shutdown, so none can still
            // reference this buffer. Driver is dropping, so this is the
            // buffer's only explicit release.
            unsafe { mem::ManuallyDrop::drop(&mut self.unparker_buf) };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::net::TcpListener;
    use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
    use std::os::unix::net::UnixStream;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::pin::pin;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{mpsc, Arc};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    use smallvec::SmallVec;

    use super::*;
    use crate::fd;
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

    enum LongLivedCompletion {
        Read(io::Result<u32>),
        Accept(io::Result<OwnedFd>),
    }

    impl Drop for LongLivedOp {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    unsafe impl Operation for LongLivedOp {
        type Completion = LongLivedCompletion;

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

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            match &self.io {
                LongLivedIo::Read { .. } => LongLivedCompletion::Read(result.into_result()),
                LongLivedIo::Accept(_) => {
                    LongLivedCompletion::Accept(result.into_result().map(|fd| {
                        // Safety: a successful accept CQE transfers ownership of this descriptor.
                        unsafe { OwnedFd::from_raw_fd(fd as i32) }
                    }))
                }
            }
        }
    }

    impl Singleshot for LongLivedOp {
        type Output = io::Result<u32>;

        fn complete(self, completion: Self::Completion) -> Self::Output {
            match completion {
                LongLivedCompletion::Read(result) => result,
                LongLivedCompletion::Accept(result) => result.map(|fd| fd.into_raw_fd() as u32),
            }
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
            driver.shared.fail_next_cancel_all(libc::EIO);
            let outcome = driver.shutdown_with_outcome();
            let cancel_all_attempts = driver.shared.shutdown_test.cancel_all_attempts.get();
            let drain_attempts = driver.shared.shutdown_test.drain_attempts.get();
            let shared_refs = Rc::strong_count(&driver.shared);
            drop(driver);
            tx.send((outcome, cancel_all_attempts, drain_attempts, shared_refs))
                .unwrap();
        });

        let (outcome, cancel_all_attempts, drain_attempts, shared_refs) = rx
            .recv_timeout(Duration::from_secs(2))
            .expect("shutdown did not recover from the injected cancellation failure");
        assert_eq!(outcome, ShutdownOutcome::CleanDrained);
        assert_eq!(
            cancel_all_attempts, 2,
            "a transient cancellation failure must be retried before draining"
        );
        assert_eq!(
            drain_attempts, 0,
            "the in-flight count proves reclamation without a drain barrier"
        );
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

    fn shutdown_with_cancel_all_failures(errors: &[i32]) -> (ShutdownOutcome, usize) {
        let errors = errors.to_vec();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
            for errno in errors {
                driver.shared.fail_next_cancel_all(errno);
            }
            let outcome = driver.shutdown_with_outcome();
            tx.send((
                outcome,
                driver.shared.shutdown_test.cancel_all_attempts.get(),
            ))
            .unwrap();
        });
        rx.recv_timeout(Duration::from_secs(2))
            .expect("shutdown did not drain")
    }

    #[test]
    fn unsupported_sync_cancel_reaches_idle_drain_barriers() {
        let (outcome, attempts) = shutdown_with_cancel_all_failures(&[libc::EINVAL]);

        assert_eq!(outcome, ShutdownOutcome::CleanDrained);
        assert_eq!(
            attempts, 1,
            "a permanent unsupported error must not be retried"
        );
    }

    #[test]
    fn transient_sync_cancel_error_retries_before_draining() {
        let (outcome, attempts) = shutdown_with_cancel_all_failures(&[libc::EIO, libc::ENOENT]);

        assert_eq!(outcome, ShutdownOutcome::CleanDrained);
        assert_eq!(attempts, 2, "a transient error must retry cancellation");
    }

    #[test]
    fn sync_cancel_unsupported_error_classification() {
        for errno in [libc::EINVAL, libc::EOPNOTSUPP, libc::ENOSYS] {
            assert!(Driver::sync_cancel_unsupported(
                &io::Error::from_raw_os_error(errno)
            ));
        }
        for errno in [libc::EIO, libc::EBUSY, libc::EINTR] {
            assert!(!Driver::sync_cancel_unsupported(
                &io::Error::from_raw_os_error(errno)
            ));
        }
    }

    #[test]
    fn broken_accounting_falls_back_to_a_drain_barrier_and_retries_it() {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
            let handle = driver.handle();
            let dropped = Arc::new(AtomicBool::new(false));
            let mut read = Box::pin(long_lived_read(&handle, &dropped));
            let waker = futures_test::task::noop_waker();
            assert!(read
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending());
            driver.park(ParkMode::NoPark).unwrap();
            // As if a CQE had been lost: the count no longer proves anything.
            driver.shared.accounting_broken.set(true);
            driver.shared.fail_next_drain_barrier();

            let outcome = driver.shutdown_with_outcome();
            let attempts = driver.shared.shutdown_test.drain_attempts.get();
            let in_flight = driver.shared.in_flight();
            drop(read);
            drop(handle);
            tx.send((
                outcome,
                attempts,
                in_flight,
                dropped.load(Ordering::Acquire),
            ))
            .unwrap();
        });

        let (outcome, attempts, in_flight, dropped) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("shutdown did not retry a failed drain barrier");
        assert_eq!(outcome, ShutdownOutcome::CleanDrained);
        assert_eq!(attempts, 2, "a failed barrier must be retried");
        assert_eq!(in_flight, 0, "the cancelled read was still reaped");
        assert!(dropped, "the read is reclaimed once its future drops");
    }

    #[test]
    fn cancellations_are_not_queued_behind_the_drain_barrier() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let waker = futures_test::task::noop_waker();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut read = Box::pin(long_lived_read(&handle, &dropped));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        driver.park(ParkMode::NoPark).unwrap();
        let in_flight = driver.shared.in_flight();

        // As if the fallback barrier were queued: a cancellation behind it could
        // not start before its target is terminal.
        driver.shared.drain_barrier_pushed.set(true);
        drop(read);
        assert_eq!(
            driver.shared.in_flight(),
            in_flight,
            "no cancellation queued"
        );

        // The synchronous cancel still reclaims the read.
        driver.shared.drain_barrier_pushed.set(false);
        drop(handle);
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(driver.shared.in_flight(), 0);
    }

    struct Nop;

    // Safety: a NOP references no memory and posts one terminal CQE.
    unsafe impl Operation for Nop {
        type Completion = CQEResult;

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(opcode::Nop::new().build())
        }

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }
    }

    impl Singleshot for Nop {
        type Output = io::Result<u32>;

        fn complete(self, completion: Self::Completion) -> Self::Output {
            completion.into_result()
        }
    }

    #[test]
    fn overflow_queue_keeps_order_and_moves_linked_groups_whole() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 2).unwrap();
        let handle = driver.handle();
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        // One slot is left, too few for the batch, and the NOP after the batch
        // must queue behind it rather than take the slot.
        let mut first = pin!(handle.submit(Nop));
        let mut batch = pin!(handle.submit(Nop).then(handle.submit(Nop)));
        let mut last = pin!(handle.submit(Nop));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(batch.as_mut().poll(&mut cx).is_pending());
        assert!(last.as_mut().poll(&mut cx).is_pending());
        assert_eq!(handle.queued_entries(), 3);
        assert!(driver.needs_park());

        // The first park submits `first`; only then does the batch fit.
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(handle.queued_entries(), 3);
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(handle.queued_entries(), 1, "the batch moved whole");
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(handle.queued_entries(), 0);

        let (mut first_done, mut batch_done, mut last_done) = (false, false, false);
        for _ in 0..10 {
            first_done = first_done || first.as_mut().poll(&mut cx).is_ready();
            batch_done = batch_done || batch.as_mut().poll(&mut cx).is_ready();
            last_done = last_done || last.as_mut().poll(&mut cx).is_ready();
            if first_done && batch_done && last_done {
                return;
            }
            driver.park(ParkMode::NoPark).unwrap();
        }
        panic!("queued operations did not complete");
    }

    #[test]
    fn a_queued_operation_dropped_before_it_reaches_the_kernel_is_never_pushed() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        fill_sq(&driver);
        let dropped = Arc::new(AtomicBool::new(false));
        let mut read = Box::pin(long_lived_read(&handle, &dropped));
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(&futures_test::task::noop_waker()))
            .is_pending());
        assert_eq!(handle.queued_entries(), 1);

        // Cancelled where it waits: no cancellation is pushed, and the read
        // completes without ever being counted in flight.
        drop(read);
        assert_eq!(driver.shared.in_flight(), 0);
        assert!(park_until(&mut driver, 10, || dropped.load(Ordering::Acquire)));
        assert_eq!(handle.queued_entries(), 0);
        assert_eq!(driver.shared.in_flight(), 0);
    }

    #[test]
    fn shutdown_fails_queued_operations_without_the_kernel() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        fill_sq(&driver);
        let dropped = Arc::new(AtomicBool::new(false));
        let mut read = Box::pin(long_lived_read(&handle, &dropped));
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(read.as_mut().poll(&mut cx).is_pending());
        assert_eq!(handle.queued_entries(), 1);

        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        let Poll::Ready(Err(err)) = read.as_mut().poll(&mut cx) else {
            panic!("a queued read must fail with the shutdown error");
        };
        assert_eq!(err.to_string(), "reactor is shutting down");
        drop(read);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn a_dropped_operation_does_not_wake_its_last_waker() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut read = Box::pin(long_lived_read(&handle, &dropped));
        let (waker, wakes) = futures_test::task::new_count_waker();
        assert!(read
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        driver.park(ParkMode::NoPark).unwrap();

        // Nothing will poll the read again, so its cancellation must not wake
        // the task that dropped it.
        drop(read);
        assert!(park_until(&mut driver, 20, || dropped.load(Ordering::Acquire)));
        assert_eq!(wakes.get(), 0);
    }

    #[test]
    fn incoming_ends_once_the_driver_stops_accepting_work() {
        use futures_util::StreamExt;

        let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        let items = ex.block_on(async move {
            let listener = crate::net::TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8)
                .await
                .unwrap();
            let mut incoming = pin!(listener.incoming());
            assert!(futures_util::poll!(incoming.next()).is_pending());
            crate::noop().await.unwrap();

            // As after a health error: the driver stops accepting work, and the
            // armed accept ends.
            handle.shared.set_status(Status::Closing);
            handle.shared.cancel_all().unwrap();
            let mut items = Vec::new();
            for _ in 0..8 {
                match incoming.next().await {
                    Some(item) => items.push(item.map(drop).map_err(|err| err.kind())),
                    None => break,
                }
            }
            items
        });
        assert!(
            items.len() <= 1,
            "incoming() kept re-arming into a stopped driver: {items:?}"
        );
    }

    #[test]
    fn iopoll_rings_are_rejected() {
        let mut builder = io_uring::IoUring::builder();
        builder.setup_iopoll();
        let error =
            Driver::new(builder, 8).expect_err("an IOPOLL ring cannot run timers or sockets");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn disabled_rings_are_enabled() {
        let mut builder = io_uring::IoUring::builder();
        builder.setup_r_disabled();
        let driver = Driver::new(builder, 8).unwrap();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        ex.block_on(crate::noop()).unwrap();
    }

    #[test]
    fn in_flight_counts_each_entry_until_its_terminal_completion() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);

        let dropped = Arc::new(AtomicBool::new(false));
        let mut read = Box::pin(long_lived_read(&handle, &dropped));
        assert!(read.as_mut().poll(&mut cx).is_pending());
        assert_eq!(driver.shared.in_flight(), 1, "a queued entry is counted");

        // Parking arms the eventfd read, which is counted too.
        driver
            .park(ParkMode::Timeout(Duration::from_millis(1)))
            .unwrap();
        assert_eq!(driver.shared.in_flight(), 2);

        // Dropping the read queues a cancellation, a counted entry of its own.
        drop(read);
        assert_eq!(driver.shared.in_flight(), 3);
        assert!(park_until(&mut driver, 100, || dropped.load(Ordering::Acquire)));
        assert_eq!(
            driver.shared.in_flight(),
            1,
            "the read and its cancellation have completed; the eventfd read remains"
        );

        drop(handle);
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        assert_eq!(driver.shared.in_flight(), 0);
        assert_eq!(driver.shared.shutdown_test.drain_attempts.get(), 0);
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
    fn completion_drain_budget_is_configurable() {
        const DEFAULT_BUDGET: usize = DEFAULT_COMPLETION_DRAIN_BUDGET.get();

        let mut bounded = Driver::new(io_uring::IoUring::builder(), 64).unwrap();
        preload_nop_completions(&bounded, DEFAULT_BUDGET + 1);
        bounded.park(ParkMode::NoPark).unwrap();
        assert_eq!(bounded.shared.ring.borrow_mut().completion().len(), 1);
        assert!(!bounded.needs_park());
        bounded.park(ParkMode::NoPark).unwrap();
        assert_eq!(bounded.shared.ring.borrow_mut().completion().len(), 0);

        let options = DriverOptions {
            completion_drain_budget: None,
        };
        let mut unbounded =
            Driver::new_with_options(io_uring::IoUring::builder(), 4, options).unwrap();
        preload_nop_completions(&unbounded, 3);
        unbounded.park(ParkMode::NoPark).unwrap();
        assert_eq!(unbounded.shared.ring.borrow_mut().completion().len(), 0);
    }

    struct PanicOnce(AtomicBool);

    impl Wake for PanicOnce {
        fn wake(self: Arc<Self>) {
            assert!(self.0.swap(true, Ordering::SeqCst), "completion wake panic");
        }
    }

    struct TrackedCompletion(Arc<AtomicUsize>);

    impl Drop for TrackedCompletion {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct TrackedNop {
        reaped: Arc<AtomicUsize>,
        completions_dropped: Arc<AtomicUsize>,
        operations_dropped: Arc<AtomicUsize>,
    }

    impl Drop for TrackedNop {
        fn drop(&mut self) {
            self.operations_dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    // Safety: the NOP has no external resources and its completion owns only
    // the test accounting handle.
    unsafe impl Operation for TrackedNop {
        type Completion = TrackedCompletion;

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(opcode::Nop::new().build())
        }

        unsafe fn reap(&mut self, _result: CQEResult) -> Self::Completion {
            self.reaped.fetch_add(1, Ordering::SeqCst);
            TrackedCompletion(Arc::clone(&self.completions_dropped))
        }
    }

    impl Singleshot for TrackedNop {
        type Output = ();

        fn complete(self, completion: Self::Completion) -> Self::Output {
            drop(completion);
        }
    }

    #[test]
    fn panicking_completion_waker_preserves_cqe_tail_and_resources() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let reaped = Arc::new(AtomicUsize::new(0));
        let completions_dropped = Arc::new(AtomicUsize::new(0));
        let operations_dropped = Arc::new(AtomicUsize::new(0));
        let new_op = || TrackedNop {
            reaped: Arc::clone(&reaped),
            completions_dropped: Arc::clone(&completions_dropped),
            operations_dropped: Arc::clone(&operations_dropped),
        };

        let mut first = Box::pin(driver.handle().submit(new_op()));
        let mut second = Box::pin(driver.handle().submit(new_op()));
        let panic_waker = Waker::from(Arc::new(PanicOnce(AtomicBool::new(false))));
        assert!(first
            .as_mut()
            .poll(&mut Context::from_waker(&panic_waker))
            .is_pending());
        assert!(second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());

        driver.park(ParkMode::NoPark).unwrap();
        let panic = catch_unwind(AssertUnwindSafe(|| driver.park(ParkMode::NoPark)));
        assert!(panic.is_err(), "the waker panic must propagate");

        // The first completion was reaped before the wake. Dropping its future
        // must release its completion and operation even though draining unwound.
        drop(first);
        driver.park(ParkMode::NoPark).unwrap();
        assert!(driver.deferred_cqes.borrow().is_empty());
        assert!(second
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready());
        drop(second);

        assert_eq!(reaped.load(Ordering::SeqCst), 2);
        assert_eq!(completions_dropped.load(Ordering::SeqCst), 2);
        assert_eq!(operations_dropped.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn status_transition_commits_before_panicking_waiter() {
        let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let mut panicking_waiter = pin!(driver.shared.receive_shutdown.wait());
        let panic_waker = Waker::from(Arc::new(PanicOnce(AtomicBool::new(false))));
        assert!(Future::poll(
            panicking_waiter.as_mut(),
            &mut Context::from_waker(&panic_waker)
        )
        .is_pending());

        let (waker, wake_count) = futures_test::task::new_count_waker();
        let mut following_waiter = pin!(driver.shared.receive_shutdown.wait());
        assert!(
            Future::poll(following_waiter.as_mut(), &mut Context::from_waker(&waker)).is_pending()
        );

        let panic = catch_unwind(AssertUnwindSafe(|| {
            driver.shared.set_status(Status::Closing);
        }));
        assert!(panic.is_err(), "the waiter panic must propagate");
        assert_eq!(driver.shared.status(), Status::Closing);

        driver.shared.set_status(Status::Draining);
        assert_eq!(wake_count.get(), 1);

        // This unit test transitions the status directly, without the
        // cancellation that would normally establish this state.
        driver.shared.finish_shutdown(ShutdownOutcome::CleanDrained);
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
        fill_sq(&driver);

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
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
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
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                let _ = self.fd.fd();
                Ok(opcode::Timeout::new(&self.timeout).build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
        }

        impl Singleshot for PendingFdOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        #[derive(Debug)]
        struct PendingFixedBufOp {
            fd: fd::NornFd,
            buf: crate::fixedbuf::FixedBuf<Vec<u8>>,
            timeout: Timespec,
            dropped: Rc<Cell<usize>>,
        }

        impl Drop for PendingFixedBufOp {
            fn drop(&mut self) {
                self.dropped.set(self.dropped.get() + 1);
            }
        }

        // Safety: RawOp keeps the timeout storage stable until the terminal CQE.
        // The operation also retains a registered fixed buffer during that period.
        unsafe impl Operation for PendingFixedBufOp {
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                let _ = self.fd.fd();
                let _ = self.buf.index();
                Ok(opcode::Timeout::new(&self.timeout).build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
        }

        impl Singleshot for PendingFixedBufOp {
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
        let mut fixed_op = Box::pin(handle.submit(PendingFixedBufOp {
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
        crate::test_util::run_isolated(
            "driver::tests::shutdown_waits_for_cleanup_generated_close_before_returning",
            shutdown_waits_for_cleanup_generated_close_before_returning_isolated,
        );
    }

    fn shutdown_waits_for_cleanup_generated_close_before_returning_isolated() {
        // Shutdown cancels this operation and reaps it, and dropping it then
        // closes its descriptor. That close must finish before shutdown returns.
        #[derive(Debug)]
        struct PendingFdOp {
            _fd: fd::NornFd,
            timeout: Timespec,
        }

        // Safety: RawOp keeps the timeout storage stable until the terminal CQE.
        unsafe impl Operation for PendingFdOp {
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(opcode::Timeout::new(&self.timeout).build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
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

        // Observing the writer checks the pipe the driver closed, not the
        // descriptor number. The number is safe to probe only because this test
        // runs isolated, which the dup2 below also relies on.
        let pipe_fds = crate::test_util::pipe_cloexec();
        let owned_fd = pipe_fds[0];
        let write_fd = pipe_fds[1];
        // An unrelated descriptor to place on `owned_fd`'s number below.
        let spare_fds = crate::test_util::pipe_cloexec();
        let duplicate_fd = spare_fds[0];

        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let guard = driver.enter();
        let handle = driver.handle();
        let mut op = Box::pin(handle.submit(PendingFdOp {
            _fd: fd::NornFd::from_fd(owned_fd),
            timeout: Timespec::new().sec(3_600),
        }));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
        driver.shared.submit(ParkMode::NoPark).unwrap();
        drop(op);
        drop(handle);
        drop(guard);

        Park::shutdown(&mut driver);

        assert_eq!(
            driver.shared.shutdown_test.drain_attempts.get(),
            0,
            "the in-flight count proves reclamation without a drain barrier"
        );
        crate::test_util::assert_pipe_reader_closed(write_fd);
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
            libc::close(spare_fds[1]);
        }
    }

    #[test]
    fn shutdown_submits_partial_success_remainder_before_cancel_all() {
        let (tx, rx) = mpsc::channel();

        std::thread::spawn(move || {
            #[derive(Debug)]
            struct NopOp;

            unsafe impl Operation for NopOp {
                type Completion = CQEResult;

                fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                    Ok(opcode::Nop::new().build())
                }

                unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                    result
                }
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
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
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
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
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
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
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
    fn etime_is_expected_only_for_timed_submissions() {
        for mode in [
            ParkMode::Timeout(Duration::from_millis(1)),
            ParkMode::NoPark,
            ParkMode::NextCompletion,
        ] {
            let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
            driver.shared.fail_next_submit(libc::ETIME);
            let result = driver.shared.submit(mode);
            if matches!(mode, ParkMode::Timeout(_)) {
                assert_eq!(result.unwrap(), 0);
                assert!(driver.handle().health_error().is_none());
                assert_eq!(driver.shared.status.get(), Status::Running);
            } else {
                assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ETIME));
                assert!(driver.handle().health_error().is_some());
                assert_eq!(driver.shared.status.get(), Status::Closing);
            }
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

        for errno in [libc::EAGAIN, libc::ENOMEM] {
            assert!(
                !Shared::should_record_submit_error(&io::Error::from_raw_os_error(errno)),
                "allocation failure {errno} should remain a transient submit condition"
            );
        }

        let eio = io::Error::from_raw_os_error(libc::EIO);
        assert!(
            Shared::should_record_submit_error(&eio),
            "hard submit failures should still transition driver health"
        );
    }

    #[test]
    fn transient_enter_failures_keep_the_driver_running() {
        for errno in [libc::EAGAIN, libc::ENOMEM] {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();

            driver.shared.fail_next_submit(errno);
            driver
                .park(ParkMode::NextCompletion)
                .expect("a transient park failure should yield, not fail");
            assert_eq!(driver.shared.status(), Status::Running);
            assert!(driver.handle().health_error().is_none());

            // A close queued during CQE dispatch must survive a transient
            // submit failure and reach the kernel on the next park.
            // Closure is observed through the pipe's write end rather than by
            // probing `raw_fd`, which a concurrent test could have reused.
            let [raw_fd, write_end] = crate::test_util::pipe_cloexec();
            driver.shared.fail_next_submit(errno);
            let result = driver.shared.close_fd(types::Fd(raw_fd));
            assert!(matches!(result, Err(CloseFdError::Queued(_))));
            assert_eq!(driver.shared.status(), Status::Running);
            assert!(driver.handle().health_error().is_none());

            driver.park(ParkMode::NoPark).unwrap();
            crate::test_util::assert_pipe_reader_closed(write_end);
            unsafe { libc::close(write_end) };

            // Remote wakeups still re-arm once the transient failure passes.
            let unparker = Arc::clone(&driver.unparker);
            let remote = std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                unparker.wake();
            });
            driver
                .park(ParkMode::NextCompletion)
                .expect("remote unpark should end the park");
            remote.join().unwrap();
            assert_eq!(driver.shared.status(), Status::Running);
        }
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

    fn shutdown_reclaims_hard_linked_read(builder: io_uring::Builder) {
        // A hard link issues its dependent even when the head fails. Shutdown's
        // synchronous cancel stops the head, which issues the read only
        // afterwards, so only a later cancellation can reclaim it.
        #[derive(Debug)]
        struct Head {
            timeout: Timespec,
        }

        // Safety: the timespec lives in the operation through its terminal CQE.
        unsafe impl Operation for Head {
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(opcode::Timeout::new(&self.timeout).build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
        }

        impl Singleshot for Head {
            type Output = io::Result<u32>;

            fn complete(self, completion: Self::Completion) -> Self::Output {
                completion.into_result()
            }
        }

        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut driver = Driver::new(builder, 8).unwrap();
            let handle = driver.handle();
            let dropped = Arc::new(AtomicBool::new(false));
            let mut head = Box::pin(handle.submit(Head {
                timeout: Timespec::new().sec(3_600),
            }));
            let mut read = Box::pin(long_lived_read(&handle, &dropped));
            let mut batch = SmallVec::new();
            assert!(head.as_mut().prepare_batch(&mut batch));
            assert!(read.as_mut().prepare_batch(&mut batch));
            let mut batch = batch.into_iter();
            let head_entry = batch
                .next()
                .unwrap()
                .into_entry_with_flags(Flags::IO_HARDLINK);
            let read_entry = batch.next().unwrap().into_entry_with_flags(Flags::empty());
            // Safety: both entries come from live operations whose kernel
            // references they carry.
            unsafe {
                driver.shared.try_push_raw(&head_entry).unwrap();
                driver.shared.try_push_raw(&read_entry).unwrap();
            }
            head.as_mut().finish_submit();
            read.as_mut().finish_submit();
            driver.park(ParkMode::NoPark).unwrap();

            // The operations stay alive, so only shutdown cancels them.
            let outcome = driver.shutdown_with_outcome();
            let attempts = driver.shared.shutdown_test.cancel_all_attempts.get();
            let reclaimed = dropped.load(Ordering::Acquire);
            drop((head, read));
            tx.send((outcome, attempts, reclaimed)).unwrap();
        });
        let (outcome, attempts, reclaimed) = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("shutdown never reclaimed the hard-linked read");
        assert_eq!(outcome, ShutdownOutcome::CleanDrained);
        assert!(
            attempts >= 2,
            "the read is issued after the first synchronous cancel and needs another ({attempts})"
        );
        assert!(!reclaimed, "the operation outlives shutdown in this test");
    }

    #[test]
    fn shutdown_recancels_a_request_issued_after_the_sync_cancel() {
        shutdown_reclaims_hard_linked_read(io_uring::IoUring::builder());
    }

    #[test]
    fn shutdown_recancels_a_request_issued_after_the_sync_cancel_with_defer_taskrun() {
        let mut builder = io_uring::IoUring::builder();
        builder
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer();
        shutdown_reclaims_hard_linked_read(builder);
    }

    fn long_lived_read(handle: &Handle, dropped: &Arc<AtomicBool>) -> Op<LongLivedOp> {
        let (reader, writer) = UnixStream::pair().unwrap();
        handle.submit(LongLivedOp {
            io: LongLivedIo::Read {
                reader,
                _writer: writer,
                buf: Box::new([0; 8]),
            },
            _driver: handle.clone(),
            dropped: Arc::clone(dropped),
        })
    }

    /// Fill the userspace SQ with NOPs whose completions the driver ignores.
    ///
    /// They are not counted in flight, as their completions are not.
    fn fill_sq(driver: &Driver) {
        let nop = opcode::Nop::new()
            .build()
            .user_data(Driver::CANCELLATION_TOKEN as u64);
        let mut ring = driver.shared.ring.borrow_mut();
        let mut sq = ring.submission();
        // Safety: the filler NOP references no memory.
        while unsafe { sq.push(&nop) }.is_ok() {}
    }

    fn park_until(driver: &mut Driver, max_parks: usize, done: impl Fn() -> bool) -> bool {
        for _ in 0..max_parks {
            if done() {
                return true;
            }
            driver.park(ParkMode::NoPark).unwrap();
        }
        done()
    }

    /// A NOP that records whether it was synthetic and whether it succeeded.
    struct RecordedNop(Rc<Cell<Option<(bool, bool)>>>);

    // Safety: a NOP references no memory and posts one terminal CQE.
    unsafe impl Operation for RecordedNop {
        type Completion = ();

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(opcode::Nop::new().build())
        }

        unsafe fn reap(&mut self, result: CQEResult) {
            self.0
                .set(Some((result.is_synthetic(), result.result.is_ok())));
        }
    }

    impl Singleshot for RecordedNop {
        type Output = ();

        fn complete(self, _: ()) {}
    }

    #[test]
    fn dropping_a_submitted_then_always_chain_cancels_both_requests() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let first = Arc::new(AtomicBool::new(false));
        let second = Arc::new(AtomicBool::new(false));
        let mut chain = Box::pin(
            long_lived_read(&handle, &first).then_always(long_lived_read(&handle, &second)),
        );
        let waker = futures_test::task::noop_waker();
        assert!(chain
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        driver.park(ParkMode::NoPark).unwrap();

        // Cancelling the first read does not break the hard link, so the
        // kernel issues the second read, which is cancelled once issued.
        drop(chain);
        assert!(
            park_until(&mut driver, 20, || first.load(Ordering::Acquire)
                && second.load(Ordering::Acquire)),
            "both reads must end"
        );
    }

    #[test]
    fn dropping_a_queued_then_always_chain_cancels_both_requests() {
        for always in [true, false] {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 2).unwrap();
            let handle = driver.handle();
            fill_sq(&driver);
            let dropped = Arc::new(AtomicBool::new(false));
            let outcome = Rc::new(Cell::new(None));
            let read = long_lived_read(&handle, &dropped);
            let nop = handle.submit(RecordedNop(Rc::clone(&outcome)));
            let waker = futures_test::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            if always {
                let mut chain = pin!(read.then_always(nop));
                assert!(chain.as_mut().poll(&mut cx).is_pending());
                assert_eq!(handle.queued_entries(), 2);
            } else {
                let mut chain = pin!(read.then(nop));
                assert!(chain.as_mut().poll(&mut cx).is_pending());
                assert_eq!(handle.queued_entries(), 2);
            }

            // Both requests are cancelled where they wait, so neither reaches
            // the kernel, whether the link is hard or soft.
            assert!(park_until(&mut driver, 20, || dropped
                .load(Ordering::Acquire)
                && outcome.get().is_some()));
            assert_eq!(outcome.get(), Some((true, false)), "always={always}");
            assert_eq!(handle.queued_entries(), 0);
        }
    }

    struct MultishotPoll {
        reader: UnixStream,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for MultishotPoll {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Release);
        }
    }

    // Safety: multishot poll references only the descriptor, which this
    // operation owns until it is destroyed after the terminal CQE.
    unsafe impl Operation for MultishotPoll {
        type Completion = CQEResult;

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(
                opcode::PollAdd::new(types::Fd(self.reader.as_raw_fd()), libc::POLLIN as _)
                    .multi(true)
                    .build(),
            )
        }

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }
    }

    impl crate::operation::Multishot for MultishotPoll {
        type Item = CQEResult;

        fn update(&mut self, completion: Self::Completion) -> Self::Item {
            completion
        }
    }

    #[test]
    fn drop_cancel_survives_transient_ebusy() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut op = Box::pin(long_lived_read(&handle, &dropped));
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(op.as_mut().poll(&mut cx).is_pending());
        driver.park(ParkMode::NoPark).unwrap();

        // Exhaust the submit EBUSY retries. Dropping must not depend on entering
        // the kernel, and later healthy parks must deliver the cancellation.
        for _ in 0..9 {
            driver.shared.fail_next_submit(libc::EBUSY);
        }
        drop(op);
        assert!(
            park_until(&mut driver, 5, || dropped.load(Ordering::Acquire)),
            "a cancel queued by Drop must survive transient EBUSY"
        );
        assert!(handle.health_error().is_none());
        drop(handle);
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
    }

    #[test]
    fn drop_cancel_is_deferred_when_sq_is_full() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut op = Box::pin(long_lived_read(&handle, &dropped));
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(op.as_mut().poll(&mut cx).is_pending());
        driver.park(ParkMode::NoPark).unwrap();

        fill_sq(&driver);
        drop(op);
        assert!(driver.shared.has_deferred_cancels());
        assert!(
            driver.needs_park(),
            "a deferred cancellation must request a park"
        );
        assert!(
            park_until(&mut driver, 5, || dropped.load(Ordering::Acquire)),
            "a deferred cancellation must be retried once the SQ drains"
        );
        assert!(!driver.shared.has_deferred_cancels());
        drop(handle);
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        assert_eq!(Rc::strong_count(&driver.shared), 1);
    }

    #[test]
    fn deferred_cancel_does_not_block_a_waiting_park() {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
            let handle = driver.handle();
            let dropped = Arc::new(AtomicBool::new(false));
            let mut op = Box::pin(long_lived_read(&handle, &dropped));
            let waker = futures_test::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(op.as_mut().poll(&mut cx).is_pending());
            driver.park(ParkMode::NoPark).unwrap();

            fill_sq(&driver);
            drop(op);
            // Nothing else will complete. A park that waits for a CQE without
            // first delivering the cancellation would block forever.
            for _ in 0..5 {
                if dropped.load(Ordering::Acquire) {
                    break;
                }
                driver.park(ParkMode::NextCompletion).unwrap();
            }
            tx.send(dropped.load(Ordering::Acquire)).unwrap();
            drop(handle);
        });
        assert!(rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a waiting park stranded the deferred cancellation"));
    }

    #[test]
    fn dropped_multishot_is_cancelled_after_deferral() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let dropped = Arc::new(AtomicBool::new(false));
        let (reader, mut writer) = UnixStream::pair().unwrap();
        let mut op = Box::pin(handle.submit(MultishotPoll {
            reader,
            dropped: Arc::clone(&dropped),
        }));
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(futures_core::Stream::poll_next(op.as_mut(), &mut cx).is_pending());
        driver.park(ParkMode::NoPark).unwrap();

        use std::io::Write;
        writer.write_all(b"x").unwrap();
        let mut item = None;
        for _ in 0..5 {
            driver.park(ParkMode::NoPark).unwrap();
            if let Poll::Ready(next) = futures_core::Stream::poll_next(op.as_mut(), &mut cx) {
                item = next;
                break;
            }
        }
        let item = item.expect("multishot poll must yield while armed");
        assert!(item.more(), "the multishot poll must remain armed");

        fill_sq(&driver);
        drop(op);
        assert!(driver.shared.has_deferred_cancels());
        // The abandoned poll keeps producing completions until it is cancelled.
        writer.write_all(b"y").unwrap();
        assert!(
            park_until(&mut driver, 6, || dropped.load(Ordering::Acquire)),
            "a deferred cancellation must terminate an abandoned multishot operation"
        );
        drop(handle);
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
    }

    #[test]
    fn shutdown_flushes_pending_deferred_cancels() {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let dropped = Arc::new(AtomicBool::new(false));
        let mut ops: Vec<_> = (0..3)
            .map(|_| Box::pin(long_lived_read(&handle, &dropped)))
            .collect();
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for op in &mut ops {
            assert!(op.as_mut().poll(&mut cx).is_pending());
        }
        driver.park(ParkMode::NoPark).unwrap();

        fill_sq(&driver);
        drop(ops);
        assert_eq!(driver.shared.deferred_cancels.borrow().len(), 3);
        drop(handle);
        // Exercise the path that cannot rely on synchronous cancellation.
        driver.shared.fail_next_cancel_all(libc::EINVAL);
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        assert!(!driver.shared.has_deferred_cancels());
        assert!(dropped.load(Ordering::Acquire));
        assert_eq!(Rc::strong_count(&driver.shared), 1);
    }

    /// Fill the kernel CQ ring and push `extra` further completions into the
    /// kernel's overflow list (`IORING_FEAT_NODROP`).
    fn overflow_cq(driver: &Driver, extra: usize) {
        let nop = opcode::Nop::new()
            .build()
            .user_data(Driver::CANCELLATION_TOKEN as u64);
        let mut ring = driver.shared.ring.borrow_mut();
        let cq_capacity = ring.completion().capacity();
        let mut posted = 0;
        while posted < cq_capacity + extra {
            let mut pushed = 0;
            {
                let mut sq = ring.submission();
                while posted + pushed < cq_capacity + extra && !sq.is_full() {
                    // Safety: a NOP carrying a reserved token that the driver ignores.
                    unsafe { sq.push(&nop) }.unwrap();
                    pushed += 1;
                }
            }
            assert_eq!(ring.submitter().submit().unwrap(), pushed);
            posted += pushed;
        }
        assert!(ring.submission().cq_overflow(), "CQ should have overflowed");
    }

    #[test]
    fn cq_overflow_with_pending_remote_wake_does_not_strand_completions() {
        // An armed eventfd read whose completion lands in the kernel overflow
        // list leaves the unparker woken and parked. Every later waiting park
        // then degrades to `NoPark` with an empty SQ, which must still enter the
        // kernel to flush the overflow list.
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut driver = Driver::new(io_uring::IoUring::builder(), 2).unwrap();
            // Arm the eventfd read, as any idle park does.
            driver
                .park(ParkMode::Timeout(Duration::from_millis(1)))
                .unwrap();
            assert!(driver.unparker.state().is_parked());

            let reaped = Arc::new(AtomicUsize::new(0));
            let mut op = Box::pin(driver.handle().submit(TrackedNop {
                reaped: Arc::clone(&reaped),
                completions_dropped: Arc::new(AtomicUsize::new(0)),
                operations_dropped: Arc::new(AtomicUsize::new(0)),
            }));
            // A burst of completions overflows the CQ while the executor is busy.
            overflow_cq(&driver, 1);
            // The operation completes while the overflow is pending.
            assert!(op
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            driver
                .shared
                .ring
                .borrow_mut()
                .submitter()
                .submit()
                .unwrap();

            // A remote wake completes the eventfd read into the overflow list too.
            let unparker = Arc::clone(&driver.unparker);
            std::thread::spawn(move || unparker.wake()).join().unwrap();

            let mut parks = 0;
            while parks < 1000 && reaped.load(Ordering::SeqCst) == 0 {
                driver.park(ParkMode::NextCompletion).unwrap();
                parks += 1;
            }
            let overflow = driver.shared.ring.borrow_mut().submission().cq_overflow();
            tx.send((reaped.load(Ordering::SeqCst), parks, overflow))
                .unwrap();
            drop(op);
        });
        let (reaped, parks, overflow) = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("driver thread blocked");
        assert_eq!(
            reaped, 1,
            "operation never completed after {parks} NextCompletion parks \
             (CQ overflow still pending: {overflow})"
        );
    }

    #[test]
    fn dropped_linked_request_is_cancelled_after_its_head_completes() {
        // Once a link head completes, the kernel issues its dependent from task
        // work. Under DEFER_TASKRUN that work runs only in a GETEVENTS enter,
        // after the SQEs of that enter are issued. A cancellation queued by Drop
        // in that window finds nothing (ENOENT) and must be retried once that
        // work has run, or the abandoned dependent stays in flight until shutdown.
        let mut builder = io_uring::IoUring::builder();
        builder
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer();
        let Ok(mut driver) = Driver::new(builder, 8) else {
            eprintln!("skipping: the kernel does not support DEFER_TASKRUN");
            return;
        };
        let handle = driver.handle();
        let dropped = Arc::new(AtomicBool::new(false));
        let head_reaped = Arc::new(AtomicUsize::new(0));
        let mut chain = Box::pin(
            handle
                .submit(TrackedNop {
                    reaped: Arc::clone(&head_reaped),
                    completions_dropped: Arc::new(AtomicUsize::new(0)),
                    operations_dropped: Arc::new(AtomicUsize::new(0)),
                })
                .then(long_lived_read(&handle, &dropped)),
        );
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(chain.as_mut().poll(&mut cx).is_pending());

        // Submit the linked pair. The head completes inline; the dependent read
        // is queued for issue.
        driver.park(ParkMode::NoPark).unwrap();
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(head_reaped.load(Ordering::SeqCst), 1);

        // The application abandons the request, which must cancel the read.
        drop(chain);
        for _ in 0..20 {
            if dropped.load(Ordering::Acquire) {
                break;
            }
            driver
                .park(ParkMode::Timeout(Duration::from_millis(5)))
                .unwrap();
        }
        let released = dropped.load(Ordering::Acquire);
        // The retry found its target, so none remains queued.
        let retrying = !driver.shared.cancel_retries.borrow().is_empty();
        drop(handle);
        assert_eq!(
            driver.shutdown_with_outcome(),
            ShutdownOutcome::CleanDrained
        );
        assert!(
            released,
            "the abandoned linked read was still in flight after 20 parks"
        );
        assert!(!retrying, "a completed target must not be retried");
    }

    #[test]
    fn dropped_link_before_head_cqe_is_reaped_is_cancelled_without_an_unrelated_wake() {
        // Drop the chain after its head completed in the kernel but before the
        // driver reaped the head's CQE, then park with a long timeout as an
        // idle executor would. The retry must not wait for some other wake.
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let mut builder = io_uring::IoUring::builder();
            builder
                .setup_coop_taskrun()
                .setup_defer_taskrun()
                .setup_single_issuer();
            let Ok(mut driver) = Driver::new(builder, 8) else {
                eprintln!("skipping: the kernel does not support DEFER_TASKRUN");
                tx.send(None).unwrap();
                return;
            };
            let handle = driver.handle();
            let dropped = Arc::new(AtomicBool::new(false));
            let mut chain = Box::pin(
                handle
                    .submit(TrackedNop {
                        reaped: Arc::new(AtomicUsize::new(0)),
                        completions_dropped: Arc::new(AtomicUsize::new(0)),
                        operations_dropped: Arc::new(AtomicUsize::new(0)),
                    })
                    .then(long_lived_read(&handle, &dropped)),
            );
            let waker = futures_test::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(chain.as_mut().poll(&mut cx).is_pending());
            // Submit the pair. The NOP head completes inline; its CQE is not
            // reaped yet.
            driver.park(ParkMode::NoPark).unwrap();
            drop(chain);
            let start = std::time::Instant::now();
            let mut parks = 0;
            while !dropped.load(Ordering::Acquire) && parks < 10 {
                driver
                    .park(ParkMode::Timeout(Duration::from_secs(2)))
                    .unwrap();
                parks += 1;
            }
            tx.send(Some((
                dropped.load(Ordering::Acquire),
                start.elapsed(),
                parks,
            )))
            .unwrap();
            drop(handle);
            let _ = driver.shutdown_with_outcome();
        });
        let Some((released, elapsed, parks)) = rx.recv_timeout(Duration::from_secs(30)).unwrap()
        else {
            return;
        };
        assert!(released, "the abandoned linked read was never released");
        assert!(
            elapsed < Duration::from_millis(500),
            "released only after {elapsed:?} ({parks} parks): the retry waited for an unrelated wake"
        );
    }
}
