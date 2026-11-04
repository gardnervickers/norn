use std::cell::RefMut;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::{io, mem};

mod header;
mod queue;
mod raw;

use header::QueueState;

use io_uring::types::CancelBuilder;
pub use raw::CQEResult;
pub(crate) use raw::{OperationId, RawOpRef};

use io_uring::squeue::Flags;
use smallvec::SmallVec;

use crate::driver::CancelRetry;
use crate::error::SubmitError;

/// Low-level request customization for advanced `io_uring` users.
///
/// # Safety
///
/// Implementing this trait asserts that every entry returned by [`Operation::configure`]
/// remains valid for the entire lifetime of the kernel operation. In particular:
///
/// - every pointer, file descriptor, fixed-resource index, and other resource referenced by
///   the entry must remain valid for every access the opcode permits, through the terminal CQE;
/// - memory that the kernel may read or write must remain allocated at a stable address and
///   must obey Rust's aliasing rules for the entire period of kernel access. An implementation
///   must not expose references that conflict with those accesses;
/// - the operation must account for every CQE the entry can produce. The runtime treats the
///   first CQE without `IORING_CQE_F_MORE` as terminal, so the entry must not permit any later
///   CQE or kernel access associated with the operation;
/// - requesting cancellation does not end the operation's lifetime. All referenced resources
///   must remain valid until the original operation produces its terminal CQE;
/// - [`Operation::reap`] must convert every kernel CQE or synthetic completion into an owned
///   value that accounts for all resources created, selected, or otherwise transferred by the
///   kernel. Dropping that value must release those resources without consulting the operation
///   again;
/// - [`Operation::reap`] must not unwind. A CQE cannot be replayed after it has been removed from
///   the kernel completion queue; and
/// - an entry that sets `IOSQE_BUFFER_SELECT` must select from a buffer group the implementation
///   owns. A buffer selected from a group registered through
///   [`RecvBufRing`](crate::bufring::RecvBufRing) is never returned to that ring, so it is lost to
///   every other receive.
///
/// The runtime places the operation at a stable address before calling
/// [`Operation::configure`] and does not move it while the entry may be submitted or accessed by
/// the kernel. After the operation is known not to be in flight, it may be moved into its
/// completion handler. The runtime overwrites the SQE's `user_data` field for its own tracking
/// and cannot verify any of the requirements above.
pub unsafe trait Operation {
    /// The owned value produced from one completion.
    ///
    /// Values are queued until application code consumes them. If the request is abandoned,
    /// they are instead dropped in completion order while the operation is destroyed.
    type Completion: 'static;

    /// Configure a new [`io_uring::squeue::Entry`] for this operation.
    ///
    /// The runtime calls this once, when the operation is first polled or when a linked
    /// request containing it is first polled. An operation dropped before then is never
    /// configured. A panic in `configure` propagates out of that poll and leaves the operation
    /// unusable: polling it again panics.
    ///
    /// Configuration failures are delivered through the operation's normal completion path;
    /// the operation is not submitted to the kernel.
    ///
    /// The address of `self` remains stable while the returned entry may be accessed by the
    /// kernel. Implementations may store pointers to fields of `self` in the entry, but must not
    /// invalidate the pointed-to storage during that period.
    ///
    /// The runtime owns the `IOSQE_IO_LINK`, `IOSQE_IO_HARDLINK`, and
    /// `IOSQE_CQE_SKIP_SUCCESS` flags. The submission queue is shared, so a link would join
    /// the entry to whichever SQE is queued next, and a skipped success CQE would leave the
    /// operation without its terminal completion. Use [`Request::then`](crate::Request::then)
    /// to link requests. An entry that sets one of these flags fails configuration with
    /// [`io::ErrorKind::InvalidInput`].
    ///
    /// # Errors
    ///
    /// Returns an error when the operation cannot construct a valid submission
    /// queue entry. The runtime delivers this through the normal completion path.
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry>;

    /// Convert one kernel or synthetic completion into an owned value.
    ///
    /// The runtime invokes this exactly once for each kernel CQE or synthetic completion, before
    /// exposing terminal state or waking application code. It may be called multiple times for a
    /// multishot operation. A panic from this method aborts the process because the completion
    /// cannot be safely replayed.
    ///
    /// Configuration failures, submission failures, and cancellation before submission are
    /// represented by synthetic error completions. Implementations can distinguish them from
    /// kernel CQEs with [`CQEResult::is_synthetic`].
    ///
    /// # Safety
    ///
    /// `result` must be an unreaped kernel or synthetic completion produced for this operation.
    /// Supplying a completion from a different operation can cause the implementation to claim
    /// resources that it does not own.
    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion;
}

/// A singleshot request that resolves to one final output.
pub trait Singleshot: Operation {
    /// The value returned once the final completion is observed.
    type Output;

    /// Convert the terminal completion into the request's output.
    fn complete(self, completion: Self::Completion) -> Self::Output;

    /// Handle a non-terminal completion.
    ///
    /// The default implementation panics.
    fn update(&mut self, _completion: Self::Completion) {
        panic!("unhandled non-terminal completion for singleshot operation")
    }
}

/// A multishot request that can yield many items from one submission.
pub trait Multishot: Operation {
    /// The item yielded by each completion.
    type Item;

    /// Handle a non-terminal completion.
    fn update(&mut self, completion: Self::Completion) -> Self::Item;

    /// Convert the terminal completion into an optional final item.
    fn complete(self, completion: Self::Completion) -> Option<Self::Item>
    where
        Self: Sized,
    {
        let _ = completion;
        None
    }
}

pub(crate) struct ConfiguredEntry {
    entry: io_uring::squeue::Entry,
    handle: RawOpRef,
}

impl ConfiguredEntry {
    pub(crate) fn target_user_data(&self) -> u64 {
        self.handle.as_raw_usize() as u64
    }

    pub(crate) fn into_entry_with_flags(self, flags: Flags) -> io_uring::squeue::Entry {
        self.entry
            .flags(flags)
            .user_data(self.handle.into_raw_usize() as u64)
    }

    pub(crate) fn new(handle: RawOpRef, entry: io_uring::squeue::Entry) -> Self {
        Self { entry, handle }
    }

    /// Record that the driver queued this entry because the SQ was full.
    pub(crate) fn mark_queued(&self) {
        self.handle.header().set_queue_state(QueueState::Queued);
    }

    /// Whether the operation was cancelled while this entry was queued.
    pub(crate) fn cancelled_while_queued(&self) -> bool {
        self.handle.header().queue_state() == QueueState::Cancelled
    }

    /// Record that this entry left the driver's queue for the SQ.
    pub(crate) fn mark_dequeued(&self) {
        self.handle.header().set_queue_state(QueueState::Idle);
    }

    /// Complete an entry that never reached the kernel.
    pub(crate) fn complete_unsubmitted(self, err: io::Error) {
        self.mark_dequeued();
        // Safety: this entry holds the operation's kernel reference and was
        // never pushed, so this synthetic completion is its only terminal one.
        unsafe { self.handle.reap(CQEResult::synthetic(Err(err))) };
    }
}

/// An owned identity for an operation submitted through this driver.
///
/// Keeping this value alive keeps the operation allocation alive, which makes
/// its `user_data` identity safe to use as the target of a later `io_uring`
/// control request even if the original operation completes in the meantime.
pub(crate) struct OpTarget {
    handle: RawOpRef,
}

impl OpTarget {
    pub(crate) fn user_data(&self) -> u64 {
        self.handle.as_raw_usize() as u64
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.handle.is_complete()
    }

    /// Cancel the operation where it waits, if its entry is still in the
    /// driver's overflow queue. Returns false once the entry has left it.
    pub(crate) fn cancel_if_queued(&self) -> bool {
        let header = self.handle.header();
        if header.queue_state() == QueueState::Idle {
            return false;
        }
        header.set_queue_state(QueueState::Cancelled);
        true
    }
}

impl Clone for OpTarget {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
        }
    }
}

impl std::fmt::Debug for OpTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpTarget").finish_non_exhaustive()
    }
}

/// A driver-internal asynchronous cancellation of a submitted operation.
///
/// The cancellation names its target by `user_data`, so it can match only that
/// one request. It is not the `IORING_ASYNC_CANCEL_FD` or `_ANY` form, which
/// match every request on a descriptor or every request at all (shutdown uses
/// the latter through the driver's `cancel_all`). Its own completion
/// says only whether the kernel found the target (`0`, `-ENOENT`,
/// `-EALREADY`), not how the target finished; the target's own CQE is
/// authoritative.
///
/// The request owns its target's identity until the kernel reports the
/// cancellation's own terminal CQE. The target allocation, and therefore the
/// numeric `user_data` the cancellation matches, cannot be reused while the
/// cancellation can still reach the kernel.
pub(crate) struct AsyncCancel {
    target: OpTarget,
    retry: Option<CancelRetry>,
}

impl AsyncCancel {
    /// Build a detached submission entry cancelling `target`.
    ///
    /// The entry carries the only reference to the cancellation request.
    /// Dropping the entry without pushing it releases the request.
    pub(crate) fn entry(target: OpTarget, retry: Option<CancelRetry>) -> ConfiguredEntry {
        let handle = TypedHandle::new(Self { target, retry });
        let entry = handle
            .data_mut()
            .expect("new cancellation missing data")
            .configure()
            .expect("async cancellation configuration is infallible");
        ConfiguredEntry::new(handle.untyped(), entry)
    }
}

// Safety: the entry references no memory; it names its target only by the
// `user_data` value whose allocation `target` retains through the terminal CQE.
unsafe impl Operation for AsyncCancel {
    type Completion = ();

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let criteria = CancelBuilder::user_data(self.target.user_data());
        Ok(io_uring::opcode::AsyncCancel2::new(criteria).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        // For a linked request, ENOENT while the target is still incomplete
        // can mean the kernel has not issued it yet: its predecessor completed
        // but the task work that issues it had not run. Schedule the one
        // allowed retry. ENOENT can also mean the target is issued but not
        // cancellable, which is why the retry is bounded. Other results end
        // the attempt: success and EALREADY mean the kernel found the target.
        let Some(retry) = self.retry.take() else {
            return;
        };
        let not_found = matches!(
            result.into_result(),
            Err(err) if err.raw_os_error() == Some(libc::ENOENT)
        );
        if not_found && !self.target.is_complete() {
            retry.schedule(self.target.clone());
        }
    }
}

pin_project_lite::pin_project! {
    /// A lazily-submitted `io_uring` operation.
    #[must_use = "future does nothing unless you `.await` or poll them"]
    pub struct Op<T>
    where
        T: 'static,
    {
        state: State<T>,
        reactor: crate::Handle,
        // The request linked immediately before this one, when it was
        // submitted as a later member of a linked batch.
        predecessor: Option<OpTarget>,
    }

    impl<T> PinnedDrop for Op<T> where T: 'static {
        fn drop(me: Pin<&mut Self>) {
            let this = me.project();
            match this.state {
                State::Submitted { inner } => {
                    if inner.needs_cancel() {
                        // Nothing will poll this future again, so its later
                        // completions must not wake the task that dropped it.
                        drop(inner.inner.inner.header().take_waker());
                        this.reactor.cancel_op(
                            OpTarget {
                                handle: inner.inner.inner.clone(),
                            },
                            this.predecessor.take(),
                        );
                    }
                }
                State::Prepared { .. } | State::Waiting { .. } | State::Done => {}
            }
        }
    }
}

/// Where an [`Op`] is in submission.
///
/// An operation is configured when it is first submitted, not when it is
/// created, so its SQE moves from [`Operation::configure`] straight toward the
/// submission queue instead of being carried in every operation future.
enum State<T>
where
    T: 'static,
{
    /// Not configured yet.
    Prepared { handle: TypedHandle<T> },
    /// Configured as a member of a linked batch that has not been handed to
    /// the driver yet.
    Waiting { handle: TypedHandle<T> },
    /// Handed to the driver, which pushes it to the SQ or queues it until there
    /// is room, or completed without the kernel by a synthetic completion.
    Submitted { inner: SubmittedOp<T> },
    /// Placeholder while moving between states.
    Done,
}

impl<T> State<T>
where
    T: Operation + 'static,
{
    /// Configure a prepared operation's entry.
    ///
    /// A configured operation waits for its entry to be queued. A configuration
    /// failure instead completes it synthetically, so nothing reaches the
    /// kernel. Returns `None` for that failure and for any state other than
    /// prepared.
    fn configure(&mut self) -> Option<ConfiguredEntry> {
        if !matches!(self, State::Prepared { .. }) {
            return None;
        }
        let State::Prepared { handle } = mem::replace(self, State::Done) else {
            unreachable!("operation state changed during configuration");
        };
        match handle.configure() {
            Ok(entry) => {
                *self = State::Waiting { handle };
                Some(entry)
            }
            Err(err) => {
                Self::complete_synthetic(self, handle, err);
                None
            }
        }
    }

    fn prepare_batch(&mut self, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        match self.configure() {
            Some(entry) => {
                batch.push(entry);
                true
            }
            None => false,
        }
    }

    fn cancel_unsubmitted(&mut self) -> bool {
        if !matches!(self, State::Prepared { .. }) {
            return false;
        }
        let State::Prepared { handle } = mem::replace(self, State::Done) else {
            unreachable!("operation state changed during cancellation");
        };
        Self::complete_synthetic(self, handle, io::Error::from_raw_os_error(libc::ECANCELED));
        true
    }

    fn finish_submit(&mut self) {
        if !matches!(self, State::Waiting { .. }) {
            return;
        }
        let State::Waiting { handle } = mem::replace(self, State::Done) else {
            unreachable!("operation state changed during submission");
        };
        *self = State::Submitted {
            inner: SubmittedOp { inner: handle },
        };
    }

    fn fail_submit(&mut self, err: &SubmitError) {
        if !matches!(self, State::Waiting { .. }) {
            return;
        }
        let State::Waiting { handle } = mem::replace(self, State::Done) else {
            unreachable!("operation state changed during submission");
        };
        Self::complete_synthetic(self, handle, err.to_io_error());
    }

    /// Complete an operation whose entry never reached the kernel.
    fn complete_synthetic(state: &mut Self, handle: TypedHandle<T>, err: io::Error) {
        // Safety: no entry for `handle`'s operation reached the kernel, and every
        // caller leaves the operation submitted, so this is its only terminal
        // completion.
        unsafe { handle.untyped().reap(CQEResult::synthetic(Err(err))) };
        *state = State::Submitted {
            inner: SubmittedOp { inner: handle },
        };
    }
}

impl<T> Op<T>
where
    T: Operation + 'static,
{
    pub(crate) fn new(data: T, reactor: crate::Handle) -> Self {
        Self {
            state: State::Prepared {
                handle: TypedHandle::new(data),
            },
            reactor,
            predecessor: None,
        }
    }

    pub(crate) fn new_with_target(data: T, reactor: crate::Handle) -> (Self, OpTarget) {
        let operation = Self::new(data, reactor);
        let State::Prepared { handle } = &operation.state else {
            unreachable!("new operation is not prepared");
        };
        let target = OpTarget {
            handle: handle.untyped(),
        };
        (operation, target)
    }

    pub(crate) fn handle(&self) -> &crate::Handle {
        &self.reactor
    }

    pub(crate) fn prepare_batch(
        mut self: Pin<&mut Self>,
        batch: &mut SmallVec<[ConfiguredEntry; 4]>,
    ) -> bool {
        let this = self.as_mut().project();
        let predecessor = batch.last().map(|entry| OpTarget {
            handle: entry.handle.clone(),
        });
        let len = batch.len();
        let can_continue = this.state.prepare_batch(batch);
        if batch.len() > len {
            *this.predecessor = predecessor;
        }
        can_continue
    }

    pub(crate) fn cancel_unsubmitted(mut self: Pin<&mut Self>) {
        let this = self.as_mut().project();
        let _ = this.state.cancel_unsubmitted();
    }

    pub(crate) fn finish_submit(mut self: Pin<&mut Self>) {
        let this = self.as_mut().project();
        this.state.finish_submit();
    }

    pub(crate) fn fail_submit(mut self: Pin<&mut Self>, err: &SubmitError) {
        let this = self.as_mut().project();
        this.state.fail_submit(err);
    }

    /// Configure and submit a prepared operation; otherwise do nothing.
    fn submit_if_prepared(self: Pin<&mut Self>) {
        let this = self.project();
        if let Some(entry) = this.state.configure() {
            match this.reactor.submit_entry(entry) {
                Ok(()) => this.state.finish_submit(),
                Err(err) => this.state.fail_submit(&err),
            }
        }
    }
}

impl<T> Future for Op<T>
where
    T: Singleshot + 'static,
{
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.as_mut().submit_if_prepared();
        let this = self.project();
        let State::Submitted { inner } = this.state else {
            unreachable!("operation not submitted");
        };
        if let Some(result) = inner.try_complete() {
            return Poll::Ready(result);
        }
        inner.inner.register_waker(cx.waker());
        Poll::Pending
    }
}

impl<T> futures_core::Stream for Op<T>
where
    T: Multishot + 'static,
{
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.as_mut().submit_if_prepared();
        let this = self.project();
        let State::Submitted { inner } = this.state else {
            unreachable!("operation not submitted");
        };
        if let Some(result) = inner.try_next() {
            return Poll::Ready(Some(result));
        }
        if inner.inner.is_complete() {
            return Poll::Ready(None);
        }
        inner.inner.register_waker(cx.waker());
        Poll::Pending
    }
}

/// A typed reference to an operation in progress.
pub(crate) struct TypedHandle<T> {
    inner: RawOpRef,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TypedHandle<T>
where
    T: Operation + 'static,
{
    pub(crate) fn new(data: T) -> Self {
        let ptr = raw::RawOp::<T>::allocate(data);
        Self {
            inner: RawOpRef::from(ptr),
            _marker: std::marker::PhantomData,
        }
    }

    /// Return an untyped [`RawOpRef`] for this operation.
    #[inline]
    pub(crate) fn untyped(&self) -> RawOpRef {
        self.inner.clone()
    }

    /// Configure this operation's entry, which carries a kernel reference.
    fn configure(&self) -> io::Result<ConfiguredEntry> {
        let mut data = self.data_mut().expect("operation already completed");
        // Check the entry by reference: threading it through `Result`
        // combinators moves the 64-byte SQE several times on the hot path.
        let entry = match T::configure(&mut data) {
            Ok(entry) if has_reserved_flags(&entry) => Err(reserved_flags_error()),
            Ok(entry) => Ok(ConfiguredEntry::new(self.untyped(), entry)),
            Err(err) => Err(err),
        };
        drop(data);
        entry
    }

    fn raw(&self) -> &raw::RawOp<T> {
        // Safety: `inner` retains the allocation and every `RawOp<T>` begins with its Header.
        unsafe { raw::RawOp::<T>::from_raw_header(self.inner.inner()).as_ref() }
    }

    fn pop_completion(&self) -> Option<(T::Completion, bool)> {
        let mut state = self.raw().state().borrow_mut();
        let completion = state.completions.pop_front()?;
        // The terminal CQE is the final entry for an operation. Before it has
        // arrived every queued completion is non-terminal; after it arrives,
        // only the queue tail can be terminal.
        let more = !self.is_complete() || !state.completions.is_empty();
        Some((completion, more))
    }

    /// Returns true if this operation is complete.
    fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }

    /// Returns a mutable reference to the data associated with this operation.
    fn data_mut(&self) -> Option<RefMut<'_, T>> {
        let state = self.raw().state().borrow_mut();
        state.data.as_ref()?;
        Some(RefMut::map(state, |state| {
            state.data.as_mut().expect("operation data disappeared")
        }))
    }

    /// Attempt to take the data from this operation.
    ///
    /// This will succeed if the operation is complete.
    ///
    fn try_take(&self) -> Option<T> {
        if !self.is_complete() {
            return None;
        }
        self.raw().state().borrow_mut().data.take()
    }

    fn register_waker(&self, waker: &Waker) {
        let header = self.inner.header();
        header.set_waker(waker);
    }
}

/// Reap one operation completion.
///
/// # Safety
///
/// `entry` must be an unreaped CQE whose `user_data` retains the operation that produced it.
#[inline]
pub(crate) unsafe fn reap_operation(entry: &io_uring::cqueue::Entry) {
    assert!(entry.user_data() > 1024);
    let handle = RawOpRef::from_raw_usize(entry.user_data() as usize);
    let result = decode_cqe_result(entry.result(), entry.flags());
    let result = CQEResult::from_cqe(result, entry.flags(), entry.user_data() as usize);
    // Safety: `handle` was reconstructed from this CQE's runtime-owned
    // `user_data`; draining the CQ invokes this path exactly once per entry.
    unsafe { handle.reap(result) };
}

/// SQE flags that only the runtime may set.
///
/// A link joins an entry to the next SQE in the shared queue, which may belong
/// to another task; the driver sets `IO_LINK` itself for requests submitted
/// together. A request that skips its success CQE never releases its operation
/// or leaves the in-flight count that driver shutdown drains to zero, and once
/// one is submitted the kernel rejects every later `IO_DRAIN` entry.
const RESERVED_SQE_FLAGS: Flags = Flags::IO_LINK
    .union(Flags::IO_HARDLINK)
    .union(Flags::SKIP_SUCCESS);

/// Whether a configured SQE sets a runtime-owned flag.
#[inline]
fn has_reserved_flags(entry: &io_uring::squeue::Entry) -> bool {
    sqe_flags(entry).intersects(RESERVED_SQE_FLAGS)
}

#[cold]
#[inline(never)]
fn reserved_flags_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "operations may not set IO_LINK, IO_HARDLINK, or CQE_SKIP_SUCCESS; \
         link requests with Request::then",
    )
}

/// Return the flags byte of a configured SQE.
fn sqe_flags(entry: &io_uring::squeue::Entry) -> Flags {
    const _: () = assert!(mem::size_of::<io_uring::squeue::Entry>() == 64);
    // Safety: `squeue::Entry` is a `repr(C)` wrapper of the kernel's
    // `io_uring_sqe`, which stores its `u8` flags at byte offset 1.
    let flags = unsafe { std::ptr::from_ref(entry).cast::<u8>().add(1).read() };
    Flags::from_bits_retain(flags)
}

fn decode_cqe_result(result: i32, flags: u32) -> io::Result<u32> {
    if result >= 0 || io_uring::cqueue::notif(flags) {
        Ok(result as u32)
    } else {
        Err(io::Error::from_raw_os_error(-result))
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct SubmittedOp<T> {
    inner: TypedHandle<T>,
}

impl<T> SubmittedOp<T> {
    fn needs_cancel(&self) -> bool {
        // A terminal CQE ends kernel ownership. Cancelling after that point is
        // redundant, and the allocation's numeric user_data may be reused
        // before a queued cancellation reaches the kernel.
        !self.inner.inner.is_complete()
    }
}

impl<T> SubmittedOp<T>
where
    T: Operation + 'static,
{
    fn try_complete(&mut self) -> Option<T::Output>
    where
        T: Singleshot,
    {
        if !self.inner.is_complete() {
            return None;
        }
        let mut data = self.inner.try_take().expect("operation already completed");
        loop {
            let (completion, more) = self
                .inner
                .pop_completion()
                .expect("terminal operation missing completion");
            if more {
                data.update(completion);
                continue;
            }
            return Some(data.complete(completion));
        }
    }

    fn try_next(&mut self) -> Option<T::Item>
    where
        T: Multishot,
    {
        let (completion, more) = self.inner.pop_completion()?;
        if more {
            let mut data = self.inner.data_mut().expect("operation already completed");
            return Some(data.update(completion));
        }
        let data = self.inner.try_take().expect("operation already completed");
        data.complete(completion)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::{Poll, Wake, Waker};

    use super::*;

    fn notif_flag() -> u32 {
        (0..=u32::MAX)
            .find(|flags| io_uring::cqueue::notif(*flags))
            .expect("missing CQE notification flag")
    }

    #[test]
    fn notification_result_preserves_usage_bits() {
        assert_eq!(
            decode_cqe_result(i32::MIN, notif_flag()).unwrap(),
            i32::MIN as u32
        );
    }

    #[test]
    fn ordinary_negative_result_remains_an_error() {
        assert_eq!(
            decode_cqe_result(-libc::EINVAL, 0)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EINVAL)
        );
    }

    #[test]
    fn sqe_flags_reads_each_configured_flag() {
        for flags in [
            Flags::empty(),
            Flags::FIXED_FILE,
            Flags::IO_DRAIN,
            Flags::IO_LINK,
            Flags::IO_HARDLINK,
            Flags::ASYNC,
            Flags::BUFFER_SELECT,
            Flags::SKIP_SUCCESS,
            Flags::all(),
        ] {
            let entry = io_uring::opcode::Nop::new()
                .build()
                .flags(flags)
                .user_data(u64::MAX);
            assert_eq!(sqe_flags(&entry), flags);
        }
    }

    #[test]
    fn only_runtime_owned_flags_fail_configuration() {
        for reserved in [Flags::IO_LINK, Flags::IO_HARDLINK, Flags::SKIP_SUCCESS] {
            let entry = io_uring::opcode::Nop::new()
                .build()
                .flags(reserved | Flags::ASYNC);
            assert!(has_reserved_flags(&entry), "{reserved:?} must be rejected");
        }
        for allowed in [
            Flags::empty(),
            Flags::FIXED_FILE,
            Flags::IO_DRAIN,
            Flags::ASYNC,
            Flags::BUFFER_SELECT,
        ] {
            let entry = io_uring::opcode::Nop::new().build().flags(allowed);
            assert!(!has_reserved_flags(&entry), "{allowed:?} must be accepted");
        }
        assert_eq!(reserved_flags_error().kind(), io::ErrorKind::InvalidInput);
    }

    #[derive(Debug, Default)]
    struct TestOp(Vec<u32>);
    unsafe impl Operation for TestOp {
        type Completion = CQEResult;

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            self.0.push(*result.result.as_ref().unwrap());
            result
        }

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            unimplemented!()
        }
    }

    #[test]
    fn complete_op() {
        let op = TypedHandle::new(TestOp::default());
        let handle = op.untyped();

        // Safety: the test supplies `handle`'s unreaped terminal completion.
        unsafe { handle.reap(CQEResult::new(Ok(0), 0)) };

        assert!(op.is_complete());
        assert_eq!(&op.data_mut().unwrap().0, &[0]);
    }

    #[test]
    fn complete_op_through_usize() {
        let op = TypedHandle::new(TestOp::default());
        let handle = op.untyped();

        let handle_usize = handle.into_raw_usize();
        // Safety: `handle_usize` retains the operation's kernel reference, and
        // the test supplies its unreaped terminal completion.
        unsafe {
            RawOpRef::from_raw_usize(handle_usize).reap(CQEResult::new(Ok(0), 0));
        }

        assert!(op.is_complete());
        assert_eq!(&op.data_mut().unwrap().0, &[0]);
    }

    #[test]
    fn drop_op_through_usize() {
        let op = TypedHandle::new(TestOp::default());
        let handle = op.untyped();

        let handle_usize = handle.into_raw_usize();
        let handle = unsafe { RawOpRef::from_raw_usize(handle_usize) };
        drop(handle);
    }

    #[test]
    fn op_target_retains_operation_identity_and_allocation() {
        #[derive(Debug)]
        struct DropTracked(Rc<Cell<bool>>);

        impl Drop for DropTracked {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        unsafe impl Operation for DropTracked {
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                unreachable!("the target lifetime test does not configure an SQE")
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
        }

        let dropped = Rc::new(Cell::new(false));
        let typed = TypedHandle::new(DropTracked(Rc::clone(&dropped)));
        let target = OpTarget {
            handle: typed.untyped(),
        };
        let user_data = target.user_data();

        drop(typed);
        assert!(
            !dropped.get(),
            "target must retain the operation allocation"
        );
        assert_eq!(target.user_data(), user_data);

        drop(target);
        assert!(
            dropped.get(),
            "last target reference must release the operation"
        );
    }

    #[derive(Debug, Default)]
    struct TestMultishot;

    unsafe impl Operation for TestMultishot {
        type Completion = CQEResult;

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            unimplemented!()
        }
    }

    impl Multishot for TestMultishot {
        type Item = u32;

        fn update(&mut self, result: CQEResult) -> Self::Item {
            result.result.unwrap()
        }

        fn complete(self, result: CQEResult) -> Option<Self::Item> {
            Some(result.result.unwrap())
        }
    }

    #[derive(Debug, Default)]
    struct TestSingleshot(Vec<u32>);

    unsafe impl Operation for TestSingleshot {
        type Completion = CQEResult;

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            unimplemented!()
        }
    }

    impl Singleshot for TestSingleshot {
        type Output = Vec<u32>;

        fn update(&mut self, result: CQEResult) {
            self.0.push(result.into_result().unwrap());
        }

        fn complete(mut self, result: CQEResult) -> Self::Output {
            self.0.push(result.into_result().unwrap());
            self.0
        }
    }

    fn more_flag() -> u32 {
        (0..=u32::MAX)
            .find(|flags| io_uring::cqueue::more(*flags))
            .expect("missing CQE more flag")
    }

    thread_local! {
        static WAKE_ACTION: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None);
    }

    struct TestWake;

    impl Wake for TestWake {
        fn wake(self: Arc<Self>) {
            Self::run();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            Self::run();
        }
    }

    impl TestWake {
        fn run() {
            WAKE_ACTION.with(|action| {
                action.borrow_mut().as_mut().expect("wake action missing")();
            });
        }
    }

    fn test_waker(action: impl FnMut() + 'static) -> Waker {
        WAKE_ACTION.with(|slot| {
            assert!(slot.borrow().is_none(), "wake action already installed");
            *slot.borrow_mut() = Some(Box::new(action));
        });
        Waker::from(Arc::new(TestWake))
    }

    fn clear_wake_action() {
        WAKE_ACTION.with(|slot| *slot.borrow_mut() = None);
    }

    #[test]
    fn multishot_completions_are_fifo() {
        let typed = TypedHandle::new(TestMultishot);
        let more = more_flag();
        // Safety: the test supplies each modeled CQE once, in completion order.
        unsafe {
            typed.untyped().reap(CQEResult::new(Ok(10), more));
            typed.untyped().reap(CQEResult::new(Ok(20), more));
            typed.untyped().reap(CQEResult::new(Ok(30), 0));
        }

        let mut submitted = SubmittedOp { inner: typed };

        assert_eq!(submitted.try_next(), Some(10));
        assert_eq!(submitted.try_next(), Some(20));
        assert_eq!(submitted.try_next(), Some(30));
        assert_eq!(submitted.try_next(), None);
        assert!(submitted.inner.is_complete());
    }

    #[test]
    fn singleshot_consumes_one_terminal_completion_directly() {
        let typed = TypedHandle::new(TestSingleshot::default());
        // Safety: the test supplies `typed`'s unreaped terminal completion.
        unsafe { typed.untyped().reap(CQEResult::new(Ok(30), 0)) };
        let mut submitted = SubmittedOp { inner: typed };

        assert_eq!(submitted.try_complete(), Some(vec![30]));
    }

    #[test]
    fn singleshot_preserves_multiple_completion_order() {
        let typed = TypedHandle::new(TestSingleshot::default());
        let kernel_ref = typed.untyped().into_raw_usize();
        for (value, flags) in [(10, more_flag()), (20, more_flag()), (30, 0)] {
            // Safety: `kernel_ref` retains this operation across MORE
            // completions, and each modeled CQE is reaped once in order.
            unsafe {
                RawOpRef::from_raw_usize(kernel_ref).reap(CQEResult::new(Ok(value), flags));
            }
        }
        let mut submitted = SubmittedOp { inner: typed };

        assert_eq!(submitted.try_complete(), Some(vec![10, 20, 30]));
    }

    #[test]
    fn submitted_op_needs_cancel_until_terminal_completion() {
        let typed = TypedHandle::new(TestOp::default());
        let completion = typed.untyped();
        let submitted = SubmittedOp { inner: typed };

        assert!(submitted.needs_cancel());
        // Safety: the test supplies `completion`'s unreaped terminal completion.
        unsafe { completion.reap(CQEResult::new(Ok(0), 0)) };
        assert!(!submitted.needs_cancel());
    }

    #[test]
    fn multishot_more_completion_still_needs_cancel() {
        let typed = TypedHandle::new(TestMultishot);
        let completion = typed.untyped();
        let submitted = SubmittedOp { inner: typed };

        // Safety: the test supplies this operation's MORE and terminal
        // completions once, in completion order.
        unsafe {
            completion.clone().reap(CQEResult::new(Ok(10), more_flag()));
        }
        assert!(submitted.needs_cancel());
        unsafe { completion.reap(CQEResult::new(Ok(20), 0)) };
        assert!(!submitted.needs_cancel());
    }

    #[test]
    fn dropped_multishot_drops_reaped_completions_once_in_fifo_order() {
        const OPERATION_DROP: u32 = u32::MAX;

        struct DropTrackedCompletion {
            value: u32,
            dropped: Rc<RefCell<Vec<u32>>>,
        }

        impl Drop for DropTrackedCompletion {
            fn drop(&mut self) {
                self.dropped.borrow_mut().push(self.value);
            }
        }

        struct ReapTrackedMultishot(Rc<RefCell<Vec<u32>>>);

        impl Drop for ReapTrackedMultishot {
            fn drop(&mut self) {
                self.0.borrow_mut().push(OPERATION_DROP);
            }
        }

        unsafe impl Operation for ReapTrackedMultishot {
            type Completion = DropTrackedCompletion;

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                DropTrackedCompletion {
                    value: result.into_result().unwrap(),
                    dropped: Rc::clone(&self.0),
                }
            }

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                unimplemented!()
            }
        }

        let cleaned = Rc::new(RefCell::new(Vec::new()));
        let typed = TypedHandle::new(ReapTrackedMultishot(Rc::clone(&cleaned)));
        let kernel_ref = typed.untyped().into_raw_usize();
        for value in [10, 20] {
            // Safety: `kernel_ref` retains this operation across MORE
            // completions, and each modeled completion is reaped once.
            unsafe {
                RawOpRef::from_raw_usize(kernel_ref).reap(CQEResult::new(Ok(value), more_flag()));
            }
        }

        drop(typed);
        assert!(cleaned.borrow().is_empty());

        // Safety: the test supplies the unreaped terminal completion retained
        // by `kernel_ref`.
        unsafe {
            RawOpRef::from_raw_usize(kernel_ref).reap(CQEResult::new(Ok(30), 0));
        }

        assert_eq!(&*cleaned.borrow(), &[10, 20, 30, OPERATION_DROP]);
    }

    #[test]
    fn multishot_more_completion_survives_panicking_waker() {
        let typed = TypedHandle::new(TestMultishot);
        let kernel_ref = typed.untyped().into_raw_usize();
        let waker = test_waker(|| panic!("wake panic"));
        typed.register_waker(&waker);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safety: the test supplies the unreaped MORE completion retained
            // by `kernel_ref`.
            unsafe {
                RawOpRef::from_raw_usize(kernel_ref).reap(CQEResult::new(Ok(10), more_flag()));
            }
        }));
        assert!(result.is_err());
        clear_wake_action();

        assert_eq!(typed.inner.header().refcount(), 2);

        // Safety: the test supplies the unreaped terminal completion retained
        // by `kernel_ref`.
        unsafe {
            RawOpRef::from_raw_usize(kernel_ref).reap(CQEResult::new(Ok(20), 0));
        }
        assert_eq!(typed.inner.header().refcount(), 1);

        let mut submitted = SubmittedOp { inner: typed };
        assert_eq!(submitted.try_next(), Some(10));
        assert_eq!(submitted.try_next(), Some(20));
        assert_eq!(submitted.try_next(), None);
    }

    #[test]
    fn multishot_waker_can_poll_synchronously() {
        let typed = TypedHandle::new(TestMultishot);
        let kernel_ref = typed.untyped().into_raw_usize();
        let submitted = Rc::new(RefCell::new(SubmittedOp { inner: typed }));
        let observed = Rc::new(Cell::new(None));
        let waker = test_waker({
            let submitted = Rc::clone(&submitted);
            let observed = Rc::clone(&observed);
            move || observed.set(submitted.borrow_mut().try_next())
        });
        submitted.borrow().inner.register_waker(&waker);

        // Safety: the test supplies the unreaped MORE completion retained by
        // `kernel_ref`.
        unsafe {
            RawOpRef::from_raw_usize(kernel_ref).reap(CQEResult::new(Ok(10), more_flag()));
        }
        clear_wake_action();

        assert_eq!(observed.get(), Some(10));
        assert_eq!(submitted.borrow().inner.inner.header().refcount(), 2);

        // Safety: the test supplies the unreaped terminal completion retained
        // by `kernel_ref`.
        unsafe {
            RawOpRef::from_raw_usize(kernel_ref).reap(CQEResult::new(Ok(20), 0));
        }
        assert_eq!(submitted.borrow_mut().try_next(), Some(20));
        assert_eq!(submitted.borrow_mut().try_next(), None);
        assert_eq!(submitted.borrow().inner.inner.header().refcount(), 1);
    }

    #[test]
    fn terminal_waker_can_complete_and_drop_operation_synchronously() {
        let typed = TypedHandle::new(TestSingleshot::default());
        let terminal_ref = typed.untyped();
        let submitted = Rc::new(RefCell::new(Some(SubmittedOp { inner: typed })));
        let observed = Rc::new(RefCell::new(None));
        let waker = test_waker({
            let submitted = Rc::clone(&submitted);
            let observed = Rc::clone(&observed);
            move || {
                let mut submitted = submitted
                    .borrow_mut()
                    .take()
                    .expect("submitted operation missing during wake");
                *observed.borrow_mut() = submitted.try_complete();
                drop(submitted);
            }
        });
        submitted
            .borrow()
            .as_ref()
            .unwrap()
            .inner
            .register_waker(&waker);

        // Safety: the test supplies `terminal_ref`'s unreaped terminal completion.
        unsafe { terminal_ref.reap(CQEResult::new(Ok(30), 0)) };
        clear_wake_action();

        assert!(submitted.borrow().is_none());
        assert_eq!(observed.borrow().as_deref(), Some([30].as_slice()));
    }

    #[test]
    fn terminal_completion_survives_panicking_waker() {
        let typed = TypedHandle::new(TestSingleshot::default());
        let terminal_ref = typed.untyped();
        let waker = test_waker(|| panic!("wake panic"));
        typed.register_waker(&waker);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Safety: the test supplies `terminal_ref`'s unreaped terminal completion.
            unsafe { terminal_ref.reap(CQEResult::new(Ok(30), 0)) };
        }));
        assert!(result.is_err());
        clear_wake_action();

        assert!(typed.is_complete());
        assert_eq!(typed.inner.header().refcount(), 1);
        let mut submitted = SubmittedOp { inner: typed };
        assert_eq!(submitted.try_complete(), Some(vec![30]));
    }

    #[test]
    fn panicking_reap_aborts_process() {
        const CHILD_ENV: &str = "NORN_URING_PANICKING_REAP_CHILD";
        const TEST_NAME: &str = "operation::tests::panicking_reap_aborts_process";

        if std::env::var_os(CHILD_ENV).is_some() {
            struct PanickingReap;

            unsafe impl Operation for PanickingReap {
                type Completion = ();

                fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                    unreachable!("death test does not configure an SQE")
                }

                unsafe fn reap(&mut self, _result: CQEResult) -> Self::Completion {
                    panic!("reap panic")
                }
            }

            let typed = TypedHandle::new(PanickingReap);
            // Safety: the test supplies `typed`'s unreaped terminal completion.
            unsafe { typed.untyped().reap(CQEResult::new(Ok(0), 0)) };
            unreachable!("panicking reap must abort")
        }

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg(TEST_NAME)
            .arg("--exact")
            .arg("--test-threads=1")
            .env(CHILD_ENV, "1")
            .status()
            .expect("failed to spawn reap death test");

        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(libc::SIGABRT));
    }

    #[test]
    fn submit_failure_returns_error_instead_of_panicking() {
        #[derive(Debug)]
        struct SubmitFailureOp;

        unsafe impl Operation for SubmitFailureOp {
            type Completion = CQEResult;

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }
        }

        impl Singleshot for SubmitFailureOp {
            type Output = (bool, io::Result<()>);

            fn complete(self, result: CQEResult) -> Self::Output {
                let synthetic = result.is_synthetic();
                (synthetic, result.result.map(drop))
            }
        }

        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        norn_executor::park::Park::shutdown(&mut driver);

        let mut op = std::pin::pin!(handle.submit(SubmitFailureOp));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Future::poll(op.as_mut(), &mut cx)
        }))
        .expect("poll should not panic");

        match poll_result {
            Poll::Ready((true, Err(_))) => {}
            other => panic!("expected ready error, got: {other:?}"),
        }
    }

    #[test]
    fn configuration_failure_completes_without_submitting() {
        #[derive(Debug)]
        struct ConfigurationFailureOp;

        unsafe impl Operation for ConfigurationFailureOp {
            type Completion = CQEResult;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid op"))
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }
        }

        impl Singleshot for ConfigurationFailureOp {
            type Output = (bool, io::Result<()>);

            fn complete(self, result: CQEResult) -> Self::Output {
                let synthetic = result.is_synthetic();
                (synthetic, result.result.map(drop))
            }
        }

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut op = std::pin::pin!(handle.submit(ConfigurationFailureOp));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let Poll::Ready((synthetic, Err(err))) = Future::poll(op.as_mut(), &mut cx) else {
            panic!("configuration failure should complete immediately")
        };
        assert!(synthetic);
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn first_poll_hands_the_entry_to_the_driver() {
        #[derive(Debug)]
        struct NopOp;

        unsafe impl Operation for NopOp {
            type Completion = CQEResult;

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }
        }

        impl Singleshot for NopOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut op = std::pin::pin!(handle.submit(NopOp));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
        assert!(matches!(
            op.as_ref().get_ref().state,
            State::Submitted { .. }
        ));
    }

    #[derive(Debug)]
    struct CountedNop(Rc<Cell<usize>>);

    unsafe impl Operation for CountedNop {
        type Completion = CQEResult;

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            self.0.set(self.0.get() + 1);
            Ok(io_uring::opcode::Nop::new().build())
        }

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }
    }

    impl Singleshot for CountedNop {
        type Output = io::Result<u32>;

        fn complete(self, result: CQEResult) -> Self::Output {
            result.into_result()
        }
    }

    #[test]
    fn operation_is_configured_once_when_first_polled() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let configured = Rc::new(Cell::new(0));

        drop(handle.submit(CountedNop(Rc::clone(&configured))));
        assert_eq!(
            configured.get(),
            0,
            "a dropped operation must not be configured"
        );

        let mut op = std::pin::pin!(handle.submit(CountedNop(Rc::clone(&configured))));
        assert_eq!(
            configured.get(),
            0,
            "creating an operation must not configure it"
        );
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
        assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
        assert_eq!(configured.get(), 1);
    }

    #[test]
    fn queued_operation_is_configured_once_and_submitted_after_backpressure() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        let configured = Rc::new(Cell::new(0));

        let results = ex.block_on(async {
            let mut ops = Vec::new();
            for _ in 0..4 {
                ops.push(Box::pin(handle.submit(CountedNop(Rc::clone(&configured)))));
            }
            // Poll every operation once against a one-entry SQ so that later
            // ones wait in the driver's queue with their entries configured.
            let waker = futures_test::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            for op in &mut ops {
                assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
            }
            assert_eq!(handle.queued_entries(), 3, "the full SQ queues the rest");
            let mut results = Vec::new();
            for op in ops {
                results.push(op.await);
            }
            results
        });

        assert!(results.iter().all(|result| matches!(result, Ok(0))));
        assert_eq!(configured.get(), 4);
    }

    #[test]
    fn operation_future_holds_neither_its_sqe_nor_a_push_future() {
        assert!(
            mem::size_of::<Op<CountedNop>>() <= 4 * mem::size_of::<usize>(),
            "an Op holds its handle, driver handle and predecessor: {} bytes",
            mem::size_of::<Op<CountedNop>>()
        );
    }

    #[test]
    fn multishot_terminal_completion_is_not_sent_to_update() {
        #[derive(Debug)]
        struct TerminalMultishot {
            updates: Rc<Cell<usize>>,
            complete: Rc<Cell<usize>>,
        }

        unsafe impl Operation for TerminalMultishot {
            type Completion = CQEResult;

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                result
            }

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                unimplemented!()
            }
        }

        impl Multishot for TerminalMultishot {
            type Item = ();

            fn update(&mut self, result: CQEResult) -> Self::Item {
                assert!(result.more(), "terminal completion must not call update");
                self.updates.set(self.updates.get() + 1);
            }

            fn complete(self, result: CQEResult) -> Option<Self::Item> {
                assert!(!result.more());
                self.complete.set(self.complete.get() + 1);
                None
            }
        }

        let updates = Rc::new(Cell::new(0));
        let complete = Rc::new(Cell::new(0));
        let typed = TypedHandle::new(TerminalMultishot {
            updates: Rc::clone(&updates),
            complete: Rc::clone(&complete),
        });
        // Safety: the test supplies `typed`'s unreaped terminal cancellation
        // completion.
        unsafe {
            typed.untyped().reap(CQEResult::new(
                Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                0,
            ));
        }

        let mut submitted = SubmittedOp { inner: typed };
        assert_eq!(submitted.try_next(), None);
        assert_eq!(updates.get(), 0);
        assert_eq!(complete.get(), 1);
        assert!(submitted.inner.is_complete());
    }
}
