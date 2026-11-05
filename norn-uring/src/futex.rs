//! Process-private futex waits and wakes submitted through `io_uring`.
//!
//! A [`Futex`] owns a shared 32-bit futex word. Runtime tasks can wait for the
//! word to be woken without blocking the runtime thread, and can wake waiters
//! blocked in other threads without entering the kernel separately from the
//! driver's next submission.
//!
//! The kernel opcodes require Linux 6.7 or newer. Older kernels complete the
//! requests with `EINVAL`.
//!
//! # Scope
//!
//! Every request uses `FUTEX2_PRIVATE` with a 32-bit word. Private futexes
//! interoperate with `futex(2)` `FUTEX_*_PRIVATE` operations on the same
//! address from any thread of this process, but not with other processes.
//! Process-shared, NUMA, and non-32-bit futexes are not exposed because their
//! storage and kernel-write semantics differ.
//!
//! # Example
//!
//! A runtime task waits for another thread to publish a value and wake it
//! with `futex(2)`.
//!
//! ```no_run
//! use std::sync::atomic::Ordering;
//!
//! use norn_executor::LocalExecutor;
//! use norn_uring::futex::Futex;
//!
//! let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64)?;
//! let mut executor = LocalExecutor::new(driver);
//! let futex = Futex::new(0);
//! let publisher = futex.clone();
//! let thread = std::thread::spawn(move || {
//!     publisher.store(1, Ordering::Release);
//!     // Safety: the futex word is a live, aligned `u32`.
//!     unsafe {
//!         libc::syscall(
//!             libc::SYS_futex,
//!             publisher.as_ptr(),
//!             libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
//!             i32::MAX,
//!         );
//!     }
//! });
//! executor.block_on(async {
//!     while futex.load(Ordering::Acquire) == 0 {
//!         // Mismatches and spurious wakes are handled by re-checking the word.
//!         futex.wait(0).await?;
//!     }
//!     Ok::<(), std::io::Error>(())
//! })?;
//! thread.join().unwrap();
//! # Ok::<(), std::io::Error>(())
//! ```

use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use io_uring::{opcode, types};

use crate::operation::{CQEResult, Operation, Singleshot};

/// `FUTEX2_SIZE_U32` from `<linux/futex.h>`.
const FUTEX2_SIZE_U32: u32 = 0x02;
/// `FUTEX2_PRIVATE` from `<linux/futex.h>`.
const FUTEX2_PRIVATE: u32 = 128;
/// The only futex2 flags Norn submits.
const FUTEX_FLAGS: u32 = FUTEX2_SIZE_U32 | FUTEX2_PRIVATE;
/// `FUTEX_WAITV_MAX` from `<linux/futex.h>`.
const FUTEX_WAITV_MAX: usize = 128;
/// The kernel interprets the wake count as a C `int`.
const MAX_WAKE_COUNT: u32 = i32::MAX as u32;

/// A bitset that matches every waiter, as `FUTEX_BITSET_MATCH_ANY` does for
/// 32-bit futexes.
pub const MATCH_ANY: u32 = u32::MAX;

/// A shared, process-private 32-bit futex word.
///
/// Clones refer to the same word. The word is reference-counted, so a
/// submitted request keeps it at a stable address until its terminal
/// completion even if every other clone is dropped. The handle is `Send` and
/// `Sync`, allowing another thread to update the word and wake runtime waiters.
///
/// The word is accessed through [`Deref`] to [`AtomicU32`]. As with any futex,
/// a waiter must re-check the word after every wait: wakes may be spurious, and
/// a wake can race with a later update.
#[derive(Clone)]
pub struct Futex {
    word: Arc<AtomicU32>,
}

impl Futex {
    /// Create a futex word holding `value`.
    pub fn new(value: u32) -> Self {
        Self::from(Arc::new(AtomicU32::new(value)))
    }

    /// Wait while the word holds `expected`.
    ///
    /// The kernel atomically compares the word with `expected` when the request
    /// is issued. If they differ, the request resolves to
    /// [`WaitOutcome::Mismatch`]; otherwise it waits until a matching wake.
    /// The kernel treats a mismatch as a failed request, so it cancels later
    /// requests linked with [`Request::then`](crate::Request::then).
    /// Attach a deadline with [`Request::timeout`](crate::Request::timeout),
    /// which reports expiry as `ECANCELED`.
    ///
    /// Dropping a submitted wait cancels it. If a wake has already released
    /// the wait, the waker counted it among the waiters it woke, so the wait
    /// passes the wake on to another waiter on the word whose bitset
    /// intersects its own. Abandoning a woken wait therefore never strands
    /// the waiters that remain; the one that receives the passed wake sees it
    /// as a spurious wake if its condition no longer holds.
    ///
    /// # Panics
    ///
    /// Panics if called outside a running driver context.
    pub fn wait(&self, expected: u32) -> impl crate::Request<Output = io::Result<WaitOutcome>> {
        self.wait_bitset(expected, MATCH_ANY)
    }

    /// Wait while the word holds `expected`, accepting only wakes whose bitset
    /// intersects `bitset`.
    ///
    /// A zero `bitset` completes with [`io::ErrorKind::InvalidInput`] without
    /// reaching the kernel. See [`Futex::wait`] for the remaining semantics.
    ///
    /// # Panics
    ///
    /// Panics if called outside a running driver context.
    pub fn wait_bitset(
        &self,
        expected: u32,
        bitset: u32,
    ) -> impl crate::Request<Output = io::Result<WaitOutcome>> {
        crate::Handle::current().submit(Wait {
            futex: self.clone(),
            expected,
            bitset,
        })
    }

    /// Wake up to `count` waiters and return the number woken.
    ///
    /// This wakes runtime waiters on any driver as well as threads blocked in
    /// private `futex(2)` waits on this word. A `count` of zero wakes nobody,
    /// and counts above `i32::MAX` are treated as `i32::MAX`.
    ///
    /// # Panics
    ///
    /// Panics if called outside a running driver context.
    pub fn wake(&self, count: u32) -> impl crate::Request<Output = io::Result<u32>> {
        self.wake_bitset(count, MATCH_ANY)
    }

    /// Wake up to `count` waiters whose bitset intersects `bitset`.
    ///
    /// A zero `bitset` completes with [`io::ErrorKind::InvalidInput`] without
    /// reaching the kernel. See [`Futex::wake`] for the remaining semantics.
    ///
    /// # Panics
    ///
    /// Panics if called outside a running driver context.
    pub fn wake_bitset(
        &self,
        count: u32,
        bitset: u32,
    ) -> impl crate::Request<Output = io::Result<u32>> {
        crate::Handle::current().submit(Wake {
            futex: self.clone(),
            count,
            bitset,
        })
    }
}

impl From<Arc<AtomicU32>> for Futex {
    /// Use an existing shared atomic as the futex word.
    fn from(word: Arc<AtomicU32>) -> Self {
        Self { word }
    }
}

impl Deref for Futex {
    type Target = AtomicU32;

    fn deref(&self) -> &AtomicU32 {
        &self.word
    }
}

impl std::fmt::Debug for Futex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Futex")
            .field("value", &self.word.load(Ordering::Relaxed))
            .finish()
    }
}

/// The successful result of a futex wait.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WaitOutcome {
    /// A wake released the waiter. The word may still hold its old value.
    Woken,
    /// The word did not hold the expected value when the wait was issued.
    Mismatch,
}

/// The successful result of [`wait_any`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WaitAnyOutcome {
    /// A wake released the waiter. `index` identifies a woken futex in the
    /// submitted list; when several were woken, it names one of them.
    Woken {
        /// The position of a woken futex in the submitted list.
        index: usize,
    },
    /// At least one word did not hold its expected value when the wait was
    /// issued.
    Mismatch,
}

/// Wait on several futexes until any one of them is woken.
///
/// Each entry pairs a futex with the value it is expected to hold. The kernel
/// compares every word when the request is issued and resolves to
/// [`WaitAnyOutcome::Mismatch`] if any differs. Only wakes with
/// [`MATCH_ANY`]-compatible bitsets are observed; per-futex bitsets are not
/// supported by the kernel's vectored wait.
///
/// An empty list, or a list longer than the kernel's limit of 128 futexes,
/// completes with [`io::ErrorKind::InvalidInput`] without reaching the kernel.
/// Dropping a submitted wait cancels it. As with [`Futex::wait`], a wait that
/// a wake has already released passes that wake on to another waiter on the
/// woken word.
///
/// # Panics
///
/// Panics if called outside a running driver context.
pub fn wait_any(
    futexes: &[(&Futex, u32)],
) -> impl crate::Request<Output = io::Result<WaitAnyOutcome>> {
    let (futexes, expected) = futexes
        .iter()
        .map(|&(futex, expected)| (futex.clone(), expected))
        .unzip();
    crate::Handle::current().submit(WaitAny {
        futexes,
        expected,
        vector: Box::default(),
    })
}

fn check_bitset(bitset: u32) -> io::Result<u64> {
    if bitset == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "futex bitset must not be zero",
        ));
    }
    Ok(bitset.into())
}

/// Return whether a completion reports the kernel's value-mismatch result.
fn is_mismatch(result: &CQEResult) -> bool {
    !result.is_synthetic()
        && matches!(&result.result, Err(err) if err.raw_os_error() == Some(libc::EAGAIN))
}

struct Wait {
    futex: Futex,
    expected: u32,
    bitset: u32,
}

/// A wake the kernel delivered to a wait.
///
/// The waker counts a released waiter in its result whether or not the
/// application observes the wait's completion. Dropping the wake without
/// [`DeliveredWake::accept`], as happens when an abandoned wait's completion is
/// discarded, passes it on with a private `futex(2)` wake of one waiter, so no
/// wake is lost to a waiter that no longer listens.
struct DeliveredWake {
    futex: Option<Futex>,
    bitset: u32,
}

impl DeliveredWake {
    fn new(futex: &Futex, bitset: u32) -> Self {
        Self {
            futex: Some(futex.clone()),
            bitset,
        }
    }

    /// Hand the wake to the application.
    fn accept(mut self) {
        self.futex = None;
    }
}

impl Drop for DeliveredWake {
    fn drop(&mut self) {
        if let Some(futex) = self.futex.take() {
            // Safety: `futex` keeps the aligned word alive for the call. A
            // wake never blocks, so this is safe to run from the completion
            // drain, and it does not touch the ring.
            unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    futex.as_ptr(),
                    libc::FUTEX_WAKE_BITSET | libc::FUTEX_PRIVATE_FLAG,
                    1,
                    std::ptr::null::<libc::timespec>(),
                    std::ptr::null::<u32>(),
                    self.bitset,
                );
            }
        }
    }
}

/// A wait's completion, with the wake it consumed when the kernel released it.
struct WaitCompletion {
    result: CQEResult,
    wake: Option<DeliveredWake>,
}

impl WaitCompletion {
    fn new(result: CQEResult, woken: impl FnOnce() -> DeliveredWake) -> Self {
        let wake = result.result.is_ok().then(woken);
        Self { result, wake }
    }

    /// Accept any delivered wake and return the completion's result.
    fn accept(self) -> CQEResult {
        if let Some(wake) = self.wake {
            wake.accept();
        }
        self.result
    }
}

// Safety: the SQE references only the futex word, which `futex` keeps
// allocated at a stable address through the terminal CQE. The kernel reads the
// word and never writes it for private, non-NUMA 32-bit futexes.
unsafe impl Operation for Wait {
    type Completion = WaitCompletion;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let bitset = check_bitset(self.bitset)?;
        Ok(opcode::FutexWait::new(
            self.futex.as_ptr(),
            self.expected.into(),
            bitset,
            FUTEX_FLAGS,
        )
        .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        WaitCompletion::new(result, || DeliveredWake::new(&self.futex, self.bitset))
    }
}

impl Singleshot for Wait {
    type Output = io::Result<WaitOutcome>;

    fn complete(self, completion: WaitCompletion) -> Self::Output {
        let result = completion.accept();
        if is_mismatch(&result) {
            return Ok(WaitOutcome::Mismatch);
        }
        result.result.map(|_| WaitOutcome::Woken)
    }
}

struct Wake {
    futex: Futex,
    count: u32,
    bitset: u32,
}

// Safety: the SQE references only the futex word, which `futex` keeps
// allocated at a stable address through the terminal CQE. Waking never
// accesses the word's contents.
unsafe impl Operation for Wake {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let bitset = check_bitset(self.bitset)?;
        let count = self.count.min(MAX_WAKE_COUNT);
        Ok(opcode::FutexWake::new(self.futex.as_ptr(), count.into(), bitset, FUTEX_FLAGS).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Wake {
    type Output = io::Result<u32>;

    fn complete(self, result: CQEResult) -> Self::Output {
        result.result
    }
}

struct WaitAny {
    futexes: Vec<Futex>,
    expected: Vec<u32>,
    vector: Box<[types::FutexWaitV]>,
}

// Safety: the SQE references `vector`, a heap allocation owned by this
// operation, and the futex words named by its entries, which `futexes` keeps
// allocated. Neither is moved, freed, or written by Rust before the terminal
// CQE. The kernel only reads them.
unsafe impl Operation for WaitAny {
    type Completion = WaitCompletion;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let len = self.futexes.len();
        if len == 0 || len > FUTEX_WAITV_MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("futex wait list must contain 1 to {FUTEX_WAITV_MAX} futexes, got {len}"),
            ));
        }
        self.vector = self
            .futexes
            .iter()
            .zip(&self.expected)
            .map(|(futex, &expected)| {
                types::FutexWaitV::new()
                    .val(expected.into())
                    .uaddr(futex.as_ptr() as u64)
                    .flags(FUTEX_FLAGS)
            })
            .collect();
        Ok(opcode::FutexWaitV::new(self.vector.as_ptr(), len as u32).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        // The vectored wait observes only `MATCH_ANY` wakes, so the delivered
        // wake is passed on the same way.
        let woken = match &result.result {
            Ok(index) => self.futexes.get(*index as usize),
            Err(_) => None,
        };
        let wake = woken.map(|futex| DeliveredWake::new(futex, MATCH_ANY));
        WaitCompletion { result, wake }
    }
}

impl Singleshot for WaitAny {
    type Output = io::Result<WaitAnyOutcome>;

    fn complete(self, completion: WaitCompletion) -> Self::Output {
        let result = completion.accept();
        if is_mismatch(&result) {
            return Ok(WaitAnyOutcome::Mismatch);
        }
        let index = result.result? as usize;
        debug_assert!(index < self.futexes.len());
        Ok(WaitAnyOutcome::Woken { index })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eagain() -> io::Error {
        io::Error::from_raw_os_error(libc::EAGAIN)
    }

    fn wait(bitset: u32) -> Wait {
        Wait {
            futex: Futex::new(0),
            expected: 0,
            bitset,
        }
    }

    fn complete_wait(mut op: Wait, result: CQEResult) -> io::Result<WaitOutcome> {
        // Safety: the test supplies one completion for this unsubmitted op.
        let completion = unsafe { op.reap(result) };
        op.complete(completion)
    }

    #[test]
    fn kernel_eagain_is_a_mismatch() {
        let outcome = complete_wait(wait(MATCH_ANY), CQEResult::new(Err(eagain()), 0));
        assert_eq!(outcome.unwrap(), WaitOutcome::Mismatch);
    }

    #[test]
    fn synthetic_eagain_remains_an_error() {
        let err = complete_wait(wait(MATCH_ANY), CQEResult::synthetic(Err(eagain()))).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EAGAIN));
    }

    #[test]
    fn cancellation_is_reported_as_an_error() {
        let canceled = io::Error::from_raw_os_error(libc::ECANCELED);
        let err = complete_wait(wait(MATCH_ANY), CQEResult::new(Err(canceled), 0)).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));
    }

    #[test]
    fn only_a_woken_wait_holds_a_wake_to_pass_on() {
        let mut op = wait(MATCH_ANY);
        // Safety: each completion below is supplied once to this unsubmitted op.
        let woken = unsafe { op.reap(CQEResult::new(Ok(0), 0)) };
        assert!(woken.wake.is_some());
        woken.accept();
        for err in [eagain(), io::Error::from_raw_os_error(libc::ECANCELED)] {
            // Safety: as above.
            let failed = unsafe { op.reap(CQEResult::new(Err(err), 0)) };
            assert!(failed.wake.is_none());
        }
    }

    /// Wait in `futex(2)` on another thread until woken, and report whether
    /// the wait ended within ten seconds.
    fn thread_waiter(futex: &Futex) -> std::thread::JoinHandle<bool> {
        let futex = futex.clone();
        std::thread::spawn(move || {
            let timeout = libc::timespec {
                tv_sec: 10,
                tv_nsec: 0,
            };
            // Safety: `futex` keeps the aligned word alive for the call.
            let rc = unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    futex.as_ptr(),
                    libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
                    0,
                    &timeout,
                )
            };
            rc == 0
        })
    }

    #[test]
    fn dropping_a_delivered_wake_wakes_another_waiter() {
        let futex = Futex::new(0);
        let waiter = thread_waiter(&futex);
        // The thread may not be queued yet, so pass wakes until one lands. If
        // dropping never wakes, the thread's own timeout ends the loop.
        while !waiter.is_finished() {
            drop(DeliveredWake::new(&futex, MATCH_ANY));
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(waiter.join().unwrap(), "no passed wake reached the waiter");
    }

    #[test]
    fn zero_bitset_fails_configuration() {
        let err = wait(0).configure().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let mut wake = Wake {
            futex: Futex::new(0),
            count: 1,
            bitset: 0,
        };
        let err = wake.configure().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn wait_any_outcome_reports_kernel_index() {
        let futexes = vec![Futex::new(0), Futex::new(0)];
        let mut op = WaitAny {
            expected: vec![0; futexes.len()],
            futexes,
            vector: Box::default(),
        };
        // Safety: the test supplies one completion for this unsubmitted op.
        let completion = unsafe { op.reap(CQEResult::new(Ok(1), 0)) };
        let woken = completion
            .wake
            .as_ref()
            .and_then(|wake| wake.futex.as_ref());
        assert!(std::ptr::eq(
            woken.unwrap().as_ptr(),
            op.futexes[1].as_ptr()
        ));
        let outcome = op.complete(completion).unwrap();
        assert_eq!(outcome, WaitAnyOutcome::Woken { index: 1 });
    }

    #[test]
    fn wait_any_rejects_lists_outside_kernel_bounds() {
        for len in [0, FUTEX_WAITV_MAX + 1] {
            let mut op = WaitAny {
                futexes: vec![Futex::new(0); len],
                expected: vec![0; len],
                vector: Box::default(),
            };
            let err = op.configure().unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "len {len}");
        }
    }

    #[test]
    fn wait_any_vector_names_each_word() {
        let futexes = vec![Futex::new(0), Futex::new(7)];
        let mut op = WaitAny {
            expected: vec![0, 7],
            futexes,
            vector: Box::default(),
        };
        op.configure().unwrap();
        assert_eq!(op.vector.len(), 2);
        // Safety: `types::FutexWaitV` is a transparent wrapper over the
        // 24-byte `struct futex_waitv { u64 val; u64 uaddr; u32 flags; u32 __reserved; }`.
        let raw: &[[u64; 3]] =
            unsafe { std::slice::from_raw_parts(op.vector.as_ptr().cast(), op.vector.len()) };
        for ((entry, futex), expected) in raw.iter().zip(&op.futexes).zip([0_u64, 7]) {
            assert_eq!(entry[0], expected);
            assert_eq!(entry[1], futex.as_ptr() as u64);
            assert_eq!(entry[2] as u32, FUTEX_FLAGS);
        }
    }
}
