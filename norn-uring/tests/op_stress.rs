#![cfg(target_os = "linux")]

//! Randomized stress of the operation lifecycle: submission under SQ
//! backpressure, soft- and hard-linked requests, configuration failures,
//! cancellation by drop and abort, timeouts with control requests, and driver
//! shutdown. Every operation's data is tracked so a leak or double drop is
//! detected.
//!
//! `OP_STRESS_SEEDS=<n>` sets how many seeds `op_lifecycle_stress` runs (20 by
//! default), each on 1-, 2-, 4- and 64-entry rings, with and without
//! `DEFER_TASKRUN`. `OP_STRESS_SEED=<seed>` (with optional
//! `OP_STRESS_ENTRIES=<n>`) replays one run instead. The two modes never run
//! together, because they share the `LIVE` count.
//!
//! A linked batch must fit the submission queue or it is rejected before it
//! reaches the kernel, so the scenario sizes its chains to the ring: three
//! linked operations need 3 entries, a timeout-bounded operation needs 2, and
//! a 1-entry ring only sees single operations. Every output is checked, and
//! each run must see linked requests complete through the kernel.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::time::Duration;

use norn_uring::{CQEResult, Handle, Operation, Request, Singleshot};

static LIVE: AtomicIsize = AtomicIsize::new(0);
static COMPLETED: AtomicUsize = AtomicUsize::new(0);
/// Linked chains of NOPs that completed successfully through the kernel.
static LINKED_OK: AtomicUsize = AtomicUsize::new(0);
/// Timeout-bounded blocked reads that the linked timeout cancelled.
static TIMED_OUT: AtomicUsize = AtomicUsize::new(0);

/// Unexpected outputs, reported by the scenario because tasks that panic are
/// aborted and joined silently.
type Failures = Rc<RefCell<Vec<String>>>;

struct Live;

impl Live {
    fn new() -> Self {
        LIVE.fetch_add(1, Ordering::SeqCst);
        Live
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        LIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Nop,
    FailConfigure,
    /// A read from a pipe nobody writes: completes only by cancellation.
    Blocked(i32),
}

struct Tracked {
    kind: Kind,
    buf: Box<[u8; 8]>,
    _live: Live,
}

impl Tracked {
    fn new(kind: Kind) -> Self {
        Self {
            kind,
            buf: Box::new([0; 8]),
            _live: Live::new(),
        }
    }
}

struct TrackedCompletion {
    result: io::Result<u32>,
    _live: Live,
}

// Safety: the read targets a pipe that outlives the driver, into a buffer the
// operation owns; each entry posts one terminal CQE.
unsafe impl Operation for Tracked {
    type Completion = TrackedCompletion;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        match self.kind {
            Kind::Nop => Ok(io_uring::opcode::Nop::new().build()),
            Kind::FailConfigure => Err(io::Error::from_raw_os_error(libc::EINVAL)),
            Kind::Blocked(fd) => {
                Ok(
                    io_uring::opcode::Read::new(io_uring::types::Fd(fd), self.buf.as_mut_ptr(), 8)
                        .build(),
                )
            }
        }
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        TrackedCompletion {
            result: result.into_result(),
            _live: Live::new(),
        }
    }
}

impl Singleshot for Tracked {
    type Output = io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        COMPLETED.fetch_add(1, Ordering::SeqCst);
        completion.result
    }
}

/// xorshift64*
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

struct PollOnce<'a, F: Future + ?Sized>(Pin<&'a mut F>);

impl<F: Future + ?Sized> Future for PollOnce<'_, F> {
    type Output = Poll<F::Output>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.0.as_mut().poll(cx))
    }
}

struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn rejected_as_too_large(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::InvalidInput
        && err.to_string().contains("exceeds submission queue")
}

/// Why `result` is not an output a `kind` operation can produce, if so. A
/// request cancelled by a failed or cancelled predecessor, or by its linked
/// timeout, completes with `ECANCELED`. Nothing may be rejected for its size.
fn unexpected(kind: Kind, result: &io::Result<u32>) -> Option<String> {
    let cancelled_or_failed = |err: &io::Error| {
        matches!(
            (kind, err.raw_os_error()),
            (_, Some(libc::ECANCELED)) | (Kind::FailConfigure, Some(libc::EINVAL))
        )
    };
    match result {
        Ok(0) if matches!(kind, Kind::Nop) => None,
        Err(err) if rejected_as_too_large(err) => Some(format!(
            "a {} never reached the kernel: {err}",
            kind_name(kind)
        )),
        Err(err) if cancelled_or_failed(err) => None,
        other => Some(format!("{other:?} from a {}", kind_name(kind))),
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Nop => "Nop",
        Kind::FailConfigure => "FailConfigure",
        Kind::Blocked(_) => "Blocked",
    }
}

/// Check the outputs of requests submitted together, each `(kind, output)`
/// in submission order, and return whether every one succeeded. `soft` marks
/// them soft-linked: once one fails, every later one is cancelled. A chain of
/// NOPs on a running driver must succeed.
fn check_chain(
    what: &str,
    soft: bool,
    outputs: &[(Kind, &io::Result<u32>)],
    failures: &Failures,
) -> bool {
    let mut failures = failures.borrow_mut();
    let mut failed = false;
    for (kind, output) in outputs {
        if let Some(message) = unexpected(*kind, output) {
            failures.push(format!("{what}: {message}"));
        } else if soft && failed && output.is_ok() {
            failures.push(format!(
                "{what}: a {} ran after a failed link",
                kind_name(*kind)
            ));
        }
        failed |= output.is_err();
    }
    if failed && outputs.iter().all(|(kind, _)| matches!(kind, Kind::Nop)) {
        failures.push(format!("{what}: a chain of NOPs failed"));
    }
    !failed
}

/// A lone operation has no predecessor to cancel it: a NOP succeeds, and a
/// failed configuration reports its own error.
fn check_single(kind: Kind, output: &io::Result<u32>, failures: &Failures) {
    let expected = match kind {
        Kind::Nop => output.as_ref().is_ok_and(|n| *n == 0),
        Kind::FailConfigure => output
            .as_ref()
            .is_err_and(|e| e.raw_os_error() == Some(libc::EINVAL)),
        // Cancelled by its linked timeout, when it has one.
        Kind::Blocked(_) => output
            .as_ref()
            .is_err_and(|e| e.raw_os_error() == Some(libc::ECANCELED)),
    };
    if !expected {
        let message = unexpected(kind, output).unwrap_or_else(|| format!("{output:?}"));
        failures.borrow_mut().push(format!("single: {message}"));
    }
}

fn kind(rng: &mut Rng, pipe: i32) -> Kind {
    match rng.below(10) {
        0 => Kind::FailConfigure,
        1 | 2 => Kind::Blocked(pipe),
        _ => Kind::Nop,
    }
}

async fn run_scenario(seed: u64, iterations: usize, pipe: i32, entries: u32) {
    let handle = Handle::current();
    let mut rng = Rng(seed | 1);
    let mut tasks = Vec::new();
    let mut parked_ops: Vec<Pin<Box<dyn Future<Output = io::Result<u32>>>>> = Vec::new();
    let in_flight = Rc::new(Cell::new(0usize));
    let failures = Failures::default();

    for _ in 0..iterations {
        match rng.below(12) {
            0..=2 => {
                let kind = kind(&mut rng, pipe);
                let op = handle.submit(Tracked::new(kind));
                let in_flight = Rc::clone(&in_flight);
                let failures = Rc::clone(&failures);
                in_flight.set(in_flight.get() + 1);
                tasks.push(norn_executor::spawn(async move {
                    let output = op.await;
                    check_single(kind, &output, &failures);
                    in_flight.set(in_flight.get() - 1);
                }));
            }
            3 => {
                // At most as many linked operations as the ring has entries.
                let kinds = [
                    kind(&mut rng, pipe),
                    kind(&mut rng, pipe),
                    kind(&mut rng, pipe),
                ];
                let hard = rng.below(2) == 0;
                let failures = Rc::clone(&failures);
                let [k0, k1, k2] = kinds;
                match entries.min(3) {
                    1 => {
                        let op = handle.submit(Tracked::new(k0));
                        tasks.push(norn_executor::spawn(async move {
                            check_single(k0, &op.await, &failures);
                        }));
                    }
                    2 => {
                        let (first, second) = (
                            handle.submit(Tracked::new(k0)),
                            handle.submit(Tracked::new(k1)),
                        );
                        // Half the chains hard-link their first pair.
                        let pair = if hard {
                            first.then_always(second)
                        } else {
                            first.then(second)
                        };
                        tasks.push(norn_executor::spawn(async move {
                            let (a, b) = pair.await;
                            let outputs = [(k0, &a), (k1, &b)];
                            if check_chain("pair", !hard, &outputs, &failures) {
                                LINKED_OK.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    }
                    _ => {
                        let first = handle.submit(Tracked::new(k0));
                        let second = handle.submit(Tracked::new(k1));
                        let pair = if hard {
                            first.then_always(second)
                        } else {
                            first.then(second)
                        };
                        let chain = pair.then(handle.submit(Tracked::new(k2)));
                        tasks.push(norn_executor::spawn(async move {
                            let ((a, b), c) = chain.await;
                            let outputs = [(k0, &a), (k1, &b), (k2, &c)];
                            if check_chain("chain", !hard, &outputs, &failures) {
                                LINKED_OK.fetch_add(1, Ordering::SeqCst);
                            }
                        }));
                    }
                }
            }
            4 => {
                let kind = kind(&mut rng, pipe);
                let micros = rng.below(2000);
                let failures = Rc::clone(&failures);
                let op = handle.submit(Tracked::new(kind));
                if entries >= 2 {
                    // The operation and its linked timeout are two entries.
                    let req = op.timeout(Duration::from_micros(micros));
                    tasks.push(norn_executor::spawn(async move {
                        let output = req.await;
                        if matches!(kind, Kind::Blocked(_)) {
                            // Only the timeout can end a read of an idle pipe.
                            if output
                                .as_ref()
                                .is_err_and(|e| e.raw_os_error() == Some(libc::ECANCELED))
                            {
                                TIMED_OUT.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        check_single(kind, &output, &failures);
                    }));
                } else {
                    tasks.push(norn_executor::spawn(async move {
                        check_single(kind, &op.await, &failures);
                    }));
                }
            }
            5 => {
                // Poll once, then keep it around to drop later (cancel path).
                let mut op: Pin<Box<dyn Future<Output = io::Result<u32>>>> =
                    Box::pin(handle.submit(Tracked::new(kind(&mut rng, pipe))));
                if PollOnce(op.as_mut()).await.is_pending() {
                    parked_ops.push(op);
                }
            }
            6 => {
                // Never polled.
                drop(handle.submit(Tracked::new(kind(&mut rng, pipe))));
            }
            7 => {
                if !parked_ops.is_empty() {
                    let index = rng.below(parked_ops.len() as u64) as usize;
                    drop(parked_ops.swap_remove(index));
                }
            }
            8 => {
                if !tasks.is_empty() {
                    let index = rng.below(tasks.len() as u64) as usize;
                    tasks.swap_remove(index).abort();
                }
            }
            9 => {
                let timeout = handle.timeout(Duration::from_micros(rng.below(3000)));
                let control = timeout.control();
                let action = rng.below(3);
                let reset = Duration::from_micros(rng.below(3000));
                let failures = Rc::clone(&failures);
                tasks.push(norn_executor::spawn(async move {
                    let mut timeout = std::pin::pin!(timeout);
                    let _ = PollOnce(timeout.as_mut()).await;
                    let controlled = match action {
                        0 => Some(("cancel", control.cancel().await)),
                        1 => Some(("reset", control.reset(reset).await)),
                        _ => None,
                    };
                    if let Some((what, Err(err))) = controlled {
                        if rejected_as_too_large(&err)
                            || err.raw_os_error() != Some(libc::ECANCELED)
                        {
                            failures.borrow_mut().push(format!("timeout {what}: {err}"));
                        }
                    }
                    match timeout.await {
                        Ok(_) => {}
                        Err(err) if err.raw_os_error() == Some(libc::ECANCELED) => {}
                        Err(err) => failures.borrow_mut().push(format!("timeout: {err}")),
                    }
                }));
            }
            10 => YieldNow(false).await,
            _ => {
                if let Err(err) = handle.timeout(Duration::from_micros(50)).await {
                    failures.borrow_mut().push(format!("timeout: {err}"));
                }
            }
        }
    }
    let failures = failures.borrow();
    assert!(
        failures.is_empty(),
        "seed {seed} entries {entries}: {failures:#?}"
    );
    drop(failures);
    // Abandon everything still pending; shutdown must reclaim it.
    drop(parked_ops);
    for task in tasks {
        task.abort();
    }
}

fn run(seed: u64, builder: io_uring::Builder, entries: u32) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut fds = [0; 2];
        // Safety: `fds` has room for both descriptors.
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        let driver = norn_uring::Driver::new(builder, entries).unwrap();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        let linked_before = LINKED_OK.load(Ordering::SeqCst);
        ex.block_on(run_scenario(seed, 3000, fds[0], entries));
        let linked = LINKED_OK.load(Ordering::SeqCst) - linked_before;
        drop(ex);
        // Safety: shutdown reclaimed every read, and nothing else owns these.
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
        tx.send((LIVE.load(Ordering::SeqCst), linked)).unwrap();
    });
    let (live, linked) = rx
        .recv_timeout(Duration::from_secs(60))
        .unwrap_or_else(|_| {
            panic!("seed {seed} entries {entries}: scenario panicked, or it or shutdown stalled")
        });
    assert_eq!(live, 0, "seed {seed}: leaked or double-dropped op data");
    // A 1-entry ring has no room to link; larger rings must have run chains.
    assert_eq!(
        linked > 0,
        entries >= 2,
        "seed {seed} entries {entries}: {linked} linked chains completed"
    );
}

#[test]
fn op_lifecycle_stress_one() {
    let Some(seed) = std::env::var("OP_STRESS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
    else {
        return;
    };
    let entries = std::env::var("OP_STRESS_ENTRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let builder = if seed % 4 == 3 {
        let mut defer = io_uring::IoUring::builder();
        defer
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer();
        defer
    } else {
        io_uring::IoUring::builder()
    };
    run(seed, builder, entries);
}

#[test]
fn op_lifecycle_stress() {
    if std::env::var("OP_STRESS_SEED").is_ok() {
        return;
    }
    let seeds = std::env::var("OP_STRESS_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(20u64);
    for seed in 0..seeds {
        for entries in [1u32, 2, 4, 64] {
            run(seed * 4 + 1, io_uring::IoUring::builder(), entries);
            let mut defer = io_uring::IoUring::builder();
            defer
                .setup_coop_taskrun()
                .setup_defer_taskrun()
                .setup_single_issuer();
            run(seed * 4 + 3, defer, entries);
        }
    }
    assert!(
        TIMED_OUT.load(Ordering::SeqCst) > 0,
        "no linked timeout ever cancelled a blocked read"
    );
    eprintln!(
        "completed {} ops, {} linked chains, {} timeouts",
        COMPLETED.load(Ordering::SeqCst),
        LINKED_OK.load(Ordering::SeqCst),
        TIMED_OUT.load(Ordering::SeqCst)
    );
}
