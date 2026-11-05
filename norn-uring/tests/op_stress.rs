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

use std::cell::Cell;
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

fn kind(rng: &mut Rng, pipe: i32) -> Kind {
    match rng.below(10) {
        0 => Kind::FailConfigure,
        1 | 2 => Kind::Blocked(pipe),
        _ => Kind::Nop,
    }
}

async fn run_scenario(seed: u64, iterations: usize, pipe: i32) {
    let handle = Handle::current();
    let mut rng = Rng(seed | 1);
    let mut tasks = Vec::new();
    let mut parked_ops: Vec<Pin<Box<dyn Future<Output = io::Result<u32>>>>> = Vec::new();
    let in_flight = Rc::new(Cell::new(0usize));

    for _ in 0..iterations {
        match rng.below(12) {
            0..=2 => {
                let op = handle.submit(Tracked::new(kind(&mut rng, pipe)));
                let in_flight = Rc::clone(&in_flight);
                in_flight.set(in_flight.get() + 1);
                tasks.push(norn_executor::spawn(async move {
                    let _ = op.await;
                    in_flight.set(in_flight.get() - 1);
                }));
            }
            3 => {
                let first = handle.submit(Tracked::new(kind(&mut rng, pipe)));
                let second = handle.submit(Tracked::new(kind(&mut rng, pipe)));
                // Half the chains hard-link their first pair.
                let pair = if rng.below(2) == 0 {
                    first.then(second)
                } else {
                    first.then_always(second)
                };
                let chain = pair.then(handle.submit(Tracked::new(kind(&mut rng, pipe))));
                tasks.push(norn_executor::spawn(async move {
                    let _ = chain.await;
                }));
            }
            4 => {
                let req = handle
                    .submit(Tracked::new(kind(&mut rng, pipe)))
                    .timeout(Duration::from_micros(rng.below(2000)));
                tasks.push(norn_executor::spawn(async move {
                    let _ = req.await;
                }));
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
                tasks.push(norn_executor::spawn(async move {
                    let mut timeout = std::pin::pin!(timeout);
                    let _ = PollOnce(timeout.as_mut()).await;
                    match action {
                        0 => {
                            let _ = control.cancel().await;
                        }
                        1 => {
                            let _ = control.reset(reset).await;
                        }
                        _ => {}
                    }
                    let _ = timeout.await;
                }));
            }
            10 => YieldNow(false).await,
            _ => {
                let _ = handle.timeout(Duration::from_micros(50)).await;
            }
        }
    }
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
        ex.block_on(run_scenario(seed, 3000, fds[0]));
        drop(ex);
        // Safety: shutdown reclaimed every read, and nothing else owns these.
        unsafe {
            libc::close(fds[0]);
            libc::close(fds[1]);
        }
        tx.send(LIVE.load(Ordering::SeqCst)).unwrap();
    });
    let live = rx
        .recv_timeout(Duration::from_secs(60))
        .unwrap_or_else(|_| panic!("seed {seed} entries {entries}: scenario or shutdown stalled"));
    assert_eq!(live, 0, "seed {seed}: leaked or double-dropped op data");
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
    eprintln!("completed {} ops", COMPLETED.load(Ordering::SeqCst));
}
