//! Seeded randomized stress harness for the norn-uring operation lifecycle.
//!
//! Every scenario is derived from one `u64` seed: ring geometry and setup
//! flags, executor and park behaviour, the set of actors and every random
//! choice each actor makes (per-actor RNG streams, so choices do not depend on
//! cross-actor interleaving). Kernel and thread timing is not deterministic,
//! so a failing seed may need a few repetitions to reproduce.
//!
//! The test is ignored by default because it is long-running. It must stay the
//! only test in this binary and must run with `--test-threads 1`: it observes
//! process-wide state such as the descriptor table.
//!
//! ```text
//! STRESS_SEED_START=0 STRESS_SECS=600 \
//!   cargo test -p norn-uring --test stress_lifecycle -- --ignored --nocapture --test-threads 1
//! STRESS_SEED=1234 STRESS_REPEAT=20 cargo test ... # replay one seed
//! ```
//!
//! The harness opens many loopback connections. Ephemeral-port exhaustion on the
//! host is counted as an environment issue (`STRESS ENV`) rather than a failure,
//! but it can also stall a seed. On a host with many `TIME_WAIT` sockets, run
//! it in a private network namespace, for example with
//! `unshare -rn bash -c 'ip link set lo up && exec "$@"' _ <test binary> ...`.
//!
//! Environment knobs: `STRESS_SEED_START`, `STRESS_SEEDS`, `STRESS_SECS`,
//! `STRESS_SEED`, `STRESS_REPEAT`, `STRESS_HANG_SECS`, `STRESS_NO_DEFER`
//! (never use DEFER_TASKRUN), `STRESS_BIG_CQ` (avoid CQ overflow),
//! `STRESS_NO_REMOTE_UNPARK`, `STRESS_SHUTDOWN` (graceful|abortsome|abrupt),
//! `STRESS_NO_LEFTOVERS` (never drop resources after the driver; by default
//! half of the seeds do), `STRESS_ACTOR_MASK` (bitmask of actors to run),
//! `STRESS_SETTLE_MS` (settle time before the probe and pool checks),
//! `STRESS_TRACE_SEND`, `STRESS_VERBOSE`.
#![cfg(target_os = "linux")]

use norn_uring::BufResult;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::net::Shutdown;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use norn_executor::park::{Park, ParkMode, Unpark};
use norn_executor::{spawn, LocalExecutor};
use norn_uring::buf::{StableBuf, StableBufMut};
use norn_uring::bufring::RecvBufRing;
use norn_uring::fixedbuf::FixedBufTable;
use norn_uring::fs::File;
use norn_uring::net::{TcpListener, TcpSocket, UdpSocket};
use norn_uring::{Driver, DriverOptions, Handle, Request, TimeoutOutcome};

// ---------------------------------------------------------------------------
// PRNG
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
    /// Uniform in `lo..hi` (`hi` exclusive).
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi.saturating_sub(lo))
    }
    fn usize(&mut self, lo: usize, hi: usize) -> usize {
        self.range(lo as u64, hi as u64) as usize
    }
    fn chance(&mut self, p: f64) -> bool {
        ((self.next_u64() >> 11) as f64 / (1u64 << 53) as f64) < p
    }
    fn pick<T: Copy>(&mut self, xs: &[T]) -> T {
        xs[self.below(xs.len() as u64) as usize]
    }
    fn fork(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }
    fn micros(&mut self, hi: u64) -> Duration {
        Duration::from_micros(self.below(hi))
    }
    fn drop_plan(&mut self, p_drop: f64) -> DropPlan {
        if !self.chance(p_drop) {
            return DropPlan::Never;
        }
        match self.below(4) {
            0 => DropPlan::BeforePoll,
            1 => DropPlan::AfterPolls(1),
            2 => DropPlan::AfterPolls(self.range(2, 6) as u32),
            _ => DropPlan::OnWake,
        }
    }
    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next_u64() as u8).collect()
    }
}

fn pattern(stream: u64, pos: u64) -> u8 {
    let x = pos
        .wrapping_add(stream.wrapping_mul(0x9E37_79B9_7F4A_7C15))
        .wrapping_mul(0xD6E8_FEB8_6659_FD93);
    ((x >> 56) as u8) ^ (pos as u8)
}

fn fill_pattern(stream: u64, pos: u64, len: usize) -> Vec<u8> {
    (0..len as u64).map(|i| pattern(stream, pos + i)).collect()
}

// ---------------------------------------------------------------------------
// Tracked buffers: a global live count proves every kernel-owned buffer is
// eventually released.
// ---------------------------------------------------------------------------

static LIVE_BUFS: AtomicUsize = AtomicUsize::new(0);

struct TrackedBuf(Vec<u8>);

impl TrackedBuf {
    fn with_capacity(n: usize) -> Self {
        LIVE_BUFS.fetch_add(1, Ordering::Relaxed);
        TrackedBuf(Vec::with_capacity(n))
    }
    fn from_vec(v: Vec<u8>) -> Self {
        LIVE_BUFS.fetch_add(1, Ordering::Relaxed);
        TrackedBuf(v)
    }
}

impl Drop for TrackedBuf {
    fn drop(&mut self) {
        LIVE_BUFS.fetch_sub(1, Ordering::Relaxed);
    }
}

unsafe impl StableBuf for TrackedBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }
    fn bytes_init(&self) -> usize {
        self.0.len()
    }
}

unsafe impl StableBufMut for TrackedBuf {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.0.as_mut_ptr()
    }
    fn bytes_remaining(&self) -> usize {
        self.0.capacity() - self.0.len()
    }
    unsafe fn set_init(&mut self, init_len: usize) {
        if self.0.len() < init_len {
            self.0.set_len(init_len);
        }
    }
}

// ---------------------------------------------------------------------------
// Drop-timing wrapper
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DropPlan {
    Never,
    /// Drop without ever polling: the operation is never submitted.
    BeforePoll,
    /// Drop after this many `Pending` polls (normally after submission).
    AfterPolls(u32),
    /// Drop on the first poll after the inner future was woken, without
    /// polling it: the completion is queued but not consumed.
    OnWake,
}

struct WakeFlag {
    woken: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Wake for WakeFlag {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
        let waker = self.waker.lock().unwrap().clone();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct Chaos<F: Future> {
    fut: Option<Pin<Box<F>>>,
    plan: DropPlan,
    polls: u32,
    flag: Arc<WakeFlag>,
}

impl<F: Future> Chaos<F> {
    fn new(fut: F, plan: DropPlan) -> Self {
        Self {
            fut: Some(Box::pin(fut)),
            plan,
            polls: 0,
            flag: Arc::new(WakeFlag {
                woken: AtomicBool::new(false),
                waker: Mutex::new(None),
            }),
        }
    }
}

impl<F: Future> Future for Chaos<F> {
    type Output = Option<F::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.fut.is_none() {
            return Poll::Ready(None);
        }
        match this.plan {
            DropPlan::BeforePoll => {
                this.fut = None;
                return Poll::Ready(None);
            }
            DropPlan::OnWake if this.polls > 0 && this.flag.woken.load(Ordering::SeqCst) => {
                this.fut = None;
                return Poll::Ready(None);
            }
            _ => {}
        }
        *this.flag.waker.lock().unwrap() = Some(cx.waker().clone());
        this.flag.woken.store(false, Ordering::SeqCst);
        let waker = Waker::from(Arc::clone(&this.flag));
        let mut inner_cx = Context::from_waker(&waker);
        let fut = this.fut.as_mut().unwrap();
        match fut.as_mut().poll(&mut inner_cx) {
            Poll::Ready(v) => {
                this.fut = None;
                Poll::Ready(Some(v))
            }
            Poll::Pending => {
                this.polls += 1;
                if let DropPlan::AfterPolls(k) = this.plan {
                    if this.polls >= k {
                        this.fut = None;
                        return Poll::Ready(None);
                    }
                }
                Poll::Pending
            }
        }
    }
}

/// A spawned child task that is aborted when its owner is dropped, so that
/// aborting an actor also stops the tasks it spawned.
struct Scoped<J, A: Fn(&J)> {
    handle: Option<Pin<Box<J>>>,
    abort: A,
}

impl<J, A: Fn(&J)> Drop for Scoped<J, A> {
    fn drop(&mut self) {
        if let Some(h) = &self.handle {
            (self.abort)(h);
        }
    }
}

impl<J: Future, A: Fn(&J) + Unpin> Future for Scoped<J, A> {
    type Output = J::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let out = std::task::ready!(this.handle.as_mut().unwrap().as_mut().poll(cx));
        this.handle = None;
        Poll::Ready(out)
    }
}

fn make_scoped<J, A: Fn(&J)>(handle: J, abort: A) -> Scoped<J, A> {
    Scoped {
        handle: Some(Box::pin(handle)),
        abort,
    }
}

macro_rules! spawn_scoped {
    ($fut:expr) => {
        make_scoped(spawn($fut), |h| h.abort())
    };
}

fn boxed<'a, F: Future + 'a>(f: F) -> Pin<Box<dyn Future<Output = F::Output> + 'a>> {
    Box::pin(f)
}

enum Either<A, B> {
    Left(A),
    Right(B),
}

async fn race<A: Future, B: Future>(a: A, b: B) -> Either<A::Output, B::Output> {
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    std::future::poll_fn(move |cx| {
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    })
    .await
}

async fn sleep(d: Duration) {
    let _ = Handle::current().timeout(d).await;
}

// ---------------------------------------------------------------------------
// Chaos park layer: randomizes park modes.
// ---------------------------------------------------------------------------

struct ChaosPark<P> {
    inner: P,
    rng: Rng,
    p_nopark: f64,
    p_timeout: f64,
    max_timeout_us: u64,
}

impl<P: Park> Park for ChaosPark<P> {
    type Unparker = P::Unparker;
    type Guard = P::Guard;

    fn park(&mut self, mode: ParkMode) -> Result<(), io::Error> {
        let mode = match mode {
            ParkMode::NextCompletion if self.rng.chance(self.p_nopark) => ParkMode::NoPark,
            ParkMode::NextCompletion if self.rng.chance(self.p_timeout) => {
                ParkMode::Timeout(self.rng.micros(self.max_timeout_us))
            }
            ParkMode::Timeout(d) if self.rng.chance(self.p_timeout) => {
                ParkMode::Timeout(d.min(self.rng.micros(self.max_timeout_us)))
            }
            mode => mode,
        };
        self.inner.park(mode)
    }
    fn enter(&self) -> Self::Guard {
        self.inner.enter()
    }
    fn unparker(&self) -> Self::Unparker {
        self.inner.unparker()
    }
    fn needs_park(&self) -> bool {
        self.inner.needs_park()
    }
    fn shutdown(&mut self) {
        self.inner.shutdown()
    }
}

// ---------------------------------------------------------------------------
// Logger capturing norn-uring warnings.
// ---------------------------------------------------------------------------

static WARNINGS: Mutex<Vec<String>> = Mutex::new(Vec::new());
static STATUS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn status(id: usize, msg: impl FnOnce() -> String) {
    let mut s = STATUS.lock().unwrap();
    if s.len() <= id {
        s.resize(id + 1, String::new());
    }
    s[id] = msg();
}

struct CaptureLogger;

impl log::Log for CaptureLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Warn
    }
    fn log(&self, record: &log::Record<'_>) {
        if record.level() <= log::Level::Warn && record.target().starts_with("norn") {
            WARNINGS
                .lock()
                .unwrap()
                .push(format!("[{}] {}", record.target(), record.args()));
        }
    }
    fn flush(&self) {}
}

static LOGGER: CaptureLogger = CaptureLogger;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShutdownMode {
    /// Stop actors cooperatively, join them, probe buffer rings, then drop.
    Graceful,
    /// Abort some actor tasks, join the rest, probe, then drop.
    AbortSome,
    /// Return from `block_on` with everything in flight and drop the executor.
    Abrupt,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActorKind {
    Nop,
    Timeout,
    File,
    Pipe,
    TcpEcho,
    TcpAccept,
    Udp,
}

#[derive(Clone, Debug)]
struct RingSpec {
    bgid: u16,
    ring_entries: u16,
    buf_cnt: u16,
    buf_len: usize,
}

#[derive(Clone, Debug)]
struct Config {
    sq_entries: u32,
    cq_entries: Option<u32>,
    coop_taskrun: bool,
    taskrun_flag: bool,
    single_issuer: bool,
    defer_taskrun: bool,
    sqpoll: Option<u32>,
    drain_budget: Option<usize>,
    task_budget: usize,
    p_nopark: f64,
    p_timeout: f64,
    remote_unpark: bool,
    actors: Vec<ActorKind>,
    duration_ms: u64,
    shutdown: ShutdownMode,
    rings: Vec<RingSpec>,
    fixed_pool: Option<(usize, usize)>,
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).map(|v| v != "0").unwrap_or(false)
}

impl Config {
    fn generate(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let sq_entries = rng.pick(&[1u32, 2, 2, 4, 4, 8, 8, 16, 32, 64, 256]);
        let mut cq_entries = match rng.below(4) {
            0 => Some(sq_entries),
            1 => Some(sq_entries * 2),
            2 => Some((sq_entries * 16).min(4096)),
            _ => None,
        };
        if env_flag("STRESS_BIG_CQ") {
            cq_entries = Some(4096);
        }
        let flags = rng.below(6);
        let (mut coop_taskrun, single_issuer, mut defer_taskrun, mut taskrun_flag) = match flags {
            0 => (false, false, false, false),
            1 => (true, false, false, false),
            2 => (true, true, true, false),
            3 => (true, true, true, true),
            4 => (false, true, false, false),
            _ => (true, true, false, true),
        };
        if env_flag("STRESS_NO_DEFER") {
            defer_taskrun = false;
        }
        let sqpoll = if !defer_taskrun && rng.chance(0.08) {
            Some(rng.pick(&[1u32, 10, 1000]))
        } else {
            None
        };
        if sqpoll.is_some() {
            // The kernel rejects SQPOLL combined with these task-run flags.
            coop_taskrun = false;
            taskrun_flag = false;
        }
        let drain_budget = match rng.below(4) {
            0 => None,
            1 => Some(1),
            2 => Some(rng.usize(2, 8)),
            _ => Some(32),
        };
        let task_budget = rng.pick(&[1usize, 2, 4, 32, 32, 128]);
        let p_nopark = rng.pick(&[0.0, 0.0, 0.05, 0.3]);
        let p_timeout = rng.pick(&[0.0, 0.1, 0.5, 1.0]);
        let remote_unpark = rng.chance(0.5) && !env_flag("STRESS_NO_REMOTE_UNPARK");
        let kinds = [
            ActorKind::Nop,
            ActorKind::Timeout,
            ActorKind::File,
            ActorKind::Pipe,
            ActorKind::TcpEcho,
            ActorKind::TcpEcho,
            ActorKind::TcpEcho,
            ActorKind::TcpAccept,
            ActorKind::Udp,
            ActorKind::Udp,
        ];
        let n_actors = rng.usize(1, 9);
        let actors = (0..n_actors).map(|_| rng.pick(&kinds)).collect();
        let duration_ms = rng.range(20, 600);
        let mut shutdown = match rng.below(10) {
            0..=5 => ShutdownMode::Graceful,
            6 | 7 => ShutdownMode::AbortSome,
            _ => ShutdownMode::Abrupt,
        };
        match std::env::var("STRESS_SHUTDOWN").as_deref() {
            Ok("graceful") => shutdown = ShutdownMode::Graceful,
            Ok("abortsome") => shutdown = ShutdownMode::AbortSome,
            Ok("abrupt") => shutdown = ShutdownMode::Abrupt,
            _ => {}
        }
        let n_rings = rng.usize(1, 4);
        let rings = (0..n_rings)
            .map(|i| {
                let buf_cnt = rng.pick(&[1u16, 2, 3, 4, 8, 16, 64]);
                let ring_entries = if rng.chance(0.5) {
                    buf_cnt
                } else {
                    buf_cnt.next_power_of_two() * 2
                };
                RingSpec {
                    bgid: i as u16 + 1,
                    ring_entries,
                    buf_cnt,
                    buf_len: rng.pick(&[64usize, 300, 512, 2048, 4096]),
                }
            })
            .collect();
        let fixed_pool = if rng.chance(0.5) {
            Some((rng.usize(1, 8), rng.pick(&[512usize, 4096, 16384])))
        } else {
            None
        };
        Config {
            sq_entries,
            cq_entries,
            coop_taskrun,
            taskrun_flag,
            single_issuer,
            defer_taskrun,
            sqpoll,
            drain_budget,
            task_budget,
            p_nopark,
            p_timeout,
            remote_unpark,
            actors,
            duration_ms,
            shutdown,
            rings,
            fixed_pool,
        }
    }

    fn builder(&self) -> io_uring::Builder {
        let mut b = io_uring::IoUring::builder();
        if let Some(cq) = self.cq_entries {
            b.setup_cqsize(cq);
        }
        if self.coop_taskrun {
            b.setup_coop_taskrun();
        }
        if self.taskrun_flag {
            b.setup_taskrun_flag();
        }
        if self.single_issuer {
            b.setup_single_issuer();
        }
        if self.defer_taskrun {
            b.setup_defer_taskrun();
        }
        if let Some(idle) = self.sqpoll {
            b.setup_sqpoll(idle);
        }
        b
    }
}

// ---------------------------------------------------------------------------
// Scenario context
// ---------------------------------------------------------------------------

struct Ctx {
    seed: u64,
    cfg: Config,
    errors: RefCell<Vec<String>>,
    stop: Cell<bool>,
    rings: Vec<RecvBufRing>,
    pool: Option<Rc<FixedBufTable<Vec<u8>>>>,
    peer_stop: Arc<AtomicBool>,
    peers: RefCell<Vec<std::thread::JoinHandle<()>>>,
    tmpdir: PathBuf,
    verbose: bool,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        // An undrained peer thread would be detached with its sockets open.
        debug_assert!(
            std::thread::panicking() || self.peers.borrow().is_empty(),
            "peer threads left in Ctx"
        );
    }
}

fn env_error(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(libc::EADDRINUSE | libc::EADDRNOTAVAIL | libc::EPERM | libc::EMFILE | libc::ENFILE)
    )
}

static ENV_ISSUES: AtomicUsize = AtomicUsize::new(0);
/// Seeds skipped because the kernel refused their ring configuration.
static UNSUPPORTED_CONFIGS: AtomicUsize = AtomicUsize::new(0);
/// Seeds that finished with at least one environment issue.
static ENV_SEEDS: AtomicUsize = AtomicUsize::new(0);

impl Ctx {
    /// Host resource exhaustion (ephemeral ports, conntrack) is not a runtime
    /// bug; it is counted and reported separately.
    fn env(&self, msg: String) {
        ENV_ISSUES.fetch_add(1, Ordering::SeqCst);
        if self.verbose {
            eprintln!("seed {} environment: {msg}", self.seed);
        }
    }
    fn fail(&self, msg: String) {
        if self.verbose {
            eprintln!("seed {} error: {msg}", self.seed);
        }
        self.errors.borrow_mut().push(msg);
    }
    fn stopping(&self) -> bool {
        self.stop.get()
    }
    fn spawn_peer(&self, f: impl FnOnce() + Send + 'static) {
        self.peers.borrow_mut().push(std::thread::spawn(f));
    }
    fn ring(&self, rng: &mut Rng) -> RecvBufRing {
        self.rings[rng.usize(0, self.rings.len())].clone()
    }
}

/// Errors that a stressed but correct runtime may legitimately report.
fn benign_net_error(e: &io::Error) -> bool {
    matches!(
        e.raw_os_error(),
        Some(
            libc::ECONNRESET
                | libc::EPIPE
                | libc::ECANCELED
                | libc::ETIME
                | libc::ENOBUFS
                | libc::ENOTCONN
                | libc::ECONNREFUSED
                | libc::EINTR
                | libc::EAGAIN
        )
    )
}

fn is_batch_too_large(e: &io::Error) -> bool {
    e.kind() == io::ErrorKind::InvalidInput && e.to_string().contains("exceeds submission queue")
}

// ---------------------------------------------------------------------------
// Actors
// ---------------------------------------------------------------------------

async fn nop_actor(ctx: Rc<Ctx>, mut rng: Rng) {
    let n = rng.range(1, 300);
    for _ in 0..n {
        if ctx.stopping() {
            break;
        }
        if rng.chance(0.1) {
            let k = rng.usize(1, 80);
            let futs: Vec<_> = (0..k)
                .map(|_| Chaos::new(norn_uring::noop(), rng.drop_plan(0.3)))
                .collect();
            futures_util::future::join_all(futs).await;
        } else {
            let _ = Chaos::new(norn_uring::noop(), rng.drop_plan(0.3)).await;
        }
        if rng.chance(0.05) {
            sleep(rng.micros(500)).await;
        }
    }
}

async fn timeout_actor(ctx: Rc<Ctx>, mut rng: Rng, id: usize) {
    let handle = Handle::current();
    let idle = match UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await {
        Ok(s) => Rc::new(s),
        Err(e) if env_error(&e) => return ctx.env(format!("timeout actor {id}: udp bind {e}")),
        Err(e) => return ctx.fail(format!("timeout actor {id}: udp bind {e}")),
    };
    let n = rng.range(1, 40);
    for _ in 0..n {
        if ctx.stopping() {
            break;
        }
        match rng.below(6) {
            0 => {
                let d = rng.micros(3000);
                let plan = rng.drop_plan(0.3);
                let start = Instant::now();
                match Chaos::new(handle.timeout(d), plan).await {
                    Some(Ok(TimeoutOutcome::Expired)) => {
                        if start.elapsed() + Duration::from_micros(200) < d {
                            ctx.fail(format!(
                                "timeout actor {id}: expired after {:?} < {d:?}",
                                start.elapsed()
                            ));
                        }
                    }
                    Some(Ok(other)) => ctx.fail(format!(
                        "timeout actor {id}: uncanceled timeout -> {other:?}"
                    )),
                    Some(Err(e)) => ctx.fail(format!("timeout actor {id}: timeout error {e}")),
                    None => {}
                }
            }
            1 | 2 => {
                // Cancel (1) or reset (2) a long timeout from another task.
                let long = Duration::from_secs(5);
                let t = handle.timeout(long);
                let control = t.control();
                let delay = rng.micros(2000);
                let reset = rng.below(3) == 2;
                let new_d = rng.micros(2000);
                let ctl = spawn_scoped!(async move {
                    sleep(delay).await;
                    if reset {
                        control.reset(new_d).await.map(|_| ())
                    } else {
                        control.cancel().await.map(|_| ())
                    }
                });
                let start = Instant::now();
                let outcome = t.await;
                let ctl = ctl.await;
                let elapsed = start.elapsed();
                if elapsed >= Duration::from_secs(4) {
                    ctx.fail(format!(
                        "timeout actor {id}: {} lost; timeout ran {elapsed:?} ({outcome:?})",
                        if reset { "reset" } else { "cancel" }
                    ));
                }
                match (reset, &outcome) {
                    (false, Ok(TimeoutOutcome::Canceled)) | (true, Ok(TimeoutOutcome::Expired)) => {}
                    _ => ctx.fail(format!(
                        "timeout actor {id}: reset={reset} unexpected outcome {outcome:?}, control {ctl:?}"
                    )),
                }
                if let Ok(Err(e)) = ctl {
                    ctx.fail(format!("timeout actor {id}: control error {e}"));
                }
            }
            3 => {
                // Linked timeout on an operation that never completes.
                let d = rng.micros(3000);
                let plan = rng.drop_plan(0.2);
                let start = Instant::now();
                let req = idle.poll_readiness(libc::POLLIN as u32).timeout(d);
                match Chaos::new(req, plan).await {
                    Some(Ok(ev)) => ctx.fail(format!(
                        "timeout actor {id}: idle socket became readable {ev:?}"
                    )),
                    Some(Err(e)) if e.raw_os_error() == Some(libc::ECANCELED) => {
                        if start.elapsed() + Duration::from_micros(200) < d {
                            ctx.fail(format!(
                                "timeout actor {id}: linked timeout fired after {:?} < {d:?}",
                                start.elapsed()
                            ));
                        }
                    }
                    Some(Err(e)) if is_batch_too_large(&e) && ctx.cfg.sq_entries < 2 => {}
                    Some(Err(e)) => ctx.fail(format!("timeout actor {id}: linked poll error {e}")),
                    None => {}
                }
            }
            4 => {
                // Linked timeout on an operation that completes immediately.
                let plan = rng.drop_plan(0.2);
                let buf = TrackedBuf::with_capacity(rng.usize(1, 64));
                let req = idle
                    .poll_readiness(libc::POLLOUT as u32)
                    .timeout(Duration::from_secs(5));
                let start = Instant::now();
                match Chaos::new(req, plan).await {
                    Some(Ok(_)) => {}
                    Some(Err(e)) if is_batch_too_large(&e) && ctx.cfg.sq_entries < 2 => {}
                    Some(Err(e)) => ctx.fail(format!("timeout actor {id}: POLLOUT error {e}")),
                    None => {}
                }
                if start.elapsed() > Duration::from_secs(4) {
                    ctx.fail(format!(
                        "timeout actor {id}: immediate op waited for timeout"
                    ));
                }
                drop(buf);
            }
            _ => {
                // A burst of standalone timeouts, some dropped.
                let k = rng.usize(1, 40);
                let futs: Vec<_> = (0..k)
                    .map(|_| Chaos::new(handle.timeout(rng.micros(1000)), rng.drop_plan(0.4)))
                    .collect();
                for r in futures_util::future::join_all(futs)
                    .await
                    .into_iter()
                    .flatten()
                {
                    if let Err(e) = r {
                        ctx.fail(format!("timeout actor {id}: burst timeout error {e}"));
                    }
                }
            }
        }
    }
}

const FILE_SIZE: usize = 64 * 1024;

struct FileModel {
    bytes: Vec<Option<u8>>,
}

impl FileModel {
    fn set(&mut self, off: usize, data: &[u8]) {
        for (i, b) in data.iter().enumerate() {
            self.bytes[off + i] = Some(*b);
        }
    }
    fn forget(&mut self, off: usize, len: usize) {
        for b in &mut self.bytes[off..off + len] {
            *b = None;
        }
    }
    fn check(&self, ctx: &Ctx, what: &str, off: usize, data: &[u8]) {
        for (i, b) in data.iter().enumerate() {
            if let Some(expected) = self.bytes[off + i] {
                if expected != *b {
                    ctx.fail(format!(
                        "{what}: byte {} = {b:#x}, expected {expected:#x}",
                        off + i
                    ));
                    return;
                }
            }
        }
    }
}

fn file_op_error(ctx: &Ctx, what: &str, e: &io::Error) {
    if is_batch_too_large(e) && ctx.cfg.sq_entries < 3 {
        return;
    }
    if e.raw_os_error() == Some(libc::ECANCELED) {
        return;
    }
    ctx.fail(format!("{what}: {e}"));
}

async fn file_actor(ctx: Rc<Ctx>, mut rng: Rng, id: usize) {
    let path = ctx.tmpdir.join(format!("file-{id}"));
    let file = match File::with_options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await
    {
        Ok(f) => Rc::new(f),
        Err(e) => return ctx.fail(format!("file actor {id}: open {e}")),
    };
    // Preallocate so every in-range read has a fully known model.
    let BufResult(r, _) = file
        .write_at(TrackedBuf::from_vec(vec![0; FILE_SIZE]), 0)
        .await;
    match r {
        Ok(n) if n == FILE_SIZE => {}
        other => return ctx.fail(format!("file actor {id}: prefill {other:?}")),
    }
    let model = Rc::new(RefCell::new(FileModel {
        bytes: vec![Some(0); FILE_SIZE],
    }));
    let n = rng.range(1, 80);
    for _ in 0..n {
        if ctx.stopping() {
            break;
        }
        let concurrent = if rng.chance(0.2) { rng.usize(2, 5) } else { 1 };
        // Disjoint slices so concurrent operations never overlap.
        let slice = FILE_SIZE / concurrent;
        let mut futs: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();
        for c in 0..concurrent {
            let base = c * slice;
            let len = rng.usize(1, slice.min(8192) + 1);
            let off = base + rng.usize(0, slice - len + 1);
            let op = rng.below(9);
            let plan = rng.drop_plan(0.25);
            let with_timeout = if rng.chance(0.15) {
                Some(rng.micros(2000))
            } else {
                None
            };
            let file = Rc::clone(&file);
            let model = Rc::clone(&model);
            let ctx = Rc::clone(&ctx);
            let mut rng = rng.fork();
            futs.push(Box::pin(async move {
                file_one_op(
                    &ctx,
                    &file,
                    &model,
                    &mut rng,
                    id,
                    op,
                    off,
                    len,
                    plan,
                    with_timeout,
                )
                .await
            }));
        }
        futures_util::future::join_all(futs).await;
    }
    // Final full verification.
    let BufResult(r, buf) = file.read_at(TrackedBuf::with_capacity(FILE_SIZE), 0).await;
    match r {
        Ok(n) if n == FILE_SIZE => {
            model
                .borrow()
                .check(&ctx, &format!("file actor {id}: final read"), 0, &buf.0)
        }
        other => ctx.fail(format!("file actor {id}: final read {other:?}")),
    }
    let Ok(file) = Rc::try_unwrap(file) else {
        return ctx.fail(format!("file actor {id}: file still shared"));
    };
    if rng.chance(0.5) {
        match file.close().await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => ctx.fail(format!("file actor {id}: close {e}")),
        }
    }
    let _ = std::fs::remove_file(&path);
}

#[allow(clippy::too_many_arguments)]
async fn file_one_op(
    ctx: &Ctx,
    file: &File,
    model: &RefCell<FileModel>,
    rng: &mut Rng,
    id: usize,
    op: u64,
    off: usize,
    len: usize,
    plan: DropPlan,
    with_timeout: Option<Duration>,
) {
    let what = format!("file actor {id} op {op} off {off} len {len} plan {plan:?}");
    macro_rules! maybe_timeout {
        ($req:expr) => {{
            let req = $req;
            match with_timeout {
                Some(d) => Chaos::new(boxed(req.timeout(d)), plan).await,
                None => Chaos::new(boxed(req), plan).await,
            }
        }};
    }
    match op {
        0 | 1 => {
            let data = rng.bytes(len);
            match maybe_timeout!(file.write_at(TrackedBuf::from_vec(data.clone()), off as u64)) {
                Some(BufResult(Ok(n), _)) if n == len => model.borrow_mut().set(off, &data),
                Some(BufResult(Ok(n), _)) => {
                    model.borrow_mut().forget(off, len);
                    ctx.fail(format!("{what}: short write {n}"));
                }
                Some(BufResult(Err(e), _)) => {
                    model.borrow_mut().forget(off, len);
                    file_op_error(ctx, &what, &e);
                }
                None => {
                    if plan != DropPlan::BeforePoll {
                        model.borrow_mut().forget(off, len)
                    }
                }
            }
        }
        2 | 3 => match maybe_timeout!(file.read_at(TrackedBuf::with_capacity(len), off as u64)) {
            Some(BufResult(Ok(n), buf)) => {
                if n != len || buf.0.len() != n {
                    ctx.fail(format!("{what}: short read {n} buf {}", buf.0.len()));
                }
                model
                    .borrow()
                    .check(ctx, &what, off, &buf.0[..n.min(buf.0.len())]);
            }
            Some(BufResult(Err(e), _)) => file_op_error(ctx, &what, &e),
            None => {}
        },
        4 => {
            let data = rng.bytes(len);
            let parts = rng.usize(1, 9).min(len);
            let mut bufs = Vec::new();
            let mut start = 0;
            for p in 0..parts {
                let end = if p + 1 == parts {
                    len
                } else {
                    start + rng.usize(0, len - start + 1)
                };
                bufs.push(TrackedBuf::from_vec(data[start..end].to_vec()));
                start = end;
            }
            match Chaos::new(file.writev_at(bufs, off as u64), plan).await {
                Some(BufResult(Ok(n), _)) if n == len => model.borrow_mut().set(off, &data),
                Some(BufResult(Ok(n), _)) => {
                    model.borrow_mut().forget(off, len);
                    ctx.fail(format!("{what}: short writev {n}"));
                }
                Some(BufResult(Err(e), _)) => {
                    model.borrow_mut().forget(off, len);
                    file_op_error(ctx, &what, &e);
                }
                None => {
                    if plan != DropPlan::BeforePoll {
                        model.borrow_mut().forget(off, len)
                    }
                }
            }
        }
        5 => {
            let parts = rng.usize(1, 9).min(len);
            let mut sizes = Vec::new();
            let mut remaining = len;
            for p in 0..parts {
                let s = if p + 1 == parts {
                    remaining
                } else {
                    rng.usize(0, remaining + 1)
                };
                sizes.push(s);
                remaining -= s;
            }
            let bufs: Vec<_> = sizes
                .iter()
                .map(|s| TrackedBuf::with_capacity(*s))
                .collect();
            match Chaos::new(file.readv_at(bufs, off as u64), plan).await {
                Some(BufResult(Ok(n), bufs)) => {
                    let data: Vec<u8> = bufs.iter().flat_map(|b| b.0.iter().copied()).collect();
                    if n != len || data.len() != n {
                        ctx.fail(format!("{what}: short readv {n} data {}", data.len()));
                    }
                    model.borrow().check(ctx, &what, off, &data);
                }
                Some(BufResult(Err(e), _)) => file_op_error(ctx, &what, &e),
                None => {}
            }
        }
        6 => {
            // Linked write then read of the same range.
            let data = rng.bytes(len);
            let req = file
                .write_at(TrackedBuf::from_vec(data.clone()), off as u64)
                .then(file.read_at(TrackedBuf::with_capacity(len), off as u64));
            match maybe_timeout!(req) {
                Some((BufResult(Ok(w), _), BufResult(Ok(r), rbuf))) => {
                    if w != len || r != len {
                        ctx.fail(format!("{what}: linked short w={w} r={r}"));
                        model.borrow_mut().forget(off, len);
                    } else {
                        if rbuf.0 != data {
                            ctx.fail(format!("{what}: linked read did not observe linked write"));
                        }
                        model.borrow_mut().set(off, &data);
                    }
                }
                Some((BufResult(w, _), BufResult(r, _))) => {
                    model.borrow_mut().forget(off, len);
                    for e in [w.err(), r.err()].into_iter().flatten() {
                        file_op_error(ctx, &format!("{what} linked"), &e);
                    }
                }
                None => {
                    if plan != DropPlan::BeforePoll {
                        model.borrow_mut().forget(off, len)
                    }
                }
            }
        }
        7 => {
            // Fixed-buffer I/O.
            let Some(pool) = ctx.pool.as_ref() else {
                return;
            };
            let Ok(mut fb) = pool.try_acquire() else {
                return;
            };
            if rng.chance(0.5) {
                let data = rng.bytes(len.min(fb.capacity()));
                let wlen = data.len();
                fb.set_payload(&data).unwrap();
                match maybe_timeout!(file.write_fixed_at(fb, off as u64)) {
                    Some(BufResult(Ok(n), _)) if n == wlen => model.borrow_mut().set(off, &data),
                    Some(BufResult(Ok(n), _)) => {
                        model.borrow_mut().forget(off, wlen);
                        ctx.fail(format!("{what}: short fixed write {n}"));
                    }
                    Some(BufResult(Err(e), _)) => {
                        model.borrow_mut().forget(off, wlen);
                        file_op_error(ctx, &what, &e);
                    }
                    None => {
                        if plan != DropPlan::BeforePoll {
                            model.borrow_mut().forget(off, wlen)
                        }
                    }
                }
            } else {
                let rlen = len.min(fb.capacity());
                fb.set_range(0..rlen).unwrap();
                match maybe_timeout!(file.read_fixed_at(fb, off as u64)) {
                    Some(BufResult(Ok(n), fb)) => {
                        if n != rlen || fb.len() != n {
                            ctx.fail(format!("{what}: short fixed read {n} len {}", fb.len()));
                        }
                        model.borrow().check(ctx, &what, off, fb.as_slice());
                    }
                    Some(BufResult(Err(e), _)) => file_op_error(ctx, &what, &e),
                    None => {}
                }
            }
        }
        _ => {
            let r = if rng.chance(0.5) {
                maybe_timeout!(file.sync())
            } else {
                maybe_timeout!(file.datasync())
            };
            if let Some(Err(e)) = r {
                file_op_error(ctx, &what, &e);
            }
        }
    }
}

async fn pipe_actor(ctx: Rc<Ctx>, mut rng: Rng, id: usize) {
    let (r, w) = match norn_uring::fs::pipe() {
        Ok(p) => p,
        Err(e) => return ctx.fail(format!("pipe actor {id}: pipe {e}")),
    };
    let reader = File::from_uring_fd(r.into_uring_fd());
    let writer = File::from_uring_fd(w.into_uring_fd());
    let total = rng.range(1, 256 * 1024);
    let integrity = rng.chance(0.7);
    let stream = rng.next_u64();
    let tainted = Rc::new(Cell::new(false));
    let mut wrng = rng.fork();
    let wctx = Rc::clone(&ctx);
    let wtainted = Rc::clone(&tainted);
    let wtask = spawn_scoped!(async move {
        let mut pos = 0u64;
        while pos < total {
            if wctx.stopping() && wrng.chance(0.3) {
                break;
            }
            let len = wrng.range(1, 16 * 1024).min(total - pos) as usize;
            let plan = if integrity {
                if wrng.chance(0.2) {
                    DropPlan::BeforePoll
                } else {
                    DropPlan::Never
                }
            } else {
                wrng.drop_plan(0.3)
            };
            let buf = TrackedBuf::from_vec(fill_pattern(stream, pos, len));
            match Chaos::new(writer.write_at(buf, 0), plan).await {
                Some(BufResult(Ok(n), _)) => pos += n as u64,
                Some(BufResult(Err(e), _)) if e.raw_os_error() == Some(libc::EPIPE) => break,
                Some(BufResult(Err(e), _)) => {
                    wctx.fail(format!("pipe actor {id}: write {e}"));
                    break;
                }
                None => {
                    if plan != DropPlan::BeforePoll {
                        wtainted.set(true);
                        // A cancelled write may have been partially applied.
                        break;
                    }
                }
            }
        }
        if wrng.chance(0.5) {
            match writer.close().await {
                Ok(()) => {}
                // A dropped in-flight write still leases the descriptor.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => wctx.fail(format!("pipe actor {id}: writer close {e}")),
            }
        }
        pos
    });
    let mut pos = 0u64;
    loop {
        let len = rng.usize(1, 20 * 1024);
        let plan = if integrity {
            if rng.chance(0.2) {
                DropPlan::BeforePoll
            } else {
                DropPlan::Never
            }
        } else {
            rng.drop_plan(0.3)
        };
        match Chaos::new(reader.read_at(TrackedBuf::with_capacity(len), 0), plan).await {
            Some(BufResult(Ok(0), _)) => break,
            Some(BufResult(Ok(n), buf)) => {
                if !tainted.get() {
                    let expected = fill_pattern(stream, pos, n);
                    if buf.0[..n] != expected[..] {
                        ctx.fail(format!(
                            "pipe actor {id}: data mismatch at {pos} (len {n}, integrity {integrity})"
                        ));
                        tainted.set(true);
                    }
                }
                pos += n as u64;
            }
            Some(BufResult(Err(e), _)) => {
                ctx.fail(format!("pipe actor {id}: read {e}"));
                break;
            }
            None => {
                if plan != DropPlan::BeforePoll {
                    tainted.set(true);
                }
            }
        }
    }
    let written = match wtask.await {
        Ok(n) => n,
        Err(_) => return,
    };
    if !tainted.get() && pos != written {
        ctx.fail(format!(
            "pipe actor {id}: read {pos} bytes but writer wrote {written}"
        ));
    }
}

// ----- TCP -----

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PeerBehaviour {
    Echo,
    HalfClose(u64),
    Reset(u64),
    StopReading,
}

fn std_write_all(
    s: &mut std::net::TcpStream,
    mut data: &[u8],
    stop: &AtomicBool,
) -> io::Result<()> {
    use std::io::Write;
    while !data.is_empty() {
        match s.write(data) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => data = &data[n..],
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) =>
            {
                if stop.load(Ordering::Relaxed) {
                    return Err(e);
                }
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn tcp_peer(mut s: std::net::TcpStream, behaviour: PeerBehaviour, stop: Arc<AtomicBool>) {
    use std::io::Read;
    let _ = s.set_read_timeout(Some(Duration::from_millis(20)));
    let _ = s.set_write_timeout(Some(Duration::from_millis(20)));
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut buf = vec![0u8; 64 * 1024];
    let mut echoed = 0u64;
    let limit = match behaviour {
        PeerBehaviour::HalfClose(n) | PeerBehaviour::Reset(n) => n,
        _ => u64::MAX,
    };
    if behaviour == PeerBehaviour::StopReading {
        while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        return;
    }
    loop {
        if stop.load(Ordering::Relaxed) || Instant::now() > deadline {
            return;
        }
        let want = ((limit - echoed) as usize).min(buf.len());
        if want == 0 {
            break;
        }
        match s.read(&mut buf[..want]) {
            Ok(0) => {
                let _ = s.shutdown(Shutdown::Write);
                // Wait for the norn side to finish so a late RST does not race.
                while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                    match s.read(&mut buf) {
                        Ok(0) => return,
                        Ok(_) => {}
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                        Err(e) if e.kind() == io::ErrorKind::TimedOut => {}
                        Err(_) => return,
                    }
                }
                return;
            }
            Ok(n) => {
                if std_write_all(&mut s, &buf[..n], &stop).is_err() {
                    return;
                }
                echoed += n as u64;
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) => {}
            Err(_) => return,
        }
    }
    match behaviour {
        PeerBehaviour::HalfClose(_) => {
            let _ = s.shutdown(Shutdown::Write);
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                match s.read(&mut buf) {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(e)
                        if matches!(
                            e.kind(),
                            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                        ) => {}
                    Err(_) => return,
                }
            }
        }
        PeerBehaviour::Reset(_) => {
            let sock = socket2::SockRef::from(&s);
            let _ = sock.set_linger(Some(Duration::ZERO));
        }
        _ => {}
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecvMode {
    Plain,
    Ring,
    RingMulti,
    Bundle,
    BundleMulti,
}

struct TcpReceiver {
    /// Set by the sender when a submitted send was dropped: the stream may
    /// then contain bytes the sender will resend.
    send_tainted: Rc<Cell<bool>>,
    ctx: Rc<Ctx>,
    id: usize,
    stream: u64,
    pos: u64,
    tainted: bool,
    mode: RecvMode,
    detail: String,
}

impl TcpReceiver {
    fn check(&mut self, data: &[u8]) {
        if self.send_tainted.get() {
            self.tainted = true;
        }
        if !self.tainted {
            for (i, b) in data.iter().enumerate() {
                let p = self.pos + i as u64;
                if *b != pattern(self.stream, p) {
                    // Locate where the observed bytes came from in the stream.
                    let probe = &data[i..data.len().min(i + 16)];
                    let mut source = None;
                    if probe.len() == 16 {
                        for q in 0..p + 1024 * 1024 {
                            if (0..16).all(|k| pattern(self.stream, q + k as u64) == probe[k]) {
                                source = Some(q);
                                break;
                            }
                        }
                    }
                    self.ctx.fail(format!(
                        "tcp actor {}: echo mismatch at stream offset {p} (chunk {} at {}, mode {:?}, {}); observed bytes match stream offset {source:?}; first observed {:x?}",
                        self.id,
                        data.len(),
                        self.pos,
                        self.mode,
                        self.detail,
                        probe
                    ));
                    self.tainted = true;
                    break;
                }
            }
        }
        self.pos += data.len() as u64;
    }

    fn error(&mut self, what: &str, e: &io::Error) -> bool {
        if !benign_net_error(e) {
            self.ctx.fail(format!(
                "tcp actor {}: {what} ({:?}): {e}",
                self.id, self.mode
            ));
        }
        e.raw_os_error() != Some(libc::ENOBUFS)
    }
}

async fn tcp_receive(
    sock: Rc<TcpSocket>,
    mut rx: TcpReceiver,
    ring: RecvBufRing,
    mut rng: Rng,
    integrity: bool,
) -> TcpReceiver {
    let ctx = Rc::clone(&rx.ctx);
    let plan = |rng: &mut Rng| {
        if integrity {
            if rng.chance(0.1) {
                DropPlan::BeforePoll
            } else {
                DropPlan::Never
            }
        } else {
            rng.drop_plan(0.2)
        }
    };
    let deadline = Instant::now() + Duration::from_secs(15);
    'outer: loop {
        if Instant::now() > deadline {
            break;
        }
        if ctx.stopping() && !integrity && rng.chance(0.2) {
            break;
        }
        match rx.mode {
            RecvMode::Plain => {
                let len = rng.usize(1, 32 * 1024);
                let p = plan(&mut rng);
                match Chaos::new(sock.recv(TrackedBuf::with_capacity(len)), p).await {
                    Some(BufResult(Ok(0), _)) => break,
                    Some(BufResult(Ok(n), buf)) => rx.check(&buf.0[..n]),
                    Some(BufResult(Err(e), _)) => {
                        if rx.error("recv", &e) {
                            break;
                        }
                    }
                    None => rx.tainted |= p != DropPlan::BeforePoll,
                }
            }
            RecvMode::Ring => {
                let p = plan(&mut rng);
                match Chaos::new(sock.recv_ring(&ring), p).await {
                    Some(Ok(buf)) if buf.is_empty() => break,
                    Some(Ok(buf)) => {
                        rx.check(&buf);
                        if rng.chance(0.1) {
                            sleep(rng.micros(300)).await;
                        }
                        drop(buf);
                    }
                    Some(Err(e)) => {
                        if rx.error("recv_ring", &e) {
                            break;
                        }
                        sleep(rng.micros(200)).await;
                    }
                    None => rx.tainted |= p != DropPlan::BeforePoll,
                }
            }
            RecvMode::Bundle => {
                let p = plan(&mut rng);
                match Chaos::new(sock.recv_bundle(&ring), p).await {
                    Some(Ok(bundle)) if bundle.is_empty() => break,
                    Some(Ok(bundle)) => {
                        let mut total = 0;
                        let count = bundle.buffer_count();
                        let len = bundle.len();
                        for (i, chunk) in bundle.iter().enumerate() {
                            total += chunk.len();
                            rx.detail = format!(
                                "ring {:?}, bundle len {len} buffers {count} index {i}",
                                ring
                            );
                            rx.check(chunk);
                        }
                        if total != bundle.len() {
                            ctx.fail(format!(
                                "tcp actor {}: bundle chunks {total} != len {}",
                                rx.id,
                                bundle.len()
                            ));
                        }
                        if rng.chance(0.3) {
                            let bufs = bundle.into_bufs();
                            if rng.chance(0.5) {
                                drop(bufs.into_iter().rev().collect::<Vec<_>>());
                            }
                        }
                    }
                    Some(Err(e)) => {
                        if rx.error("recv_bundle", &e) {
                            break;
                        }
                        sleep(rng.micros(200)).await;
                    }
                    None => rx.tainted |= p != DropPlan::BeforePoll,
                }
            }
            RecvMode::RingMulti | RecvMode::BundleMulti => {
                let multi = rx.mode == RecvMode::RingMulti;
                let mut ring_stream = Some(Box::pin(sock.recv_ring_multi(&ring)));
                let mut bundle_stream = Some(Box::pin(sock.recv_bundle_multi(&ring)));
                if multi {
                    bundle_stream = None;
                } else {
                    ring_stream = None;
                }
                // Drop the stream after a random number of items (non-integrity only).
                let budget = if integrity {
                    u64::MAX
                } else {
                    rng.range(1, 50)
                };
                let mut items = 0;
                loop {
                    if items >= budget {
                        rx.tainted = true;
                        continue 'outer;
                    }
                    let next = if let Some(s) = ring_stream.as_mut() {
                        match s.next().await {
                            None => None,
                            Some(Ok(buf)) => {
                                rx.check(&buf);
                                Some(Ok(()))
                            }
                            Some(Err(e)) => Some(Err(e)),
                        }
                    } else {
                        let s = bundle_stream.as_mut().unwrap();
                        match s.next().await {
                            None => None,
                            Some(Ok(bundle)) => {
                                let count = bundle.buffer_count();
                                let len = bundle.len();
                                for (i, chunk) in bundle.iter().enumerate() {
                                    rx.detail = format!(
                                        "ring {:?}, bundle len {len} buffers {count} index {i}",
                                        ring
                                    );
                                    rx.check(chunk);
                                }
                                Some(Ok(()))
                            }
                            Some(Err(e)) => Some(Err(e)),
                        }
                    };
                    items += 1;
                    match next {
                        None => break 'outer,
                        Some(Ok(())) => {
                            if rng.chance(0.05) {
                                sleep(rng.micros(300)).await;
                            }
                        }
                        Some(Err(e)) => {
                            rx.error(
                                if multi {
                                    "recv_ring_multi"
                                } else {
                                    "recv_bundle_multi"
                                },
                                &e,
                            );
                            // Streams end after a terminal error.
                            break 'outer;
                        }
                    }
                }
            }
        }
    }
    rx
}

async fn tcp_echo_actor(ctx: Rc<Ctx>, mut rng: Rng, id: usize) {
    let behaviour = match rng.below(10) {
        0..=6 => PeerBehaviour::Echo,
        7 => PeerBehaviour::HalfClose(rng.range(0, 64 * 1024)),
        8 => PeerBehaviour::Reset(rng.range(0, 64 * 1024)),
        _ => PeerBehaviour::StopReading,
    };
    let integrity = behaviour == PeerBehaviour::Echo && rng.chance(0.75);
    let total = rng.range(1, 512 * 1024);
    let stream_id = rng.next_u64();
    let norn_listens = rng.chance(0.5);
    let stop = Arc::clone(&ctx.peer_stop);

    let sock = if norn_listens {
        let listener = match TcpListener::bind("127.0.0.1:0".parse().unwrap(), 4).await {
            Ok(l) => l,
            Err(e) if env_error(&e) => return ctx.env(format!("tcp actor {id}: bind {e}")),
            Err(e) => return ctx.fail(format!("tcp actor {id}: bind {e}")),
        };
        let addr = listener.local_addr().unwrap();
        // The peer reports a failed connect: without it nothing would ever
        // be accepted, and the accept below would wait forever.
        let peer_error: Arc<Mutex<Option<io::Error>>> = Arc::new(Mutex::new(None));
        let peer_error_out = Arc::clone(&peer_error);
        ctx.spawn_peer(move || match std::net::TcpStream::connect(addr) {
            Ok(s) => tcp_peer(s, behaviour, stop),
            Err(e) => *peer_error_out.lock().unwrap() = Some(e),
        });
        let accept = async {
            if rng.chance(0.5) {
                listener.accept().await.map(|(s, _)| s)
            } else {
                let mut incoming = Box::pin(listener.incoming());
                match incoming.next().await {
                    Some(r) => r,
                    None => Err(io::Error::other("incoming ended")),
                }
            }
        };
        let accepted = match race(Box::pin(accept), sleep(Duration::from_secs(5))).await {
            Either::Left(r) => r,
            Either::Right(_) => {
                return match peer_error.lock().unwrap().take() {
                    Some(e) if env_error(&e) => {
                        ctx.env(format!("tcp actor {id}: peer connect {e}"))
                    }
                    Some(e) => ctx.fail(format!("tcp actor {id}: peer connect {e}")),
                    None => ctx.fail(format!("tcp actor {id}: accept timed out")),
                };
            }
        };
        let close_listener = rng.chance(0.5);
        if close_listener {
            match listener.close().await {
                Ok(()) => {}
                // A dropped multishot accept still leases the descriptor
                // until its cancellation completes.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => ctx.fail(format!("tcp actor {id}: listener close {e}")),
            }
        }
        match accepted {
            Ok(s) => s,
            Err(e) => return ctx.fail(format!("tcp actor {id}: accept {e}")),
        }
    } else {
        let listener = match std::net::TcpListener::bind("127.0.0.1:0") {
            Ok(l) => l,
            Err(e) => return ctx.env(format!("tcp actor {id}: std bind {e}")),
        };
        let addr = listener.local_addr().unwrap();
        ctx.spawn_peer(move || {
            listener.set_nonblocking(true).unwrap();
            let deadline = Instant::now() + Duration::from_secs(10);
            while Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((s, _)) => {
                        s.set_nonblocking(false).unwrap();
                        return tcp_peer(s, behaviour, stop);
                    }
                    Err(_) => std::thread::sleep(Duration::from_millis(1)),
                }
            }
        });
        match TcpSocket::connect(addr).await {
            Ok(s) => s,
            Err(e) if env_error(&e) => return ctx.env(format!("tcp actor {id}: connect {e}")),
            Err(e) => return ctx.fail(format!("tcp actor {id}: connect {e}")),
        }
    };
    if behaviour == PeerBehaviour::StopReading || rng.chance(0.2) {
        let _ = sock.set_send_buffer_size(rng.usize(1024, 16 * 1024)).await;
    }
    let sock = Rc::new(sock);
    let ring = ctx.ring(&mut rng);
    let mode = rng.pick(&[
        RecvMode::Plain,
        RecvMode::Ring,
        RecvMode::RingMulti,
        RecvMode::Bundle,
        RecvMode::BundleMulti,
    ]);
    status(id, || {
        format!("tcp {mode:?} ring {:?} peer {behaviour:?} integrity {integrity} listens {norn_listens}", ring)
    });
    let shared_taint = Rc::new(Cell::new(false));
    let rx = TcpReceiver {
        send_tainted: Rc::clone(&shared_taint),
        ctx: Rc::clone(&ctx),
        id,
        stream: stream_id,
        pos: 0,
        tainted: false,
        mode,
        detail: String::new(),
    };
    let receiver = spawn_scoped!(tcp_receive(
        Rc::clone(&sock),
        rx,
        ring,
        rng.fork(),
        integrity,
    ));

    // Sender.
    let mut pos = 0u64;
    let mut send_tainted = false;
    let send_deadline = Instant::now() + Duration::from_secs(10);
    while pos < total && Instant::now() < send_deadline {
        if ctx.stopping() && !integrity && rng.chance(0.2) {
            break;
        }
        let len = rng.range(1, 64 * 1024).min(total - pos) as usize;
        let plan = if integrity {
            if rng.chance(0.1) {
                DropPlan::BeforePoll
            } else {
                DropPlan::Never
            }
        } else {
            rng.drop_plan(0.2)
        };
        let buf = TrackedBuf::from_vec(fill_pattern(stream_id, pos, len));
        let zc = rng.chance(0.15);
        let res = if behaviour == PeerBehaviour::StopReading || (!integrity && rng.chance(0.2)) {
            let d = rng.micros(5000);
            if zc {
                Chaos::new(
                    boxed(
                        sock.send_zc(buf)
                            .map(|res| res.map(|z| z.bytes_sent()))
                            .timeout(d),
                    ),
                    plan,
                )
                .await
            } else {
                Chaos::new(boxed(sock.send(buf).timeout(d)), plan).await
            }
        } else if zc {
            Chaos::new(
                boxed(sock.send_zc(buf).map(|res| res.map(|z| z.bytes_sent()))),
                plan,
            )
            .await
        } else {
            Chaos::new(boxed(sock.send(buf)), plan).await
        };
        if std::env::var_os("STRESS_TRACE_SEND").is_some() {
            eprintln!(
                "TRACE tcp {id} send pos {pos} len {len} plan {plan:?} -> {:?}",
                res.as_ref()
                    .map(|r| r.0.as_ref().map_err(|e| e.to_string()))
            );
        }
        match res {
            Some(BufResult(Ok(n), _)) => pos += n as u64,
            Some(BufResult(Err(e), _)) => {
                if e.raw_os_error() == Some(libc::ECANCELED) {
                    send_tainted = true;
                    shared_taint.set(true);
                    if behaviour == PeerBehaviour::StopReading {
                        break;
                    }
                    continue;
                }
                if is_batch_too_large(&e) && ctx.cfg.sq_entries < 2 {
                    break;
                }
                if !benign_net_error(&e) {
                    ctx.fail(format!("tcp actor {id}: send ({behaviour:?}): {e}"));
                }
                break;
            }
            None => {
                if plan != DropPlan::BeforePoll {
                    send_tainted = true;
                    shared_taint.set(true);
                }
            }
        }
    }
    match sock.shutdown(Shutdown::Write).await {
        Ok(()) => {}
        Err(e) if benign_net_error(&e) => {}
        Err(e) => ctx.fail(format!("tcp actor {id}: shutdown {e}")),
    }
    let rx = match receiver.await {
        Ok(rx) => rx,
        Err(_) => return,
    };
    if integrity && !send_tainted && !rx.tainted && pos == total && rx.pos != total {
        ctx.fail(format!(
            "tcp actor {id}: received {} of {total} echoed bytes (mode {:?}, peer {behaviour:?})",
            rx.pos, rx.mode
        ));
    }
    if !send_tainted && !rx.tainted && rx.pos > pos {
        ctx.fail(format!(
            "tcp actor {id}: received {} bytes but only sent {pos}",
            rx.pos
        ));
    }
    let sock = match Rc::try_unwrap(sock) {
        Ok(s) => s,
        Err(_) => return,
    };
    if rng.chance(0.5) {
        match sock.close().await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => ctx.fail(format!("tcp actor {id}: close {e}")),
        }
    }
}

async fn tcp_accept_actor(ctx: Rc<Ctx>, mut rng: Rng, id: usize) {
    let listener = match TcpListener::bind("127.0.0.1:0".parse().unwrap(), 16).await {
        Ok(l) => l,
        Err(e) if env_error(&e) => return ctx.env(format!("accept actor {id}: bind {e}")),
        Err(e) => return ctx.fail(format!("accept actor {id}: bind {e}")),
    };
    let addr = listener.local_addr().unwrap();
    let conns = rng.usize(1, 16);
    let stop = Arc::clone(&ctx.peer_stop);
    let mut prng = rng.fork();
    ctx.spawn_peer(move || {
        let mut held = Vec::new();
        for _ in 0..conns {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            match std::net::TcpStream::connect(addr) {
                Ok(s) => match prng.below(3) {
                    0 => {
                        let _ = socket2::SockRef::from(&s).set_linger(Some(Duration::ZERO));
                    }
                    1 => {
                        use std::io::Write;
                        let mut s = s;
                        let _ = s.write_all(b"hello");
                        held.push(s);
                    }
                    _ => held.push(s),
                },
                Err(_) => break,
            }
            if prng.chance(0.2) {
                std::thread::sleep(Duration::from_micros(prng.below(2000)));
            }
        }
        let deadline = Instant::now() + Duration::from_millis(500);
        while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        drop(held);
    });
    let mut accepted = 0;
    let mut incoming = Box::pin(listener.incoming());
    let budget = rng.range(1, conns as u64 + 2);
    let deadline = Instant::now() + Duration::from_secs(5);
    while accepted < budget && Instant::now() < deadline {
        status(id, || format!("accept {accepted}/{budget}"));
        let item = if rng.chance(0.3) {
            let ring = ctx.ring(&mut rng);
            match race(incoming.next(), sleep(Duration::from_millis(50))).await {
                Either::Left(item) => {
                    // Try a ring receive on the accepted socket.
                    if let Some(Ok(s)) = &item {
                        match race(s.recv_ring(&ring), sleep(Duration::from_millis(5))).await {
                            Either::Left(Ok(buf)) => {
                                if !buf.is_empty() && &buf[..] != b"hello" {
                                    ctx.fail(format!("accept actor {id}: bad payload"));
                                }
                            }
                            Either::Left(Err(e)) if !benign_net_error(&e) => {
                                ctx.fail(format!("accept actor {id}: recv_ring {e}"))
                            }
                            _ => {}
                        }
                    }
                    item
                }
                Either::Right(()) => continue,
            }
        } else {
            match race(incoming.next(), sleep(Duration::from_millis(50))).await {
                Either::Left(item) => item,
                Either::Right(()) => continue,
            }
        };
        match item {
            None => break,
            Some(Ok(s)) => {
                accepted += 1;
                if rng.chance(0.3) {
                    let _ = s.close().await;
                }
            }
            Some(Err(e)) => {
                if !benign_net_error(&e) {
                    ctx.fail(format!("accept actor {id}: incoming {e}"));
                }
                break;
            }
        }
        if rng.chance(0.1) {
            // Drop and recreate the multishot accept mid-stream.
            incoming = Box::pin(listener.incoming());
        }
    }
    status(id, || "accept: dropping incoming".to_string());
    drop(incoming);
    if rng.chance(0.5) {
        status(id, || "accept: closing listener".to_string());
        let _ = listener.close().await;
    }
}

// ----- UDP -----

const DGRAM_MAGIC: u32 = 0xA5C3_0F96;

fn make_dgram(actor: u32, seq: u32, len: usize) -> Vec<u8> {
    let len = len.max(16);
    let mut v = Vec::with_capacity(len);
    v.extend_from_slice(&DGRAM_MAGIC.to_le_bytes());
    v.extend_from_slice(&actor.to_le_bytes());
    v.extend_from_slice(&seq.to_le_bytes());
    v.extend_from_slice(&(len as u32).to_le_bytes());
    let stream = (u64::from(actor) << 32) | u64::from(seq);
    for i in 16..len {
        v.push(pattern(stream, i as u64));
    }
    v
}

/// Validates a datagram; returns its sequence number.
fn check_dgram(actor: u32, d: &[u8]) -> Result<u32, String> {
    if d.len() < 16 {
        return Err(format!("short datagram {}", d.len()));
    }
    let word = |i: usize| u32::from_le_bytes(d[i..i + 4].try_into().unwrap());
    if word(0) != DGRAM_MAGIC || word(4) != actor {
        return Err(format!("bad header {:x?}", &d[..16]));
    }
    let seq = word(8);
    let len = word(12) as usize;
    if len != d.len() {
        return Err(format!("seq {seq}: length {} != header {len}", d.len()));
    }
    let stream = (u64::from(actor) << 32) | u64::from(seq);
    for (i, b) in d.iter().enumerate().skip(16) {
        if *b != pattern(stream, i as u64) {
            return Err(format!("seq {seq}: payload mismatch at {i}"));
        }
    }
    Ok(seq)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UdpMode {
    RecvFrom,
    RecvFromRing,
    RecvFromRingMulti,
    RecvRingMulti,
    Bundle,
}

async fn udp_actor(ctx: Rc<Ctx>, mut rng: Rng, id: usize) {
    let rx = match UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await {
        Ok(s) => Rc::new(s),
        Err(e) if env_error(&e) => return ctx.env(format!("udp actor {id}: bind {e}")),
        Err(e) => return ctx.fail(format!("udp actor {id}: bind {e}")),
    };
    let tx = match UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await {
        Ok(s) => Rc::new(s),
        Err(e) if env_error(&e) => return ctx.env(format!("udp actor {id}: bind tx {e}")),
        Err(e) => return ctx.fail(format!("udp actor {id}: bind tx {e}")),
    };
    let rx_addr = rx.local_addr().unwrap();
    let tx_addr = tx.local_addr().unwrap();
    let ring = ctx.ring(&mut rng);
    let mode = rng.pick(&[
        UdpMode::RecvFrom,
        UdpMode::RecvFromRing,
        UdpMode::RecvFromRingMulti,
        UdpMode::RecvRingMulti,
        UdpMode::Bundle,
    ]);
    // Keep datagrams within one provided buffer, including the recvmsg
    // multishot header and source address area (avoids known truncation).
    let max_len = match mode {
        UdpMode::RecvFrom => 2048,
        UdpMode::RecvFromRingMulti => ring.buf_capacity().saturating_sub(200),
        _ => ring.buf_capacity(),
    };
    if max_len < 16 {
        return;
    }
    if mode == UdpMode::RecvRingMulti {
        if let Err(e) = rx.connect(tx_addr).await {
            return ctx.fail(format!("udp actor {id}: connect {e}"));
        }
    }
    let count = rng.range(1, 200) as u32;
    let actor_tag = id as u32 ^ (ctx.seed as u32);
    let mut srng = rng.fork();
    let sctx = Rc::clone(&ctx);
    let stx = Rc::clone(&tx);
    let sent_done = Rc::new(Cell::new(false));
    let sdone = Rc::clone(&sent_done);
    let sender = spawn_scoped!(async move {
        for seq in 0..count {
            let len = srng.usize(16, max_len + 1);
            let d = make_dgram(actor_tag, seq, len);
            let plan = srng.drop_plan(0.1);
            match Chaos::new(stx.send_to(TrackedBuf::from_vec(d), rx_addr), plan).await {
                Some(BufResult(Ok(_), _)) | None => {}
                Some(BufResult(Err(e), _)) => {
                    if env_error(&e) {
                        sctx.env(format!("udp actor {id}: send_to {e}"));
                    } else if !benign_net_error(&e) {
                        sctx.fail(format!("udp actor {id}: send_to {e}"));
                    }
                }
            }
            if srng.chance(0.1) {
                sleep(srng.micros(500)).await;
            }
        }
        sdone.set(true);
    });
    status(id, || {
        format!("udp {mode:?} ring {:?} receiving {count}", ring)
    });
    let mut seen = HashSet::new();
    let record = |d: &[u8], seen: &mut HashSet<u32>| match check_dgram(actor_tag, d) {
        Ok(seq) => {
            if !seen.insert(seq) {
                ctx.fail(format!(
                    "udp actor {id} ({mode:?}): duplicate datagram {seq}"
                ));
            }
        }
        Err(e) => ctx.fail(format!("udp actor {id} ({mode:?}): {e}")),
    };
    let quiet = Duration::from_millis(30);
    let deadline = Instant::now() + Duration::from_secs(10);
    match mode {
        UdpMode::RecvFrom | UdpMode::RecvFromRing | UdpMode::Bundle => loop {
            if seen.len() as u32 == count || Instant::now() > deadline {
                break;
            }
            let plan = rng.drop_plan(0.15);
            let ev = match mode {
                UdpMode::RecvFrom => {
                    let fut = rx.recv_from(TrackedBuf::with_capacity(2048));
                    match race(Chaos::new(fut, plan), sleep(quiet)).await {
                        Either::Left(Some(BufResult(Ok((n, addr)), buf))) => {
                            if addr != tx_addr {
                                ctx.fail(format!("udp actor {id}: source {addr} != {tx_addr}"));
                            }
                            record(&buf.0[..n], &mut seen);
                            true
                        }
                        Either::Left(Some(BufResult(Err(e), _))) => {
                            if !benign_net_error(&e) {
                                ctx.fail(format!("udp actor {id}: recv_from {e}"));
                            }
                            true
                        }
                        Either::Left(None) => true,
                        Either::Right(()) => false,
                    }
                }
                UdpMode::RecvFromRing => {
                    let fut = rx.recv_from_ring(&ring);
                    match race(Chaos::new(fut, plan), sleep(quiet)).await {
                        Either::Left(Some(Ok((buf, addr)))) => {
                            if addr != tx_addr {
                                ctx.fail(format!("udp actor {id}: source {addr} != {tx_addr}"));
                            }
                            record(&buf, &mut seen);
                            true
                        }
                        Either::Left(Some(Err(e))) => {
                            if !benign_net_error(&e) {
                                ctx.fail(format!("udp actor {id}: recv_from_ring {e}"));
                            } else {
                                sleep(rng.micros(200)).await;
                            }
                            true
                        }
                        Either::Left(None) => true,
                        Either::Right(()) => false,
                    }
                }
                _ => {
                    let fut = rx.recv_bundle(&ring);
                    match race(Chaos::new(fut, plan), sleep(quiet)).await {
                        Either::Left(Some(Ok(bundle))) => {
                            let data: Vec<u8> =
                                bundle.iter().flat_map(|c| c.iter().copied()).collect();
                            record(&data, &mut seen);
                            true
                        }
                        Either::Left(Some(Err(e))) => {
                            if !benign_net_error(&e) {
                                ctx.fail(format!("udp actor {id}: recv_bundle {e}"));
                            } else {
                                sleep(rng.micros(200)).await;
                            }
                            true
                        }
                        Either::Left(None) => true,
                        Either::Right(()) => false,
                    }
                }
            };
            if !ev && sent_done.get() {
                break;
            }
        },
        UdpMode::RecvFromRingMulti | UdpMode::RecvRingMulti => {
            let budget = rng.range(1, 400);
            let mut items = 0;
            'outer: loop {
                let mut from_stream = None;
                let mut plain_stream = None;
                if mode == UdpMode::RecvFromRingMulti {
                    from_stream = Some(Box::pin(rx.recv_from_ring_multi(&ring)));
                } else {
                    plain_stream = Some(Box::pin(rx.recv_ring_multi(&ring)));
                }
                loop {
                    if seen.len() as u32 == count || Instant::now() > deadline {
                        break 'outer;
                    }
                    if items >= budget && rng.chance(0.5) {
                        items = 0;
                        continue 'outer;
                    }
                    items += 1;
                    let got = if let Some(s) = from_stream.as_mut() {
                        match race(s.next(), sleep(quiet)).await {
                            Either::Left(Some(Ok((buf, addr)))) => {
                                if addr != tx_addr {
                                    ctx.fail(format!(
                                        "udp actor {id}: multi source {addr} != {tx_addr}"
                                    ));
                                }
                                record(buf.payload(), &mut seen);
                                Some(true)
                            }
                            Either::Left(Some(Err(e))) => {
                                if !benign_net_error(&e) {
                                    ctx.fail(format!("udp actor {id}: recv_from_ring_multi {e}"));
                                }
                                continue 'outer;
                            }
                            Either::Left(None) => continue 'outer,
                            Either::Right(()) => Some(false),
                        }
                    } else {
                        let s = plain_stream.as_mut().unwrap();
                        match race(s.next(), sleep(quiet)).await {
                            Either::Left(Some(Ok(buf))) => {
                                record(&buf, &mut seen);
                                Some(true)
                            }
                            Either::Left(Some(Err(e))) => {
                                if !benign_net_error(&e) {
                                    ctx.fail(format!("udp actor {id}: recv_ring_multi {e}"));
                                }
                                continue 'outer;
                            }
                            Either::Left(None) => continue 'outer,
                            Either::Right(()) => Some(false),
                        }
                    };
                    if got == Some(false) && sent_done.get() {
                        break 'outer;
                    }
                }
            }
        }
    }
    status(id, || {
        format!("udp {mode:?} got {}/{count}, awaiting sender", seen.len())
    });
    let _ = sender.await;
    status(id, || "udp closing".to_string());
    if rng.chance(0.5) {
        if let Ok(rx) = Rc::try_unwrap(rx) {
            let _ = rx.close().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Post-run buffer-ring probe
// ---------------------------------------------------------------------------

/// The kernel's receive-drop counter for the UDP socket bound to `port` in this
/// network namespace (the `drops` column of `/proc/net/udp`), or 0 if unknown.
fn udp_drops(port: u16) -> u64 {
    let Ok(table) = std::fs::read_to_string("/proc/net/udp") else {
        return 0;
    };
    let suffix = format!(":{port:04X}");
    table
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let local = fields.get(1)?;
            if local.ends_with(&suffix) {
                fields.last()?.parse::<u64>().ok()
            } else {
                None
            }
        })
        .sum()
}

/// How long a probe waits for a datagram that was already sent to loopback.
/// A delivered datagram arrives at once, so expiry means it was lost.
const PROBE_RECV_TIMEOUT: Duration = Duration::from_secs(2);

/// `recv_from_ring` that gives up with `TimedOut` instead of blocking forever
/// on a datagram the kernel dropped, which would otherwise show up as a
/// watchdog hang rather than as the `udp_drops` environment case.
async fn probe_recv(
    rx: &UdpSocket,
    ring: &RecvBufRing,
) -> io::Result<(norn_uring::bufring::BufRingBuf, std::net::SocketAddr)> {
    match race(rx.recv_from_ring(ring), sleep(PROBE_RECV_TIMEOUT)).await {
        Either::Left(result) => result,
        Either::Right(()) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("no datagram within {PROBE_RECV_TIMEOUT:?}"),
        )),
    }
}

/// Proves the ring holds exactly `buf_cnt` distinct selectable buffers and is
/// not quarantined.
async fn probe_ring(ctx: &Ctx, ring: &RecvBufRing) {
    let cnt = ring.buf_count() as usize;
    let rx = match UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await {
        Ok(s) => s,
        Err(e) if env_error(&e) => return ctx.env(format!("probe: bind {e}")),
        Err(e) => return ctx.fail(format!("probe: bind {e}")),
    };
    let tx = match std::net::UdpSocket::bind("127.0.0.1:0") {
        Ok(tx) => tx,
        Err(e) => return ctx.env(format!("probe: std bind {e}")),
    };
    let addr = rx.local_addr().unwrap();
    for i in 0..=cnt {
        if let Err(e) = tx.send_to(&(i as u32).to_le_bytes(), addr) {
            return ctx.env(format!("probe: send_to {e}"));
        }
    }
    let mut held = Vec::new();
    let mut ptrs = HashSet::new();
    for i in 0..cnt {
        match probe_recv(&rx, ring).await {
            Ok((buf, _)) => {
                if buf[..] != (i as u32).to_le_bytes() {
                    ctx.fail(format!("probe: datagram {i} payload {:?}", &buf[..]));
                }
                if !ptrs.insert(buf.as_ptr() as usize) {
                    ctx.fail(format!("probe: buffer handed out twice ({ring:?})"));
                }
                held.push(buf);
            }
            // Only a datagram the kernel itself dropped (full receive queue,
            // memory pressure) explains missing buffers; an unrelated
            // environment issue elsewhere in the seed does not. A timeout is
            // env only if the kernel dropped every datagram not yet received
            // (`cnt + 1 - i` were still due), so a lost completion for a
            // delivered datagram still fails the seed.
            Err(e)
                if {
                    let drops = udp_drops(addr.port());
                    if e.kind() == io::ErrorKind::TimedOut {
                        drops >= (cnt + 1 - i) as u64
                    } else {
                        drops > 0
                    }
                } =>
            {
                return ctx.env(format!(
                    "probe: {i} of {cnt} (kernel dropped datagrams): {e}"
                ));
            }
            Err(e) => {
                ctx.fail(format!(
                    "probe: only {i} of {cnt} buffers selectable ({ring:?}): {e}"
                ));
                return;
            }
        }
    }
    match probe_recv(&rx, ring).await {
        Err(e) if e.raw_os_error() == Some(libc::ENOBUFS) => {}
        Err(e) if e.kind() == io::ErrorKind::TimedOut && udp_drops(addr.port()) > 0 => {
            return ctx.env(format!("probe: extra datagram lost to a kernel drop: {e}"));
        }
        Ok((buf, _)) => ctx.fail(format!(
            "probe: ring yielded more than {cnt} buffers (extra {:?}, {ring:?})",
            &buf[..]
        )),
        Err(e) => ctx.fail(format!("probe: expected ENOBUFS, got {e}")),
    }
    drop(held);
    match probe_recv(&rx, ring).await {
        Ok((buf, _)) if buf[..] == (cnt as u32).to_le_bytes() => {}
        Err(e) if e.kind() == io::ErrorKind::TimedOut && udp_drops(addr.port()) > 0 => {
            ctx.env(format!("probe: final datagram lost to a kernel drop: {e}"))
        }
        Ok((buf, _)) => ctx.fail(format!("probe: final payload {:?}", &buf[..])),
        Err(e) => ctx.fail(format!("probe: ring unusable after returns: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Scenario driver
// ---------------------------------------------------------------------------

fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

fn fd_list() -> Vec<String> {
    let mut v = Vec::new();
    for e in std::fs::read_dir("/proc/self/fd").unwrap().flatten() {
        let target = std::fs::read_link(e.path())
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        v.push(format!("{}->{target}", e.file_name().to_string_lossy()));
    }
    v
}

static CURRENT_SEED: AtomicU64 = AtomicU64::new(u64::MAX);
static SEED_STARTED_MS: AtomicU64 = AtomicU64::new(0);
static PHASE: AtomicUsize = AtomicUsize::new(0);
const PHASES: [&str; 5] = ["setup", "run", "teardown", "post-checks", "idle"];

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn start_watchdog(hang_secs: u64) {
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_millis(500));
        let seed = CURRENT_SEED.load(Ordering::SeqCst);
        if seed == u64::MAX {
            continue;
        }
        let started = SEED_STARTED_MS.load(Ordering::SeqCst);
        if now_ms().saturating_sub(started) > hang_secs * 1000 {
            let phase = PHASES[PHASE.load(Ordering::SeqCst)];
            // Write to the real stderr: this thread inherits libtest's output
            // capture, which `abort` would throw away.
            use std::io::Write;
            let mut err = std::io::stderr().lock();
            let _ = writeln!(
                err,
                "STRESS HANG seed={seed} phase={phase} after {hang_secs}s (replay: STRESS_SEED={seed} STRESS_REPEAT=20)\nconfig: {:?}",
                Config::generate(seed)
            );
            for w in WARNINGS.lock().unwrap().iter() {
                let _ = writeln!(err, "  warning: {w}");
            }
            for (i, st) in STATUS.lock().unwrap().iter().enumerate() {
                let _ = writeln!(err, "  actor {i}: {st}");
            }
            let _ = err.flush();
            std::process::abort();
        }
    });
}

fn run_seed(seed: u64, verbose: bool) -> Result<(), Vec<String>> {
    let cfg = Config::generate(seed);
    if verbose {
        eprintln!("seed {seed}: {cfg:?}");
    }
    PHASE.store(0, Ordering::SeqCst);
    SEED_STARTED_MS.store(now_ms(), Ordering::SeqCst);
    CURRENT_SEED.store(seed, Ordering::SeqCst);
    WARNINGS.lock().unwrap().clear();
    STATUS.lock().unwrap().clear();
    ENV_ISSUES.store(0, Ordering::SeqCst);
    let live_before = LIVE_BUFS.load(Ordering::SeqCst);
    let fds_before = fd_count();
    let tmpdir = std::env::temp_dir().join(format!("norn-stress-{}-{seed}", std::process::id()));

    let options = DriverOptions {
        completion_drain_budget: cfg.drain_budget.and_then(NonZeroUsize::new),
    };
    let driver = match Driver::new_with_options(cfg.builder(), cfg.sq_entries, options) {
        Ok(d) => d,
        Err(e) => {
            UNSUPPORTED_CONFIGS.fetch_add(1, Ordering::SeqCst);
            if verbose {
                eprintln!("seed {seed}: unsupported ring config: {e}");
            }
            return Ok(());
        }
    };
    // Created only once the seed really runs, so skipped seeds leave nothing
    // behind in the temp dir.
    let _ = std::fs::create_dir_all(&tmpdir);
    let unparker = driver.unparker();
    let unpark_stop = Arc::new(AtomicBool::new(false));
    let unpark_thread = if cfg.remote_unpark {
        let stop = Arc::clone(&unpark_stop);
        let mut rng = Rng::new(seed ^ 0x5555);
        Some(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                unparker.unpark();
                let d = rng.below(800);
                if d > 50 {
                    std::thread::sleep(Duration::from_micros(d));
                }
            }
        }))
    } else {
        drop(unparker);
        None
    };
    let park = ChaosPark {
        inner: driver,
        rng: Rng::new(seed ^ 0xAAAA),
        p_nopark: cfg.p_nopark,
        p_timeout: cfg.p_timeout,
        max_timeout_us: 2000,
    };
    let mut ex = LocalExecutor::new_with_config(
        park,
        norn_executor::Config {
            task_poll_budget: NonZeroUsize::new(cfg.task_budget).unwrap(),
        },
    );
    let peer_stop = Arc::new(AtomicBool::new(false));
    let errors_cell: Rc<RefCell<Vec<String>>> = Rc::new(RefCell::new(Vec::new()));
    let peers_cell: Rc<RefCell<Vec<std::thread::JoinHandle<()>>>> =
        Rc::new(RefCell::new(Vec::new()));
    let leftovers = {
        let cfg = cfg.clone();
        let peer_stop = Arc::clone(&peer_stop);
        let errors_out = Rc::clone(&errors_cell);
        let peers_out = Rc::clone(&peers_cell);
        let tmpdir = tmpdir.clone();
        PHASE.store(1, Ordering::SeqCst);
        ex.block_on(async move {
            let mut rings = Vec::new();
            for spec in &cfg.rings {
                match RecvBufRing::builder(spec.bgid)
                    .ring_entries(spec.ring_entries)
                    .buf_cnt(spec.buf_cnt)
                    .buf_len(spec.buf_len)
                    .build()
                {
                    Ok(r) => rings.push(r),
                    Err(e) => errors_out
                        .borrow_mut()
                        .push(format!("ring build {spec:?}: {e}")),
                }
            }
            if rings.is_empty() {
                return Vec::new();
            }
            let pool = cfg.fixed_pool.and_then(|(n, len)| {
                match Handle::current().register_fixed_buffers(vec![vec![0u8; len]; n]) {
                    Ok(p) => Some(Rc::new(p)),
                    Err(e) => {
                        errors_out
                            .borrow_mut()
                            .push(format!("fixed buffer registration: {e}"));
                        None
                    }
                }
            });
            let ctx = Rc::new(Ctx {
                seed,
                cfg: cfg.clone(),
                errors: RefCell::new(Vec::new()),
                stop: Cell::new(false),
                rings,
                pool,
                peer_stop,
                peers: RefCell::new(Vec::new()),
                tmpdir,
                verbose,
            });
            let mut rng = Rng::new(seed.wrapping_mul(31).wrapping_add(7));
            let mut handles = Vec::new();
            let mask = env_u64("STRESS_ACTOR_MASK").unwrap_or(u64::MAX);
            for (id, kind) in cfg.actors.iter().enumerate() {
                if mask & (1 << id) == 0 {
                    continue;
                }
                let arng = Rng::new(seed ^ ((id as u64 + 1) << 40) ^ 0x1234_5678);
                let c = Rc::clone(&ctx);
                let fut: Pin<Box<dyn Future<Output = ()>>> = match kind {
                    ActorKind::Nop => Box::pin(nop_actor(c, arng)),
                    ActorKind::Timeout => Box::pin(timeout_actor(c, arng, id)),
                    ActorKind::File => Box::pin(file_actor(c, arng, id)),
                    ActorKind::Pipe => Box::pin(pipe_actor(c, arng, id)),
                    ActorKind::TcpEcho => Box::pin(tcp_echo_actor(c, arng, id)),
                    ActorKind::TcpAccept => Box::pin(tcp_accept_actor(c, arng, id)),
                    ActorKind::Udp => Box::pin(udp_actor(c, arng, id)),
                };
                let kind = *kind;
                status(id, || format!("{kind:?} start"));
                let h = spawn(async move {
                    fut.await;
                    status(id, || format!("{kind:?} done"));
                });
                handles.push(h);
            }
            match cfg.shutdown {
                ShutdownMode::Abrupt => {
                    sleep(Duration::from_millis(rng.range(0, cfg.duration_ms))).await;
                }
                ShutdownMode::Graceful | ShutdownMode::AbortSome => {
                    sleep(Duration::from_millis(cfg.duration_ms)).await;
                    if cfg.shutdown == ShutdownMode::AbortSome {
                        for h in &handles {
                            if rng.chance(0.5) {
                                h.abort();
                            }
                        }
                    }
                    ctx.stop.set(true);
                    for h in handles.drain(..) {
                        let _ = h.await;
                    }
                    // Let dropped operations' cancellations settle.
                    for _ in 0..4 {
                        sleep(Duration::from_millis(2)).await;
                    }
                    if let Some(ms) = env_u64("STRESS_SETTLE_MS") {
                        sleep(Duration::from_millis(ms)).await;
                    }
                    if let Some(e) = Handle::current().health_error() {
                        ctx.fail(format!("driver health error: {e}"));
                    }
                    for ring in &ctx.rings {
                        probe_ring(&ctx, ring).await;
                    }
                    if let Some(pool) = &ctx.pool {
                        // Every slot must be free once all actors finished.
                        let mut held = Vec::new();
                        while let Ok(b) = pool.try_acquire() {
                            held.push(b);
                        }
                        if held.len() != pool.len() {
                            ctx.fail(format!(
                                "fixed buffer pool: {} of {} slots free after join",
                                held.len(),
                                pool.len()
                            ));
                        }
                    }
                }
            }
            // Resources that outlive the executor and driver: a held
            // provided buffer, its ring, sockets and a fixed-buffer pool.
            let mut leftovers: Vec<Box<dyn std::any::Any>> = Vec::new();
            // On by default (STRESS_NO_LEFTOVERS turns them off): they cover
            // descriptors dropped after the driver shut down.
            if !env_flag("STRESS_NO_LEFTOVERS") && rng.chance(0.5) {
                let ring = ctx.rings[0].clone();
                if let Ok(rx) = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await {
                    if let Ok(tx) = std::net::UdpSocket::bind("127.0.0.1:0") {
                        let _ = tx.send_to(b"leftover", rx.local_addr().unwrap());
                    }
                    if let Either::Left(Ok((buf, _))) =
                        race(rx.recv_from_ring(&ring), sleep(Duration::from_millis(50))).await
                    {
                        leftovers.push(Box::new(buf));
                    }
                    // A pending POLLPRI readiness poll that only teardown completes.
                    let pending = Box::pin(rx.poll_readiness(libc::POLLPRI as u32));
                    let mut pending = Some(pending);
                    std::future::poll_fn(|cx| {
                        let _ = pending.as_mut().unwrap().as_mut().poll(cx);
                        Poll::Ready(())
                    })
                    .await;
                    leftovers.push(Box::new(rx));
                    leftovers.push(Box::new(pending));
                }
                leftovers.push(Box::new(ring));
                if let Some(pool) = &ctx.pool {
                    leftovers.push(Box::new(Rc::clone(pool)));
                    if let Ok(fb) = pool.try_acquire() {
                        leftovers.push(Box::new(fb));
                    }
                }
                // Drop in a random order after the driver is gone.
                for i in (1..leftovers.len()).rev() {
                    let j = rng.usize(0, i + 1);
                    leftovers.swap(i, j);
                }
            }
            // Hand over errors and peer threads only now: in Abrupt mode the
            // actors keep running (and spawn peers) while the leftovers above
            // await, and a peer left in `ctx` would be detached with its
            // sockets still open, skewing the descriptor check.
            errors_out
                .borrow_mut()
                .extend(ctx.errors.borrow_mut().drain(..));
            peers_out
                .borrow_mut()
                .extend(ctx.peers.borrow_mut().drain(..));
            leftovers
            // `handles` (Abrupt) and `ctx` drop here or with the executor.
        })
    };
    PHASE.store(2, Ordering::SeqCst);
    drop(ex);
    drop(leftovers);
    PHASE.store(3, Ordering::SeqCst);
    peer_stop.store(true, Ordering::SeqCst);
    unpark_stop.store(true, Ordering::SeqCst);
    for p in peers_cell.borrow_mut().drain(..) {
        let _ = p.join();
    }
    if let Some(t) = unpark_thread {
        let _ = t.join();
    }
    let _ = std::fs::remove_dir_all(&tmpdir);

    let mut errors = std::mem::take(&mut *errors_cell.borrow_mut());
    let live_after = LIVE_BUFS.load(Ordering::SeqCst);
    if live_after != live_before {
        errors.push(format!(
            "tracked buffers leaked: {} live after teardown",
            live_after as isize - live_before as isize
        ));
    }
    let fds_after = fd_count();
    if fds_after != fds_before {
        errors.push(format!(
            "fd count {fds_before} -> {fds_after} after teardown: {:?}",
            fd_list()
        ));
    }
    for w in WARNINGS.lock().unwrap().iter() {
        // Runtime warnings that signal broken accounting rather than a
        // stressed-but-correct run (transient retries are not listed).
        const FATAL_WARNINGS: [&str; 8] = [
            "quarantin",
            "accounting",
            "invalid_user_data",
            "failed",
            "untrusted",
            "drain_barrier",
            "generation_mismatch",
            "state_mismatch",
        ];
        if FATAL_WARNINGS.iter().any(|term| w.contains(term)) {
            errors.push(format!("runtime warning: {w}"));
        }
    }
    PHASE.store(4, Ordering::SeqCst);
    CURRENT_SEED.store(u64::MAX, Ordering::SeqCst);
    let env = ENV_ISSUES.load(Ordering::SeqCst);
    if env > 0 {
        ENV_SEEDS.fetch_add(1, Ordering::SeqCst);
        eprintln!("STRESS ENV seed={seed} issues={env}");
        if !errors.is_empty() {
            errors.push(format!("note: {env} host resource errors in this run"));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

#[test]
#[ignore = "long-running randomized stress harness; see module docs"]
fn stress_lifecycle() {
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Warn);
    start_watchdog(env_u64("STRESS_HANG_SECS").unwrap_or(60));
    let verbose = env_flag("STRESS_VERBOSE");
    let seeds: Box<dyn Iterator<Item = u64>> = if let Some(seed) = env_u64("STRESS_SEED") {
        Box::new(std::iter::repeat_n(
            seed,
            env_u64("STRESS_REPEAT").unwrap_or(1) as usize,
        ))
    } else {
        let start = env_u64("STRESS_SEED_START").unwrap_or(0);
        let count = env_u64("STRESS_SEEDS").unwrap_or(if env_u64("STRESS_SECS").is_some() {
            u64::MAX - start
        } else {
            20
        });
        Box::new(start..start.saturating_add(count))
    };
    let time_limit = env_u64("STRESS_SECS").map(Duration::from_secs);
    let began = Instant::now();
    let mut failures = Vec::new();
    let mut ran = 0u64;
    for seed in seeds {
        if time_limit.is_some_and(|t| began.elapsed() > t) {
            break;
        }
        let t0 = Instant::now();
        eprintln!("STRESS BEGIN seed={seed}");
        let result = run_seed(seed, verbose);
        ran += 1;
        if verbose {
            eprintln!("seed {seed} done in {:?}", t0.elapsed());
        }
        if let Err(errors) = result {
            for (i, st) in STATUS.lock().unwrap().iter().enumerate() {
                eprintln!("  actor {i}: {st}");
            }
            eprintln!(
                "STRESS FAIL seed={seed} config={:?}\n  {}",
                Config::generate(seed),
                errors.join("\n  ")
            );
            failures.push(seed);
        }
    }
    let unsupported = UNSUPPORTED_CONFIGS.load(Ordering::SeqCst);
    let executed = ran.saturating_sub(unsupported as u64);
    eprintln!(
        "STRESS SUMMARY ran={ran} failed={} env_seeds={} unsupported_configs={unsupported} \
         (of ran, {unsupported} seeds skipped because the kernel refused their ring config; \
         they count as passes, so {executed} seeds actually ran) elapsed={:?} \
         failures={failures:?}",
        failures.len(),
        ENV_SEEDS.load(Ordering::SeqCst),
        began.elapsed()
    );
    assert!(failures.is_empty(), "failing seeds: {failures:?}");
}
