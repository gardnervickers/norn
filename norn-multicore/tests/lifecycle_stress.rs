//! Seeded, randomized lifecycle stress harness for full Norn runtime stacks.
//!
//! Every seed builds a `RuntimeGroup` whose workers each run
//! `LocalExecutor<norn_channel::Driver<norn_timer::Driver<norn_uring::Driver>>>`
//! and drives cross-worker channel traffic, timers, io_uring work, task
//! spawning and abortion, and group shutdown or join at random moments.
//!
//! Seeds also randomize SQPOLL rings (falling back to a regular ring when the
//! kernel refuses SQPOLL) and receive bundles (when the kernel supports them).
//!
//! The test is ignored by default because it is long-running. Run it with:
//!
//! ```console
//! STRESS_SECS=600 nix develop -c cargo test -p norn-multicore --test lifecycle_stress -- --ignored --nocapture
//! ```
//!
//! A failing seed is printed; `STRESS_SEED=<n>` replays the same plan, but
//! thread and kernel interleavings differ between runs, so loop a seed to
//! reproduce a race. The harness opens many loopback connections; on a host
//! with many `TIME_WAIT` sockets, run it in a private network namespace, for
//! example `unshare -rn bash -c 'ip link set lo up && exec "$@"' _ <command>`.
//! The summary prints how many SQPOLL rings fell back to regular rings.
//! Environment variables:
//!
//! - `STRESS_SEED=<n>`: run exactly one seed.
//! - `STRESS_SEED_START=<n>` and `STRESS_SECS=<s>`: run consecutive seeds
//!   from `n` for `s` seconds (default 30).
//! - `STRESS_SEED_TIMEOUT_SECS=<s>`: per-seed watchdog (default 60).
//! - `STRESS_STOP_ON_FAILURE=1`: stop at the first failing seed.
#![cfg(target_os = "linux")]

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::future::{poll_fn, Future};
use std::io;
use std::net::SocketAddr;
use std::pin::{pin, Pin};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc as std_mpsc, Arc, Mutex, OnceLock};
use std::task::{Context, Poll};
use std::thread;
use std::time::{Duration, Instant};

use futures_util::future::{select, Either};
use futures_util::StreamExt;
use norn_channel::mpsc::{
    self, DetachedReceiver, DetachedShardedReceiver, Receiver, Sender, ShardedReceiver,
    ShardedSender, TryRecvError, TrySendError,
};
use norn_executor::spawn;
use norn_multicore::{
    BoxError, JoinError, RuntimeGroup, StartError, WorkerContext, WorkerFailureKind, WorkerId,
};
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::{TcpListener, TcpSocket};

// ---------------------------------------------------------------------------
// Deterministic randomness
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03)
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
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.below(hi - lo + 1)
    }
    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
    fn fork(&mut self) -> Rng {
        Rng::new(self.next_u64())
    }
    fn ms(&mut self, lo: u64, hi: u64) -> Duration {
        Duration::from_millis(self.range(lo, hi))
    }
}

// ---------------------------------------------------------------------------
// Global accounting
// ---------------------------------------------------------------------------

static MSG_CREATED: AtomicUsize = AtomicUsize::new(0);
static MSG_DROPPED: AtomicUsize = AtomicUsize::new(0);
static TASK_CREATED: AtomicUsize = AtomicUsize::new(0);
static TASK_DROPPED: AtomicUsize = AtomicUsize::new(0);
static FAILURES: Mutex<Vec<String>> = Mutex::new(Vec::new());
static CURRENT_SEED: AtomicU64 = AtomicU64::new(u64::MAX);
static SEED_DEADLINE_MS: AtomicU64 = AtomicU64::new(0);
static TRACE: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn fail(msg: impl Into<String>) {
    let msg = msg.into();
    let name = thread::current().name().unwrap_or("?").to_owned();
    FAILURES
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(format!("[{name}] {msg}"));
}

fn trace(msg: impl Into<String>) {
    let mut t = TRACE.lock().unwrap_or_else(|e| e.into_inner());
    if t.len() < 400 {
        let name = thread::current().name().unwrap_or("?").to_owned();
        t.push(format!(
            "{:>8.3}ms [{name}] {}",
            epoch().elapsed().as_secs_f64() * 1000.0,
            msg.into()
        ));
    }
}

/// Per-channel accounting. Every message accepted with `Ok` must end up
/// either received or discarded by channel teardown.
#[derive(Debug, Default)]
struct ChanStats {
    name: String,
    sent_ok: AtomicUsize,
    received: AtomicUsize,
    discarded: AtomicUsize,
    explicitly_closed: AtomicBool,
    saw_closed: AtomicBool,
    planned: AtomicUsize,
}

impl ChanStats {
    fn new(name: String) -> Arc<Self> {
        Arc::new(Self {
            name,
            ..Default::default()
        })
    }
}

struct Msg {
    stats: Arc<ChanStats>,
    seq: u64,
    state: Cell<u8>,
}

const MSG_INFLIGHT: u8 = 0;
const MSG_RECEIVED: u8 = 1;
const MSG_REJECTED: u8 = 2;

impl Msg {
    fn new(stats: &Arc<ChanStats>, seq: u64) -> Self {
        MSG_CREATED.fetch_add(1, SeqCst);
        Msg {
            stats: Arc::clone(stats),
            seq,
            state: Cell::new(MSG_INFLIGHT),
        }
    }
    fn rejected(self) {
        self.state.set(MSG_REJECTED);
    }
    fn received(self) {
        self.state.set(MSG_RECEIVED);
        self.stats.received.fetch_add(1, SeqCst);
    }
}

impl Drop for Msg {
    fn drop(&mut self) {
        if self.state.get() == MSG_INFLIGHT {
            self.stats.discarded.fetch_add(1, SeqCst);
        }
        let _ = self.seq;
        MSG_DROPPED.fetch_add(1, SeqCst);
    }
}

/// Counts task futures so leaked tasks are visible after teardown.
struct Tracked<F> {
    inner: Pin<Box<F>>,
}

impl<F> Tracked<F> {
    fn new(f: F) -> Self {
        TASK_CREATED.fetch_add(1, SeqCst);
        Tracked { inner: Box::pin(f) }
    }
}

impl<F: Future> Future for Tracked<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        self.inner.as_mut().poll(cx)
    }
}

impl<F> Drop for Tracked<F> {
    fn drop(&mut self) {
        TASK_DROPPED.fetch_add(1, SeqCst);
    }
}

fn tspawn<F>(f: F) -> norn_task::JoinHandle<F::Output>
where
    F: Future + 'static,
{
    spawn(Tracked::new(f))
}

static LIVE_WORKERS: AtomicUsize = AtomicUsize::new(0);

struct LiveWorker;

impl Drop for LiveWorker {
    fn drop(&mut self) {
        LIVE_WORKERS.fetch_sub(1, SeqCst);
    }
}

thread_local! {
    /// Registered before any other harness thread-local on a worker thread,
    /// so its destructor runs after theirs (glibc runs them in reverse).
    static LIVE: RefCell<Option<LiveWorker>> = const { RefCell::new(None) };
}

fn register_live_worker() {
    LIVE_WORKERS.fetch_add(1, SeqCst);
    LIVE.with(|l| *l.borrow_mut() = Some(LiveWorker));
}

thread_local! {
    /// Resources deliberately dropped at thread exit, after the executor and
    /// its drivers have been torn down.
    static STASH: RefCell<Vec<Box<dyn Any>>> = const { RefCell::new(Vec::new()) };
}

fn stash<T: 'static>(value: T) {
    STASH.with(|s| s.borrow_mut().push(Box::new(value)));
}

// ---------------------------------------------------------------------------
// Small async helpers
// ---------------------------------------------------------------------------

async fn yield_now() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await
}

async fn sleep(d: Duration) {
    if let Err(e) = norn_timer::Handle::current().sleep(d).await {
        fail(format!("sleep({d:?}) failed while running: {e:?}"));
    }
}

async fn backoff(rng: &mut Rng) {
    match rng.below(4) {
        0 => yield_now().await,
        1 => drop(norn_uring::noop().await),
        _ => sleep(rng.ms(0, 2)).await,
    }
}

// ---------------------------------------------------------------------------
// Channel wrappers
// ---------------------------------------------------------------------------

enum DetachedInbox {
    Plain(DetachedReceiver<Msg>),
    Sharded(DetachedShardedReceiver<Msg>),
}

impl DetachedInbox {
    fn attach(self, handle: &norn_channel::Handle) -> Inbox {
        match self {
            DetachedInbox::Plain(r) => Inbox::Plain(r.attach(handle)),
            DetachedInbox::Sharded(r) => Inbox::Sharded(r.attach(handle)),
        }
    }
}

enum Inbox {
    Plain(Receiver<Msg>),
    Sharded(ShardedReceiver<Msg>),
}

impl Inbox {
    async fn recv(&mut self) -> Option<Msg> {
        match self {
            Inbox::Plain(r) => r.recv().await,
            Inbox::Sharded(r) => r.recv().await,
        }
    }
    async fn recv_many(&mut self, out: &mut Vec<Msg>, limit: usize) -> usize {
        match self {
            Inbox::Plain(r) => r.recv_many(out, limit).await,
            Inbox::Sharded(r) => r.recv_many(out, limit).await,
        }
    }
    fn try_recv(&mut self) -> Result<Msg, TryRecvError> {
        match self {
            Inbox::Plain(r) => r.try_recv(),
            Inbox::Sharded(r) => r.try_recv(),
        }
    }
    fn try_recv_many(&mut self, out: &mut Vec<Msg>, limit: usize) -> Result<usize, TryRecvError> {
        match self {
            Inbox::Plain(r) => r.try_recv_many(out, limit),
            Inbox::Sharded(r) => r.try_recv_many(out, limit),
        }
    }
    fn close(&mut self) {
        match self {
            Inbox::Plain(r) => r.close(),
            Inbox::Sharded(r) => r.close(),
        }
    }
}

enum Outbox {
    Plain(Sender<Msg>),
    Sharded(ShardedSender<Msg>),
}

/// Each variant carries how many messages were accepted before it.
enum SendOutcome {
    Sent(usize),
    Full(usize),
    Closed(#[allow(dead_code)] usize),
}

impl Outbox {
    /// Send up to `n` messages starting at `seq`.
    fn send(&self, stats: &Arc<ChanStats>, seq: u64, n: usize, many: bool) -> SendOutcome {
        match self {
            Outbox::Plain(tx) if many && n > 1 => {
                let mut values = (seq..seq + n as u64).map(|s| Msg::new(stats, s));
                match tx.try_send_many(&mut values) {
                    Ok(sent) => {
                        stats.sent_ok.fetch_add(sent, SeqCst);
                        SendOutcome::Sent(sent)
                    }
                    Err(e) => {
                        let sent = e.sent();
                        stats.sent_ok.fetch_add(sent, SeqCst);
                        match e.into_error() {
                            TrySendError::Full(m) => {
                                m.rejected();
                                SendOutcome::Full(sent)
                            }
                            TrySendError::Closed(m) => {
                                m.rejected();
                                SendOutcome::Closed(sent)
                            }
                        }
                    }
                }
            }
            _ => {
                let mut sent = 0;
                for s in seq..seq + n as u64 {
                    let msg = Msg::new(stats, s);
                    let res = match self {
                        Outbox::Plain(tx) => tx.try_send(msg),
                        Outbox::Sharded(tx) => tx.try_send(msg),
                    };
                    match res {
                        Ok(()) => {
                            stats.sent_ok.fetch_add(1, SeqCst);
                            sent += 1;
                        }
                        Err(TrySendError::Full(m)) => {
                            m.rejected();
                            return SendOutcome::Full(sent);
                        }
                        Err(TrySendError::Closed(m)) => {
                            m.rejected();
                            return SendOutcome::Closed(sent);
                        }
                    }
                }
                SendOutcome::Sent(sent)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Scenario plan
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Chaos {
    ReturnErr,
    Panic,
    ReturnOk,
    RequestShutdown,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum InboxFate {
    /// Receive until the channel reports closure.
    Drain,
    /// Call `close()` after this many messages, then drain.
    CloseAfter(usize),
    /// Drop the receiver after this many messages.
    DropAfter(usize),
    /// Never attach; drop the detached receiver at main start.
    NeverAttach,
    /// Attach, then stash the receiver in thread-local storage.
    StashAfter(usize),
}

#[derive(Clone, Copy, Debug)]
enum GroupAction {
    Shutdown,
    Drop,
    Join,
}

#[derive(Clone, Copy, Debug)]
enum Outside {
    NaturalJoin,
    ShutdownAfter(Duration),
    DropAfter(Duration),
    RequestStorm(Duration),
    InWorker {
        worker: usize,
        action: GroupAction,
        delay: Duration,
    },
}

#[derive(Clone, Copy, Debug)]
struct RingCfg {
    size: u32,
    sqpoll: bool,
    coop: bool,
    single_issuer: bool,
    budget: Option<usize>,
}

struct OutboxPlan {
    dst: usize,
    tx: Outbox,
    stats: Arc<ChanStats>,
    quota: usize,
    abort_after: Option<Duration>,
}

struct WorkerSetup {
    id: usize,
    rng: Rng,
    strict: bool,
    linger_until_shutdown: bool,
    inbox: Option<(DetachedInbox, Arc<ChanStats>, InboxFate)>,
    ping: Option<(DetachedReceiver<Msg>, Arc<ChanStats>)>,
    outboxes: Vec<OutboxPlan>,
    tcp_listen: Vec<Sender<SocketAddr>>,
    tcp_connect: Vec<DetachedReceiver<SocketAddr>>,
    tcp_rounds: usize,
    tcp_done: Arc<AtomicUsize>,
    group_rx: Option<(DetachedReceiver<RuntimeGroup>, GroupAction, Duration)>,
    group_report: std_mpsc::Sender<String>,
    chaos: Option<(Duration, Chaos)>,
    timers: usize,
    nops: usize,
    aborts: usize,
    panicking_tasks: usize,
    lingering: bool,
    stash_misc: bool,
    aux_stack: Option<u8>,
    ring: RingCfg,
}

fn intentional_failure(kind: WorkerFailureKind, text: &str) -> bool {
    text.contains("intentional")
        && matches!(
            kind,
            WorkerFailureKind::Application | WorkerFailureKind::Panic | WorkerFailureKind::Startup
        )
}

fn check_join_error(where_: &str, err: &JoinError) {
    for f in err.failures() {
        let text = f.to_string();
        if !intentional_failure(f.kind(), &text) {
            fail(format!("{where_}: unexpected worker failure {f:?}"));
        }
    }
}

// ---------------------------------------------------------------------------
// Park stack
// ---------------------------------------------------------------------------

type Stack = norn_timer::Driver<norn_uring::Driver>;

fn build_park(cfg: RingCfg) -> io::Result<Stack> {
    let build = |cfg: RingCfg| -> io::Result<Stack> {
        let mut builder = io_uring::IoUring::builder();
        if cfg.coop && !cfg.sqpoll {
            builder.setup_coop_taskrun();
        }
        if cfg.single_issuer {
            builder.setup_single_issuer();
        }
        if cfg.sqpoll {
            builder.setup_sqpoll(10);
        }
        let options = norn_uring::DriverOptions {
            completion_drain_budget: cfg.budget.and_then(std::num::NonZeroUsize::new),
        };
        let uring = norn_uring::Driver::new_with_options(builder, cfg.size, options)?;
        Ok(norn_timer::Driver::new(uring, norn_timer::Clock::system()))
    };
    match build(cfg) {
        // SQPOLL can be refused without the needed privileges; run that worker
        // on a regular ring instead of failing the seed, and count it so a
        // host that never gets SQPOLL shows up in the summary. Any other error
        // is a real failure.
        Err(e)
            if cfg.sqpoll && matches!(e.raw_os_error(), Some(libc::EPERM) | Some(libc::EACCES)) =>
        {
            SQPOLL_FALLBACKS.fetch_add(1, SeqCst);
            build(RingCfg {
                sqpoll: false,
                ..cfg
            })
        }
        result => result,
    }
}

/// Rings that asked for SQPOLL but fell back to a regular ring.
static SQPOLL_FALLBACKS: AtomicUsize = AtomicUsize::new(0);

/// Whether the kernel supports receive bundles, probed once.
fn bundles_supported() -> bool {
    static SUPPORTED: OnceLock<bool> = OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        let supported = match io_uring::IoUring::new(2) {
            Ok(ring) => ring.params().is_feature_recvsend_bundle(),
            Err(e) => {
                eprintln!("STRESS bundle probe failed ({e}); bundle coverage is off");
                false
            }
        };
        eprintln!("STRESS bundles supported: {supported}");
        supported
    })
}

// ---------------------------------------------------------------------------
// Worker behaviour
// ---------------------------------------------------------------------------

async fn producer(plan: OutboxPlan, mut rng: Rng, strict: bool, me: usize) {
    let OutboxPlan {
        dst,
        tx,
        stats,
        quota,
        ..
    } = plan;
    let mut seq = 0u64;
    let mut full_retries = 0;
    while (seq as usize) < quota {
        let remaining = quota - seq as usize;
        let n = (rng.range(1, 8) as usize).min(remaining);
        let many = rng.chance(50);
        match tx.send(&stats, seq, n, many) {
            SendOutcome::Sent(k) => {
                seq += k as u64;
                full_retries = 0;
            }
            SendOutcome::Full(k) => {
                seq += k as u64;
                full_retries += 1;
                if !strict && full_retries > 200 {
                    // Abandon the stream so a stalled peer cannot wedge us.
                    return;
                }
                backoff(&mut rng).await;
            }
            SendOutcome::Closed(_) => {
                if strict {
                    fail(format!("{me}->{dst}: channel closed in strict mode"));
                }
                return;
            }
        }
        if rng.chance(30) {
            backoff(&mut rng).await;
        }
    }
}

async fn consumer(mut inbox: Inbox, stats: Arc<ChanStats>, fate: InboxFate, mut rng: Rng) {
    let mut received = 0usize;
    let mut buf = Vec::new();
    loop {
        match fate {
            InboxFate::CloseAfter(n) if received >= n => {
                if !stats.explicitly_closed.swap(true, SeqCst) {
                    inbox.close();
                }
            }
            InboxFate::DropAfter(n) if received >= n => {
                drop(inbox);
                return;
            }
            InboxFate::StashAfter(n) if received >= n => {
                stats.explicitly_closed.store(true, SeqCst);
                stash(inbox);
                return;
            }
            _ => {}
        }
        let got: Option<usize> = match rng.below(4) {
            0 => match inbox.recv().await {
                Some(m) => {
                    m.received();
                    Some(1)
                }
                None => None,
            },
            1 => {
                let limit = rng.range(1, 16) as usize;
                let n = inbox.recv_many(&mut buf, limit).await;
                if n > limit {
                    fail(format!("{}: recv_many returned {n} > {limit}", stats.name));
                }
                for m in buf.drain(..) {
                    m.received();
                }
                if n == 0 {
                    None
                } else {
                    Some(n)
                }
            }
            2 => match inbox.try_recv() {
                Ok(m) => {
                    m.received();
                    Some(1)
                }
                Err(TryRecvError::Empty) => {
                    backoff(&mut rng).await;
                    Some(0)
                }
                Err(TryRecvError::Closed) => None,
            },
            _ => {
                let limit = rng.range(1, 16) as usize;
                match inbox.try_recv_many(&mut buf, limit) {
                    Ok(n) => {
                        for m in buf.drain(..) {
                            m.received();
                        }
                        Some(n)
                    }
                    Err(TryRecvError::Empty) => {
                        backoff(&mut rng).await;
                        Some(0)
                    }
                    Err(TryRecvError::Closed) => None,
                }
            }
        };
        match got {
            Some(n) => received += n,
            None => {
                stats.saw_closed.store(true, SeqCst);
                if !stats.explicitly_closed.load(SeqCst) {
                    // Every sender has disconnected, so every successful send
                    // happened before this observation.
                    let sent = stats.sent_ok.load(SeqCst);
                    let got = stats.received.load(SeqCst);
                    if sent != got {
                        fail(format!(
                            "{}: Closed observed after {got} receptions but {sent} sends succeeded",
                            stats.name
                        ));
                    }
                }
                // Closed must be sticky.
                if let Ok(m) = inbox.try_recv() {
                    if !stats.explicitly_closed.load(SeqCst) {
                        fail(format!("{}: message after Closed", stats.name));
                    }
                    m.received();
                }
                if rng.chance(30) {
                    stash(inbox);
                }
                return;
            }
        }
    }
}

async fn timer_task(mut rng: Rng, rounds: usize) {
    let handle = norn_timer::Handle::current();
    for _ in 0..rounds {
        let d = rng.ms(0, 12);
        let kind = rng.below(6);
        match kind {
            0 | 1 => {
                let t0 = Instant::now();
                let r = handle.sleep(d).await;
                if r.is_err() {
                    fail(format!("sleep returned {r:?} while running"));
                }
                if t0.elapsed() < d {
                    fail(format!("sleep({d:?}) fired early after {:?}", t0.elapsed()));
                }
            }
            2 => {
                // Cancel a long sleep before it fires.
                let long = handle.sleep(d + Duration::from_millis(rng.range(5, 1000)));
                let short = handle.sleep(rng.ms(0, 3));
                match select(pin!(long), pin!(short)).await {
                    Either::Left((r, _)) => {
                        if r.is_err() {
                            fail(format!("long sleep returned {r:?}"));
                        }
                    }
                    Either::Right((r, _)) => {
                        if r.is_err() {
                            fail(format!("short sleep returned {r:?}"));
                        }
                    }
                }
            }
            3 => {
                // Register, then reset.
                let mut s = pin!(handle.sleep(d));
                let short = handle.sleep(rng.ms(0, 2));
                let fired_first = match select(s.as_mut(), pin!(short)).await {
                    Either::Left(_) => true,
                    Either::Right(_) => false,
                };
                let t1 = Instant::now();
                s.as_mut().reset();
                let r = s.await;
                if r.is_err() {
                    fail(format!("reset sleep returned {r:?}"));
                }
                if t1.elapsed() < d {
                    fail(format!(
                        "reset sleep({d:?}) fired early after {:?} (fired_first={fired_first})",
                        t1.elapsed()
                    ));
                }
            }
            4 => {
                let r = handle
                    .sleep_until(Instant::now() - Duration::from_millis(5))
                    .await;
                if r.is_err() {
                    fail(format!("past sleep_until returned {r:?}"));
                }
            }
            _ => {
                let n = rng.range(1, 20) as usize;
                let mut sleeps = Vec::new();
                for _ in 0..n {
                    sleeps.push(handle.sleep(rng.ms(0, 6)));
                }
                // Drop half of them early.
                let keep = rng.below(n as u64 + 1) as usize;
                sleeps.truncate(keep);
                for r in futures_util::future::join_all(sleeps).await {
                    if r.is_err() {
                        fail(format!("joined sleep returned {r:?}"));
                    }
                }
            }
        }
    }
}

async fn nop_task(mut rng: Rng, rounds: usize, ring_size: u32) {
    for _ in 0..rounds {
        let k = rng.range(1, (ring_size / 2).max(1) as u64) as usize;
        let ops: Vec<_> = (0..k).map(|_| norn_uring::noop()).collect();
        if rng.chance(20) {
            // Drop some submitted operations early.
            let mut ops = ops;
            let mut fut = pin!(futures_util::future::join_all(ops.drain(..)));
            let _ = select(fut.as_mut(), pin!(yield_now())).await;
        } else {
            futures_util::future::join_all(ops).await;
        }
        if rng.chance(40) {
            yield_now().await;
        }
    }
}

async fn send_all(sock: &TcpSocket, mut data: Vec<u8>) -> io::Result<()> {
    while !data.is_empty() {
        let (res, mut back) = sock.send(data).await;
        let n = res?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "send returned 0"));
        }
        back.drain(..n);
        data = back;
    }
    Ok(())
}

async fn echo_conn(sock: TcpSocket, ring: RecvBufRing, mut rng: Rng) -> io::Result<()> {
    if bundles_supported() && rng.chance(40) {
        let mut stream = Box::pin(sock.recv_bundle_multi(&ring));
        while let Some(item) = stream.next().await {
            let bundle = item?;
            let copy: Vec<u8> = bundle.iter().flatten().copied().collect();
            drop(bundle);
            send_all(&sock, copy).await?;
        }
        drop(stream);
        let _ = sock.close().await;
        return Ok(());
    }
    let mut stream = Box::pin(sock.recv_ring_multi(&ring));
    loop {
        let Some(item) = stream.next().await else {
            break;
        };
        let buf = item?;
        if rng.chance(50) {
            let len = buf.len();
            let (res, buf) = sock.send(buf).await;
            let n = res?;
            if n < len {
                let rest = buf[n..].to_vec();
                drop(buf);
                send_all(&sock, rest).await?;
            }
        } else {
            let copy = buf.to_vec();
            drop(buf);
            send_all(&sock, copy).await?;
        }
    }
    drop(stream);
    let _ = sock.close().await;
    Ok(())
}

async fn tcp_listener(
    addr_txs: Vec<Sender<SocketAddr>>,
    rounds_done: Arc<AtomicUsize>,
    mut rng: Rng,
    strict: bool,
) {
    let _ = rounds_done;
    let listener = match TcpListener::bind("127.0.0.1:0".parse().unwrap(), 64).await {
        Ok(l) => l,
        Err(e) => return fail(format!("bind failed: {e}")),
    };
    let addr = listener.local_addr().unwrap();
    trace(format!("echo listener {addr}"));
    let expected = addr_txs.len();
    for tx in addr_txs {
        if tx.try_send(addr).is_err() && strict {
            fail("addr send failed in strict mode");
        }
    }
    let ring = match RecvBufRing::builder(1)
        .buf_cnt(rng.range(1, 8) as u16)
        .buf_len(rng.range(64, 4096) as usize)
        .build()
    {
        Ok(r) => r,
        Err(e) => return fail(format!("bufring build failed: {e}")),
    };
    let mut echoes = Vec::new();
    for _ in 0..expected {
        match listener.accept().await {
            Ok((sock, peer)) => {
                trace(format!("echo accepted {peer}"));
                let ring = ring.clone();
                let r = rng.fork();
                echoes.push(tspawn(echo_conn(sock, ring, r)));
            }
            Err(e) => {
                if strict {
                    fail(format!("accept failed: {e}"));
                }
                break;
            }
        }
    }
    let _ = listener.close().await;
    for e in echoes {
        match e.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                if strict {
                    fail(format!("echo failed: {err}"));
                }
            }
            Err(te) => {
                if strict || te.is_panic() {
                    fail(format!("echo task failed: {te:?}"));
                }
            }
        }
    }
}

async fn tcp_connector(
    addr_rx: Receiver<SocketAddr>,
    rounds: usize,
    rounds_done: Arc<AtomicUsize>,
    mut rng: Rng,
    strict: bool,
) {
    let mut addr_rx = addr_rx;
    let Some(addr) = addr_rx.recv().await else {
        if strict {
            fail("listener never published an address");
        }
        return;
    };
    drop(addr_rx);
    let sock = match TcpSocket::connect(addr).await {
        Ok(s) => {
            trace(format!("echo client {:?} -> {addr}", s.local_addr()));
            s
        }
        Err(e) => {
            if strict {
                fail(format!("connect failed: {e}"));
            }
            return;
        }
    };
    for _ in 0..rounds {
        let len = rng.range(1, 8192) as usize;
        let start = rng.next_u64() as u8;
        let data: Vec<u8> = (0..len).map(|i| start.wrapping_add(i as u8)).collect();
        if let Err(e) = send_all(&sock, data.clone()).await {
            if strict {
                fail(format!("connector send failed: {e}"));
            }
            return;
        }
        let mut got = Vec::with_capacity(len);
        while got.len() < len {
            let (res, buf) = sock.recv(Vec::with_capacity(len - got.len())).await;
            match res {
                Ok(0) => {
                    if strict {
                        fail(format!("unexpected EOF after {}/{len} bytes", got.len()));
                    }
                    return;
                }
                Ok(_) => got.extend_from_slice(&buf),
                Err(e) => {
                    if strict {
                        fail(format!("connector recv failed: {e}"));
                    }
                    return;
                }
            }
        }
        if got != data {
            fail(format!("echo mismatch for {len} bytes"));
            return;
        }
        rounds_done.fetch_add(1, SeqCst);
    }
    let _ = sock.close().await;
}

/// Tasks deliberately left with kernel-owned operations in flight at shutdown.
async fn lingering(mut rng: Rng) {
    let listener = match TcpListener::bind("127.0.0.1:0".parse().unwrap(), 4).await {
        Ok(l) => l,
        Err(e) => return fail(format!("linger bind: {e}")),
    };
    let addr = listener.local_addr().unwrap();
    trace(format!("linger listener {addr}"));
    let client = tspawn(async move { TcpSocket::connect(addr).await });
    let (server, _) = match listener.accept().await {
        Ok(p) => p,
        Err(e) => return fail(format!("linger accept: {e}")),
    };
    let client = match client.await {
        Ok(Ok(c)) => c,
        other => return fail(format!("linger connect: {:?}", other.map(|r| r.err()))),
    };
    trace(format!("linger client {:?}", client.local_addr()));
    // Pending single-shot receive.
    let server = Rc::new(server);
    let s2 = Rc::clone(&server);
    tspawn(async move {
        let (_res, _buf) = s2.recv(Vec::with_capacity(64)).await;
    })
    .detach();
    // Pending accept.
    tspawn(async move {
        let _ = listener.accept().await;
    })
    .detach();
    // Pending multishot receive on a second buffer ring.
    if let Ok(ring) = RecvBufRing::builder(2).buf_cnt(2).buf_len(256).build() {
        let c = Rc::new(client);
        let c2 = Rc::clone(&c);
        let stash_ring = rng.chance(50);
        if stash_ring {
            stash(ring.clone());
        }
        tspawn(async move {
            let mut s = Box::pin(c2.recv_ring_multi(&ring));
            let _ = s.next().await;
        })
        .detach();
        if rng.chance(50) {
            stash(c);
        }
    }
    // A long sleep.
    tspawn(async move {
        let _ = norn_timer::Handle::current()
            .sleep(Duration::from_secs(3600))
            .await;
    })
    .detach();
    if rng.chance(50) {
        stash(server);
    }
}

fn aux_stack(mode: u8) {
    // Build a second complete stack inside a running worker and drop it with
    // queued tasks and buffered channel traffic.
    let builder = norn_channel::DriverBuilder::new();
    let (tx, rx) = mpsc::bounded::<u64>(builder.endpoint(), 4);
    let park = match build_park(RingCfg {
        size: 8,
        sqpoll: false,
        coop: false,
        single_issuer: false,
        budget: Some(32),
    }) {
        Ok(p) => p,
        Err(e) => return fail(format!("aux park: {e}")),
    };
    let driver = builder.build(park);
    let rx = rx.attach(&driver.handle());
    let executor = norn_executor::LocalExecutor::new(driver);
    let _ = tx.try_send(1);
    let h = executor.handle().spawn(Tracked::new(async move {
        let mut rx = rx;
        let _ = rx.recv().await;
    }));
    h.detach();
    match mode {
        0 => drop(executor),
        1 => {
            stash(executor);
            stash(tx);
        }
        _ => {
            thread::spawn(move || drop(tx)).join().unwrap();
            drop(executor);
        }
    }
}

async fn spawn_abort(mut rng: Rng, count: usize, panics: usize) {
    let mut handles = Vec::new();
    for i in 0..count {
        let mut r = rng.fork();
        let h = tspawn(async move {
            for _ in 0..r.range(1, 50) {
                match r.below(3) {
                    0 => drop(norn_uring::noop().await),
                    1 => sleep(r.ms(0, 3)).await,
                    _ => yield_now().await,
                }
            }
            i
        });
        handles.push(h);
    }
    for _ in 0..panics {
        let mut r = rng.fork();
        let h = tspawn(async move {
            sleep(r.ms(0, 3)).await;
            panic!("intentional task panic");
        });
        match h.await {
            Err(e) if e.is_panic() => {}
            other => fail(format!("panicking task returned {:?}", other.err())),
        }
    }
    for h in &handles {
        if rng.chance(50) {
            sleep(rng.ms(0, 2)).await;
            h.abort();
        }
    }
    for h in handles {
        match h.await {
            Ok(_) => {}
            Err(e) if e.is_cancelled() => {}
            Err(e) => fail(format!("aborted task returned unexpected error {e:?}")),
        }
    }
}

async fn ping_consumer(mut rx: Receiver<Msg>, stats: Arc<ChanStats>) {
    while let Some(m) = rx.recv().await {
        m.received();
    }
    stats.saw_closed.store(true, SeqCst);
    let sent = stats.sent_ok.load(SeqCst);
    let got = stats.received.load(SeqCst);
    if sent != got {
        fail(format!(
            "{}: Closed observed after {got} receptions but {sent} sends succeeded",
            stats.name
        ));
    }
}

async fn work(ctx: WorkerContext, s: &mut WorkerSetup) {
    let me = s.id;
    let strict = s.strict;
    let mut rng = s.rng.fork();
    let mut handles = Vec::new();

    if let Some((inbox, stats, fate)) = s.inbox.take() {
        if fate == InboxFate::NeverAttach {
            stats.explicitly_closed.store(true, SeqCst);
            drop(inbox);
        } else {
            let inbox = inbox.attach(ctx.channels());
            handles.push(tspawn(consumer(inbox, stats, fate, rng.fork())));
        }
    }
    if let Some((ping, stats)) = s.ping.take() {
        let ping = ping.attach(ctx.channels());
        handles.push(tspawn(ping_consumer(ping, stats)));
    }
    let mut abortable = Vec::new();
    for plan in s.outboxes.drain(..) {
        let abort_after = plan.abort_after;
        let h = tspawn(producer(plan, rng.fork(), strict, me));
        match abort_after {
            Some(d) => abortable.push((h, d)),
            None => handles.push(h),
        }
    }
    if !s.tcp_listen.is_empty() {
        let txs = std::mem::take(&mut s.tcp_listen);
        handles.push(tspawn(tcp_listener(
            txs,
            Arc::clone(&s.tcp_done),
            rng.fork(),
            strict,
        )));
    }
    for rx in s.tcp_connect.drain(..) {
        let rx = rx.attach(ctx.channels());
        handles.push(tspawn(tcp_connector(
            rx,
            s.tcp_rounds,
            Arc::clone(&s.tcp_done),
            rng.fork(),
            strict,
        )));
    }
    for _ in 0..s.timers {
        handles.push(tspawn(timer_task(rng.fork(), rng.range(1, 20) as usize)));
    }
    for _ in 0..s.nops {
        handles.push(tspawn(nop_task(
            rng.fork(),
            rng.range(1, 20) as usize,
            s.ring.size,
        )));
    }
    if s.aborts > 0 || s.panicking_tasks > 0 {
        handles.push(tspawn(spawn_abort(rng.fork(), s.aborts, s.panicking_tasks)));
    }
    if let Some(mode) = s.aux_stack {
        aux_stack(mode);
    }
    if s.lingering {
        lingering(rng.fork()).await;
    }
    if s.stash_misc {
        let mut sl = Box::pin(norn_timer::Handle::current().sleep(Duration::from_secs(60)));
        let _ = poll_fn(|cx| Poll::Ready(sl.as_mut().poll(cx))).await;
        stash(sl);
        let mut sr = Box::pin(ctx.shutdown_requested());
        let _ = poll_fn(|cx| Poll::Ready(sr.as_mut().poll(cx))).await;
        stash(sr);
        stash(ctx.clone());
        stash(norn_uring::Handle::current());
    }
    let mut deferred_group = None;
    if let Some((rx, action, delay)) = s.group_rx.take() {
        let rx = rx.attach(ctx.channels());
        let report = s.group_report.clone();
        if matches!(action, GroupAction::Join) {
            // A worker may join the group only after its own work is done;
            // otherwise it blocks peers that wait on it (a documented hazard).
            deferred_group = Some(group_task(rx, action, delay, report, me));
        } else {
            handles.push(tspawn(group_task(rx, action, delay, report, me)));
        }
    }

    // Abort producers whose plan says so, after their delay.
    for (h, d) in abortable {
        sleep(d).await;
        h.abort();
        match h.await {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(e) => fail(format!("aborted producer: {e:?}")),
        }
    }
    for h in handles {
        if let Err(e) = h.await {
            if e.is_panic() {
                let msg = e.into_panic().map(|p| panic_text(&*p)).unwrap_or_default();
                if !msg.contains("intentional") {
                    fail(format!("worker {me} task panicked: {msg}"));
                }
            } else {
                fail(format!("worker {me} task failed: {e:?}"));
            }
        }
    }
    if let Some(g) = deferred_group {
        g.await;
    }
}

async fn group_task(
    mut rx: Receiver<RuntimeGroup>,
    action: GroupAction,
    delay: Duration,
    report: std_mpsc::Sender<String>,
    me: usize,
) {
    let Some(group) = rx.recv().await else {
        return;
    };
    sleep(delay).await;
    trace(format!("worker {me} runs {action:?} on the group"));
    let started = Instant::now();
    let result = match action {
        GroupAction::Shutdown => group.shutdown(),
        GroupAction::Join => group.join(),
        GroupAction::Drop => {
            drop(group);
            Ok(())
        }
    };
    let elapsed = started.elapsed();
    if let Err(e) = &result {
        check_join_error("in-worker group action", e);
    }
    let _ = report.send(format!("{action:?} from worker {me} took {elapsed:?}"));
}

fn panic_text(p: &(dyn Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic>".to_owned()
    }
}

async fn worker_main(ctx: WorkerContext, mut s: WorkerSetup) -> Result<(), BoxError> {
    let me = s.id;
    trace(format!("worker {me} main starts ring={:?}", s.ring));
    let chaos = s.chaos;
    let linger = s.linger_until_shutdown;
    let mut rng = s.rng.fork();
    let shutdown = ctx.shutdown_requested();
    let chaos_fut = async move {
        match chaos {
            Some((d, c)) => {
                sleep(d).await;
                c
            }
            None => std::future::pending().await,
        }
    };
    let ctx2 = ctx.clone();
    let outcome = {
        let work = work(ctx2, &mut s);
        match select(pin!(work), select(pin!(shutdown), pin!(chaos_fut))).await {
            Either::Left(((), _)) => "work",
            Either::Right((Either::Left(((), _)), _)) => "shutdown",
            Either::Right((Either::Right((c, _)), _)) => {
                trace(format!("worker {me} chaos {c:?}"));
                match c {
                    Chaos::ReturnErr => return Err("intentional worker error".into()),
                    Chaos::Panic => panic!("intentional worker panic"),
                    Chaos::ReturnOk => return Ok(()),
                    Chaos::RequestShutdown => {
                        ctx.request_shutdown();
                        "chaos-shutdown"
                    }
                }
            }
        }
    };
    trace(format!("worker {me} outcome {outcome}"));
    if outcome != "shutdown" && linger {
        ctx.shutdown_requested().await;
    }
    // Traffic after shutdown was requested.
    if rng.chance(50) {
        for _ in 0..rng.range(1, 5) {
            drop(norn_uring::noop().await);
            sleep(rng.ms(0, 2)).await;
        }
    }
    trace(format!("worker {me} main returns"));
    Ok(())
}

// ---------------------------------------------------------------------------
// Process inspection
// ---------------------------------------------------------------------------

fn count_fds() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

fn threads() -> Vec<(String, String)> {
    let mut out = Vec::new();
    if let Ok(dir) = std::fs::read_dir("/proc/self/task") {
        for e in dir.flatten() {
            let tid = e.file_name().to_string_lossy().into_owned();
            let comm = std::fs::read_to_string(e.path().join("comm"))
                .unwrap_or_default()
                .trim()
                .to_owned();
            out.push((tid, comm));
        }
    }
    out
}

fn count_user_threads() -> usize {
    threads()
        .into_iter()
        .filter(|(_, comm)| !comm.starts_with("iou-"))
        .count()
}

fn dump_threads() -> String {
    let mut s = String::new();
    for (tid, comm) in threads() {
        let base = format!("/proc/self/task/{tid}");
        let wchan = std::fs::read_to_string(format!("{base}/wchan")).unwrap_or_default();
        let syscall = std::fs::read_to_string(format!("{base}/syscall")).unwrap_or_default();
        s.push_str(&format!(
            "  {tid} {comm:<20} wchan={wchan} syscall={}\n",
            syscall.trim()
        ));
    }
    s
}

fn fd_listing() -> String {
    let mut s = String::new();
    if let Ok(dir) = std::fs::read_dir("/proc/self/fd") {
        for e in dir.flatten() {
            let target = std::fs::read_link(e.path())
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            let mut extra = String::new();
            if let Some(ino) = target
                .strip_prefix("socket:[")
                .and_then(|t| t.strip_suffix(']'))
            {
                for table in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
                    for line in std::fs::read_to_string(table).unwrap_or_default().lines() {
                        let cols: Vec<&str> = line.split_whitespace().collect();
                        if cols.len() > 9 && cols[9] == ino {
                            extra =
                                format!(" local={} remote={} state={}", cols[1], cols[2], cols[3]);
                        }
                    }
                }
            }
            s.push_str(&format!(
                "  {} -> {target}{extra}\n",
                e.file_name().to_string_lossy()
            ));
        }
    }
    s
}

fn fd_snapshot() -> std::collections::BTreeMap<String, String> {
    let mut m = std::collections::BTreeMap::new();
    if let Ok(dir) = std::fs::read_dir("/proc/self/fd") {
        for e in dir.flatten() {
            let target = std::fs::read_link(e.path())
                .map(|p| p.display().to_string())
                .unwrap_or_default();
            m.insert(e.file_name().to_string_lossy().into_owned(), target);
        }
    }
    m
}

fn wait_until(limit: Duration, mut f: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    loop {
        if f() {
            return true;
        }
        if start.elapsed() > limit {
            return false;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

fn start_watchdog() {
    static STARTED: OnceLock<()> = OnceLock::new();
    STARTED.get_or_init(|| {
        thread::Builder::new()
            .name("stress-watchdog".into())
            .spawn(|| loop {
                thread::sleep(Duration::from_millis(200));
                let deadline = SEED_DEADLINE_MS.load(SeqCst);
                if deadline != 0 && epoch().elapsed().as_millis() as u64 > deadline {
                    let seed = CURRENT_SEED.load(SeqCst);
                    eprintln!("WATCHDOG: seed {seed} exceeded its deadline; threads:");
                    eprintln!("{}", dump_threads());
                    for line in TRACE.lock().unwrap_or_else(|e| e.into_inner()).iter() {
                        eprintln!("  trace {line}");
                    }
                    for f in FAILURES.lock().unwrap_or_else(|e| e.into_inner()).iter() {
                        eprintln!("  failure so far: {f}");
                    }
                    eprintln!("STRESS-HANG seed={seed}");
                    std::process::abort();
                }
            })
            .unwrap();
    });
}

fn install_panic_hook() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        std::panic::set_hook(Box::new(|info| {
            let msg = panic_text(info.payload());
            if msg.contains("intentional") {
                return;
            }
            let loc = info
                .location()
                .map(|l| format!("{}:{}", l.file(), l.line()))
                .unwrap_or_default();
            let bt = std::backtrace::Backtrace::force_capture();
            eprintln!(
                "UNEXPECTED PANIC on {:?}: {msg} at {loc}\n{bt}",
                thread::current().name()
            );
            fail(format!("unexpected panic: {msg} at {loc}"));
        }));
    });
}

// ---------------------------------------------------------------------------
// One seed
// ---------------------------------------------------------------------------

struct ExtProducer {
    tx: Outbox,
    stats: Arc<ChanStats>,
    quota: usize,
    rng: Rng,
    hammer: bool,
}

fn run_ext_producer(p: ExtProducer, strict: bool, hold: bool, stop: Arc<AtomicBool>) {
    let ExtProducer {
        tx,
        stats,
        quota,
        mut rng,
        hammer,
    } = p;
    let mut seq = 0u64;
    let mut full = 0u32;
    while (seq as usize) < quota {
        if !strict && stop.load(SeqCst) {
            break;
        }
        let n = if hammer {
            1
        } else {
            (rng.range(1, 8) as usize).min(quota - seq as usize)
        };
        match tx.send(&stats, seq, n, rng.chance(50)) {
            SendOutcome::Sent(k) => {
                seq += k as u64;
                full = 0;
            }
            SendOutcome::Full(k) => {
                seq += k as u64;
                full += 1;
                if !strict && full > 2000 {
                    break;
                }
                if hammer {
                    std::hint::spin_loop();
                } else {
                    thread::sleep(Duration::from_micros(rng.range(0, 200)));
                }
            }
            SendOutcome::Closed(_) => {
                if strict {
                    fail(format!(
                        "{}: external sender saw Closed in strict mode",
                        stats.name
                    ));
                }
                break;
            }
        }
        if !hammer && rng.chance(20) {
            thread::sleep(Duration::from_micros(rng.range(0, 300)));
        }
    }
    if hold && rng.chance(50) {
        // Hold the sender until the group is gone.
        while !stop.load(SeqCst) {
            thread::sleep(Duration::from_millis(1));
        }
    }
    drop(tx);
}

#[derive(Debug, Default)]
#[allow(dead_code)] // Fields are reported through `Debug`.
struct SeedSummary {
    outside: String,
    strict: bool,
    workers: usize,
    sent: usize,
    received: usize,
    discarded: usize,
    shutdown_time: Option<Duration>,
}

fn run_seed(seed: u64, timeout: Duration) -> SeedSummary {
    let mut rng = Rng::new(seed);
    CURRENT_SEED.store(seed, SeqCst);
    SEED_DEADLINE_MS.store(
        epoch().elapsed().as_millis() as u64 + timeout.as_millis() as u64,
        SeqCst,
    );
    TRACE.lock().unwrap().clear();
    MSG_CREATED.store(0, SeqCst);
    MSG_DROPPED.store(0, SeqCst);
    TASK_CREATED.store(0, SeqCst);
    TASK_DROPPED.store(0, SeqCst);

    let base_fds = count_fds();
    let base_snapshot = fd_snapshot();
    let base_threads = count_user_threads();

    let workers = rng.range(1, 4) as usize;
    let outside = match rng.below(10) {
        0..=2 => Outside::NaturalJoin,
        3 | 4 => Outside::ShutdownAfter(rng.ms(0, 60)),
        5 => Outside::DropAfter(rng.ms(0, 60)),
        6 => Outside::RequestStorm(rng.ms(0, 60)),
        _ => Outside::InWorker {
            worker: rng.below(workers as u64) as usize,
            action: match rng.below(3) {
                0 => GroupAction::Shutdown,
                1 => GroupAction::Drop,
                _ => GroupAction::Join,
            },
            delay: rng.ms(0, 40),
        },
    };
    let natural = matches!(
        outside,
        Outside::NaturalJoin
            | Outside::InWorker {
                action: GroupAction::Join,
                ..
            }
    );
    let chaos_enabled = !natural || rng.chance(20);
    let strict = natural && !chaos_enabled;
    let startup_failure = !natural && rng.chance(4);
    let mut summary = SeedSummary {
        outside: format!("{outside:?}"),
        strict,
        workers,
        ..Default::default()
    };

    let mut builder = RuntimeGroup::builder(workers);
    let stop = Arc::new(AtomicBool::new(false));
    let mut all_stats: Vec<Arc<ChanStats>> = Vec::new();
    let tcp_done = Arc::new(AtomicUsize::new(0));
    let (report_tx, report_rx) = std_mpsc::channel::<String>();

    // Per-worker inboxes, with producers drawn from other workers, the worker
    // itself, and external threads.
    let mut outboxes: Vec<Vec<OutboxPlan>> = (0..workers).map(|_| Vec::new()).collect();
    let mut inboxes: Vec<Option<(DetachedInbox, Arc<ChanStats>, InboxFate)>> = Vec::new();
    let mut ext: Vec<ExtProducer> = Vec::new();
    let mut pings = Vec::new();
    for dst in 0..workers {
        let endpoint = builder.endpoint(WorkerId::new(dst)).unwrap().clone();
        let stats = ChanStats::new(format!("inbox-{dst}"));
        all_stats.push(Arc::clone(&stats));
        let mut producers: Vec<Option<usize>> = (0..workers)
            .filter(|w| (*w != dst || rng.chance(30)) && rng.chance(80))
            .map(Some)
            .collect();
        for _ in 0..rng.below(3) {
            producers.push(None);
        }
        if producers.is_empty() {
            producers.push(None);
        }
        let fate = if !chaos_enabled {
            InboxFate::Drain
        } else {
            match rng.below(10) {
                0 => InboxFate::CloseAfter(rng.below(200) as usize),
                1 => InboxFate::DropAfter(rng.below(200) as usize),
                2 => InboxFate::NeverAttach,
                3 => InboxFate::StashAfter(rng.below(200) as usize),
                _ => InboxFate::Drain,
            }
        };
        let sharded = rng.chance(50);
        let senders: Vec<Outbox>;
        let detached;
        if sharded {
            let capacity = rng.range(producers.len() as u64, 64) as usize;
            let (txs, rx) = mpsc::bounded_sharded::<Msg>(&endpoint, capacity, producers.len());
            senders = txs.into_iter().map(Outbox::Sharded).collect();
            detached = DetachedInbox::Sharded(rx);
        } else {
            let capacity = rng.range(1, 64) as usize;
            let (tx, rx) = mpsc::bounded::<Msg>(&endpoint, capacity);
            let mut v: Vec<Outbox> = (1..producers.len())
                .map(|_| Outbox::Plain(tx.clone()))
                .collect();
            v.push(Outbox::Plain(tx));
            senders = v;
            detached = DetachedInbox::Plain(rx);
        }
        for (producer, tx) in producers.into_iter().zip(senders) {
            let quota = rng.range(0, 400) as usize;
            stats.planned.fetch_add(quota, SeqCst);
            match producer {
                Some(src) => outboxes[src].push(OutboxPlan {
                    dst,
                    tx,
                    stats: Arc::clone(&stats),
                    quota,
                    abort_after: (chaos_enabled && rng.chance(15)).then(|| rng.ms(0, 20)),
                }),
                None => ext.push(ExtProducer {
                    tx,
                    stats: Arc::clone(&stats),
                    quota,
                    rng: rng.fork(),
                    hammer: false,
                }),
            }
        }
        inboxes.push(Some((detached, stats, fate)));

        // A capacity-one ping channel hammered from an external thread races
        // remote unparks against parks.
        if rng.chance(50) {
            let pstats = ChanStats::new(format!("ping-{dst}"));
            all_stats.push(Arc::clone(&pstats));
            let (ptx, prx) = mpsc::bounded::<Msg>(&endpoint, 1);
            let quota = rng.range(100, 3000) as usize;
            pstats.planned.fetch_add(quota, SeqCst);
            ext.push(ExtProducer {
                tx: Outbox::Plain(ptx),
                stats: Arc::clone(&pstats),
                quota,
                rng: rng.fork(),
                hammer: true,
            });
            pings.push((dst, prx, pstats));
        } else {
            pings.push((
                dst,
                mpsc::bounded::<Msg>(&endpoint, 1).1,
                ChanStats::new("unused".into()),
            ));
        }
    }
    let mut ping_slots: Vec<Option<(DetachedReceiver<Msg>, Arc<ChanStats>)>> = pings
        .into_iter()
        .map(|(_, rx, s)| {
            if s.name == "unused" {
                drop(rx);
                None
            } else {
                Some((rx, s))
            }
        })
        .collect();

    // TCP echo topology: each connector connects to one listener worker.
    let mut tcp_listen: Vec<Vec<Sender<SocketAddr>>> = (0..workers).map(|_| Vec::new()).collect();
    let mut tcp_connect: Vec<Vec<DetachedReceiver<SocketAddr>>> =
        (0..workers).map(|_| Vec::new()).collect();
    let tcp_rounds = rng.range(1, 6) as usize;
    let mut tcp_expected = 0;
    if rng.chance(70) {
        let listener = rng.below(workers as u64) as usize;
        for (c, connects) in tcp_connect.iter_mut().enumerate() {
            if rng.chance(70) {
                let endpoint = builder.endpoint(WorkerId::new(c)).unwrap().clone();
                let (tx, rx) = mpsc::bounded::<SocketAddr>(&endpoint, 1);
                tcp_listen[listener].push(tx);
                connects.push(rx);
                tcp_expected += tcp_rounds;
            }
        }
    }

    // In-worker group action.
    let mut group_tx = None;
    let mut group_rxs: Vec<Option<(DetachedReceiver<RuntimeGroup>, GroupAction, Duration)>> =
        (0..workers).map(|_| None).collect();
    if let Outside::InWorker {
        worker,
        action,
        delay,
    } = outside
    {
        let endpoint = builder.endpoint(WorkerId::new(worker)).unwrap().clone();
        let (tx, rx) = mpsc::bounded::<RuntimeGroup>(&endpoint, 1);
        group_tx = Some(tx);
        group_rxs[worker] = Some((rx, action, delay));
    }

    let mut setups = Vec::new();
    for id in 0..workers {
        let ring = RingCfg {
            size: 1 << rng.range(5, 8),
            sqpoll: rng.chance(15),
            coop: rng.chance(30),
            single_issuer: rng.chance(30),
            budget: if rng.chance(20) {
                None
            } else {
                Some(rng.range(1, 64) as usize)
            },
        };
        let chaos = (chaos_enabled && rng.chance(35)).then(|| {
            // A worker that silently returns early can strand peers that
            // wait for it; only do that when someone will request shutdown.
            let c = match rng.below(4) {
                0 => Chaos::ReturnErr,
                1 => Chaos::Panic,
                2 if !natural => Chaos::ReturnOk,
                _ => Chaos::RequestShutdown,
            };
            (rng.ms(0, 50), c)
        });
        setups.push(WorkerSetup {
            id,
            rng: rng.fork(),
            strict,
            linger_until_shutdown: !natural && rng.chance(70),
            inbox: inboxes[id].take(),
            ping: ping_slots[id].take(),
            outboxes: std::mem::take(&mut outboxes[id]),
            tcp_listen: std::mem::take(&mut tcp_listen[id]),
            tcp_connect: std::mem::take(&mut tcp_connect[id]),
            tcp_rounds,
            tcp_done: Arc::clone(&tcp_done),
            group_rx: group_rxs[id].take(),
            group_report: report_tx.clone(),
            chaos,
            timers: rng.range(0, 3) as usize,
            nops: rng.range(0, 3) as usize,
            aborts: rng.range(0, 6) as usize,
            panicking_tasks: if rng.chance(30) {
                rng.range(1, 2) as usize
            } else {
                0
            },
            lingering: rng.chance(50),
            stash_misc: rng.chance(30),
            aux_stack: rng.chance(15).then(|| rng.below(3) as u8),
            ring,
        });
    }
    drop(report_tx);

    let failing_worker = startup_failure.then(|| rng.below(workers as u64) as usize);
    let factory_panics = rng.chance(50);
    for setup in setups {
        let id = setup.id;
        let ring = setup.ring;
        let fail_here = failing_worker == Some(id);
        builder
            .worker_with(
                WorkerId::new(id),
                move || {
                    register_live_worker();
                    if fail_here {
                        if factory_panics {
                            panic!("intentional factory panic");
                        }
                        return Err(io::Error::other("intentional startup failure"));
                    }
                    build_park(ring)
                },
                move |ctx| worker_main(ctx, setup),
            )
            .unwrap();
    }

    // External producers start before the group, so some traffic lands before
    // receivers attach.
    let mut ext_threads = Vec::new();
    for p in ext {
        let stop = Arc::clone(&stop);
        ext_threads.push(
            thread::Builder::new()
                .name("stress-ext".into())
                .spawn(move || run_ext_producer(p, strict, !natural, stop))
                .unwrap(),
        );
    }

    match builder.start() {
        Err(StartError::Worker(e)) => {
            if !startup_failure {
                fail(format!("unexpected start failure: {e:?}"));
            }
            check_join_error("start", &e);
            if !e.failures().iter().any(|f| {
                f.kind() == WorkerFailureKind::Startup || f.kind() == WorkerFailureKind::Panic
            }) {
                fail(format!("startup failure not reported: {e:?}"));
            }
            drop(group_tx.take());
        }
        Err(e) => fail(format!("unexpected start error: {e}")),
        Ok(group) => {
            if startup_failure {
                fail("start succeeded despite a failing park factory");
            }
            trace(format!("group started, outside={outside:?}"));
            // After an outside join, shutdown or drop returns, every worker
            // thread must already be gone: thread-local destructors run before
            // the thread join completes. (An in-worker action detaches its own
            // worker, so that case is polled below instead.)
            let workers_gone = |what: &str| {
                let live = LIVE_WORKERS.load(SeqCst);
                if live != 0 {
                    fail(format!(
                        "{what} returned while {live} workers were still running"
                    ));
                }
            };
            match outside {
                Outside::NaturalJoin => {
                    if let Err(e) = group.join() {
                        check_join_error("join", &e);
                    }
                    workers_gone("join");
                }
                Outside::ShutdownAfter(d) => {
                    thread::sleep(d);
                    let t = Instant::now();
                    let r = group.shutdown();
                    summary.shutdown_time = Some(t.elapsed());
                    if let Err(e) = r {
                        check_join_error("shutdown", &e);
                    }
                    workers_gone("shutdown");
                }
                Outside::DropAfter(d) => {
                    thread::sleep(d);
                    let t = Instant::now();
                    drop(group);
                    summary.shutdown_time = Some(t.elapsed());
                    workers_gone("drop");
                }
                Outside::RequestStorm(d) => {
                    thread::sleep(d);
                    let t = Instant::now();
                    thread::scope(|scope| {
                        for _ in 0..3 {
                            scope.spawn(|| {
                                for _ in 0..10 {
                                    group.request_shutdown();
                                    thread::yield_now();
                                }
                            });
                        }
                    });
                    let r = group.join();
                    summary.shutdown_time = Some(t.elapsed());
                    if let Err(e) = r {
                        check_join_error("storm join", &e);
                    }
                    workers_gone("storm join");
                }
                Outside::InWorker { .. } => {
                    let tx = group_tx.take().unwrap();
                    match tx.try_send(group) {
                        Ok(()) => {}
                        Err(TrySendError::Closed(g)) | Err(TrySendError::Full(g)) => {
                            trace("group channel refused the group");
                            if let Err(e) = g.shutdown() {
                                check_join_error("fallback shutdown", &e);
                            }
                        }
                    }
                    drop(tx);
                }
            }
        }
    }
    drop(group_tx);

    // Wait for every worker thread to exit, including a worker detached by an
    // in-worker join, and for external producers to finish.
    let exit_limit = Duration::from_secs(10);
    if !wait_until(exit_limit, || LIVE_WORKERS.load(SeqCst) == 0) {
        // `LIVE_WORKERS` is process-wide, so a stuck worker would fail every
        // later seed too. Stop instead of reporting them all.
        eprintln!(
            "{} worker threads still running after {exit_limit:?}:\n{}",
            LIVE_WORKERS.load(SeqCst),
            dump_threads()
        );
        eprintln!("STRESS-HANG seed={seed}");
        std::process::abort();
    }
    if !wait_until(exit_limit, || {
        count_user_threads()
            <= base_threads + ext_threads.iter().filter(|t| !t.is_finished()).count()
    }) {
        fail(format!(
            "worker threads did not exit within {exit_limit:?}:\n{}",
            dump_threads()
        ));
    }
    stop.store(true, SeqCst);
    for t in ext_threads {
        if t.join().is_err() {
            fail("external producer panicked");
        }
    }
    if !wait_until(exit_limit, || count_user_threads() <= base_threads) {
        fail(format!("threads leaked:\n{}", dump_threads()));
    }
    for line in report_rx.try_iter() {
        trace(line);
    }

    // Accounting.
    for s in &all_stats {
        let sent = s.sent_ok.load(SeqCst);
        let recv = s.received.load(SeqCst);
        let disc = s.discarded.load(SeqCst);
        summary.sent += sent;
        summary.received += recv;
        summary.discarded += disc;
        if sent != recv + disc {
            fail(format!(
                "{}: sent_ok={sent} but received={recv} + discarded={disc}",
                s.name
            ));
        }
        if strict {
            let planned = s.planned.load(SeqCst);
            if sent != planned || recv != planned || disc != 0 {
                fail(format!(
                    "{}: strict run planned={planned} sent={sent} received={recv} discarded={disc}",
                    s.name
                ));
            }
            if !s.saw_closed.load(SeqCst) {
                fail(format!("{}: strict run never observed Closed", s.name));
            }
        }
    }
    if strict && tcp_done.load(SeqCst) != tcp_expected {
        fail(format!(
            "tcp: {} of {tcp_expected} echo rounds completed",
            tcp_done.load(SeqCst)
        ));
    }
    let (mc, md) = (MSG_CREATED.load(SeqCst), MSG_DROPPED.load(SeqCst));
    if mc != md {
        fail(format!("messages leaked: created={mc} dropped={md}"));
    }
    let (tc, td) = (TASK_CREATED.load(SeqCst), TASK_DROPPED.load(SeqCst));
    if tc != td {
        fail(format!("tasks leaked: created={tc} dropped={td}"));
    }
    if let Some(t) = summary.shutdown_time {
        if t > Duration::from_secs(5) {
            fail(format!("shutdown took {t:?}"));
        }
    }
    drop(all_stats);
    if !wait_until(Duration::from_secs(5), || count_fds() <= base_fds) {
        fail(format!(
            "fds leaked: baseline {base_fds}, now {}\n{}",
            count_fds(),
            fd_listing()
        ));
    }
    let now = fd_snapshot();
    for (fd, target) in &base_snapshot {
        if target.contains("/proc/") {
            continue;
        }
        match now.get(fd) {
            Some(t) if t == target => {}
            other => fail(format!(
                "pre-existing fd {fd} ({target}) was closed or replaced: now {other:?}"
            )),
        }
    }
    SEED_DEADLINE_MS.store(0, SeqCst);
    summary
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

#[test]
#[ignore = "long-running randomized stress test"]
fn lifecycle_stress() {
    install_panic_hook();
    start_watchdog();
    let timeout = Duration::from_secs(env_u64("STRESS_SEED_TIMEOUT_SECS").unwrap_or(60));
    let stop_on_failure = env_u64("STRESS_STOP_ON_FAILURE").unwrap_or(0) != 0;
    let (first, budget, single) = match env_u64("STRESS_SEED") {
        Some(seed) => (seed, Duration::ZERO, true),
        None => (
            env_u64("STRESS_SEED_START").unwrap_or(0),
            Duration::from_secs(env_u64("STRESS_SECS").unwrap_or(30)),
            false,
        ),
    };
    let started = Instant::now();
    let mut seed = first;
    let mut ran = 0u64;
    let mut failed = Vec::new();
    loop {
        let t = Instant::now();
        let summary = run_seed(seed, timeout);
        let failures: Vec<String> =
            std::mem::take(&mut *FAILURES.lock().unwrap_or_else(|e| e.into_inner()));
        ran += 1;
        if !failures.is_empty() {
            eprintln!("STRESS-FAIL seed={seed} {summary:?} in {:?}", t.elapsed());
            for f in &failures {
                eprintln!("  {f}");
            }
            for line in TRACE.lock().unwrap().iter() {
                eprintln!("  trace {line}");
            }
            failed.push(seed);
            if stop_on_failure {
                break;
            }
        } else if single || ran % 200 == 0 {
            eprintln!(
                "seed={seed} ok {summary:?} in {:?} (ran {ran}, elapsed {:?})",
                t.elapsed(),
                started.elapsed()
            );
        }
        seed += 1;
        if single || started.elapsed() >= budget {
            break;
        }
    }
    eprintln!(
        "STRESS-DONE ran={ran} seeds {first}..{seed} failed={failed:?} sqpoll_fallbacks={} elapsed={:?}",
        SQPOLL_FALLBACKS.load(SeqCst),
        started.elapsed()
    );
    assert!(failed.is_empty(), "failing seeds: {failed:?}");
}
