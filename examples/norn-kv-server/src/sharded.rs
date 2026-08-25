//! Fixed-worker sharding for the networking benchmark.
//!
//! Each worker owns its listener, `io_uring` driver, executor, and in-memory
//! shard. Cross-thread requests and responses contain owned, `Send` data only;
//! sockets, tasks, and wakers remain on the worker which created them.
//! Sharded mode requires multishot receive. `STAT` returns `UnknownCommand`
//! until cross-shard statistics aggregation is defined.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc as std_mpsc, Arc};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

use futures_util::future::{self, Either};
use futures_util::{FutureExt, StreamExt};
use norn_channel::mpsc::{
    self, DetachedReceiver, DetachedShardedReceiver, Sender, ShardedSender, TrySendError,
};
use norn_channel::DriverBuilder;
use norn_executor::{spawn, LocalExecutor};
use norn_uring::buf::{BufCursor, StableBuf};
use norn_uring::bufring::{BufRingBuf, RecvBufRing};
use norn_uring::net::{TcpListener, TcpListenerOptions, TcpSocket};
use smallvec::SmallVec;

use crate::codec::{DecodeStatus, DecodedFrame, FrameDecoder, ResponseEncoder};
use crate::handler::{ConnectionAction, HandlerConfig, MemoryHandler};
use crate::protocol::{Command, RequestHeader, Status, HEADER_LEN};
use crate::server::{RecvMode, ServerConfig};

const INLINE_KEY_LEN: usize = 32;
const DEFAULT_PAIR_CAPACITY: usize = 1_024;
const PUMP_BATCH: usize = 64;
const RECV_RING_BUFFERS: u16 = 8_192;
const RECV_BUFFER_LEN: usize = 8 * 1024;
const MAX_IN_FLIGHT_PER_CONNECTION: usize = 32;

/// Configuration for a fixed group of sharded networking workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardedServerConfig {
    /// Address shared by every `SO_REUSEPORT` listener.
    pub listen: SocketAddr,
    /// Listener accept backlog.
    pub backlog: u32,
    /// Entries in each worker's private `io_uring`.
    pub ring_entries: u32,
    /// Number of worker threads and in-memory shards.
    pub workers: usize,
    /// Optional one-to-one worker CPU assignment.
    pub worker_cpus: Vec<usize>,
    /// Command execution backend constructed independently on every worker.
    pub handler: HandlerConfig,
    /// Limits inherited from the single-worker networking server.
    ///
    /// The receive mode must be [`RecvMode::Multishot`].
    pub server: ServerConfig,
    /// Maximum credited cross-worker exchanges per directed worker pair.
    ///
    /// A remote request acquires one paired request/response credit before its
    /// key and value are copied. The credit is returned after the matching
    /// response reaches the origin worker. Request and response queues are
    /// reserved for every directed worker pair, so aggregate queue capacity is
    /// `2 * workers^2 * pair_capacity`.
    pub pair_capacity: usize,
}

impl Default for ShardedServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:11211".parse().unwrap(),
            backlog: 1_024,
            ring_entries: 256,
            workers: 2,
            worker_cpus: Vec::new(),
            handler: HandlerConfig::Memory,
            server: ServerConfig::default(),
            pair_capacity: DEFAULT_PAIR_CAPACITY,
        }
    }
}

impl ShardedServerConfig {
    fn validate(&self) -> io::Result<()> {
        if self.workers < 2 {
            return Err(invalid_input("sharded mode requires at least two workers"));
        }
        self.server.validate()?;
        if self.backlog == 0 || self.ring_entries == 0 || self.pair_capacity == 0 {
            return Err(invalid_input("numeric limits must be greater than zero"));
        }
        let receiver_capacity = self
            .workers
            .checked_mul(self.pair_capacity)
            .ok_or_else(|| invalid_input("sharded receiver capacity overflows usize"))?;
        self.workers
            .checked_mul(receiver_capacity)
            .and_then(|capacity| capacity.checked_mul(2))
            .ok_or_else(|| invalid_input("aggregate sharded channel capacity overflows usize"))?;
        if self.server.recv_mode != RecvMode::Multishot {
            return Err(invalid_input(
                "sharded mode currently requires --recv-mode multishot",
            ));
        }
        if !self.worker_cpus.is_empty() && self.worker_cpus.len() != self.workers {
            return Err(invalid_input(
                "--worker-cpus must contain exactly one CPU per worker",
            ));
        }
        let mut distinct = HashSet::with_capacity(self.worker_cpus.len());
        for &cpu in &self.worker_cpus {
            if cpu >= libc::CPU_SETSIZE as usize {
                return Err(invalid_input(format!(
                    "worker CPU {cpu} exceeds CPU_SETSIZE"
                )));
            }
            if !distinct.insert(cpu) {
                return Err(invalid_input(format!(
                    "worker CPU {cpu} appears more than once"
                )));
            }
        }
        Ok(())
    }
}

/// A running fixed group of sharded networking workers.
///
/// Dropping this handle requests cooperative shutdown but does not wait for
/// the worker threads. Call [`Self::shutdown`] to stop and join them, or
/// [`Self::wait`] to run until a worker exits.
#[derive(Debug)]
pub struct ShardedServer {
    address: SocketAddr,
    controls: Vec<Sender<Control>>,
    stopping: Arc<AtomicBool>,
    workers: Vec<WorkerThread>,
    exits: std_mpsc::Receiver<WorkerExit>,
}

#[derive(Debug)]
struct WorkerThread {
    index: usize,
    join: JoinHandle<io::Result<()>>,
}

#[derive(Debug)]
struct WorkerExit {
    index: usize,
    error: Option<String>,
}

impl ShardedServer {
    /// Start every worker after all reuse-port listeners bind successfully.
    ///
    /// When `listen` uses port zero, worker zero chooses the port before the
    /// remaining workers bind the same concrete address.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid configuration, affinity failure, worker
    /// creation failure, driver creation failure, or listener bind failure.
    pub fn start(config: ShardedServerConfig) -> io::Result<Self> {
        config.validate()?;
        let (worker_channels, controls) = build_topology(config.workers, config.pair_capacity);
        let active = Arc::new(AtomicUsize::new(0));
        let next_connection = Arc::new(AtomicU64::new(0));
        let stopping = Arc::new(AtomicBool::new(false));
        let (exit_tx, exit_rx) = std_mpsc::channel();
        let mut workers = Vec::with_capacity(config.workers);
        let mut channels = worker_channels.into_iter();

        let (first, first_startup) = spawn_worker(
            0,
            config.listen,
            channels.next().unwrap(),
            &config,
            Arc::clone(&active),
            Arc::clone(&next_connection),
            Arc::clone(&stopping),
            exit_tx.clone(),
        )?;
        workers.push(first);
        let address = match recv_startup(first_startup, 0) {
            Ok(address) => address,
            Err(error) => {
                stop_and_join(&stopping, &controls, workers);
                return Err(error);
            }
        };

        for (index, worker_channels) in channels.enumerate() {
            let index = index + 1;
            match spawn_worker(
                index,
                address,
                worker_channels,
                &config,
                Arc::clone(&active),
                Arc::clone(&next_connection),
                Arc::clone(&stopping),
                exit_tx.clone(),
            ) {
                Ok((worker, startup)) => {
                    workers.push(worker);
                    if let Err(error) = recv_startup(startup, index) {
                        stop_and_join(&stopping, &controls, workers);
                        return Err(error);
                    }
                }
                Err(error) => {
                    stop_and_join(&stopping, &controls, workers);
                    return Err(error);
                }
            }
        }
        drop(exit_tx);

        for control in &controls {
            if control.try_send(Control::Start).is_err() {
                stop_and_join(&stopping, &controls, workers);
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "worker control channel closed during startup",
                ));
            }
        }

        Ok(Self {
            address,
            controls,
            stopping,
            workers,
            exits: exit_rx,
        })
    }

    /// Return the concrete address shared by every worker listener.
    pub const fn local_addr(&self) -> SocketAddr {
        self.address
    }

    /// Wait until a worker exits, stop its peers, and join the group.
    ///
    /// # Errors
    ///
    /// Returns the worker failure, or an unexpected-exit error when a worker
    /// returned cleanly without a shutdown request.
    pub fn wait(mut self) -> io::Result<()> {
        let first = self.exits.recv().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "all worker exit reporters disconnected",
            )
        })?;
        request_stop(&self.stopping, &self.controls);
        let joined = join_workers(std::mem::take(&mut self.workers));
        joined?;
        match first.error {
            Some(error) => Err(io::Error::other(format!(
                "worker {} failed: {error}",
                first.index
            ))),
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("worker {} exited unexpectedly", first.index),
            )),
        }
    }

    /// Request cooperative shutdown and join every worker.
    ///
    /// # Errors
    ///
    /// Returns the first worker failure or panic.
    pub fn shutdown(mut self) -> io::Result<()> {
        request_stop(&self.stopping, &self.controls);
        join_workers(std::mem::take(&mut self.workers))
    }
}

impl Drop for ShardedServer {
    fn drop(&mut self) {
        request_stop(&self.stopping, &self.controls);
    }
}

/// Pin the calling worker thread to one Linux CPU.
///
/// # Errors
///
/// Returns an error when the CPU index is outside `CPU_SETSIZE` or Linux
/// rejects the affinity request.
pub fn pin_current_thread(cpu: usize) -> io::Result<()> {
    if cpu >= libc::CPU_SETSIZE as usize {
        return Err(invalid_input(format!("CPU {cpu} exceeds CPU_SETSIZE")));
    }
    // SAFETY: cpu_set_t is initialized before use; CPU_SET is guarded by the
    // CPU_SETSIZE check; pid zero names the calling thread on Linux.
    let result = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
    };
    if result == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

struct StartupReporter {
    worker: usize,
    sender: Option<std_mpsc::Sender<io::Result<SocketAddr>>>,
}

impl StartupReporter {
    fn ready(&mut self, address: SocketAddr) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Ok(address));
        }
    }

    fn failed(&mut self, error: &io::Error) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(io::Error::new(
                error.kind(),
                format!("worker {} startup failed: {error}", self.worker),
            )));
        }
    }
}

impl Drop for StartupReporter {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(io::Error::other(format!(
                "worker {} exited before reporting startup",
                self.worker
            ))));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_worker(
    index: usize,
    listen: SocketAddr,
    channels: WorkerChannels,
    config: &ShardedServerConfig,
    active: Arc<AtomicUsize>,
    next_connection: Arc<AtomicU64>,
    stopping: Arc<AtomicBool>,
    exits: std_mpsc::Sender<WorkerExit>,
) -> io::Result<(WorkerThread, std_mpsc::Receiver<io::Result<SocketAddr>>)> {
    let (startup_tx, startup_rx) = std_mpsc::channel();
    let cpu = config.worker_cpus.get(index).copied();
    let backlog = config.backlog;
    let ring_entries = config.ring_entries;
    let worker_count = config.workers;
    let pair_capacity = config.pair_capacity;
    let handler = config.handler;
    let server = config.server;
    let join = thread::Builder::new()
        .name(format!("norn-kv-{index}"))
        .spawn(move || {
            let mut startup = StartupReporter {
                worker: index,
                sender: Some(startup_tx),
            };
            let result = catch_unwind(AssertUnwindSafe(|| {
                run_worker(
                    index,
                    listen,
                    backlog,
                    ring_entries,
                    worker_count,
                    pair_capacity,
                    handler,
                    server,
                    cpu,
                    channels,
                    active,
                    next_connection,
                    stopping,
                    &mut startup,
                )
            }))
            .unwrap_or_else(|panic| {
                Err(io::Error::other(format!(
                    "worker panicked: {}",
                    panic_message(&*panic)
                )))
            });
            if let Err(error) = &result {
                startup.failed(error);
            }
            let _ = exits.send(WorkerExit {
                index,
                error: result.as_ref().err().map(ToString::to_string),
            });
            result
        })?;
    Ok((WorkerThread { index, join }, startup_rx))
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    index: usize,
    listen: SocketAddr,
    backlog: u32,
    ring_entries: u32,
    worker_count: usize,
    pair_capacity: usize,
    handler: HandlerConfig,
    server: ServerConfig,
    cpu: Option<usize>,
    channels: WorkerChannels,
    active: Arc<AtomicUsize>,
    next_connection: Arc<AtomicU64>,
    stopping: Arc<AtomicBool>,
    startup: &mut StartupReporter,
) -> io::Result<()> {
    if let Some(cpu) = cpu {
        pin_current_thread(cpu).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("pin worker {index} to CPU {cpu}: {error}"),
            )
        })?;
    }

    let uring =
        norn_uring::Driver::new(io_uring::IoUring::builder(), ring_entries).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("create worker {index} io_uring driver: {error}"),
            )
        })?;
    let WorkerChannels {
        driver,
        request_txs,
        request_rx,
        response_txs,
        response_rx,
        control_rx,
    } = channels;
    let driver = driver.build(uring);
    let handle = driver.handle();
    let request_rx = request_rx.attach(&handle);
    let response_rx = response_rx.attach(&handle);
    let control_rx = control_rx.attach(&handle);
    let (fatal_tx, fatal_rx) = mpsc::bounded::<String>(&driver.endpoint(), 2);
    let fatal_rx = fatal_rx.attach(&handle);
    let mut executor = LocalExecutor::new(driver);
    executor.try_block_on(worker_main(
        index,
        listen,
        backlog,
        worker_count,
        pair_capacity,
        handler,
        server,
        request_txs,
        request_rx,
        response_txs,
        response_rx,
        control_rx,
        fatal_tx,
        fatal_rx,
        active,
        next_connection,
        stopping,
        startup,
    ))?
}

#[allow(clippy::too_many_arguments)]
async fn worker_main(
    index: usize,
    listen: SocketAddr,
    backlog: u32,
    worker_count: usize,
    pair_capacity: usize,
    handler: HandlerConfig,
    server: ServerConfig,
    request_txs: Vec<ShardedSender<ShardRequest>>,
    request_rx: norn_channel::mpsc::ShardedReceiver<ShardRequest>,
    response_txs: Vec<ShardedSender<ShardResponse>>,
    response_rx: norn_channel::mpsc::ShardedReceiver<ShardResponse>,
    mut control_rx: norn_channel::mpsc::Receiver<Control>,
    fatal_tx: Sender<String>,
    fatal_rx: norn_channel::mpsc::Receiver<String>,
    active: Arc<AtomicUsize>,
    next_connection: Arc<AtomicU64>,
    stopping: Arc<AtomicBool>,
    startup: &mut StartupReporter,
) -> io::Result<()> {
    let listener =
        TcpListener::bind_with_options(listen, backlog, TcpListenerOptions::new().reuse_port(true))
            .await
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("bind worker {index} listener at {listen}: {error}"),
                )
            })?;
    let address = listener.local_addr()?;
    let recv_ring = RecvBufRing::builder(1)
        .buf_cnt(RECV_RING_BUFFERS)
        .buf_len(RECV_BUFFER_LEN)
        .build()?;
    startup.ready(address);

    match control_rx.recv().await {
        Some(Control::Start) => {}
        Some(Control::Stop) | None => return Ok(()),
    }

    let handler = handler.build();
    let router = RouterLocal::new(
        request_txs,
        pair_capacity,
        fatal_tx.clone(),
        Arc::clone(&stopping),
    );
    let request_router = Rc::clone(&router);
    let request_fatal = fatal_tx.clone();
    let request_handler = handler.clone();
    let request_stopping = Arc::clone(&stopping);
    spawn(async move {
        let outcome = AssertUnwindSafe(request_pump(
            index,
            worker_count,
            request_rx,
            response_txs,
            request_handler,
        ))
        .catch_unwind()
        .await;
        let failure = match outcome {
            Ok(Ok(())) => Some("shard request pump exited unexpectedly".to_owned()),
            Ok(Err(error)) => Some(error.to_string()),
            Err(panic) => Some(format!(
                "shard request pump panicked: {}",
                panic_message(&*panic)
            )),
        };
        request_router.close();
        if !request_stopping.swap(true, Ordering::AcqRel) {
            let _ = request_fatal.try_send(failure.unwrap());
        }
    })
    .detach();

    let response_router = Rc::clone(&router);
    let response_fatal = fatal_tx.clone();
    let response_stopping = Arc::clone(&stopping);
    spawn(async move {
        let outcome = AssertUnwindSafe(response_pump(response_rx, Rc::clone(&response_router)))
            .catch_unwind()
            .await;
        let failure = match outcome {
            Ok(Ok(())) => Some("shard response pump exited unexpectedly".to_owned()),
            Ok(Err(error)) => Some(error.to_string()),
            Err(panic) => Some(format!(
                "shard response pump panicked: {}",
                panic_message(&*panic)
            )),
        };
        response_router.close();
        if !response_stopping.swap(true, Ordering::AcqRel) {
            let _ = response_fatal.try_send(failure.unwrap());
        }
    })
    .detach();
    drop(fatal_tx);

    let result = serve_worker(
        index,
        worker_count,
        listener,
        handler,
        router.clone(),
        recv_ring,
        server,
        control_rx,
        fatal_rx,
        active,
        next_connection,
    )
    .await;
    router.close();
    result
}

fn recv_startup(
    startup: std_mpsc::Receiver<io::Result<SocketAddr>>,
    index: usize,
) -> io::Result<SocketAddr> {
    startup.recv().map_err(|_| {
        io::Error::other(format!(
            "worker {index} disconnected before reporting startup"
        ))
    })?
}

fn request_stop(stopping: &AtomicBool, controls: &[Sender<Control>]) {
    stopping.store(true, Ordering::Release);
    for control in controls {
        let _ = control.try_send(Control::Stop);
    }
}

fn stop_and_join(stopping: &AtomicBool, controls: &[Sender<Control>], workers: Vec<WorkerThread>) {
    request_stop(stopping, controls);
    let _ = join_workers(workers);
}

fn join_workers(workers: Vec<WorkerThread>) -> io::Result<()> {
    let mut first_error = None;
    for worker in workers {
        match worker.join.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) if first_error.is_none() => {
                first_error = Some(io::Error::new(
                    error.kind(),
                    format!("worker {} failed: {error}", worker.index),
                ));
            }
            Err(panic) if first_error.is_none() => {
                first_error = Some(io::Error::other(format!(
                    "worker {} panicked: {}",
                    worker.index,
                    panic_message(&*panic)
                )));
            }
            Ok(Err(_)) | Err(_) => {}
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn panic_message(panic: &(dyn std::any::Any + Send + 'static)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

async fn request_pump(
    worker: usize,
    worker_count: usize,
    mut requests: norn_channel::mpsc::ShardedReceiver<ShardRequest>,
    responses: Vec<ShardedSender<ShardResponse>>,
    handler: MemoryHandler,
) -> io::Result<()> {
    let mut batch = Vec::with_capacity(PUMP_BATCH);
    loop {
        batch.clear();
        let received = requests.recv_many(&mut batch, PUMP_BATCH).await;
        if received == 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "shard request receiver closed",
            ));
        }
        for request in batch.drain(..) {
            if request.origin >= responses.len()
                || key_owner(request.command.key(), worker_count) != worker
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "misrouted sharded request",
                ));
            }

            let mut bytes = Vec::new();
            let result = handler
                .execute_command(
                    request.header,
                    request.command.borrowed(),
                    &mut ResponseEncoder::new(&mut bytes),
                )
                .map(|action| CommandResponse { action, bytes })
                .map_err(|error| RemoteError::Encoding(error.to_string()));
            // Every admitted request produces exactly one reverse envelope,
            // even when a quiet command deliberately encodes zero wire bytes.
            let response = ShardResponse {
                owner: worker,
                id: request.id,
                result,
            };
            match responses[request.origin].try_send(response) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    return Err(io::Error::other(
                        "shard response lane filled despite paired credits",
                    ));
                }
                Err(TrySendError::Closed(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "shard response channel closed",
                    ));
                }
            }
        }
        yield_once().await;
    }
}

async fn response_pump(
    mut responses: norn_channel::mpsc::ShardedReceiver<ShardResponse>,
    router: Rc<RouterLocal>,
) -> io::Result<()> {
    let mut batch = Vec::with_capacity(PUMP_BATCH);
    loop {
        batch.clear();
        if responses.recv_many(&mut batch, PUMP_BATCH).await == 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "shard response receiver closed",
            ));
        }
        for response in batch.drain(..) {
            router.complete(response)?;
        }
        yield_once().await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_worker(
    worker: usize,
    worker_count: usize,
    listener: TcpListener,
    handler: MemoryHandler,
    router: Rc<RouterLocal>,
    recv_ring: RecvBufRing,
    config: ServerConfig,
    mut control: norn_channel::mpsc::Receiver<Control>,
    mut fatal: norn_channel::mpsc::Receiver<String>,
    active: Arc<AtomicUsize>,
    next_connection: Arc<AtomicU64>,
) -> io::Result<()> {
    let mut incoming = Box::pin(listener.incoming());
    loop {
        futures_util::select_biased! {
            command = control.recv().fuse() => match command {
                Some(Control::Stop) | None => return Ok(()),
                Some(Control::Start) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "worker received duplicate start command",
                    ));
                }
            },
            failure = fatal.recv().fuse() => {
                return Err(io::Error::other(failure.unwrap_or_else(|| {
                    "worker pumps exited without reporting a result".to_owned()
                })));
            },
            next = incoming.next().fuse() => {
                let Some(next) = next else {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "reuse-port listener ended",
                    ));
                };
                let socket = next?;
                let admitted = active.try_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |current| (current < config.max_connections).then_some(current + 1),
                );
                if admitted.is_err() {
                    socket.close().await?;
                    continue;
                }
                let connection = next_connection.fetch_add(1, Ordering::Relaxed);
                if connection == u64::MAX {
                    active.fetch_sub(1, Ordering::AcqRel);
                    socket.close().await?;
                    return Err(io::Error::other("connection identity space exhausted"));
                }

                let guard = ShardedConnectionGuard(Arc::clone(&active));
                let handler = handler.clone();
                let connection_router = Rc::clone(&router);
                let recv_ring = recv_ring.clone();
                spawn(async move {
                    let outcome = AssertUnwindSafe(serve_sharded_connection(
                        socket,
                        worker,
                        worker_count,
                        connection,
                        handler,
                        Rc::clone(&connection_router),
                        recv_ring,
                        config,
                    ))
                    .catch_unwind()
                    .await;
                    drop(guard);
                    match outcome {
                        Ok(Err(error)) => {
                            if !matches!(
                                error.kind(),
                                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                            ) {
                                eprintln!("connection failed: {error}");
                            }
                        }
                        Ok(Ok(())) => {}
                        Err(panic) => {
                            connection_router.fail(&format!(
                                "connection task panicked: {}",
                                panic_message(&*panic)
                            ));
                        }
                    }
                })
                .detach();
            },
        }
    }
}

struct ShardedConnectionGuard(Arc<AtomicUsize>);

impl Drop for ShardedConnectionGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_sharded_connection(
    socket: TcpSocket,
    worker: usize,
    worker_count: usize,
    connection: u64,
    handler: MemoryHandler,
    router: Rc<RouterLocal>,
    recv_ring: RecvBufRing,
    config: ServerConfig,
) -> io::Result<()> {
    socket.set_nodelay(true).await?;
    let replies = router.register_connection(connection)?;
    let mut incoming = Box::pin(socket.recv_ring_multi(&recv_ring));
    let decoder = FrameDecoder::new(config.max_body_len);
    let mut pending = Vec::with_capacity(RECV_BUFFER_LEN);
    let mut output = Vec::with_capacity(config.max_body_len + HEADER_LEN);
    let mut stop_dispatch = false;
    let mut close_ready = false;
    let mut peer_eof = false;

    loop {
        let mut consumed = 0;
        let mut dispatched = 0;
        let mut need_more = false;
        while !stop_dispatch
            && replies.len() < MAX_IN_FLIGHT_PER_CONNECTION
            && dispatched < config.max_batch_commands
        {
            let DecodeStatus::Frame {
                frame,
                consumed: frame_len,
            } = decoder
                .decode(&pending[consumed..])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            else {
                need_more = true;
                break;
            };

            let closes =
                dispatch_frame(frame, worker, worker_count, &handler, &router, &replies).await?;
            consumed += frame_len;
            dispatched += 1;
            stop_dispatch |= closes;
        }

        if consumed != 0 {
            let remaining = pending.len() - consumed;
            pending.copy_within(consumed.., 0);
            pending.truncate(remaining);
        }
        if need_more && pending.len() > config.max_body_len + HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partial request frame exceeds configured maximum",
            ));
        }

        let mut completed = 0;
        while output.len() < config.max_batch_response_bytes
            && completed < config.max_batch_commands
        {
            let Some(result) = replies.try_next() else {
                break;
            };
            let response = result?;
            close_ready |= response.action == ConnectionAction::Close;
            output.extend_from_slice(&response.bytes);
            completed += 1;
        }

        if !output.is_empty() {
            output = send_all(&socket, output).await?;
            output.clear();
        }
        if (close_ready || peer_eof) && replies.is_empty() {
            if peer_eof {
                drop(incoming);
                return socket.close().await;
            }
            // QUIT leaves the peer open and the multishot receive armed.
            // Dropping both lets the receive's terminal cancellation retain
            // the descriptor until its ordinary deferred close boundary.
            drop(incoming);
            drop(socket);
            return Ok(());
        }
        if !need_more
            && !stop_dispatch
            && !pending.is_empty()
            && replies.len() < MAX_IN_FLIGHT_PER_CONNECTION
        {
            // Admission stopped on a batch or in-flight limit, not on an
            // incomplete frame. Revisit already-buffered bytes before waiting
            // for another receive event.
            continue;
        }

        enum Event {
            Receive(Option<io::Result<BufRingBuf>>),
            Response(io::Result<CommandResponse>),
        }

        let event = if peer_eof || stop_dispatch || replies.len() >= MAX_IN_FLIGHT_PER_CONNECTION {
            Event::Response(replies.next().await)
        } else if replies.is_empty() {
            Event::Receive(incoming.next().await)
        } else {
            let receive = incoming.next();
            let response = replies.next();
            futures_util::pin_mut!(receive, response);
            match future::select(receive, response).await {
                Either::Left((received, _)) => Event::Receive(received),
                Either::Right((response, _)) => Event::Response(response),
            }
        };

        match event {
            Event::Receive(Some(Ok(selected))) if !selected.is_empty() => {
                pending.extend_from_slice(selected.as_slice());
            }
            Event::Receive(Some(Ok(_)) | None) => {
                if !pending.is_empty() {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                }
                peer_eof = true;
            }
            Event::Receive(Some(Err(error))) => return Err(error),
            Event::Response(response) => {
                let response = response?;
                close_ready |= response.action == ConnectionAction::Close;
                output.extend_from_slice(&response.bytes);
            }
        }
    }
}

async fn dispatch_frame(
    frame: DecodedFrame<'_>,
    worker: usize,
    worker_count: usize,
    handler: &MemoryHandler,
    router: &Rc<RouterLocal>,
    replies: &ConnectionReplies,
) -> io::Result<bool> {
    let DecodedFrame::Request(request) = frame else {
        let mut bytes = Vec::new();
        let action = handler
            .handle(frame, &mut ResponseEncoder::new(&mut bytes))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        replies.push_ready(CommandResponse { action, bytes })?;
        return Ok(false);
    };

    let header = request.header();
    let command = request.command();
    let closes = command == Command::Quit;
    if matches!(command, Command::Stat { .. }) {
        // Local shard statistics would be observably incomplete. Until an
        // explicit aggregation protocol exists, sharded STAT is unsupported.
        let mut bytes = Vec::new();
        ResponseEncoder::new(&mut bytes)
            .append_empty(header, Status::UnknownCommand)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        replies.push_ready(CommandResponse {
            action: ConnectionAction::Continue,
            bytes,
        })?;
        return Ok(false);
    }

    let Some(key) = command_key(command) else {
        let mut bytes = Vec::new();
        let action = handler
            .execute_command(header, command, &mut ResponseEncoder::new(&mut bytes))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        replies.push_ready(CommandResponse { action, bytes })?;
        return Ok(closes);
    };
    let owner = key_owner(key, worker_count);
    if owner == worker {
        let mut bytes = Vec::new();
        let action = handler
            .execute_command(header, command, &mut ResponseEncoder::new(&mut bytes))
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        replies.push_ready(CommandResponse { action, bytes })?;
        return Ok(closes);
    }

    // Acquire before copying key/value bytes. If the pair has exhausted its
    // credits, this task stops polling the multishot stream and owns no remote
    // request body. The already-armed kernel receive is bounded by the fixed
    // per-worker provided-buffer ring; it is not an unbounded owned backlog.
    let permit = router.acquire(owner).await?;
    let command = OwnedCommand::copy_from(command).expect("keyed command must be ownable");
    router.submit_remote(replies, permit, worker, owner, header, command)?;
    Ok(closes)
}

fn command_key(command: Command<'_>) -> Option<&[u8]> {
    match command {
        Command::Get { key, .. } | Command::Set { key, .. } | Command::Delete { key, .. } => {
            Some(key)
        }
        Command::Noop
        | Command::Version
        | Command::Stat { .. }
        | Command::Quit
        | Command::Unknown => None,
    }
}

async fn send_all(socket: &TcpSocket, buffer: Vec<u8>) -> io::Result<Vec<u8>> {
    let mut cursor = BufCursor::new(buffer);
    while cursor.bytes_init() != 0 {
        let (result, returned) = socket.send(cursor).await;
        cursor = returned;
        let sent = result?;
        if sent == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        cursor.consume(sent);
    }
    Ok(cursor.into_inner())
}

async fn yield_once() {
    YieldOnce(false).await;
}

struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn invalid_input(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

type OwnedKey = SmallVec<[u8; INLINE_KEY_LEN]>;

#[derive(Debug)]
enum OwnedCommand {
    Get {
        key: OwnedKey,
        quiet: bool,
    },
    Set {
        key: OwnedKey,
        flags: u32,
        value: Vec<u8>,
        quiet: bool,
    },
    Delete {
        key: OwnedKey,
        quiet: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestId {
    connection: u64,
    sequence: u64,
}

#[derive(Debug)]
struct ShardRequest {
    origin: usize,
    id: RequestId,
    header: RequestHeader,
    command: OwnedCommand,
}

#[derive(Debug)]
struct CommandResponse {
    action: ConnectionAction,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct ShardResponse {
    owner: usize,
    id: RequestId,
    result: Result<CommandResponse, RemoteError>,
}

#[derive(Debug, Clone)]
enum RemoteError {
    ChannelClosed,
    Encoding(String),
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ChannelClosed => formatter.write_str("shard channel closed"),
            Self::Encoding(message) => {
                write!(formatter, "shard response encoding failed: {message}")
            }
        }
    }
}

impl std::error::Error for RemoteError {}

impl From<RemoteError> for io::Error {
    fn from(error: RemoteError) -> Self {
        io::Error::new(io::ErrorKind::BrokenPipe, error)
    }
}

#[derive(Debug, Clone, Copy)]
enum Control {
    Start,
    Stop,
}

struct WorkerChannels {
    driver: DriverBuilder,
    request_txs: Vec<ShardedSender<ShardRequest>>,
    request_rx: DetachedShardedReceiver<ShardRequest>,
    response_txs: Vec<ShardedSender<ShardResponse>>,
    response_rx: DetachedShardedReceiver<ShardResponse>,
    control_rx: DetachedReceiver<Control>,
}

fn build_topology(
    workers: usize,
    pair_capacity: usize,
) -> (Vec<WorkerChannels>, Vec<Sender<Control>>) {
    assert!(workers != 0, "worker count must be non-zero");
    assert!(pair_capacity != 0, "pair capacity must be non-zero");
    let total_capacity = workers
        .checked_mul(pair_capacity)
        .expect("sharded channel capacity overflow");

    let drivers: Vec<_> = (0..workers).map(|_| DriverBuilder::new()).collect();
    let mut request_rows: Vec<Vec<_>> = (0..workers).map(|_| Vec::with_capacity(workers)).collect();
    let mut request_rxs = Vec::with_capacity(workers);
    for driver in &drivers {
        let (senders, receiver) = mpsc::bounded_sharded(driver.endpoint(), total_capacity, workers);
        assert_eq!(receiver.capacity(), total_capacity);
        assert_eq!(receiver.lanes(), workers);
        for (origin, sender) in senders.into_iter().enumerate() {
            assert_eq!(sender.capacity(), pair_capacity);
            request_rows[origin].push(sender);
        }
        request_rxs.push(receiver);
    }

    let mut response_rows: Vec<Vec<_>> =
        (0..workers).map(|_| Vec::with_capacity(workers)).collect();
    let mut response_rxs = Vec::with_capacity(workers);
    for driver in &drivers {
        let (senders, receiver) = mpsc::bounded_sharded(driver.endpoint(), total_capacity, workers);
        assert_eq!(receiver.capacity(), total_capacity);
        assert_eq!(receiver.lanes(), workers);
        for (owner, sender) in senders.into_iter().enumerate() {
            assert_eq!(sender.capacity(), pair_capacity);
            response_rows[owner].push(sender);
        }
        response_rxs.push(receiver);
    }

    let mut controls = Vec::with_capacity(workers);
    let mut control_rxs = Vec::with_capacity(workers);
    for driver in &drivers {
        let (sender, receiver) = mpsc::bounded(driver.endpoint(), 2);
        controls.push(sender);
        control_rxs.push(receiver);
    }

    let workers = drivers
        .into_iter()
        .zip(request_rows)
        .zip(request_rxs)
        .zip(response_rows)
        .zip(response_rxs)
        .zip(control_rxs)
        .map(
            |(((((driver, request_txs), request_rx), response_txs), response_rx), control_rx)| {
                WorkerChannels {
                    driver,
                    request_txs,
                    request_rx,
                    response_txs,
                    response_rx,
                    control_rx,
                }
            },
        )
        .collect();

    (workers, controls)
}

struct PeerCredits {
    available: usize,
    capacity: usize,
    next_waiter: u64,
    waiters: VecDeque<(u64, Waker)>,
}

impl PeerCredits {
    fn new(capacity: usize) -> Self {
        Self {
            available: capacity,
            capacity,
            next_waiter: 0,
            waiters: VecDeque::new(),
        }
    }
}

enum ReplySlot {
    Vacant,
    Ready {
        sequence: u64,
        result: Result<CommandResponse, RemoteError>,
    },
    RemotePending {
        sequence: u64,
        owner: usize,
    },
}

// One fixed reorder window replaces per-command futures and request maps. The
// active connection owns admission and ordered draining; the router retains
// this allocation only while reverse responses may still need to return pair
// credits after disconnect.
struct ReplyWindow {
    connection: u64,
    head_sequence: u64,
    len: usize,
    remote_outstanding: usize,
    attached: bool,
    closed: bool,
    slots: [ReplySlot; MAX_IN_FLIGHT_PER_CONNECTION],
    waker: Option<Waker>,
}

impl ReplyWindow {
    fn new(connection: u64) -> Self {
        Self {
            connection,
            head_sequence: 0,
            len: 0,
            remote_outstanding: 0,
            attached: true,
            closed: false,
            slots: std::array::from_fn(|_| ReplySlot::Vacant),
            waker: None,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn tail_sequence(&self) -> io::Result<u64> {
        let sequence = self
            .head_sequence
            .checked_add(self.len as u64)
            .ok_or_else(|| io::Error::other("connection sequence space exhausted"))?;
        if sequence == u64::MAX {
            return Err(io::Error::other("connection sequence space exhausted"));
        }
        Ok(sequence)
    }

    fn push_ready(&mut self, response: CommandResponse) -> io::Result<()> {
        self.ensure_admission()?;
        let sequence = self.tail_sequence()?;
        let index = slot_index(sequence);
        assert!(
            matches!(self.slots[index], ReplySlot::Vacant),
            "reply window reused an occupied slot"
        );
        self.slots[index] = ReplySlot::Ready {
            sequence,
            result: Ok(response),
        };
        self.len += 1;
        Ok(())
    }

    fn reserve_remote(&mut self, owner: usize) -> io::Result<RequestId> {
        self.ensure_admission()?;
        let sequence = self.tail_sequence()?;
        let index = slot_index(sequence);
        assert!(
            matches!(self.slots[index], ReplySlot::Vacant),
            "reply window reused an occupied slot"
        );
        self.slots[index] = ReplySlot::RemotePending { sequence, owner };
        self.len += 1;
        self.remote_outstanding += 1;
        Ok(RequestId {
            connection: self.connection,
            sequence,
        })
    }

    fn rollback_remote(&mut self, id: RequestId, owner: usize) {
        let tail = self
            .tail_sequence()
            .expect("reserved reply sequence overflowed");
        assert_eq!(id.connection, self.connection);
        assert_eq!(tail.checked_sub(1), Some(id.sequence));
        let index = slot_index(id.sequence);
        assert!(matches!(
            self.slots[index],
            ReplySlot::RemotePending {
                sequence,
                owner: slot_owner,
            } if sequence == id.sequence && slot_owner == owner
        ));
        self.slots[index] = ReplySlot::Vacant;
        self.len -= 1;
        self.remote_outstanding -= 1;
    }

    fn try_pop(&mut self) -> Option<Result<CommandResponse, RemoteError>> {
        if self.len == 0 {
            return None;
        }
        let index = slot_index(self.head_sequence);
        let slot = std::mem::replace(&mut self.slots[index], ReplySlot::Vacant);
        match slot {
            ReplySlot::Ready { sequence, result } => {
                assert_eq!(sequence, self.head_sequence);
                self.head_sequence = self
                    .head_sequence
                    .checked_add(1)
                    .expect("connection sequence space exhausted");
                self.len -= 1;
                Some(result)
            }
            slot => {
                self.slots[index] = slot;
                None
            }
        }
    }

    fn complete(
        &mut self,
        id: RequestId,
        owner: usize,
        result: Result<CommandResponse, RemoteError>,
    ) -> io::Result<Option<Waker>> {
        if id.connection != self.connection {
            return Err(invalid_response(
                "response used the wrong connection identity",
            ));
        }
        let index = slot_index(id.sequence);
        match self.slots[index] {
            ReplySlot::RemotePending {
                sequence,
                owner: expected_owner,
            } if sequence == id.sequence && expected_owner == owner => {}
            _ => {
                return Err(invalid_response(
                    "duplicate response or response for the wrong reply slot",
                ));
            }
        }

        self.remote_outstanding -= 1;
        if self.attached && !self.closed {
            self.slots[index] = ReplySlot::Ready {
                sequence: id.sequence,
                result,
            };
        } else {
            self.slots[index] = ReplySlot::Vacant;
        }
        Ok((self.attached && id.sequence == self.head_sequence)
            .then(|| self.waker.take())
            .flatten())
    }

    fn poll_next(&mut self, context: &mut Context<'_>) -> Poll<io::Result<CommandResponse>> {
        if let Some(result) = self.try_pop() {
            self.waker = None;
            return Poll::Ready(result.map_err(io::Error::from));
        }
        if self.closed {
            self.waker = None;
            return Poll::Ready(Err(io::Error::from(RemoteError::ChannelClosed)));
        }
        if self.len != 0 {
            let index = slot_index(self.head_sequence);
            assert!(matches!(
                self.slots[index],
                ReplySlot::RemotePending { sequence, .. } if sequence == self.head_sequence
            ));
        }
        if self
            .waker
            .as_ref()
            .is_none_or(|waker| !waker.will_wake(context.waker()))
        {
            self.waker = Some(context.waker().clone());
        }
        Poll::Pending
    }

    fn ensure_admission(&self) -> io::Result<()> {
        if !self.attached || self.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "connection reply window is closed",
            ));
        }
        if self.len == MAX_IN_FLIGHT_PER_CONNECTION {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "connection reply window is full",
            ));
        }
        Ok(())
    }

    fn detach(&mut self) -> bool {
        self.attached = false;
        self.waker = None;
        for slot in &mut self.slots {
            if matches!(slot, ReplySlot::Ready { .. }) {
                *slot = ReplySlot::Vacant;
            }
        }
        // `len` intentionally remains unchanged after detach: this window can
        // no longer admit or drain replies. Only the surviving RemotePending
        // tombstones and `remote_outstanding` govern late-response validation,
        // credit return, and registry removal.
        self.remote_outstanding == 0
    }

    fn close(&mut self) -> Option<Waker> {
        self.closed = true;
        self.waker.take()
    }

    fn removable(&self) -> bool {
        !self.attached && self.remote_outstanding == 0
    }
}

fn slot_index(sequence: u64) -> usize {
    sequence as usize % MAX_IN_FLIGHT_PER_CONNECTION
}

fn invalid_response(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct ConnectionReplies {
    router: Rc<RouterLocal>,
    connection: u64,
    window: Rc<RefCell<ReplyWindow>>,
}

impl ConnectionReplies {
    fn len(&self) -> usize {
        self.window.borrow().len()
    }

    fn is_empty(&self) -> bool {
        self.window.borrow().is_empty()
    }

    fn push_ready(&self, response: CommandResponse) -> io::Result<()> {
        self.window.borrow_mut().push_ready(response)
    }

    fn reserve_remote(&self, owner: usize) -> io::Result<RequestId> {
        self.window.borrow_mut().reserve_remote(owner)
    }

    fn rollback_remote(&self, id: RequestId, owner: usize) {
        self.window.borrow_mut().rollback_remote(id, owner);
    }

    fn try_next(&self) -> Option<io::Result<CommandResponse>> {
        self.window
            .borrow_mut()
            .try_pop()
            .map(|result| result.map_err(io::Error::from))
    }

    fn next(&self) -> NextReply<'_> {
        NextReply {
            window: &self.window,
        }
    }
}

impl Drop for ConnectionReplies {
    fn drop(&mut self) {
        self.router.detach_connection(self.connection, &self.window);
    }
}

struct NextReply<'a> {
    window: &'a RefCell<ReplyWindow>,
}

impl Future for NextReply<'_> {
    type Output = io::Result<CommandResponse>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.window.borrow_mut().poll_next(context)
    }
}

struct RouterLocal {
    request_txs: Vec<ShardedSender<ShardRequest>>,
    fatal: Sender<String>,
    stopping: Arc<AtomicBool>,
    credits: Vec<RefCell<PeerCredits>>,
    connections: RefCell<HashMap<u64, Rc<RefCell<ReplyWindow>>>>,
    closed: Cell<bool>,
}

impl RouterLocal {
    fn new(
        request_txs: Vec<ShardedSender<ShardRequest>>,
        pair_capacity: usize,
        fatal: Sender<String>,
        stopping: Arc<AtomicBool>,
    ) -> Rc<Self> {
        assert!(pair_capacity != 0);
        let credits = request_txs
            .iter()
            .map(|sender| {
                assert_eq!(sender.capacity(), pair_capacity);
                RefCell::new(PeerCredits::new(pair_capacity))
            })
            .collect();
        Rc::new(Self {
            request_txs,
            fatal,
            stopping,
            credits,
            connections: RefCell::new(HashMap::new()),
            closed: Cell::new(false),
        })
    }

    fn register_connection(self: &Rc<Self>, connection: u64) -> io::Result<ConnectionReplies> {
        if self.closed.get() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "shard router is closed",
            ));
        }
        let window = Rc::new(RefCell::new(ReplyWindow::new(connection)));
        let mut connections = self.connections.borrow_mut();
        if connections.contains_key(&connection) {
            return Err(invalid_response("duplicate connection identity"));
        }
        connections.insert(connection, Rc::clone(&window));
        drop(connections);
        Ok(ConnectionReplies {
            router: Rc::clone(self),
            connection,
            window,
        })
    }

    fn acquire(self: &Rc<Self>, peer: usize) -> AcquireCredit {
        assert!(peer < self.credits.len());
        AcquireCredit {
            router: Rc::clone(self),
            peer,
            waiter: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_remote(
        self: &Rc<Self>,
        replies: &ConnectionReplies,
        mut permit: CreditPermit,
        origin: usize,
        peer: usize,
        header: RequestHeader,
        command: OwnedCommand,
    ) -> io::Result<()> {
        assert_eq!(permit.peer, peer);
        assert!(Rc::ptr_eq(&permit.router, self));
        assert!(Rc::ptr_eq(&replies.router, self));
        if self.closed.get() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "shard router closed before request submission",
            ));
        }
        let id = replies.reserve_remote(peer)?;
        let request = ShardRequest {
            origin,
            id,
            header,
            command,
        };

        match self.request_txs[peer].try_send(request) {
            Ok(()) => {
                permit.commit();
                Ok(())
            }
            Err(TrySendError::Full(_)) => {
                replies.rollback_remote(id, peer);
                self.fail("shard request lane filled despite an outstanding credit");
                Err(io::Error::other(
                    "shard request lane filled despite an outstanding credit",
                ))
            }
            Err(TrySendError::Closed(_)) => {
                replies.rollback_remote(id, peer);
                self.fail("shard request channel closed");
                Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "shard request channel closed",
                ))
            }
        }
    }

    fn complete(&self, response: ShardResponse) -> io::Result<()> {
        if response.owner >= self.credits.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "shard response named an unknown owner",
            ));
        }

        let window = {
            let connections = self.connections.borrow();
            connections.get(&response.id.connection).cloned()
        }
        .ok_or_else(|| invalid_response("response used an unknown connection identity"))?;
        let (waker, remove) = {
            let mut window = window.borrow_mut();
            let waker = window.complete(response.id, response.owner, response.result)?;
            (waker, window.removable())
        };

        // A credit is returned only after the unique pending slot validates
        // the response dequeued by this origin worker.
        self.release(response.owner);
        if remove {
            let mut connections = self.connections.borrow_mut();
            if connections
                .get(&response.id.connection)
                .is_some_and(|candidate| Rc::ptr_eq(candidate, &window))
            {
                connections.remove(&response.id.connection);
            }
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    fn detach_connection(&self, connection: u64, window: &Rc<RefCell<ReplyWindow>>) {
        let remove = window.borrow_mut().detach();
        if remove {
            let mut connections = self.connections.borrow_mut();
            if connections
                .get(&connection)
                .is_some_and(|candidate| Rc::ptr_eq(candidate, window))
            {
                connections.remove(&connection);
            }
        }
    }

    fn release(&self, peer: usize) {
        let waker = {
            let mut credits = self.credits[peer].borrow_mut();
            assert!(
                credits.available < credits.capacity,
                "shard response returned an unowned credit"
            );
            credits.available += 1;
            credits.waiters.front().map(|(_, waker)| waker.clone())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn cancel_waiter(&self, peer: usize, waiter: u64) {
        let waker = {
            let mut credits = self.credits[peer].borrow_mut();
            let Some(position) = credits
                .waiters
                .iter()
                .position(|(candidate, _)| *candidate == waiter)
            else {
                return;
            };
            let was_front = position == 0;
            credits.waiters.remove(position);
            (was_front && credits.available != 0)
                .then(|| credits.waiters.front().map(|(_, waker)| waker.clone()))
                .flatten()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn close(&self) {
        if self.closed.replace(true) {
            return;
        }

        let mut wakers = Vec::new();
        for credits in &self.credits {
            wakers.extend(
                credits
                    .borrow_mut()
                    .waiters
                    .drain(..)
                    .map(|(_, waker)| waker),
            );
        }
        let windows: Vec<_> = self.connections.borrow().values().cloned().collect();
        for window in windows {
            if let Some(waker) = window.borrow_mut().close() {
                wakers.push(waker);
            }
        }
        for waker in wakers {
            waker.wake();
        }
    }

    fn fail(&self, message: &str) {
        self.close();
        if !self.stopping.swap(true, Ordering::AcqRel) {
            let _ = self.fatal.try_send(message.to_owned());
        }
    }
}

struct AcquireCredit {
    router: Rc<RouterLocal>,
    peer: usize,
    waiter: Option<u64>,
}

impl Future for AcquireCredit {
    type Output = io::Result<CreditPermit>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.router.closed.get() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "shard response channel closed",
            )));
        }

        let mut acquired = false;
        let mut assigned_waiter = self.waiter;
        let mut next_waker = None;
        {
            let mut credits = self.router.credits[self.peer].borrow_mut();
            let owns_front = assigned_waiter
                .is_some_and(|waiter| credits.waiters.front().map(|(id, _)| *id) == Some(waiter));
            if credits.available != 0 && (credits.waiters.is_empty() || owns_front) {
                if owns_front {
                    credits.waiters.pop_front();
                }
                credits.available -= 1;
                acquired = true;
                if credits.available != 0 {
                    next_waker = credits.waiters.front().map(|(_, waker)| waker.clone());
                }
            } else if let Some(waiter) = assigned_waiter {
                let (_, waker) = credits
                    .waiters
                    .iter_mut()
                    .find(|(id, _)| *id == waiter)
                    .expect("registered credit waiter disappeared");
                if !waker.will_wake(context.waker()) {
                    *waker = context.waker().clone();
                }
            } else {
                let waiter = credits.next_waiter;
                credits.next_waiter = credits.next_waiter.wrapping_add(1);
                credits.waiters.push_back((waiter, context.waker().clone()));
                assigned_waiter = Some(waiter);
            }
        }

        self.waiter = assigned_waiter;
        if let Some(waker) = next_waker {
            waker.wake();
        }
        if acquired {
            self.waiter = None;
            Poll::Ready(Ok(CreditPermit {
                router: Rc::clone(&self.router),
                peer: self.peer,
                committed: false,
            }))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for AcquireCredit {
    fn drop(&mut self) {
        if let Some(waiter) = self.waiter.take() {
            self.router.cancel_waiter(self.peer, waiter);
        }
    }
}

struct CreditPermit {
    router: Rc<RouterLocal>,
    peer: usize,
    committed: bool,
}

impl CreditPermit {
    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for CreditPermit {
    fn drop(&mut self) {
        if !self.committed {
            self.router.release(self.peer);
        }
    }
}

impl OwnedCommand {
    fn copy_from(command: Command<'_>) -> Option<Self> {
        match command {
            Command::Get { key, quiet } => Some(Self::Get {
                key: SmallVec::from_slice(key),
                quiet,
            }),
            Command::Set {
                key,
                flags,
                value,
                quiet,
            } => Some(Self::Set {
                key: SmallVec::from_slice(key),
                flags,
                value: value.to_vec(),
                quiet,
            }),
            Command::Delete { key, quiet } => Some(Self::Delete {
                key: SmallVec::from_slice(key),
                quiet,
            }),
            Command::Noop
            | Command::Version
            | Command::Stat { .. }
            | Command::Quit
            | Command::Unknown => None,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::Get { key, .. } | Self::Set { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    fn borrowed(&self) -> Command<'_> {
        match self {
            Self::Get { key, quiet } => Command::Get { key, quiet: *quiet },
            Self::Set {
                key,
                flags,
                value,
                quiet,
            } => Command::Set {
                key,
                flags: *flags,
                value,
                quiet: *quiet,
            },
            Self::Delete { key, quiet } => Command::Delete { key, quiet: *quiet },
        }
    }
}

fn key_owner(key: &[u8], workers: usize) -> usize {
    debug_assert!(workers != 0);
    (key_hash(key) % workers as u64) as usize
}

fn key_hash(key: &[u8]) -> u64 {
    // FNV-1a is explicit so ownership is stable across workers and processes.
    key.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::task::Wake;
    use std::time::Duration;

    use futures_util::task::noop_waker;
    use norn_executor::park::SpinPark;

    use super::*;
    use crate::protocol::{
        OP_GET, OP_NOOP, OP_QUIT, OP_SETQ, OP_STAT, REQUEST_MAGIC, RESPONSE_MAGIC,
    };

    fn header(opcode: u8, key_len: usize, extras_len: usize, body_len: usize) -> RequestHeader {
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[0] = REQUEST_MAGIC;
        bytes[1] = opcode;
        bytes[2..4].copy_from_slice(&(key_len as u16).to_be_bytes());
        bytes[4] = extras_len as u8;
        bytes[8..12].copy_from_slice(&(body_len as u32).to_be_bytes());
        RequestHeader::decode(&bytes).unwrap()
    }

    fn id(sequence: u64) -> RequestId {
        RequestId {
            connection: 7,
            sequence,
        }
    }

    fn shard_request(sequence: u64, key: &[u8]) -> ShardRequest {
        ShardRequest {
            origin: 0,
            id: id(sequence),
            header: header(OP_GET, key.len(), 0, key.len()),
            command: OwnedCommand::Get {
                key: SmallVec::from_slice(key),
                quiet: false,
            },
        }
    }

    fn response_for(connection: u64, sequence: u64, owner: usize, bytes: Vec<u8>) -> ShardResponse {
        ShardResponse {
            owner,
            id: RequestId {
                connection,
                sequence,
            },
            result: Ok(CommandResponse {
                action: ConnectionAction::Continue,
                bytes,
            }),
        }
    }

    fn response(sequence: u64, owner: usize, bytes: Vec<u8>) -> ShardResponse {
        response_for(7, sequence, owner, bytes)
    }

    fn router_fixture(
        capacity: usize,
    ) -> (
        Rc<RouterLocal>,
        DetachedShardedReceiver<ShardRequest>,
        DetachedReceiver<String>,
        DriverBuilder,
    ) {
        let driver = DriverBuilder::new();
        let (senders, requests) = mpsc::bounded_sharded(driver.endpoint(), capacity, 1);
        let (fatal, fatal_rx) = mpsc::bounded(driver.endpoint(), 4);
        let router = RouterLocal::new(senders, capacity, fatal, Arc::new(AtomicBool::new(false)));
        (router, requests, fatal_rx, driver)
    }

    fn acquire_now(router: &Rc<RouterLocal>) -> CreditPermit {
        router
            .acquire(0)
            .now_or_never()
            .expect("available credit remained pending")
            .unwrap()
    }

    fn submit_get(
        router: &Rc<RouterLocal>,
        replies: &ConnectionReplies,
        permit: CreditPermit,
        key: &[u8],
    ) -> io::Result<()> {
        router.submit_remote(
            replies,
            permit,
            0,
            0,
            header(OP_GET, key.len(), 0, key.len()),
            OwnedCommand::Get {
                key: SmallVec::from_slice(key),
                quiet: false,
            },
        )
    }

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let waker = noop_waker();
        let mut context = Context::from_waker(&waker);
        Pin::new(future).poll(&mut context)
    }

    struct FlagWake(Arc<AtomicBool>);

    impl Wake for FlagWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    fn poll_with_flag<F: Future + Unpin>(
        future: &mut F,
        flag: &Arc<AtomicBool>,
    ) -> Poll<F::Output> {
        let waker = Waker::from(Arc::new(FlagWake(Arc::clone(flag))));
        let mut context = Context::from_waker(&waker);
        Pin::new(future).poll(&mut context)
    }

    #[test]
    fn key_hash_has_fixed_vectors_and_covers_shards() {
        assert_eq!(key_hash(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(key_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(key_hash(b"foobar"), 0x8594_4171_f739_67e8);
        assert_eq!(key_owner(b"a", 2), 0);
        assert_eq!(key_owner(b"b", 2), 1);
    }

    #[test]
    fn common_get_key_stays_inline() {
        let command = OwnedCommand::copy_from(Command::Get {
            key: b"memtier-0000000000000042",
            quiet: false,
        })
        .unwrap();
        let OwnedCommand::Get { key, .. } = command else {
            unreachable!();
        };
        assert!(!key.spilled());
    }

    #[test]
    fn owned_command_round_trips_borrowed_fields() {
        let command = OwnedCommand::copy_from(Command::Set {
            key: b"key",
            flags: 7,
            value: b"value",
            quiet: true,
        })
        .unwrap();
        assert_eq!(command.key(), b"key");
        assert_eq!(
            command.borrowed(),
            Command::Set {
                key: b"key",
                flags: 7,
                value: b"value",
                quiet: true,
            }
        );
    }

    #[test]
    fn cross_thread_envelopes_are_send_owned_values() {
        fn assert_send<T: Send>() {}
        assert_send::<ShardRequest>();
        assert_send::<ShardResponse>();
        assert_send::<OwnedCommand>();
        assert_send::<CommandResponse>();
    }

    #[test]
    fn topology_routes_origin_to_owner_and_back_to_origin() {
        let (topology, _controls) = build_topology(3, 2);
        let mut topology: Vec<_> = topology.into_iter().map(Some).collect();
        let origin = topology[0].take().unwrap();
        let unrelated = topology[1].take().unwrap();
        let owner = topology[2].take().unwrap();

        origin.request_txs[2]
            .try_send(shard_request(1, b"owner-two"))
            .unwrap();
        let WorkerChannels {
            driver,
            request_rx,
            response_txs,
            ..
        } = owner;
        let driver = driver.build(SpinPark);
        let mut request_rx = request_rx.attach(&driver.handle());
        let received = request_rx.try_recv().unwrap();
        assert_eq!(received.origin, 0);
        assert_eq!(received.id, id(1));
        response_txs[0]
            .try_send(response(1, 2, b"reply".to_vec()))
            .unwrap();
        drop(request_rx);
        drop(driver);

        let WorkerChannels {
            driver,
            response_rx,
            ..
        } = origin;
        let driver = driver.build(SpinPark);
        let mut response_rx = response_rx.attach(&driver.handle());
        let received = response_rx.try_recv().unwrap();
        assert_eq!(received.owner, 2);
        assert_eq!(received.id, id(1));
        drop(response_rx);
        drop(driver);

        let WorkerChannels {
            driver,
            response_rx,
            ..
        } = unrelated;
        let driver = driver.build(SpinPark);
        let mut response_rx = response_rx.attach(&driver.handle());
        assert!(matches!(
            response_rx.try_recv(),
            Err(norn_channel::mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn acquired_credit_is_returned_when_router_closes_before_submit() {
        let (router, _requests, _fatal, _driver) = router_fixture(1);
        let replies = router.register_connection(7).unwrap();
        let permit = acquire_now(&router);
        router.close();
        assert!(submit_get(&router, &replies, permit, b"key").is_err());
        assert_eq!(router.credits[0].borrow().available, 1);
        assert!(replies.is_empty());
    }

    #[test]
    fn disconnect_discards_late_quiet_response_and_returns_credit() {
        let (router, _requests, _fatal, _driver) = router_fixture(1);
        let replies = router.register_connection(7).unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"key").unwrap();
        assert_eq!(router.credits[0].borrow().available, 0);
        drop(replies);
        assert_eq!(router.connections.borrow().len(), 1);

        router.complete(response(0, 0, Vec::new())).unwrap();
        assert_eq!(router.credits[0].borrow().available, 1);
        assert!(router.connections.borrow().is_empty());
    }

    #[test]
    fn duplicate_response_does_not_over_return_credit() {
        let (router, _requests, _fatal, _driver) = router_fixture(2);
        let replies = router.register_connection(7).unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"key").unwrap();
        router.complete(response(0, 0, Vec::new())).unwrap();
        assert_eq!(router.credits[0].borrow().available, 2);
        assert!(router.complete(response(0, 0, Vec::new())).is_err());
        assert_eq!(router.credits[0].borrow().available, 2);
        assert!(replies.try_next().unwrap().is_ok());
    }

    #[test]
    fn pair_credit_prevents_request_lane_full() {
        let (router, requests, _fatal, driver) = router_fixture(1);
        let driver = driver.build(SpinPark);
        let mut requests = requests.attach(&driver.handle());
        let replies = router.register_connection(7).unwrap();

        submit_get(&router, &replies, acquire_now(&router), b"one").unwrap();
        assert_eq!(requests.try_recv().unwrap().id, id(0));
        let mut second_credit = router.acquire(0);
        assert!(poll_once(&mut second_credit).is_pending());

        router.complete(response(0, 0, Vec::new())).unwrap();
        assert!(replies.try_next().unwrap().is_ok());
        let second_credit = match poll_once(&mut second_credit) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("returned response credit did not admit next request"),
        };
        submit_get(&router, &replies, second_credit, b"two").unwrap();
        assert_eq!(requests.try_recv().unwrap().id, id(1));
        router.complete(response(1, 0, Vec::new())).unwrap();
        assert!(replies.try_next().unwrap().is_ok());
        assert_eq!(router.credits[0].borrow().available, 1);
    }

    #[test]
    fn reversed_remote_completions_are_drained_in_request_order() {
        let (router, _requests, _fatal, _driver) = router_fixture(2);
        let replies = router.register_connection(7).unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"first").unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"second").unwrap();

        router.complete(response(1, 0, vec![1])).unwrap();
        assert!(replies.try_next().is_none());
        router.complete(response(0, 0, vec![0])).unwrap();

        assert_eq!(replies.try_next().unwrap().unwrap().bytes, vec![0]);
        assert_eq!(replies.try_next().unwrap().unwrap().bytes, vec![1]);
        assert!(replies.is_empty());
        assert_eq!(router.credits[0].borrow().available, 2);
    }

    #[test]
    fn quiet_completion_and_quit_wait_behind_remote_head() {
        let (router, _requests, _fatal, _driver) = router_fixture(1);
        let replies = router.register_connection(7).unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"remote").unwrap();
        replies
            .push_ready(CommandResponse {
                action: ConnectionAction::Continue,
                bytes: Vec::new(),
            })
            .unwrap();
        replies
            .push_ready(CommandResponse {
                action: ConnectionAction::Close,
                bytes: Vec::new(),
            })
            .unwrap();

        assert!(replies.try_next().is_none());
        router.complete(response(0, 0, vec![9])).unwrap();
        let remote = replies.try_next().unwrap().unwrap();
        let quiet = replies.try_next().unwrap().unwrap();
        let quit = replies.try_next().unwrap().unwrap();
        assert_eq!(remote.bytes, vec![9]);
        assert_eq!(quiet.action, ConnectionAction::Continue);
        assert!(quiet.bytes.is_empty());
        assert_eq!(quit.action, ConnectionAction::Close);
        assert!(quit.bytes.is_empty());
    }

    #[test]
    fn reply_window_enforces_maximum_and_reuses_wrapped_slots() {
        let (router, _requests, _fatal, _driver) = router_fixture(1);
        let replies = router.register_connection(7).unwrap();

        for value in 0_u8..32 {
            replies
                .push_ready(CommandResponse {
                    action: ConnectionAction::Continue,
                    bytes: vec![value],
                })
                .unwrap();
        }
        let error = replies
            .push_ready(CommandResponse {
                action: ConnectionAction::Continue,
                bytes: vec![32],
            })
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

        for expected in 0_u8..16 {
            assert_eq!(replies.try_next().unwrap().unwrap().bytes, vec![expected]);
        }
        for value in 32_u8..48 {
            replies
                .push_ready(CommandResponse {
                    action: ConnectionAction::Continue,
                    bytes: vec![value],
                })
                .unwrap();
        }
        for expected in 16_u8..48 {
            assert_eq!(replies.try_next().unwrap().unwrap().bytes, vec![expected]);
        }
        for value in 48_u8..80 {
            replies
                .push_ready(CommandResponse {
                    action: ConnectionAction::Continue,
                    bytes: vec![value],
                })
                .unwrap();
        }
        for expected in 48_u8..80 {
            assert_eq!(replies.try_next().unwrap().unwrap().bytes, vec![expected]);
        }
        assert!(replies.is_empty());
    }

    #[test]
    fn reply_window_rejects_unadvanceable_final_sequence() {
        let mut ready = ReplyWindow::new(7);
        ready.head_sequence = u64::MAX;
        assert!(ready
            .push_ready(CommandResponse {
                action: ConnectionAction::Continue,
                bytes: Vec::new(),
            })
            .is_err());

        let mut remote = ReplyWindow::new(7);
        remote.head_sequence = u64::MAX;
        assert!(remote.reserve_remote(0).is_err());
    }

    #[test]
    fn wrapped_slot_rejects_stale_response_without_returning_credit() {
        let (router, requests, _fatal, driver) = router_fixture(1);
        let driver = driver.build(SpinPark);
        let mut requests = requests.attach(&driver.handle());
        let replies = router.register_connection(7).unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"old").unwrap();
        assert_eq!(requests.try_recv().unwrap().id, id(0));
        router.complete(response(0, 0, vec![0])).unwrap();
        assert_eq!(replies.try_next().unwrap().unwrap().bytes, vec![0]);

        for _ in 1..MAX_IN_FLIGHT_PER_CONNECTION {
            replies
                .push_ready(CommandResponse {
                    action: ConnectionAction::Continue,
                    bytes: Vec::new(),
                })
                .unwrap();
            assert!(replies.try_next().unwrap().is_ok());
        }
        submit_get(&router, &replies, acquire_now(&router), b"new").unwrap();
        assert_eq!(
            requests.try_recv().unwrap().id,
            id(MAX_IN_FLIGHT_PER_CONNECTION as u64)
        );
        assert_eq!(router.credits[0].borrow().available, 0);

        assert!(router.complete(response(0, 0, vec![7])).is_err());
        assert_eq!(router.credits[0].borrow().available, 0);
        router
            .complete(response(MAX_IN_FLIGHT_PER_CONNECTION as u64, 0, vec![8]))
            .unwrap();
        assert_eq!(router.credits[0].borrow().available, 1);
        assert_eq!(replies.try_next().unwrap().unwrap().bytes, vec![8]);
    }

    #[test]
    fn disconnect_tombstone_blocks_connection_id_reuse_until_late_response() {
        let (router, _requests, _fatal, _driver) = router_fixture(1);
        let replies = router.register_connection(7).unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"old").unwrap();
        drop(replies);

        assert!(router.register_connection(7).is_err());
        assert_eq!(router.credits[0].borrow().available, 0);
        router.complete(response(0, 0, Vec::new())).unwrap();
        assert_eq!(router.credits[0].borrow().available, 1);
        assert!(router.register_connection(7).is_ok());
    }

    #[test]
    fn close_wakes_head_waiter_but_late_response_still_returns_credit() {
        let (router, _requests, _fatal, _driver) = router_fixture(1);
        let replies = router.register_connection(7).unwrap();
        submit_get(&router, &replies, acquire_now(&router), b"key").unwrap();
        let woke = Arc::new(AtomicBool::new(false));
        {
            let mut next = replies.next();
            assert!(poll_with_flag(&mut next, &woke).is_pending());
            router.close();
            assert!(woke.load(Ordering::Acquire));
            assert!(matches!(poll_once(&mut next), Poll::Ready(Err(_))));
        }
        assert_eq!(router.credits[0].borrow().available, 0);
        drop(replies);
        assert_eq!(router.connections.borrow().len(), 1);

        router.complete(response(0, 0, Vec::new())).unwrap();
        assert_eq!(router.credits[0].borrow().available, 1);
        assert!(router.connections.borrow().is_empty());
    }

    #[test]
    fn credit_waiters_are_fifo_and_cancellation_advances_the_queue() {
        let (router, _requests, _fatal, _driver) = router_fixture(1);
        let first = acquire_now(&router);
        let mut cancelled = router.acquire(0);
        let mut next = router.acquire(0);
        assert!(poll_once(&mut cancelled).is_pending());
        assert!(poll_once(&mut next).is_pending());
        drop(cancelled);
        drop(first);
        let permit = match poll_once(&mut next) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("next FIFO waiter was not admitted"),
        };
        drop(permit);
        assert_eq!(router.credits[0].borrow().available, 1);
    }

    #[test]
    fn released_credit_batch_cascades_across_fifo_waiters() {
        const CAPACITY: usize = 3;
        let (router, _requests, _fatal, _driver) = router_fixture(CAPACITY);
        let held: Vec<_> = (0..CAPACITY).map(|_| acquire_now(&router)).collect();
        let mut waiters: Vec<_> = (0..CAPACITY).map(|_| router.acquire(0)).collect();
        let flags: Vec<_> = (0..CAPACITY)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();
        for (waiter, flag) in waiters.iter_mut().zip(&flags) {
            assert!(poll_with_flag(waiter, flag).is_pending());
        }

        drop(held);
        assert!(flags[0].load(Ordering::Acquire));
        assert!(!flags[1].load(Ordering::Acquire));
        assert!(!flags[2].load(Ordering::Acquire));

        let mut reacquired = Vec::new();
        for index in 0..CAPACITY {
            assert!(flags[index].load(Ordering::Acquire));
            match poll_with_flag(&mut waiters[index], &flags[index]) {
                Poll::Ready(Ok(permit)) => reacquired.push(permit),
                _ => panic!("woken FIFO waiter {index} did not acquire"),
            }
            if index + 1 < CAPACITY {
                assert!(flags[index + 1].load(Ordering::Acquire));
            }
        }
        assert_eq!(router.credits[0].borrow().available, 0);
        drop(reacquired);
        assert_eq!(router.credits[0].borrow().available, CAPACITY);
    }

    #[test]
    fn close_wakes_credit_waiters_and_request_receiver_close_is_fatal() {
        let (router, requests, _fatal, _driver) = router_fixture(1);
        let replies = router.register_connection(7).unwrap();
        let permit = acquire_now(&router);
        let mut waiter = router.acquire(0);
        assert!(poll_once(&mut waiter).is_pending());
        drop(requests);
        assert!(submit_get(&router, &replies, permit, b"key").is_err());
        assert!(router.closed.get());
        assert!(matches!(poll_once(&mut waiter), Poll::Ready(Err(_))));
        assert!(replies.is_empty());
    }

    #[test]
    fn requested_stop_suppresses_new_router_fatal_notification() {
        let (router, _requests, fatal, driver) = router_fixture(1);
        router.stopping.store(true, Ordering::Release);
        router.fail("expected close while stopping");

        let driver = driver.build(SpinPark);
        let mut fatal = fatal.attach(&driver.handle());
        assert!(matches!(
            fatal.try_recv(),
            Err(norn_channel::mpsc::TryRecvError::Empty)
        ));
    }

    #[test]
    fn validation_rejects_unsupported_worker_shapes() {
        let config = ShardedServerConfig {
            workers: 1,
            ..ShardedServerConfig::default()
        };
        assert!(config.validate().is_err());
        let config = ShardedServerConfig {
            server: ServerConfig {
                recv_mode: RecvMode::Exact,
                ..ServerConfig::default()
            },
            ..ShardedServerConfig::default()
        };
        assert!(config.validate().is_err());
        let config = ShardedServerConfig {
            worker_cpus: vec![0],
            ..ShardedServerConfig::default()
        };
        assert!(config.validate().is_err());
        let config = ShardedServerConfig {
            worker_cpus: vec![0, 0],
            ..ShardedServerConfig::default()
        };
        assert!(config.validate().is_err());
        let config = ShardedServerConfig {
            pair_capacity: usize::MAX / 8 + 1,
            ..ShardedServerConfig::default()
        };
        let error = config.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "aggregate sharded channel capacity overflows usize"
        );
    }

    #[test]
    fn default_pair_capacity_is_1024() {
        assert_eq!(ShardedServerConfig::default().pair_capacity, 1_024);
    }

    fn binary_request(opcode: u8, key: &[u8], extras: &[u8], value: &[u8], opaque: u32) -> Vec<u8> {
        let body_len = extras.len() + key.len() + value.len();
        let mut bytes = Vec::with_capacity(HEADER_LEN + body_len);
        bytes.push(REQUEST_MAGIC);
        bytes.push(opcode);
        bytes.extend_from_slice(&(key.len() as u16).to_be_bytes());
        bytes.push(extras.len() as u8);
        bytes.push(0);
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&(body_len as u32).to_be_bytes());
        bytes.extend_from_slice(&opaque.to_be_bytes());
        bytes.extend_from_slice(&0_u64.to_be_bytes());
        bytes.extend_from_slice(extras);
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        bytes
    }

    fn read_response(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
        let mut header = [0_u8; HEADER_LEN];
        stream.read_exact(&mut header)?;
        assert_eq!(header[0], RESPONSE_MAGIC);
        let body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let mut response = header.to_vec();
        response.resize(HEADER_LEN + body_len, 0);
        stream.read_exact(&mut response[HEADER_LEN..])?;
        Ok(response)
    }

    #[test]
    fn two_worker_loopback_handles_mixed_pipeline_and_unknown_stat() -> io::Result<()> {
        let mut config = ShardedServerConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..ShardedServerConfig::default()
        };
        config.server.max_batch_commands = 1;
        config.pair_capacity = 1;
        let server = ShardedServer::start(config)?;
        let address = server.local_addr();

        let result = (|| -> io::Result<()> {
            let mut stream = TcpStream::connect(address)?;
            stream.set_read_timeout(Some(Duration::from_secs(3)))?;
            stream.set_write_timeout(Some(Duration::from_secs(3)))?;
            let keys = [b"a".as_slice(), b"b".as_slice()];
            assert_eq!(key_owner(keys[0], 2), 0);
            assert_eq!(key_owner(keys[1], 2), 1);
            let mut extras = Vec::new();
            extras.extend_from_slice(&7_u32.to_be_bytes());
            extras.extend_from_slice(&0_u32.to_be_bytes());

            let mut pipeline = Vec::new();
            pipeline.extend(binary_request(OP_SETQ, keys[0], &extras, b"zero", 1));
            pipeline.extend(binary_request(OP_SETQ, keys[1], &extras, b"one", 2));
            pipeline.extend(binary_request(OP_NOOP, &[], &[], &[], 3));
            pipeline.extend(binary_request(OP_GET, keys[0], &[], &[], 4));
            pipeline.extend(binary_request(OP_GET, keys[1], &[], &[], 5));
            pipeline.extend(binary_request(OP_STAT, &[], &[], &[], 6));
            pipeline.extend(binary_request(OP_NOOP, &[], &[], &[], 7));
            stream.write_all(&pipeline)?;

            let noop = read_response(&mut stream)?;
            assert_eq!(u32::from_be_bytes(noop[12..16].try_into().unwrap()), 3);
            for (opaque, value) in [(4_u32, b"zero".as_slice()), (5, b"one".as_slice())] {
                let get = read_response(&mut stream)?;
                assert_eq!(u32::from_be_bytes(get[12..16].try_into().unwrap()), opaque);
                assert_eq!(&get[HEADER_LEN + 4..], value);
            }
            let stat = read_response(&mut stream)?;
            assert_eq!(u32::from_be_bytes(stat[12..16].try_into().unwrap()), 6);
            assert_eq!(
                u16::from_be_bytes(stat[6..8].try_into().unwrap()),
                Status::UnknownCommand as u16
            );
            let final_noop = read_response(&mut stream)?;
            assert_eq!(
                u32::from_be_bytes(final_noop[12..16].try_into().unwrap()),
                7
            );
            Ok(())
        })();

        let shutdown = server.shutdown();
        result?;
        shutdown
    }

    #[test]
    fn two_worker_fixed_response_mode_uses_sharded_request_path() -> io::Result<()> {
        let server = ShardedServer::start(ShardedServerConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            handler: HandlerConfig::FixedResponse { value_len: 64 },
            ..ShardedServerConfig::default()
        })?;
        let address = server.local_addr();

        let result = (|| -> io::Result<()> {
            let mut stream = TcpStream::connect(address)?;
            stream.set_read_timeout(Some(Duration::from_secs(3)))?;
            stream.set_write_timeout(Some(Duration::from_secs(3)))?;
            let keys = [b"a".as_slice(), b"b".as_slice()];
            assert_eq!(key_owner(keys[0], 2), 0);
            assert_eq!(key_owner(keys[1], 2), 1);

            let mut pipeline = Vec::new();
            pipeline.extend(binary_request(OP_GET, keys[0], &[], &[], 1));
            pipeline.extend(binary_request(OP_GET, keys[1], &[], &[], 2));
            stream.write_all(&pipeline)?;

            for opaque in [1_u32, 2] {
                let response = read_response(&mut stream)?;
                assert_eq!(
                    u32::from_be_bytes(response[12..16].try_into().unwrap()),
                    opaque
                );
                assert_eq!(response.len(), HEADER_LEN + 4 + 64);
                assert_eq!(&response[HEADER_LEN..HEADER_LEN + 4], &0_u32.to_be_bytes());
                assert!(response[HEADER_LEN + 4..].iter().all(|&byte| byte == 0x5a));
            }
            Ok(())
        })();

        let shutdown = server.shutdown();
        result?;
        shutdown
    }

    #[test]
    fn sharded_quit_closes_an_open_peer_without_an_explicit_close_race() -> io::Result<()> {
        let server = ShardedServer::start(ShardedServerConfig {
            listen: "127.0.0.1:0".parse().unwrap(),
            ..ShardedServerConfig::default()
        })?;
        let address = server.local_addr();

        let result = (|| -> io::Result<()> {
            let mut stream = TcpStream::connect(address)?;
            stream.set_read_timeout(Some(Duration::from_secs(3)))?;
            stream.set_write_timeout(Some(Duration::from_secs(3)))?;
            stream.write_all(&binary_request(OP_QUIT, &[], &[], &[], 17))?;

            let response = read_response(&mut stream)?;
            assert_eq!(response[1], OP_QUIT);
            assert_eq!(u32::from_be_bytes(response[12..16].try_into().unwrap()), 17);

            let mut trailing = [0; 1];
            assert_eq!(stream.read(&mut trailing)?, 0);
            Ok(())
        })();

        let shutdown = server.shutdown();
        result?;
        shutdown
    }

    #[test]
    fn repeated_shutdown_during_pipelined_work_is_clean() -> io::Result<()> {
        for _ in 0..8 {
            let server = ShardedServer::start(ShardedServerConfig {
                listen: "127.0.0.1:0".parse().unwrap(),
                ..ShardedServerConfig::default()
            })?;
            let mut stream = TcpStream::connect(server.local_addr())?;
            stream.set_write_timeout(Some(Duration::from_secs(3)))?;
            let mut extras = Vec::new();
            extras.extend_from_slice(&0_u32.to_be_bytes());
            extras.extend_from_slice(&0_u32.to_be_bytes());
            let mut pipeline = Vec::new();
            for opaque in 0..256 {
                let key = if opaque % 2 == 0 { b"a" } else { b"b" };
                pipeline.extend(binary_request(OP_SETQ, key, &extras, b"value", opaque));
            }
            pipeline.extend(binary_request(OP_NOOP, &[], &[], &[], 257));
            stream.write_all(&pipeline)?;
            std::thread::sleep(Duration::from_millis(1));
            server.shutdown()?;
        }
        Ok(())
    }
}
