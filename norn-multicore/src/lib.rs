//! Process-local orchestration for fixed groups of Norn runtime threads.
//!
//! `norn-multicore` manages runtime topology and lifecycle without changing
//! Norn's local execution model. Each worker owns one OS thread, one
//! [`norn_executor::LocalExecutor`], and one thread-affine task set. Bounded
//! [`norn_channel`] channels are the explicit data path between workers; tasks
//! and their wakers never migrate between runtime threads.
//!
//! # Topology construction
//!
//! Construction is deliberately split into two phases. First allocate the
//! worker destinations, then build channels against their portable endpoints
//! and install one async main function per worker. [`Builder::start`] attaches
//! every endpoint and releases all worker mains through one startup gate.
//!
//! ```
//! use norn_channel::mpsc;
//! use norn_multicore::{RuntimeGroup, WorkerId};
//!
//! let mut builder = RuntimeGroup::builder(2);
//! let frontend = WorkerId::new(0);
//! let storage = WorkerId::new(1);
//! let (requests, request_rx) =
//!     mpsc::bounded(builder.endpoint(storage).unwrap(), 16);
//! let (responses, response_rx) =
//!     mpsc::bounded(builder.endpoint(frontend).unwrap(), 16);
//!
//! builder
//!     .worker(frontend, move |context| async move {
//!         let mut responses = response_rx.attach(context.channels());
//!         requests.try_send(41).unwrap();
//!         assert_eq!(responses.recv().await, Some(42));
//!     })
//!     .unwrap();
//! builder
//!     .worker(storage, move |context| async move {
//!         let mut requests = request_rx.attach(context.channels());
//!         let request = requests.recv().await.unwrap();
//!         responses.try_send(request + 1).unwrap();
//!     })
//!     .unwrap();
//!
//! builder.start().unwrap().join().unwrap();
//! ```
//!
//! # Shutdown
//!
//! [`RuntimeGroup::request_shutdown`] is cooperative. It resolves every
//! [`WorkerContext::shutdown_requested`] future and wakes the corresponding
//! executor, but worker mains decide how to drain their local services. Use
//! [`RuntimeGroup::shutdown`] to request shutdown and then join every thread.
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    clippy::missing_safety_doc
)]

use std::any::Any;
use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::future::{poll_fn, Future};
use std::io;
use std::pin::{pin, Pin};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};

use norn_channel::{mpsc as channel, DriverBuilder, Endpoint};
use norn_executor::park::{Park, ThreadPark};
use norn_executor::LocalExecutor;

/// A boxed worker error that is safe to return to the group owner.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// The stable, process-local identity of one runtime worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerId(usize);

impl WorkerId {
    /// Construct an identity from its zero-based worker index.
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    /// Return the zero-based worker index.
    pub const fn index(self) -> usize {
        self.0
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

mod output {
    use super::BoxError;
    use std::error::Error;

    pub trait Sealed {}

    impl Sealed for () {}

    impl<E> Sealed for Result<(), E> where E: Error + Send + Sync + 'static {}

    /// Converts an async worker main's output into its lifecycle result.
    ///
    /// This trait is sealed and implemented for `()` and `Result<(), E>`.
    pub trait WorkerOutput: Sealed {
        /// Convert the worker output into a type-erased result.
        fn into_result(self) -> Result<(), BoxError>;
    }

    impl WorkerOutput for () {
        fn into_result(self) -> Result<(), BoxError> {
            Ok(())
        }
    }

    impl<E> WorkerOutput for Result<(), E>
    where
        E: Error + Send + Sync + 'static,
    {
        fn into_result(self) -> Result<(), BoxError> {
            self.map_err(|error| Box::new(error) as BoxError)
        }
    }
}

#[doc(hidden)]
pub use output::WorkerOutput;

#[derive(Debug)]
struct ShutdownRemote {
    requested: AtomicBool,
    signal: channel::Sender<()>,
}

impl ShutdownRemote {
    fn new(signal: channel::Sender<()>) -> Self {
        Self {
            requested: AtomicBool::new(false),
            signal,
        }
    }

    fn request(&self) {
        if self.requested.swap(true, Ordering::AcqRel) {
            return;
        }

        let _ = self.signal.try_send(());
    }

    fn is_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
struct ShutdownState {
    remote: Arc<ShutdownRemote>,
    wakers: RefCell<Vec<Waker>>,
}

impl ShutdownState {
    fn new(remote: Arc<ShutdownRemote>) -> Self {
        Self {
            remote,
            wakers: RefCell::new(Vec::new()),
        }
    }

    fn deliver(&self) {
        let wakers = std::mem::take(&mut *self.wakers.borrow_mut());
        for waker in wakers {
            waker.wake();
        }
    }

    fn poll_requested(&self, cx: &Context<'_>) -> Poll<()> {
        if self.remote.is_requested() {
            return Poll::Ready(());
        }

        let mut wakers = self.wakers.borrow_mut();
        if self.remote.is_requested() {
            return Poll::Ready(());
        }
        if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
            wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
}

#[derive(Debug)]
struct GroupControl {
    workers: Vec<Arc<ShutdownRemote>>,
}

impl GroupControl {
    fn request_all(&self) {
        for worker in &self.workers {
            worker.request();
        }
    }
}

/// A future that resolves when group shutdown has been requested.
#[derive(Debug)]
#[must_use = "shutdown requests are observed only when this future is awaited or polled"]
pub struct ShutdownRequested {
    state: Rc<ShutdownState>,
}

impl Future for ShutdownRequested {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.state.poll_requested(cx)
    }
}

/// Destination-local context passed to one worker's async main function.
///
/// This value is deliberately not [`Send`], because its channel handle stores
/// receiver wakers that belong to the current runtime thread.
#[derive(Clone, Debug)]
pub struct WorkerContext {
    id: WorkerId,
    channels: norn_channel::Handle,
    control: Arc<GroupControl>,
    shutdown: Rc<ShutdownState>,
}

impl WorkerContext {
    /// Return this worker's stable identity.
    pub const fn id(&self) -> WorkerId {
        self.id
    }

    /// Return the destination-local handle used to attach channel receivers.
    pub fn channels(&self) -> &norn_channel::Handle {
        &self.channels
    }

    /// Return whether group shutdown has already been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.control.workers[self.id.index()].is_requested()
    }

    /// Return a future that resolves when group shutdown is requested.
    pub fn shutdown_requested(&self) -> ShutdownRequested {
        ShutdownRequested {
            state: Rc::clone(&self.shutdown),
        }
    }

    /// Request cooperative shutdown of every worker in the group.
    pub fn request_shutdown(&self) {
        self.control.request_all();
    }
}

/// The reason one worker failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerFailureKind {
    /// Constructing the worker's park stack failed before startup.
    Startup,
    /// The park stack returned an error while running the executor.
    Park,
    /// The worker's async main function returned an error.
    Application,
    /// Worker setup or execution panicked.
    Panic,
}

/// A structured failure returned by one runtime worker.
pub struct WorkerFailure {
    worker: WorkerId,
    kind: WorkerFailureKind,
    message: String,
    source: Option<BoxError>,
}

impl WorkerFailure {
    fn error(worker: WorkerId, kind: WorkerFailureKind, source: BoxError) -> Self {
        Self {
            worker,
            kind,
            message: source.to_string(),
            source: Some(source),
        }
    }

    fn panic(worker: WorkerId, panic: Box<dyn Any + Send + 'static>) -> Self {
        Self {
            worker,
            kind: WorkerFailureKind::Panic,
            message: panic_message(&*panic),
            source: None,
        }
    }

    /// Return the failed worker's identity.
    pub const fn worker(&self) -> WorkerId {
        self.worker
    }

    /// Return the failure category.
    pub const fn kind(&self) -> WorkerFailureKind {
        self.kind
    }
}

impl fmt::Debug for WorkerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkerFailure")
            .field("worker", &self.worker)
            .field("kind", &self.kind)
            .field("message", &self.message)
            .finish()
    }
}

impl fmt::Display for WorkerFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "worker {} {:?} failure: {}",
            self.worker, self.kind, self.message
        )
    }
}

impl Error for WorkerFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|error| error as &(dyn Error + 'static))
    }
}

/// One or more failures observed while joining a runtime group.
#[derive(Debug)]
pub struct JoinError {
    failures: Vec<WorkerFailure>,
}

impl JoinError {
    /// Return all worker failures in ascending worker order.
    pub fn failures(&self) -> &[WorkerFailure] {
        &self.failures
    }

    /// Consume the error and return its worker failures.
    pub fn into_failures(self) -> Vec<WorkerFailure> {
        self.failures
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} runtime worker(s) failed", self.failures.len())
    }
}

impl Error for JoinError {}

/// An error configuring one worker slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigureError {
    /// The worker identity is outside this builder's fixed topology.
    UnknownWorker(WorkerId),
    /// The worker already has an async main function.
    AlreadyConfigured(WorkerId),
}

impl fmt::Display for ConfigureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownWorker(worker) => write!(f, "worker {worker} is outside this topology"),
            Self::AlreadyConfigured(worker) => write!(f, "worker {worker} is already configured"),
        }
    }
}

impl Error for ConfigureError {}

/// An error starting a runtime group.
#[derive(Debug)]
pub enum StartError {
    /// One or more worker slots have no async main function.
    Unconfigured(Vec<WorkerId>),
    /// The operating system refused to create a worker thread.
    Spawn {
        /// The worker whose thread could not be created.
        worker: WorkerId,
        /// The underlying thread creation error.
        source: io::Error,
    },
    /// At least one worker failed before the atomic startup gate opened.
    Worker(JoinError),
}

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unconfigured(workers) => {
                write!(f, "{} worker(s) are unconfigured", workers.len())
            }
            Self::Spawn { worker, source } => {
                write!(f, "failed to spawn worker {worker}: {source}")
            }
            Self::Worker(error) => write!(f, "worker startup failed: {error}"),
        }
    }
}

impl Error for StartError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Spawn { source, .. } => Some(source),
            Self::Worker(source) => Some(source),
            Self::Unconfigured(_) => None,
        }
    }
}

enum StartDecision {
    Pending,
    Run,
    Abort,
}

struct StartGate {
    decision: Mutex<StartDecision>,
    changed: Condvar,
}

impl StartGate {
    fn new() -> Self {
        Self {
            decision: Mutex::new(StartDecision::Pending),
            changed: Condvar::new(),
        }
    }

    fn decide(&self, decision: StartDecision) {
        *lock(&self.decision) = decision;
        self.changed.notify_all();
    }

    fn wait(&self) -> bool {
        let mut decision = lock(&self.decision);
        while matches!(*decision, StartDecision::Pending) {
            decision = self
                .changed
                .wait(decision)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        matches!(*decision, StartDecision::Run)
    }
}

enum StartEvent {
    Ready,
    Failed,
}

enum WorkerExit {
    Completed,
    Aborted,
    Failed(WorkerFailure),
}

struct Bootstrap {
    id: WorkerId,
    driver: DriverBuilder,
    shutdown: channel::DetachedReceiver<()>,
    control: Arc<GroupControl>,
    gate: Arc<StartGate>,
    startup: mpsc::Sender<StartEvent>,
    startup_reported: Arc<AtomicBool>,
}

type WorkerTask = Box<dyn FnOnce(Bootstrap) -> WorkerExit + Send + 'static>;

struct WorkerSlot {
    driver: Option<DriverBuilder>,
    endpoint: Endpoint,
    shutdown: Option<channel::DetachedReceiver<()>>,
    task: Option<WorkerTask>,
}

/// A fixed runtime topology under construction.
pub struct Builder {
    workers: Vec<WorkerSlot>,
    control: Arc<GroupControl>,
    thread_name_prefix: String,
    stack_size: Option<usize>,
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder")
            .field("workers", &self.workers.len())
            .field("thread_name_prefix", &self.thread_name_prefix)
            .field("stack_size", &self.stack_size)
            .finish()
    }
}

impl Builder {
    /// Create a builder containing `worker_count` fixed worker destinations.
    ///
    /// # Panics
    ///
    /// Panics if `worker_count` is zero.
    pub fn new(worker_count: usize) -> Self {
        assert!(
            worker_count > 0,
            "runtime group must contain at least one worker"
        );

        let mut shutdown_controls = Vec::with_capacity(worker_count);
        let workers = (0..worker_count)
            .map(|_| {
                let driver = DriverBuilder::new();
                let endpoint = driver.endpoint().clone();
                let (signal, shutdown) = channel::bounded(&endpoint, 1);
                shutdown_controls.push(Arc::new(ShutdownRemote::new(signal)));
                WorkerSlot {
                    driver: Some(driver),
                    endpoint,
                    shutdown: Some(shutdown),
                    task: None,
                }
            })
            .collect();
        let control = Arc::new(GroupControl {
            workers: shutdown_controls,
        });

        Self {
            workers,
            control,
            thread_name_prefix: "norn-worker".to_owned(),
            stack_size: None,
        }
    }

    /// Return the fixed number of workers in this topology.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Iterate over every valid worker identity in ascending order.
    pub fn worker_ids(&self) -> impl ExactSizeIterator<Item = WorkerId> + '_ {
        (0..self.workers.len()).map(WorkerId::new)
    }

    /// Return the portable channel endpoint for `worker`.
    pub fn endpoint(&self, worker: WorkerId) -> Option<&Endpoint> {
        self.workers.get(worker.index()).map(|slot| &slot.endpoint)
    }

    /// Set the prefix used for worker OS thread names.
    ///
    /// Worker indices are appended as `"<prefix>-<index>"`.
    pub fn thread_name_prefix(&mut self, prefix: impl Into<String>) -> &mut Self {
        self.thread_name_prefix = prefix.into();
        self
    }

    /// Set the stack size, in bytes, for every worker OS thread.
    pub fn stack_size(&mut self, stack_size: usize) -> &mut Self {
        self.stack_size = Some(stack_size);
        self
    }

    /// Configure one worker with [`ThreadPark`] and a thread-local async main.
    ///
    /// `main` is moved to the worker thread before it is invoked. Its returned
    /// future does not need to implement [`Send`]. The future may return `()`
    /// or `Result<(), E>` for an error type implementing [`Error`], [`Send`],
    /// and [`Sync`].
    pub fn worker<F, Fut, O>(
        &mut self,
        worker: WorkerId,
        main: F,
    ) -> Result<&mut Self, ConfigureError>
    where
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = O> + 'static,
        O: WorkerOutput + 'static,
    {
        self.worker_with(
            worker,
            || Ok::<ThreadPark, io::Error>(ThreadPark::default()),
            main,
        )
    }

    /// Configure one worker with a custom fallible [`Park`] factory.
    ///
    /// The factory and `main` closure are moved to the destination thread. The
    /// park value and async main future are constructed there, so neither needs
    /// to implement [`Send`]. This supports destination-local timer and
    /// `io_uring` driver stacks without weakening their thread-affinity rules.
    pub fn worker_with<P, PF, PE, F, Fut, O>(
        &mut self,
        worker: WorkerId,
        park_factory: PF,
        main: F,
    ) -> Result<&mut Self, ConfigureError>
    where
        P: Park + 'static,
        PF: FnOnce() -> Result<P, PE> + Send + 'static,
        PE: Error + Send + Sync + 'static,
        F: FnOnce(WorkerContext) -> Fut + Send + 'static,
        Fut: Future<Output = O> + 'static,
        O: WorkerOutput + 'static,
    {
        let Some(slot) = self.workers.get_mut(worker.index()) else {
            return Err(ConfigureError::UnknownWorker(worker));
        };
        if slot.task.is_some() {
            return Err(ConfigureError::AlreadyConfigured(worker));
        }

        slot.task = Some(Box::new(move |bootstrap| {
            let Bootstrap {
                id,
                driver,
                shutdown,
                control,
                gate,
                startup,
                startup_reported,
            } = bootstrap;
            let park = match park_factory() {
                Ok(park) => park,
                Err(error) => {
                    let failure =
                        WorkerFailure::error(worker, WorkerFailureKind::Startup, Box::new(error));
                    startup_reported.store(true, Ordering::Release);
                    let _ = startup.send(StartEvent::Failed);
                    control.request_all();
                    return WorkerExit::Failed(failure);
                }
            };

            let driver = driver.build(park);
            let channels = driver.handle();
            let shutdown = shutdown.attach(&channels);
            let shutdown_state =
                Rc::new(ShutdownState::new(Arc::clone(&control.workers[id.index()])));
            let mut executor = LocalExecutor::new(driver);
            startup_reported.store(true, Ordering::Release);
            let _ = startup.send(StartEvent::Ready);
            if !gate.wait() {
                return WorkerExit::Aborted;
            }

            let context = WorkerContext {
                id,
                channels,
                control: Arc::clone(&control),
                shutdown: Rc::clone(&shutdown_state),
            };
            let worker_main = run_worker_main(main(context), shutdown, shutdown_state);
            let result = match executor.try_block_on(worker_main) {
                Ok(output) => output.into_result().map_err(|error| {
                    WorkerFailure::error(worker, WorkerFailureKind::Application, error)
                }),
                Err(error) => Err(WorkerFailure::error(
                    worker,
                    WorkerFailureKind::Park,
                    Box::new(error),
                )),
            };

            match result {
                Ok(()) => WorkerExit::Completed,
                Err(failure) => {
                    control.request_all();
                    WorkerExit::Failed(failure)
                }
            }
        }));
        Ok(self)
    }

    /// Spawn and atomically start every configured worker.
    ///
    /// Each thread constructs its destination-local park stack and executor,
    /// then waits at a startup gate. Worker async mains begin only after every
    /// worker reports successful setup. If any setup fails, no async main is
    /// run and all successfully prepared workers are joined before returning.
    pub fn start(mut self) -> Result<RuntimeGroup, StartError> {
        let unconfigured: Vec<_> = self
            .workers
            .iter()
            .enumerate()
            .filter_map(|(index, worker)| worker.task.is_none().then_some(WorkerId::new(index)))
            .collect();
        if !unconfigured.is_empty() {
            return Err(StartError::Unconfigured(unconfigured));
        }

        let gate = Arc::new(StartGate::new());
        let (startup_tx, startup_rx) = mpsc::channel();
        let mut threads = Vec::with_capacity(self.workers.len());

        for (index, mut slot) in self.workers.drain(..).enumerate() {
            let id = WorkerId::new(index);
            let task = slot.task.take().expect("workers were validated above");
            let driver = slot.driver.take().expect("worker driver already consumed");
            let shutdown = slot
                .shutdown
                .take()
                .expect("worker shutdown receiver already consumed");
            let control = Arc::clone(&self.control);
            let gate_for_worker = Arc::clone(&gate);
            let startup = startup_tx.clone();
            let startup_reported = Arc::new(AtomicBool::new(false));
            let reported_for_worker = Arc::clone(&startup_reported);
            let startup_for_panic = startup_tx.clone();
            let control_for_panic = Arc::clone(&self.control);
            let bootstrap = Bootstrap {
                id,
                driver,
                shutdown,
                control,
                gate: gate_for_worker,
                startup,
                startup_reported,
            };
            let thread_main = move || {
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| task(bootstrap)));
                match result {
                    Ok(exit) => exit,
                    Err(panic) => {
                        if !reported_for_worker.swap(true, Ordering::AcqRel) {
                            let _ = startup_for_panic.send(StartEvent::Failed);
                        }
                        control_for_panic.request_all();
                        WorkerExit::Failed(WorkerFailure::panic(id, panic))
                    }
                }
            };

            let mut thread_builder =
                thread::Builder::new().name(format!("{}-{index}", self.thread_name_prefix));
            if let Some(stack_size) = self.stack_size {
                thread_builder = thread_builder.stack_size(stack_size);
            }
            match thread_builder.spawn(thread_main) {
                Ok(join) => threads.push(WorkerThread { id, join }),
                Err(source) => {
                    gate.decide(StartDecision::Abort);
                    self.control.request_all();
                    join_worker_threads(threads);
                    return Err(StartError::Spawn { worker: id, source });
                }
            }
        }
        drop(startup_tx);

        let mut startup_failed = false;
        for _ in 0..threads.len() {
            match startup_rx.recv() {
                Ok(StartEvent::Ready) => {}
                Ok(StartEvent::Failed) | Err(_) => startup_failed = true,
            }
        }

        if startup_failed {
            gate.decide(StartDecision::Abort);
            self.control.request_all();
            let failures = collect_worker_failures(threads);
            return Err(StartError::Worker(JoinError { failures }));
        }

        gate.decide(StartDecision::Run);
        Ok(RuntimeGroup {
            control: self.control,
            workers: threads,
        })
    }
}

async fn run_worker_main<F>(
    main: F,
    mut shutdown: channel::Receiver<()>,
    state: Rc<ShutdownState>,
) -> F::Output
where
    F: Future,
{
    let mut main = pin!(main);
    let mut shutdown = pin!(shutdown.recv());
    let mut delivered = false;

    poll_fn(|cx| {
        if !delivered && shutdown.as_mut().poll(cx).is_ready() {
            delivered = true;
            state.deliver();
        }
        main.as_mut().poll(cx)
    })
    .await
}

struct WorkerThread {
    id: WorkerId,
    join: JoinHandle<WorkerExit>,
}

/// A running fixed group of process-local Norn runtime threads.
pub struct RuntimeGroup {
    control: Arc<GroupControl>,
    workers: Vec<WorkerThread>,
}

impl fmt::Debug for RuntimeGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeGroup")
            .field("workers", &self.workers.len())
            .field(
                "shutdown_requested",
                &self
                    .control
                    .workers
                    .iter()
                    .all(|worker| worker.is_requested()),
            )
            .finish()
    }
}

impl RuntimeGroup {
    /// Begin constructing a fixed topology with `worker_count` workers.
    ///
    /// # Panics
    ///
    /// Panics if `worker_count` is zero.
    pub fn builder(worker_count: usize) -> Builder {
        Builder::new(worker_count)
    }

    /// Return the number of runtime workers in this group.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Request cooperative shutdown of every worker.
    ///
    /// This method wakes shutdown waiters but does not wait for worker threads.
    /// Worker mains remain responsible for draining local work and returning.
    pub fn request_shutdown(&self) {
        self.control.request_all();
    }

    /// Wait for every worker to finish naturally.
    pub fn join(mut self) -> Result<(), JoinError> {
        let failures = collect_worker_failures(std::mem::take(&mut self.workers));
        if failures.is_empty() {
            Ok(())
        } else {
            Err(JoinError { failures })
        }
    }

    /// Request cooperative shutdown, then wait for every worker to finish.
    pub fn shutdown(self) -> Result<(), JoinError> {
        self.request_shutdown();
        self.join()
    }
}

impl Drop for RuntimeGroup {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}

fn collect_worker_failures(workers: Vec<WorkerThread>) -> Vec<WorkerFailure> {
    let mut failures = Vec::new();
    for worker in workers {
        match worker.join.join() {
            Ok(WorkerExit::Completed | WorkerExit::Aborted) => {}
            Ok(WorkerExit::Failed(failure)) => failures.push(failure),
            Err(panic) => failures.push(WorkerFailure::panic(worker.id, panic)),
        }
    }
    failures.sort_by_key(WorkerFailure::worker);
    failures
}

fn join_worker_threads(workers: Vec<WorkerThread>) {
    for worker in workers {
        let _ = worker.join.join();
    }
}

fn panic_message(panic: &(dyn Any + Send + 'static)) -> String {
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::{pending, poll_fn};
    use std::pin::pin;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{mpsc as std_mpsc, Arc};

    use norn_channel::mpsc;
    use norn_executor::park::{Park, ParkMode, SpinPark, Unpark};

    use super::*;

    #[test]
    fn topology_exposes_stable_worker_endpoints() {
        let builder = Builder::new(3);
        assert_eq!(builder.worker_count(), 3);
        assert_eq!(
            builder.worker_ids().collect::<Vec<_>>(),
            [WorkerId::new(0), WorkerId::new(1), WorkerId::new(2)]
        );
        assert!(builder.endpoint(WorkerId::new(0)).is_some());
        assert!(builder.endpoint(WorkerId::new(3)).is_none());
    }

    #[test]
    fn configuration_rejects_unknown_and_duplicate_workers() {
        let mut builder = Builder::new(1);
        assert!(matches!(
            builder.worker(WorkerId::new(1), |_| async {}),
            Err(ConfigureError::UnknownWorker(WorkerId(1)))
        ));
        builder.worker(WorkerId::new(0), |_| async {}).unwrap();
        assert!(matches!(
            builder.worker(WorkerId::new(0), |_| async {}),
            Err(ConfigureError::AlreadyConfigured(WorkerId(0)))
        ));
    }

    #[test]
    fn start_rejects_unconfigured_workers() {
        let mut builder = Builder::new(2);
        builder.worker(WorkerId::new(0), |_| async {}).unwrap();
        let error = builder.start().unwrap_err();
        assert!(matches!(
            error,
            StartError::Unconfigured(workers) if workers == [WorkerId::new(1)]
        ));
    }

    #[test]
    fn workers_exchange_messages_without_bootstrap_transport() {
        let mut builder = Builder::new(2);
        let worker_a = WorkerId::new(0);
        let worker_b = WorkerId::new(1);
        let (to_a, inbox_a) = mpsc::bounded(builder.endpoint(worker_a).unwrap(), 4);
        let (to_b, inbox_b) = mpsc::bounded(builder.endpoint(worker_b).unwrap(), 4);

        builder
            .worker(worker_a, move |context| async move {
                let mut inbox_a = inbox_a.attach(context.channels());
                to_b.try_send(10).unwrap();
                assert_eq!(inbox_a.recv().await, Some(11));
            })
            .unwrap();
        builder
            .worker(worker_b, move |context| async move {
                let mut inbox_b = inbox_b.attach(context.channels());
                let value = inbox_b.recv().await.unwrap();
                to_a.try_send(value + 1).unwrap();
            })
            .unwrap();

        builder.start().unwrap().join().unwrap();
    }

    #[test]
    fn worker_futures_may_be_non_send() {
        let mut builder = Builder::new(1);
        builder
            .worker(WorkerId::new(0), |_| async move {
                let local = Rc::new(Cell::new(0));
                local.set(1);
                assert_eq!(local.get(), 1);
            })
            .unwrap();
        builder.start().unwrap().join().unwrap();
    }

    #[test]
    fn shutdown_wakes_every_worker() {
        let mut builder = Builder::new(3);
        for worker in builder.worker_ids().collect::<Vec<_>>() {
            builder
                .worker(worker, |context| async move {
                    context.shutdown_requested().await;
                    assert!(context.is_shutdown_requested());
                })
                .unwrap();
        }

        let group = builder.start().unwrap();
        assert_eq!(group.worker_count(), 3);
        group.shutdown().unwrap();
    }

    #[test]
    fn shutdown_wakes_background_tasks_on_their_owning_workers() {
        let mut builder = Builder::new(2);
        let (ready_tx, ready_rx) = std_mpsc::channel();

        for worker in builder.worker_ids().collect::<Vec<_>>() {
            let ready = ready_tx.clone();
            builder
                .worker(worker, move |context| async move {
                    let waiter_context = context.clone();
                    let waiter = norn_executor::spawn(async move {
                        let mut shutdown = pin!(waiter_context.shutdown_requested());
                        let mut announced = false;
                        poll_fn(|cx| {
                            let result = shutdown.as_mut().poll(cx);
                            if !announced {
                                ready.send(()).unwrap();
                                announced = true;
                            }
                            result
                        })
                        .await;
                    });

                    waiter.await.unwrap();
                })
                .unwrap();
        }
        drop(ready_tx);

        let group = builder.start().unwrap();
        for _ in 0..2 {
            ready_rx.recv().unwrap();
        }
        group.shutdown().unwrap();
    }

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }
    }

    impl Error for TestError {}

    #[test]
    fn worker_error_requests_peer_shutdown() {
        let mut builder = Builder::new(2);
        builder
            .worker(WorkerId::new(0), |_| async {
                Err::<(), _>(TestError("application failed"))
            })
            .unwrap();
        builder
            .worker(WorkerId::new(1), |context| async move {
                context.shutdown_requested().await;
            })
            .unwrap();

        let error = builder.start().unwrap().join().unwrap_err();
        assert_eq!(error.failures().len(), 1);
        assert_eq!(error.failures()[0].worker(), WorkerId::new(0));
        assert_eq!(error.failures()[0].kind(), WorkerFailureKind::Application);
    }

    #[test]
    fn worker_panic_requests_peer_shutdown() {
        let mut builder = Builder::new(2);
        builder
            .worker::<_, _, ()>(WorkerId::new(0), |_| async {
                panic!("worker panic");
            })
            .unwrap();
        builder
            .worker(WorkerId::new(1), |context| async move {
                context.shutdown_requested().await;
            })
            .unwrap();

        let error = builder.start().unwrap().join().unwrap_err();
        assert_eq!(error.failures().len(), 1);
        assert_eq!(error.failures()[0].worker(), WorkerId::new(0));
        assert_eq!(error.failures()[0].kind(), WorkerFailureKind::Panic);
    }

    #[test]
    fn startup_failure_prevents_every_worker_main() {
        let mut builder = Builder::new(2);
        let ran = Arc::new(AtomicBool::new(false));
        let ran_by_worker = Arc::clone(&ran);
        builder
            .worker_with(
                WorkerId::new(0),
                || Err::<SpinPark, _>(TestError("park setup failed")),
                |_| async {},
            )
            .unwrap();
        builder
            .worker(WorkerId::new(1), move |_| async move {
                ran_by_worker.store(true, Ordering::Release);
            })
            .unwrap();

        let error = builder.start().unwrap_err();
        assert!(!ran.load(Ordering::Acquire));
        let StartError::Worker(error) = error else {
            panic!("expected worker startup failure");
        };
        assert_eq!(error.failures().len(), 1);
        assert_eq!(error.failures()[0].kind(), WorkerFailureKind::Startup);
    }

    #[derive(Debug)]
    struct FailingPark;

    #[derive(Clone, Debug)]
    struct FailingUnparker;

    impl Unpark for FailingUnparker {
        fn unpark(&self) {}
    }

    impl Park for FailingPark {
        type Unparker = FailingUnparker;
        type Guard = ();

        fn park(&mut self, _: ParkMode) -> Result<(), io::Error> {
            Err(io::Error::other("park failed"))
        }

        fn enter(&self) -> Self::Guard {}

        fn unparker(&self) -> Self::Unparker {
            FailingUnparker
        }

        fn needs_park(&self) -> bool {
            false
        }

        fn shutdown(&mut self) {}
    }

    #[test]
    fn park_errors_are_reported_without_panicking() {
        let mut builder = Builder::new(1);
        builder
            .worker_with(
                WorkerId::new(0),
                || Ok::<_, TestError>(FailingPark),
                |_| pending::<()>(),
            )
            .unwrap();

        let error = builder.start().unwrap().join().unwrap_err();
        assert_eq!(error.failures().len(), 1);
        assert_eq!(error.failures()[0].kind(), WorkerFailureKind::Park);
    }
}
