//! A single-threaded executor for driving [`Future`]s to completion.
//!
//! [`LocalExecutor`] runs local tasks from `norn-task` and delegates idle time
//! to a [`park::Park`] implementation. Park layers may block the thread or
//! drive another subsystem such as timers or `io_uring`.
//!
//! Futures spawned through [`spawn`] or [`Handle::spawn`] must be `'static`, but
//! they do not need to implement `Send`; all task polling remains on the thread
//! driving the executor.
//!
//! # Example
//!
//! ```
//! use norn_executor::park::SpinPark;
//! use norn_executor::{spawn, LocalExecutor};
//!
//! let mut executor = LocalExecutor::new(SpinPark);
//! let answer = executor.block_on(async {
//!     spawn(async { 40 + 2 }).await.unwrap()
//! });
//!
//! assert_eq!(answer, 42);
//! ```
//!
//! # Runtime layers
//!
//! See [`park`] for the composition interface and built-in parking
//! implementations.
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    rustdoc::bare_urls,
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_safety_doc
)]
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;

use norn_metrics::Padded;
use norn_task::JoinHandle;

const DEFAULT_TASK_POLL_BUDGET: NonZeroUsize = NonZeroUsize::new(32).unwrap();

mod context;
mod metrics;
/// Parking abstractions and built-in park implementations.
pub mod park;
mod wakerfn;

pub use metrics::{ExecutorMetrics, ExecutorSnapshot};

/// A single-threaded executor for driving [`Future`]s to completion.
///
/// [`LocalExecutor`] can be driven by calling [`LocalExecutor::block_on`].
///
/// # Panics on drop
///
/// Dropping a [`LocalExecutor`] cancels its remaining tasks and then shuts down
/// the [`park::Park`] layer. If waking a task's `JoinHandle` waker panics while
/// the tasks are cancelled, every task is still finished and
/// [`park::Park::shutdown`] still runs, and the first such panic is then
/// propagated out of `drop`. A drop that happens while the thread is already
/// panicking aborts the process, as for any panic in a destructor during
/// unwinding.
pub struct LocalExecutor<P: park::Park> {
    /// Task queue contains tasks which are ready to be executed.
    taskqueue: norn_task::TaskQueue,
    park: P,
    config: Config,
    root_notifier: Option<wakerfn::RootNotifier<P::Unparker>>,
    counters: Arc<Padded<metrics::ExecutorCounters>>,
}

/// Configuration for [`LocalExecutor`] scheduling.
///
/// The default task-poll budget is 32.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Maximum number of runnable task polls before control returns to the
    /// root future and the [`park::Park`] layer.
    pub task_poll_budget: NonZeroUsize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            task_poll_budget: DEFAULT_TASK_POLL_BUDGET,
        }
    }
}

impl<P: park::Park> std::fmt::Debug for LocalExecutor<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutor")
            .field("taskqueue", &self.taskqueue)
            .field("config", &self.config)
            .finish()
    }
}

impl<P: park::Park> LocalExecutor<P> {
    /// Construct a new [`LocalExecutor`] with the given [`park::Park`].
    ///
    /// The [`LocalExecutor`] will use the given [`park::Park`] to block the
    /// driver thread when there are no tasks ready to be executed. Scheduling
    /// uses [`Config::default`].
    pub fn new(park: P) -> Self {
        Self::new_with_config(park, Config::default())
    }

    /// Construct a new [`LocalExecutor`] with the given [`park::Park`] and
    /// scheduling configuration.
    pub fn new_with_config(park: P, config: Config) -> Self {
        let root_notifier = Some(wakerfn::root_notifier(park.unparker()));
        Self {
            taskqueue: norn_task::TaskQueue::new(),
            park,
            config,
            root_notifier,
            counters: Arc::default(),
        }
    }

    /// Returns a [`Handle`] to the [`LocalExecutor`].
    ///
    /// The [`Handle`] can be used to spawn new tasks onto the [`LocalExecutor`].
    pub fn handle(&self) -> Handle {
        Handle {
            taskqueue: self.taskqueue.clone(),
        }
    }

    /// A view of this executor's scheduling counters and task counts that any
    /// thread can read.
    pub fn metrics(&self) -> ExecutorMetrics {
        ExecutorMetrics {
            counters: Arc::clone(&self.counters),
            tasks: self.taskqueue.metrics(),
        }
    }

    /// The metric sources of this executor and of every layer in its
    /// [`park::Park`] stack, readable from any thread.
    ///
    /// The executor's [`ExecutorMetrics`] comes first, followed by whatever
    /// each park layer adds in [`park::Park::metric_sources`]. Use
    /// [`norn_metrics::Sources::get`] for one layer's typed view.
    pub fn metric_sources(&self) -> norn_metrics::Sources {
        let mut sources = norn_metrics::Sources::new();
        sources.push(self.metrics());
        self.park.metric_sources(&mut sources);
        sources
    }

    /// Blocks the current thread until the provided [`Future`] has completed.
    ///
    /// This will run all tasks which have been spawned onto the [`LocalExecutor`]
    /// and drive the [`park::Park`] instance.
    ///
    /// ### Panics
    /// Panics if [`park::Park::park`] returns an error, or if an executor is
    /// already running on this thread: `block_on` and [`try_block_on`] cannot
    /// be nested, including from inside a spawned task. Over a timer or `io_uring`
    /// park the panic comes from that driver, which is entered first, and says
    /// "driver" rather than "executor".
    ///
    /// [`try_block_on`]: LocalExecutor::try_block_on
    pub fn block_on<F>(&mut self, fut: F) -> F::Output
    where
        F: Future,
    {
        self.try_block_on(fut)
            .unwrap_or_else(|error| panic!("park failed: {error}"))
    }

    /// Blocks the current thread until the provided [`Future`] has completed.
    ///
    /// This is the fallible counterpart to [`LocalExecutor::block_on`]. It
    /// returns an error when the underlying [`park::Park`] layer fails instead
    /// of panicking, allowing runtime orchestration layers to propagate driver
    /// failures through their normal lifecycle APIs.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying [`park::Park::park`] operation
    /// fails.
    ///
    /// # Panics
    ///
    /// Panics, instead of returning an error, if an executor is already running
    /// on this thread: like [`LocalExecutor::block_on`], it cannot be nested.
    pub fn try_block_on<F>(&mut self, fut: F) -> Result<F::Output, std::io::Error>
    where
        F: Future,
    {
        let _g = self.enter();
        let fut = pin!(fut);
        let notifier = match self.root_notifier.take() {
            Some(notifier) => notifier,
            None => wakerfn::root_notifier(self.park.unparker()),
        };
        let mut root = wakerfn::FutureHarness::new(fut, notifier);

        loop {
            if let Some(result) = root.try_poll() {
                self.root_notifier = root.into_reusable_notifier();
                return Ok(result);
            }
            let mut has_remaining_tasks = false;
            let budget = self.config.task_poll_budget.get();
            let mut polled = 0;
            while let Some(next) = self.taskqueue.next() {
                next.run();
                polled += 1;
                if polled == budget {
                    has_remaining_tasks = self.taskqueue.runnable() > 0;
                    break;
                }
                if self.park.needs_park() {
                    has_remaining_tasks = self.taskqueue.runnable() > 0;
                    break;
                }
            }
            // Published once per tick rather than per poll, and before parking
            // so that a reader sees them while the executor waits.
            let counters = &*self.counters;
            counters.polls.add(polled as u64);
            if polled == budget && has_remaining_tasks {
                counters.budget_exhausted.incr();
            }
            let mut mode = park::ParkMode::NextCompletion;
            if root.is_notified() || has_remaining_tasks {
                mode = park::ParkMode::NoPark;
            } else {
                counters.waiting_parks.incr();
            }
            counters.park_epoch.incr();
            let parked = self.park.park(mode);
            counters.park_epoch.incr();
            parked?;
        }
    }

    fn enter(&self) -> (P::Guard, context::ContextGuard) {
        let g1 = self.park.enter();
        let g2 = context::Context::enter(self.handle());
        (g1, g2)
    }
}

/// A handle to a [`LocalExecutor`].
#[derive(Debug, Clone)]
pub struct Handle {
    taskqueue: norn_task::TaskQueue,
}

impl Handle {
    /// Returns a [`Handle`] to the current [`LocalExecutor`].
    ///
    /// ### Panics
    /// This function will panic if called from outside of a [`LocalExecutor`]
    /// context. That includes a destructor that runs at thread exit, after the
    /// thread's executor context has been destroyed; a panic there aborts the
    /// process, so a task destructor must not call this when its executor is
    /// stored in a `thread_local!`.
    pub fn current() -> Self {
        context::Context::handle().expect("executor not set")
    }

    /// Spawn a [`Future`] onto the [`LocalExecutor`].
    ///
    /// The spawned future will run on the thread driving the [`LocalExecutor`].
    ///
    /// # Aborts
    ///
    /// The task belongs to the thread that runs it. Cloning, waking or dropping
    /// its waker from any other thread aborts the process (after printing a
    /// message), so a future must not hand its waker to another thread or to
    /// something completed there, such as a `std::thread`, or a channel or oneshot
    /// from another runtime (for example `futures::channel` or `tokio::sync`) whose
    /// sender completes on another thread. `norn-channel` is safe: it wakes receivers
    /// only on their own runtime thread. The process also aborts, instead of
    /// unwinding, if dropping the
    /// future or its output panics, or if the task's reference count overflows.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        self.taskqueue.spawn(future)
    }
}

/// Spawn a [`Future`] onto the [`LocalExecutor`].
///
/// The spawned future will run on the thread driving the [`LocalExecutor`].
///
/// # Panics
///
/// Panics if called outside a [`LocalExecutor`] context, for example from
/// another thread or from a plain function that no executor is driving. That
/// includes a destructor that runs at thread exit, after the thread's executor
/// context has been destroyed.
///
/// # Aborts
///
/// The task belongs to the thread that runs it. Cloning, waking or dropping
/// its waker from any other thread aborts the process (after printing a
/// message), so a future must not hand its waker to another thread or to
/// something completed there, such as a `std::thread`, or a channel or oneshot
/// from another runtime (for example `futures::channel` or `tokio::sync`) whose
/// sender completes on another thread. `norn-channel` is safe: it wakes receivers
/// only on their own runtime thread. The process also aborts, instead of
/// unwinding, if dropping the
/// future or its output panics, or if the task's reference count overflows.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
{
    context::Context::with_handle(|handle| handle.spawn(future)).expect("executor not set")
}

impl<P: park::Park> Drop for LocalExecutor<P> {
    fn drop(&mut self) {
        // Task destructors may need this executor's context, but `Drop` can
        // run inside another executor or after thread-local storage is gone.
        // Install this context temporarily instead of asserting it is empty.
        let _g = (
            self.park.enter_for_shutdown(),
            context::Context::enter_for_shutdown(self.handle()),
        );
        // `TaskQueue::shutdown` finishes every task and then re-raises the
        // first join-waker panic. Still shut the park down while the context
        // guard is alive, then resume the panic.
        let tasks = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.taskqueue.shutdown();
        }));
        self.park.shutdown();
        if let Err(payload) = tasks {
            std::panic::resume_unwind(payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future;
    use std::io;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Poll;
    use std::task::Waker;

    use crate::park::SpinPark;

    use super::*;

    struct SelfWaker {
        polls: Rc<Cell<usize>>,
    }

    impl Future for SelfWaker {
        type Output = ();

        fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<()> {
            self.polls.set(self.polls.get() + 1);
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    struct CountedReady {
        polls: Rc<Cell<usize>>,
    }

    impl Future for CountedReady {
        type Output = ();

        fn poll(self: std::pin::Pin<&mut Self>, _: &mut std::task::Context<'_>) -> Poll<()> {
            self.polls.set(self.polls.get() + 1);
            Poll::Ready(())
        }
    }

    #[derive(Clone, Copy, Debug)]
    struct NoopUnparker;

    impl park::Unpark for NoopUnparker {
        fn unpark(&self) {}
    }

    #[derive(Debug)]
    struct ReadyOnPark {
        ready: Rc<Cell<bool>>,
        root_waker: Rc<RefCell<Option<Waker>>>,
        modes: Rc<RefCell<Vec<park::ParkMode>>>,
    }

    impl park::Park for ReadyOnPark {
        type Unparker = NoopUnparker;
        type Guard = ();

        fn park(&mut self, mode: park::ParkMode) -> io::Result<()> {
            self.modes.borrow_mut().push(mode);
            self.ready.set(true);
            self.root_waker
                .borrow_mut()
                .take()
                .expect("root waker not registered")
                .wake();
            Ok(())
        }

        fn enter(&self) -> Self::Guard {}

        fn unparker(&self) -> Self::Unparker {
            NoopUnparker
        }

        fn needs_park(&self) -> bool {
            false
        }

        fn shutdown(&mut self) {}
    }

    /// Ticks and polls are published once per tick, and the parked flag is
    /// clear once `block_on` returns.
    #[test]
    fn metrics_count_ticks_and_polls() {
        let mut executor = LocalExecutor::new(SpinPark);
        let metrics = executor.metrics();
        let polls = Rc::new(Cell::new(0));
        let observed = Rc::clone(&polls);
        executor.block_on(async move {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let polls = Rc::clone(&observed);
                    spawn(async move {
                        // Two polls: one pending yield, then completion.
                        let mut yielded = false;
                        std::future::poll_fn(|cx| {
                            polls.set(polls.get() + 1);
                            if yielded {
                                return Poll::Ready(());
                            }
                            yielded = true;
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        })
                        .await
                    })
                })
                .collect();
            for handle in handles {
                handle.await.unwrap();
            }
        });
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.polls, polls.get() as u64);
        assert_eq!(snapshot.polls, 8);
        // Woken tasks requeue behind the others, so one tick can poll all
        // eight within the default budget.
        assert!(snapshot.ticks >= 1, "{snapshot:?}");
        assert!(!snapshot.parked);
        assert_eq!(snapshot.tasks.spawned, 4);
        assert_eq!(snapshot.tasks.finished, 4);
    }

    /// The executor's own view comes first; a park layer without counters
    /// adds nothing.
    #[test]
    fn metric_sources_start_with_the_executor() {
        let mut executor = LocalExecutor::new(SpinPark);
        executor.block_on(async { spawn(async {}).await.unwrap() });
        let sources = executor.metric_sources();
        assert_eq!(sources.len(), 1);
        let snapshot = sources.get::<ExecutorMetrics>().unwrap().snapshot();
        assert_eq!(snapshot.tasks.spawned, 1);
    }

    /// A budget smaller than the runnable set ends ticks early, and each such
    /// tick is counted.
    #[test]
    fn metrics_count_budget_exhaustion() {
        let config = Config {
            task_poll_budget: NonZeroUsize::new(2).unwrap(),
        };
        let mut executor = LocalExecutor::new_with_config(SpinPark, config);
        let metrics = executor.metrics();
        executor.block_on(async {
            let handles: Vec<_> = (0..5).map(|_| spawn(async {})).collect();
            for handle in handles {
                handle.await.unwrap();
            }
        });
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.polls, 5);
        assert!(snapshot.budget_exhausted >= 2, "{snapshot:?}");
    }

    #[test]
    fn block_on() {
        let mut executor = LocalExecutor::new(SpinPark);

        let res = executor.block_on(async { 1 + 1 });
        assert_eq!(res, 2);
        assert!(executor.root_notifier.is_some());
    }

    #[test]
    fn task_poll_budget_bounds_root_starvation() {
        for budget in [2, 5] {
            let polls = Rc::new(Cell::new(0));
            let observed_polls = Rc::clone(&polls);
            let config = Config {
                task_poll_budget: NonZeroUsize::new(budget).unwrap(),
            };
            let mut executor = LocalExecutor::new_with_config(SpinPark, config);

            executor.block_on(async move {
                spawn(SelfWaker {
                    polls: Rc::clone(&observed_polls),
                })
                .detach();
                spawn(CountedReady {
                    polls: Rc::clone(&observed_polls),
                })
                .await
                .unwrap();
            });

            assert_eq!(polls.get(), budget);
        }
    }

    #[test]
    fn task_poll_budget_hands_control_to_park() {
        for budget in [2, 5] {
            let polls = Rc::new(Cell::new(0));
            let ready = Rc::new(Cell::new(false));
            let root_waker = Rc::new(RefCell::new(None));
            let modes = Rc::new(RefCell::new(Vec::new()));
            let park = ReadyOnPark {
                ready: Rc::clone(&ready),
                root_waker: Rc::clone(&root_waker),
                modes: Rc::clone(&modes),
            };
            let config = Config {
                task_poll_budget: NonZeroUsize::new(budget).unwrap(),
            };
            let mut executor = LocalExecutor::new_with_config(park, config);
            let mut spawned = false;

            executor.block_on(future::poll_fn(|cx| {
                if ready.get() {
                    return Poll::Ready(());
                }
                *root_waker.borrow_mut() = Some(cx.waker().clone());
                if !spawned {
                    spawn(SelfWaker {
                        polls: Rc::clone(&polls),
                    })
                    .detach();
                    spawned = true;
                }
                Poll::Pending
            }));

            assert_eq!(polls.get(), budget);
            assert_eq!(&*modes.borrow(), &[park::ParkMode::NoPark]);
        }
    }

    #[test]
    fn escaped_root_waker_is_not_reused() {
        let mut executor = LocalExecutor::new(SpinPark);
        let escaped = Rc::new(RefCell::new(None::<Waker>));
        let escaped_by_future = Rc::clone(&escaped);

        executor.block_on(future::poll_fn(move |cx| {
            *escaped_by_future.borrow_mut() = Some(cx.waker().clone());
            Poll::Ready(())
        }));

        assert!(executor.root_notifier.is_none());
        drop(escaped.borrow_mut().take());

        executor.block_on(async {});
        assert!(executor.root_notifier.is_some());
    }

    #[test]
    fn root_waker_can_wake_from_another_thread() {
        let mut executor = LocalExecutor::new(crate::park::ThreadPark::default());
        let completed = Arc::new(AtomicBool::new(false));
        let completed_by_worker = Arc::clone(&completed);
        let (waker_tx, waker_rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let waker: Waker = waker_rx.recv().unwrap();
            completed_by_worker.store(true, Ordering::Release);
            waker.wake();
        });

        let mut sent_waker = false;
        executor.block_on(future::poll_fn(|cx| {
            if completed.load(Ordering::Acquire) {
                return Poll::Ready(());
            }
            if !sent_waker {
                waker_tx.send(cx.waker().clone()).unwrap();
                sent_waker = true;
            }
            Poll::Pending
        }));
        worker.join().unwrap();
    }

    #[test]
    #[allow(clippy::waker_clone_wake)]
    fn same_thread_root_wake_does_not_unpark() {
        #[derive(Clone, Debug)]
        struct CountingUnparker(Arc<AtomicUsize>);

        impl park::Unpark for CountingUnparker {
            fn unpark(&self) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        #[derive(Debug)]
        struct CountingPark(CountingUnparker);

        impl park::Park for CountingPark {
            type Unparker = CountingUnparker;
            type Guard = ();

            fn park(&mut self, _: park::ParkMode) -> io::Result<()> {
                Ok(())
            }

            fn enter(&self) -> Self::Guard {}

            fn unparker(&self) -> Self::Unparker {
                self.0.clone()
            }

            fn needs_park(&self) -> bool {
                false
            }

            fn shutdown(&mut self) {}
        }

        let unpark_count = Arc::new(AtomicUsize::new(0));
        let park = CountingPark(CountingUnparker(Arc::clone(&unpark_count)));
        let mut executor = LocalExecutor::new(park);
        let mut yielded = false;
        executor.block_on(future::poll_fn(|cx| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                // Exercise the owned vtable while remaining on the executor
                // thread. `wake_by_ref` would only test the borrowed vtable.
                cx.waker().clone().wake();
                Poll::Pending
            }
        }));

        assert_eq!(unpark_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn spawn_in_blockon() {
        let mut executor = LocalExecutor::new(SpinPark);

        let handle = executor.handle();
        let res = executor.block_on(async move {
            let handle = handle.clone();
            handle.spawn(async move { 1 + 1 }).await
        });
        assert_eq!(res.unwrap(), 2);
    }

    #[test]
    fn spawn_before_blockon() {
        let mut executor = LocalExecutor::new(SpinPark);
        let handle = executor.handle();

        let f1 = handle.spawn(async { 1 + 1 });
        executor.block_on(async move {
            let res = f1.await.unwrap();
            assert_eq!(res, 2);
        });
    }

    #[test]
    fn spawn_after_shutdown() {
        let executor = LocalExecutor::new(SpinPark);
        let handle = executor.handle();

        drop(executor);
        let f1 = handle.spawn(async { 1 + 1 });
        let f1 = pin!(f1);

        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let Poll::Ready(res) = f1.poll(&mut cx) else {
            panic!("expected ready");
        };
        assert!(res.is_err());
        assert!(res.err().unwrap().is_cancelled());
    }

    #[test]
    fn spawn_from_context() {
        let mut executor = LocalExecutor::new(SpinPark);

        executor.block_on(async {
            let f1 = crate::spawn(async { 1 + 1 });
            let res = f1.await.unwrap();
            assert_eq!(res, 2);
        })
    }

    #[test]
    fn block_on_panics_on_park_error() {
        #[derive(Clone, Copy, Debug)]
        struct TestUnparker;

        impl park::Unpark for TestUnparker {
            fn unpark(&self) {}
        }

        #[derive(Debug)]
        struct FailingPark;

        impl park::Park for FailingPark {
            type Unparker = TestUnparker;
            type Guard = ();

            fn park(&mut self, _: park::ParkMode) -> io::Result<()> {
                Err(io::Error::other("park failed"))
            }

            fn enter(&self) -> Self::Guard {}

            fn unparker(&self) -> Self::Unparker {
                TestUnparker
            }

            fn needs_park(&self) -> bool {
                false
            }

            fn shutdown(&mut self) {}
        }

        let mut fallible = LocalExecutor::new(FailingPark);
        let error = fallible
            .try_block_on(future::pending::<()>())
            .expect_err("try_block_on should return park errors");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "park failed");

        let mut executor = LocalExecutor::new(FailingPark);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.block_on(async {
                future::pending::<()>().await;
            })
        }));
        assert!(result.is_err(), "block_on should panic on park error");
    }

    #[test]
    fn spawn_on_a_closed_queue_may_reenter_the_executor_context() {
        struct EntersExecutor;

        impl Drop for EntersExecutor {
            fn drop(&mut self) {
                // Runs while `spawn` cancels this future on a closed queue.
                // Entering a nested executor panics because a context is
                // already set; each panic is caught so none escapes into the
                // task system's abort-on-panic destructor guard.
                let mut nested = LocalExecutor::new(SpinPark);
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    nested.block_on(async {});
                }));
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                    drop(nested);
                }));
            }
        }

        struct SpawnsOnDrop;

        impl Drop for SpawnsOnDrop {
            fn drop(&mut self) {
                let guard = EntersExecutor;
                spawn(async move {
                    let _guard = &guard;
                    future::pending::<()>().await
                })
                .detach();
            }
        }

        let executor = LocalExecutor::new(SpinPark);
        let task = SpawnsOnDrop;
        executor
            .handle()
            .spawn(async move {
                let _task = &task;
                future::pending::<()>().await
            })
            .detach();
        drop(executor);
    }
}
