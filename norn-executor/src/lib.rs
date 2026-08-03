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
use std::pin::pin;

use norn_task::JoinHandle;

mod context;
/// Parking abstractions and built-in park implementations.
pub mod park;
mod wakerfn;

/// A single-threaded executor for driving [`Future`]s to completion.
///
/// [`LocalExecutor`] can be driven by calling [`LocalExecutor::block_on`].
pub struct LocalExecutor<P: park::Park> {
    /// Task queue contains tasks which are ready to be executed.
    taskqueue: norn_task::TaskQueue,
    park: P,
    root_notifier: Option<wakerfn::RootNotifier<P::Unparker>>,
}

impl<P: park::Park> std::fmt::Debug for LocalExecutor<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExecutor")
            .field("taskqueue", &self.taskqueue)
            .finish()
    }
}

impl<P: park::Park> LocalExecutor<P> {
    /// Construct a new [`LocalExecutor`] with the given [`park::Park`].
    ///
    /// The [`LocalExecutor`] will use the given [`park::Park`] to block the
    /// driver thread when there are no tasks ready to be executed.
    pub fn new(park: P) -> Self {
        let root_notifier = Some(wakerfn::root_notifier(park.unparker()));
        Self {
            taskqueue: norn_task::TaskQueue::new(),
            park,
            root_notifier,
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

    /// Blocks the current thread until the provided [`Future`] has completed.
    ///
    /// This will run all tasks which have been spawned onto the [`LocalExecutor`]
    /// and drive the [`park::Park`] instance.
    ///
    /// ### Panics
    /// Panics if [`park::Park::park`] returns an error.
    pub fn block_on<F>(&mut self, fut: F) -> F::Output
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
                return result;
            }
            let mut has_remaining_tasks = false;
            while let Some(next) = self.taskqueue.next() {
                next.run();
                if self.park.needs_park() {
                    has_remaining_tasks = self.taskqueue.runnable() > 0;
                    break;
                }
            }
            let mut mode = park::ParkMode::NextCompletion;
            if root.is_notified() || has_remaining_tasks {
                mode = park::ParkMode::NoPark;
            }
            self.park.park(mode).unwrap();
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
    /// context.
    pub fn current() -> Self {
        context::Context::handle().expect("executor not set")
    }

    /// Spawn a [`Future`] onto the [`LocalExecutor`].
    ///
    /// The spawned future will run on the thread driving the [`LocalExecutor`].
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
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
{
    Handle::current().spawn(future)
}

impl<P: park::Park> Drop for LocalExecutor<P> {
    fn drop(&mut self) {
        let _g = self.enter();
        self.taskqueue.shutdown();
        self.park.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future;
    use std::io;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Poll;
    use std::task::Waker;

    use crate::park::SpinPark;

    use super::*;

    #[test]
    fn block_on() {
        let mut executor = LocalExecutor::new(SpinPark);

        let res = executor.block_on(async { 1 + 1 });
        assert_eq!(res, 2);
        assert!(executor.root_notifier.is_some());
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

        let mut executor = LocalExecutor::new(FailingPark);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            executor.block_on(async {
                future::pending::<()>().await;
            })
        }));
        assert!(result.is_err(), "block_on should panic on park error");
    }
}
