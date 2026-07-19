use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::tasks::TaskSet;
use crate::{RegisteredTask, Runnable, Schedule};

mod basic;
mod combo;
mod panic;
mod reentrancy;
mod wake;

struct TestSpawner {
    shared: Rc<Shared>,
}

struct Shared {
    runqueue: RefCell<VecDeque<Runnable>>,
    owned: TaskSet,
}

impl TestSpawner {
    fn new() -> Self {
        Self {
            shared: Rc::new(Shared {
                runqueue: RefCell::default(),
                owned: TaskSet::default(),
            }),
        }
    }

    fn spawn<F>(&self, future: F) -> crate::JoinHandle<F::Output>
    where
        F: Future + 'static,
    {
        // Safety: The future is bound by 'static
        let (notified, handle) = unsafe { self.shared.owned.bind(future, self.shared.clone()) };
        if let Some(notified) = notified {
            self.shared.schedule(notified);
        }
        handle
    }

    fn next(&self) -> Option<Runnable> {
        self.shared.runqueue.borrow_mut().pop_front()
    }

    fn shutdown(&self) {
        self.shared.owned.shutdown();
        self.shared.runqueue.borrow_mut().drain(..);
    }
}

impl Schedule for Shared {
    fn unbind(&self, task: &RegisteredTask) {
        unsafe { self.owned.remove(task) };
    }

    fn schedule(&self, task: Runnable) {
        self.runqueue.borrow_mut().push_back(task);
    }
}

impl Drop for TestSpawner {
    fn drop(&mut self) {
        self.shared.owned.shutdown();
        while let Some(next) = self.next() {
            drop(next);
        }
    }
}

struct TestFuture;

impl Future for TestFuture {
    type Output = TestFutureOutput;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        TestState::with(|s| {
            s.num_polls += 1;
            if s.store_waker {
                s.stashed_waker = Some(cx.waker().clone());
            }
            assert!(!s.panic_on_run, "task panic on run");
            if s.return_pending {
                Poll::Pending
            } else {
                Poll::Ready(TestFutureOutput())
            }
        })
    }
}

impl Drop for TestFuture {
    fn drop(&mut self) {
        TestState::with(|s| {
            s.task_dropped = true;
            assert!(!s.panic_on_drop, "task panic on drop");
        });
    }
}

struct TestFutureOutput();

impl Drop for TestFutureOutput {
    fn drop(&mut self) {
        TestState::with(|s| {
            s.output_dropped = true;
            assert!(!s.output_panic_on_drop, "output panic on drop");
        });
    }
}

#[allow(clippy::struct_excessive_bools)]
pub(crate) struct TestState {
    pub(crate) panic_on_run: bool,
    pub(crate) panic_on_drop: bool,
    pub(crate) output_panic_on_drop: bool,
    pub(crate) num_polls: usize,
    pub(crate) task_dropped: bool,
    pub(crate) output_dropped: bool,
    pub(crate) return_pending: bool,
    pub(crate) store_waker: bool,
    pub(crate) stashed_waker: Option<Waker>,
}

impl TestState {
    fn new() -> Self {
        TestState {
            panic_on_run: false,
            panic_on_drop: false,
            output_panic_on_drop: false,
            num_polls: 0,
            task_dropped: false,
            output_dropped: false,
            return_pending: false,
            store_waker: false,
            stashed_waker: None,
        }
    }

    /// Set the TLS for the [`TestState`].
    ///
    /// When the return goes out of scope, the [`TestState`] will
    /// automatically be reset.
    pub(crate) fn enter() -> impl Drop {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                STATE.with(|v| v.borrow_mut().take());
            }
        }
        STATE.with(|v| {
            v.borrow_mut().replace(TestState::new());
        });
        Reset
    }

    pub(crate) fn with<U>(f: impl Fn(&mut TestState) -> U) -> U {
        STATE.with(|v| {
            let inner = &mut *v.borrow_mut();
            let state = inner.as_mut().expect("must call TestState::enter first");
            f(state)
        })
    }
}

thread_local! {static STATE: RefCell<Option<TestState>> = const { RefCell::new(None) }}

async fn yield_now() {
    struct YieldNow(bool);
    impl std::future::Future for YieldNow {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.get_mut().0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    YieldNow(false).await;
}
