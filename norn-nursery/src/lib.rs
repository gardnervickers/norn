//! Scoped async concurrency on top of `norn-task`.
//!
//! `norn-nursery` provides a `scope!` macro that allows spawning child tasks
//! which may borrow data from outside the scope.
//!
//! This crate is inspired by the [`moro`](https://github.com/nikomatsakis/moro)
//! experiment.
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    clippy::missing_safety_doc
)]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::{pending, Future};
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use norn_task::{JoinHandle, Runnable, Schedule, TaskSet};
use pin_project_lite::pin_project;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Creates an async scope within which child jobs can be spawned.
///
/// Child jobs may borrow from stack values that outlive the scope and they are
/// all driven to completion before the scope completes, unless the scope is
/// terminated early via [`Scope::terminate`].
///
/// # Examples
///
/// ```rust
/// use norn_executor::park::SpinPark;
/// use norn_executor::LocalExecutor;
///
/// let mut ex = LocalExecutor::new(SpinPark);
/// let output = ex.block_on(async {
///     let values = vec![10, 11, 12];
///     let suffix = String::from("!");
///     norn_nursery::scope!(|scope| {
///         // These child futures borrow from stack data outside the scope body:
///         // `values` and `suffix` are not `'static`.
///         let first = scope.spawn(async {
///             values.iter().sum::<i32>()
///         });
///         let second = scope.spawn(async {
///             suffix.len()
///         });
///
///         first.await.unwrap() + second.await.unwrap() as i32
///     })
///     .await
/// });
///
/// assert_eq!(output, 34);
/// ```
///
/// ```rust
/// use norn_executor::park::SpinPark;
/// use norn_executor::LocalExecutor;
///
/// let mut ex = LocalExecutor::new(SpinPark);
/// let output = ex.block_on(async {
///     norn_nursery::scope!(|scope| -> Result<(), &'static str> {
///         std::mem::drop(scope.spawn(async move {
///             let _: () = scope.terminate(Err("stop")).await;
///         }));
///         Ok(())
///     })
///     .await
/// });
///
/// assert_eq!(output, Err("stop"));
/// ```
#[macro_export]
macro_rules! scope {
    (|$scope:ident| -> $result:ty { $($body:tt)* }) => {{
        $crate::scope_fn::<$result, _>(|$scope| {
            let future = async move { $($body)* };
            Box::pin(future)
        })
    }};
    (|$scope:ident| $body:expr) => {{
        $crate::scope_fn(|$scope| {
            let future = async move { $body };
            Box::pin(future)
        })
    }};
}

/// A scoped async concurrency handle.
///
/// Use [`Scope::spawn`] to spawn child tasks that are owned by the scope.
pub struct Scope<'scope, 'env, R> {
    shared: Rc<Shared>,
    terminated: RefCell<Option<R>>,
    _marker: PhantomData<&'scope &'env ()>,
}

impl<'scope, 'env, R> Scope<'scope, 'env, R> {
    fn new() -> Self {
        Self {
            shared: Rc::new(Shared::default()),
            terminated: RefCell::new(None),
            _marker: PhantomData,
        }
    }

    /// Spawn a child task into this scope.
    ///
    /// The task may borrow data from outside the scope. The scope will not
    /// complete until all spawned child tasks have either finished or the scope
    /// has been terminated.
    pub fn spawn<F>(&'scope self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'scope,
    {
        let scheduler = Scheduler {
            shared: Rc::clone(&self.shared),
        };

        // Safety: The caller only receives `&Scope` while the surrounding
        // `ScopeBody` exists and guarantees that `clear` is called before
        // dropping captures borrowed by child futures.
        let (runnable, handle) = unsafe { self.shared.taskset.bind(future, scheduler) };
        if let Some(runnable) = runnable {
            self.shared.active.set(self.shared.active.get() + 1);
            self.shared.runqueue.borrow_mut().push_back(runnable);
            self.shared.wake_scope();
        }
        handle
    }

    /// Terminate the scope with `value`.
    ///
    /// All tasks are cancelled when the scope is next polled.
    ///
    /// The returned future never resolves; awaiting it is a convenient way to
    /// stop the current task immediately.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use norn_executor::park::SpinPark;
    /// use norn_executor::LocalExecutor;
    ///
    /// let mut ex = LocalExecutor::new(SpinPark);
    /// let output = ex.block_on(async {
    ///     norn_nursery::scope!(|scope| -> Result<(), &'static str> {
    ///         std::mem::drop(scope.spawn(async move {
    ///             let _: () = scope.terminate(Err("cancelled")).await;
    ///         }));
    ///         Ok(())
    ///     })
    ///     .await
    /// });
    ///
    /// assert_eq!(output, Err("cancelled"));
    /// ```
    pub fn terminate<T>(&'scope self, value: R) -> impl Future<Output = T> + 'scope
    where
        T: 'scope,
    {
        if self.terminated.borrow().is_none() {
            *self.terminated.borrow_mut() = Some(value);
            self.shared.wake_scope();
        }
        pending()
    }

    fn poll_tasks(&self) {
        loop {
            let runnable = self.shared.runqueue.borrow_mut().pop_front();
            let Some(runnable) = runnable else {
                break;
            };
            runnable.run();
            if self.terminated.borrow().is_some() {
                break;
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.shared.active.get() == 0 && self.shared.runqueue.borrow().is_empty()
    }

    fn clear(&self) {
        self.shared.taskset.shutdown();
        self.shared.runqueue.borrow_mut().clear();
        self.shared.active.set(0);
        self.shared.waker.borrow_mut().take();
    }

    fn set_waker(&self, waker: &Waker) {
        *self.shared.waker.borrow_mut() = Some(waker.clone());
    }

    fn take_terminated(&self) -> Option<R> {
        self.terminated.borrow_mut().take()
    }
}

impl<'scope, 'env, R> std::fmt::Debug for Scope<'scope, 'env, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scope").finish()
    }
}

impl<'scope, 'env, R> Drop for Scope<'scope, 'env, R> {
    fn drop(&mut self) {
        self.clear();
    }
}

#[derive(Default)]
struct Shared {
    waker: RefCell<Option<Waker>>,
    runqueue: RefCell<VecDeque<Runnable>>,
    taskset: TaskSet,
    active: Cell<usize>,
}

impl Shared {
    fn wake_scope(&self) {
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

#[derive(Clone)]
struct Scheduler {
    shared: Rc<Shared>,
}

impl Schedule for Scheduler {
    fn schedule(&self, runnable: Runnable) {
        self.shared.runqueue.borrow_mut().push_back(runnable);
        self.shared.wake_scope();
    }

    fn unbind(&self, registered: &norn_task::RegisteredTask) {
        unsafe {
            self.shared.taskset.remove(registered);
        }
        self.shared
            .active
            .set(self.shared.active.get().saturating_sub(1));
        self.shared.wake_scope();
    }
}

pin_project! {
    /// The future returned by [`scope_fn`] and [`scope!`].
    #[must_use = "futures do nothing unless polled or awaited"]
    pub struct ScopeBody<'env, R: 'env, F>
    where
        F: Future<Output = R>,
    {
        #[pin]
        body_future: Option<F>,
        body_result: Option<R>,
        scope: Rc<Scope<'env, 'env, R>>,
    }
}

impl<'env, R, F> ScopeBody<'env, R, F>
where
    F: Future<Output = R>,
{
    fn new(body_future: F, scope: Rc<Scope<'env, 'env, R>>) -> Self {
        Self {
            body_future: Some(body_future),
            body_result: None,
            scope,
        }
    }
}

impl<'env, R, F> std::fmt::Debug for ScopeBody<'env, R, F>
where
    F: Future<Output = R>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopeBody").finish()
    }
}

impl<'env, R, F> Future for ScopeBody<'env, R, F>
where
    F: Future<Output = R>,
{
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        if let Some(terminated) = this.scope.take_terminated() {
            this.scope.clear();
            this.body_future.set(None);
            this.body_result.take();
            return Poll::Ready(terminated);
        }

        if let Some(body_future) = this.body_future.as_mut().as_pin_mut() {
            if let Poll::Ready(result) = body_future.poll(cx) {
                *this.body_result = Some(result);
                this.body_future.set(None);
            }
        }

        this.scope.poll_tasks();

        if let Some(terminated) = this.scope.take_terminated() {
            this.scope.clear();
            this.body_future.set(None);
            this.body_result.take();
            return Poll::Ready(terminated);
        }

        if this.scope.is_idle() {
            if let Some(result) = this.body_result.take() {
                return Poll::Ready(result);
            }
        }

        this.scope.set_waker(cx.waker());
        Poll::Pending
    }
}

/// Creates a new nursery scope.
///
/// Prefer using the [`scope!`] macro.
pub fn scope_fn<'env, R, B>(body: B) -> ScopeBody<'env, R, BoxFuture<'env, R>>
where
    R: 'env,
    for<'scope> B: FnOnce(&'scope Scope<'scope, 'env, R>) -> BoxFuture<'scope, R>,
{
    let scope = Rc::new(Scope::new());

    // Safety: `scope` is heap allocated and moved by pointer, so the address
    // of the underlying value remains stable while `body_future` is alive.
    let scope_ref: *const Scope<'_, 'env, R> = &*scope;
    let body_future = body(unsafe { &*scope_ref });

    ScopeBody::new(body_future, scope)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::future::{pending, Future};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    use norn_executor::park::SpinPark;
    use norn_executor::LocalExecutor;

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    fn poll_once<F>(fut: Pin<&mut F>) -> Poll<F::Output>
    where
        F: Future,
    {
        let waker = Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        fut.poll(&mut cx)
    }

    #[test]
    fn borrows_outer_stack() {
        let mut ex = LocalExecutor::new(SpinPark);
        let out = ex.block_on(async {
            let value = 22;
            crate::scope!(|scope| {
                let child = scope.spawn(async move { value });
                child.await.unwrap() * 4
            })
            .await
        });
        assert_eq!(out, 88);
    }

    #[test]
    fn nested_spawn() {
        let mut ex = LocalExecutor::new(SpinPark);
        let out = ex.block_on(async {
            let base = 3;
            crate::scope!(|scope| {
                let parent = scope.spawn(async move {
                    let child = scope.spawn(async move { base + 1 });
                    child.await.unwrap() * 2
                });
                parent.await.unwrap() + 1
            })
            .await
        });
        assert_eq!(out, 9);
    }

    #[test]
    fn fan_out_borrowed_reads() {
        let mut ex = LocalExecutor::new(SpinPark);
        let out = ex.block_on(async {
            let input = [1_i32, 2, 3, 4, 5, 6, 7, 8];
            crate::scope!(|scope| {
                let mut handles = Vec::new();
                for value in &input {
                    handles.push(scope.spawn(async move { *value * 2 }));
                }

                let mut sum = 0;
                for handle in handles {
                    sum += handle.await.unwrap();
                }
                sum
            })
            .await
        });
        assert_eq!(out, 72);
    }

    #[test]
    fn drop_join_handle_keeps_task_running() {
        let mut ex = LocalExecutor::new(SpinPark);
        let completed = Cell::new(0usize);
        let completed_ref = &completed;
        ex.block_on(async {
            crate::scope!(|scope| {
                for _ in 0..16 {
                    std::mem::drop(scope.spawn(async {
                        completed_ref.set(completed_ref.get() + 1);
                    }));
                }
            })
            .await
        });
        assert_eq!(completed.get(), 16);
    }

    #[test]
    fn abort_cancelled_join_handle() {
        let mut ex = LocalExecutor::new(SpinPark);
        let out = ex.block_on(async {
            crate::scope!(|scope| {
                let handle = scope.spawn(async {
                    pending::<()>().await;
                    1usize
                });
                handle.abort();
                let err = handle.await.unwrap_err();
                assert!(err.is_cancelled());
                11usize
            })
            .await
        });
        assert_eq!(out, 11);
    }

    #[test]
    fn panic_is_reported_via_join_handle() {
        let mut ex = LocalExecutor::new(SpinPark);
        let out = ex.block_on(async {
            crate::scope!(|scope| {
                let handle = scope.spawn(async move {
                    panic!("boom");
                    #[allow(unreachable_code)]
                    0usize
                });
                let err = handle.await.unwrap_err();
                assert!(err.is_panic());
                7usize
            })
            .await
        });
        assert_eq!(out, 7);
    }

    #[test]
    fn terminate_first_value_wins() {
        let mut ex = LocalExecutor::new(SpinPark);
        let out = ex.block_on(async {
            crate::scope!(|scope| -> &'static str {
                std::mem::drop(scope.spawn(async move {
                    let _: () = scope.terminate("first").await;
                }));
                std::mem::drop(scope.spawn(async move {
                    let _: () = scope.terminate("second").await;
                }));
                "body"
            })
            .await
        });
        assert_eq!(out, "first");
    }

    #[test]
    fn terminate_short_circuits_scope() {
        let mut ex = LocalExecutor::new(SpinPark);
        let out = ex.block_on(async {
            crate::scope!(|scope| -> Result<(), &'static str> {
                std::mem::drop(scope.spawn(async move {
                    let _: () = scope.terminate(Err("boom")).await;
                }));
                Ok(())
            })
            .await
        });

        assert_eq!(out, Err("boom"));
    }

    #[test]
    fn drop_partially_polled_scope_drops_borrowing_children() {
        struct DropFlag<'a>(&'a Cell<bool>);

        impl Drop for DropFlag<'_> {
            fn drop(&mut self) {
                self.0.set(true);
            }
        }

        let dropped = Cell::new(false);
        let dropped_ref = &dropped;
        let mut fut = Box::pin(crate::scope!(|scope| {
            std::mem::drop(scope.spawn(async {
                let _flag = DropFlag(dropped_ref);
                pending::<()>().await;
            }));
            pending::<()>().await
        }));

        let poll = poll_once(fut.as_mut());
        assert!(matches!(poll, Poll::Pending));

        drop(fut);
        assert!(dropped.get());
    }
}
