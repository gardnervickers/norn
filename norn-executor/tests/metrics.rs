//! Executor metrics stay right when a panic unwinds out of `block_on`.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use norn_executor::park::{Park, ParkMode, SpinPark, Unpark};
use norn_executor::{ExecutorMetrics, LocalExecutor};

#[derive(Clone, Copy, Debug)]
struct NoopUnpark;

impl Unpark for NoopUnpark {
    fn unpark(&self) {}
}

/// Panics out of its first `park`, then records whether the metrics say the
/// executor is running while it is inside later parks.
struct PanicOnce {
    panicked: Rc<Cell<bool>>,
    metrics: Rc<RefCell<Option<ExecutorMetrics>>>,
    running_in_park: Rc<Cell<Option<bool>>>,
}

impl Park for PanicOnce {
    type Unparker = NoopUnpark;
    type Guard = ();

    fn park(&mut self, _: ParkMode) -> io::Result<()> {
        if !self.panicked.replace(true) {
            panic!("park panicked");
        }
        if let Some(metrics) = &*self.metrics.borrow() {
            self.running_in_park.set(Some(metrics.snapshot().running));
        }
        Ok(())
    }

    fn enter(&self) -> Self::Guard {}

    fn unparker(&self) -> Self::Unparker {
        NoopUnpark
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {}
}

/// A panic out of `park` must not leave the running flag inverted for a
/// reused executor.
#[test]
fn park_panic_leaves_running_flag_correct() {
    let slot = Rc::new(RefCell::new(None));
    let running_in_park = Rc::new(Cell::new(None));
    let mut executor = LocalExecutor::new(PanicOnce {
        panicked: Rc::new(Cell::new(false)),
        metrics: Rc::clone(&slot),
        running_in_park: Rc::clone(&running_in_park),
    });
    let metrics = executor.metrics();
    *slot.borrow_mut() = Some(metrics.clone());

    let first = panic::catch_unwind(AssertUnwindSafe(|| {
        executor.block_on(std::future::pending::<()>())
    }));
    assert!(first.is_err());
    let after_panic = metrics.snapshot();

    let running_in_root = {
        let metrics = metrics.clone();
        executor.block_on(async move {
            let running = metrics.snapshot().running;
            // Yield once so the executor parks before completing.
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                if yielded {
                    return Poll::Ready(());
                }
                yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            })
            .await;
            running
        })
    };
    let after_return = metrics.snapshot();
    assert_eq!(
        (
            after_panic.running,
            running_in_root,
            running_in_park.get(),
            after_return.running
        ),
        (false, true, Some(false), false),
        "after panic {after_panic:?}, after return {after_return:?}"
    );
}

struct PanicOnWake;

impl Wake for PanicOnWake {
    fn wake(self: Arc<Self>) {
        panic!("join waker panicked");
    }
}

/// A join-waker panic unwinds out of `block_on` mid-tick; that tick's polls
/// are still counted.
#[test]
fn polls_of_an_unwinding_tick_are_counted() {
    let mut executor = LocalExecutor::new(SpinPark);
    let metrics = executor.metrics();
    let mut join = executor.handle().spawn(async {});
    let waker = Waker::from(Arc::new(PanicOnWake));
    assert!(Pin::new(&mut join)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(waker);
    let ran = panic::catch_unwind(AssertUnwindSafe(|| {
        executor.block_on(std::future::pending::<()>())
    }));
    assert!(ran.is_err());
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.tasks.finished, 1);
    assert_eq!(snapshot.polls, 1, "{snapshot:?}");
    assert!(!snapshot.running, "{snapshot:?}");
}

/// Records what the metrics say while its own shutdown runs.
struct RecordShutdown {
    metrics: Rc<RefCell<Option<ExecutorMetrics>>>,
    running_in_shutdown: Rc<Cell<Option<bool>>>,
}

impl Park for RecordShutdown {
    type Unparker = NoopUnpark;
    type Guard = ();

    fn park(&mut self, _: ParkMode) -> io::Result<()> {
        Ok(())
    }

    fn enter(&self) -> Self::Guard {}

    fn unparker(&self) -> Self::Unparker {
        NoopUnpark
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {
        if let Some(metrics) = &*self.metrics.borrow() {
            self.running_in_shutdown
                .set(Some(metrics.snapshot().running));
        }
    }
}

/// Records whether the executor reports running when this task is dropped.
struct RecordDrop {
    metrics: ExecutorMetrics,
    running_in_drop: Rc<Cell<Option<bool>>>,
}

impl Drop for RecordDrop {
    fn drop(&mut self) {
        self.running_in_drop
            .set(Some(self.metrics.snapshot().running));
    }
}

/// Dropping tasks is user code and counts as running; the park layer's own
/// shutdown, which waits for the kernel, does not.
#[test]
fn drop_runs_task_destructors_but_not_park_shutdown() {
    let slot = Rc::new(RefCell::new(None));
    let running_in_shutdown = Rc::new(Cell::new(None));
    let running_in_drop = Rc::new(Cell::new(None));
    let executor = LocalExecutor::new(RecordShutdown {
        metrics: Rc::clone(&slot),
        running_in_shutdown: Rc::clone(&running_in_shutdown),
    });
    let metrics = executor.metrics();
    *slot.borrow_mut() = Some(metrics.clone());
    let guard = RecordDrop {
        metrics: metrics.clone(),
        running_in_drop: Rc::clone(&running_in_drop),
    };
    executor
        .handle()
        .spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await
        })
        .detach();
    drop(executor);
    assert_eq!(
        (running_in_drop.get(), running_in_shutdown.get()),
        (Some(true), Some(false))
    );
    assert!(!metrics.snapshot().running);
}
