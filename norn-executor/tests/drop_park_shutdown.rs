//! A join waker that panics while `LocalExecutor::drop` shuts down its task
//! queue must not skip `Park::shutdown`; the panic still propagates.
use std::cell::Cell;
use std::future::Future;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use norn_executor::park::{Park, ParkMode, Unpark};
use norn_executor::LocalExecutor;

#[derive(Clone, Copy, Debug)]
struct NoopUnpark;
impl Unpark for NoopUnpark {
    fn unpark(&self) {}
}

#[derive(Debug)]
struct RecordShutdown(Rc<Cell<bool>>);

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
        self.0.set(true);
    }
}

struct PanicOnWake;
impl Wake for PanicOnWake {
    fn wake(self: Arc<Self>) {
        panic!("join waker panicked");
    }
}

#[test]
fn park_shutdown_runs_even_if_a_join_waker_panics_during_executor_drop() {
    let shut_down = Rc::new(Cell::new(false));
    let executor = LocalExecutor::new(RecordShutdown(Rc::clone(&shut_down)));
    let mut join = executor.handle().spawn(std::future::pending::<()>());
    let waker = Waker::from(Arc::new(PanicOnWake));
    assert!(Pin::new(&mut join)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(waker);

    let dropped = panic::catch_unwind(AssertUnwindSafe(|| drop(executor)));
    assert!(dropped.is_err(), "the join waker's panic propagates");
    assert!(
        shut_down.get(),
        "LocalExecutor::drop skipped Park::shutdown after a join waker panicked"
    );
    drop(join);
}
