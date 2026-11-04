//! Dropping an executor over a timer [`Driver`] must tolerate an existing
//! timer context and destroyed thread-local storage.
use std::cell::RefCell;

use norn_executor::park::SpinPark;
use norn_executor::LocalExecutor;
use norn_timer::{Clock, Driver, Handle};

type Runtime = LocalExecutor<Driver<SpinPark>>;

fn runtime() -> Runtime {
    LocalExecutor::new(Driver::new(SpinPark, Clock::simulated()))
}

thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = RefCell::new(Some(runtime()));
}

#[test]
fn timer_executor_in_thread_local_can_be_dropped_at_thread_exit() {
    let worker = std::thread::spawn(|| {
        // The runtime slot is registered before the timer context slot, so
        // the context slot is destroyed first at thread exit.
        RUNTIME.with(|runtime| {
            let value = runtime
                .borrow_mut()
                .as_mut()
                .unwrap()
                .block_on(async { 40 + 2 });
            assert_eq!(value, 42);
        });
    });
    worker
        .join()
        .expect("dropping a thread-local timer executor must not panic or abort");
}

#[test]
fn dropping_idle_timer_executor_inside_another_restores_context() {
    let mut outer = runtime();
    outer.block_on(async {
        drop(runtime());
        // The outer timer context is restored after the inner teardown.
        let _handle = Handle::current();
    });
}
