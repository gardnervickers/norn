#![cfg(target_os = "linux")]
//! Dropping an executor over a uring [`Driver`] must tolerate an existing
//! driver context and destroyed thread-local storage.
use std::cell::RefCell;

use norn_executor::LocalExecutor;
use norn_uring::Driver;

type Runtime = LocalExecutor<Driver>;

fn runtime() -> Runtime {
    LocalExecutor::new(Driver::new(io_uring::IoUring::builder(), 8).unwrap())
}

thread_local! {
    static RUNTIME: RefCell<Option<Runtime>> = RefCell::new(Some(runtime()));
}

#[test]
fn uring_executor_in_thread_local_can_be_dropped_at_thread_exit() {
    let worker = std::thread::spawn(|| {
        // The runtime slot is registered before the driver context slot, so
        // the context slot is destroyed first at thread exit.
        RUNTIME.with(|runtime| {
            runtime
                .borrow_mut()
                .as_mut()
                .unwrap()
                .block_on(norn_uring::noop());
        });
    });
    worker
        .join()
        .expect("dropping a thread-local uring executor must not panic or abort");
}

#[test]
fn dropping_idle_uring_executor_inside_another_restores_context() {
    let mut outer = runtime();
    outer.block_on(async {
        drop(runtime());
        // The outer driver context is restored after the inner teardown.
        norn_uring::noop().await;
    });
}
