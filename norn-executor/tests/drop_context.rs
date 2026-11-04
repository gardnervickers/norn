//! Dropping a [`LocalExecutor`] must not depend on the thread-local executor
//! context being empty or even still alive.
//!
//! `Drop` enters the executor context so task destructors can reach it. That
//! must tolerate thread-local storage that has already been destroyed (an
//! executor stored in a `thread_local!`) and an existing context belonging to
//! another executor (an executor dropped inside another executor's task).
use std::cell::RefCell;

use norn_executor::park::SpinPark;
use norn_executor::{spawn, LocalExecutor};

thread_local! {
    static RUNTIME: RefCell<Option<LocalExecutor<SpinPark>>> =
        RefCell::new(Some(LocalExecutor::new(SpinPark)));
}

#[test]
fn executor_in_thread_local_can_be_dropped_at_thread_exit() {
    let worker = std::thread::spawn(|| {
        // Initialize the runtime slot first, then enter the executor context
        // for the first time on this thread. TLS destructors run in reverse
        // registration order, so the executor's own context slot is
        // destroyed before this runtime is dropped.
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
        .expect("dropping a thread-local executor must not panic or abort");
}

#[test]
fn rejected_nested_block_on_is_reported_as_a_task_panic() {
    // The nested `block_on` is rejected with a panic. Unwinding then drops the
    // nested executor while the outer executor's context is installed.
    let mut outer = LocalExecutor::new(SpinPark);
    let result = outer.block_on(async {
        spawn(async {
            let mut nested = LocalExecutor::new(SpinPark);
            nested.block_on(async {});
        })
        .await
    });
    let error = result.expect_err("nested block_on must panic");
    assert!(error.is_panic());
}

#[test]
fn dropping_idle_executor_inside_another_executor_does_not_panic() {
    let mut outer = LocalExecutor::new(SpinPark);

    outer.block_on(async {
        let inner = LocalExecutor::new(SpinPark);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(inner)));
        assert!(
            result.is_ok(),
            "dropping an idle executor panicked inside another executor"
        );
        // The outer executor's context must be restored afterward.
        spawn(async {}).await.unwrap();
    });
}

#[test]
#[cfg_attr(miri, ignore = "spawns a child process")]
fn dropping_nested_executor_during_shutdown_does_not_abort() {
    // A failure here aborts the process, so run the scenario in a child.
    const CHILD: &str = "NORN_EXECUTOR_NESTED_DROP_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let inner = LocalExecutor::new(SpinPark);
        let outer = LocalExecutor::new(SpinPark);
        outer
            .handle()
            .spawn(async move {
                let _inner = inner;
                std::future::pending::<()>().await;
            })
            .detach();
        drop(outer);
        return;
    }

    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("dropping_nested_executor_during_shutdown_does_not_abort")
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "nested executor drop aborted the process: {status}"
    );
}

thread_local! {
    static TASK_RUNTIME: RefCell<Option<LocalExecutor<SpinPark>>> =
        RefCell::new(Some(LocalExecutor::new(SpinPark)));
}

static LOOKUP_PANIC: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

/// Looks up the current executor from a destructor and records the panic.
struct LookupOnDrop;

impl Drop for LookupOnDrop {
    fn drop(&mut self) {
        let result = std::panic::catch_unwind(norn_executor::Handle::current);
        let message = match result {
            Ok(_) => "found an executor".to_owned(),
            Err(payload) => payload
                .downcast_ref::<&str>()
                .map(ToString::to_string)
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_default(),
        };
        *LOOKUP_PANIC.lock().unwrap() = Some(message);
    }
}

#[test]
fn task_destructor_at_thread_exit_reports_no_executor() {
    let worker = std::thread::spawn(|| {
        TASK_RUNTIME.with(|runtime| {
            let mut runtime = runtime.borrow_mut();
            let runtime = runtime.as_mut().unwrap();
            runtime.block_on(async {
                let guard = LookupOnDrop;
                spawn(async move {
                    let _guard = guard;
                    std::future::pending::<()>().await;
                })
                .detach();
            });
        });
    });
    worker
        .join()
        .expect("a task destructor at thread exit must not abort");
    // The executor context is gone by then, and the lookup must say so rather
    // than panic with a thread-local access error.
    let message = LOOKUP_PANIC.lock().unwrap().take();
    assert_eq!(message.as_deref(), Some("executor not set"));
}
