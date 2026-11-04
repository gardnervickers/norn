//! Polling a `JoinHandle` again after it returned `Ready` panics, with a
//! message that names the actual misuse.
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use norn_task::TaskQueue;

#[test]
fn polling_a_join_handle_after_ready_names_the_misuse() {
    let queue = TaskQueue::new();
    let mut handle = queue.spawn(async { 7_u32 });
    queue.next().expect("task was scheduled").run();

    let mut cx = Context::from_waker(Waker::noop());
    match Pin::new(&mut handle).poll(&mut cx) {
        Poll::Ready(Ok(7)) => {}
        other => panic!("unexpected first poll: {other:?}"),
    }

    let second = panic::catch_unwind(AssertUnwindSafe(|| {
        let _ = Pin::new(&mut handle).poll(&mut cx);
    }));
    let payload = second.expect_err("polling a finished JoinHandle must panic");
    let message = payload
        .downcast_ref::<&str>()
        .map(ToString::to_string)
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default();
    assert!(
        message.contains("polled again after returning Ready"),
        "misleading panic message: {message}"
    );
}
