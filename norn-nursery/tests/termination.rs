//! Regression coverage for scope termination ordering.

use std::cell::Cell;
use std::future::pending;
use std::rc::Rc;

use norn_executor::park::SpinPark;
use norn_executor::LocalExecutor;

/// `Scope::terminate` cancels every task, so a body that terminates the
/// scope must not let already-queued children run first.
#[test]
fn body_termination_does_not_poll_queued_children() {
    let polls = Rc::new(Cell::new(0));
    let first = Rc::clone(&polls);
    let second = Rc::clone(&polls);
    let mut ex = LocalExecutor::new(SpinPark);
    let out = ex.block_on(async move {
        norn_nursery::scope!(|scope| -> &'static str {
            std::mem::drop(scope.spawn(async move |_| {
                first.set(first.get() + 1);
                pending::<()>().await;
            }));
            std::mem::drop(scope.spawn(async move |_| {
                second.set(second.get() + 1);
                pending::<()>().await;
            }));
            let _: () = scope.terminate("stop").await;
            unreachable!()
        })
        .await
    });
    assert_eq!(out, "stop");
    assert_eq!(polls.get(), 0, "a child ran after the body terminated");
}
