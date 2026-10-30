//! Test the basic cases of immediately completing futures.
use std::cell::RefCell;
use std::rc::Rc;

use futures::FutureExt;

use crate::JoinHandle;

use super::{yield_now, TestFuture, TestState};

#[test]
fn drop_and_abort() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);

    TestState::with(|v| {
        assert!(!v.task_dropped);
    });
    spawner.shutdown();
    TestState::with(|v| {
        assert!(v.task_dropped);
    });
    handle.abort();
}

#[test]
fn abort_and_drop() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);

    handle.abort();
    TestState::with(|v| {
        assert!(!v.task_dropped);
    });
    spawner.shutdown();
    TestState::with(|v| {
        assert!(v.task_dropped);
    });
}

#[test]
fn abort_twice() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);

    handle.abort();
    handle.abort();
    TestState::with(|v| {
        assert!(!v.task_dropped);
    });
    spawner.shutdown();
    TestState::with(|v| {
        assert!(v.task_dropped);
    });
}

#[test]
fn drop_handle_and_run() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);
    drop(handle);
    spawner.next().unwrap().run();
    TestState::with(|v| {
        assert!(v.task_dropped);
        assert_eq!(v.num_polls, 1);
        assert!(v.output_dropped);
    });
}

#[test]
fn run_and_drop_handle() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);
    spawner.next().unwrap().run();
    drop(handle);
    TestState::with(|v| {
        assert!(v.task_dropped);
        assert_eq!(v.num_polls, 1);
        assert!(v.output_dropped);
    });
}

#[test]
fn abort_and_run() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);
    handle.abort();
    spawner.next().unwrap().run();
    TestState::with(|v| {
        assert!(v.task_dropped);
        assert_eq!(v.num_polls, 0);
        assert!(!v.output_dropped);
    });
    assert!(handle.now_or_never().unwrap().is_err());
}

#[test]
fn run_and_abort() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);
    spawner.next().unwrap().run();
    handle.abort();
    TestState::with(|v| {
        assert!(v.task_dropped);
        assert_eq!(v.num_polls, 1);
        assert!(!v.output_dropped);
    });
    assert!(handle.now_or_never().unwrap().is_ok());
    TestState::with(|v| {
        assert!(v.output_dropped);
    });
}

#[test]
fn run_and_self_abort() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let howd_this_get_here = Rc::new(RefCell::new(None));
    let put_handle_here = Rc::clone(&howd_this_get_here);
    let handle = spawner.spawn(async move {
        let handle: JoinHandle<()> = howd_this_get_here.borrow_mut().take().unwrap();
        handle.abort();
    });

    // Put the tasks join handle in the refcell, which the task will then try to abort.
    put_handle_here.borrow_mut().replace(handle);
    spawner.next().unwrap().run();
}

#[test]
fn run_and_self_abort_and_yield() {
    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let howd_this_get_here = Rc::new(RefCell::new(None));
    let put_handle_here = Rc::clone(&howd_this_get_here);
    let handle = spawner.spawn(async move {
        let handle: JoinHandle<()> = howd_this_get_here.borrow_mut().take().unwrap();
        handle.abort();
        // This will allow us to hit the weird case where a task is aborted while it is running,
        // by itself, and it also does not immediately finish (returns Poll::Pending).
        yield_now().await;
    });

    // Put the tasks join handle in the refcell, which the task will then try to abort.
    put_handle_here.borrow_mut().replace(handle);
    spawner.next().unwrap().run();
}

#[test]
fn drop_handle_releases_join_waker() {
    use std::sync::Arc;
    use std::task::{Context, Wake, Waker};

    #[derive(Default)]
    struct TrackedWaker(std::sync::atomic::AtomicBool);

    impl Wake for TrackedWaker {
        fn wake(self: Arc<Self>) {
            self.0.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    let _e = TestState::enter();
    let spawner = super::TestSpawner::new();
    let mut handle = spawner.spawn(std::future::pending::<()>());
    let join_waker = Arc::new(TrackedWaker::default());
    let waker = Waker::from(Arc::clone(&join_waker));
    assert!(handle
        .poll_unpin(&mut Context::from_waker(&waker))
        .is_pending());
    drop(waker);
    assert_eq!(Arc::strong_count(&join_waker), 2);

    drop(handle);
    assert_eq!(
        Arc::strong_count(&join_waker),
        1,
        "dropping the handle of an incomplete task must release its join waker"
    );
    spawner.shutdown();
    assert!(!join_waker.0.load(std::sync::atomic::Ordering::Relaxed));
}
