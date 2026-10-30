//! Reentrant join-waker registration regressions, also exercised under Miri.
use std::cell::RefCell;
use std::future::{pending, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, RawWaker, RawWakerVTable, Wake, Waker};

use norn_task::TaskQueue;

thread_local! {
    static SHUTDOWN_QUEUE: RefCell<Option<TaskQueue>> = const { RefCell::new(None) };
}

struct ShutdownOnDrop;
struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
// The owned destructor callback is the point of this waker.
#[allow(clippy::manual_noop_waker)]
impl Wake for ShutdownOnDrop {
    fn wake(self: Arc<Self>) {}
}
impl Drop for ShutdownOnDrop {
    fn drop(&mut self) {
        shutdown_queue();
    }
}

fn shutdown_queue() {
    SHUTDOWN_QUEUE.with(|slot| {
        let queue = slot.borrow_mut().take();
        if let Some(queue) = queue {
            queue.shutdown();
        }
    });
}

#[test]
fn replacing_join_waker_allows_old_waker_destructor_to_shutdown_queue() {
    let queue = TaskQueue::new();
    SHUTDOWN_QUEUE.with(|slot| *slot.borrow_mut() = Some(queue.clone()));
    let mut join = std::pin::pin!(queue.spawn(pending::<()>()));
    let old_waker = Waker::from(Arc::new(ShutdownOnDrop));
    assert!(join
        .as_mut()
        .poll(&mut Context::from_waker(&old_waker))
        .is_pending());
    // Make the task header own the final strong reference to ShutdownOnDrop.
    drop(old_waker);
    // Replacing it drops arbitrary user state. Its destructor reenters the
    // header through TaskQueue::shutdown -> notify_join_handle.
    let notified = Arc::new(CountWake(AtomicUsize::new(0)));
    let new_waker = Waker::from(notified.clone());
    let result = join.as_mut().poll(&mut Context::from_waker(&new_waker));
    assert!(
        result.is_ready() || notified.0.load(Ordering::Relaxed) > 0,
        "shutdown during waker registration must return Ready or notify the new waker"
    );
}

// Each raw waker owns one Arc<CountWake>. All callbacks preserve the Arc
// ownership rules and use atomic counts, so the waker is Send + Sync. The
// clone callback additionally shuts down a queue local to its calling thread.
static SHUTDOWN_ON_CLONE: RawWakerVTable = RawWakerVTable::new(
    |data| unsafe {
        Arc::increment_strong_count(data.cast::<CountWake>());
        shutdown_queue();
        RawWaker::new(data, &SHUTDOWN_ON_CLONE)
    },
    |data| unsafe { Arc::from_raw(data.cast::<CountWake>()).wake() },
    |data| unsafe {
        (*data.cast::<CountWake>())
            .0
            .fetch_add(1, Ordering::Relaxed);
    },
    |data| unsafe { drop(Arc::from_raw(data.cast::<CountWake>())) },
);

#[test]
fn cloning_join_waker_can_complete_before_registration() {
    let queue = TaskQueue::new();
    let mut join = std::pin::pin!(queue.spawn(pending::<()>()));
    let old_waker = Waker::from(Arc::new(CountWake(AtomicUsize::new(0))));
    assert!(join
        .as_mut()
        .poll(&mut Context::from_waker(&old_waker))
        .is_pending());

    SHUTDOWN_QUEUE.with(|slot| *slot.borrow_mut() = Some(queue.clone()));
    let notified = Arc::new(CountWake(AtomicUsize::new(0)));
    // Safety: the vtable owns and reference-counts this Arc as described above.
    let new_waker = unsafe {
        Waker::from_raw(RawWaker::new(
            Arc::into_raw(notified.clone()).cast(),
            &SHUTDOWN_ON_CLONE,
        ))
    };
    let result = join.as_mut().poll(&mut Context::from_waker(&new_waker));
    assert!(
        result.is_ready() || notified.0.load(Ordering::Relaxed) > 0,
        "completion during clone must return Ready or notify the new waker"
    );
}
