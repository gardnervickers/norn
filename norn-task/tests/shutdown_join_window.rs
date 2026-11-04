//! Shutdown marks an idle task complete before its future has been destroyed
//! and its cancellation stored. User destructors run in that window, and a
//! join handle polled from there must wait for the cancellation result.
use std::cell::{Cell, RefCell};
use std::future::{poll_fn, Future};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use norn_task::{JoinHandle, RegisteredTask, Runnable, Schedule, TaskError, TaskQueue, TaskSet};

/// A scheduler that runs woken tasks inline, as the reentrancy regression
/// `inline_scheduler_may_run_the_last_task_reference` already permits.
struct Inline {
    set: TaskSet,
}

struct InlineScheduler(Rc<Inline>);

impl Schedule for InlineScheduler {
    fn schedule(&self, runnable: Runnable) {
        runnable.run();
    }

    fn unbind(&self, registered: &RegisteredTask) {
        unsafe { self.0.set.remove(registered) };
    }
}

/// Wakes the consumer when the producer's future is destroyed, like a
/// channel sender closing its channel.
struct CloseOnDrop(Rc<RefCell<Option<Waker>>>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some(waker) = self.0.borrow_mut().take() {
            waker.wake();
        }
    }
}

#[test]
fn consumer_woken_by_cancelled_producer_sees_its_cancellation() {
    let inline = Rc::new(Inline {
        set: TaskSet::new(),
    });
    let consumer_waker = Rc::new(RefCell::new(None::<Waker>));
    let producer_join = Rc::new(RefCell::new(None::<JoinHandle<()>>));

    // Producer: pending forever; its future owns the "sender".
    let sender = CloseOnDrop(Rc::clone(&consumer_waker));
    let producer = async move {
        let _sender = sender;
        std::future::pending::<()>().await;
    };
    // Safety: everything is 'static.
    let (producer_runnable, producer_handle) = unsafe {
        inline
            .set
            .bind(producer, InlineScheduler(Rc::clone(&inline)))
    };
    producer_runnable.unwrap().run();
    *producer_join.borrow_mut() = Some(producer_handle);

    // Consumer: waits for the "channel" to close, then joins the producer.
    let task_waker = Rc::clone(&consumer_waker);
    let task_join = Rc::clone(&producer_join);
    let mut registered = false;
    let consumer = poll_fn(move |cx| {
        if !std::mem::replace(&mut registered, true) {
            *task_waker.borrow_mut() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let mut join = task_join.borrow_mut();
        let join = join.as_mut().unwrap();
        Pin::new(join).poll(cx)
    });
    let (consumer_runnable, consumer_handle) = unsafe {
        inline
            .set
            .bind(consumer, InlineScheduler(Rc::clone(&inline)))
    };
    consumer_runnable.unwrap().run();

    // Shutting down cancels the producer first. Its future's destructor wakes
    // the consumer, which joins the producer while the producer is COMPLETE
    // but its cancellation result has not been stored yet.
    inline.set.shutdown();

    let mut consumer_handle = consumer_handle;
    let consumer_result =
        Pin::new(&mut consumer_handle).poll(&mut Context::from_waker(Waker::noop()));
    let Poll::Ready(consumer_result) = consumer_result else {
        panic!("the consumer finished or was cancelled during shutdown");
    };
    match consumer_result {
        // The consumer observed the producer's cancellation.
        Ok(Err(producer_error)) => assert!(producer_error.is_cancelled()),
        // Or the consumer itself was cancelled by the shutdown.
        Err(error) if error.is_cancelled() => {}
        Err(error) => panic!(
            "joining a task during its shutdown panicked inside norn-task: {:?}",
            panic_message(error)
        ),
        Ok(Ok(())) => panic!("producer never completes"),
    }
}

/// The same window reached without a custom scheduler: a destructor that
/// polls the task's own join handle while the queue cancels the task.
struct PollOwnJoinOnDrop {
    join: Rc<RefCell<Option<JoinHandle<()>>>>,
    poll_panicked: Rc<Cell<Option<bool>>>,
    polled_pending: Rc<Cell<bool>>,
}

impl Drop for PollOwnJoinOnDrop {
    fn drop(&mut self) {
        let mut join = self.join.borrow_mut();
        let join = join.as_mut().unwrap();
        // Catch the panic so it does not hit the abort-on-panic guard around
        // future destruction.
        let polled = panic::catch_unwind(AssertUnwindSafe(|| {
            Pin::new(join).poll(&mut Context::from_waker(Waker::noop()))
        }));
        self.poll_panicked.set(Some(polled.is_err()));
        self.polled_pending
            .set(polled.is_ok_and(|polled| polled.is_pending()));
    }
}

#[test]
fn polling_join_from_cancelled_future_destructor_does_not_panic() {
    let queue = TaskQueue::new();
    let join = Rc::new(RefCell::new(None));
    let poll_panicked = Rc::new(Cell::new(None));
    let polled_pending = Rc::new(Cell::new(false));
    let guard = PollOwnJoinOnDrop {
        join: Rc::clone(&join),
        poll_panicked: Rc::clone(&poll_panicked),
        polled_pending: Rc::clone(&polled_pending),
    };
    let handle = queue.spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    *join.borrow_mut() = Some(handle);

    queue.shutdown();

    assert_eq!(
        poll_panicked.get(),
        Some(false),
        "polling a join handle while its task is being cancelled panicked"
    );
    assert!(
        polled_pending.get(),
        "the join handle resolved before the cancellation was stored"
    );
    let mut join = join.borrow_mut().take().unwrap();
    let Poll::Ready(result) = Pin::new(&mut join).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("the join handle did not resolve after shutdown");
    };
    assert!(result.unwrap_err().is_cancelled());
}

fn panic_message(error: TaskError) -> String {
    let payload = error.into_panic().unwrap();
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_default()
}
