//! Destructor and panic-payload re-entry into the task system while
//! shutdown or a JoinHandle drop is destroying a task's future or output.
//! Run under Miri; these exercise FutureCell's lifecycle aliasing rules.
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use norn_task::{JoinHandle, TaskQueue};

type Slot<T> = Rc<RefCell<Option<JoinHandle<T>>>>;

fn noop_cx() -> Context<'static> {
    Context::from_waker(Waker::noop())
}

/// Output whose destructor polls, then drops, its own task's join handle.
struct ReentrantOutput {
    slot: Slot<ReentrantOutput>,
    dropped: Rc<Cell<u32>>,
}
impl Drop for ReentrantOutput {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
        let j = self.slot.borrow_mut().take();
        if let Some(mut j) = j {
            let p = Pin::new(&mut j).poll(&mut noop_cx());
            // Either still running (pending) or a fresh outcome.
            if p.is_pending() {
                *self.slot.borrow_mut() = Some(j);
            }
        }
    }
}

/// Future that shuts its own queue down during poll and returns Ready with an
/// output whose destructor re-enters. Queue shutdown wins, so the output is
/// dropped by `cancel` (drop_output while RUNNING).
struct ShutdownThenReady {
    q: TaskQueue,
    slot: Slot<ReentrantOutput>,
    dropped: Rc<Cell<u32>>,
}
impl Future for ShutdownThenReady {
    type Output = ReentrantOutput;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<ReentrantOutput> {
        // Poll own join handle during poll.
        let j = self.slot.borrow_mut().take();
        if let Some(mut j) = j {
            assert!(Pin::new(&mut j).poll(&mut noop_cx()).is_pending());
            j.abort();
            *self.slot.borrow_mut() = Some(j);
        }
        self.q.shutdown();
        Poll::Ready(ReentrantOutput {
            slot: self.slot.clone(),
            dropped: self.dropped.clone(),
        })
    }
}

#[test]
fn ready_output_dropped_by_deferred_cancel_reenters() {
    let q = TaskQueue::new();
    let slot: Slot<ReentrantOutput> = Rc::new(RefCell::new(None));
    let dropped = Rc::new(Cell::new(0));
    let h = q.spawn(ShutdownThenReady {
        q: q.clone(),
        slot: slot.clone(),
        dropped: dropped.clone(),
    });
    *slot.borrow_mut() = Some(h);
    q.next().unwrap().run();
    assert_eq!(dropped.get(), 1);
    let mut j = slot.borrow_mut().take().unwrap();
    let Poll::Ready(r) = Pin::new(&mut j).poll(&mut noop_cx()) else {
        panic!("not ready")
    };
    assert!(matches!(r, Err(ref e) if e.is_cancelled()));
}

// Panic payload whose destructor re-enters its own task.
thread_local! {
    static PAYLOAD_CTX: RefCell<Option<(Slot<()>, TaskQueue)>> = const { RefCell::new(None) };
}
struct Payload;
impl Drop for Payload {
    fn drop(&mut self) {
        let Some((slot, q)) = PAYLOAD_CTX.with(|c| c.borrow_mut().take()) else {
            return;
        };
        let j = slot.borrow_mut().take();
        if let Some(mut j) = j {
            let _ = Pin::new(&mut j).poll(&mut noop_cx());
            drop(j);
        }
        q.shutdown();
        drop(q.spawn(async {}));
    }
}

struct FieldGuard {
    slot: Slot<()>,
}
impl Drop for FieldGuard {
    fn drop(&mut self) {
        let j = self.slot.borrow_mut().take();
        if let Some(mut j) = j {
            assert!(Pin::new(&mut j).poll(&mut noop_cx()).is_pending());
            *self.slot.borrow_mut() = Some(j);
        }
    }
}

struct Panicker {
    q: TaskQueue,
    slot: Slot<()>,
    _g: FieldGuard,
}
impl Future for Panicker {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        self.q.shutdown();
        PAYLOAD_CTX.with(|c| *c.borrow_mut() = Some((self.slot.clone(), self.q.clone())));
        std::panic::panic_any(Payload);
    }
}

#[test]
fn panic_with_reentrant_payload_and_field_destructor_under_shutdown() {
    let q = TaskQueue::new();
    let slot: Slot<()> = Rc::new(RefCell::new(None));
    let h = q.spawn(Panicker {
        q: q.clone(),
        slot: slot.clone(),
        _g: FieldGuard { slot: slot.clone() },
    });
    *slot.borrow_mut() = Some(h);
    q.next().unwrap().run();
    assert!(slot.borrow().is_none());
}

/// Self-referential (!Unpin) async future that owns its own join handle,
/// polls it during poll, and whose locals' destructors drop it.
#[test]
fn unpin_async_owning_join_handle() {
    let q = TaskQueue::new();
    let slot: Slot<u32> = Rc::new(RefCell::new(None));
    let s2 = slot.clone();
    let q2 = q.clone();
    let h = q.spawn(async move {
        let local = [1u32, 2, 3];
        let r = &local;
        let g = FieldGuardU { slot: s2.clone() };
        std::future::poll_fn(|_| {
            let j = s2.borrow_mut().take();
            if let Some(mut j) = j {
                assert!(Pin::new(&mut j).poll(&mut noop_cx()).is_pending());
                *s2.borrow_mut() = Some(j);
            }
            Poll::Ready(())
        })
        .await;
        q2.shutdown();
        drop(g);
        r[0] + r[2]
    });
    *slot.borrow_mut() = Some(h);
    q.next().unwrap().run();
    // Shutdown during the poll wins over the produced value.
    let mut j = slot.borrow_mut().take().unwrap();
    let Poll::Ready(r) = Pin::new(&mut j).poll(&mut noop_cx()) else {
        panic!()
    };
    assert!(r.unwrap_err().is_cancelled());
}

struct FieldGuardU {
    slot: Slot<u32>,
}
impl Drop for FieldGuardU {
    fn drop(&mut self) {
        let j = self.slot.borrow_mut().take();
        if let Some(mut j) = j {
            let _ = Pin::new(&mut j).poll(&mut noop_cx());
            *self.slot.borrow_mut() = Some(j);
        }
    }
}

/// bind doc: a task that closes its set inside its own poll keeps its future
/// until the poll returns, then it is destroyed.
struct Flag(Rc<Cell<bool>>);
impl Drop for Flag {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[test]
fn bind_doc_deferred_destroy_on_pending() {
    let q = TaskQueue::new();
    let dropped = Rc::new(Cell::new(false));
    let d2 = dropped.clone();
    let q2 = q.clone();
    let flag = Flag(dropped.clone());
    let h = q.spawn(async move {
        let _f = flag;
        q2.shutdown();
        assert!(!d2.get(), "future destroyed mid-poll");
        std::future::pending::<()>().await;
    });
    q.next().unwrap().run();
    assert!(dropped.get(), "future not destroyed when poll returned");
    let mut h = h;
    assert!(
        matches!(Pin::new(&mut h).poll(&mut noop_cx()), Poll::Ready(Err(e)) if e.is_cancelled())
    );
}

/// Output destructor (destroyed via drop_join_handle) that shuts the queue
/// down and spawns/runs more work.
struct OutQ(TaskQueue, Slot<OutQ>);
impl Drop for OutQ {
    fn drop(&mut self) {
        let j = self.1.borrow_mut().take();
        drop(j);
        self.0.shutdown();
        drop(self.0.spawn(async {}));
        while let Some(r) = self.0.next() {
            r.run();
        }
    }
}

#[test]
fn output_destroy_via_join_drop_reenters() {
    let q = TaskQueue::new();
    let slot: Slot<OutQ> = Rc::new(RefCell::new(None));
    let q2 = q.clone();
    let s2 = slot.clone();
    let h = q.spawn(async move { OutQ(q2, s2) });
    q.next().unwrap().run();
    drop(h);
    drop(q);
}
