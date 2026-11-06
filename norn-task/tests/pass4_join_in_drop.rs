//! Pass-4 reproducer: polling a join handle from a destructor that runs while
//! shutdown destroys that task's future (the window #185 made a supported,
//! Pending-returning path).
//!
//! `take_output` (and `begin_drop`/`destroy` via `drop_join_handle`) created a
//! reference to the whole `FutureCell` storage while the future stored there
//! was still being dropped, and a field of that future is borrowed by its own
//! `Drop::drop(&mut self)`. Before FutureCell moved its state out of the
//! storage cell, Miri (Stacked Borrows) reported Undefined Behavior here.
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use norn_task::{JoinHandle, TaskQueue};

type Slot = Rc<RefCell<Option<JoinHandle<()>>>>;

/// Polls the join handle stored in `slot` when dropped.
struct PollJoinOnDrop {
    slot: Slot,
}

impl Drop for PollJoinOnDrop {
    fn drop(&mut self) {
        let join = self.slot.borrow_mut().take();
        if let Some(mut join) = join {
            let polled = Pin::new(&mut join).poll(&mut Context::from_waker(Waker::noop()));
            assert!(polled.is_pending());
            *self.slot.borrow_mut() = Some(join);
        }
    }
}

/// A hand-written future so the guard is a plain field of the stored future.
struct Holder {
    _guard: PollJoinOnDrop,
}

impl Future for Holder {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

#[test]
fn poll_own_join_handle_from_cancelled_future_field_destructor() {
    let queue = TaskQueue::new();
    let slot: Slot = Rc::new(RefCell::new(None));
    let handle = queue.spawn(Holder {
        _guard: PollJoinOnDrop {
            slot: Rc::clone(&slot),
        },
    });
    *slot.borrow_mut() = Some(handle);
    queue.next().unwrap().run();

    queue.shutdown();

    let join = slot.borrow_mut().take().unwrap();
    let mut join = join;
    let Poll::Ready(result) = Pin::new(&mut join).poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("join handle did not resolve after shutdown");
    };
    assert!(result.unwrap_err().is_cancelled());
}

/// Dropping the join handle from the same window goes through
/// `drop_join_handle` -> `FutureCell::destroy` -> `begin_drop`, which also
/// takes `&mut` over the whole cell.
struct DropJoinOnDrop {
    slot: Slot,
}

impl Drop for DropJoinOnDrop {
    fn drop(&mut self) {
        let join = self.slot.borrow_mut().take();
        drop(join);
    }
}

struct DropHolder {
    _guard: DropJoinOnDrop,
}

impl Future for DropHolder {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

#[test]
fn drop_own_join_handle_from_cancelled_future_field_destructor() {
    let queue = TaskQueue::new();
    let slot: Slot = Rc::new(RefCell::new(None));
    let handle = queue.spawn(DropHolder {
        _guard: DropJoinOnDrop {
            slot: Rc::clone(&slot),
        },
    });
    *slot.borrow_mut() = Some(handle);
    queue.next().unwrap().run();

    queue.shutdown();
    assert!(slot.borrow().is_none());
}

/// The plain "task owns its own join handle" cycle, with a hand-written
/// (`Unpin`) future and no custom `Drop` impl: shutdown drops the future, the
/// field's `Rc::drop(&mut self)` drops the last handle, and
/// `drop_join_handle` -> `destroy` -> `begin_drop` retags the cell storage
/// that still contains that protected `Rc`.
struct OwnsJoin {
    _join: Slot,
}

impl Future for OwnsJoin {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

#[test]
fn task_owning_its_join_handle_is_shut_down() {
    let queue = TaskQueue::new();
    let slot: Slot = Rc::new(RefCell::new(None));
    let handle = queue.spawn(OwnsJoin {
        _join: Rc::clone(&slot),
    });
    *slot.borrow_mut() = Some(handle);
    drop(slot);
    drop(queue);
}
