use std::cell::UnsafeCell;
use std::task::Waker;

use cordyceps::list::Links;

use crate::state::StateCell;
use crate::task_cell::VTable;

pub struct Header {
    state: StateCell,
    vtable: &'static VTable,
    waker: UnsafeCell<Option<Waker>>,
    pub(crate) thread: std::thread::ThreadId,
    pub(crate) links: cordyceps::list::Links<Self>,
}

impl std::fmt::Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Header").finish()
    }
}

impl Header {
    pub(crate) fn new(state: StateCell, vtable: &'static VTable) -> Self {
        Self {
            state,
            vtable,
            waker: UnsafeCell::new(None),
            thread: std::thread::current().id(),
            links: Links::default(),
        }
    }

    pub(crate) fn state(&self) -> &StateCell {
        &self.state
    }

    pub(crate) fn vtable(&self) -> &'static VTable {
        self.vtable
    }

    pub(crate) fn notify_join_handle(&self) {
        let waker = {
            let w = unsafe { &mut *self.waker.get() };
            if let Some(waker) = w.take() {
                waker
            } else {
                return;
            }
        };
        waker.wake();
    }

    pub(crate) fn set_waker(&self, waker: &Waker) {
        let waker_slot = unsafe { &mut *self.waker.get() };
        if let Some(existing) = waker_slot {
            if !existing.will_wake(waker) {
                *existing = waker.clone();
            }
        } else {
            *waker_slot = Some(waker.clone());
        }
    }
}
