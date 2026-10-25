use std::cell::UnsafeCell;
use std::task::Waker;

use cordyceps::list::Links;

use crate::state::StateCell;
use crate::task_cell::VTable;

pub(crate) struct Header {
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
    pub(crate) fn new(
        state: StateCell,
        vtable: &'static VTable,
        thread: std::thread::ThreadId,
    ) -> Self {
        Self {
            state,
            vtable,
            waker: UnsafeCell::new(None),
            thread,
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
        // `will_wake` does not invoke user code. Clone and drop can both
        // re-enter the task system, so neither may borrow the slot.
        if unsafe { &*self.waker.get() }
            .as_ref()
            .is_some_and(|existing| existing.will_wake(waker))
        {
            return;
        }
        let new_waker = waker.clone();
        let old_waker = unsafe { &mut *self.waker.get() }.replace(new_waker);
        drop(old_waker);
    }
}
