use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use cordyceps::{list, Linked};

use crate::{error, wheels::Wheels, Clock};

pin_project_lite::pin_project! {
    /// Future returned by [`crate::Handle::sleep`] and
    /// [`crate::Handle::sleep_until`].
    ///
    /// This future resolves once the specified duration has elapsed, or with
    /// [`crate::Error::Shutdown`] if the time driver shuts down first.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Sleep {
        wheels: Rc<Wheels>,
        #[pin]
        entry: Entry,
        clock: Clock,
        deadline: u64,
        relative: Option<Duration>,
    }

    impl PinnedDrop for Sleep {
        fn drop(this: Pin<&mut Self>) {
            let mut me = this.project();
            if me.entry.is_registered() {
                // Safety: We are not moving the entry, so it is safe to
                // construct a `NonNull` from a pinned reference.
                unsafe {
                    let entry = ptr::NonNull::from(Pin::into_inner_unchecked(me.entry.as_mut()));
                    me.wheels.remove(entry);
                }
            }
        }
    }
}

impl std::fmt::Debug for Sleep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Sleep").finish()
    }
}

impl Future for Sleep {
    type Output = Result<(), error::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: We are not moving the entry, so it is safe to
        // construct a `NonNull` from a pinned reference.
        let mut me = self.project();
        loop {
            match me.entry.state.get() {
                State::Unregistered => {
                    debug_assert!(!me.entry.is_registered());

                    me.wheels
                        .add_at(me.entry.as_mut(), *me.deadline, me.clock.tick());
                    continue;
                }
                State::Registered => {
                    debug_assert!(me.entry.is_registered());
                    // Safety: Timer entries are local-only. This mutable access
                    // is short-lived and performs no callbacks.
                    let w = unsafe { &mut *me.entry.waker.get() };
                    if !w
                        .as_ref()
                        .is_some_and(|existing| existing.will_wake(cx.waker()))
                    {
                        *w = Some(cx.waker().clone());
                    }
                    return Poll::Pending;
                }
                State::Fired => {
                    debug_assert!(!me.entry.is_registered());
                    let complete = me.entry.complete.replace(Ok(()));
                    return Poll::Ready(complete);
                }
            }
        }
    }
}

impl Sleep {
    pub(crate) fn new(wheels: Rc<Wheels>, clock: Clock, duration: Duration) -> Self {
        Self {
            wheels,
            entry: Entry::new(),
            deadline: clock.deadline_after(duration),
            clock,
            relative: Some(duration),
        }
    }

    pub(crate) fn new_at(wheels: Rc<Wheels>, clock: Clock, deadline: u64) -> Self {
        Self {
            wheels,
            entry: Entry::new(),
            clock,
            deadline,
            relative: None,
        }
    }

    /// Reset this [`Sleep`] instance.
    ///
    /// This will unlink it from the timer if it is currently registered and
    /// reset the timer entry. Relative sleeps restart their original duration
    /// from the current clock; absolute sleeps retain their original deadline.
    pub fn reset(self: Pin<&mut Self>) {
        let mut me = self.project();
        if let Some(duration) = me.relative {
            *me.deadline = me.clock.deadline_after(*duration);
        }
        if me.entry.is_registered() {
            // Safety: We are not moving the entry, so it is safe to construct
            // a `NonNull` from this pinned reference.
            unsafe {
                let entry = ptr::NonNull::from(Pin::into_inner_unchecked(me.entry.as_mut()));
                me.wheels.remove(entry);
            }
        }
        me.entry.state.set(State::Unregistered);
        debug_assert!(!me.entry.is_registered());
        me.entry.complete.set(Ok(()));
        me.entry.deadline.set(0);
        // Safety: Reset has exclusive access to the sleep and entry.
        unsafe { (*me.entry.waker.get()).take() };
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct Entry {
        state: Cell<State>,
        waker: UnsafeCell<Option<Waker>>,
        complete: Cell<Result<(), error::Error>>,
        deadline: Cell<u64>,
        wheel: Cell<u8>,
        slot: Cell<u8>,
        #[pin]
        pointers: UnsafeCell<list::Links<Entry>>,
        _p: PhantomPinned,
    }
}

impl Entry {
    /// Creates a new entry.
    fn new() -> Self {
        Self {
            state: Cell::new(State::Unregistered),
            waker: UnsafeCell::new(None),
            complete: Cell::new(Ok(())),
            pointers: UnsafeCell::new(list::Links::new()),
            _p: PhantomPinned,
            deadline: Cell::new(0),
            wheel: Cell::new(0),
            slot: Cell::new(0),
        }
    }

    /// Returns true if this entry is currently
    /// registered with a timer.
    pub(crate) fn is_registered(&self) -> bool {
        self.state.get() == State::Registered
    }

    pub(crate) fn expiration(&self) -> u64 {
        debug_assert!(self.is_registered());
        self.deadline.get()
    }

    /// Set the entry as registered in the timer.
    ///
    /// Takes the tick at which the entry was registered.
    pub(crate) fn set_registered(&self, tick: u64) {
        debug_assert!(!self.is_registered());
        self.state.set(State::Registered);
        self.deadline.set(tick);
    }

    pub(crate) fn set_location(&self, wheel: usize, slot: usize) {
        debug_assert!(self.is_registered());
        self.wheel
            .set(u8::try_from(wheel).expect("wheel index out of range"));
        self.slot
            .set(u8::try_from(slot).expect("slot index out of range"));
    }

    pub(crate) fn location(&self) -> (usize, usize) {
        debug_assert!(self.is_registered());
        (usize::from(self.wheel.get()), usize::from(self.slot.get()))
    }

    pub(crate) fn fire(&self, completion: Result<(), error::Error>) {
        self.state.set(State::Fired);
        self.complete.set(completion);
        // Take the waker before invoking it so no mutable entry access spans
        // the callback.
        let waker = unsafe { (*self.waker.get()).take() };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum State {
    Unregistered,
    Registered,
    Fired,
}

unsafe impl Linked<list::Links<Entry>> for Entry {
    type Handle = ptr::NonNull<Entry>;

    fn into_ptr(r: Self::Handle) -> ptr::NonNull<Self> {
        r
    }

    unsafe fn from_ptr(ptr: ptr::NonNull<Self>) -> Self::Handle {
        ptr
    }

    unsafe fn links(ptr: ptr::NonNull<Self>) -> ptr::NonNull<list::Links<Entry>> {
        let links = &raw const (*ptr.as_ptr()).pointers;
        ptr::NonNull::new_unchecked((*links).get())
    }
}
