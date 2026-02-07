use std::cell::{Cell, RefCell, UnsafeCell};
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use cordyceps::{list, Linked};

use crate::error;

pub(crate) trait TimerList {
    fn remove(&self, entry: ptr::NonNull<Entry>);

    fn add(&self, entry: Pin<&mut Entry>, duration: Duration);
}

pin_project_lite::pin_project! {
    pub(crate) struct Sleep<T: TimerList> {
        timer: T,
        #[pin]
        entry: Entry,
        duration: Duration,
    }

    impl<T> PinnedDrop for Sleep<T> where T: TimerList {
        fn drop(this: Pin<&mut Self>) {
            let mut me = this.project();
            if me.entry.is_registered() {
                // Safety: We are not moving the entry, so it is safe to
                // construct a `NonNull` from a pinned reference.
                unsafe {
                    let entry = ptr::NonNull::from(Pin::into_inner_unchecked(me.entry.as_mut()));
                    me.timer.remove(entry);
                }
            }
        }
    }
}

impl<T> Future for Sleep<T>
where
    T: TimerList,
{
    type Output = Result<(), error::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: We are not moving the entry, so it is safe to
        // construct a `NonNull` from a pinned reference.
        let mut me = self.project();
        loop {
            match me.entry.state.get() {
                State::Unregistered => {
                    debug_assert!(!me.entry.is_registered());

                    me.timer.add(me.entry.as_mut(), *me.duration);
                    continue;
                }
                State::Registered => {
                    debug_assert!(me.entry.is_registered());
                    let mut w = me.entry.waker.borrow_mut();
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

impl<T> Sleep<T>
where
    T: TimerList,
{
    pub(crate) fn new(timer: T, duration: Duration) -> Self {
        Self {
            timer,
            entry: Entry::new(),
            duration,
        }
    }

    /// Reset this [`Sleep`] instance.
    ///
    /// This will unlink it from the timer if it is currently registered,
    /// and reset the deadline to zero. Future calls to [`Sleep::poll`] will
    /// then re-register the sleep relative to the current tick.
    pub(crate) fn reset(&mut self) {
        if self.entry.is_registered() {
            let entry = ptr::NonNull::from(&mut self.entry);
            self.timer.remove(entry);
        }
        debug_assert!(!self.entry.is_registered());
        self.entry.complete.set(Ok(()));
        self.entry.deadline.set(0);
        self.entry.waker.borrow_mut().take();
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct Entry {
        state: Cell<State>,
        waker: RefCell<Option<Waker>>,
        complete: Cell<Result<(), error::Error>>,
        deadline: Cell<u64>,
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
            waker: RefCell::new(None),
            complete: Cell::new(Ok(())),
            pointers: UnsafeCell::new(list::Links::new()),
            _p: PhantomPinned,
            deadline: Cell::new(0),
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

    pub(crate) fn fire(&self, completion: Result<(), error::Error>) {
        self.state.set(State::Fired);
        self.complete.set(completion);
        if let Some(waker) = self.waker.borrow_mut().take() {
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
