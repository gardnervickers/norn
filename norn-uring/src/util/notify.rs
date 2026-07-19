use std::cell::{Cell, RefCell, UnsafeCell};
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::ptr::{self, NonNull};
use std::task::{Context, Poll, Waker};

use cordyceps::{list, Linked, List};
use pin_project_lite::pin_project;

/// [`Notify`] is a notification mechanism for tasks.
///
/// Tasks can wait on a [`Notify`] instance, and then be woken by
/// a call to [`Notify::notify`].
///
/// If a [`Notified`] is dropped before its future completes, it
/// will pass the wakeup to the next [`Notified`] in the queue.
pub(crate) struct Notify {
    waiters: RefCell<List<Entry>>,
    count: Cell<usize>,
}

impl std::fmt::Debug for Notify {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Notify")
            .field("waiters", &self.count.get())
            .finish()
    }
}

impl Default for Notify {
    fn default() -> Self {
        Self {
            waiters: RefCell::new(List::new()),
            count: Cell::new(0),
        }
    }
}

impl Notify {
    /// Returns a [`Notified`] which can be used to wait on this [`Notify`].
    ///
    /// Tasks waiting on a [`Notified`] will be woken when [`Notify::notify`]
    /// is called. Calling [`Notify::notify(n)`] guarantees that n tasks will
    /// both be woken up and complete their wait.
    ///
    /// If a woken Notified instance is dropped, it's notification will be
    /// passed on to the next Notified in the queue.
    pub(crate) fn wait(&self) -> Notified<'_> {
        Notified {
            notify: self,
            entry: Entry::new(),
        }
    }

    /// Returns the number of waiters on this [`Notify`].
    pub(crate) fn waiters(&self) -> usize {
        self.count.get()
    }

    /// Notify the next `n` waiters.
    ///
    /// This will return the number of waiters notified. If there are
    /// fewer waiters than `n`, then the number of waiters notified will
    /// be equal to the number of waiters.
    pub(crate) fn notify(&self, n: usize) -> usize {
        let mut removed = 0;
        while removed != n {
            let waker = {
                let mut waiters = self.waiters.borrow_mut();
                let Some(entry) = waiters.pop_front() else {
                    break;
                };
                let entry = unsafe { entry.as_ref() };
                let waker = entry.fire();
                self.count.set(self.count.get() - 1);
                removed += 1;
                waker
            };

            // Waking may execute arbitrary code, including re-entering this
            // Notify. The list, entry state, waiter count, and waker ownership
            // have all been committed before reaching this point.
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        removed
    }

    /// Cancel a dropped Notified.
    ///
    /// This will only be called if the Notified is
    /// dropped while still being linked.
    unsafe fn cancel(&self, entry: Pin<&mut Entry>) {
        self.count.set(self.count.get() - 1);
        let ptr = unsafe {
            let entry = Pin::into_inner_unchecked(entry);
            ptr::NonNull::from(entry)
        };
        let mut waiters = self.waiters.borrow_mut();
        waiters.remove(ptr);
    }
}

pin_project! {
    pub(crate) struct Notified<'n> {
        notify: &'n Notify,
        #[pin]
        entry: Entry,

    }

    impl<'n> PinnedDrop for Notified<'n> {
        fn drop(mut this: Pin<&mut Self>) {
            let me = this.project();
            let state = me.entry.state.get();
            if matches!(state, State::Linked) {
                // Safety: we know that the notified is linked.
                unsafe { me.notify.cancel(me.entry) };
                return;
            }
            if matches!(state, State::Fired) {
                // If this was fired, pass the notification
                // on to the next waiter.
                me.notify.notify(1);
            }
        }
    }
}

impl Future for Notified<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let entry = this.entry.as_mut();
        match entry.state.get() {
            State::Unregistered => {
                entry.state.set(State::Linked);
                entry.waker.borrow_mut().replace(cx.waker().clone());
                let ptr = unsafe {
                    let entry = Pin::into_inner_unchecked(entry);
                    ptr::NonNull::from(entry)
                };
                this.notify.waiters.borrow_mut().push_back(ptr);
                this.notify.count.set(this.notify.count.get() + 1);
                Poll::Pending
            }
            State::Linked => {
                entry.waker.borrow_mut().replace(cx.waker().clone());
                Poll::Pending
            }
            State::Fired => {
                entry.state.set(State::Completed);
                Poll::Ready(())
            }
            State::Completed => unreachable!(),
        }
    }
}

pin_project! {
    struct Entry {
        #[pin]
        links: UnsafeCell<list::Links<Entry>>,
        state: Cell<State>,
        waker: RefCell<Option<Waker>>,
        _pin: PhantomPinned
    }
}

#[derive(Clone, Copy)]
enum State {
    Unregistered,
    Linked,
    Fired,
    Completed,
}

impl Entry {
    fn new() -> Self {
        Self {
            links: UnsafeCell::new(list::Links::new()),
            state: Cell::new(State::Unregistered),
            waker: RefCell::new(None),
            _pin: PhantomPinned,
        }
    }

    fn fire(&self) -> Option<Waker> {
        self.state.set(State::Fired);
        self.waker.borrow_mut().take()
    }
}

unsafe impl Linked<list::Links<Entry>> for Entry {
    type Handle = NonNull<Entry>;

    fn into_ptr(r: Self::Handle) -> NonNull<Self> {
        r
    }

    unsafe fn from_ptr(ptr: NonNull<Self>) -> Self::Handle {
        ptr
    }

    unsafe fn links(target: NonNull<Self>) -> NonNull<list::Links<Entry>> {
        // Safety: &raw const avoids the need to create a temporary reference.
        let links = &raw const (*target.as_ptr()).links;
        // Safety: target is nonnull, so the reference to it's pointers are also nonnull.
        unsafe { NonNull::new_unchecked((*links).get()) }
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Wake;

    use super::*;

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("test waker panicked");
        }
    }

    struct ReentrantWake {
        notify: *const Notify,
        wakes: Arc<AtomicUsize>,
    }

    // Safety: this test waker is only invoked synchronously on the thread that
    // owns `notify`, and it is dropped before `notify` goes out of scope.
    unsafe impl Send for ReentrantWake {}
    unsafe impl Sync for ReentrantWake {}

    impl Wake for ReentrantWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::Relaxed);
            unsafe {
                (*self.notify).notify(1);
            }
        }
    }

    #[test]
    fn notify_wakeup() {
        let (waker, count) = futures_test::task::new_count_waker();
        let mut cx = Context::from_waker(&waker);
        let notify = Notify::default();
        let mut notified = pin!(notify.wait());

        assert_eq!(notify.waiters(), 0);
        // Poll the notified once to register it.
        assert!(notified.as_mut().poll(&mut cx).is_pending());
        // The notified should be registered now.
        assert_eq!(notify.waiters(), 1);

        // Notify the notified.
        assert_eq!(notify.notify(1), 1);
        // This should have triggered a wakeup.
        assert_eq!(count, 1);
        assert!(notified.as_mut().poll(&mut cx).is_ready());
        // The notified should be unregistered now.
        assert_eq!(notify.waiters(), 0);
    }

    #[test]
    fn panicking_waker_leaves_remaining_waiters_valid() {
        let notify = Notify::default();
        let panic_waker = Waker::from(Arc::new(PanicWake));
        let (count_waker, count) = futures_test::task::new_count_waker();
        let mut panic_cx = Context::from_waker(&panic_waker);
        let mut count_cx = Context::from_waker(&count_waker);
        let mut first = pin!(notify.wait());
        let mut second = pin!(notify.wait());

        assert!(first.as_mut().poll(&mut panic_cx).is_pending());
        assert!(second.as_mut().poll(&mut count_cx).is_pending());
        assert_eq!(notify.waiters(), 2);

        let result = catch_unwind(AssertUnwindSafe(|| notify.notify(usize::MAX)));
        assert!(result.is_err());
        assert_eq!(notify.waiters(), 1);
        assert!(first.as_mut().poll(&mut panic_cx).is_ready());

        assert_eq!(notify.notify(1), 1);
        assert_eq!(count, 1);
        assert_eq!(notify.waiters(), 0);
        assert!(second.as_mut().poll(&mut count_cx).is_ready());
    }

    #[test]
    fn synchronous_reentrant_waker_can_notify_another_waiter() {
        let notify = Notify::default();
        let reentrant_wakes = Arc::new(AtomicUsize::new(0));
        let reentrant_waker = Waker::from(Arc::new(ReentrantWake {
            notify: &notify,
            wakes: Arc::clone(&reentrant_wakes),
        }));
        let (count_waker, count) = futures_test::task::new_count_waker();
        let mut reentrant_cx = Context::from_waker(&reentrant_waker);
        let mut count_cx = Context::from_waker(&count_waker);
        let mut first = pin!(notify.wait());
        let mut second = pin!(notify.wait());

        assert!(first.as_mut().poll(&mut reentrant_cx).is_pending());
        assert!(second.as_mut().poll(&mut count_cx).is_pending());
        assert_eq!(notify.waiters(), 2);

        assert_eq!(notify.notify(1), 1);
        assert_eq!(reentrant_wakes.load(Ordering::Relaxed), 1);
        assert_eq!(count, 1);
        assert_eq!(notify.waiters(), 0);
        assert!(first.as_mut().poll(&mut reentrant_cx).is_ready());
        assert!(second.as_mut().poll(&mut count_cx).is_ready());
    }

    #[test]
    fn drop_notified() {
        let (waker, count) = futures_test::task::new_count_waker();
        let mut cx = Context::from_waker(&waker);
        let notify = Notify::default();
        let mut notified1 = Box::pin(notify.wait());
        let mut notified2 = Box::pin(notify.wait());

        // Poll once to register the notified with the list.
        assert!(notified1.as_mut().poll(&mut cx).is_pending());
        assert!(notified2.as_mut().poll(&mut cx).is_pending());

        // Notify a single waiter.
        assert_eq!(notify.notify(1), 1);
        assert_eq!(count, 1);
        // Drop the notified before it is woken up.
        drop(notified1);

        // We should see the notification passed to the next waiter.
        assert!(notified2.as_mut().poll(&mut cx).is_ready());
    }

    #[test]
    fn drop_with_waiters() {
        let (waker, _) = futures_test::task::new_count_waker();
        let mut cx = Context::from_waker(&waker);
        let notify = Notify::default();
        let mut notified1 = Box::pin(notify.wait());
        let mut notified2 = Box::pin(notify.wait());

        assert!(notified1.as_mut().poll(&mut cx).is_pending());
        assert!(notified2.as_mut().poll(&mut cx).is_pending());
    }
}
