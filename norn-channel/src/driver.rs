use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use norn_executor::park::{Park, ParkMode, Unpark};

pub(crate) trait RegisteredReceiver {
    fn wake_if_ready(&self);
    fn close(&self);
}

struct Registry {
    next_id: Cell<u64>,
    closed: Cell<bool>,
    receivers: RefCell<HashMap<u64, Rc<dyn RegisteredReceiver>>>,
}

impl Registry {
    fn new() -> Self {
        Self {
            next_id: Cell::new(0),
            closed: Cell::new(false),
            receivers: RefCell::new(HashMap::new()),
        }
    }

    fn register(&self, receiver: Rc<dyn RegisteredReceiver>) -> Option<u64> {
        if self.closed.get() {
            receiver.close();
            return None;
        }

        let id = self.next_id.get();
        self.next_id
            .set(id.checked_add(1).expect("channel registration id overflow"));
        self.receivers.borrow_mut().insert(id, receiver);
        Some(id)
    }

    fn unregister(&self, id: u64) {
        self.receivers.borrow_mut().remove(&id);
    }

    fn wake_ready(&self) {
        for receiver in self.receivers.borrow().values() {
            receiver.wake_if_ready();
        }
    }

    fn shutdown(&self) {
        if self.closed.replace(true) {
            return;
        }

        let mut receivers = self.receivers.borrow_mut();
        for receiver in receivers.values() {
            receiver.close();
        }
        receivers.clear();
    }
}

pub(crate) struct Remote {
    pending: AtomicBool,
    unparker: Box<dyn Unpark + Send + Sync>,
}

impl fmt::Debug for Remote {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Remote")
            .field("pending", &self.pending)
            .finish_non_exhaustive()
    }
}

impl Remote {
    fn new<U>(unparker: U) -> Self
    where
        U: Unpark + Send + Sync + 'static,
    {
        Self {
            pending: AtomicBool::new(false),
            unparker: Box::new(unparker),
        }
    }

    pub(crate) fn notify(&self) {
        if !self.pending.swap(true, Ordering::AcqRel) {
            self.unparker.unpark();
        }
    }

    fn take_pending(&self) -> bool {
        self.pending.swap(false, Ordering::AcqRel)
    }

    fn is_pending(&self) -> bool {
        self.pending.load(Ordering::Acquire)
    }
}

/// A destination-thread handle used to create channels.
///
/// The handle is deliberately not [`Send`]. Channels must be registered on the
/// same thread that owns the associated [`Driver`]. The senders returned by
/// [`crate::mpsc::bounded`] are the cross-thread part of the API.
#[derive(Clone)]
pub struct Handle {
    registry: Rc<Registry>,
    remote: Arc<Remote>,
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle").finish_non_exhaustive()
    }
}

impl Handle {
    pub(crate) fn register(&self, receiver: Rc<dyn RegisteredReceiver>) -> Option<u64> {
        self.registry.register(receiver)
    }

    pub(crate) fn unregister(&self, id: u64) {
        self.registry.unregister(id);
    }

    pub(crate) fn remote(&self) -> Arc<Remote> {
        Arc::clone(&self.remote)
    }
}

/// A [`Park`] wrapper that delivers cross-thread channel readiness.
///
/// The driver wraps any existing Norn park layer. Remote senders use the inner
/// park layer's unparker, while receiver wakers are always invoked by this
/// driver on the destination executor thread.
pub struct Driver<P> {
    inner: P,
    registry: Rc<Registry>,
    remote: Arc<Remote>,
}

impl<P> fmt::Debug for Driver<P>
where
    P: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Driver")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<P> Driver<P>
where
    P: Park,
{
    /// Wrap an existing park layer with cross-thread channel delivery.
    ///
    /// Construct the driver on the thread that will run the executor.
    pub fn new(inner: P) -> Self {
        let registry = Rc::new(Registry::new());
        let remote = Arc::new(Remote::new(inner.unparker()));
        Self {
            inner,
            registry,
            remote,
        }
    }

    /// Return a destination-thread handle used to create channels.
    pub fn handle(&self) -> Handle {
        Handle {
            registry: Rc::clone(&self.registry),
            remote: Arc::clone(&self.remote),
        }
    }
}

impl<P> Park for Driver<P>
where
    P: Park,
{
    type Unparker = P::Unparker;
    type Guard = P::Guard;

    fn park(&mut self, mut mode: ParkMode) -> Result<(), std::io::Error> {
        if self.remote.take_pending() {
            self.registry.wake_ready();
            mode = ParkMode::NoPark;
        }

        let result = self.inner.park(mode);

        if self.remote.take_pending() {
            self.registry.wake_ready();
        }

        result
    }

    fn enter(&self) -> Self::Guard {
        self.inner.enter()
    }

    fn unparker(&self) -> Self::Unparker {
        self.inner.unparker()
    }

    fn needs_park(&self) -> bool {
        self.remote.is_pending() || self.inner.needs_park()
    }

    fn shutdown(&mut self) {
        self.registry.shutdown();
        self.inner.shutdown();
    }
}

impl<P> Drop for Driver<P> {
    fn drop(&mut self) {
        self.registry.shutdown();
    }
}
