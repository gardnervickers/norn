//! Bounded multi-producer, single-consumer channels.
//!
//! Receive-side batching is bounded explicitly by the caller. Bulk submission
//! is intentionally deferred until its partial-enqueue and ownership-return
//! semantics can be designed against measured workloads.

use std::cell::RefCell;
use std::error::Error;
use std::fmt;
use std::future::poll_fn;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crossbeam_queue::ArrayQueue;

use crate::driver::{RegisteredReceiver, Remote};
use crate::Handle;

/// Create a bounded multi-producer, single-consumer channel.
///
/// The channel is registered with `handle` and its [`Receiver`] is bound to
/// that handle's executor thread. [`Sender`] values may be cloned and moved to
/// foreign threads.
///
/// # Panics
///
/// Panics if `capacity` is zero.
pub fn bounded<T>(handle: &Handle, capacity: usize) -> (Sender<T>, Receiver<T>)
where
    T: Send + 'static,
{
    assert!(capacity > 0, "channel capacity must be non-zero");

    let shared = Arc::new(Shared {
        queue: ArrayQueue::new(capacity),
        receiver_closed: AtomicBool::new(false),
        senders: AtomicUsize::new(1),
        remote: handle.remote(),
    });
    let local = Rc::new(Local {
        shared: Arc::clone(&shared),
        waker: RefCell::new(None),
    });
    let registered: Rc<dyn RegisteredReceiver> = local.clone();
    let registration = handle.register(registered);

    (
        Sender { shared },
        Receiver {
            local,
            handle: handle.clone(),
            registration,
        },
    )
}

struct Shared<T> {
    queue: ArrayQueue<T>,
    receiver_closed: AtomicBool,
    senders: AtomicUsize,
    remote: Arc<Remote>,
}

impl<T> Shared<T> {
    fn is_disconnected(&self) -> bool {
        self.receiver_closed.load(Ordering::Acquire) || self.senders.load(Ordering::Acquire) == 0
    }
}

struct Local<T> {
    shared: Arc<Shared<T>>,
    waker: RefCell<Option<Waker>>,
}

impl<T> Local<T> {
    fn register_waker(&self, waker: &Waker) {
        let mut current = self.waker.borrow_mut();
        if current
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *current = Some(waker.clone());
        }
    }

    fn clear_waker(&self) {
        self.waker.borrow_mut().take();
    }
}

impl<T> RegisteredReceiver for Local<T>
where
    T: Send + 'static,
{
    fn wake_if_ready(&self) {
        if !self.shared.queue.is_empty() || self.shared.is_disconnected() {
            if let Some(waker) = self.waker.borrow_mut().take() {
                waker.wake();
            }
        }
    }

    fn close(&self) {
        self.shared.receiver_closed.store(true, Ordering::Release);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

struct WaitGuard<T> {
    local: Rc<Local<T>>,
}

impl<T> Drop for WaitGuard<T> {
    fn drop(&mut self) {
        self.local.clear_waker();
    }
}

/// The sending side of a bounded cross-thread channel.
pub struct Sender<T> {
    shared: Arc<Shared<T>>,
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("capacity", &self.shared.queue.capacity())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.shared.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.shared.senders.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.remote.notify();
        }
    }
}

impl<T> Sender<T> {
    /// Attempt to enqueue one message without waiting for capacity.
    ///
    /// On failure, ownership of the message is returned to the caller.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        // TODO: Add bounded bulk submit after defining partial-enqueue return
        // semantics and measuring whether producers naturally form batches.
        if self.is_closed() {
            return Err(TrySendError::Closed(value));
        }

        match self.shared.queue.push(value) {
            Ok(()) => {
                self.shared.remote.notify();
                Ok(())
            }
            Err(value) if self.is_closed() => Err(TrySendError::Closed(value)),
            Err(value) => Err(TrySendError::Full(value)),
        }
    }

    /// Return the fixed capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.shared.queue.capacity()
    }

    /// Return whether the receiver has closed.
    pub fn is_closed(&self) -> bool {
        self.shared.receiver_closed.load(Ordering::Acquire)
    }
}

/// The destination-thread receiving side of a bounded channel.
///
/// A receiver is neither cloneable nor [`Send`]. It stores ordinary Norn task
/// wakers, which the channel driver invokes only on the destination thread.
pub struct Receiver<T> {
    local: Rc<Local<T>>,
    handle: Handle,
    registration: Option<u64>,
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("capacity", &self.local.shared.queue.capacity())
            .field("closed", &self.local.shared.is_disconnected())
            .finish()
    }
}

impl<T> Receiver<T> {
    /// Attempt to receive one message without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(value) = self.local.shared.queue.pop() {
            return Ok(value);
        }
        if self.local.shared.is_disconnected() {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Receive one message, waiting until a message arrives or the channel closes.
    pub async fn recv(&mut self) -> Option<T> {
        let _wait = WaitGuard {
            local: Rc::clone(&self.local),
        };
        poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Attempt to drain up to `limit` messages without waiting.
    ///
    /// Messages are appended to `output`. The returned count never exceeds
    /// `limit`, even when more messages are ready in the channel.
    ///
    /// # Panics
    ///
    /// Panics if `limit` is zero.
    pub fn try_recv_many(
        &mut self,
        output: &mut Vec<T>,
        limit: usize,
    ) -> Result<usize, TryRecvError> {
        assert!(limit > 0, "receive limit must be non-zero");
        let received = self.drain_into(output, limit);
        if received > 0 {
            Ok(received)
        } else if self.local.shared.is_disconnected() {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Wait for at least one message, then drain up to `limit` messages.
    ///
    /// Messages are appended to `output`. The returned count is in
    /// `1..=limit`, or zero when the channel is closed and empty. This method
    /// never performs an unbounded drain.
    ///
    /// # Panics
    ///
    /// Panics if `limit` is zero.
    pub async fn recv_many(&mut self, output: &mut Vec<T>, limit: usize) -> usize {
        assert!(limit > 0, "receive limit must be non-zero");
        let _wait = WaitGuard {
            local: Rc::clone(&self.local),
        };
        poll_fn(|cx| self.poll_recv_many(cx, output, limit)).await
    }

    /// Close the receiving side while retaining access to buffered messages.
    ///
    /// Subsequent sends fail immediately. Receive operations may continue to
    /// drain messages that were already buffered and then report closure.
    pub fn close(&mut self) {
        self.local
            .shared
            .receiver_closed
            .store(true, Ordering::Release);
    }

    /// Return the fixed capacity of the channel.
    pub fn capacity(&self) -> usize {
        self.local.shared.queue.capacity()
    }

    fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        if let Some(value) = self.local.shared.queue.pop() {
            self.local.clear_waker();
            return Poll::Ready(Some(value));
        }
        if self.local.shared.is_disconnected() {
            self.local.clear_waker();
            return Poll::Ready(None);
        }

        self.local.register_waker(cx.waker());

        if let Some(value) = self.local.shared.queue.pop() {
            self.local.clear_waker();
            Poll::Ready(Some(value))
        } else if self.local.shared.is_disconnected() {
            self.local.clear_waker();
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    fn poll_recv_many(
        &mut self,
        cx: &mut Context<'_>,
        output: &mut Vec<T>,
        limit: usize,
    ) -> Poll<usize> {
        let received = self.drain_into(output, limit);
        if received > 0 {
            self.local.clear_waker();
            return Poll::Ready(received);
        }
        if self.local.shared.is_disconnected() {
            self.local.clear_waker();
            return Poll::Ready(0);
        }

        self.local.register_waker(cx.waker());

        let received = self.drain_into(output, limit);
        if received > 0 {
            self.local.clear_waker();
            Poll::Ready(received)
        } else if self.local.shared.is_disconnected() {
            self.local.clear_waker();
            Poll::Ready(0)
        } else {
            Poll::Pending
        }
    }

    fn drain_into(&self, output: &mut Vec<T>, limit: usize) -> usize {
        let start = output.len();
        for _ in 0..limit {
            let Some(value) = self.local.shared.queue.pop() else {
                break;
            };
            output.push(value);
        }
        output.len() - start
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.close();
        self.local.clear_waker();
        if let Some(id) = self.registration.take() {
            self.handle.unregister(id);
        }
    }
}

/// An error returned by [`Sender::try_send`].
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    /// The bounded queue has no remaining capacity.
    Full(T),
    /// The receiver has closed.
    Closed(T),
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => f.write_str("channel is full"),
            Self::Closed(_) => f.write_str("channel is closed"),
        }
    }
}

impl<T> Error for TrySendError<T> where T: fmt::Debug {}

/// An error returned by non-blocking receive operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel is currently empty but may receive more messages.
    Empty,
    /// The channel is closed and empty.
    Closed,
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("channel is empty"),
            Self::Closed => f.write_str("channel is closed"),
        }
    }
}

impl Error for TryRecvError {}

#[cfg(test)]
mod tests {
    use std::thread;

    use norn_executor::park::{SpinPark, ThreadPark};
    use norn_executor::LocalExecutor;

    use super::*;
    use crate::Driver;

    #[test]
    fn bounded_queue_preserves_values_on_full() {
        let driver = Driver::new(SpinPark);
        let (sender, mut receiver) = bounded(&driver.handle(), 2);

        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert_eq!(sender.try_send(3), Err(TrySendError::Full(3)));
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn bounded_bulk_receive_never_exceeds_limit() {
        let driver = Driver::new(SpinPark);
        let handle = driver.handle();
        let (sender, mut receiver) = bounded(&handle, 8);
        let mut executor = LocalExecutor::new(driver);

        for value in 0..5 {
            sender.try_send(value).unwrap();
        }

        let mut output = Vec::new();
        assert_eq!(executor.block_on(receiver.recv_many(&mut output, 3)), 3);
        assert_eq!(output, [0, 1, 2]);
        assert_eq!(receiver.try_recv_many(&mut output, 8), Ok(2));
        assert_eq!(output, [0, 1, 2, 3, 4]);
    }

    #[test]
    fn cross_thread_bulk_receive_has_no_lost_wakeups() {
        const MESSAGES: usize = 100_000;

        let driver = Driver::new(ThreadPark::default());
        let (sender, mut receiver) = bounded(&driver.handle(), 64);
        let mut executor = LocalExecutor::new(driver);

        let producer = thread::spawn(move || {
            for mut value in 0..MESSAGES {
                loop {
                    match sender.try_send(value) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            value = returned;
                            std::hint::spin_loop();
                        }
                        Err(TrySendError::Closed(_)) => panic!("receiver closed early"),
                    }
                }
            }
        });

        executor.block_on(async {
            let mut output = Vec::with_capacity(32);
            let mut expected = 0;
            loop {
                let received = receiver.recv_many(&mut output, 32).await;
                if received == 0 {
                    break;
                }
                assert!(received <= 32);
                for value in output.drain(..) {
                    assert_eq!(value, expected);
                    expected += 1;
                }
            }
            assert_eq!(expected, MESSAGES);
        });

        producer.join().unwrap();
    }

    #[test]
    fn multiple_producers_preserve_each_producer_order() {
        const PRODUCERS: usize = 4;
        const MESSAGES: usize = 25_000;

        let driver = Driver::new(ThreadPark::default());
        let (sender, mut receiver) = bounded(&driver.handle(), 64);
        let mut executor = LocalExecutor::new(driver);

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|producer| {
                let sender = sender.clone();
                thread::spawn(move || {
                    for sequence in 0..MESSAGES {
                        let mut message = (producer, sequence);
                        loop {
                            match sender.try_send(message) {
                                Ok(()) => break,
                                Err(TrySendError::Full(returned)) => {
                                    message = returned;
                                    std::hint::spin_loop();
                                }
                                Err(TrySendError::Closed(_)) => {
                                    panic!("receiver closed early")
                                }
                            }
                        }
                    }
                })
            })
            .collect();
        drop(sender);

        executor.block_on(async {
            let mut batch = Vec::with_capacity(32);
            let mut next = [0; PRODUCERS];
            while receiver.recv_many(&mut batch, 32).await != 0 {
                for (producer, sequence) in batch.drain(..) {
                    assert_eq!(sequence, next[producer]);
                    next[producer] += 1;
                }
            }
            assert_eq!(next, [MESSAGES; PRODUCERS]);
        });

        for producer in producers {
            producer.join().unwrap();
        }
    }

    #[test]
    fn last_sender_drop_wakes_receiver() {
        let driver = Driver::new(ThreadPark::default());
        let (sender, mut receiver) = bounded::<usize>(&driver.handle(), 1);
        let mut executor = LocalExecutor::new(driver);

        thread::spawn(move || drop(sender)).join().unwrap();
        assert_eq!(executor.block_on(receiver.recv()), None);
    }

    #[test]
    fn closing_receiver_rejects_new_messages_after_buffer_drains() {
        let driver = Driver::new(SpinPark);
        let (sender, mut receiver) = bounded(&driver.handle(), 2);

        sender.try_send(1).unwrap();
        receiver.close();
        assert_eq!(sender.try_send(2), Err(TrySendError::Closed(2)));
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Closed));
    }

    #[test]
    fn driver_shutdown_closes_registered_channels() {
        let driver = Driver::new(SpinPark);
        let (sender, _receiver) = bounded::<usize>(&driver.handle(), 1);
        let executor = LocalExecutor::new(driver);

        drop(executor);
        assert!(sender.is_closed());
        assert_eq!(sender.try_send(1), Err(TrySendError::Closed(1)));
    }

    #[test]
    #[should_panic(expected = "receive limit must be non-zero")]
    fn bulk_receive_requires_a_non_zero_limit() {
        let driver = Driver::new(SpinPark);
        let (_sender, mut receiver) = bounded::<usize>(&driver.handle(), 1);
        let mut output = Vec::new();
        let _ = receiver.try_recv_many(&mut output, 0);
    }
}
