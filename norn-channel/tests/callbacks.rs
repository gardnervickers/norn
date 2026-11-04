//! Receiver wakers run on the destination thread from `Driver::park` and
//! `Park::shutdown`. They may re-enter the channel driver, and a panicking
//! waker must not lose other receivers' wakeups or leave channels open.

use std::cell::RefCell;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use norn_channel::{mpsc, Driver};
use norn_executor::park::{Park, ParkMode, SpinPark};

thread_local! {
    static OTHER_RECEIVER: RefCell<Option<mpsc::Receiver<u32>>> = const { RefCell::new(None) };
}

struct DropOtherReceiver;

impl Wake for DropOtherReceiver {
    fn wake(self: Arc<Self>) {
        OTHER_RECEIVER.with(|other| drop(other.borrow_mut().take()));
    }
}

fn drop_another_receiver(shutdown: bool) {
    let mut driver = Driver::new(SpinPark);
    let handle = driver.handle();
    let (sender, receiver) = mpsc::bounded::<u32>(&driver.endpoint(), 1);
    let (_other_sender, other) = mpsc::bounded::<u32>(&driver.endpoint(), 1);
    let mut receiver = receiver.attach(&handle);
    OTHER_RECEIVER.with(|slot| *slot.borrow_mut() = Some(other.attach(&handle)));

    let waker = Waker::from(Arc::new(DropOtherReceiver));
    let mut cx = Context::from_waker(&waker);
    let mut recv = pin!(receiver.recv());
    assert!(recv.as_mut().poll(&mut cx).is_pending());

    if shutdown {
        driver.shutdown();
        assert_eq!(recv.as_mut().poll(&mut cx), Poll::Ready(None));
    } else {
        sender.try_send(7).unwrap();
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(recv.as_mut().poll(&mut cx), Poll::Ready(Some(7)));
    }
    OTHER_RECEIVER.with(|slot| assert!(slot.borrow().is_none()));
}

#[test]
fn wake_callback_can_drop_another_receiver() {
    drop_another_receiver(false);
}

#[test]
fn shutdown_callback_can_drop_another_receiver() {
    drop_another_receiver(true);
}

/// Panics on the first `panics` wakes and counts the rest.
struct PanicThenCount {
    panics: usize,
    calls: AtomicUsize,
    woken: AtomicUsize,
}

impl Wake for PanicThenCount {
    fn wake(self: Arc<Self>) {
        if self.calls.fetch_add(1, Ordering::Relaxed) < self.panics {
            panic!("test wake panic");
        }
        self.woken.fetch_add(1, Ordering::Relaxed);
    }
}

const RECEIVERS: usize = 4;

/// Attach `RECEIVERS` channels whose pending receives share one waker that
/// panics `panics` times.
fn panic_preserves_other_receivers(shutdown: bool, panics: usize) {
    let mut driver = Driver::new(SpinPark);
    let handle = driver.handle();
    let wake = Arc::new(PanicThenCount {
        panics,
        calls: AtomicUsize::new(0),
        woken: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);

    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..RECEIVERS {
        let (sender, receiver) = mpsc::bounded::<u32>(&driver.endpoint(), 1);
        senders.push(sender);
        receivers.push(receiver.attach(&handle));
    }
    let mut waits: Vec<_> = receivers
        .iter_mut()
        .map(|receiver| Box::pin(receiver.recv()))
        .collect();
    for wait in &mut waits {
        assert!(wait.as_mut().poll(&mut cx).is_pending());
    }

    if shutdown {
        assert!(catch_unwind(AssertUnwindSafe(|| driver.shutdown())).is_err());
        assert!(senders.iter().all(|sender| sender.is_closed()));
    } else {
        for (value, sender) in senders.iter().enumerate() {
            sender.try_send(value as u32).unwrap();
        }
        assert!(catch_unwind(AssertUnwindSafe(|| driver.park(ParkMode::NoPark))).is_err());
    }
    assert_eq!(wake.calls.load(Ordering::Relaxed), RECEIVERS);
    assert_eq!(wake.woken.load(Ordering::Relaxed), RECEIVERS - panics);
}

#[test]
fn wake_panic_still_wakes_other_ready_receivers() {
    panic_preserves_other_receivers(false, 1);
}

#[test]
fn shutdown_wake_panic_still_closes_and_wakes_every_channel() {
    panic_preserves_other_receivers(true, 1);
}

/// A second panicking waker in the same batch must not abort the process.
#[test]
fn second_wake_panic_still_wakes_other_ready_receivers() {
    panic_preserves_other_receivers(false, 2);
}

#[test]
fn second_shutdown_wake_panic_still_closes_and_wakes_every_channel() {
    panic_preserves_other_receivers(true, 2);
}

thread_local! {
    static TO_ATTACH: RefCell<Option<(mpsc::DetachedReceiver<u32>, norn_channel::Handle)>> =
        const { RefCell::new(None) };
    static ATTACHED: RefCell<Option<mpsc::Receiver<u32>>> = const { RefCell::new(None) };
}

struct AttachReceiver;

impl Wake for AttachReceiver {
    fn wake(self: Arc<Self>) {
        let (receiver, handle) = TO_ATTACH.with(|slot| slot.borrow_mut().take()).unwrap();
        let receiver = receiver.attach(&handle);
        ATTACHED.with(|slot| *slot.borrow_mut() = Some(receiver));
    }
}

/// A waker attaches a new receiver while the driver is waking receivers or
/// shutting down. During shutdown the new channel must still end up closed.
fn attach_from_waker(shutdown: bool) {
    let mut driver = Driver::new(SpinPark);
    let handle = driver.handle();
    let (sender, receiver) = mpsc::bounded::<u32>(&driver.endpoint(), 1);
    let (late_sender, late) = mpsc::bounded::<u32>(&driver.endpoint(), 1);
    let mut receiver = receiver.attach(&handle);
    TO_ATTACH.with(|slot| *slot.borrow_mut() = Some((late, handle.clone())));

    let waker = Waker::from(Arc::new(AttachReceiver));
    let mut cx = Context::from_waker(&waker);
    let mut recv = pin!(receiver.recv());
    assert!(recv.as_mut().poll(&mut cx).is_pending());

    if shutdown {
        driver.shutdown();
        assert_eq!(recv.as_mut().poll(&mut cx), Poll::Ready(None));
        assert!(late_sender.is_closed());
        let mut late = ATTACHED.with(|slot| slot.borrow_mut().take()).unwrap();
        assert_eq!(late.try_recv(), Err(mpsc::TryRecvError::Closed));
    } else {
        sender.try_send(7).unwrap();
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(recv.as_mut().poll(&mut cx), Poll::Ready(Some(7)));

        // The receiver attached from the waker is registered and woken by
        // later parks like any other.
        let mut late = ATTACHED.with(|slot| slot.borrow_mut().take()).unwrap();
        let late_wake = Arc::new(PanicThenCount {
            panics: 0,
            calls: AtomicUsize::new(0),
            woken: AtomicUsize::new(0),
        });
        let late_waker = Waker::from(late_wake.clone());
        let mut late_cx = Context::from_waker(&late_waker);
        let mut late_recv = pin!(late.recv());
        assert!(late_recv.as_mut().poll(&mut late_cx).is_pending());
        late_sender.try_send(8).unwrap();
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(late_wake.woken.load(Ordering::Relaxed), 1);
        assert_eq!(late_recv.as_mut().poll(&mut late_cx), Poll::Ready(Some(8)));
    }
}

#[test]
fn wake_callback_can_attach_a_receiver() {
    attach_from_waker(false);
}

#[test]
fn shutdown_callback_can_attach_a_receiver() {
    attach_from_waker(true);
}
