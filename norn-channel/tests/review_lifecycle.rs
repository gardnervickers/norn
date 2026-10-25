use norn_channel::{mpsc, Driver};
use norn_executor::park::SpinPark;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc as std_mpsc, Arc,
};

#[cfg(miri)]
const ITERATIONS: usize = 16;
#[cfg(not(miri))]
const ITERATIONS: usize = 100_000;

#[test]
fn review_last_send_is_not_hidden_by_disconnect() {
    let driver = Driver::new(SpinPark);
    let (dispatch, requests) = std_mpsc::channel::<mpsc::Sender<usize>>();
    let producer = std::thread::spawn(move || {
        for sender in requests {
            sender.try_send(42).unwrap();
        }
    });
    for iteration in 0..ITERATIONS {
        let (sender, detached) = mpsc::bounded(&driver.endpoint(), 1);
        let mut receiver = detached.attach(&driver.handle());
        dispatch.send(sender).unwrap();
        loop {
            match receiver.try_recv() {
                Ok(42) => break,
                Ok(other) => panic!("unexpected {other}"),
                Err(mpsc::TryRecvError::Empty) => std::hint::spin_loop(),
                Err(mpsc::TryRecvError::Closed) => {
                    panic!(
                        "closed before final message at iteration {iteration}; next receive = {:?}",
                        receiver.try_recv()
                    );
                }
            }
        }
    }
    drop(dispatch);
    producer.join().unwrap();
}

#[test]
fn review_sharded_last_send_is_not_hidden_by_disconnect() {
    let driver = Driver::new(SpinPark);
    let (dispatch, requests) = std_mpsc::channel::<mpsc::ShardedSender<usize>>();
    let producer = std::thread::spawn(move || {
        for sender in requests {
            sender.try_send(42).unwrap();
        }
    });
    for iteration in 0..ITERATIONS {
        let (mut senders, detached) = mpsc::bounded_sharded(&driver.endpoint(), 1, 1);
        let mut receiver = detached.attach(&driver.handle());
        dispatch.send(senders.pop().unwrap()).unwrap();
        loop {
            match receiver.try_recv() {
                Ok(42) => break,
                Ok(other) => panic!("unexpected {other}"),
                Err(mpsc::TryRecvError::Empty) => std::hint::spin_loop(),
                Err(mpsc::TryRecvError::Closed) => panic!("sharded closed before final message at iteration {iteration}; next receive = {:?}", receiver.try_recv()),
            }
        }
    }
    drop(dispatch);
    producer.join().unwrap();
}

macro_rules! channel {
    (plain, $endpoint:expr, $capacity:expr) => {
        mpsc::bounded($endpoint, $capacity)
    };
    (sharded, $endpoint:expr, $capacity:expr) => {{
        let (mut senders, receiver) = mpsc::bounded_sharded($endpoint, $capacity, 2);
        (senders.pop().unwrap(), receiver)
    }};
}

macro_rules! drop_tests {
    ($name:ident, $kind:ident, $sender:ident) => {
        #[test]
        fn $name() {
            struct Message {
                _sender: Option<mpsc::$sender<Message>>,
                dropped: Arc<AtomicUsize>,
            }
            impl Drop for Message {
                fn drop(&mut self) {
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            for attached in [false, true] {
                let driver = Driver::new(SpinPark);
                let dropped = Arc::new(AtomicUsize::new(0));
                let (sender, receiver) = channel!($kind, &driver.endpoint(), 4);
                for _ in 0..2 {
                    sender
                        .try_send(Message {
                            _sender: None,
                            dropped: dropped.clone(),
                        })
                        .ok()
                        .unwrap();
                }
                if attached {
                    drop(receiver.attach(&driver.handle()));
                } else {
                    drop(receiver);
                }
                assert!(sender.is_closed());
                assert_eq!(dropped.load(Ordering::Relaxed), 2);
                drop(sender);
                assert_eq!(dropped.load(Ordering::Relaxed), 2);

                // The queued message is the last remaining sender, forming a
                // cycle that only terminal receiver destruction can break.
                let (sender, receiver) = channel!($kind, &driver.endpoint(), 2);
                sender
                    .try_send(Message {
                        _sender: Some(sender.clone()),
                        dropped: dropped.clone(),
                    })
                    .ok()
                    .unwrap();
                drop(sender);
                if attached {
                    drop(receiver.attach(&driver.handle()));
                } else {
                    drop(receiver);
                }
                assert_eq!(dropped.load(Ordering::Relaxed), 3);

                // A pair of request/reply queues can form the same cycle.
                let (sender_a, receiver_a) = channel!($kind, &driver.endpoint(), 2);
                let (sender_b, receiver_b) = channel!($kind, &driver.endpoint(), 2);
                sender_a
                    .try_send(Message {
                        _sender: Some(sender_b.clone()),
                        dropped: dropped.clone(),
                    })
                    .ok()
                    .unwrap();
                sender_b
                    .try_send(Message {
                        _sender: Some(sender_a.clone()),
                        dropped: dropped.clone(),
                    })
                    .ok()
                    .unwrap();
                drop((sender_a, sender_b));
                if attached {
                    drop((
                        receiver_a.attach(&driver.handle()),
                        receiver_b.attach(&driver.handle()),
                    ));
                } else {
                    drop((receiver_a, receiver_b));
                }
                assert_eq!(dropped.load(Ordering::Relaxed), 5);
            }
        }
    };
}

drop_tests!(
    plain_receiver_drop_discards_values_and_breaks_cycles,
    plain,
    Sender
);
drop_tests!(
    sharded_receiver_drop_discards_values_and_breaks_cycles,
    sharded,
    ShardedSender
);

macro_rules! concurrent_drop_test {
    ($name:ident, $kind:ident) => {
        #[test]
        fn $name() {
            struct CountDrop(Arc<AtomicUsize>);
            impl Drop for CountDrop {
                fn drop(&mut self) {
                    self.0.fetch_add(1, Ordering::Relaxed);
                }
            }
            #[cfg(miri)]
            const ROUNDS: usize = 2;
            #[cfg(not(miri))]
            const ROUNDS: usize = 100;
            for attached in [false, true] {
                for _ in 0..ROUNDS {
                    let driver = Driver::new(SpinPark);
                    let (sender, receiver) = channel!($kind, &driver.endpoint(), 8);
                    let dropped = Arc::new(AtomicUsize::new(0));
                    let start = Arc::new(std::sync::Barrier::new(5));
                    let producers: Vec<_> = (0..4)
                        .map(|_| {
                            let sender = sender.clone();
                            let start = start.clone();
                            let dropped = dropped.clone();
                            std::thread::spawn(move || {
                                start.wait();
                                for _ in 0..16 {
                                    // A failed send returns ownership; discard it
                                    // too so exactly 64 values must be destroyed.
                                    drop(sender.try_send(CountDrop(dropped.clone())));
                                }
                                sender
                            })
                        })
                        .collect();
                    if attached {
                        let receiver = receiver.attach(&driver.handle());
                        start.wait();
                        drop(receiver);
                    } else {
                        start.wait();
                        drop(receiver);
                    }
                    let senders: Vec<_> =
                        producers.into_iter().map(|p| p.join().unwrap()).collect();
                    assert_eq!(dropped.load(Ordering::Relaxed), 64);
                    assert!(sender.is_closed());
                    drop((sender, senders));
                    assert_eq!(dropped.load(Ordering::Relaxed), 64);
                }
            }
        }
    };
}

concurrent_drop_test!(
    plain_concurrent_send_and_receiver_drop_discards_every_value,
    plain
);
concurrent_drop_test!(
    sharded_concurrent_send_and_receiver_drop_discards_every_value,
    sharded
);

#[test]
fn sharded_receiver_drop_discards_every_lane() {
    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    for attached in [false, true] {
        let driver = Driver::new(SpinPark);
        let (senders, receiver) = mpsc::bounded_sharded(&driver.endpoint(), 6, 3);
        let dropped = Arc::new(AtomicUsize::new(0));
        for sender in &senders {
            for _ in 0..2 {
                sender.try_send(CountDrop(dropped.clone())).ok().unwrap();
            }
        }
        if attached {
            drop(receiver.attach(&driver.handle()));
        } else {
            drop(receiver);
        }
        assert_eq!(dropped.load(Ordering::Relaxed), 6);
        assert!(senders.iter().all(mpsc::ShardedSender::is_closed));
        drop(senders);
        assert_eq!(dropped.load(Ordering::Relaxed), 6);
    }
}

#[test]
fn failed_attachment_discards_queued_values() {
    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    macro_rules! check {
        ($sender:expr, $receiver:expr, $handle:expr) => {{
            let sender = $sender;
            let receiver = $receiver;
            let dropped = Arc::new(AtomicUsize::new(0));
            sender.try_send(CountDrop(dropped.clone())).ok().unwrap();
            assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                receiver.attach($handle);
            }))
            .is_err());
            assert!(sender.is_closed());
            assert_eq!(dropped.load(Ordering::Relaxed), 1);
        }};
    }
    let driver = Driver::new(SpinPark);
    let wrong_driver = Driver::new(SpinPark);
    let (sender, receiver) = mpsc::bounded(&driver.endpoint(), 1);
    check!(sender, receiver, &wrong_driver.handle());
    let (mut senders, receiver) = mpsc::bounded_sharded(&driver.endpoint(), 2, 2);
    check!(senders.pop().unwrap(), receiver, &wrong_driver.handle());
}
