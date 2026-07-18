use std::borrow::Cow;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::mpsc::{self as std_mpsc, Receiver as StdReceiver, SyncSender};
use std::task::{Context, Poll};
use std::thread::{self, JoinHandle};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_channel::mpsc::{self, Sender, TrySendError};
use norn_channel::{Driver, DriverBuilder};
use norn_executor::park::{SpinPark, ThreadPark};
use norn_executor::LocalExecutor;

mod support;

const MESSAGES_PER_ROUND: usize = 262_144;
const QUEUE_CAPACITY: usize = MESSAGES_PER_ROUND;
const STOP: usize = usize::MAX;

struct ThroughputBench {
    producers: usize,
    receive_limit: usize,
    sharded: bool,
}

enum ThroughputSender {
    Shared(Sender<usize>),
    Sharded(norn_channel::mpsc::ShardedSender<usize>),
}

macro_rules! consume_throughput {
    ($receiver:ident, $consumer_done:ident, $receive_limit:expr) => {
        async move {
            let mut messages = Vec::with_capacity($receive_limit);
            let mut round_messages = 0;
            loop {
                let received = $receiver.recv_many(&mut messages, $receive_limit).await;
                if received == 0 {
                    break;
                }
                assert!(received <= $receive_limit);
                for message in messages.drain(..) {
                    assert_ne!(message, STOP);
                    round_messages += 1;
                }
                if round_messages == MESSAGES_PER_ROUND {
                    $consumer_done.send(()).unwrap();
                    round_messages = 0;
                } else {
                    assert!(round_messages < MESSAGES_PER_ROUND);
                }
            }
            assert_eq!(round_messages, 0);
        }
    };
}

macro_rules! spawn_throughput_producer {
    ($sender:ident, $cpu:ident, $start_rx:ident, $done_tx:ident, $messages:ident, $send:ident) => {
        thread::spawn(move || {
            pin_current_thread($cpu);
            while $start_rx.recv().is_ok() {
                for value in 0..$messages {
                    $send(&$sender, value);
                }
                $done_tx.send(()).unwrap();
            }
        })
    };
}

impl bencher::TDynBenchFn for ThroughputBench {
    fn run(&self, b: &mut Bencher) {
        let mut runtime = ThroughputRuntime::new(self.producers, self.receive_limit, self.sharded);
        b.bytes = (MESSAGES_PER_ROUND * std::mem::size_of::<usize>()) as u64;
        b.iter(|| {
            runtime.run_round();
            black_box(())
        });
    }
}

struct ThroughputRuntime {
    starts: Vec<SyncSender<()>>,
    producer_done: Vec<StdReceiver<()>>,
    consumer_done: StdReceiver<()>,
    producers: Vec<JoinHandle<()>>,
    consumer: Option<JoinHandle<()>>,
}

impl ThroughputRuntime {
    fn new(producers: usize, receive_limit: usize, sharded: bool) -> Self {
        assert!(producers > 0);
        assert_eq!(MESSAGES_PER_ROUND % producers, 0);

        let (consumer_done_tx, consumer_done) = std_mpsc::sync_channel(1);
        let (mut senders, consumer) = if sharded {
            let driver = DriverBuilder::new();
            let (senders, receiver) =
                mpsc::bounded_sharded(driver.endpoint(), QUEUE_CAPACITY, producers);
            let senders = senders.into_iter().map(ThroughputSender::Sharded).collect();
            let consumer = thread::spawn(move || {
                pin_current_thread(consumer_cpu());
                let driver = driver.build(ThreadPark::default());
                let mut receiver = receiver.attach(&driver.handle());
                let mut executor = LocalExecutor::new(driver);
                executor.block_on(consume_throughput!(
                    receiver,
                    consumer_done_tx,
                    receive_limit
                ));
            });
            (senders, consumer)
        } else {
            let driver = DriverBuilder::new();
            let (sender, receiver) = mpsc::bounded(driver.endpoint(), QUEUE_CAPACITY);
            let consumer = thread::spawn(move || {
                pin_current_thread(consumer_cpu());
                let driver = driver.build(ThreadPark::default());
                let mut receiver = receiver.attach(&driver.handle());
                let mut executor = LocalExecutor::new(driver);
                executor.block_on(consume_throughput!(
                    receiver,
                    consumer_done_tx,
                    receive_limit
                ));
            });
            (vec![ThroughputSender::Shared(sender)], consumer)
        };

        if senders.len() == 1 {
            for _ in 1..producers {
                let sender = match &senders[0] {
                    ThroughputSender::Shared(sender) => ThroughputSender::Shared(sender.clone()),
                    ThroughputSender::Sharded(_) => unreachable!(),
                };
                senders.push(sender);
            }
        }
        assert_eq!(senders.len(), producers);
        let producer_cpus = producer_cpus();
        let messages_per_producer = MESSAGES_PER_ROUND / producers;
        let mut starts = Vec::with_capacity(producers);
        let mut producer_done = Vec::with_capacity(producers);
        let mut producer_threads = Vec::with_capacity(producers);

        for (index, sender) in senders.into_iter().enumerate() {
            let (start_tx, start_rx) = std_mpsc::sync_channel(1);
            let (done_tx, done_rx) = std_mpsc::sync_channel(1);
            let cpu = producer_cpus.get(index).copied();
            let producer = match sender {
                ThroughputSender::Shared(sender) => spawn_throughput_producer!(
                    sender,
                    cpu,
                    start_rx,
                    done_tx,
                    messages_per_producer,
                    send_retry
                ),
                ThroughputSender::Sharded(sender) => spawn_throughput_producer!(
                    sender,
                    cpu,
                    start_rx,
                    done_tx,
                    messages_per_producer,
                    send_sharded_retry
                ),
            };
            producer_threads.push(producer);
            starts.push(start_tx);
            producer_done.push(done_rx);
        }
        Self {
            starts,
            producer_done,
            consumer_done,
            producers: producer_threads,
            consumer: Some(consumer),
        }
    }

    fn run_round(&mut self) {
        for start in &self.starts {
            start.send(()).unwrap();
        }
        for done in &self.producer_done {
            done.recv().unwrap();
        }
        self.consumer_done.recv().unwrap();
    }
}

impl Drop for ThroughputRuntime {
    fn drop(&mut self) {
        self.starts.clear();
        for producer in self.producers.drain(..) {
            producer.join().unwrap();
        }
        self.consumer.take().unwrap().join().unwrap();
    }
}

enum Ping {
    Message(usize),
    Stop,
}

struct RoundTripBench;

impl bencher::TDynBenchFn for RoundTripBench {
    fn run(&self, b: &mut Bencher) {
        let mut runtime = RoundTripRuntime::new();
        let mut sequence = 0;
        b.iter(|| {
            sequence += 1;
            black_box(runtime.round_trip(black_box(sequence)));
        });
    }
}

struct RoundTripRuntime {
    executor: LocalExecutor<Driver<ThreadPark>>,
    request: Sender<Ping>,
    response: norn_channel::mpsc::Receiver<usize>,
    worker: Option<JoinHandle<()>>,
}

impl RoundTripRuntime {
    fn new() -> Self {
        // Construct both directions before either runtime starts.
        let local_driver = DriverBuilder::new();
        let worker_driver = DriverBuilder::new();
        let (request, request_rx) = mpsc::bounded(worker_driver.endpoint(), 1);
        let (response_tx, response) = mpsc::bounded(local_driver.endpoint(), 1);

        let driver = local_driver.build(ThreadPark::default());
        let response = response.attach(&driver.handle());
        let executor = LocalExecutor::new(driver);

        let worker = thread::spawn(move || {
            pin_current_thread(consumer_cpu());
            let driver = worker_driver.build(ThreadPark::default());
            let mut request_rx = request_rx.attach(&driver.handle());
            let mut executor = LocalExecutor::new(driver);

            executor.block_on(async move {
                while let Some(message) = request_rx.recv().await {
                    match message {
                        Ping::Message(value) => send_retry(&response_tx, value),
                        Ping::Stop => break,
                    }
                }
            });
        });

        Self {
            executor,
            request,
            response,
            worker: Some(worker),
        }
    }

    fn round_trip(&mut self, value: usize) -> usize {
        self.executor.block_on(async {
            send_retry(&self.request, Ping::Message(value));
            let response = self.response.recv().await.expect("worker closed early");
            assert_eq!(response, value);
            response
        })
    }
}

impl Drop for RoundTripRuntime {
    fn drop(&mut self) {
        send_retry(&self.request, Ping::Stop);
        self.worker.take().unwrap().join().unwrap();
    }
}

struct PlainYieldBench;

impl bencher::TDynBenchFn for PlainYieldBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = LocalExecutor::new(SpinPark);
        b.iter(|| {
            executor.block_on(YieldOnce(false));
            black_box(())
        });
    }
}

struct ChannelYieldBench;

impl bencher::TDynBenchFn for ChannelYieldBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = LocalExecutor::new(Driver::new(SpinPark));
        b.iter(|| {
            executor.block_on(YieldOnce(false));
            black_box(())
        });
    }
}

struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn send_retry<T>(sender: &Sender<T>, mut value: T) {
    loop {
        match sender.try_send(value) {
            Ok(()) => return,
            Err(TrySendError::Full(returned)) => {
                value = returned;
                std::hint::spin_loop();
            }
            Err(TrySendError::Closed(_)) => panic!("channel closed early"),
        }
    }
}

fn send_sharded_retry(sender: &norn_channel::mpsc::ShardedSender<usize>, mut value: usize) {
    loop {
        match sender.try_send(value) {
            Ok(()) => return,
            Err(TrySendError::Full(returned)) => {
                value = returned;
                std::hint::spin_loop();
            }
            Err(TrySendError::Closed(_)) => panic!("channel closed early"),
        }
    }
}

fn benches() -> Vec<TestDescAndFn> {
    let mut benches = vec![
        dynamic_bench(
            "throughput/1p1c/recv_limit=1/messages=262144",
            ThroughputBench {
                producers: 1,
                receive_limit: 1,
                sharded: false,
            },
        ),
        dynamic_bench(
            "throughput/1p1c/recv_limit=16/messages=262144",
            ThroughputBench {
                producers: 1,
                receive_limit: 16,
                sharded: false,
            },
        ),
        dynamic_bench(
            "throughput/1p1c/recv_limit=32/messages=262144",
            ThroughputBench {
                producers: 1,
                receive_limit: 32,
                sharded: false,
            },
        ),
        dynamic_bench(
            "throughput/4p1c/recv_limit=32/messages=262144",
            ThroughputBench {
                producers: 4,
                receive_limit: 32,
                sharded: false,
            },
        ),
        dynamic_bench(
            "throughput/4p1c/sharded/recv_limit=32/messages=262144",
            ThroughputBench {
                producers: 4,
                receive_limit: 32,
                sharded: true,
            },
        ),
        dynamic_bench("latency/1p1c/round_trip", RoundTripBench),
        dynamic_bench("executor/yield_once/plain", PlainYieldBench),
        dynamic_bench("executor/yield_once/channel_driver", ChannelYieldBench),
    ];
    benches.sort_by(|left, right| left.desc.name.cmp(&right.desc.name));
    benches
}

fn dynamic_bench(name: &'static str, bench: impl bencher::TDynBenchFn + 'static) -> TestDescAndFn {
    TestDescAndFn {
        desc: TestDesc {
            name: Cow::Borrowed(name),
            ignore: false,
        },
        testfn: TestFn::DynBenchFn(Box::new(bench)),
    }
}

fn consumer_cpu() -> Option<usize> {
    std::env::var("NORN_CHANNEL_CONSUMER_CPU")
        .ok()
        .map(|value| value.parse().expect("invalid consumer CPU"))
}

fn producer_cpus() -> Vec<usize> {
    std::env::var("NORN_CHANNEL_PRODUCER_CPUS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|cpu| cpu.parse().expect("invalid producer CPU"))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(target_os = "linux")]
fn pin_current_thread(cpu: Option<usize>) {
    let Some(cpu) = cpu else {
        return;
    };
    unsafe {
        let mut set = std::mem::zeroed::<libc::cpu_set_t>();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let result = libc::pthread_setaffinity_np(
            libc::pthread_self(),
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        );
        assert_eq!(result, 0, "failed to pin benchmark thread to CPU {cpu}");
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread(_: Option<usize>) {}

fn main() {
    pin_current_thread(producer_cpus().first().copied());
    support::run(benches());
}
