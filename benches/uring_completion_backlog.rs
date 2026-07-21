#![cfg(target_os = "linux")]

use std::borrow::Cow;
use std::hint::black_box;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use futures::stream::{Stream, StreamExt};
use futures::task::noop_waker;
use norn_uring::bufring::{BufRingBuf, RecvBufRing};
use norn_uring::net::UdpSocket;

mod support;

const RING_DEPTH: u32 = 512;
const BUFFER_COUNT: u16 = 32_768;
const BUFFER_LEN: usize = 8;

fn executor() -> norn_executor::LocalExecutor<norn_uring::Driver> {
    let mut builder = io_uring::IoUring::builder();
    builder
        .dontfork()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .setup_submit_all();
    norn_executor::LocalExecutor::new(norn_uring::Driver::new(builder, RING_DEPTH).unwrap())
}

struct SteadyMultishotBench(usize);

impl bencher::TDynBenchFn for SteadyMultishotBench {
    fn run(&self, b: &mut Bencher) {
        let messages = self.0;
        let mut executor = executor();
        let (sender, receiver, ring) = executor.block_on(async {
            let sender = UdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let receiver = UdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            sender
                .connect(receiver.local_addr().unwrap())
                .await
                .unwrap();
            receiver
                .connect(sender.local_addr().unwrap())
                .await
                .unwrap();
            let ring = RecvBufRing::builder(51)
                .buf_cnt(BUFFER_COUNT)
                .buf_len(BUFFER_LEN)
                .build()
                .unwrap();
            (sender, receiver, ring)
        });
        let mut recv = Box::pin(receiver.recv_ring_multi(&ring));

        b.bytes = (messages * BUFFER_LEN) as u64;
        b.iter(|| {
            executor.block_on(run_steady(&sender, recv.as_mut(), messages));
        });

        executor.block_on(async move {
            drop(recv);
            drop(ring);
            drop(receiver);
            drop(sender);
            norn_uring::noop().await;
        });
    }
}

async fn run_steady(
    sender: &UdpSocket,
    mut recv: Pin<&mut impl Stream<Item = io::Result<BufRingBuf>>>,
    messages: usize,
) {
    let mut payload = vec![0u8; BUFFER_LEN];
    for sequence in 0..messages {
        payload.copy_from_slice(&(sequence as u64).to_ne_bytes());
        let send = sender.send(payload);
        let mut recv_ref = recv.as_mut();
        let receive = recv_ref.next();
        let ((sent, next_payload), received) = futures::future::join(send, receive).await;
        assert_eq!(sent.unwrap(), BUFFER_LEN);
        payload = next_payload;
        let received = received
            .expect("steady multishot receive ended")
            .expect("steady multishot receive failed");
        assert_eq!(received.as_slice(), &(sequence as u64).to_ne_bytes());
    }
}

struct BurstBench(usize);

struct LaggedBurstBench(usize);

impl bencher::TDynBenchFn for LaggedBurstBench {
    fn run(&self, b: &mut Bencher) {
        let count = self.0;
        let mut executor = executor();
        let timeout = executor.block_on(async {
            let mut timeout =
                Box::pin(norn_uring::Handle::current().submit(MultishotTimeout::new(count)));
            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(matches!(timeout.as_mut().poll_next(&mut cx), Poll::Pending));

            // An IO_DRAIN submitted after the multishot timeout cannot finish
            // until the multishot request has produced its terminal CQE. Await
            // that fence without polling the stream so every timeout CQE
            // accumulates in the operation-local queue.
            norn_uring::Handle::current().submit(DrainNop).await;
            timeout
        });
        let mut timeout = Some(timeout);

        // Force one timed iteration: queue preparation above is deliberately
        // excluded so this isolates the cost of consuming a real CQE backlog.
        b.bench_n(1, |b| {
            b.iter(|| {
                let mut timeout = timeout.take().unwrap();
                executor.block_on(async {
                    let mut completions = 0usize;
                    while timeout.next().await.is_some() {
                        completions += 1;
                    }
                    assert_eq!(completions, count);
                });
                black_box(())
            });
        });
    }
}

#[derive(Debug)]
struct DrainNop;

unsafe impl norn_uring::Operation for DrainNop {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Nop::new()
            .build()
            .flags(io_uring::squeue::Flags::IO_DRAIN))
    }

    fn cleanup(&mut self, result: norn_uring::CQEResult) {
        result.into_result().unwrap();
    }
}

impl norn_uring::Singleshot for DrainNop {
    type Output = ();

    fn complete(self, result: norn_uring::CQEResult) -> Self::Output {
        result.into_result().unwrap();
    }
}

impl bencher::TDynBenchFn for BurstBench {
    fn run(&self, b: &mut Bencher) {
        let count = self.0;
        let mut executor = executor();
        b.bytes = (count * std::mem::size_of::<norn_uring::CQEResult>()) as u64;
        b.iter(|| {
            executor.block_on(async {
                let mut timeout =
                    Box::pin(norn_uring::Handle::current().submit(MultishotTimeout::new(count)));
                let mut completions = 0usize;
                while timeout.next().await.is_some() {
                    completions += 1;
                }
                assert_eq!(completions, count);
            });
        });
    }
}

#[derive(Debug)]
struct MultishotTimeout {
    interval: Box<io_uring::types::Timespec>,
    repeats: u32,
}

impl MultishotTimeout {
    fn new(repeats: usize) -> Self {
        Self {
            interval: Box::new(io_uring::types::Timespec::new()),
            repeats: repeats.try_into().unwrap(),
        }
    }
}

unsafe impl norn_uring::Operation for MultishotTimeout {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Timeout::new(&*self.interval)
            .count(self.repeats)
            .flags(io_uring::types::TimeoutFlags::MULTISHOT)
            .build())
    }

    fn cleanup(&mut self, result: norn_uring::CQEResult) {
        validate_timeout_result(result, true);
    }
}

impl norn_uring::Multishot for MultishotTimeout {
    type Item = ();

    fn update(&mut self, result: norn_uring::CQEResult) -> Self::Item {
        validate_timeout_result(result, false);
    }

    fn complete(self, result: norn_uring::CQEResult) -> Option<Self::Item> {
        validate_timeout_result(result, true);
        Some(())
    }
}

fn validate_timeout_result(result: norn_uring::CQEResult, cancellation_allowed: bool) {
    let err = result
        .into_result()
        .expect_err("multishot timeout completion unexpectedly succeeded");
    assert!(
        err.raw_os_error() == Some(libc::ETIME)
            || cancellation_allowed && err.raw_os_error() == Some(libc::ECANCELED),
        "unexpected multishot timeout completion: {err}"
    );
}

fn benches() -> Vec<TestDescAndFn> {
    let mut benches = Vec::new();
    benches.push(TestDescAndFn {
        desc: TestDesc {
            name: Cow::from("real_multishot/steady/messages=4096"),
            ignore: false,
        },
        testfn: TestFn::DynBenchFn(Box::new(SteadyMultishotBench(4_096))),
    });
    for count in [64usize, 1_024, 4_096] {
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!("real_multishot/burst/messages={count}")),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(BurstBench(count))),
        });
    }
    benches.push(TestDescAndFn {
        desc: TestDesc {
            name: Cow::from("real_multishot/lagged/messages=16384/consume"),
            ignore: false,
        },
        testfn: TestFn::DynBenchFn(Box::new(LaggedBurstBench(16_384))),
    });
    benches
}

fn main() {
    support::run(benches());
}
