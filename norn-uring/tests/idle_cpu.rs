#![cfg(target_os = "linux")]

//! An idle executor must block in the kernel. After each scenario exercises
//! cancellation, linking, backpressure, readiness, remote unparks or signals on
//! one ring setup, the runtime thread's CPU time over an idle wait must stay
//! low, and shutdown must be quick.
//!
//! The matrix measures time, so it is ignored by default. Run it with
//! `cargo test -p norn-uring --test idle_cpu -- --ignored`, and raise the CPU
//! budget with `IDLE_CPU_MS=<ms>` (30 by default) on a loaded host.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::{Duration, Instant};

use norn_uring::{CQEResult, Handle, Operation, Request, Singleshot};

enum Probe {
    Nop,
    AsyncRead { fd: i32, buf: Box<[u8; 8]> },
    BlockedRead { fd: i32, buf: Box<[u8; 8]> },
}

// Safety: each read targets a pipe that outlives the driver, into a buffer the
// operation owns; every entry posts one terminal CQE.
unsafe impl Operation for Probe {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(match self {
            Probe::Nop => io_uring::opcode::Nop::new().build(),
            Probe::AsyncRead { fd, buf } => {
                io_uring::opcode::Read::new(io_uring::types::Fd(*fd), buf.as_mut_ptr(), 8)
                    .build()
                    .flags(io_uring::squeue::Flags::ASYNC)
            }
            Probe::BlockedRead { fd, buf } => {
                io_uring::opcode::Read::new(io_uring::types::Fd(*fd), buf.as_mut_ptr(), 8).build()
            }
        })
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Probe {
    type Output = io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion.into_result()
    }
}

fn pipe() -> (OwnedFd, OwnedFd) {
    let mut fds = [0; 2];
    // Safety: `fds` has room for both descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // Safety: pipe2 returned two descriptors that nothing else owns.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

fn thread_cpu_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // Safety: `ts` is a valid timespec to write.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) },
        0
    );
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

fn read(handle: &Handle, fd: i32) -> norn_uring::Op<Probe> {
    handle.submit(Probe::BlockedRead {
        fd,
        buf: Box::new([0; 8]),
    })
}

async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return std::task::Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    })
    .await
}

fn poll_once<F: std::future::Future>(fut: std::pin::Pin<&mut F>) {
    let waker = futures_test::task::noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    let _ = fut.poll(&mut cx);
}

#[derive(Clone, Copy, Debug)]
enum Setup {
    Default,
    Coop,
    Defer,
    Sqpoll,
}

fn builder(setup: Setup) -> io_uring::Builder {
    let mut b = io_uring::IoUring::builder();
    match setup {
        Setup::Default => {}
        Setup::Coop => {
            b.setup_coop_taskrun();
        }
        Setup::Defer => {
            b.setup_coop_taskrun()
                .setup_defer_taskrun()
                .setup_single_issuer();
        }
        Setup::Sqpoll => {
            b.setup_sqpoll(50);
        }
    }
    b
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Plain,
    DropReadThenRead,
    DropNopThenRead,
    DropReadWithLinkTimeout,
    DropChainWithLinkTimeout,
    QueuedReads,
    DropQueuedReads,
    UdpReadinessMulti,
    DropNopThenReadSpawned,
    RemoteUnparks,
    Signals,
    PendingAsyncRead,
    DropAsyncReadThenRead,
}

const IDLE: Duration = Duration::from_millis(300);

fn run(setup: Setup, size: u32, scenario: Scenario) -> (Duration, Duration) {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let driver = norn_uring::Driver::new(builder(setup), size).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let helper = match scenario {
            Scenario::RemoteUnparks => {
                use norn_executor::park::{Park, Unpark};
                let unparker = driver.unparker();
                let stop = stop.clone();
                Some(std::thread::spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        unparker.unpark();
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }))
            }
            Scenario::Signals => {
                extern "C" fn handler(_: libc::c_int) {}
                // Safety: the handler does nothing, so it is async-signal-safe.
                unsafe { libc::signal(libc::SIGUSR1, handler as *const () as libc::sighandler_t) };
                // Safety: pthread_self has no preconditions.
                let tid = unsafe { libc::pthread_self() };
                let stop = stop.clone();
                let tid = tid as usize;
                Some(std::thread::spawn(move || {
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        // Safety: the runtime thread outlives this helper, which
                        // is joined before that thread exits.
                        unsafe { libc::pthread_kill(tid as libc::pthread_t, libc::SIGUSR1) };
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }))
            }
            _ => None,
        };
        let mut ex = norn_executor::LocalExecutor::new(driver);
        let (r1, _w1) = pipe();
        let (r2, _w2) = pipe();
        let (fd1, fd2) = (r1.as_raw_fd(), r2.as_raw_fd());
        let cpu = ex.block_on(async move {
            let handle = Handle::current();
            let mut keep: Vec<Box<dyn std::any::Any>> = Vec::new();
            match scenario {
                Scenario::Plain | Scenario::RemoteUnparks | Scenario::Signals => {}
                Scenario::PendingAsyncRead => {
                    let op = handle.submit(Probe::AsyncRead {
                        fd: fd1,
                        buf: Box::new([0; 8]),
                    });
                    keep.push(Box::new(norn_executor::spawn(async move {
                        let _ = op.await;
                    })));
                }
                Scenario::DropAsyncReadThenRead => {
                    let mut chain = Box::pin(
                        handle
                            .submit(Probe::AsyncRead {
                                fd: fd1,
                                buf: Box::new([0; 8]),
                            })
                            .then(read(&handle, fd2)),
                    );
                    poll_once(chain.as_mut());
                    yield_now().await;
                    drop(chain);
                }
                Scenario::DropReadThenRead => {
                    let mut chain = Box::pin(read(&handle, fd1).then(read(&handle, fd2)));
                    poll_once(chain.as_mut());
                    yield_now().await;
                    drop(chain);
                }
                Scenario::DropNopThenRead => {
                    let mut chain = Box::pin(handle.submit(Probe::Nop).then(read(&handle, fd2)));
                    poll_once(chain.as_mut());
                    yield_now().await;
                    yield_now().await;
                    drop(chain);
                }
                Scenario::DropNopThenReadSpawned => {
                    let chain = handle.submit(Probe::Nop).then(read(&handle, fd2));
                    let task = norn_executor::spawn(async move {
                        let _ = chain.await;
                    });
                    yield_now().await;
                    yield_now().await;
                    task.abort();
                    keep.push(Box::new(task));
                }
                Scenario::DropReadWithLinkTimeout => {
                    let mut chain = Box::pin(read(&handle, fd1).timeout(Duration::from_secs(10)));
                    poll_once(chain.as_mut());
                    yield_now().await;
                    drop(chain);
                }
                Scenario::DropChainWithLinkTimeout => {
                    let mut chain = Box::pin(
                        read(&handle, fd1)
                            .then(read(&handle, fd2))
                            .timeout(Duration::from_secs(10)),
                    );
                    poll_once(chain.as_mut());
                    yield_now().await;
                    drop(chain);
                }
                Scenario::QueuedReads | Scenario::DropQueuedReads => {
                    let mut ops: Vec<_> = (0..6)
                        .map(|i| Box::pin(read(&handle, if i % 2 == 0 { fd1 } else { fd2 })))
                        .collect();
                    for op in &mut ops {
                        poll_once(op.as_mut());
                    }
                    if matches!(scenario, Scenario::DropQueuedReads) {
                        drop(ops);
                    } else {
                        std::mem::forget(ops);
                    }
                }
                Scenario::UdpReadinessMulti => {
                    let sock = norn_uring::net::UdpSocket::bind("127.0.0.1:0".parse().unwrap())
                        .await
                        .unwrap();
                    keep.push(Box::new(norn_executor::spawn(async move {
                        use futures_util::StreamExt;
                        let mut s = std::pin::pin!(sock.poll_readiness_multi(libc::POLLIN as u32));
                        while let Some(ev) = s.next().await {
                            if ev.is_err() {
                                break;
                            }
                        }
                    })));
                }
            }
            // Let cancellations settle.
            for _ in 0..4 {
                yield_now().await;
            }
            let _ = handle.timeout(Duration::from_millis(20)).await;
            let start = thread_cpu_time();
            let _ = handle.timeout(IDLE).await;
            let cpu = thread_cpu_time() - start;
            drop(keep);
            cpu
        });
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        if let Some(h) = helper {
            h.join().unwrap();
        }
        let start = Instant::now();
        drop(ex);
        let shutdown = start.elapsed();
        drop((r1, r2));
        tx.send((cpu, shutdown)).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(20))
        .unwrap_or_else(|_| panic!("{setup:?}/{size}/{scenario:?} hung"))
}

#[test]
#[ignore = "measures CPU time; run with --ignored"]
fn idle_cpu_matrix() {
    let setups = [
        (Setup::Default, 8),
        (Setup::Coop, 8),
        (Setup::Defer, 8),
        (Setup::Sqpoll, 8),
        (Setup::Default, 1),
        (Setup::Defer, 1),
        (Setup::Sqpoll, 1),
        (Setup::Default, 2),
        (Setup::Coop, 2),
    ];
    let scenarios = [
        Scenario::Plain,
        Scenario::DropReadThenRead,
        Scenario::DropNopThenRead,
        Scenario::DropNopThenReadSpawned,
        Scenario::DropReadWithLinkTimeout,
        Scenario::DropChainWithLinkTimeout,
        Scenario::QueuedReads,
        Scenario::DropQueuedReads,
        Scenario::UdpReadinessMulti,
        Scenario::RemoteUnparks,
        Scenario::Signals,
        Scenario::PendingAsyncRead,
        Scenario::DropAsyncReadThenRead,
    ];
    let mut bad = Vec::new();
    for (setup, size) in setups {
        for scenario in scenarios {
            let (cpu, shutdown) = run(setup, size, scenario);
            eprintln!("{setup:?}/{size}/{scenario:?}: idle cpu {cpu:?} shutdown {shutdown:?}");
            if cpu
                > Duration::from_millis(
                    std::env::var("IDLE_CPU_MS")
                        .ok()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(30),
                )
                || shutdown > Duration::from_millis(250)
            {
                bad.push(format!(
                    "{setup:?}/{size}/{scenario:?}: cpu {cpu:?} shutdown {shutdown:?}"
                ));
            }
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}
