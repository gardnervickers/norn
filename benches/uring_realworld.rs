#![cfg(target_os = "linux")]

use std::borrow::Cow;
use std::cmp;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use bencher::bench;
use bencher::{run_tests_console, Bencher, TestDesc, TestDescAndFn, TestFn, TestOpts};
use futures::stream::{FuturesUnordered, StreamExt};

use norn_uring::fs;
use norn_uring::net::UdpSocket;

const RING_DEPTH: u32 = 256;

fn new_executor() -> norn_executor::LocalExecutor<norn_uring::Driver> {
    let mut builder = io_uring::IoUring::builder();
    builder
        .dontfork()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .setup_submit_all();
    let driver = norn_uring::Driver::new(builder, RING_DEPTH).unwrap();
    norn_executor::LocalExecutor::new(driver)
}

struct BenchDir {
    path: PathBuf,
}

impl BenchDir {
    fn new(tag: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock went backwards")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "norn-bench-{tag}-pid{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn join(&self, path: impl AsRef<Path>) -> PathBuf {
        self.path.join(path)
    }
}

impl Drop for BenchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

async fn udp_request_response_worker(
    sockets: &[UdpSocket],
    server_addr: std::net::SocketAddr,
    payload_len: usize,
    total_requests: usize,
) -> io::Result<()> {
    let lanes = cmp::max(1, sockets.len());
    let per_lane = total_requests / lanes;
    let extra = total_requests % lanes;
    let mut pending = FuturesUnordered::new();

    for (lane, socket) in sockets.iter().enumerate() {
        let lane_requests = per_lane + usize::from(lane < extra);
        if lane_requests > 0 {
            pending.push(Box::pin(udp_request_response_lane(
                socket,
                server_addr,
                payload_len,
                lane_requests,
            )));
        }
    }

    while let Some(result) = pending.next().await {
        result?;
    }

    Ok(())
}

async fn udp_request_response_lane(
    socket: &UdpSocket,
    server_addr: std::net::SocketAddr,
    payload_len: usize,
    requests: usize,
) -> io::Result<()> {
    let mut send_buf = vec![0x5A; payload_len];
    let mut recv_buf = vec![0u8; payload_len];
    for _ in 0..requests {
        let (send_res, send) = socket.send_to(send_buf, server_addr).await;
        send_buf = send;
        let sent = send_res?;
        assert_eq!(sent, payload_len);

        let (recv_res, recv) = socket.recv_from(recv_buf).await;
        recv_buf = recv;
        let (n, _) = recv_res?;
        assert_eq!(n, payload_len);
    }
    Ok(())
}

struct ThreadUdpEchoServer {
    addr: std::net::SocketAddr,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl ThreadUdpEchoServer {
    fn new(payload_len: usize) -> io::Result<Self> {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0")?;
        socket.set_read_timeout(Some(Duration::from_millis(50)))?;
        let addr = socket.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);

        let thread = std::thread::spawn(move || {
            let mut buf = vec![0u8; payload_len];
            while !stop_clone.load(Ordering::Relaxed) {
                match socket.recv_from(&mut buf) {
                    Ok((n, peer)) => {
                        let sent = socket.send_to(&buf[..n], peer).unwrap();
                        assert_eq!(sent, n);
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {}
                    Err(err) if err.kind() == io::ErrorKind::TimedOut => {}
                    Err(err) => panic!("udp echo server recv failed: {err}"),
                }
            }
        });

        Ok(Self {
            addr,
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for ThreadUdpEchoServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = std::net::UdpSocket::bind("127.0.0.1:0")
            .and_then(|waker| waker.send_to(&[0u8], self.addr).map(|_| ()));
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct UdpRequestResponseBench {
    total_requests: usize,
    payload_len: usize,
    window: usize,
}

impl UdpRequestResponseBench {
    fn new(total_requests: usize, payload_len: usize, window: usize) -> Self {
        Self {
            total_requests,
            payload_len,
            window,
        }
    }
}

impl bencher::TDynBenchFn for UdpRequestResponseBench {
    fn run(&self, b: &mut Bencher) {
        let server = ThreadUdpEchoServer::new(self.payload_len).unwrap();
        let server_addr = server.addr;
        let mut ex = new_executor();
        let lane_count = cmp::max(1, cmp::min(self.window, self.total_requests));
        let mut client_sockets = ex.block_on(async {
            let mut sockets = Vec::with_capacity(lane_count);
            for _ in 0..lane_count {
                sockets.push(
                    UdpSocket::bind("127.0.0.1:0".parse().unwrap())
                        .await
                        .unwrap(),
                );
            }
            sockets
        });

        b.iter(|| {
            let total_requests = self.total_requests;
            let payload_len = self.payload_len;
            ex.block_on(async {
                udp_request_response_worker(
                    &client_sockets,
                    server_addr,
                    payload_len,
                    total_requests,
                )
                .await
                .unwrap();
            });
        });

        ex.block_on(async {
            for socket in client_sockets.drain(..) {
                socket.close().await.unwrap();
            }
        });
    }
}

async fn file_write_read_worker(
    file: &fs::File,
    worker_index: usize,
    block_size: usize,
    rounds: usize,
    slots_per_worker: usize,
) -> io::Result<()> {
    let start_slot = worker_index * slots_per_worker;
    let mut write_buf = vec![worker_index as u8; block_size];
    let mut read_buf = vec![0u8; block_size];

    for round in 0..rounds {
        let slot = start_slot + (round % slots_per_worker);
        let offset = (slot * block_size) as u64;

        write_buf[0] = (round as u8).wrapping_add(worker_index as u8);
        let (write_res, write) = file.write_at(write_buf, offset).await;
        write_buf = write;
        let written = write_res?;
        assert_eq!(written, block_size);

        let (read_res, read) = file.read_at(read_buf, offset).await;
        read_buf = read;
        let read_n = read_res?;
        assert_eq!(read_n, block_size);
    }

    Ok(())
}

struct FileWriteReadBench {
    workers: usize,
    rounds_per_worker: usize,
    block_size: usize,
    slots_per_worker: usize,
}

impl FileWriteReadBench {
    fn new(
        workers: usize,
        rounds_per_worker: usize,
        block_size: usize,
        slots_per_worker: usize,
    ) -> Self {
        Self {
            workers,
            rounds_per_worker,
            block_size,
            slots_per_worker,
        }
    }
}

impl bencher::TDynBenchFn for FileWriteReadBench {
    fn run(&self, b: &mut Bencher) {
        let dir = BenchDir::new("uring-file-rw");
        let path = dir.join("bench.dat");
        let file_len = (self.workers * self.slots_per_worker * self.block_size) as u64;
        {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)
                .unwrap();
            file.set_len(file_len).unwrap();
        }

        let mut ex = new_executor();
        let mut files = ex.block_on(async {
            let mut out = Vec::with_capacity(self.workers);
            for _ in 0..self.workers {
                let mut opts = fs::OpenOptions::new();
                opts.read(true).write(true);
                out.push(opts.open(&path).await.unwrap());
            }
            out
        });

        b.iter(|| {
            let rounds_per_worker = self.rounds_per_worker;
            let block_size = self.block_size;
            let slots_per_worker = self.slots_per_worker;
            ex.block_on(async {
                let mut workers = FuturesUnordered::new();
                for (worker_index, file) in files.iter().enumerate() {
                    workers.push(file_write_read_worker(
                        file,
                        worker_index,
                        block_size,
                        rounds_per_worker,
                        slots_per_worker,
                    ));
                }

                while let Some(result) = workers.next().await {
                    result.unwrap();
                }
            });
        });

        ex.block_on(async {
            for file in files.drain(..) {
                file.close().await.unwrap();
            }
        });
    }
}

fn benches() -> Vec<TestDescAndFn> {
    let mut benches = vec![];

    for total_requests in [4_096, 8_192] {
        for payload_len in [64, 1024] {
            for window in [1, 2, 4, 8, 16, 32, 64] {
                benches.push(TestDescAndFn {
                    desc: TestDesc {
                        name: Cow::from(format!(
                            "bench_udp_request_response/window={window}/total_requests={total_requests}/payload={payload_len}"
                        )),
                        ignore: false,
                    },
                    testfn: TestFn::DynBenchFn(Box::new(UdpRequestResponseBench::new(
                        total_requests,
                        payload_len,
                        window,
                    ))),
                });
            }
        }
    }

    for workers in [1, 8] {
        for total_round_trips in [2_048, 16_384] {
            for block_size in [4 * 1024, 16 * 1024] {
                let rounds_per_worker = cmp::max(total_round_trips / workers, 1);
                // Keep the active working set moderate but larger than the hot request window.
                let slots_per_worker = cmp::max(rounds_per_worker, 512);
                benches.push(TestDescAndFn {
                    desc: TestDesc {
                        name: Cow::from(format!(
                            "bench_file_write_read_roundtrip/workers={workers}/total_round_trips={total_round_trips}/block_size={block_size}"
                        )),
                        ignore: false,
                    },
                    testfn: TestFn::DynBenchFn(Box::new(FileWriteReadBench::new(
                        workers,
                        rounds_per_worker,
                        block_size,
                        slots_per_worker,
                    ))),
                });
            }
        }
    }

    benches
}

fn run_oneshot(filter: &str) -> io::Result<()> {
    let mut matching = benches()
        .into_iter()
        .filter(|bench| bench.desc.name.contains(filter))
        .collect::<Vec<_>>();

    if matching.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("no benchmark matched filter: {filter}"),
        ));
    }

    if matching.len() > 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "filter matched {} benchmarks; please provide a more specific filter",
                matching.len()
            ),
        ));
    }

    let benchmark = matching.remove(0);
    println!("oneshot benchmark: {}", benchmark.desc.name);
    let started = std::time::Instant::now();
    match benchmark.testfn {
        TestFn::DynBenchFn(benchfn) => bench::run_once(|harness| benchfn.run(harness)),
        TestFn::StaticBenchFn(benchfn) => bench::run_once(benchfn),
    }
    println!("oneshot elapsed_ms={}", started.elapsed().as_millis());
    Ok(())
}

fn main() {
    if let Ok(filter) = std::env::var("NORN_BENCH_ONESHOT") {
        run_oneshot(&filter).unwrap();
        return;
    }

    let mut test_opts = TestOpts::default();
    if let Some(arg) = std::env::args().skip(1).find(|arg| *arg != "--bench") {
        test_opts.filter = Some(arg);
    }
    run_tests_console(&test_opts, benches()).unwrap();
}
