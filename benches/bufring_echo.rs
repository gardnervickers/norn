#![cfg(target_os = "linux")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::UdpSocket;

const WARMUP_REQUESTS: usize = 1_024;
const DEFAULT_REQUESTS: usize = 32_768;
const DEFAULT_PAYLOAD_LEN: usize = 1_024;

#[derive(Clone, Copy)]
enum EchoMode {
    Copy,
    Direct,
}

impl EchoMode {
    fn parse(value: &str) -> Self {
        match value {
            "copy" => Self::Copy,
            "direct" => Self::Direct,
            _ => panic!("mode must be `copy` or `direct`"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Direct => "direct",
        }
    }
}

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        new_ptr
    }
}

fn new_executor() -> norn_executor::LocalExecutor<norn_uring::Driver> {
    let mut builder = io_uring::IoUring::builder();
    builder
        .dontfork()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .setup_submit_all();
    let driver = norn_uring::Driver::new(builder, 256).unwrap();
    norn_executor::LocalExecutor::new(driver)
}

async fn echo_copy(
    server: &UdpSocket,
    ring: &RecvBufRing,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    for _ in 0..requests {
        let (buf, peer) = server.recv_from_ring(ring).await?;
        assert_eq!(buf.len(), payload_len);
        let send_buf = Bytes::copy_from_slice(buf.as_slice());
        drop(buf);
        let (result, _) = server.send_to(send_buf, peer).await;
        assert_eq!(result?, payload_len);
    }
    Ok(())
}

async fn echo_direct(
    server: &UdpSocket,
    ring: &RecvBufRing,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    for _ in 0..requests {
        let (buf, peer) = server.recv_from_ring(ring).await?;
        assert_eq!(buf.len(), payload_len);
        let (result, buf) = server.send_to(buf, peer).await;
        assert_eq!(result?, payload_len);
        drop(buf);
    }
    Ok(())
}

async fn client_round_trip(
    client: &UdpSocket,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut send_buf = vec![0x5A; payload_len];
    let mut recv_buf = Vec::with_capacity(payload_len);
    for _ in 0..requests {
        let (result, buf) = client.send(send_buf).await;
        send_buf = buf;
        assert_eq!(result?, payload_len);

        let (result, buf) = client.recv(recv_buf).await;
        recv_buf = buf;
        assert_eq!(result?, payload_len);
        assert!(recv_buf.iter().all(|byte| *byte == 0x5A));
    }
    Ok(())
}

async fn run_round(
    mode: EchoMode,
    server: &UdpSocket,
    client: &UdpSocket,
    ring: &RecvBufRing,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let server = async {
        match mode {
            EchoMode::Copy => echo_copy(server, ring, requests, payload_len).await,
            EchoMode::Direct => echo_direct(server, ring, requests, payload_len).await,
        }
    };
    let (server_result, client_result) =
        futures::join!(server, client_round_trip(client, requests, payload_len),);
    server_result?;
    client_result
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = EchoMode::parse(args.next().as_deref().unwrap_or("copy"));
    let requests = args
        .next()
        .map(|value| value.parse().expect("requests must be an integer"))
        .unwrap_or(DEFAULT_REQUESTS);
    let payload_len = args
        .next()
        .map(|value| value.parse().expect("payload length must be an integer"))
        .unwrap_or(DEFAULT_PAYLOAD_LEN);

    let mut executor = new_executor();
    let (server, client, ring) = executor.block_on(async {
        let server = UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let client = UdpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        client.connect(server.local_addr().unwrap()).await.unwrap();
        let ring = RecvBufRing::builder(1)
            .buf_cnt(256)
            .buf_len(payload_len)
            .build()
            .unwrap();
        (server, client, ring)
    });

    executor
        .block_on(run_round(
            mode,
            &server,
            &client,
            &ring,
            WARMUP_REQUESTS,
            payload_len,
        ))
        .unwrap();

    let before_allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let before_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
    let start = Instant::now();
    executor
        .block_on(run_round(
            mode,
            &server,
            &client,
            &ring,
            requests,
            payload_len,
        ))
        .unwrap();
    let elapsed = start.elapsed();
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_allocations;
    let allocated_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed) - before_bytes;

    println!(
        "mode={} requests={requests} payload={payload_len} elapsed_ns={} ns_per_request={:.2} requests_per_second={:.2} allocations={allocations} allocations_per_request={:.4} allocated_bytes={allocated_bytes} allocated_bytes_per_request={:.2}",
        mode.as_str(),
        elapsed.as_nanos(),
        elapsed.as_nanos() as f64 / requests as f64,
        requests as f64 / elapsed.as_secs_f64(),
        allocations as f64 / requests as f64,
        allocated_bytes as f64 / requests as f64,
    );
}
