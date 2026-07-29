#![cfg(target_os = "linux")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::io;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures::StreamExt;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::{TcpListener, TcpSocket};

const BUFFER_LEN: usize = 256;
const RING_BUFFERS: u16 = 256;
const WARMUP_REQUESTS: usize = 1_024;
const DEFAULT_REQUESTS: usize = 16_384;

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
    let builder = io_uring::IoUring::builder();
    let driver = norn_uring::Driver::new(builder, 256).unwrap();
    norn_executor::LocalExecutor::new(driver)
}

async fn receive_bundles(
    server: &TcpSocket,
    ring: &RecvBufRing,
    requests: usize,
    segments: usize,
) -> io::Result<()> {
    let payload_len = segments * BUFFER_LEN;
    let mut acknowledgements = vec![0xA5];
    let mut recv = pin!(server.recv_bundle_multi(ring));

    for _ in 0..requests {
        let mut received = 0;
        let mut received_buffers = 0;
        while received < payload_len {
            let bundle = recv
                .next()
                .await
                .ok_or_else(|| io::Error::other("bundle receive stream ended"))??;
            assert!(received + bundle.len() <= payload_len);
            assert!(bundle
                .iter()
                .flat_map(|chunk| chunk.iter())
                .all(|byte| *byte == 0x5A));
            received += bundle.len();
            received_buffers += bundle.buffer_count();
            drop(bundle);
        }
        assert_eq!(received_buffers, segments);

        let (result, returned) = server.send(acknowledgements).await;
        acknowledgements = returned;
        assert_eq!(result?, 1);
    }

    Ok(())
}

async fn send_requests(client: &TcpSocket, requests: usize, segments: usize) -> io::Result<()> {
    let payload_len = segments * BUFFER_LEN;
    let mut payload = vec![0x5A; payload_len];
    let mut acknowledgement = Vec::with_capacity(1);

    for _ in 0..requests {
        let (result, returned) = client.send(payload).await;
        payload = returned;
        assert_eq!(result?, payload_len);

        let (result, returned) = client.recv(acknowledgement).await;
        acknowledgement = returned;
        assert_eq!(result?, 1);
        assert_eq!(acknowledgement, [0xA5]);
    }

    Ok(())
}

async fn run_round(
    server: &TcpSocket,
    client: &TcpSocket,
    ring: &RecvBufRing,
    requests: usize,
    segments: usize,
) -> io::Result<()> {
    let (server_result, client_result) = futures::join!(
        receive_bundles(server, ring, requests, segments),
        send_requests(client, requests, segments),
    );
    server_result?;
    client_result
}

fn main() {
    let mut args = std::env::args().skip(1);
    let segments = args
        .next()
        .map(|value| value.parse().expect("segments must be an integer"))
        .unwrap_or(1);
    let requests = args
        .next()
        .map(|value| value.parse().expect("requests must be an integer"))
        .unwrap_or(DEFAULT_REQUESTS);
    assert!(matches!(segments, 1 | 2 | 4 | 16));

    let mut executor = new_executor();
    let (server, client, ring) = executor.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 128)
            .await
            .unwrap();
        let connect = TcpSocket::connect(listener.local_addr().unwrap());
        let (accepted, client) = futures::join!(listener.accept(), connect);
        let (server, _) = accepted.unwrap();
        let client = client.unwrap();
        server.set_nodelay(true).await.unwrap();
        client.set_nodelay(true).await.unwrap();
        let ring = RecvBufRing::builder(1)
            .buf_cnt(RING_BUFFERS)
            .buf_len(BUFFER_LEN)
            .build()
            .unwrap();
        (server, client, ring)
    });

    executor
        .block_on(run_round(
            &server,
            &client,
            &ring,
            WARMUP_REQUESTS,
            segments,
        ))
        .unwrap();

    let before_allocations = ALLOCATIONS.load(Ordering::Relaxed);
    let before_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed);
    let start = Instant::now();
    executor
        .block_on(run_round(&server, &client, &ring, requests, segments))
        .unwrap();
    let elapsed = start.elapsed();
    let allocations = ALLOCATIONS.load(Ordering::Relaxed) - before_allocations;
    let allocated_bytes = ALLOCATED_BYTES.load(Ordering::Relaxed) - before_bytes;

    println!(
        "segments={segments} requests={requests} payload={} elapsed_ns={} ns_per_request={:.2} requests_per_second={:.2} allocations={allocations} allocations_per_request={:.4} allocated_bytes={allocated_bytes} allocated_bytes_per_request={:.2}",
        segments * BUFFER_LEN,
        elapsed.as_nanos(),
        elapsed.as_nanos() as f64 / requests as f64,
        requests as f64 / elapsed.as_secs_f64(),
        allocations as f64 / requests as f64,
        allocated_bytes as f64 / requests as f64,
    );
}
