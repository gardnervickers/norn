#![cfg(target_os = "linux")]
//! Reproducer: a socket that outlives its driver can leak its descriptor.
//!
//! The socket keeps the driver's `Handle`, so dropping it after the driver has
//! shut down queues an `IORING_OP_CLOSE` on the post-shutdown ring and returns
//! without waiting for its completion. Shutdown has already submitted
//! `IOSQE_IO_DRAIN` sentinels, so the kernel may defer that close until every
//! other request is freed. When the socket holds the last `Handle`, the ring
//! is closed immediately afterwards and ring teardown cancels the deferred
//! close: the descriptor stays open forever.
//!
//! The kernel's deferral depends on when cancelled requests are freed, so the
//! reproducer runs several runtimes concurrently to make it reliable.

use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::{TcpListener, TcpSocket};

fn socket_inode(socket: &TcpSocket) -> String {
    let local = socket.local_addr().unwrap();
    let peer = socket.peer_addr().unwrap();
    for entry in std::fs::read_dir("/proc/self/fd").unwrap().flatten() {
        // Other threads open and close descriptors while this scans, so any
        // entry may vanish or stop being a socket before it is inspected.
        let Ok(fd) = entry.file_name().to_string_lossy().parse::<i32>() else {
            continue;
        };
        // Safety: the borrowed descriptor is never closed here.
        let stream = std::mem::ManuallyDrop::new(unsafe {
            <std::net::TcpStream as std::os::fd::FromRawFd>::from_raw_fd(fd)
        });
        if stream.local_addr().ok() == Some(local) && stream.peer_addr().ok() == Some(peer) {
            if let Ok(link) = std::fs::read_link(entry.path()) {
                return link.display().to_string();
            }
        }
    }
    panic!("socket descriptor not found");
}

fn is_open(inode: &str) -> bool {
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .flatten()
        .filter_map(|e| std::fs::read_link(e.path()).ok())
        .any(|p| p.display().to_string() == inode)
}

/// Returns whether the client descriptor was still open well after the last
/// owner dropped it.
fn one_round() -> bool {
    let mut outlives_driver = None;
    let mut inode = String::new();
    // Stays empty if the round was skipped.
    {
        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 32).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(driver);
        executor.block_on(async {
            // Port churn can briefly exhaust ephemeral ports; skip that round.
            let Ok(listener) = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 4).await else {
                return;
            };
            let addr = listener.local_addr().unwrap();
            let connect = norn_executor::spawn(async move { TcpSocket::connect(addr).await });
            let (_server, _) = listener.accept().await.unwrap();
            let Ok(client) = connect.await.unwrap() else {
                return;
            };
            let client = Rc::new(client);
            inode = socket_inode(&client);

            // A multishot receive is still in flight when the runtime stops.
            let ring = RecvBufRing::builder(1)
                .buf_cnt(2)
                .buf_len(256)
                .build()
                .unwrap();
            let receiver = Rc::clone(&client);
            norn_executor::spawn(async move {
                let mut stream = Box::pin(receiver.recv_ring_multi(&ring));
                let _ = stream.next().await;
            })
            .detach();
            norn_uring::noop().await;

            // The application keeps the socket beyond the runtime.
            outlives_driver = Some(client);
        });
        // Dropping the executor shuts the driver down; the socket's Handle
        // keeps the ring itself alive.
        drop(executor);
    }
    // Last owner: queues a close on the shut-down ring, then drops the ring.
    drop(outlives_driver);

    if inode.is_empty() {
        return false;
    }
    let deadline = std::time::Instant::now() + Duration::from_millis(500);
    while is_open(&inode) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    is_open(&inode)
}

#[test]
fn socket_dropped_after_driver_shutdown_is_closed() {
    const THREADS: usize = 8;
    const ROUNDS: usize = 600;
    let leaked = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..THREADS)
        .map(|_| {
            let leaked = Arc::clone(&leaked);
            std::thread::spawn(move || {
                for _ in 0..ROUNDS {
                    if one_round() {
                        leaked.fetch_add(1, Ordering::Relaxed);
                    }
                    if leaked.load(Ordering::Relaxed) > 0 {
                        return;
                    }
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(
        leaked.load(Ordering::Relaxed),
        0,
        "a descriptor dropped after its driver shut down was never closed"
    );
}
