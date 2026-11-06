//! A peer that never reads its responses must not stall other connections.
//!
//! Every multishot connection on a worker shares one provided-buffer ring.
//! The hog pipelines tiny GETs for large fixed responses and never reads
//! them, so its connection task blocks in `send_all` and stops polling its
//! receive stream. The kernel keeps the multishot receive armed, and each
//! tiny segment takes a whole ring buffer that is returned only when the task
//! polls again. Once the hog holds the whole ring, every other connection on
//! the worker waits for a buffer return that never comes.

use std::future::Future;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::AsRawFd;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use norn_executor::LocalExecutor;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::{TcpListener, TcpSocket};

use crate::protocol::{HEADER_LEN, OP_GET, OP_NOOP, REQUEST_MAGIC, RESPONSE_MAGIC};

/// Ring buffers shared by the hog and the probes.
const RING_BUFFERS: u16 = 512;
/// Separate hog segments: several times the ring, so the ring alone cannot
/// absorb them.
const HOG_SEGMENTS: u32 = 4 * RING_BUFFERS as u32;
/// Fixed GET response size. A few of these fill the hog's socket buffers.
pub(crate) const HOG_RESPONSE_BYTES: usize = 64 * 1024;
const PROBES: u32 = 8;
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// Serve one hog and then [`PROBES`] well-behaved connections that share one
/// small receive ring, and assert that every probe is answered.
///
/// `serve` runs one accepted connection to completion. Its index is the
/// accept order, starting with the hog at zero.
pub(crate) fn hog_does_not_stall_other_connections<F, Fut>(serve: F)
where
    F: Fn(u64, TcpSocket, RecvBufRing) -> Fut + Send + 'static,
    Fut: Future<Output = io::Result<()>> + 'static,
{
    let (address_tx, address_rx) = mpsc::sync_channel::<SocketAddr>(1);
    let server = thread::spawn(move || -> io::Result<()> {
        // Room for every ring buffer, as the server sizes its CQ, and for
        // each connection's other completions.
        let mut builder = io_uring::IoUring::builder();
        builder.setup_cqsize(256 + u32::from(RING_BUFFERS) + 4 * (PROBES + 1));
        let driver = norn_uring::Driver::new(builder, 256)?;
        let mut executor = LocalExecutor::new(driver);
        executor.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 16).await?;
            address_tx.send(listener.local_addr()?).unwrap();
            let ring = crate::server::recv_ring(RING_BUFFERS)?;
            let mut tasks = Vec::new();
            for index in 0..=u64::from(PROBES) {
                let (socket, _) = listener.accept().await?;
                tasks.push(norn_executor::spawn(serve(index, socket, ring.clone())));
            }
            for task in tasks {
                // The hog's connection fails when it closes with unread
                // responses; only the probes' replies are under test.
                let _ = task.await.unwrap();
            }
            Ok(())
        })
    });
    let address = match address_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(address) => address,
        // The server thread ended before announcing its address.
        Err(mpsc::RecvTimeoutError::Disconnected) => match server.join() {
            Ok(Err(error)) => panic!("server failed to start: {error}"),
            Ok(Ok(())) => panic!("server exited without announcing its address"),
            Err(panic) => std::panic::resume_unwind(panic),
        },
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("server did not announce its address"),
    };

    let mut hog = TcpStream::connect(address).unwrap();
    hog.set_nodelay(true).unwrap();
    shrink_receive_buffer(&hog);
    hog.set_nonblocking(true).unwrap();
    let mut sent = 0;
    for sequence in 0..HOG_SEGMENTS {
        let key = format!("k{sequence}");
        let bytes = request(OP_GET, key.as_bytes(), sequence);
        match hog.write(&bytes) {
            Ok(written) => {
                // A partial write would split a request across segments.
                assert_eq!(written, bytes.len(), "hog request {sequence} was split");
                sent += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => panic!("hog send failed: {error}"),
        }
        // Let the server receive each request as its own segment.
        thread::sleep(Duration::from_micros(100));
    }
    assert!(
        sent > u32::from(RING_BUFFERS),
        "hog sent only {sent} requests"
    );
    thread::sleep(Duration::from_millis(500));

    let outcomes: Vec<_> = (0..PROBES).map(|opaque| probe(address, opaque)).collect();
    drop(hog);
    let stalled = outcomes.iter().filter(|outcome| outcome.is_err()).count();
    assert_eq!(
        stalled, 0,
        "a non-reading peer stalled {stalled} of {PROBES} connections: {outcomes:?}"
    );
    server.join().unwrap().unwrap();
}

fn probe(address: SocketAddr, opaque: u32) -> Result<Duration, String> {
    let mut stream = TcpStream::connect(address).map_err(|error| error.to_string())?;
    stream.set_nodelay(true).unwrap();
    stream.set_read_timeout(Some(PROBE_TIMEOUT)).unwrap();
    let started = Instant::now();
    stream
        .write_all(&request(OP_NOOP, &[], opaque))
        .map_err(|error| error.to_string())?;
    let mut header = [0_u8; HEADER_LEN];
    stream
        .read_exact(&mut header)
        .map_err(|error| match error.kind() {
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => "timed out".to_owned(),
            _ => error.to_string(),
        })?;
    assert_eq!(header[0], RESPONSE_MAGIC);
    assert_eq!(header[1], OP_NOOP);
    assert_eq!(&header[12..16], &opaque.to_be_bytes());
    Ok(started.elapsed())
}

fn request(opcode: u8, key: &[u8], opaque: u32) -> Vec<u8> {
    let mut bytes = vec![0_u8; HEADER_LEN];
    bytes[0] = REQUEST_MAGIC;
    bytes[1] = opcode;
    bytes[2..4].copy_from_slice(&(key.len() as u16).to_be_bytes());
    bytes[8..12].copy_from_slice(&(key.len() as u32).to_be_bytes());
    bytes[12..16].copy_from_slice(&opaque.to_be_bytes());
    bytes.extend_from_slice(key);
    bytes
}

/// Keep the hog's unread responses from hiding in a large receive buffer, so
/// the server's sends block soon.
fn shrink_receive_buffer(stream: &TcpStream) {
    let size: libc::c_int = 4096;
    // Safety: the descriptor is open and the option value is a valid c_int.
    let result = unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            std::ptr::addr_of!(size).cast(),
            std::mem::size_of_val(&size) as libc::socklen_t,
        )
    };
    assert_eq!(result, 0, "{}", io::Error::last_os_error());
}
