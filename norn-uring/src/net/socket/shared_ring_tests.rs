//! Multishot receives that pause at a ring's `max_unconsumed_per_receive`.
//!
//! These tests drive the driver by hand, so a stream is polled only when a
//! test says so. "Unpolled" below means a stream that holds received buffers
//! its consumer has not asked for.

use std::io::Write;
use std::net::{SocketAddr, TcpListener as StdListener, TcpStream as StdStream};
use std::os::fd::IntoRawFd;
use std::pin::Pin;
use std::task::{Context, Poll as TaskPoll};
use std::time::{Duration, Instant};

use futures_core::Stream;
use norn_executor::park::{Park, ParkMode};

use super::*;
use crate::fd::UringFd;

const LIMIT: usize = 8;
const SEGMENT: usize = 16;

/// The bytes a receive item carries.
trait Payload {
    fn append_to(&self, out: &mut Vec<u8>);
    /// An orderly end of stream.
    fn is_eof(&self) -> bool;
}

impl Payload for BufRingBuf {
    fn append_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self);
    }

    fn is_eof(&self) -> bool {
        self.capacity() == 0
    }
}

impl Payload for BufRingBufBundle {
    fn append_to(&self, out: &mut Vec<u8>) {
        for chunk in self.iter() {
            out.extend_from_slice(chunk);
        }
    }

    fn is_eof(&self) -> bool {
        self.buffer_count() == 0
    }
}

impl Payload for (RecvMsgRingBuf, SocketAddr) {
    fn append_to(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.0.payload());
    }

    fn is_eof(&self) -> bool {
        false
    }
}

fn driver() -> crate::Driver {
    crate::Driver::new(io_uring::IoUring::builder(), 64).unwrap()
}

fn ring(buf_cnt: u16, buf_len: usize, limit: Option<usize>) -> RecvBufRing {
    let builder = RecvBufRing::builder(3).buf_cnt(buf_cnt).buf_len(buf_len);
    match limit {
        Some(limit) => builder.max_unconsumed_per_receive(limit),
        None => builder,
    }
    .build()
    .unwrap()
}

fn tcp_pair(driver: &crate::Driver) -> (Socket, StdStream) {
    let listener = StdListener::bind("127.0.0.1:0").unwrap();
    let peer = StdStream::connect(listener.local_addr().unwrap()).unwrap();
    peer.set_nodelay(true).unwrap();
    let (accepted, _) = listener.accept().unwrap();
    accepted.set_nonblocking(true).unwrap();
    let socket =
        Socket::from_uring_fd(UringFd::from_fd_on(accepted.into_raw_fd(), driver.handle()));
    (socket, peer)
}

fn udp_pair(driver: &crate::Driver) -> (Socket, std::net::UdpSocket, SocketAddr) {
    let receiver = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    receiver.set_nonblocking(true).unwrap();
    let address = receiver.local_addr().unwrap();
    let sender = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let socket =
        Socket::from_uring_fd(UringFd::from_fd_on(receiver.into_raw_fd(), driver.handle()));
    (socket, sender, address)
}

fn settle(driver: &mut crate::Driver) {
    for _ in 0..8 {
        driver.park(ParkMode::NoPark).unwrap();
    }
}

/// Distinct contents for segment `index`, so reordering or loss is visible.
fn segment(index: usize, len: usize) -> Vec<u8> {
    (0..len).map(|offset| (index * 7 + offset) as u8).collect()
}

/// Send `count` segments, letting the driver reap each one separately so
/// each takes its own ring buffers.
fn send_segments(
    driver: &mut crate::Driver,
    peer: &mut StdStream,
    first: usize,
    count: usize,
    len: usize,
) -> Vec<u8> {
    let mut sent = Vec::new();
    for index in first..first + count {
        let bytes = segment(index, len);
        peer.write_all(&bytes).unwrap();
        sent.extend_from_slice(&bytes);
        settle(driver);
    }
    sent
}

fn new_stream<T: RingReceive>(
    driver: &mut crate::Driver,
    socket: &Socket,
    ring: &RecvBufRing,
    cx: &mut Context<'_>,
) -> Pin<Box<RingRecvStream<T>>> {
    let mut stream = Box::pin(RingRecvStream::<T>::new(socket, ring, 0));
    // The first poll submits the receive; nothing has arrived yet.
    assert!(stream.as_mut().poll_next(cx).is_pending());
    settle(driver);
    stream
}

enum End {
    /// `want` bytes arrived; the stream remains open.
    Bytes,
    Eof,
    Error(io::Error),
    Ended,
}

/// Poll `stream` until it has yielded `want` bytes or ended.
fn collect<T: RingReceive>(
    driver: &mut crate::Driver,
    stream: &mut Pin<Box<RingRecvStream<T>>>,
    cx: &mut Context<'_>,
    want: usize,
) -> (Vec<u8>, End)
where
    T::Buffer: Payload,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut bytes = Vec::new();
    loop {
        if bytes.len() >= want {
            return (bytes, End::Bytes);
        }
        match stream.as_mut().poll_next(cx) {
            TaskPoll::Ready(Some(Ok(item))) if item.is_eof() => return (bytes, End::Eof),
            TaskPoll::Ready(Some(Ok(item))) => item.append_to(&mut bytes),
            TaskPoll::Ready(Some(Err(error))) => return (bytes, End::Error(error)),
            TaskPoll::Ready(None) => return (bytes, End::Ended),
            TaskPoll::Pending => {
                assert!(
                    Instant::now() < deadline,
                    "receive stalled after {} of {want} bytes",
                    bytes.len()
                );
                driver.park(ParkMode::NoPark).unwrap();
            }
        }
    }
}

fn expect_bytes<T: RingReceive>(
    driver: &mut crate::Driver,
    stream: &mut Pin<Box<RingRecvStream<T>>>,
    cx: &mut Context<'_>,
    expected: &[u8],
) where
    T::Buffer: Payload,
{
    let (bytes, end) = collect(driver, stream, cx, expected.len());
    match end {
        End::Bytes => {}
        End::Eof => panic!("stream ended after {} bytes", bytes.len()),
        End::Error(error) => panic!("stream failed after {} bytes: {error}", bytes.len()),
        End::Ended => panic!("stream ended after {} bytes", bytes.len()),
    }
    assert_eq!(bytes, expected, "data was lost, duplicated or reordered");
}

/// Assert that the paused stream has settled: it holds at least `limit`
/// buffers, and further segments do not raise that count.
fn assert_paused(
    driver: &mut crate::Driver,
    ring: &RecvBufRing,
    limit: usize,
    sent: usize,
) -> usize {
    settle(driver);
    let held = ring.test_owned_buffers();
    assert!(held >= limit, "stream paused early, holding {held} buffers");
    assert!(
        held < sent,
        "an unpolled stream took a buffer for every one of {sent} segments"
    );
    held
}

/// An unpolled stream stops near its limit, another stream on the ring keeps
/// receiving, and the paused stream later yields every byte in order and
/// re-arms.
fn pause_and_resume<T: RingReceive>()
where
    T::Buffer: Payload,
{
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(64, 64, Some(LIMIT));
    let (waker, wakes) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);

    let (slow_socket, mut slow_peer) = tcp_pair(&driver);
    let mut slow = new_stream::<T>(&mut driver, &slow_socket, &ring, &mut cx);
    let sent = 4 * LIMIT;
    let mut expected = send_segments(&mut driver, &mut slow_peer, 0, sent, SEGMENT);
    let held = assert_paused(&mut driver, &ring, LIMIT, sent);
    eprintln!(
        "{}: limit {LIMIT}, held {held} (overshoot {})",
        std::any::type_name::<T>(),
        held - LIMIT
    );
    assert!(
        held <= LIMIT + 4,
        "overshoot of {} buffers past the limit",
        held - LIMIT
    );

    // A paused stream does not wake itself: back-pressure lifts only when the
    // consumer asks for data.
    let woken = wakes.get();
    expected.extend(send_segments(&mut driver, &mut slow_peer, sent, 4, SEGMENT));
    settle(&mut driver);
    assert_eq!(wakes.get(), woken, "a paused stream woke its task");
    assert_eq!(ring.test_owned_buffers(), held);

    // Another stream on the same ring receives promptly.
    let (fast_socket, mut fast_peer) = tcp_pair(&driver);
    let mut fast = new_stream::<T>(&mut driver, &fast_socket, &ring, &mut cx);
    for index in 0..LIMIT * 2 {
        let bytes = segment(1000 + index, SEGMENT);
        fast_peer.write_all(&bytes).unwrap();
        expect_bytes(&mut driver, &mut fast, &mut cx, &bytes);
    }

    // Resuming yields the backlog, then what the socket kept, in order.
    expect_bytes(&mut driver, &mut slow, &mut cx, &expected);
    // The receive re-armed and keeps receiving.
    for index in 0..LIMIT {
        let bytes = segment(2000 + index, SEGMENT);
        slow_peer.write_all(&bytes).unwrap();
        expect_bytes(&mut driver, &mut slow, &mut cx, &bytes);
    }
    drop((slow, fast));
    settle(&mut driver);
    assert_eq!(ring.test_owned_buffers(), 0);
}

#[test]
fn unpolled_stream_pauses_at_its_limit_and_resumes_in_order() {
    pause_and_resume::<RecvRingMulti>();
    pause_and_resume::<RecvRingBundleMulti>();
}

#[test]
fn unlimited_ring_keeps_filling_an_unpolled_stream() {
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(64, 64, None);
    let (waker, _) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, mut peer) = tcp_pair(&driver);
    let mut stream = new_stream::<RecvRingMulti>(&mut driver, &socket, &ring, &mut cx);
    let expected = send_segments(&mut driver, &mut peer, 0, 4 * LIMIT, SEGMENT);
    settle(&mut driver);
    assert_eq!(ring.test_owned_buffers(), 4 * LIMIT);
    expect_bytes(&mut driver, &mut stream, &mut cx, &expected);
}

#[test]
fn a_polled_stream_below_its_limit_does_not_pause() {
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(64, 64, Some(LIMIT));
    let (waker, _) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, mut peer) = tcp_pair(&driver);
    let mut stream = new_stream::<RecvRingMulti>(&mut driver, &socket, &ring, &mut cx);
    let op = stream.as_ref().get_ref().current.as_ref().unwrap() as *const Op<RecvRingMulti>;
    for index in 0..4 * LIMIT {
        let bytes = segment(index, SEGMENT);
        peer.write_all(&bytes).unwrap();
        expect_bytes(&mut driver, &mut stream, &mut cx, &bytes);
    }
    // The original receive is still armed.
    let current = stream.as_ref().get_ref().current.as_ref().unwrap() as *const Op<RecvRingMulti>;
    assert!(std::ptr::eq(op, current), "a keeping-pace stream re-armed");
}

#[test]
fn bundle_limit_counts_buffers() {
    let mut driver = driver();
    let _guard = driver.enter();
    // Each 40-byte segment fills three 16-byte buffers.
    let ring = ring(64, 16, Some(LIMIT));
    let (waker, _) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, mut peer) = tcp_pair(&driver);
    let mut stream = new_stream::<RecvRingBundleMulti>(&mut driver, &socket, &ring, &mut cx);
    let expected = send_segments(&mut driver, &mut peer, 0, 2 * LIMIT, 40);
    settle(&mut driver);
    let held = ring.test_owned_buffers();
    // Counting completions would allow LIMIT bundles, three buffers each.
    assert!(
        (LIMIT..LIMIT + 3 * 2).contains(&held),
        "bundle stream holds {held} buffers at a limit of {LIMIT}"
    );
    expect_bytes(&mut driver, &mut stream, &mut cx, &expected);
}

/// A UDP pause is lossless while the socket's receive buffer has room. Beyond
/// that the kernel drops datagrams, as for any slow receiver.
fn udp_pause<T: RingReceive>()
where
    T::Buffer: Payload,
{
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(64, 256, Some(LIMIT));
    let (waker, wakes) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, sender, address) = udp_pair(&driver);
    let mut stream = new_stream::<T>(&mut driver, &socket, &ring, &mut cx);
    let datagrams = 4 * LIMIT;
    let mut expected = Vec::new();
    for index in 0..datagrams {
        let bytes = segment(index, SEGMENT);
        sender.send_to(&bytes, address).unwrap();
        expected.extend_from_slice(&bytes);
        settle(&mut driver);
    }
    assert_paused(&mut driver, &ring, LIMIT, datagrams);
    let woken = wakes.get();
    settle(&mut driver);
    assert_eq!(wakes.get(), woken, "a paused stream woke its task");
    expect_bytes(&mut driver, &mut stream, &mut cx, &expected);
}

#[test]
fn udp_pause_keeps_queued_datagrams() {
    udp_pause::<RecvRingMulti>();
    udp_pause::<RecvFromRingMulti>();
}

/// Run a stream to its pause, then let `race` act on the peer, and check that
/// the stream yields a prefix of what was sent and then ends as `check`
/// expects.
fn race_pause<T: RingReceive>(
    buf_cnt: u16,
    race: impl FnOnce(StdStream) -> Vec<u8>,
    check: impl FnOnce(End),
) where
    T::Buffer: Payload,
{
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(buf_cnt, 64, Some(LIMIT));
    let (waker, _) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, mut peer) = tcp_pair(&driver);
    let mut stream = new_stream::<T>(&mut driver, &socket, &ring, &mut cx);
    // Stop one short of the limit, then send the segment that reaches it and
    // the racing terminal before the driver reaps either, so the pause and
    // the terminal can land in the same completion batch.
    let mut expected = send_segments(&mut driver, &mut peer, 0, LIMIT - 1, SEGMENT);
    let last = segment(LIMIT - 1, SEGMENT);
    peer.write_all(&last).unwrap();
    expected.extend_from_slice(&last);
    expected.extend(race(peer));
    settle(&mut driver);
    let (bytes, end) = collect(&mut driver, &mut stream, &mut cx, usize::MAX);
    assert_eq!(ring.test_pauses(), 1, "the receive did not pause");
    assert!(
        expected.starts_with(&bytes),
        "a racing terminal corrupted the stream"
    );
    eprintln!(
        "{}: {} of {} bytes, then {}",
        std::any::type_name::<T>(),
        bytes.len(),
        expected.len(),
        match &end {
            End::Bytes => "open".to_owned(),
            End::Eof => "EOF".to_owned(),
            End::Error(error) => error.to_string(),
            End::Ended => "end".to_owned(),
        }
    );
    check(end);
    drop(stream);
    settle(&mut driver);
    assert_eq!(ring.test_owned_buffers(), 0);
}

#[test]
fn eof_racing_a_pause_ends_the_stream_after_all_data() {
    fn check(end: End) {
        assert!(matches!(end, End::Eof | End::Ended), "EOF was not reported");
    }
    let race = |peer: StdStream| {
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        Vec::new()
    };
    race_pause::<RecvRingMulti>(64, race, check);
    race_pause::<RecvRingBundleMulti>(64, race, check);
}

#[test]
fn reset_racing_a_pause_is_a_terminal_error() {
    fn check(end: End) {
        match end {
            End::Error(error) => {
                assert_eq!(error.raw_os_error(), Some(libc::ECONNRESET), "{error}")
            }
            // Linux may report a reset with unread data as an orderly end.
            End::Eof | End::Ended => {}
            End::Bytes => unreachable!(),
        }
    }
    let race = |peer: StdStream| {
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        // Safety: the descriptor is open and `linger` is a valid option value.
        let result = unsafe {
            libc::setsockopt(
                std::os::fd::AsRawFd::as_raw_fd(&peer),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                std::ptr::addr_of!(linger).cast(),
                std::mem::size_of_val(&linger) as libc::socklen_t,
            )
        };
        assert_eq!(result, 0);
        // Closing with a zero linger sends RST.
        drop(peer);
        Vec::new()
    };
    race_pause::<RecvRingMulti>(64, race, check);
    race_pause::<RecvRingBundleMulti>(64, race, check);
}

#[test]
fn enobufs_racing_a_pause_waits_and_resumes_in_order() {
    // The ring holds one buffer more than the limit. Data arriving before the
    // cancellation takes the last buffer, and the kernel ends the receive
    // because the ring is empty.
    let race = |mut peer: StdStream| {
        let mut extra = Vec::new();
        for index in LIMIT..LIMIT + 4 {
            let bytes = segment(index, SEGMENT);
            peer.write_all(&bytes).unwrap();
            extra.extend_from_slice(&bytes);
        }
        peer.shutdown(std::net::Shutdown::Write).unwrap();
        extra
    };
    fn check(end: End) {
        assert!(
            matches!(end, End::Eof | End::Ended),
            "ENOBUFS or the pause was not absorbed"
        );
    }
    race_pause::<RecvRingMulti>(LIMIT as u16 + 1, race, check);
    race_pause::<RecvRingBundleMulti>(LIMIT as u16 + 1, race, check);
}

#[test]
fn eof_after_a_pause_is_delivered_after_the_backlog() {
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(64, 64, Some(LIMIT));
    let (waker, _) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, mut peer) = tcp_pair(&driver);
    let mut stream = new_stream::<RecvRingMulti>(&mut driver, &socket, &ring, &mut cx);
    let mut expected = send_segments(&mut driver, &mut peer, 0, 2 * LIMIT, SEGMENT);
    assert_paused(&mut driver, &ring, LIMIT, 2 * LIMIT);
    expected.extend(send_segments(&mut driver, &mut peer, 2 * LIMIT, 2, SEGMENT));
    peer.shutdown(std::net::Shutdown::Write).unwrap();
    settle(&mut driver);
    let (bytes, end) = collect(&mut driver, &mut stream, &mut cx, usize::MAX);
    assert_eq!(bytes, expected);
    assert!(
        matches!(end, End::Eof),
        "EOF was not delivered after resuming"
    );
}

#[test]
fn dropping_a_paused_stream_returns_its_buffers() {
    for settle_first in [false, true] {
        let mut driver = driver();
        let _guard = driver.enter();
        let ring = ring(64, 64, Some(LIMIT));
        let (waker, _) = futures_test::task::new_count_waker();
        let mut cx = Context::from_waker(&waker);
        let (socket, mut peer) = tcp_pair(&driver);
        let stream = new_stream::<RecvRingMulti>(&mut driver, &socket, &ring, &mut cx);
        let _ = send_segments(&mut driver, &mut peer, 0, LIMIT - 1, SEGMENT);
        peer.write_all(&segment(LIMIT, SEGMENT)).unwrap();
        // Drop either with the pause's cancellation still in flight or after
        // its terminal completion is queued.
        driver.park(ParkMode::NoPark).unwrap();
        if settle_first {
            settle(&mut driver);
        }
        assert_eq!(ring.test_pauses(), 1);
        drop(stream);
        settle(&mut driver);
        assert_eq!(ring.test_owned_buffers(), 0);

        // The ring still serves a new stream.
        let mut stream = new_stream::<RecvRingMulti>(&mut driver, &socket, &ring, &mut cx);
        let bytes = segment(5000, SEGMENT);
        peer.write_all(&bytes).unwrap();
        let (received, _) = collect(&mut driver, &mut stream, &mut cx, 1);
        assert!(!received.is_empty());
    }
}

#[test]
fn shutdown_while_paused_ends_the_stream_after_the_backlog() {
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(64, 64, Some(LIMIT));
    let (waker, _) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, mut peer) = tcp_pair(&driver);
    let mut stream = new_stream::<RecvRingMulti>(&mut driver, &socket, &ring, &mut cx);
    let expected = send_segments(&mut driver, &mut peer, 0, 2 * LIMIT, SEGMENT);
    let held = assert_paused(&mut driver, &ring, LIMIT, 2 * LIMIT);
    driver.shutdown();
    let mut bytes = Vec::new();
    loop {
        match stream.as_mut().poll_next(&mut cx) {
            TaskPoll::Ready(Some(Ok(item))) => item.append_to(&mut bytes),
            TaskPoll::Ready(Some(Err(error))) => panic!("shutdown failed the stream: {error}"),
            TaskPoll::Ready(None) => break,
            TaskPoll::Pending => panic!("a paused stream stalled across shutdown"),
        }
    }
    // The backlog is delivered; the receive is not re-armed.
    assert_eq!(bytes, expected[..held * SEGMENT]);
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        TaskPoll::Ready(None)
    ));
}

#[test]
fn quarantine_while_paused_fails_the_re_arm() {
    let mut driver = driver();
    let _guard = driver.enter();
    let ring = ring(64, 64, Some(LIMIT));
    let (waker, _) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    let (socket, mut peer) = tcp_pair(&driver);
    let mut stream = new_stream::<RecvRingMulti>(&mut driver, &socket, &ring, &mut cx);
    let expected = send_segments(&mut driver, &mut peer, 0, 2 * LIMIT, SEGMENT);
    let held = assert_paused(&mut driver, &ring, LIMIT, 2 * LIMIT);
    ring.test_quarantine();
    let (bytes, end) = collect(&mut driver, &mut stream, &mut cx, usize::MAX);
    assert_eq!(bytes, expected[..held * SEGMENT]);
    let End::Error(error) = end else {
        panic!("a quarantined ring re-armed a paused receive");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(matches!(
        stream.as_mut().poll_next(&mut cx),
        TaskPoll::Ready(None)
    ));
}

#[test]
fn zero_limit_is_rejected() {
    let driver = driver();
    let _guard = driver.enter();
    let error = RecvBufRing::builder(3)
        .buf_cnt(4)
        .max_unconsumed_per_receive(0)
        .build()
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let ring = ring(4, 64, Some(2));
    assert_eq!(ring.max_unconsumed_per_receive(), Some(2));
}
