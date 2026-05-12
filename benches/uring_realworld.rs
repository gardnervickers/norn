#![cfg(target_os = "linux")]

use std::borrow::Cow;
use std::cmp;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::{pin, Pin};
use std::time::{SystemTime, UNIX_EPOCH};

use bencher::bench;
use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use futures::stream::{FuturesUnordered, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use norn_uring::buf::{BufCursor, StableBuf};
use norn_uring::bufring::{BufRing, BufRingBuf, BufRingBufBundle};
use norn_uring::fs;
use norn_uring::net::UdpSocket as NornUdpSocket;
use norn_uring::net::{TcpListener as NornTcpListener, TcpSocket as NornTcpSocket};

mod support;

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

async fn norn_udp_echo_server(
    socket: &NornUdpSocket,
    payload_len: usize,
    total_requests: usize,
) -> io::Result<()> {
    let mut buf = vec![0u8; payload_len];
    for _ in 0..total_requests {
        let (recv_res, recv) = socket.recv_from(buf).await;
        buf = recv;
        let (n, peer) = recv_res?;
        assert_eq!(n, payload_len);

        let (send_res, send) = socket.send_to(buf, peer).await;
        buf = send;
        let sent = send_res?;
        assert_eq!(sent, payload_len);
    }
    Ok(())
}

async fn norn_udp_request_response_worker(
    sockets: &[NornUdpSocket],
    recv_mode: UdpRecvMode,
    recv_rings: &[BufRing],
    server_addr: std::net::SocketAddr,
    payload_len: usize,
    total_requests: usize,
) -> io::Result<()> {
    if recv_mode == UdpRecvMode::Multi {
        assert_eq!(sockets.len(), recv_rings.len());
    }
    let lanes = cmp::max(1, sockets.len());
    let per_lane = total_requests / lanes;
    let extra = total_requests % lanes;
    let mut pending: FuturesUnordered<Pin<Box<dyn Future<Output = io::Result<()>> + '_>>> =
        FuturesUnordered::new();

    for (lane, socket) in sockets.iter().enumerate() {
        let lane_requests = per_lane + usize::from(lane < extra);
        if lane_requests > 0 {
            match recv_mode {
                UdpRecvMode::Single => {
                    pending.push(Box::pin(norn_udp_request_response_lane_single(
                        socket,
                        server_addr,
                        payload_len,
                        lane_requests,
                    )))
                }
                UdpRecvMode::Multi => pending.push(Box::pin(norn_udp_request_response_lane_multi(
                    socket,
                    &recv_rings[lane],
                    server_addr,
                    payload_len,
                    lane_requests,
                ))),
            }
        }
    }

    while let Some(result) = pending.next().await {
        result?;
    }

    Ok(())
}

async fn norn_udp_request_response_lane_single(
    socket: &NornUdpSocket,
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
        let (n, addr) = recv_res?;
        assert_eq!(n, payload_len);
        assert_eq!(addr, server_addr);
    }
    Ok(())
}

async fn norn_udp_request_response_lane_multi(
    socket: &NornUdpSocket,
    recv_ring: &BufRing,
    server_addr: std::net::SocketAddr,
    payload_len: usize,
    requests: usize,
) -> io::Result<()> {
    let mut send_buf = vec![0x5A; payload_len];
    let mut recv = pin!(socket.recv_from_ring_multi(recv_ring));
    for _ in 0..requests {
        let (send_res, send) = socket.send_to(send_buf, server_addr).await;
        send_buf = send;
        let sent = send_res?;
        assert_eq!(sent, payload_len);

        let (buf, addr) = recv.next().await.expect("multishot stream ended")?;
        assert_eq!(addr, server_addr);
        assert_eq!(buf.len(), payload_len);
    }
    Ok(())
}

async fn tokio_udp_echo_server(
    socket: &tokio::net::UdpSocket,
    payload_len: usize,
    total_requests: usize,
) -> io::Result<()> {
    let mut buf = vec![0u8; payload_len];
    for _ in 0..total_requests {
        let (n, peer) = socket.recv_from(&mut buf).await?;
        assert_eq!(n, payload_len);

        let sent = socket.send_to(&buf[..n], peer).await?;
        assert_eq!(sent, payload_len);
    }
    Ok(())
}

async fn tokio_udp_request_response_worker(
    sockets: &[tokio::net::UdpSocket],
    server_addr: std::net::SocketAddr,
    payload_len: usize,
    total_requests: usize,
) -> io::Result<()> {
    let lanes = cmp::max(1, sockets.len());
    let per_lane = total_requests / lanes;
    let extra = total_requests % lanes;
    let mut pending: FuturesUnordered<Pin<Box<dyn Future<Output = io::Result<()>> + '_>>> =
        FuturesUnordered::new();

    for (lane, socket) in sockets.iter().enumerate() {
        let lane_requests = per_lane + usize::from(lane < extra);
        if lane_requests > 0 {
            pending.push(Box::pin(tokio_udp_request_response_lane(
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

async fn tokio_udp_request_response_lane(
    socket: &tokio::net::UdpSocket,
    server_addr: std::net::SocketAddr,
    payload_len: usize,
    requests: usize,
) -> io::Result<()> {
    let send_buf = vec![0x5A; payload_len];
    let mut recv_buf = vec![0u8; payload_len];
    for _ in 0..requests {
        let sent = socket.send_to(&send_buf, server_addr).await?;
        assert_eq!(sent, payload_len);

        let (n, addr) = socket.recv_from(&mut recv_buf).await?;
        assert_eq!(n, payload_len);
        assert_eq!(addr, server_addr);
    }
    Ok(())
}

struct UdpRequestResponseBench {
    runtime: RuntimeKind,
    total_requests: usize,
    payload_len: usize,
    window: usize,
    recv_mode: UdpRecvMode,
}

impl UdpRequestResponseBench {
    fn new(
        runtime: RuntimeKind,
        total_requests: usize,
        payload_len: usize,
        window: usize,
        recv_mode: UdpRecvMode,
    ) -> Self {
        Self {
            runtime,
            total_requests,
            payload_len,
            window,
            recv_mode,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeKind {
    Norn,
    Tokio,
}

impl RuntimeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Norn => "norn",
            Self::Tokio => "tokio",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UdpRecvMode {
    Single,
    Multi,
}

impl UdpRecvMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Multi => "multi",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TcpRecvMode {
    Normal,
    BufRing,
    BufRingMulti,
    BufRingBundleMulti,
}

impl TcpRecvMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::BufRing => "bufring",
            Self::BufRingMulti => "bufring_multi",
            Self::BufRingBundleMulti => "bufring_bundle_multi",
        }
    }
}

impl bencher::TDynBenchFn for UdpRequestResponseBench {
    fn run(&self, b: &mut Bencher) {
        match self.runtime {
            RuntimeKind::Norn => self.run_norn(b),
            RuntimeKind::Tokio => self.run_tokio(b),
        }
    }
}

impl UdpRequestResponseBench {
    fn run_norn(&self, b: &mut Bencher) {
        let mut ex = new_executor();
        let lane_count = cmp::max(1, cmp::min(self.window, self.total_requests));
        let recv_mode = self.recv_mode;
        let (server_socket, server_addr, mut client_sockets, mut recv_rings) = ex.block_on(async {
            let server_socket = NornUdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let server_addr = server_socket.local_addr().unwrap();
            let mut sockets = Vec::with_capacity(lane_count);
            for _ in 0..lane_count {
                sockets.push(
                    NornUdpSocket::bind("127.0.0.1:0".parse().unwrap())
                        .await
                        .unwrap(),
                );
            }
            let mut rings = Vec::with_capacity(lane_count);
            if recv_mode == UdpRecvMode::Multi {
                let ring_buf_len = cmp::max(2048, self.payload_len * 2);
                for lane in 0..lane_count {
                    rings.push(
                        BufRing::builder((100 + lane) as u16)
                            .buf_cnt(32)
                            .buf_len(ring_buf_len)
                            .build()
                            .unwrap(),
                    );
                }
            }
            (server_socket, server_addr, sockets, rings)
        });

        b.iter(|| {
            let total_requests = self.total_requests;
            let payload_len = self.payload_len;
            ex.block_on(async {
                futures::try_join!(
                    norn_udp_echo_server(&server_socket, payload_len, total_requests),
                    norn_udp_request_response_worker(
                        &client_sockets,
                        recv_mode,
                        &recv_rings,
                        server_addr,
                        payload_len,
                        total_requests,
                    )
                )
                .unwrap();
            });
        });

        ex.block_on(async {
            for socket in client_sockets.drain(..) {
                socket.close().await.unwrap();
            }
            recv_rings.clear();
            server_socket.close().await.unwrap();
        });
    }

    fn run_tokio(&self, b: &mut Bencher) {
        assert_eq!(self.recv_mode, UdpRecvMode::Single);
        let lane_count = cmp::max(1, cmp::min(self.window, self.total_requests));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        let (server_socket, server_addr, client_sockets) = rt.block_on(async {
            let server_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let server_addr = server_socket.local_addr().unwrap();
            let mut sockets = Vec::with_capacity(lane_count);
            for _ in 0..lane_count {
                sockets.push(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
            }
            (server_socket, server_addr, sockets)
        });

        b.iter(|| {
            let total_requests = self.total_requests;
            let payload_len = self.payload_len;
            rt.block_on(async {
                futures::try_join!(
                    tokio_udp_echo_server(&server_socket, payload_len, total_requests),
                    tokio_udp_request_response_worker(
                        &client_sockets,
                        server_addr,
                        payload_len,
                        total_requests,
                    )
                )
                .unwrap();
            });
        });
    }
}

async fn tcp_read_exact<S>(mut stream: Pin<&mut S>, buf: &mut [u8]) -> io::Result<()>
where
    S: AsyncRead,
{
    let mut read = 0;
    while read < buf.len() {
        let n = stream.as_mut().read(&mut buf[read..]).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "tcp stream closed before full frame",
            ));
        }
        read += n;
    }
    Ok(())
}

async fn tcp_write_all<S>(mut stream: Pin<&mut S>, buf: &[u8]) -> io::Result<()>
where
    S: AsyncWrite,
{
    stream.as_mut().write_all(buf).await?;
    stream.as_mut().flush().await
}

async fn norn_tcp_send_all(socket: &NornTcpSocket, buf: Vec<u8>) -> io::Result<Vec<u8>> {
    let mut cursor: BufCursor<Vec<u8>> = buf.into_cursor();
    loop {
        let (send_res, next) = socket.send(cursor).await;
        cursor = next;
        let n = send_res?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "tcp send wrote zero bytes",
            ));
        }
        cursor.consume(n);
        if cursor.bytes_init() == 0 {
            return Ok(cursor.into_inner());
        }
    }
}

async fn norn_tcp_recv_exact_ring(
    socket: &NornTcpSocket,
    ring: &BufRing,
    expected_byte: u8,
    payload_len: usize,
) -> io::Result<()> {
    let mut read = 0;
    while read < payload_len {
        let (buf, _) = socket.recv_ring(ring).await?;
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "tcp stream closed before full bufring frame",
            ));
        }
        if read + buf.len() > payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tcp bufring receive crossed frame boundary",
            ));
        }
        assert!(buf.as_slice().iter().all(|byte| *byte == expected_byte));
        read += buf.len();
    }
    Ok(())
}

async fn norn_tcp_recv_exact_ring_multi<S>(
    mut recv: Pin<&mut S>,
    expected_byte: u8,
    payload_len: usize,
) -> io::Result<()>
where
    S: Stream<Item = io::Result<BufRingBuf>>,
{
    let mut read = 0;
    while read < payload_len {
        let buf = recv.as_mut().next().await.ok_or_else(|| {
            io::Error::new(io::ErrorKind::UnexpectedEof, "tcp multishot receive ended")
        })??;
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "tcp stream closed before full multishot bufring frame",
            ));
        }
        if read + buf.len() > payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tcp multishot bufring receive crossed frame boundary",
            ));
        }
        assert!(buf.as_slice().iter().all(|byte| *byte == expected_byte));
        read += buf.len();
    }
    Ok(())
}

async fn norn_tcp_recv_exact_bundle_multi<S>(
    mut recv: Pin<&mut S>,
    expected_byte: u8,
    payload_len: usize,
) -> io::Result<()>
where
    S: Stream<Item = io::Result<BufRingBufBundle>>,
{
    let mut read = 0;
    while read < payload_len {
        let bundle = recv.as_mut().next().await.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "tcp multishot bundle receive ended",
            )
        })??;
        if bundle.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "tcp stream closed before full multishot bundle frame",
            ));
        }
        if read + bundle.len() > payload_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tcp multishot bundle receive crossed frame boundary",
            ));
        }
        for chunk in bundle.iter() {
            assert!(chunk.iter().all(|byte| *byte == expected_byte));
        }
        read += bundle.len();
    }
    Ok(())
}

async fn norn_tcp_echo_server(
    listener: &NornTcpListener,
    connections: usize,
    requests_per_connection: usize,
    payload_len: usize,
    recv_mode: TcpRecvMode,
) -> io::Result<()> {
    let mut handlers: FuturesUnordered<Pin<Box<dyn Future<Output = io::Result<()>> + '_>>> =
        FuturesUnordered::new();

    for connection in 0..connections {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true).await?;
        match recv_mode {
            TcpRecvMode::Normal => {
                handlers.push(Box::pin(norn_tcp_echo_connection_normal(
                    socket,
                    requests_per_connection,
                    payload_len,
                )));
            }
            TcpRecvMode::BufRing | TcpRecvMode::BufRingMulti | TcpRecvMode::BufRingBundleMulti => {
                let ring = tcp_bufring(2_000, connection, payload_len)?;
                match recv_mode {
                    TcpRecvMode::BufRing => {
                        handlers.push(Box::pin(norn_tcp_echo_connection_bufring(
                            socket,
                            ring,
                            requests_per_connection,
                            payload_len,
                        )));
                    }
                    TcpRecvMode::BufRingMulti => {
                        handlers.push(Box::pin(norn_tcp_echo_connection_bufring_multi(
                            socket,
                            ring,
                            requests_per_connection,
                            payload_len,
                        )));
                    }
                    TcpRecvMode::BufRingBundleMulti => {
                        handlers.push(Box::pin(norn_tcp_echo_connection_bufring_bundle_multi(
                            socket,
                            ring,
                            requests_per_connection,
                            payload_len,
                        )));
                    }
                    TcpRecvMode::Normal => unreachable!(),
                }
            }
        }
    }

    while let Some(result) = handlers.next().await {
        result?;
    }
    Ok(())
}

async fn norn_tcp_echo_connection_normal(
    socket: NornTcpSocket,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut stream = pin!(socket.into_stream());
    let mut buf = vec![0; payload_len];
    for _ in 0..requests {
        tcp_read_exact(stream.as_mut(), &mut buf).await?;
        assert!(buf.iter().all(|byte| *byte == 0x5A));
        tcp_write_all(stream.as_mut(), &buf).await?;
    }
    Ok(())
}

async fn norn_tcp_echo_connection_bufring(
    socket: NornTcpSocket,
    ring: BufRing,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut send_buf = vec![0x5A; payload_len];
    for _ in 0..requests {
        norn_tcp_recv_exact_ring(&socket, &ring, 0x5A, payload_len).await?;
        send_buf = norn_tcp_send_all(&socket, send_buf).await?;
    }
    socket.close().await
}

async fn norn_tcp_echo_connection_bufring_multi(
    socket: NornTcpSocket,
    ring: BufRing,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut send_buf = vec![0x5A; payload_len];
    // Drop the multishot op before closing; it owns an fd reference.
    {
        let mut recv = pin!(socket.recv_ring_multi(&ring));
        for _ in 0..requests {
            norn_tcp_recv_exact_ring_multi(recv.as_mut(), 0x5A, payload_len).await?;
            send_buf = norn_tcp_send_all(&socket, send_buf).await?;
        }
    }
    socket.close().await
}

async fn norn_tcp_echo_connection_bufring_bundle_multi(
    socket: NornTcpSocket,
    ring: BufRing,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut send_buf = vec![0x5A; payload_len];
    // Drop the multishot op before closing; it owns an fd reference.
    {
        let mut recv = pin!(socket.recv_bundle_multi(&ring));
        for _ in 0..requests {
            norn_tcp_recv_exact_bundle_multi(recv.as_mut(), 0x5A, payload_len).await?;
            send_buf = norn_tcp_send_all(&socket, send_buf).await?;
        }
    }
    socket.close().await
}

async fn norn_tcp_request_response_clients(
    server_addr: std::net::SocketAddr,
    connections: usize,
    requests_per_connection: usize,
    payload_len: usize,
    recv_mode: TcpRecvMode,
) -> io::Result<()> {
    let mut clients: FuturesUnordered<Pin<Box<dyn Future<Output = io::Result<()>> + '_>>> =
        FuturesUnordered::new();

    for connection in 0..connections {
        match recv_mode {
            TcpRecvMode::Normal => {
                clients.push(Box::pin(norn_tcp_request_response_client_normal(
                    server_addr,
                    requests_per_connection,
                    payload_len,
                )));
            }
            TcpRecvMode::BufRing | TcpRecvMode::BufRingMulti | TcpRecvMode::BufRingBundleMulti => {
                clients.push(Box::pin(norn_tcp_request_response_client_bufring(
                    server_addr,
                    connection,
                    requests_per_connection,
                    payload_len,
                    recv_mode,
                )));
            }
        }
    }

    while let Some(result) = clients.next().await {
        result?;
    }
    Ok(())
}

async fn norn_tcp_request_response_client_normal(
    server_addr: std::net::SocketAddr,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let socket = NornTcpSocket::connect(server_addr).await?;
    socket.set_nodelay(true).await?;
    let mut stream = pin!(socket.into_stream());
    let send_buf = vec![0x5A; payload_len];
    let mut recv_buf = vec![0; payload_len];
    for _ in 0..requests {
        tcp_write_all(stream.as_mut(), &send_buf).await?;
        tcp_read_exact(stream.as_mut(), &mut recv_buf).await?;
        assert!(recv_buf.iter().all(|byte| *byte == 0x5A));
    }
    Ok(())
}

async fn norn_tcp_request_response_client_bufring(
    server_addr: std::net::SocketAddr,
    connection: usize,
    requests: usize,
    payload_len: usize,
    recv_mode: TcpRecvMode,
) -> io::Result<()> {
    let socket = NornTcpSocket::connect(server_addr).await?;
    socket.set_nodelay(true).await?;
    let ring = tcp_bufring(3_000, connection, payload_len)?;
    let mut send_buf = vec![0x5A; payload_len];
    match recv_mode {
        TcpRecvMode::BufRing => {
            for _ in 0..requests {
                send_buf = norn_tcp_send_all(&socket, send_buf).await?;
                norn_tcp_recv_exact_ring(&socket, &ring, 0x5A, payload_len).await?;
            }
        }
        TcpRecvMode::BufRingMulti => {
            // Drop the multishot op before closing; it owns an fd reference.
            {
                let mut recv = pin!(socket.recv_ring_multi(&ring));
                for _ in 0..requests {
                    send_buf = norn_tcp_send_all(&socket, send_buf).await?;
                    norn_tcp_recv_exact_ring_multi(recv.as_mut(), 0x5A, payload_len).await?;
                }
            }
        }
        TcpRecvMode::BufRingBundleMulti => {
            // Drop the multishot op before closing; it owns an fd reference.
            {
                let mut recv = pin!(socket.recv_bundle_multi(&ring));
                for _ in 0..requests {
                    send_buf = norn_tcp_send_all(&socket, send_buf).await?;
                    norn_tcp_recv_exact_bundle_multi(recv.as_mut(), 0x5A, payload_len).await?;
                }
            }
        }
        TcpRecvMode::Normal => unreachable!(),
    }
    socket.close().await
}

async fn tokio_tcp_echo_server(
    listener: &tokio::net::TcpListener,
    connections: usize,
    requests_per_connection: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut handlers: FuturesUnordered<Pin<Box<dyn Future<Output = io::Result<()>> + '_>>> =
        FuturesUnordered::new();

    for _ in 0..connections {
        let (stream, _) = listener.accept().await?;
        stream.set_nodelay(true)?;
        handlers.push(Box::pin(tokio_tcp_echo_connection(
            stream,
            requests_per_connection,
            payload_len,
        )));
    }

    while let Some(result) = handlers.next().await {
        result?;
    }
    Ok(())
}

async fn tokio_tcp_echo_connection(
    stream: tokio::net::TcpStream,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut stream = pin!(stream);
    let mut buf = vec![0; payload_len];
    for _ in 0..requests {
        tcp_read_exact(stream.as_mut(), &mut buf).await?;
        assert!(buf.iter().all(|byte| *byte == 0x5A));
        tcp_write_all(stream.as_mut(), &buf).await?;
    }
    Ok(())
}

async fn tokio_tcp_request_response_clients(
    server_addr: std::net::SocketAddr,
    connections: usize,
    requests_per_connection: usize,
    payload_len: usize,
) -> io::Result<()> {
    let mut clients: FuturesUnordered<Pin<Box<dyn Future<Output = io::Result<()>> + '_>>> =
        FuturesUnordered::new();

    for _ in 0..connections {
        clients.push(Box::pin(tokio_tcp_request_response_client(
            server_addr,
            requests_per_connection,
            payload_len,
        )));
    }

    while let Some(result) = clients.next().await {
        result?;
    }
    Ok(())
}

async fn tokio_tcp_request_response_client(
    server_addr: std::net::SocketAddr,
    requests: usize,
    payload_len: usize,
) -> io::Result<()> {
    let stream = tokio::net::TcpStream::connect(server_addr).await?;
    stream.set_nodelay(true)?;
    let mut stream = pin!(stream);
    let send_buf = vec![0x5A; payload_len];
    let mut recv_buf = vec![0; payload_len];
    for _ in 0..requests {
        tcp_write_all(stream.as_mut(), &send_buf).await?;
        tcp_read_exact(stream.as_mut(), &mut recv_buf).await?;
        assert!(recv_buf.iter().all(|byte| *byte == 0x5A));
    }
    Ok(())
}

fn tcp_bufring(base: u16, connection: usize, payload_len: usize) -> io::Result<BufRing> {
    BufRing::builder(base + connection as u16)
        .buf_cnt(64)
        .buf_len(cmp::max(payload_len, 1))
        .build()
}

struct TcpRequestResponseBench {
    runtime: RuntimeKind,
    connections: usize,
    requests_per_connection: usize,
    payload_len: usize,
    recv_mode: TcpRecvMode,
}

impl TcpRequestResponseBench {
    fn new(
        runtime: RuntimeKind,
        connections: usize,
        requests_per_connection: usize,
        payload_len: usize,
        recv_mode: TcpRecvMode,
    ) -> Self {
        Self {
            runtime,
            connections,
            requests_per_connection,
            payload_len,
            recv_mode,
        }
    }
}

impl bencher::TDynBenchFn for TcpRequestResponseBench {
    fn run(&self, b: &mut Bencher) {
        match self.runtime {
            RuntimeKind::Norn => self.run_norn(b),
            RuntimeKind::Tokio => self.run_tokio(b),
        }
    }
}

impl TcpRequestResponseBench {
    fn run_norn(&self, b: &mut Bencher) {
        let mut ex = new_executor();
        let (listener, server_addr) = ex.block_on(async {
            let listener = NornTcpListener::bind("127.0.0.1:0".parse().unwrap(), 1024)
                .await
                .unwrap();
            let server_addr = listener.local_addr().unwrap();
            (listener, server_addr)
        });

        b.iter(|| {
            let connections = self.connections;
            let requests_per_connection = self.requests_per_connection;
            let payload_len = self.payload_len;
            let recv_mode = self.recv_mode;
            ex.block_on(async {
                futures::try_join!(
                    norn_tcp_echo_server(
                        &listener,
                        connections,
                        requests_per_connection,
                        payload_len,
                        recv_mode,
                    ),
                    norn_tcp_request_response_clients(
                        server_addr,
                        connections,
                        requests_per_connection,
                        payload_len,
                        recv_mode,
                    )
                )
                .unwrap();
            });
        });

        ex.block_on(async {
            listener.close().await.unwrap();
        });
    }

    fn run_tokio(&self, b: &mut Bencher) {
        assert_eq!(self.recv_mode, TcpRecvMode::Normal);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap();
        let (listener, server_addr) = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server_addr = listener.local_addr().unwrap();
            (listener, server_addr)
        });

        b.iter(|| {
            let connections = self.connections;
            let requests_per_connection = self.requests_per_connection;
            let payload_len = self.payload_len;
            rt.block_on(async {
                futures::try_join!(
                    tokio_tcp_echo_server(
                        &listener,
                        connections,
                        requests_per_connection,
                        payload_len,
                    ),
                    tokio_tcp_request_response_clients(
                        server_addr,
                        connections,
                        requests_per_connection,
                        payload_len,
                    )
                )
                .unwrap();
            });
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
                for runtime in [RuntimeKind::Norn, RuntimeKind::Tokio] {
                    let recv_modes: &[UdpRecvMode] = match runtime {
                        RuntimeKind::Norn => &[UdpRecvMode::Single, UdpRecvMode::Multi],
                        RuntimeKind::Tokio => &[UdpRecvMode::Single],
                    };
                    for recv_mode in recv_modes {
                        benches.push(TestDescAndFn {
                            desc: TestDesc {
                                name: Cow::from(format!(
                                    "bench_udp_request_response/runtime={}/recv={}/window={window}/total_requests={total_requests}/payload={payload_len}",
                                    runtime.as_str(),
                                    recv_mode.as_str()
                                )),
                                ignore: false,
                            },
                            testfn: TestFn::DynBenchFn(Box::new(UdpRequestResponseBench::new(
                                runtime,
                                total_requests,
                                payload_len,
                                window,
                                *recv_mode,
                            ))),
                        });
                    }
                }
            }
        }
    }

    for connections in [1, 8, 64] {
        for requests_per_connection in [64, 512] {
            for payload_len in [64, 1024] {
                for runtime in [RuntimeKind::Norn, RuntimeKind::Tokio] {
                    let recv_modes: &[TcpRecvMode] = match runtime {
                        RuntimeKind::Norn => &[
                            TcpRecvMode::Normal,
                            TcpRecvMode::BufRing,
                            TcpRecvMode::BufRingMulti,
                            TcpRecvMode::BufRingBundleMulti,
                        ],
                        RuntimeKind::Tokio => &[TcpRecvMode::Normal],
                    };
                    for recv_mode in recv_modes {
                        benches.push(TestDescAndFn {
                            desc: TestDesc {
                                name: Cow::from(format!(
                                    "bench_tcp_request_response/runtime={}/recv={}/connections={connections}/requests_per_connection={requests_per_connection}/payload={payload_len}",
                                    runtime.as_str(),
                                    recv_mode.as_str()
                                )),
                                ignore: false,
                            },
                            testfn: TestFn::DynBenchFn(Box::new(TcpRequestResponseBench::new(
                                runtime,
                                connections,
                                requests_per_connection,
                                payload_len,
                                *recv_mode,
                            ))),
                        });
                    }
                }
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

    support::run(benches());
}
