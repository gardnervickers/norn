#[cfg(target_os = "linux")]
mod linux {
    use std::cell::RefCell;
    use std::future::Future;
    use std::io;
    use std::mem::ManuallyDrop;
    use std::net::SocketAddr;
    use std::ops::{Deref, DerefMut};
    use std::pin::{pin, Pin};
    use std::rc::Rc;
    use std::task::{ready, Context, Poll};
    use std::time::Instant;

    use futures_util::StreamExt;
    use norn_executor::{spawn, LocalExecutor};
    use norn_timer::Clock;
    use norn_uring::net::{TcpListener, TcpSocket};
    use tonic::{Request, Response, Status};

    pub mod pingpong {
        tonic::include_proto!("pingpong.v1");
    }

    type BoxError = Box<dyn std::error::Error>;

    #[derive(Clone)]
    struct PingPongService {
        // The inner state is !Send/!Sync (Rc<RefCell<_>>). PanicSyncSend provides
        // runtime thread-affinity checks while satisfying transport trait bounds.
        state: PanicSyncSend<Rc<RefCell<State>>>,
        disk_io: bool,
    }

    #[derive(Default)]
    struct State {
        request_count: u64,
    }

    #[derive(Debug, Clone, Copy)]
    struct RunConfig {
        requests: usize,
        disk_io: bool,
        bench: bool,
    }

    impl Default for RunConfig {
        fn default() -> Self {
            Self {
                requests: 1,
                disk_io: true,
                bench: false,
            }
        }
    }

    fn print_usage() {
        eprintln!(
            "Usage: cargo run -p ping-pong-grpc -- [--bench] [--requests N] [--disk=on|off|true|false]"
        );
    }

    fn parse_bool_arg(raw: &str) -> io::Result<bool> {
        match raw {
            "1" | "on" | "true" | "yes" => Ok(true),
            "0" | "off" | "false" | "no" => Ok(false),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid bool value: {raw}"),
            )),
        }
    }

    fn parse_args() -> io::Result<RunConfig> {
        let mut config = RunConfig::default();
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--help" || arg == "-h" {
                print_usage();
                std::process::exit(0);
            } else if arg == "--bench" {
                config.bench = true;
            } else if arg == "--no-disk" {
                config.disk_io = false;
            } else if arg == "--requests" {
                let raw = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --requests")
                })?;
                config.requests = raw.parse::<usize>().map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid --requests value '{raw}': {err}"),
                    )
                })?;
            } else if let Some(raw) = arg.strip_prefix("--requests=") {
                config.requests = raw.parse::<usize>().map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid --requests value '{raw}': {err}"),
                    )
                })?;
            } else if arg == "--disk" {
                let raw = args.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "missing value for --disk")
                })?;
                config.disk_io = parse_bool_arg(&raw)?;
            } else if let Some(raw) = arg.strip_prefix("--disk=") {
                config.disk_io = parse_bool_arg(raw)?;
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument: {arg}"),
                ));
            }
        }

        if config.requests == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "requests must be greater than zero",
            ));
        }

        Ok(config)
    }

    impl PingPongService {
        fn new(disk_io: bool) -> Self {
            Self {
                state: PanicSyncSend::new(Rc::new(RefCell::new(State::default()))),
                disk_io,
            }
        }
    }

    #[tonic::async_trait]
    impl pingpong::ping_pong_server::PingPong for PingPongService {
        async fn ping(
            &self,
            request: Request<pingpong::PingRequest>,
        ) -> Result<Response<pingpong::PingReply>, Status> {
            let payload = request.into_inner().message;
            let request_count = {
                let mut state = self.state.borrow_mut();
                state.request_count += 1;
                state.request_count
            };

            let disk_echo = if self.disk_io {
                PanicSyncSend::new(ping_disk_roundtrip(request_count, &payload))
                    .await
                    .map_err(|err| Status::internal(format!("disk roundtrip failed: {err}")))?
            } else {
                payload.clone()
            };

            Ok(Response::new(pingpong::PingReply {
                message: format!("pong: {payload} (count={request_count}, disk_echo={disk_echo})"),
            }))
        }
    }

    async fn ping_disk_roundtrip(request_count: u64, payload: &str) -> io::Result<String> {
        let path = std::env::temp_dir().join(format!(
            "norn-ping-pong-{request_count}-{}.txt",
            std::process::id()
        ));
        let mut opts = norn_uring::fs::OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(true);
        let file = opts.open(&path).await?;

        let write_buf = payload.as_bytes().to_vec();
        let expected_len = write_buf.len();
        let (write_res, _) = file.write_at(write_buf, 0).await;
        let bytes_written = write_res?;
        if bytes_written != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!("short write: expected {expected_len}, got {bytes_written}"),
            ));
        }

        file.sync().await?;

        let (read_res, read_buf) = file.read_at(vec![0_u8; expected_len], 0).await;
        let bytes_read = read_res?;
        if bytes_read != expected_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("short read: expected {expected_len}, got {bytes_read}"),
            ));
        }

        file.close().await?;
        norn_uring::fs::remove_file(&path).await?;

        String::from_utf8(read_buf[..bytes_read].to_vec())
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
    }

    pub fn run() -> Result<(), BoxError> {
        let config = parse_args()?;
        let builder = io_uring::IoUring::builder();
        let driver = norn_uring::Driver::new(builder, 256)?;
        let driver = norn_timer::Driver::new(driver, Clock::system());
        let mut executor = LocalExecutor::new(driver);

        executor.block_on(async move {
            let listener = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
            let addr = listener.local_addr()?;

            let service = PingPongService::new(config.disk_io);
            let server = spawn(run_server(listener, service));

            let ping = "ping from norn".to_string();
            let start = Instant::now();
            let pong = run_client(addr, ping.clone(), config.requests).await?;
            let elapsed = start.elapsed();
            if config.bench || config.requests > 1 {
                let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
                let elapsed_ms = elapsed_secs * 1_000.0;
                let req_per_sec = config.requests as f64 / elapsed_secs;
                println!(
                    "benchmark mode={} requests={} elapsed_ms={elapsed_ms:.3} req_per_sec={req_per_sec:.3}",
                    if config.disk_io { "disk" } else { "memory" },
                    config.requests
                );
                println!("last_response: {pong}");
            } else {
                println!("request: {ping}");
                println!("response: {pong}");
            }

            match server.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(Box::new(err)),
            }

            Ok(())
        })
    }

    async fn run_server(listener: TcpListener, service: PingPongService) -> Result<(), BoxError> {
        let mut incoming = pin!(listener.incoming());
        let next = incoming
            .next()
            .await
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "no socket"))?;
        let socket = next?;

        let io = Box::pin(NornIo {
            inner: socket.into_stream(),
        });
        let svc = pingpong::ping_pong_server::PingPongServer::new(service);
        let svc = hyper_util::service::TowerToHyperService::new(svc);

        hyper::server::conn::http2::Builder::new(NornEx)
            .timer(NornTimer)
            .serve_connection(io, svc)
            .await?;

        Ok(())
    }

    async fn run_client(
        addr: SocketAddr,
        message: String,
        requests: usize,
    ) -> Result<String, BoxError> {
        let socket = TcpSocket::connect(addr).await?;
        let stream = Box::pin(NornIo {
            inner: socket.into_stream(),
        });
        let (sender, conn): (
            hyper::client::conn::http2::SendRequest<tonic::body::Body>,
            _,
        ) = hyper::client::conn::http2::handshake(NornEx, stream).await?;
        spawn(async move {
            if let Err(err) = conn.await {
                eprintln!("client connection error: {err}");
            }
        })
        .detach();

        let service = HyperGrpcService { sender };
        let origin: hyper::Uri = format!("http://{addr}").parse()?;
        let mut client = pingpong::ping_pong_client::PingPongClient::with_origin(service, origin);
        let mut last_response = String::new();
        for _ in 0..requests {
            let response = client
                .ping(Request::new(pingpong::PingRequest {
                    message: message.clone(),
                }))
                .await?
                .into_inner();
            last_response = response.message;
        }

        Ok(last_response)
    }

    struct HyperGrpcService {
        sender: hyper::client::conn::http2::SendRequest<tonic::body::Body>,
    }

    impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
        for HyperGrpcService
    {
        type Response = tonic::codegen::http::Response<hyper::body::Incoming>;
        type Error = hyper::Error;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + 'static>>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.sender.poll_ready(cx)
        }

        fn call(&mut self, req: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
            Box::pin(self.sender.send_request(req))
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct NornEx;

    impl<F> hyper::rt::Executor<F> for NornEx
    where
        F: Future<Output = ()> + 'static,
    {
        fn execute(&self, fut: F) {
            spawn(fut).detach();
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct NornTimer;

    impl hyper::rt::Timer for NornTimer {
        fn sleep(&self, duration: std::time::Duration) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
            let handle = norn_timer::Handle::current();
            let sleep = handle.sleep(duration);
            Box::pin(NornSleep {
                inner: PanicSyncSend::new(sleep),
            })
        }

        fn sleep_until(
            &self,
            deadline: std::time::Instant,
        ) -> std::pin::Pin<Box<dyn hyper::rt::Sleep>> {
            let handle = norn_timer::Handle::current();
            let now = handle.clock().now();
            let duration = deadline - now;
            self.sleep(duration)
        }
    }

    pin_project_lite::pin_project! {
        struct NornSleep {
            #[pin]
            inner: PanicSyncSend<norn_timer::Sleep>,
        }
    }

    impl Future for NornSleep {
        type Output = ();

        fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.project();
            ready!(this.inner.poll(cx)).unwrap();
            Poll::Ready(())
        }
    }

    impl hyper::rt::Sleep for NornSleep {}

    pin_project_lite::pin_project! {
        struct NornIo<T> {
            #[pin]
            inner: T,
        }
    }

    impl<T> hyper::rt::Read for NornIo<T>
    where
        T: tokio::io::AsyncRead,
    {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            mut buf: hyper::rt::ReadBufCursor<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            let n = unsafe {
                let mut tbuf = tokio::io::ReadBuf::uninit(buf.as_mut());
                ready!(tokio::io::AsyncRead::poll_read(
                    self.project().inner,
                    cx,
                    &mut tbuf
                ))?;
                tbuf.filled().len()
            };
            unsafe {
                buf.advance(n);
            }
            Poll::Ready(Ok(()))
        }
    }

    impl<T> hyper::rt::Write for NornIo<T>
    where
        T: tokio::io::AsyncWrite,
    {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, std::io::Error>> {
            tokio::io::AsyncWrite::poll_write(self.project().inner, cx, buf)
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            tokio::io::AsyncWrite::poll_flush(self.project().inner, cx)
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), std::io::Error>> {
            tokio::io::AsyncWrite::poll_shutdown(self.project().inner, cx)
        }

        fn is_write_vectored(&self) -> bool {
            tokio::io::AsyncWrite::is_write_vectored(&self.inner)
        }

        fn poll_write_vectored(
            self: std::pin::Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[std::io::IoSlice<'_>],
        ) -> Poll<Result<usize, std::io::Error>> {
            tokio::io::AsyncWrite::poll_write_vectored(self.project().inner, cx, bufs)
        }
    }

    pub struct PanicSyncSend<T> {
        value: Option<ManuallyDrop<T>>,
        tid: std::thread::ThreadId,
    }

    impl<T> PanicSyncSend<T> {
        pub fn new(value: T) -> PanicSyncSend<T> {
            PanicSyncSend {
                value: Some(ManuallyDrop::new(value)),
                tid: std::thread::current().id(),
            }
        }

        #[inline]
        fn is_accessible(&self) -> bool {
            self.tid == std::thread::current().id()
        }

        #[inline]
        fn assert_accessible(&self) {
            assert!(
                self.is_accessible(),
                "PanicSyncSend<T> accessed on different thread than it was created on"
            );
        }
    }

    impl<T: Clone> Clone for PanicSyncSend<T> {
        fn clone(&self) -> Self {
            self.assert_accessible();
            Self::new((**self).clone())
        }
    }

    unsafe impl<T> Send for PanicSyncSend<T> {}
    unsafe impl<T> Sync for PanicSyncSend<T> {}

    impl<T> AsRef<T> for PanicSyncSend<T> {
        fn as_ref(&self) -> &T {
            self.assert_accessible();
            self.value.as_ref().unwrap()
        }
    }

    impl<T> AsMut<T> for PanicSyncSend<T> {
        fn as_mut(&mut self) -> &mut T {
            self.assert_accessible();
            self.value.as_mut().unwrap()
        }
    }

    impl<T> Drop for PanicSyncSend<T> {
        fn drop(&mut self) {
            self.assert_accessible();
            if let Some(mut v) = self.value.take() {
                unsafe { ManuallyDrop::drop(&mut v) }
            }
        }
    }

    impl<T> Deref for PanicSyncSend<T> {
        type Target = T;

        fn deref(&self) -> &Self::Target {
            self.assert_accessible();
            self.value.as_ref().unwrap()
        }
    }

    impl<T> DerefMut for PanicSyncSend<T> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.assert_accessible();
            self.value.as_mut().unwrap()
        }
    }

    impl<T> std::future::Future for PanicSyncSend<T>
    where
        T: Future,
    {
        type Output = T::Output;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            unsafe { self.map_unchecked_mut(Self::deref_mut).poll(cx) }
        }
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    linux::run()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("This example requires Linux (norn-uring is Linux-only).");
}
