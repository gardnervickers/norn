#![cfg(target_os = "linux")]

use std::borrow::Cow;
use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::pin::{pin, Pin};
use std::task::{ready, Context, Poll};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use futures::{stream, StreamExt};
use hyper::client::conn::http2;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use norn_executor::{spawn, LocalExecutor};
use norn_timer::Clock;
use norn_uring::net::{TcpListener as NornTcpListener, TcpSocket as NornTcpSocket};
use tonic::{Request, Response, Status};

mod support;

mod proto {
    tonic::include_proto!("norn.bench.v1");
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type BenchClient = proto::runtime_bench_client::RuntimeBenchClient<HyperGrpcService>;

const RING_DEPTH: u32 = 256;
const WARMUP_REQUESTS: usize = 128;

#[derive(Debug, Clone, Copy)]
enum RuntimeKind {
    Norn,
    Tokio,
}

impl RuntimeKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Norn => "norn",
            Self::Tokio => "tokio-current-thread",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Workload {
    name: &'static str,
    requests: usize,
    in_flight: usize,
    payload_len: usize,
    yields_per_rpc: u32,
}

const WORKLOADS: [Workload; 2] = [
    Workload {
        name: "protocol-control",
        requests: 512,
        in_flight: 64,
        payload_len: 16,
        yields_per_rpc: 0,
    },
    Workload {
        name: "runtime-wakes",
        requests: 512,
        in_flight: 64,
        payload_len: 16,
        yields_per_rpc: 128,
    },
];

#[derive(Debug, Clone, Copy)]
struct BenchmarkService {
    yields_per_rpc: u32,
}

#[tonic::async_trait]
impl proto::runtime_bench_server::RuntimeBench for BenchmarkService {
    async fn round_trip(
        &self,
        request: Request<proto::BenchRequest>,
    ) -> Result<Response<proto::BenchReply>, Status> {
        let request = request.into_inner();
        let checksum = checksum(request.request_id, &request.payload);

        // This is deliberately the exact same future on both runtimes. Each
        // pending poll self-wakes the HTTP/2 stream task without adding more
        // protobuf or HTTP/2 work.
        YieldMany::new(self.yields_per_rpc).await;

        Ok(Response::new(proto::BenchReply {
            request_id: request.request_id,
            checksum,
        }))
    }
}

struct YieldMany {
    remaining: u32,
}

impl YieldMany {
    fn new(remaining: u32) -> Self {
        Self { remaining }
    }
}

impl Future for YieldMany {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.remaining == 0 {
            Poll::Ready(())
        } else {
            this.remaining -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

fn checksum(request_id: u64, payload: &[u8]) -> u64 {
    payload.iter().fold(request_id, |checksum, byte| {
        checksum.rotate_left(5) ^ u64::from(*byte)
    })
}

#[derive(Clone)]
struct HyperGrpcService {
    sender: hyper::client::conn::http2::SendRequest<tonic::body::Body>,
}

impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>>
    for HyperGrpcService
{
    type Response = tonic::codegen::http::Response<hyper::body::Incoming>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.sender.poll_ready(cx)
    }

    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        Box::pin(self.sender.send_request(request))
    }
}

async fn run_batch(
    client: &BenchClient,
    payload: &[u8],
    first_request_id: u64,
    requests: usize,
    in_flight: usize,
) -> Result<u64, BoxError> {
    let pending = stream::iter(0..requests)
        .map(|offset| {
            let mut client = client.clone();
            let payload = payload.to_vec();
            async move {
                let request_id = first_request_id + offset as u64;
                let expected_checksum = checksum(request_id, &payload);
                let reply = client
                    .round_trip(Request::new(proto::BenchRequest {
                        request_id,
                        payload,
                    }))
                    .await?
                    .into_inner();

                if reply.request_id != request_id || reply.checksum != expected_checksum {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "bad response for request {request_id}: id={}, checksum={}",
                            reply.request_id, reply.checksum
                        ),
                    )
                    .into());
                }

                Ok::<(), BoxError>(())
            }
        })
        .buffer_unordered(in_flight);
    futures::pin_mut!(pending);

    while let Some(result) = pending.next().await {
        result?;
    }

    first_request_id
        .checked_add(requests as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "request id overflow").into())
}

struct GrpcBench {
    runtime: RuntimeKind,
    workload: Workload,
}

impl GrpcBench {
    fn new(runtime: RuntimeKind, workload: Workload) -> Self {
        Self { runtime, workload }
    }

    fn run_norn(&self, b: &mut Bencher) {
        let mut executor = new_norn_executor();
        let fixture = executor
            .block_on(setup_norn(self.workload.yields_per_rpc))
            .unwrap();
        let payload = vec![0x5a; self.workload.payload_len];
        let mut next_request_id = 1;

        next_request_id = executor
            .block_on(run_batch(
                &fixture.client,
                &payload,
                next_request_id,
                WARMUP_REQUESTS,
                self.workload.in_flight,
            ))
            .unwrap();

        b.iter(|| {
            next_request_id = executor
                .block_on(run_batch(
                    &fixture.client,
                    &payload,
                    next_request_id,
                    self.workload.requests,
                    self.workload.in_flight,
                ))
                .unwrap();
        });

        executor.block_on(shutdown_norn(fixture));
    }

    fn run_tokio(&self, b: &mut Bencher) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let fixture = runtime
            .block_on(setup_tokio(self.workload.yields_per_rpc))
            .unwrap();
        let payload = vec![0x5a; self.workload.payload_len];
        let mut next_request_id = 1;

        next_request_id = runtime
            .block_on(run_batch(
                &fixture.client,
                &payload,
                next_request_id,
                WARMUP_REQUESTS,
                self.workload.in_flight,
            ))
            .unwrap();

        b.iter(|| {
            next_request_id = runtime
                .block_on(run_batch(
                    &fixture.client,
                    &payload,
                    next_request_id,
                    self.workload.requests,
                    self.workload.in_flight,
                ))
                .unwrap();
        });

        runtime.block_on(shutdown_tokio(fixture));
    }
}

impl bencher::TDynBenchFn for GrpcBench {
    fn run(&self, b: &mut Bencher) {
        match self.runtime {
            RuntimeKind::Norn => self.run_norn(b),
            RuntimeKind::Tokio => self.run_tokio(b),
        }
    }
}

fn new_norn_executor() -> LocalExecutor<norn_timer::Driver<norn_uring::Driver>> {
    let mut builder = io_uring::IoUring::builder();
    builder
        .dontfork()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .setup_submit_all();
    let driver = norn_uring::Driver::new(builder, RING_DEPTH).unwrap();
    let driver = norn_timer::Driver::new(driver, Clock::system());
    LocalExecutor::new(driver)
}

struct NornFixture {
    client: BenchClient,
    client_connection: norn_task::JoinHandle<Result<(), hyper::Error>>,
    server: norn_task::JoinHandle<Result<(), BoxError>>,
}

async fn setup_norn(yields_per_rpc: u32) -> Result<NornFixture, BoxError> {
    let listener = NornTcpListener::bind("127.0.0.1:0".parse()?, 128).await?;
    let address = listener.local_addr()?;
    let server = spawn(run_norn_server(listener, yields_per_rpc));

    let socket = NornTcpSocket::connect(address).await?;
    let io = Box::pin(NornIo {
        inner: socket.into_stream(),
    });
    let (sender, connection): (
        hyper::client::conn::http2::SendRequest<tonic::body::Body>,
        _,
    ) = http2::handshake(NornEx, io).await?;
    let client_connection = spawn(connection);
    let origin = format!("http://{address}").parse()?;
    let client = proto::runtime_bench_client::RuntimeBenchClient::with_origin(
        HyperGrpcService { sender },
        origin,
    );

    Ok(NornFixture {
        client,
        client_connection,
        server,
    })
}

async fn run_norn_server(listener: NornTcpListener, yields_per_rpc: u32) -> Result<(), BoxError> {
    let mut incoming = pin!(listener.incoming());
    let socket = incoming
        .next()
        .await
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "listener closed"))??;
    let io = Box::pin(NornIo {
        inner: socket.into_stream(),
    });
    let service =
        proto::runtime_bench_server::RuntimeBenchServer::new(BenchmarkService { yields_per_rpc });
    let service = hyper_util::service::TowerToHyperService::new(service);

    hyper::server::conn::http2::Builder::new(NornEx)
        .timer(NornTimer)
        .serve_connection(io, service)
        .await?;

    Ok(())
}

async fn shutdown_norn(fixture: NornFixture) {
    let NornFixture {
        client,
        client_connection,
        server,
    } = fixture;
    drop(client);
    client_connection.abort();
    server.abort();
    let _ = client_connection.await;
    let _ = server.await;
}

struct TokioFixture {
    client: BenchClient,
    client_connection: tokio::task::JoinHandle<Result<(), hyper::Error>>,
    server: tokio::task::JoinHandle<Result<(), BoxError>>,
}

async fn setup_tokio(yields_per_rpc: u32) -> Result<TokioFixture, BoxError> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(run_tokio_server(listener, yields_per_rpc));

    let stream = tokio::net::TcpStream::connect(address).await?;
    let (sender, connection): (
        hyper::client::conn::http2::SendRequest<tonic::body::Body>,
        _,
    ) = http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await?;
    let client_connection = tokio::spawn(connection);
    let origin = format!("http://{address}").parse()?;
    let client = proto::runtime_bench_client::RuntimeBenchClient::with_origin(
        HyperGrpcService { sender },
        origin,
    );

    Ok(TokioFixture {
        client,
        client_connection,
        server,
    })
}

async fn run_tokio_server(
    listener: tokio::net::TcpListener,
    yields_per_rpc: u32,
) -> Result<(), BoxError> {
    let (stream, _peer) = listener.accept().await?;
    let service =
        proto::runtime_bench_server::RuntimeBenchServer::new(BenchmarkService { yields_per_rpc });
    let service = hyper_util::service::TowerToHyperService::new(service);

    hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .timer(TokioTimer::new())
        .serve_connection(TokioIo::new(stream), service)
        .await?;

    Ok(())
}

async fn shutdown_tokio(fixture: TokioFixture) {
    let TokioFixture {
        client,
        client_connection,
        server,
    } = fixture;
    drop(client);
    client_connection.abort();
    server.abort();
    let _ = client_connection.await;
    let _ = server.await;
}

fn benches() -> Vec<TestDescAndFn> {
    let mut benches = Vec::new();
    for workload in WORKLOADS {
        for runtime in [RuntimeKind::Norn, RuntimeKind::Tokio] {
            benches.push(TestDescAndFn {
                desc: TestDesc {
                    name: Cow::from(format!(
                        "bench_grpc_runtime/runtime={}/workload={}/yields_per_rpc={}/in_flight={}/requests={}/payload={}",
                        runtime.as_str(),
                        workload.name,
                        workload.yields_per_rpc,
                        workload.in_flight,
                        workload.requests,
                        workload.payload_len,
                    )),
                    ignore: false,
                },
                testfn: TestFn::DynBenchFn(Box::new(GrpcBench::new(runtime, workload))),
            });
        }
    }
    benches
}

fn main() {
    if std::env::var_os("NORN_GRPC_BENCH_SMOKE").is_some() {
        smoke_test();
        return;
    }
    support::run(benches());
}

fn smoke_test() {
    const SMOKE_REQUESTS: usize = 8;
    const SMOKE_IN_FLIGHT: usize = 4;

    for workload in WORKLOADS {
        let payload = vec![0x5a; workload.payload_len];

        let mut norn = new_norn_executor();
        let fixture = norn.block_on(setup_norn(workload.yields_per_rpc)).unwrap();
        norn.block_on(run_batch(
            &fixture.client,
            &payload,
            1,
            SMOKE_REQUESTS,
            SMOKE_IN_FLIGHT,
        ))
        .unwrap();
        norn.block_on(shutdown_norn(fixture));

        let tokio = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .unwrap();
        let fixture = tokio
            .block_on(setup_tokio(workload.yields_per_rpc))
            .unwrap();
        tokio
            .block_on(run_batch(
                &fixture.client,
                &payload,
                1,
                SMOKE_REQUESTS,
                SMOKE_IN_FLIGHT,
            ))
            .unwrap();
        tokio.block_on(shutdown_tokio(fixture));

        println!("smoke workload={} ok", workload.name);
    }
}

#[derive(Debug, Clone, Copy)]
struct NornEx;

impl<F> hyper::rt::Executor<F> for NornEx
where
    F: Future<Output = ()> + 'static,
{
    fn execute(&self, future: F) {
        spawn(future).detach();
    }
}

#[derive(Debug, Clone, Copy)]
struct NornTimer;

impl hyper::rt::Timer for NornTimer {
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
        let sleep = norn_timer::Handle::current().sleep(duration);
        Box::pin(NornSleep {
            inner: PanicSyncSend::new(sleep),
        })
    }

    fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn hyper::rt::Sleep>> {
        let handle = norn_timer::Handle::current();
        let duration = deadline.saturating_duration_since(handle.clock().now());
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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut buffer: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        let read = unsafe {
            let mut tokio_buffer = tokio::io::ReadBuf::uninit(buffer.as_mut());
            ready!(tokio::io::AsyncRead::poll_read(
                self.project().inner,
                cx,
                &mut tokio_buffer,
            ))?;
            tokio_buffer.filled().len()
        };
        unsafe {
            buffer.advance(read);
        }
        Poll::Ready(Ok(()))
    }
}

impl<T> hyper::rt::Write for NornIo<T>
where
    T: tokio::io::AsyncWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        tokio::io::AsyncWrite::poll_write(self.project().inner, cx, buffer)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        tokio::io::AsyncWrite::poll_flush(self.project().inner, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        tokio::io::AsyncWrite::poll_shutdown(self.project().inner, cx)
    }

    fn is_write_vectored(&self) -> bool {
        tokio::io::AsyncWrite::is_write_vectored(&self.inner)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        tokio::io::AsyncWrite::poll_write_vectored(self.project().inner, cx, buffers)
    }
}

struct PanicSyncSend<T> {
    value: Option<ManuallyDrop<T>>,
    thread_id: std::thread::ThreadId,
}

impl<T> PanicSyncSend<T> {
    fn new(value: T) -> Self {
        Self {
            value: Some(ManuallyDrop::new(value)),
            thread_id: std::thread::current().id(),
        }
    }

    #[inline]
    fn assert_accessible(&self) {
        assert_eq!(
            self.thread_id,
            std::thread::current().id(),
            "PanicSyncSend<T> accessed from a different thread"
        );
    }
}

// Safety: every access and the drop path check that they run on the thread
// where the value was created.
unsafe impl<T> Send for PanicSyncSend<T> {}

// Safety: shared access also checks thread affinity before exposing the value.
unsafe impl<T> Sync for PanicSyncSend<T> {}

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

impl<T> Drop for PanicSyncSend<T> {
    fn drop(&mut self) {
        self.assert_accessible();
        if let Some(value) = self.value.as_mut() {
            unsafe { ManuallyDrop::drop(value) };
        }
    }
}

impl<T> Future for PanicSyncSend<T>
where
    T: Future,
{
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(Self::deref_mut).poll(cx) }
    }
}
