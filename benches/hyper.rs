use std::borrow::Cow;
use std::cmp;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::pin::{pin, Pin};

use std::task::{ready, Context, Poll};

use bencher::{run_tests_console, TestDesc, TestDescAndFn, TestFn, TestOpts};
use futures::StreamExt;
use http_body_util::BodyExt;
use hyper::body::{Bytes, Frame};
use hyper::client::conn::http2;
use hyper::service::service_fn;
use hyper::{Error, Request, Response, Uri};
use norn_executor::spawn;
use norn_timer::Clock;
use norn_uring::net::{TcpListener, TcpSocket};

struct HyperBench {
    clients: usize,
    requests: usize,
}
impl HyperBench {
    fn new(clients: usize, requests: usize) -> Self {
        Self { clients, requests }
    }
}
impl bencher::TDynBenchFn for HyperBench {
    fn run(&self, b: &mut bencher::Bencher) {
        let mut builder = io_uring::IoUring::builder();
        builder
            .dontfork()
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer()
            .setup_submit_all();
        let driver = norn_uring::Driver::new(builder, 32).unwrap();
        let driver = norn_timer::Driver::new(driver, Clock::system());
        let mut ex = norn_executor::LocalExecutor::new(driver);

        b.iter(|| {
            ex.block_on(async {
                let listener = TcpListener::bind("0.0.0.0:0".parse().unwrap(), 1024)
                    .await
                    .unwrap();
                let server_addr = listener.local_addr().unwrap();
                spawn(http2_server(listener)).detach();
                let mut handles = vec![];
                let uri: Uri = format!("http://127.0.0.1:{}", server_addr.port())
                    .parse()
                    .unwrap();
                for _ in 0..self.clients {
                    let handle = spawn(http2_client(uri.clone(), self.requests));
                    handles.push(handle);
                }
                for handle in handles {
                    handle.await.unwrap().unwrap();
                }
            })
            .unwrap()
        })
    }
}

pub fn benches() -> ::std::vec::Vec<TestDescAndFn> {
    let mut benches = vec![];
    for num_clients in [1] {
        for n in [128, 1024] {
            let per_client = cmp::max(n / num_clients, 1);
            benches.push(TestDescAndFn {
                desc: TestDesc {
                    name: Cow::from(format!(
                        "bench_hyper/num_clients={}/num_requests={}",
                        num_clients, n
                    )),
                    ignore: false,
                },
                testfn: TestFn::DynBenchFn(Box::new(HyperBench::new(num_clients, per_client))),
            })
        }
    }
    benches
}

fn main() {
    let mut test_opts = TestOpts::default();
    if let Some(arg) = ::std::env::args().skip(1).find(|arg| *arg != "--bench") {
        test_opts.filter = Some(arg);
    }
    let mut all = Vec::new();
    all.extend(benches());
    run_tests_console(&test_opts, all).unwrap();
}

async fn http2_client(url: hyper::Uri, n_req: usize) -> Result<(), Box<dyn std::error::Error>> {
    let host = url.host().expect("no host");
    let port = url.port_u16().unwrap_or(80);
    let addr = format!("{}:{}", host, port).parse::<SocketAddr>()?;
    let socket = TcpSocket::connect(addr).await?;
    let stream = socket.into_stream();
    let stream = Box::pin(NornIo { inner: stream });
    let (mut sender, conn) = http2::handshake(NornEx, stream).await?;
    spawn(async move {
        if let Err(err) = conn.await {
            eprintln!("client connection error: {}", err);
        }
    })
    .detach();
    let authority = url.authority().unwrap().clone();
    for _ in 0..n_req {
        let req = Request::builder()
            .uri(url.clone())
            .header(hyper::header::HOST, authority.as_str())
            .body(Body { inner: None })?;
        let mut res = sender.send_request(req).await?;

        while let Some(frame) = res.frame().await {
            let _ = frame?;
        }
    }

    Ok(())
}

struct Body {
    inner: Option<Bytes>,
}

impl hyper::body::Body for Body {
    type Data = Bytes;

    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let v = self.get_mut().inner.take().map(|d| Ok(Frame::data(d)));
        Poll::Ready(v)
    }
}

async fn http2_server(listener: TcpListener) -> Result<(), Box<dyn std::error::Error>> {
    let mut incoming = pin!(listener.incoming());
    while let Some(next) = incoming.next().await {
        let socket = next?;
        let io = NornIo {
            inner: socket.into_stream(),
        };
        let service = service_fn(move |_| async move {
            let body = Body {
                inner: Some(Bytes::from_static(b"hello world!")),
            };
            Ok::<_, Error>(Response::new(body))
        });
        spawn(async move {
            let io = Box::pin(io);
            if let Err(err) = hyper::server::conn::http2::Builder::new(NornEx)
                .timer(NornTimer)
                .serve_connection(io, service)
                .await
            {
                eprintln!("server connection error: {}", err);
            }
        })
        .detach();
    }

    Ok(())
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

/// [PanicSyncSend] makes a !Send type Send by performing a runtime check on access (and drop)
/// to ensure that the originating thread is the only thread accessing or dropping the value.
///
/// If a different thread attempts to use or drop the value, a panic will occur and the value will be leaked.
///
/// This adds some addition runtime overhead to check the thread id.
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

    /// Returns `true` if the calling thread is able to access the `T`.
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

    pub fn into_inner(mut self) -> T {
        let v = self.value.take().unwrap();
        ManuallyDrop::into_inner(v)
    }
}

/// Safety: Access and drop of the [PanicSyncSend<T>] type is only permitted on the thread which creates it. All other operations cause a panic.
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
            // Safety: We know that the drop is being run on the creating thread, so its safe to manually drop.
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
        // Safety: PanicSyncSend is pinned and points to the inner future, so transitively it is pinned too.
        unsafe { self.map_unchecked_mut(Self::deref_mut).poll(cx) }
    }
}
