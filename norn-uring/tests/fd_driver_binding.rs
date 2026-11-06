#![cfg(target_os = "linux")]

//! Descriptors returned by opens and accepts belong to the driver that
//! submitted the operation, not to the context that finishes the future.

use norn_uring::BufResult;
use std::future::{poll_fn, Future};
use std::io::{Read, Write};
use std::pin::{pin, Pin};
use std::task::{Context, Poll, Waker};

use norn_executor::LocalExecutor;
use norn_uring::fs::File;
use norn_uring::net::TcpListener;

/// Polls `fut` once inside the executor so it submits its operation, then
/// finishes it outside any driver context while the driver reaps completions.
fn finish_outside_context<F: Future>(
    executor: &mut LocalExecutor<norn_uring::Driver>,
    fut: F,
) -> F::Output {
    let mut fut: Pin<Box<F>> = Box::pin(fut);
    executor.block_on(async {
        let first = poll_fn(|cx| Poll::Ready(fut.as_mut().poll(cx))).await;
        assert!(first.is_pending());
    });
    loop {
        let mut cx = Context::from_waker(Waker::noop());
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                // Only the driver turn matters, not `noop()`'s result.
                #[allow(clippy::let_unit_value)]
                let _turn = executor.block_on(norn_uring::noop());
            }
        }
    }
}

#[test]
fn open_finished_outside_driver_context_returns_usable_file() {
    let path = std::env::temp_dir().join(format!("norn-fd-binding-{}", std::process::id()));
    std::fs::write(&path, b"norn").unwrap();

    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 32).unwrap();
    let mut executor = LocalExecutor::new(driver);

    let file = finish_outside_context(&mut executor, File::open(path.clone())).unwrap();

    // Back inside the same driver's context, using the file must not panic.
    let BufResult(result, buf) =
        executor.block_on(async { file.read_at(Vec::with_capacity(4), 0).await });
    assert_eq!(result.unwrap(), 4);
    assert_eq!(buf, b"norn");
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn accept_finished_outside_driver_context_returns_usable_socket() {
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 32).unwrap();
    let mut executor = LocalExecutor::new(driver);

    let listener = executor
        .block_on(TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8))
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let client = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream.write_all(b"norn").unwrap();
        let mut reply = [0_u8; 2];
        stream.read_exact(&mut reply).unwrap();
        reply
    });

    let (socket, _) = finish_outside_context(&mut executor, listener.accept()).unwrap();

    // Back inside the driver's context, using the socket must not panic.
    executor.block_on(async {
        let BufResult(result, buf) = socket.recv(Vec::with_capacity(4)).await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buf, b"norn");
        let BufResult(result, _) = socket.send(b"ok".to_vec()).await;
        assert_eq!(result.unwrap(), 2);
        socket.close().await.unwrap();
    });
    assert_eq!(&client.join().unwrap(), b"ok");
}

#[test]
fn incoming_connection_finished_outside_driver_context_returns_usable_socket() {
    use futures_util::StreamExt;

    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 32).unwrap();
    let mut executor = LocalExecutor::new(driver);

    let listener = executor
        .block_on(TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8))
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let client = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        stream.write_all(b"norn").unwrap();
        let mut reply = [0_u8; 2];
        stream.read_exact(&mut reply).unwrap();
        reply
    });

    let mut incoming = pin!(listener.incoming());
    let socket = finish_outside_context(&mut executor, async {
        incoming.next().await.expect("incoming stream ended")
    })
    .unwrap();

    executor.block_on(async {
        let BufResult(result, buf) = socket.recv(Vec::with_capacity(4)).await;
        assert_eq!(result.unwrap(), 4);
        assert_eq!(buf, b"norn");
        let BufResult(result, _) = socket.send(b"ok".to_vec()).await;
        assert_eq!(result.unwrap(), 2);
        socket.close().await.unwrap();
    });
    assert_eq!(&client.join().unwrap(), b"ok");
}
