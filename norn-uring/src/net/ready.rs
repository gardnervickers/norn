//! Readiness-driven nonblocking I/O shared by the stream sockets.
use std::io;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_core::Stream;

use crate::net::socket;
use crate::operation::Op;

const LOG: &str = "norn_uring::net::ready";

/// Linux `IOV_MAX`: the most `iovec`s one `sendmsg(2)` call accepts.
const IOV_MAX: usize = 1024;

pin_project_lite::pin_project! {
    /// Readiness-driven nonblocking I/O on one direction of a socket.
    ///
    /// The operation is attempted first. Only after it returns `EAGAIN` is a
    /// multishot poll armed. It then stays armed for the life of the half, so
    /// later waits submit nothing; the kernel posts a completion whenever the
    /// socket becomes ready.
    ///
    /// The kernel also posts those completions while the caller keeps up
    /// without waiting. Every completion queued on the poll was reaped before
    /// the current attempt ran, so after the attempt none of them says
    /// anything the attempt has not already observed: a success leaves the
    /// half notified and the next call attempts the operation directly, and
    /// an `EAGAIN` shows the readiness they reported has been consumed. Each
    /// attempt therefore discards them without a syscall, which bounds the
    /// queue by the completions reaped between two attempts. Readiness that
    /// arrives after an `EAGAIN` wakes the socket's wait queue again and posts
    /// a new completion, so discarding loses none.
    ///
    /// This relies on completions never being reaped while a task is being
    /// polled. Only the driver's park drains the CQ, between task polls, so
    /// nothing can be reaped between the attempt and the discard that
    /// follows it. If completions were ever reaped during a poll (a drain
    /// on the submit path, or a nested driver run from inside a task), a
    /// completion for readiness that arrived after the attempt's `EAGAIN`
    /// could be queued before the discard and dropped with the stale ones,
    /// losing the wakeup.
    ///
    /// The kernel can end a multishot poll without an error, for example when
    /// it cannot post a completion to a full CQ. The next wait then arms a
    /// fresh poll, which checks the current readiness mask when it is armed,
    /// so readiness that arrived in between is not lost.
    pub(crate) struct ReadyStream {
        inner: socket::Socket,
        #[pin]
        armed: Option<Op<socket::Poll<true>>>,
        notified: bool,
    }
}

impl ReadyStream {
    pub(crate) fn new(inner: socket::Socket) -> Self {
        Self {
            inner,
            armed: None,
            notified: true,
        }
    }

    fn poll_op<U>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut f: impl FnMut(ManuallyDrop<socket2::Socket>) -> io::Result<U>,
        flags: u32,
    ) -> Poll<io::Result<U>> {
        loop {
            log::trace!(target: LOG, "poll_op");
            ready!(self.as_mut().poll_ready(cx, flags))?;
            log::trace!(target: LOG, "poll_op.ready");
            let mut this = self.as_mut().project();
            let sock = this.inner.as_socket();
            let res = f(sock);
            discard_stale(this.armed.as_mut());
            match res {
                Ok(res) => {
                    log::trace!(target: LOG, "poll_op.success");
                    return Poll::Ready(Ok(res));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    log::trace!(target: LOG, "poll_op.would_block");
                    *this.notified = false;
                }
                Err(err) => {
                    log::trace!(target: LOG, "poll_op.err");
                    return Poll::Ready(Err(err));
                }
            }
        }
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>, flags: u32) -> Poll<io::Result<()>> {
        let mut this = self.project();
        loop {
            if *this.notified {
                // If the notified bit is set, return immediately.
                return Poll::Ready(Ok(()));
            }
            if let Some(armed) = this.armed.as_mut().as_pin_mut() {
                match ready!(armed.poll_next(cx)) {
                    Some(Ok(_)) => {
                        log::trace!(target: LOG, "poll_op.ready.stream_notified");
                        *this.notified = true;
                    }
                    Some(Err(err)) => {
                        // A failed poll has ended; the next wait arms a fresh
                        // one.
                        this.armed.set(None);
                        return Poll::Ready(Err(err));
                    }
                    // The kernel ended the poll without an error. Its terminal
                    // completion has been delivered, so it may be replaced.
                    None => {
                        log::trace!(target: LOG, "poll_op.ready.stream_done");
                        this.armed.set(None);
                    }
                }
            } else {
                log::trace!(target: LOG, "poll_op.ready.stream_arm");
                this.armed
                    .set(Some(this.inner.poll_readiness::<true>(flags)));
            }
        }
    }
}

/// Discard readiness events queued on `armed` without a syscall.
///
/// Called after an attempt, every queued event was reaped before the attempt
/// ran and is stale, because the driver never reaps completions while a task
/// is being polled; see [`ReadyStream`]. A poll that has ended, with or
/// without an error, is dropped so the next wait arms a fresh one.
#[inline]
fn discard_stale(armed: Pin<&mut Option<Op<socket::Poll<true>>>>) {
    if armed
        .as_ref()
        .get_ref()
        .as_ref()
        .is_some_and(Op::has_queued)
    {
        discard_stale_slow(armed);
    }
}

#[cold]
#[inline(never)]
fn discard_stale_slow(mut armed: Pin<&mut Option<Op<socket::Poll<true>>>>) {
    let Some(mut op) = armed.as_mut().as_pin_mut() else {
        return;
    };
    loop {
        match op.as_mut().poll_queued() {
            Poll::Pending => return,
            Poll::Ready(Some(Ok(_))) => log::trace!(target: LOG, "poll_op.stale"),
            Poll::Ready(Some(Err(_)) | None) => break,
        }
    }
    armed.set(None);
}

impl ReadyStream {
    /// Receive into the unfilled part of `buf`.
    pub(crate) fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let n = ready!(self.poll_op(
            cx,
            |sock| { unsafe { sock.recv(buf.unfilled_mut()) } },
            socket::READ_FLAGS as u32,
        ))?;
        unsafe {
            buf.assume_init(n);
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }

    /// Send from `buf`. A closed peer fails the send instead of raising
    /// `SIGPIPE`.
    pub(crate) fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_op(
            cx,
            |sock| sock.send_with_flags(buf, libc::MSG_NOSIGNAL),
            socket::WRITE_FLAGS as u32,
        )
    }

    /// Send from `bufs`, like [`poll_write`](Self::poll_write).
    pub(crate) fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        // The kernel rejects more than IOV_MAX iovecs with EMSGSIZE. Like
        // `std`, pass only the first IOV_MAX slices; a short write is valid.
        // Skip leading empty slices first, so non-empty input never caps down
        // to a prefix of empty slices and reports `Ok(0)`.
        let skip = bufs.iter().take_while(|buf| buf.is_empty()).count();
        let bufs = &bufs[skip..];
        let bufs = &bufs[..bufs.len().min(IOV_MAX)];
        self.poll_op(
            cx,
            |sock| sock.send_vectored_with_flags(bufs, libc::MSG_NOSIGNAL),
            socket::WRITE_FLAGS as u32,
        )
    }

    /// Shut down the write half.
    pub(crate) fn poll_shutdown_write(self: Pin<&mut Self>) -> Poll<io::Result<()>> {
        // `shutdown(SHUT_WR)` does not depend on send-buffer space, so it must
        // not wait on the write readiness gate, which stays closed after an
        // abandoned `EAGAIN` write until the peer drains the buffer.
        let sock = self.project().inner.as_socket();
        Poll::Ready(sock.shutdown(std::net::Shutdown::Write))
    }
}
