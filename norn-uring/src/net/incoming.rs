//! Streams of accepted connections, shared by the listeners.
use std::io;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_core::Stream;

use crate::net::socket::{self, AcceptMulti};
use crate::operation::Op;

/// Accepted connections a listener's `incoming` stream holds before it pauses.
pub(crate) const INCOMING_LIMIT: NonZeroUsize = NonZeroUsize::new(64).unwrap();

pin_project_lite::pin_project! {
    /// A stream of connections accepted by a listener, each wrapped by `wrap`.
    pub(crate) struct Incoming<'a, T> {
        listener: &'a socket::Socket,
        limit: Option<NonZeroUsize>,
        wrap: fn(socket::Socket) -> T,
        #[pin]
        current: Option<Op<AcceptMulti>>,
        // Whether the current operation produced a connection.
        accepted: bool,
    }
}

impl<'a, T> Incoming<'a, T> {
    pub(crate) fn new(
        listener: &'a socket::Socket,
        limit: Option<NonZeroUsize>,
        wrap: fn(socket::Socket) -> T,
    ) -> Self {
        Self {
            listener,
            limit,
            wrap,
            current: None,
            accepted: false,
        }
    }
}

impl<T> Stream for Incoming<'_, T> {
    type Item = io::Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(current) = this.current.as_mut().as_pin_mut() {
                match ready!(current.poll_next(cx)) {
                    Some(Err(err)) => {
                        this.current.set(None);
                        return Poll::Ready(Some(Err(err)));
                    }

                    Some(Ok(socket)) => {
                        *this.accepted = true;
                        let socket = socket::Socket::from_fd(socket);
                        return Poll::Ready(Some(Ok((this.wrap)(socket))));
                    }
                    None => {
                        this.current.set(None);
                        // The kernel also ends a multishot accept without an
                        // error, for example when it cannot post a completion
                        // to a full completion queue, and a pause at the limit
                        // ends one quietly. Re-arm after an operation that
                        // accepted connections; one that ended without
                        // producing anything ends the stream.
                        if !std::mem::take(this.accepted) {
                            return Poll::Ready(None);
                        }
                    }
                }
            }
            // Once the driver stops accepting work, every new accept would fail
            // at once, and a consumer that continues past errors would spin.
            if !this.listener.driver_running() {
                return Poll::Ready(None);
            }
            this.current
                .set(Some(this.listener.accept_multi(*this.limit)));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}
