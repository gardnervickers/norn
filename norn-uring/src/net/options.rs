//! Per-operation options for socket receives and sends.

/// Per-operation options for a socket receive.
///
/// Set them on a receive from [`TcpSocket::recv`](crate::net::TcpSocket::recv),
/// [`UdpSocket::recv`](crate::net::UdpSocket::recv), or
/// [`UdpSocket::recv_from`](crate::net::UdpSocket::recv_from) with
/// [`WithOptions::options`](crate::WithOptions::options). The default options
/// receive the same way as not setting any.
///
/// ```no_run
/// # async fn f(socket: &norn_uring::net::TcpSocket) {
/// use norn_uring::{net::RecvOptions, WithOptions};
///
/// let header = RecvOptions::new().wait_all(true);
/// let (res, buf) = socket.recv(vec![0; 16]).options(header).await;
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct RecvOptions {
    flags: i32,
}

impl RecvOptions {
    /// Create receive options with all optional behavior disabled.
    pub const fn new() -> Self {
        Self { flags: 0 }
    }

    /// Configure whether the receive leaves the data queued.
    ///
    /// This sets `MSG_PEEK`. A later receive returns the same data.
    #[must_use]
    pub const fn peek(self, enabled: bool) -> Self {
        self.with_flag(libc::MSG_PEEK, enabled)
    }

    /// Configure whether the receive waits until the buffer is full.
    ///
    /// This sets `MSG_WAITALL`. On a stream socket the receive completes
    /// once the buffer is full, or earlier with fewer bytes at end of stream,
    /// on an error, or when interrupted. On a datagram socket it has no effect
    /// beyond receiving one datagram.
    #[must_use]
    pub const fn wait_all(self, enabled: bool) -> Self {
        self.with_flag(libc::MSG_WAITALL, enabled)
    }

    /// Configure whether a datagram receive reports the datagram's full length.
    ///
    /// This sets `MSG_TRUNC`. The returned byte count is the datagram's full
    /// length and may exceed the initialized length of the returned buffer;
    /// the excess is discarded. Stream receives, on TCP and Unix stream
    /// sockets, reject it with [`io::ErrorKind::InvalidInput`] and return the
    /// buffer unchanged, because on a stream it discards input without
    /// filling the buffer.
    ///
    /// [`io::ErrorKind::InvalidInput`]: std::io::ErrorKind::InvalidInput
    #[must_use]
    pub const fn trunc(self, enabled: bool) -> Self {
        self.with_flag(libc::MSG_TRUNC, enabled)
    }

    /// The `MSG_*` flags these options select.
    pub(crate) const fn flags(self) -> i32 {
        self.flags
    }

    const fn with_flag(mut self, flag: i32, enabled: bool) -> Self {
        self.flags = set_flag(self.flags, flag, enabled);
        self
    }
}

/// Per-operation options for a socket send.
///
/// Set them on a send from [`TcpSocket::send`](crate::net::TcpSocket::send),
/// [`UdpSocket::send`](crate::net::UdpSocket::send),
/// [`UdpSocket::send_to`](crate::net::UdpSocket::send_to), or the zerocopy
/// sends, with [`WithOptions::options`](crate::WithOptions::options). The
/// default options send the same way as not setting any.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct SendOptions {
    flags: i32,
}

impl SendOptions {
    /// Create send options with all optional behavior disabled.
    pub const fn new() -> Self {
        Self { flags: 0 }
    }

    /// Configure whether more data follows this send.
    ///
    /// This sets `MSG_MORE`. On TCP the kernel holds a partial segment back
    /// for the next send, as with `TCP_CORK`, until a send without the flag.
    /// On UDP the payload is appended to a pending datagram that a later
    /// send without the flag completes.
    #[must_use]
    pub const fn more(mut self, enabled: bool) -> Self {
        self.flags = set_flag(self.flags, libc::MSG_MORE, enabled);
        self
    }

    /// The `MSG_*` flags these options select.
    pub(crate) const fn flags(self) -> i32 {
        self.flags
    }
}

const fn set_flag(flags: i32, flag: i32, enabled: bool) -> i32 {
    if enabled {
        flags | flag
    } else {
        flags & !flag
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_options_select_no_flags() {
        assert_eq!(RecvOptions::new(), RecvOptions::default());
        assert_eq!(SendOptions::new(), SendOptions::default());
        assert_eq!(RecvOptions::new().flags(), 0);
        assert_eq!(SendOptions::new().flags(), 0);
    }

    #[test]
    fn options_set_and_clear_flags() {
        let recv = RecvOptions::new().peek(true).wait_all(true).trunc(true);
        assert_eq!(
            recv.flags(),
            libc::MSG_PEEK | libc::MSG_WAITALL | libc::MSG_TRUNC
        );
        assert_eq!(recv.peek(false).trunc(false).flags(), libc::MSG_WAITALL);

        let send = SendOptions::new().more(true);
        assert_eq!(send.flags(), libc::MSG_MORE);
        assert_eq!(send.more(false).flags(), 0);
    }
}
