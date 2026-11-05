//! Networking for Norn.
//!
//! Unless otherwise documented, functions and methods in this module require
//! an active [`crate::Driver`] context.
mod incoming;
mod options;
mod ready;
pub(crate) mod socket;
mod tcp;
mod udp;
mod unix;

pub use options::{RecvOptions, SendOptions};
pub use socket::{Event, RecvMsgRingBuf, SendZcResult, SendZcUsage};
pub use tcp::{
    TcpListener, TcpListenerOptions, TcpRecvBundleMulti, TcpRecvRingMulti, TcpSocket, TcpStream,
    TcpStreamReader, TcpStreamWriter,
};
pub use udp::{UdpRecvBundleMulti, UdpRecvFromRingMulti, UdpRecvRingMulti, UdpSocket};
pub use unix::{
    UnixDatagram, UnixListener, UnixSocket, UnixStream, UnixStreamReader, UnixStreamWriter,
};
