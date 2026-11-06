//! Networking for Norn.
//!
//! Unless otherwise documented, functions and methods in this module require
//! an active [`crate::Driver`] context.
pub(crate) mod socket;
mod steering;
mod tcp;
mod udp;

pub use socket::{Event, RecvMsgRingBuf, SendZcResult, SendZcUsage};
pub use steering::ReuseportCpuMap;
pub use tcp::{
    TcpListener, TcpListenerOptions, TcpRecvBundleMulti, TcpRecvRingMulti, TcpSocket, TcpStream,
    TcpStreamReader, TcpStreamWriter,
};
pub use udp::{UdpRecvBundleMulti, UdpRecvFromRingMulti, UdpRecvRingMulti, UdpSocket};
