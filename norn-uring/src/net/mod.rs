//! Networking for Norn.
//!
//! Unless otherwise documented, functions and methods in this module require
//! an active [`crate::Driver`] context.
mod socket;
mod tcp;
mod udp;

pub use socket::Event;
pub use tcp::{
    AttachRecvRingError, AttachRecvRingErrorKind, AttachSendRingError, AttachSendRingErrorKind,
    BundledTcpWriter, EnqueueError, FinishSendRingOutcome, SendError, TcpListener, TcpRecvBundles,
    TcpSocket, TcpStream, TcpStreamReader, TcpStreamWriter,
};
pub use udp::{
    AttachUdpSendRingError, AttachUdpSendRingErrorKind, BundledUdpSocket, DatagramCommitError,
    DatagramPushError, FinishUdpSendRingOutcome, UdpDatagramBuilder, UdpSocket,
    SEND_BUNDLE_MAX_SEGMENTS,
};
