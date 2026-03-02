//! Networking for Norn.
//!
//! Unless otherwise documented, functions and methods in this module require
//! an active [`crate::Driver`] context.
mod socket;
mod tcp;
mod udp;

pub use socket::Event;
pub use tcp::{TcpListener, TcpSocket, TcpStream, TcpStreamReader, TcpStreamWriter};
pub use udp::UdpSocket;
