use std::io;
use std::rc::Rc;

use io_uring::{opcode, types};

use crate::bufring::send::{Reconcile, SendRing};
use crate::fd::NornFd;
use crate::operation::{CQEResult, Multishot, Operation, Singleshot};

#[derive(Debug)]
pub(crate) struct SendBundleEvent {
    pub(crate) result: io::Result<Reconcile>,
    pub(crate) more: bool,
}

pub(crate) struct SendBundle {
    fd: NornFd,
    bgid: u16,
    ring: Rc<SendRing>,
}

impl std::fmt::Debug for SendBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendBundle")
            .field("fd", &self.fd)
            .field("bgid", &self.bgid)
            .finish_non_exhaustive()
    }
}

impl SendBundle {
    pub(crate) fn new(fd: NornFd, bgid: u16, ring: Rc<SendRing>) -> Self {
        Self { fd, bgid, ring }
    }

    fn process(&self, result: CQEResult) -> SendBundleEvent {
        let more = result.more();
        let flags = result.flags;
        let result = match result.result {
            Ok(bytes) => self.ring.reconcile(bytes, flags),
            Err(error) => {
                self.ring.fail_io(&error);
                Err(error)
            }
        };
        SendBundleEvent { result, more }
    }
}

// Safety: the operation owns descriptor and ring references through every CQE;
// all consumed BIDs are reconciled by one shared completion path.
unsafe impl Operation for SendBundle {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::SendBundle::new(types::Fd(fd.0), self.bgid).build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::SendBundle::new(types::Fixed(fd.0), self.bgid).build()
            }
        })
    }

    fn cleanup(&mut self, result: CQEResult) {
        let _ = self.process(result);
    }
}

impl Multishot for SendBundle {
    type Item = SendBundleEvent;

    fn update(&mut self, result: CQEResult) -> Self::Item {
        self.process(result)
    }

    fn complete(self, result: CQEResult) -> Option<Self::Item> {
        Some(self.process(result))
    }
}

#[derive(Debug)]
pub(crate) struct SendEmptyDatagram {
    fd: NornFd,
}

impl SendEmptyDatagram {
    pub(crate) fn new(fd: NornFd) -> Self {
        Self { fd }
    }
}

// Safety: a zero-length send does not dereference its data pointer, and the
// operation retains the descriptor through its terminal CQE.
unsafe impl Operation for SendEmptyDatagram {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let pointer = std::ptr::NonNull::<u8>::dangling().as_ptr();
        Ok(match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::Send::new(types::Fd(fd.0), pointer, 0).build(),
            crate::fd::FdKind::Fixed(fd) => {
                opcode::Send::new(types::Fixed(fd.0), pointer, 0).build()
            }
        })
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for SendEmptyDatagram {
    type Output = io::Result<()>;

    fn complete(self, result: CQEResult) -> Self::Output {
        result.result.map(drop)
    }
}
