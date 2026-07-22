use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures_core::Stream;

use crate::bufring::send::SendRing;
use crate::bufring::SendBuf;
use crate::fd::BundledPermit;
use crate::operation::Op;
use crate::util::notify::Notify;

use super::{SendBundle, SendEmptyDatagram, Socket};

#[derive(Debug, Clone)]
pub(crate) struct PumpError {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
    message: Rc<str>,
}

impl PumpError {
    pub(crate) fn from_io(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
            message: Rc::from(error.to_string()),
        }
    }

    pub(crate) fn kind(&self) -> io::ErrorKind {
        self.kind
    }

    pub(crate) fn raw_os_error(&self) -> Option<i32> {
        self.raw_os_error
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpPhase {
    Open,
    Finishing,
    Abandoned,
}

#[derive(Debug, Clone)]
pub(crate) enum PumpTerminal {
    Drained,
    Failed(PumpError),
    CleanupFailed(PumpError),
}

pub(crate) struct PumpControl {
    phase: Cell<PumpPhase>,
    terminal: RefCell<Option<PumpTerminal>>,
    generation: Cell<u64>,
    changed: Notify,
    pump_waker: RefCell<Option<Waker>>,
}

pub(crate) struct QueuedSegment {
    pub(crate) buffer: SendBuf,
    pub(crate) len: usize,
}

pub(crate) struct QueuedDatagram {
    pub(crate) segments: Vec<QueuedSegment>,
    pub(crate) empty_token: Option<SendBuf>,
}

#[derive(Default)]
pub(crate) struct DatagramQueue {
    pending: VecDeque<QueuedDatagram>,
    active: Option<ActiveDatagram>,
    committed: u64,
    completed: u64,
}

#[derive(Debug)]
enum ActiveDatagram {
    Bundle { target: u64 },
    Empty { _token: SendBuf },
}

impl DatagramQueue {
    pub(crate) fn push(&mut self, datagram: QueuedDatagram) {
        self.pending.push_back(datagram);
        self.committed = self.committed.wrapping_add(1);
    }

    pub(crate) fn committed(&self) -> u64 {
        self.committed
    }

    pub(crate) fn completed(&self) -> u64 {
        self.completed
    }

    fn complete_active(&mut self) {
        let active = self.active.take();
        debug_assert!(active.is_some());
        self.completed = self.completed.wrapping_add(1);
    }

    fn clear(&mut self) {
        self.pending.clear();
        self.active = None;
    }
}

enum PumpMode {
    Stream,
    Datagram(Rc<RefCell<DatagramQueue>>),
}

impl std::fmt::Debug for PumpControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PumpControl")
            .field("phase", &self.phase.get())
            .field("terminal", &self.terminal.borrow())
            .finish()
    }
}

impl PumpControl {
    pub(crate) fn new() -> Self {
        Self {
            phase: Cell::new(PumpPhase::Open),
            terminal: RefCell::new(None),
            generation: Cell::new(0),
            changed: Notify::default(),
            pump_waker: RefCell::new(None),
        }
    }

    pub(crate) fn phase(&self) -> PumpPhase {
        self.phase.get()
    }

    pub(crate) fn request_finish(&self) {
        if self.phase.get() == PumpPhase::Open {
            self.phase.set(PumpPhase::Finishing);
            self.advance();
        }
    }

    pub(crate) fn abandon(&self) {
        if self.terminal.borrow().is_none() {
            self.phase.set(PumpPhase::Abandoned);
            self.advance();
        }
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.get()
    }

    pub(crate) fn notify_work(&self) {
        self.advance();
    }

    pub(crate) async fn changed_since(&self, observed: u64) {
        loop {
            if self.generation.get() != observed || self.terminal.borrow().is_some() {
                return;
            }
            self.changed.wait().await;
        }
    }

    pub(crate) async fn wait_terminal(&self) -> PumpTerminal {
        loop {
            if let Some(terminal) = self.terminal.borrow().clone() {
                return terminal;
            }
            let observed = self.generation.get();
            let notified = self.changed.wait();
            if self.generation.get() != observed {
                continue;
            }
            notified.await;
        }
    }

    fn complete(&self, terminal: PumpTerminal) {
        self.terminal.borrow_mut().replace(terminal);
        self.advance();
    }

    fn register_pump_waker(&self, waker: &Waker) {
        let mut slot = self.pump_waker.borrow_mut();
        if slot
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *slot = Some(waker.clone());
        }
    }

    fn advance(&self) {
        self.generation.set(self.generation.get().wrapping_add(1));
        self.changed.notify(usize::MAX);
        if let Some(waker) = self.pump_waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

pub(crate) fn spawn_stream_pump(
    executor: &norn_executor::Handle,
    socket: Socket,
    ring: Rc<SendRing>,
    attachment: u64,
    control: Rc<PumpControl>,
    permit: BundledPermit,
) {
    executor
        .spawn(SendPump {
            socket,
            ring,
            attachment,
            control,
            permit: Some(permit),
            current: None,
            current_terminal: false,
            stop_requested: false,
            failure: None,
            mode: PumpMode::Stream,
        })
        .detach();
}

pub(crate) fn spawn_datagram_pump(
    executor: &norn_executor::Handle,
    socket: Socket,
    ring: Rc<SendRing>,
    attachment: u64,
    control: Rc<PumpControl>,
    permit: BundledPermit,
    queue: Rc<RefCell<DatagramQueue>>,
) {
    executor
        .spawn(SendPump {
            socket,
            ring,
            attachment,
            control,
            permit: Some(permit),
            current: None,
            current_terminal: false,
            stop_requested: false,
            failure: None,
            mode: PumpMode::Datagram(queue),
        })
        .detach();
}

struct SendPump {
    socket: Socket,
    ring: Rc<SendRing>,
    attachment: u64,
    control: Rc<PumpControl>,
    permit: Option<BundledPermit>,
    current: Option<ActiveOp>,
    current_terminal: bool,
    stop_requested: bool,
    failure: Option<PumpError>,
    mode: PumpMode,
}

enum ActiveOp {
    Bundle(Pin<Box<Op<SendBundle>>>),
    Empty(Pin<Box<Op<SendEmptyDatagram>>>),
}

impl ActiveOp {
    fn request_stop(&mut self) {
        match self {
            Self::Bundle(op) => op.as_mut().request_stop(),
            Self::Empty(op) => op.as_mut().request_stop(),
        }
    }
}

impl SendPump {
    fn finish(&mut self, terminal: PumpTerminal) -> Poll<()> {
        debug_assert!(self.current.is_none());
        drop(self.permit.take());
        self.control.complete(terminal);
        Poll::Ready(())
    }

    fn clear_private_queue(&self) {
        if let PumpMode::Datagram(queue) = &self.mode {
            queue.borrow_mut().clear();
        }
    }

    fn datagram_active(&self) -> bool {
        match &self.mode {
            PumpMode::Stream => false,
            PumpMode::Datagram(queue) => {
                matches!(
                    queue.borrow().active.as_ref(),
                    Some(ActiveDatagram::Bundle { .. })
                )
            }
        }
    }

    fn drained(&self) -> bool {
        let framing_empty = match &self.mode {
            PumpMode::Stream => true,
            PumpMode::Datagram(queue) => {
                let queue = queue.borrow();
                queue.pending.is_empty() && queue.active.is_none()
            }
        };
        framing_empty && self.ring.outstanding_is_empty() && self.ring.checked_out() == 0
    }

    fn prepare_datagram(&mut self) {
        let PumpMode::Datagram(queue) = &self.mode else {
            return;
        };
        let datagram = {
            let mut queue = queue.borrow_mut();
            if queue.active.is_some() {
                return;
            }
            queue.pending.pop_front()
        };
        let Some(datagram) = datagram else {
            return;
        };

        if datagram.segments.is_empty() {
            let token = datagram
                .empty_token
                .expect("empty UDP datagram missing its capacity token");
            queue.borrow_mut().active = Some(ActiveDatagram::Empty { _token: token });
            let permit = self
                .permit
                .as_ref()
                .expect("send pump lost bundled permit")
                .clone();
            self.current = Some(ActiveOp::Empty(Box::pin(
                self.socket.send_empty_datagram_bundled(permit),
            )));
            self.current_terminal = false;
            return;
        }
        debug_assert!(datagram.empty_token.is_none());

        for segment in datagram.segments {
            if let Err((error, _buffer)) =
                self.ring
                    .enqueue_buffer(self.attachment, segment.buffer, segment.len)
            {
                self.ring.fail_io(&error);
                return;
            }
        }
        queue.borrow_mut().active = Some(ActiveDatagram::Bundle {
            target: self.ring.accepted(),
        });
    }

    fn validate_datagram_completion(&mut self, completed: u64, empty: bool) {
        let PumpMode::Datagram(queue) = &self.mode else {
            return;
        };
        let target = match queue.borrow().active {
            Some(ActiveDatagram::Bundle { target }) => Some(target),
            _ => None,
        };
        if !empty || target != Some(completed) {
            let error = io::Error::new(
                io::ErrorKind::InvalidData,
                "UDP send bundle did not complete exactly one whole datagram",
            );
            self.ring.fail_io(&error);
            if self.failure.is_none() {
                self.failure = Some(PumpError::from_io(&error));
            }
        }
    }
}

impl Future for SendPump {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            if this.failure.is_none() {
                this.failure = this.ring.failure().as_ref().map(PumpError::from_io);
            }

            let phase = this.control.phase();
            let should_stop = phase == PumpPhase::Abandoned
                || this.failure.is_some()
                || (phase == PumpPhase::Finishing && this.drained())
                || (this.datagram_active() && this.ring.outstanding_is_empty());

            if should_stop && !this.stop_requested && !this.current_terminal {
                if let Some(current) = this.current.as_mut() {
                    this.ring.request_terminal_stop();
                    current.request_stop();
                    this.stop_requested = true;
                }
            }

            if let Some(ActiveOp::Bundle(current)) = this.current.as_mut() {
                match current.as_mut().poll_next(cx) {
                    Poll::Ready(Some(event)) => {
                        match event.result {
                            Ok(outcome) => {
                                this.validate_datagram_completion(outcome.completed, outcome.empty);
                            }
                            Err(error) => {
                                if !this.ring.expected_stop_error(&error) && this.failure.is_none()
                                {
                                    this.failure = Some(PumpError::from_io(&error));
                                }
                            }
                        }
                        this.current_terminal = !event.more;
                        this.control.notify_work();
                        continue;
                    }
                    Poll::Ready(None) => {
                        this.current = None;
                        this.current_terminal = false;
                        if this.stop_requested {
                            this.ring.clear_terminal_stop();
                        }
                        this.stop_requested = false;
                        if let PumpMode::Datagram(queue) = &this.mode {
                            if this.failure.is_none() && this.ring.outstanding_is_empty() {
                                queue.borrow_mut().complete_active();
                                this.control.notify_work();
                            }
                        }
                        continue;
                    }
                    Poll::Pending => {
                        this.ring.register_pump_waker(cx.waker());
                        this.control.register_pump_waker(cx.waker());
                        return Poll::Pending;
                    }
                }
            }

            if let Some(ActiveOp::Empty(current)) = this.current.as_mut() {
                match current.as_mut().poll(cx) {
                    Poll::Ready(result) => {
                        this.current = None;
                        this.current_terminal = false;
                        if this.stop_requested {
                            this.stop_requested = false;
                        }
                        match result {
                            Ok(()) => {
                                if let PumpMode::Datagram(queue) = &this.mode {
                                    queue.borrow_mut().complete_active();
                                }
                                this.control.notify_work();
                            }
                            Err(error)
                                if this.control.phase() == PumpPhase::Abandoned
                                    && error.raw_os_error() == Some(libc::ECANCELED) => {}
                            Err(error) => {
                                this.ring.fail_io(&error);
                                if this.failure.is_none() {
                                    this.failure = Some(PumpError::from_io(&error));
                                }
                            }
                        }
                        continue;
                    }
                    Poll::Pending => {
                        this.ring.register_pump_waker(cx.waker());
                        this.control.register_pump_waker(cx.waker());
                        return Poll::Pending;
                    }
                }
            }

            if phase == PumpPhase::Abandoned || this.failure.is_some() {
                this.clear_private_queue();
                if this.ring.checked_out() != 0 {
                    this.ring.register_pump_waker(cx.waker());
                    this.control.register_pump_waker(cx.waker());
                    return Poll::Pending;
                }
                let original_failure = this.failure.clone();
                match this.ring.sanitize_attachment(this.attachment) {
                    Ok(()) => {
                        let terminal = original_failure
                            .map(PumpTerminal::Failed)
                            .unwrap_or(PumpTerminal::Drained);
                        return this.finish(terminal);
                    }
                    Err(error) => {
                        return this
                            .finish(PumpTerminal::CleanupFailed(PumpError::from_io(&error)));
                    }
                }
            }

            if phase == PumpPhase::Finishing && this.drained() {
                return match this.ring.end_attachment(this.attachment) {
                    Ok(()) => this.finish(PumpTerminal::Drained),
                    Err(error) => {
                        this.finish(PumpTerminal::CleanupFailed(PumpError::from_io(&error)))
                    }
                };
            }

            this.prepare_datagram();

            // Preparing an empty datagram installs a singleshot operation
            // directly. Loop back to poll it before considering the ring idle.
            if this.current.is_some() {
                continue;
            }

            if !this.ring.outstanding_is_empty() {
                let permit = this
                    .permit
                    .as_ref()
                    .expect("send pump lost bundled permit")
                    .clone();
                this.current = Some(ActiveOp::Bundle(Box::pin(
                    this.socket
                        .send_bundle_bundled(Rc::clone(&this.ring), permit),
                )));
                this.current_terminal = false;
                continue;
            }

            this.ring.register_pump_waker(cx.waker());
            this.control.register_pump_waker(cx.waker());
            return Poll::Pending;
        }
    }
}

impl Drop for SendPump {
    fn drop(&mut self) {
        if self.control.terminal.borrow().is_some() {
            return;
        }
        let error = io::Error::new(
            io::ErrorKind::Interrupted,
            "runtime send pump stopped before reaching its terminal fence",
        );
        self.ring.fail_io(&error);
        self.control
            .complete(PumpTerminal::CleanupFailed(PumpError::from_io(&error)));
    }
}

#[cfg(test)]
mod tests {
    use std::mem::MaybeUninit;

    use socket2::{Domain, Type};

    use super::*;
    use crate::buf::StableBufMut;
    use crate::bufring::SendBufRing;
    use crate::fd::Direction;

    fn segment(ring: &Rc<SendRing>, attachment: u64, byte: u8) -> QueuedSegment {
        let mut buffer = ring.try_acquire(attachment).unwrap().unwrap();
        buffer.spare_capacity_mut()[..8]
            .iter_mut()
            .for_each(|slot: &mut MaybeUninit<u8>| {
                slot.write(byte);
            });
        unsafe { buffer.set_init(8) };
        QueuedSegment { buffer, len: 8 }
    }

    #[test]
    fn udp_preparation_publishes_only_the_head_and_failure_discards_the_tail() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(driver);
        executor.block_on(async {
            let socket = Socket::bind("127.0.0.1:0".parse().unwrap(), Domain::IPV4, Type::DGRAM)
                .await
                .unwrap();
            let permit = socket.acquire_bundled(Direction::Write).unwrap();
            let ring = SendBufRing::builder(31)
                .buf_count(4)
                .buf_len(16)
                .build()
                .unwrap()
                .inner;
            let attachment = ring.begin_attachment().unwrap();
            let queue = Rc::new(RefCell::new(DatagramQueue::default()));
            queue.borrow_mut().push(QueuedDatagram {
                segments: vec![segment(&ring, attachment, 1), segment(&ring, attachment, 2)],
                empty_token: None,
            });
            queue.borrow_mut().push(QueuedDatagram {
                segments: vec![segment(&ring, attachment, 3), segment(&ring, attachment, 4)],
                empty_token: None,
            });

            let control = Rc::new(PumpControl::new());
            let mut pump = SendPump {
                socket: socket.clone(),
                ring: Rc::clone(&ring),
                attachment,
                control: Rc::clone(&control),
                permit: Some(permit),
                current: None,
                current_terminal: false,
                stop_requested: false,
                failure: None,
                mode: PumpMode::Datagram(Rc::clone(&queue)),
            };

            pump.prepare_datagram();
            assert_eq!(ring.accepted(), 16);
            assert_eq!(ring.checked_out(), 2);
            assert_eq!(queue.borrow().pending.len(), 1);
            assert!(matches!(
                queue.borrow().active.as_ref(),
                Some(ActiveDatagram::Bundle { .. })
            ));

            let failure = io::Error::new(io::ErrorKind::InvalidData, "injected head failure");
            ring.fail_io(&failure);
            pump.clear_private_queue();
            assert_eq!(ring.accepted(), 16, "tail datagram became visible");
            assert_eq!(ring.checked_out(), 0);
            assert!(queue.borrow().pending.is_empty());
            assert!(queue.borrow().active.is_none());

            ring.sanitize_attachment(attachment).unwrap();
            let _ = pump.finish(PumpTerminal::Failed(PumpError::from_io(&failure)));
            drop(pump);
            drop(ring);
            socket.close().await.unwrap();
        });
    }
}
