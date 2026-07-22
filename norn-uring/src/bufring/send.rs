use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::task::Waker;

use crate::buf::{StableBuf, StableBufMut};
use crate::util::notify::Notify;
use crate::Handle;

use super::registered::{RegisteredBufRing, RegisteredEntry};
use super::{Bgid, Bid};

/// A move-only registered buffer ring for outbound bundle queues.
pub struct SendBufRing {
    pub(crate) inner: Rc<SendRing>,
}

impl fmt::Debug for SendBufRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendBufRing")
            .field("bgid", &self.inner.registered.bgid())
            .field("buf_count", &self.inner.registered.buf_count())
            .field("buf_capacity", &self.inner.registered.buf_capacity())
            .field("attached", &self.inner.state.borrow().attachment)
            .finish()
    }
}

impl SendBufRing {
    /// Creates a builder for an outbound ring with buffer-group ID `id`.
    pub fn builder(id: Bgid) -> SendBufRingBuilder {
        SendBufRingBuilder::new(id)
    }

    /// Returns the number of buffers in this ring.
    pub fn buf_count(&self) -> u16 {
        self.inner.registered.buf_count()
    }

    /// Returns the capacity of each buffer.
    pub fn buf_capacity(&self) -> usize {
        self.inner.registered.buf_capacity()
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.inner.registered.same_driver(handle)
    }
}

/// Builder for [`SendBufRing`].
#[derive(Debug, Clone, Copy)]
pub struct SendBufRingBuilder {
    bgid: Bgid,
    ring_entries: u16,
    buf_count: u16,
    buf_len: usize,
}

impl SendBufRingBuilder {
    fn new(bgid: Bgid) -> Self {
        Self {
            bgid,
            ring_entries: 128,
            buf_count: 0,
            buf_len: 4096,
        }
    }

    /// Sets the number of physical entries in the provided-buffer ring.
    pub fn ring_entries(mut self, entries: u16) -> Self {
        self.ring_entries = entries;
        self
    }

    /// Sets the number of outbound buffers.
    pub fn buf_count(mut self, count: u16) -> Self {
        self.buf_count = count;
        self
    }

    /// Sets the capacity of each outbound buffer.
    pub fn buf_len(mut self, len: usize) -> Self {
        self.buf_len = len;
        self
    }

    /// Allocates and registers an empty outbound buffer ring.
    pub fn build(self) -> io::Result<SendBufRing> {
        let mut count = self.buf_count;
        let mut entries = self.ring_entries;
        if count == 0 || entries < count {
            let max = count.max(entries);
            count = max;
            entries = max;
        }
        if entries > 1 << 15 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "ring_entries exceeded 32768",
            ));
        }
        entries = entries.next_power_of_two();

        let handle = Handle::current();
        let registered =
            RegisteredBufRing::new(self.bgid, entries, count, self.buf_len, handle.clone())?;
        handle.with_submitter(|submitter| registered.register(submitter))?;
        Ok(SendBufRing {
            inner: Rc::new(SendRing::new(registered)),
        })
    }
}

/// An exclusively owned outbound ring buffer.
pub struct SendBuf {
    ring: Rc<SendRing>,
    token: SendToken,
    initialized: usize,
    owned: bool,
}

impl fmt::Debug for SendBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendBuf")
            .field("bid", &self.token.bid)
            .field("generation", &self.token.generation)
            .field("initialized", &self.initialized)
            .field("capacity", &self.capacity())
            .finish()
    }
}

impl SendBuf {
    fn new(ring: Rc<SendRing>, token: SendToken) -> Self {
        Self {
            ring,
            token,
            initialized: 0,
            owned: true,
        }
    }

    /// Returns the number of initialized bytes.
    pub fn len(&self) -> usize {
        self.initialized
    }

    /// Returns whether no bytes have been initialized.
    pub fn is_empty(&self) -> bool {
        self.initialized == 0
    }

    /// Returns this buffer's total capacity.
    pub fn capacity(&self) -> usize {
        self.ring.registered.buf_capacity()
    }

    /// Returns the initialized prefix.
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self.ring.registered.stable_ptr(self.token.bid),
                self.initialized,
            )
        }
    }

    /// Returns the initialized prefix mutably.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.ring.registered.stable_mut_ptr(self.token.bid),
                self.initialized,
            )
        }
    }

    /// Returns the uninitialized suffix available to a direct producer.
    pub fn spare_capacity_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        unsafe {
            let start = self
                .ring
                .registered
                .stable_mut_ptr(self.token.bid)
                .add(self.initialized)
                .cast();
            std::slice::from_raw_parts_mut(start, self.capacity() - self.initialized)
        }
    }

    pub(crate) fn belongs_to(&self, ring: &Rc<SendRing>, attachment: u64) -> bool {
        Rc::ptr_eq(&self.ring, ring) && self.token.attachment == attachment
    }

    pub(crate) fn token(&self) -> SendToken {
        self.token
    }

    pub(crate) fn take_token(mut self) -> SendToken {
        self.owned = false;
        self.token
    }
}

impl Drop for SendBuf {
    fn drop(&mut self) {
        if self.owned {
            self.ring.return_checked_out(self.token);
        }
    }
}

// Safety: the BID state machine grants this value exclusive access to its
// backing allocation until it is dropped or consumed by enqueue.
unsafe impl StableBuf for SendBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.ring.registered.stable_ptr(self.token.bid)
    }

    fn bytes_init(&self) -> usize {
        self.initialized
    }
}

// Safety: the BID state machine grants this value exclusive mutable access,
// and the physical allocation never moves while the ring is registered.
unsafe impl StableBufMut for SendBuf {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.ring.registered.stable_mut_ptr(self.token.bid)
    }

    fn bytes_remaining(&self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(init_len <= self.capacity());
        self.initialized = init_len;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SendToken {
    attachment: u64,
    bid: Bid,
    generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BidState {
    Free,
    CheckedOut(SendToken),
    Outstanding(SendToken),
}

#[derive(Debug, Clone, Copy)]
struct Segment {
    token: SendToken,
    len: usize,
    end_offset: u64,
}

#[derive(Debug, Clone)]
struct StoredError {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
    message: Rc<str>,
}

impl StoredError {
    fn from_io(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
            message: Rc::from(error.to_string()),
        }
    }

    fn invalid_data(message: &'static str) -> Self {
        Self {
            kind: io::ErrorKind::InvalidData,
            raw_os_error: None,
            message: Rc::from(message),
        }
    }

    fn to_io_error(&self) -> io::Error {
        match self.raw_os_error {
            Some(code) => io::Error::from_raw_os_error(code),
            None => io::Error::new(self.kind, self.message.to_string()),
        }
    }
}

#[derive(Debug)]
struct SendState {
    attachment: Option<u64>,
    next_attachment: u64,
    next_position: u64,
    free: VecDeque<Bid>,
    bids: Vec<BidState>,
    generations: Vec<u64>,
    outstanding: VecDeque<Segment>,
    checked_out: usize,
    accepted: u64,
    completed: u64,
    failure: Option<StoredError>,
    stopping: bool,
}

impl SendState {
    fn new(count: usize) -> Self {
        Self {
            attachment: None,
            next_attachment: 1,
            next_position: 0,
            free: (0..count as u16).collect(),
            bids: vec![BidState::Free; count],
            generations: vec![0; count],
            outstanding: VecDeque::new(),
            checked_out: 0,
            accepted: 0,
            completed: 0,
            failure: None,
            stopping: false,
        }
    }

    fn begin_attachment(&mut self) -> io::Result<u64> {
        if self.attachment.is_some()
            || self.checked_out != 0
            || !self.outstanding.is_empty()
            || self.failure.is_some()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send ring is not detached and clean",
            ));
        }
        let attachment = self.next_attachment;
        self.next_attachment = self.next_attachment.wrapping_add(1).max(1);
        self.attachment = Some(attachment);
        self.accepted = 0;
        self.completed = 0;
        self.stopping = false;
        Ok(attachment)
    }

    fn checkout(&mut self, attachment: u64) -> io::Result<Option<SendToken>> {
        SendRing::validate_attachment(self, attachment)?;
        SendRing::check_failure(self)?;
        let Some(bid) = self.free.pop_front() else {
            return Ok(None);
        };
        let idx = usize::from(bid);
        self.generations[idx] = self.generations[idx].wrapping_add(1);
        let token = SendToken {
            attachment,
            bid,
            generation: self.generations[idx],
        };
        assert_eq!(self.bids[idx], BidState::Free);
        self.bids[idx] = BidState::CheckedOut(token);
        self.checked_out += 1;
        Ok(Some(token))
    }

    fn commit(
        &mut self,
        attachment: u64,
        token: SendToken,
        len: usize,
        capacity: usize,
    ) -> io::Result<RegisteredEntry> {
        SendRing::validate_attachment(self, attachment)?;
        SendRing::check_failure(self)?;
        if len == 0 || len > capacity {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queued send length must be within buffer capacity",
            ));
        }
        let idx = usize::from(token.bid);
        if self.bids.get(idx) != Some(&BidState::CheckedOut(token)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send buffer token is stale or belongs to another attachment",
            ));
        }
        let accepted = self
            .accepted
            .checked_add(len as u64)
            .ok_or_else(|| io::Error::other("send byte watermark overflowed"))?;
        self.bids[idx] = BidState::Outstanding(token);
        self.checked_out -= 1;
        self.accepted = accepted;
        self.outstanding.push_back(Segment {
            token,
            len,
            end_offset: accepted,
        });
        let entry = RegisteredEntry {
            position: self.next_position,
            bid: token.bid,
            len,
        };
        self.next_position = self.next_position.wrapping_add(1);
        Ok(entry)
    }

    fn reconcile(&mut self, bytes: usize, first_bid: Bid) -> io::Result<Reconcile> {
        SendRing::check_failure(self)?;
        if bytes == 0 {
            return Err(SendRing::fail(
                self,
                "send bundle completed with zero bytes",
            ));
        }
        if self.outstanding.front().map(|segment| segment.token.bid) != Some(first_bid) {
            return Err(SendRing::fail(
                self,
                "send bundle CQE did not start at the FIFO head",
            ));
        }

        let mut remaining = bytes;
        let mut released = Vec::new();
        while remaining != 0 {
            let Some(front) = self.outstanding.front().copied() else {
                return Err(SendRing::fail(
                    self,
                    "send bundle completed more bytes than were queued",
                ));
            };
            if remaining < front.len {
                return Err(SendRing::fail(
                    self,
                    "send bundle completion ended inside a buffer",
                ));
            }
            remaining -= front.len;
            self.outstanding.pop_front();
            self.completed = front.end_offset;
            self.bids[usize::from(front.token.bid)] = BidState::Free;
            released.push(front.token.bid);
        }
        self.free.extend(released.iter().copied());
        Ok(Reconcile {
            released: released.len(),
            completed: self.completed,
            empty: self.outstanding.is_empty(),
        })
    }

    fn return_checked_out(&mut self, token: SendToken) -> io::Result<()> {
        let idx = usize::from(token.bid);
        if self.bids.get(idx) != Some(&BidState::CheckedOut(token)) {
            return Err(SendRing::fail(self, "dropped send buffer token is stale"));
        }
        self.bids[idx] = BidState::Free;
        self.checked_out -= 1;
        self.free.push_back(token.bid);
        Ok(())
    }
}

pub(crate) struct SendRing {
    registered: RegisteredBufRing,
    state: RefCell<SendState>,
    generation: Cell<u64>,
    changed: Notify,
    pump_waker: RefCell<Option<Waker>>,
}

impl SendRing {
    fn new(registered: RegisteredBufRing) -> Self {
        let count = usize::from(registered.buf_count());
        Self {
            registered,
            state: RefCell::new(SendState::new(count)),
            generation: Cell::new(0),
            changed: Notify::default(),
            pump_waker: RefCell::new(None),
        }
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.registered.bgid()
    }

    pub(crate) fn registered_buf_count(&self) -> u16 {
        self.registered.buf_count()
    }

    pub(crate) fn begin_attachment(&self) -> io::Result<u64> {
        self.state.borrow_mut().begin_attachment()
    }

    pub(crate) fn try_acquire(self: &Rc<Self>, attachment: u64) -> io::Result<Option<SendBuf>> {
        let mut state = self.state.borrow_mut();
        Ok(state
            .checkout(attachment)?
            .map(|token| SendBuf::new(Rc::clone(self), token)))
    }

    pub(crate) fn enqueue(&self, attachment: u64, token: SendToken, len: usize) -> io::Result<u64> {
        let entry = {
            let mut state = self.state.borrow_mut();
            state.commit(attachment, token, len, self.registered.buf_capacity())?
        };
        self.registered.publish(&[entry]);
        self.advance();
        Ok(self.state.borrow().accepted)
    }

    pub(crate) fn enqueue_buffer(
        self: &Rc<Self>,
        attachment: u64,
        buffer: SendBuf,
        len: usize,
    ) -> Result<u64, (io::Error, SendBuf)> {
        if !buffer.belongs_to(self, attachment) {
            return Err((
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "send buffer belongs to another ring attachment",
                ),
                buffer,
            ));
        }
        if len == 0 || len > buffer.len() {
            return Err((
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "queued length must be nonzero and initialized",
                ),
                buffer,
            ));
        }
        let token = buffer.token;
        match self.enqueue(attachment, token, len) {
            Ok(accepted) => {
                let _ = buffer.take_token();
                Ok(accepted)
            }
            Err(error) => Err((error, buffer)),
        }
    }

    pub(crate) fn validate_staged_buffer(
        self: &Rc<Self>,
        attachment: u64,
        buffer: &SendBuf,
        len: usize,
    ) -> io::Result<()> {
        if !buffer.belongs_to(self, attachment) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send buffer belongs to another ring attachment",
            ));
        }
        let state = self.state.borrow();
        Self::validate_attachment(&state, attachment)?;
        Self::check_failure(&state)?;
        if len == 0 || len > buffer.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queued length must be nonzero and initialized",
            ));
        }
        Ok(())
    }

    pub(crate) fn reconcile(&self, bytes: u32, flags: u32) -> io::Result<Reconcile> {
        let mut state = self.state.borrow_mut();
        let first_bid = io_uring::cqueue::buffer_select(flags)
            .ok_or_else(|| Self::fail(&mut state, "send bundle CQE omitted its first BID"))?;
        let outcome = state.reconcile(bytes as usize, first_bid)?;
        drop(state);
        self.advance();
        Ok(outcome)
    }

    pub(crate) fn fail_io(&self, error: &io::Error) {
        let mut state = self.state.borrow_mut();
        if state.stopping && error.raw_os_error() == Some(libc::ECANCELED) {
            return;
        }
        if state.failure.is_none() {
            state.failure = Some(StoredError::from_io(error));
        }
        drop(state);
        self.advance();
    }

    pub(crate) fn outstanding_is_empty(&self) -> bool {
        self.state.borrow().outstanding.is_empty()
    }

    pub(crate) fn checked_out(&self) -> usize {
        self.state.borrow().checked_out
    }

    pub(crate) fn accepted(&self) -> u64 {
        self.state.borrow().accepted
    }

    pub(crate) fn completed(&self) -> u64 {
        self.state.borrow().completed
    }

    pub(crate) fn failure(&self) -> Option<io::Error> {
        self.state
            .borrow()
            .failure
            .as_ref()
            .map(StoredError::to_io_error)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation.get()
    }

    pub(crate) async fn changed_since(&self, observed: u64) {
        loop {
            if self.generation() != observed || self.failure().is_some() {
                return;
            }
            self.changed.wait().await;
        }
    }

    pub(crate) fn end_attachment(&self, attachment: u64) -> io::Result<()> {
        let mut state = self.state.borrow_mut();
        Self::validate_attachment(&state, attachment)?;
        if state.checked_out != 0 || !state.outstanding.is_empty() {
            return Err(io::Error::other("send ring still owns buffers"));
        }
        Self::check_failure(&state)?;
        state.attachment = None;
        state.stopping = false;
        Ok(())
    }

    pub(crate) fn request_terminal_stop(&self) {
        self.state.borrow_mut().stopping = true;
    }

    pub(crate) fn clear_terminal_stop(&self) {
        self.state.borrow_mut().stopping = false;
    }

    pub(crate) fn expected_stop_error(&self, error: &io::Error) -> bool {
        self.state.borrow().stopping && error.raw_os_error() == Some(libc::ECANCELED)
    }

    pub(crate) fn sanitize_attachment(&self, attachment: u64) -> io::Result<()> {
        {
            let state = self.state.borrow();
            Self::validate_attachment(&state, attachment)?;
            if state.checked_out != 0 {
                return Err(io::Error::other("send buffers are still checked out"));
            }
        }

        self.registered.reset_registration()?;

        let mut state = self.state.borrow_mut();
        let count = usize::from(self.registered.buf_count());
        state.attachment = None;
        state.next_position = 0;
        state.free = (0..count as u16).collect();
        state.bids.fill(BidState::Free);
        state.outstanding.clear();
        state.checked_out = 0;
        state.accepted = 0;
        state.completed = 0;
        state.failure = None;
        state.stopping = false;
        drop(state);
        self.advance();
        Ok(())
    }

    pub(crate) fn register_pump_waker(&self, waker: &Waker) {
        let mut slot = self.pump_waker.borrow_mut();
        if slot
            .as_ref()
            .is_none_or(|current| !current.will_wake(waker))
        {
            *slot = Some(waker.clone());
        }
    }

    fn return_checked_out(&self, token: SendToken) {
        let mut state = self.state.borrow_mut();
        let _ = state.return_checked_out(token);
        drop(state);
        self.advance();
    }

    fn validate_attachment(state: &SendState, attachment: u64) -> io::Result<()> {
        if state.attachment == Some(attachment) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send ring attachment token is stale",
            ))
        }
    }

    fn check_failure(state: &SendState) -> io::Result<()> {
        match &state.failure {
            Some(error) => Err(error.to_io_error()),
            None => Ok(()),
        }
    }

    fn fail(state: &mut SendState, message: &'static str) -> io::Error {
        if state.failure.is_none() {
            state.failure = Some(StoredError::invalid_data(message));
        }
        io::Error::new(io::ErrorKind::InvalidData, message)
    }

    fn advance(&self) {
        self.generation.set(self.generation.get().wrapping_add(1));
        self.changed.notify(usize::MAX);
        if let Some(waker) = self.pump_waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reconcile {
    pub(crate) released: usize,
    pub(crate) completed: u64,
    pub(crate) empty: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queued(lengths: &[usize]) -> (SendState, u64, Vec<SendToken>) {
        let mut state = SendState::new(lengths.len());
        let attachment = state.begin_attachment().unwrap();
        let mut tokens = Vec::new();
        for &len in lengths {
            let token = state.checkout(attachment).unwrap().unwrap();
            state.commit(attachment, token, len, 4096).unwrap();
            tokens.push(token);
        }
        (state, attachment, tokens)
    }

    #[test]
    fn reconciles_multiple_terminal_segments_and_reuses_bids() {
        let (mut state, attachment, tokens) = queued(&[10, 20, 30]);
        let first = state.reconcile(30, tokens[0].bid).unwrap();
        assert_eq!(first.released, 2);
        assert_eq!(first.completed, 30);
        assert!(!first.empty);

        let second = state.reconcile(30, tokens[2].bid).unwrap();
        assert_eq!(second.released, 1);
        assert_eq!(second.completed, 60);
        assert!(second.empty);

        let replacement = state.checkout(attachment).unwrap().unwrap();
        assert_eq!(replacement.bid, tokens[0].bid);
        assert_ne!(replacement.generation, tokens[0].generation);
    }

    #[test]
    fn completion_inside_a_buffer_fails_closed() {
        let (mut state, _, tokens) = queued(&[64, 64]);
        let error = state.reconcile(32, tokens[0].bid).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(state.failure.is_some());
        assert_eq!(state.outstanding.len(), 2);
    }

    #[test]
    fn wrong_first_bid_fails_closed() {
        let (mut state, _, tokens) = queued(&[8, 8]);
        assert!(state.reconcile(8, tokens[1].bid).is_err());
        assert!(state.failure.is_some());
        assert_eq!(state.outstanding.len(), 2);
    }

    #[test]
    fn completion_overrun_fails_closed() {
        let (mut state, _, tokens) = queued(&[8]);
        assert!(state.reconcile(16, tokens[0].bid).is_err());
        assert!(state.failure.is_some());
    }

    #[test]
    fn zero_byte_completion_fails_closed() {
        let (mut state, _, tokens) = queued(&[8]);
        assert!(state.reconcile(0, tokens[0].bid).is_err());
        assert!(state.failure.is_some());
    }

    #[test]
    fn watermark_overflow_does_not_consume_checked_out_token() {
        let mut state = SendState::new(1);
        let attachment = state.begin_attachment().unwrap();
        let token = state.checkout(attachment).unwrap().unwrap();
        state.accepted = u64::MAX;
        assert!(state.commit(attachment, token, 1, 1).is_err());
        assert_eq!(state.bids[0], BidState::CheckedOut(token));
        assert_eq!(state.checked_out, 1);
        assert!(state.outstanding.is_empty());
    }

    #[test]
    fn dropped_checkout_returns_capacity_with_new_generation() {
        let mut state = SendState::new(1);
        let attachment = state.begin_attachment().unwrap();
        let first = state.checkout(attachment).unwrap().unwrap();
        state.return_checked_out(first).unwrap();
        let second = state.checkout(attachment).unwrap().unwrap();
        assert_eq!(first.bid, second.bid);
        assert_ne!(first.generation, second.generation);
    }

    #[test]
    fn publication_position_wraps_without_changing_fifo_order() {
        let mut state = SendState::new(2);
        let attachment = state.begin_attachment().unwrap();
        state.next_position = u64::MAX;
        let first = state.checkout(attachment).unwrap().unwrap();
        let first_entry = state.commit(attachment, first, 1, 1).unwrap();
        let second = state.checkout(attachment).unwrap().unwrap();
        let second_entry = state.commit(attachment, second, 1, 1).unwrap();
        assert_eq!(first_entry.position, u64::MAX);
        assert_eq!(second_entry.position, 0);
        assert_eq!(state.outstanding[0].token, first);
        assert_eq!(state.outstanding[1].token, second);
    }

    #[test]
    fn stored_io_failure_preserves_errno() {
        let mut state = SendState::new(1);
        state.failure = Some(StoredError::from_io(&io::Error::from_raw_os_error(
            libc::EPIPE,
        )));
        let error = SendRing::check_failure(&state).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EPIPE));
    }
}
