//! Epoll-driven, pipelined load generator.
//!
//! Each client thread owns a stripe of persistent connections plus its own
//! short-lived churn connections, and runs a single non-blocking event loop.
//! Requests due at the same instant on one connection are coalesced into one
//! `write`. Close to an arrival or in closed-loop mode the loop busy-polls,
//! so client threads need dedicated cores.

use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::Instant;

use hdrhistogram::Histogram;

use crate::profile::{Op, Profile, MAX_CLASSES};
use crate::proto::{self, HEADER_LEN};
use crate::workload::{miss_key, Rng, Workload, KEY_LEN};

/// Pending-request class used for population and verification traffic,
/// which is validated but never recorded.
const UNRECORDED: u8 = u8::MAX;
const POPULATE_DEPTH: usize = 64;
const READ_BUFFER: usize = 256 * 1024;
const MAX_READS_PER_EVENT: usize = 4;
/// Busy-poll when the next deadline is closer than this.
const SPIN_NS: u64 = 1_500_000;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mode {
    Closed { depth: usize },
    Open { rate: u64 },
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub name: String,
    pub mode: Mode,
    /// Offsets from the shared start, in nanoseconds.
    pub start_ns: u64,
    pub measure_start_ns: u64,
    pub end_ns: u64,
    pub measured: bool,
}

pub fn timeline(profile: &Profile) -> Vec<Segment> {
    let ms = 1_000_000;
    let p = &profile.phases;
    let depth = profile.client.closed_loop_depth;
    let mut segments = Vec::new();
    let mut at = 0;
    if p.warmup_ms > 0 {
        segments.push(Segment {
            name: "warmup".into(),
            mode: Mode::Closed { depth },
            start_ns: 0,
            measure_start_ns: p.warmup_ms * ms,
            end_ns: p.warmup_ms * ms,
            measured: false,
        });
        at = p.warmup_ms * ms;
    }
    segments.push(Segment {
        name: "capacity".into(),
        mode: Mode::Closed { depth },
        start_ns: at,
        measure_start_ns: at + p.settle_ms * ms,
        end_ns: at + p.capacity_ms * ms,
        measured: true,
    });
    at += p.capacity_ms * ms;
    for &rate in &profile.rates {
        let end = at + (p.settle_ms + p.segment_ms) * ms;
        segments.push(Segment {
            name: format!("open-{}", rate_label(rate)),
            mode: Mode::Open { rate },
            start_ns: at,
            measure_start_ns: at + p.settle_ms * ms,
            end_ns: end,
            measured: true,
        });
        at = end;
    }
    segments
}

pub fn rate_label(rate: u64) -> String {
    if rate.is_multiple_of(1000) {
        format!("{}k", rate / 1000)
    } else {
        rate.to_string()
    }
}

pub fn new_histogram() -> Histogram<u64> {
    let mut h = Histogram::new_with_bounds(1, 60_000_000_000, 3).unwrap();
    // Touch every counter page before measurement starts.
    h.reset();
    h
}

/// Statistics of one segment from one client thread.
pub struct SegStats {
    /// Arrivals scheduled (open loop) or requests issued (closed loop) with an
    /// intended time in the measurement window.
    pub planned: u64,
    /// Requests dropped because a connection reached its outstanding cap.
    pub dropped: u64,
    /// Responses whose completion time fell in the measurement window.
    pub completed: u64,
    pub completed_by_class: [u64; MAX_CLASSES],
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub churn_sessions: u64,
    /// Offered latency (from intended time) by class, for requests whose
    /// intended time fell in the measurement window. The churn class records
    /// whole-session latency (connect through last response).
    pub latency: Vec<Histogram<u64>>,
    /// Time between an arrival's intended time and when it was written into
    /// the connection buffer.
    pub lag: Histogram<u64>,
}

impl SegStats {
    pub fn empty(classes: usize) -> Self {
        Self {
            planned: 0,
            dropped: 0,
            completed: 0,
            completed_by_class: [0; MAX_CLASSES],
            rx_bytes: 0,
            tx_bytes: 0,
            churn_sessions: 0,
            latency: (0..classes).map(|_| new_histogram()).collect(),
            lag: new_histogram(),
        }
    }

    pub fn merge(&mut self, other: &SegStats) {
        self.planned += other.planned;
        self.dropped += other.dropped;
        self.completed += other.completed;
        for (a, b) in self
            .completed_by_class
            .iter_mut()
            .zip(other.completed_by_class)
        {
            *a += b;
        }
        self.rx_bytes += other.rx_bytes;
        self.tx_bytes += other.tx_bytes;
        self.churn_sessions += other.churn_sessions;
        for (a, b) in self.latency.iter_mut().zip(&other.latency) {
            a.add(b).unwrap();
        }
        self.lag.add(&other.lag).unwrap();
    }
}

pub struct ThreadReport {
    pub segments: Vec<SegStats>,
    pub populate_ns: u64,
    pub drain_ns: u64,
    pub verified: u64,
}

/// State shared by the coordinator and all client threads.
pub struct Shared {
    pub profile: Profile,
    pub workload: Workload,
    pub segments: Vec<Segment>,
    pub address: SocketAddr,
    pub epoch: Instant,
    /// Shared start offset from `epoch`, published between the barriers.
    pub start_ns: Mutex<u64>,
    pub barrier: Barrier,
    pub failed: AtomicBool,
    pub error: Mutex<Option<String>>,
}

impl Shared {
    pub fn now_ns(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    pub fn fail(&self, message: String) {
        let mut error = self.error.lock().unwrap();
        if error.is_none() {
            *error = Some(message);
        }
        self.failed.store(true, Ordering::Release);
    }
}

#[derive(Debug, Clone, Copy)]
enum Expect {
    Value { tag: u64, len: u32 },
    Miss,
    Stored,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    opaque: u32,
    class: u8,
    intended_ns: u64,
    expect: Expect,
}

struct Conn {
    stream: TcpStream,
    /// Global connection id; owns writable keys `w:*:<id>`.
    id: u32,
    persistent: bool,
    wbuf: Vec<u8>,
    wpos: usize,
    rbuf: Vec<u8>,
    rpos: usize,
    rlen: usize,
    pending: VecDeque<Pending>,
    next_opaque: u32,
    /// Latest issued version per writable slot.
    versions: Vec<u32>,
    epollout: bool,
    dirty: bool,
    /// Churn connections: responses still expected, and session start.
    churn_remaining: usize,
    churn_intended_ns: u64,
}

impl Conn {
    fn new(stream: TcpStream, id: u32, persistent: bool, slots: u32) -> io::Result<Self> {
        stream.set_nodelay(true)?;
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            id,
            persistent,
            wbuf: Vec::with_capacity(64 * 1024),
            wpos: 0,
            rbuf: vec![0; if persistent { READ_BUFFER } else { 64 * 1024 }],
            rpos: 0,
            rlen: 0,
            pending: VecDeque::new(),
            next_opaque: 0,
            versions: if persistent {
                vec![0; slots as usize]
            } else {
                Vec::new()
            },
            epollout: false,
            dirty: false,
            churn_remaining: 0,
            churn_intended_ns: 0,
        })
    }

    fn opaque(&mut self) -> u32 {
        self.next_opaque = self.next_opaque.wrapping_add(1);
        self.next_opaque
    }
}

struct Epoll {
    fd: i32,
    events: Vec<libc::epoll_event>,
}

impl Epoll {
    fn new() -> io::Result<Self> {
        // SAFETY: plain syscall.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd,
            events: vec![libc::epoll_event { events: 0, u64: 0 }; 256],
        })
    }

    fn ctl(&self, op: i32, fd: i32, token: usize, out: bool) -> io::Result<()> {
        let mut event = libc::epoll_event {
            events: (libc::EPOLLIN | libc::EPOLLRDHUP | if out { libc::EPOLLOUT } else { 0 })
                as u32,
            u64: token as u64,
        };
        // SAFETY: `event` is a valid epoll_event for the duration of the call.
        if unsafe { libc::epoll_ctl(self.fd, op, fd, &mut event) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn wait(&mut self, timeout_ms: i32) -> io::Result<usize> {
        // SAFETY: the events buffer is valid for `len` entries.
        let n = unsafe {
            libc::epoll_wait(
                self.fd,
                self.events.as_mut_ptr(),
                self.events.len() as i32,
                timeout_ms,
            )
        };
        if n < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                return Ok(0);
            }
            return Err(error);
        }
        Ok(n as usize)
    }
}

impl Drop for Epoll {
    fn drop(&mut self) {
        // SAFETY: we own the descriptor.
        unsafe { libc::close(self.fd) };
    }
}

struct Worker<'a> {
    shared: &'a Shared,
    w: &'a Workload,
    thread: usize,
    epoll: Epoll,
    conns: Vec<Option<Conn>>,
    free: Vec<usize>,
    persistent: usize,
    rng: Rng,
    stats: Vec<SegStats>,
    start_ns: u64,
    /// Connections with completions since the last closed-loop refill.
    refill: Vec<usize>,
    dirty: Vec<usize>,
    key: [u8; KEY_LEN],
    outstanding: usize,
    churn_opened: u64,
}

fn err(context: &str, error: impl std::fmt::Display) -> String {
    format!("{context}: {error}")
}

impl<'a> Worker<'a> {
    fn new(shared: &'a Shared, thread: usize) -> Result<Self, String> {
        let profile = &shared.profile;
        let epoll = Epoll::new().map_err(|e| err("epoll_create", e))?;
        let mut conns = Vec::new();
        let total = profile.client.connections;
        let threads = profile.client.threads;
        for id in (thread..total).step_by(threads) {
            let stream = TcpStream::connect(shared.address)
                .map_err(|e| err(&format!("connect {}", shared.address), e))?;
            let conn = Conn::new(
                stream,
                id as u32,
                true,
                profile.writable.slots_per_connection,
            )
            .map_err(|e| err("configure socket", e))?;
            epoll
                .ctl(
                    libc::EPOLL_CTL_ADD,
                    conn.stream.as_raw_fd(),
                    conns.len(),
                    false,
                )
                .map_err(|e| err("epoll_ctl", e))?;
            conns.push(Some(conn));
        }
        let classes = profile.class_names().len();
        Ok(Self {
            shared,
            w: &shared.workload,
            thread,
            epoll,
            persistent: conns.len(),
            conns,
            free: Vec::new(),
            rng: Rng::new(profile.seed ^ ((thread as u64 + 1) << 40)),
            stats: shared
                .segments
                .iter()
                .map(|_| SegStats::empty(classes))
                .collect(),
            start_ns: 0,
            refill: Vec::new(),
            dirty: Vec::new(),
            key: [0; KEY_LEN],
            outstanding: 0,
            churn_opened: thread as u64 * 97,
        })
    }

    /// Index of the measured segment whose window contains `t` (relative).
    fn measured_segment(&self, t: u64) -> Option<usize> {
        self.shared
            .segments
            .iter()
            .position(|s| s.measured && t >= s.measure_start_ns && t < s.end_ns)
    }

    fn rel(&self, abs_ns: u64) -> u64 {
        abs_ns.saturating_sub(self.start_ns)
    }

    fn mark_dirty(&mut self, index: usize) {
        let conn = self.conns[index].as_mut().unwrap();
        if !conn.dirty {
            conn.dirty = true;
            self.dirty.push(index);
        }
    }

    /// Append one request of `class` to connection `index`.
    fn issue(&mut self, index: usize, class: u8, intended_ns: u64) {
        let w = self.w;
        let conn = self.conns[index].as_mut().unwrap();
        let opaque = conn.opaque();
        let expect = if class == w.churn_class {
            let ks = &w.keyspaces[w.churn_keyspace as usize];
            let id = ks.sample(&mut self.rng);
            ks.key(id, &mut self.key);
            proto::encode_get(&mut conn.wbuf, &self.key, opaque);
            Expect::Value {
                tag: ks.tag(id),
                len: ks.value_len(id) as u32,
            }
        } else {
            let c = w.classes[class as usize];
            match c.op {
                Op::Get => {
                    let ks = &w.keyspaces[c.keyspace as usize];
                    let id = ks.sample(&mut self.rng);
                    ks.key(id, &mut self.key);
                    proto::encode_get(&mut conn.wbuf, &self.key, opaque);
                    Expect::Value {
                        tag: ks.tag(id),
                        len: ks.value_len(id) as u32,
                    }
                }
                Op::GetMiss => {
                    miss_key(self.rng.next_u64(), &mut self.key);
                    proto::encode_get(&mut conn.wbuf, &self.key, opaque);
                    Expect::Miss
                }
                Op::Set => {
                    let slot = self.rng.below(u64::from(w.writable.slots)) as u32;
                    let version = conn.versions[slot as usize] + 1;
                    conn.versions[slot as usize] = version;
                    let tag = w.writable.tag(conn.id, slot, version);
                    let value = w.pool.value(tag, w.writable.value_len(tag));
                    w.writable.key(conn.id, slot, &mut self.key);
                    proto::encode_set(&mut conn.wbuf, &self.key, value, opaque);
                    Expect::Stored
                }
                Op::GetOwn => {
                    let slot = self.rng.below(u64::from(w.writable.slots)) as u32;
                    self.own_get(index, slot, opaque)
                }
            }
        };
        let conn = self.conns[index].as_mut().unwrap();
        conn.pending.push_back(Pending {
            opaque,
            class,
            intended_ns,
            expect,
        });
        self.outstanding += 1;
        self.mark_dirty(index);
    }

    fn own_get(&mut self, index: usize, slot: u32, opaque: u32) -> Expect {
        let w = self.w;
        let conn = self.conns[index].as_mut().unwrap();
        let tag = w.writable.tag(conn.id, slot, conn.versions[slot as usize]);
        w.writable.key(conn.id, slot, &mut self.key);
        proto::encode_get(&mut conn.wbuf, &self.key, opaque);
        Expect::Value {
            tag,
            len: w.writable.value_len(tag) as u32,
        }
    }

    fn push_unrecorded(&mut self, index: usize, expect: Expect, opaque: u32) {
        self.conns[index]
            .as_mut()
            .unwrap()
            .pending
            .push_back(Pending {
                opaque,
                class: UNRECORDED,
                intended_ns: 0,
                expect,
            });
        self.outstanding += 1;
        self.mark_dirty(index);
    }

    fn conn_error(&self, op: &str, index: usize, error: io::Error) -> String {
        let conn = self.conns[index].as_ref().unwrap();
        let at = self.rel(self.shared.now_ns()) as f64 / 1e9;
        let segment = self
            .shared
            .segments
            .iter()
            .find(|s| (at * 1e9) < s.end_ns as f64)
            .map_or("drain", |s| s.name.as_str());
        format!(
            "{op} on {} connection {} at {at:.3}s ({segment}) with {} outstanding, \
             {} unsent bytes: {error}",
            if conn.persistent {
                "persistent"
            } else {
                "churn"
            },
            conn.id,
            conn.pending.len(),
            conn.wbuf.len() - conn.wpos,
        )
    }

    fn flush(&mut self) -> Result<(), String> {
        while let Some(index) = self.dirty.pop() {
            let Some(conn) = self.conns[index].as_mut() else {
                continue;
            };
            conn.dirty = false;
            let mut written = 0_u64;
            loop {
                if conn.wpos == conn.wbuf.len() {
                    conn.wbuf.clear();
                    conn.wpos = 0;
                    if conn.epollout {
                        conn.epollout = false;
                        self.epoll
                            .ctl(libc::EPOLL_CTL_MOD, conn.stream.as_raw_fd(), index, false)
                            .map_err(|e| err("epoll_ctl", e))?;
                    }
                    break;
                }
                match conn.stream.write(&conn.wbuf[conn.wpos..]) {
                    Ok(0) => return Err("connection closed during write".into()),
                    Ok(n) => {
                        conn.wpos += n;
                        written += n as u64;
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        if !conn.epollout {
                            conn.epollout = true;
                            self.epoll
                                .ctl(libc::EPOLL_CTL_MOD, conn.stream.as_raw_fd(), index, true)
                                .map_err(|e| err("epoll_ctl", e))?;
                        }
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => {
                        self.dirty.push(index);
                        return Err(self.conn_error("write", index, e));
                    }
                }
            }
            if let Some(seg) = self.measured_segment(self.rel(self.shared.now_ns())) {
                self.stats[seg].tx_bytes += written;
            }
        }
        Ok(())
    }

    /// Read and validate everything available on connection `index`.
    fn on_readable(&mut self, index: usize) -> Result<(), String> {
        for _ in 0..MAX_READS_PER_EVENT {
            let conn = self.conns[index].as_mut().unwrap();
            if conn.rlen == conn.rbuf.len() {
                if conn.rpos > 0 {
                    conn.rbuf.copy_within(conn.rpos..conn.rlen, 0);
                    conn.rlen -= conn.rpos;
                    conn.rpos = 0;
                } else {
                    let len = conn.rbuf.len();
                    conn.rbuf.resize(len * 2, 0);
                }
            }
            let rlen = conn.rlen;
            match conn.stream.read(&mut conn.rbuf[rlen..]) {
                Ok(0) => {
                    if conn.pending.is_empty() && !conn.persistent {
                        return Ok(());
                    }
                    return Err(format!(
                        "server closed connection {} with {} requests outstanding",
                        conn.id,
                        conn.pending.len()
                    ));
                }
                Ok(n) => {
                    conn.rlen += n;
                    let now = self.shared.now_ns();
                    if let Some(seg) = self.measured_segment(self.rel(now)) {
                        self.stats[seg].rx_bytes += n as u64;
                    }
                    self.parse(index, now)?;
                    if self.conns[index].is_none() {
                        return Ok(()); // churn connection finished
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(self.conn_error("read", index, e)),
            }
        }
        Ok(())
    }

    fn parse(&mut self, index: usize, now: u64) -> Result<(), String> {
        let w = self.w;
        let rel_now = self.rel(now);
        let done_seg = self.measured_segment(rel_now);
        let mut completed = false;
        loop {
            let conn = self.conns[index].as_mut().unwrap();
            let available = conn.rlen - conn.rpos;
            if available < HEADER_LEN {
                break;
            }
            let frame = &conn.rbuf[conn.rpos..conn.rlen];
            let header = proto::decode_response(&frame[..HEADER_LEN])?;
            let total = HEADER_LEN + header.body_len;
            if available < total {
                if total > conn.rbuf.len() {
                    // Make room for a frame larger than the buffer.
                    conn.rbuf.copy_within(conn.rpos..conn.rlen, 0);
                    conn.rlen -= conn.rpos;
                    conn.rpos = 0;
                    conn.rbuf.resize(total.next_power_of_two(), 0);
                }
                break;
            }
            let Some(pending) = conn.pending.pop_front() else {
                return Err(format!("unexpected response on connection {}", conn.id));
            };
            if header.opaque != pending.opaque {
                return Err(format!(
                    "connection {}: response opaque {} != expected {}",
                    conn.id, header.opaque, pending.opaque
                ));
            }
            let body = &frame[HEADER_LEN..total];
            match pending.expect {
                Expect::Value { tag, len } => {
                    let extras = header.extras_len;
                    let value = &body[(extras + header.key_len).min(body.len())..];
                    if header.opcode != proto::OP_GET
                        || header.status != proto::STATUS_SUCCESS
                        || extras != 4
                        || value.len() != len as usize
                    {
                        return Err(format!(
                            "connection {}: bad GET response status {:#x} opcode {:#x} \
                             extras {extras} value {} (want {len})",
                            conn.id,
                            header.status,
                            header.opcode,
                            value.len()
                        ));
                    }
                    if value != w.pool.value(tag, len as usize) {
                        return Err(format!("connection {}: GET value mismatch", conn.id));
                    }
                }
                Expect::Miss => {
                    if header.opcode != proto::OP_GET
                        || header.status != proto::STATUS_KEY_NOT_FOUND
                    {
                        return Err(format!(
                            "connection {}: expected miss, status {:#x}",
                            conn.id, header.status
                        ));
                    }
                }
                Expect::Stored => {
                    if header.opcode != proto::OP_SET || header.status != proto::STATUS_SUCCESS {
                        return Err(format!(
                            "connection {}: SET failed with status {:#x}",
                            conn.id, header.status
                        ));
                    }
                }
            }
            conn.rpos += total;
            self.outstanding -= 1;
            completed = true;
            let persistent = conn.persistent;
            if !persistent {
                conn.churn_remaining -= 1;
            }
            let churn_done = !persistent && conn.churn_remaining == 0;
            let churn_intended = conn.churn_intended_ns;
            if pending.class != UNRECORDED {
                if let Some(seg) = done_seg {
                    self.stats[seg].completed += 1;
                    self.stats[seg].completed_by_class[pending.class as usize] += 1;
                }
                if persistent {
                    if let Some(seg) = self.measured_segment(self.rel(pending.intended_ns)) {
                        self.stats[seg].latency[pending.class as usize]
                            .saturating_record(now.saturating_sub(pending.intended_ns));
                    }
                }
            }
            if churn_done {
                if let Some(seg) = self.measured_segment(self.rel(churn_intended)) {
                    let stats = &mut self.stats[seg];
                    stats.churn_sessions += 1;
                    stats.latency[w.churn_class as usize]
                        .saturating_record(now.saturating_sub(churn_intended));
                }
                self.close_churn(index)?;
                return Ok(());
            }
        }
        let conn = self.conns[index].as_mut().unwrap();
        if conn.rpos == conn.rlen {
            conn.rpos = 0;
            conn.rlen = 0;
        } else if conn.rpos > conn.rbuf.len() / 2 {
            conn.rbuf.copy_within(conn.rpos..conn.rlen, 0);
            conn.rlen -= conn.rpos;
            conn.rpos = 0;
        }
        if completed && conn.persistent {
            self.refill.push(index);
        }
        Ok(())
    }

    fn open_churn(&mut self, intended_ns: u64) -> Result<(), String> {
        let profile = &self.shared.profile;
        self.churn_opened += 1;
        let source = 2 + (self.churn_opened % 250) as u8;
        let stream = connect_nonblocking(self.shared.address, source)
            .map_err(|e| err("churn connect", e))?;
        let mut conn = Conn::new(stream, u32::MAX, false, 0).map_err(|e| err("churn socket", e))?;
        conn.churn_remaining = profile.churn.requests_per_connection;
        conn.churn_intended_ns = intended_ns;
        let index = match self.free.pop() {
            Some(index) => index,
            None => {
                self.conns.push(None);
                self.conns.len() - 1
            }
        };
        self.epoll
            .ctl(libc::EPOLL_CTL_ADD, conn.stream.as_raw_fd(), index, false)
            .map_err(|e| err("epoll_ctl", e))?;
        self.conns[index] = Some(conn);
        if let Some(seg) = self.measured_segment(self.rel(intended_ns)) {
            self.stats[seg].planned += profile.churn.requests_per_connection as u64;
        }
        for _ in 0..profile.churn.requests_per_connection {
            self.issue(index, self.w.churn_class, intended_ns);
        }
        Ok(())
    }

    fn close_churn(&mut self, index: usize) -> Result<(), String> {
        let conn = self.conns[index].take().unwrap();
        self.epoll
            .ctl(libc::EPOLL_CTL_DEL, conn.stream.as_raw_fd(), index, false)
            .map_err(|e| err("epoll_ctl", e))?;
        self.free.push(index);
        drop(conn); // orderly FIN from the client side
        Ok(())
    }

    /// Wait for and dispatch socket events.
    fn poll(&mut self, timeout_ms: i32) -> Result<(), String> {
        let n = self
            .epoll
            .wait(timeout_ms)
            .map_err(|e| err("epoll_wait", e))?;
        for i in 0..n {
            let event = self.epoll.events[i];
            let index = event.u64 as usize;
            let flags = event.events;
            if self.conns.get(index).is_none_or(Option::is_none) {
                continue;
            }
            if flags & (libc::EPOLLIN | libc::EPOLLRDHUP | libc::EPOLLHUP | libc::EPOLLERR) as u32
                != 0
            {
                self.on_readable(index)?;
            }
            if flags & libc::EPOLLOUT as u32 != 0 && self.conns[index].is_some() {
                self.mark_dirty(index);
            }
        }
        Ok(())
    }

    fn check_failed(&self) -> Result<(), String> {
        if self.shared.failed.load(Ordering::Acquire) {
            return Err("another client thread failed".into());
        }
        Ok(())
    }

    /// Issue `counts[i]` unrecorded requests on persistent connection `i`
    /// with at most [`POPULATE_DEPTH`] outstanding per connection, then wait
    /// until every one has been acknowledged and validated.
    fn pipelined<F>(&mut self, counts: &[u64], mut work: F) -> Result<(), String>
    where
        F: FnMut(&mut Self, usize, u64),
    {
        let deadline = self.shared.now_ns() + 120_000_000_000;
        let mut issued = vec![0_u64; counts.len()];
        loop {
            self.check_failed()?;
            let mut remaining = false;
            for (index, &count) in counts.iter().enumerate() {
                while issued[index] < count
                    && self.conns[index].as_ref().unwrap().pending.len() < POPULATE_DEPTH
                {
                    work(self, index, issued[index]);
                    issued[index] += 1;
                }
                remaining |= issued[index] < count;
            }
            if !remaining && self.outstanding == 0 {
                return Ok(());
            }
            self.flush()?;
            self.poll(1)?;
            self.refill.clear();
            if self.shared.now_ns() > deadline {
                return Err(format!(
                    "pipelined phase timed out with {} requests outstanding",
                    self.outstanding
                ));
            }
        }
    }

    fn populate(&mut self) -> Result<(), String> {
        let w = self.w;
        let threads = self.shared.profile.client.threads as u64;
        let thread = self.thread as u64;
        let conns = self.persistent as u64;
        // Immutable keys: this thread writes ids congruent to its index,
        // dealt round-robin over its connections.
        for ks in &w.keyspaces {
            let mine = (ks.keys + threads - 1 - thread) / threads;
            let counts: Vec<u64> = (0..conns).map(|i| (mine + conns - 1 - i) / conns).collect();
            self.pipelined(&counts, |this, index, k| {
                let id = thread + (index as u64 + k * conns) * threads;
                let conn = this.conns[index].as_mut().unwrap();
                let opaque = conn.opaque();
                ks.key(id, &mut this.key);
                let value = w.pool.value(ks.tag(id), ks.value_len(id));
                proto::encode_set(&mut conn.wbuf, &this.key, value, opaque);
                this.push_unrecorded(index, Expect::Stored, opaque);
            })?;
        }
        // Every writable slot at version zero.
        let counts = vec![u64::from(w.writable.slots); self.persistent];
        self.pipelined(&counts, |this, index, slot| {
            let conn = this.conns[index].as_mut().unwrap();
            let opaque = conn.opaque();
            let tag = w.writable.tag(conn.id, slot as u32, 0);
            let value = w.pool.value(tag, w.writable.value_len(tag));
            w.writable.key(conn.id, slot as u32, &mut this.key);
            proto::encode_set(&mut conn.wbuf, &this.key, value, opaque);
            this.push_unrecorded(index, Expect::Stored, opaque);
        })
    }

    /// Read back every writable slot and check its last issued version.
    fn verify(&mut self) -> Result<u64, String> {
        let counts = vec![u64::from(self.w.writable.slots); self.persistent];
        self.pipelined(&counts, |this, index, slot| {
            let opaque = this.conns[index].as_mut().unwrap().opaque();
            let expect = this.own_get(index, slot as u32, opaque);
            this.push_unrecorded(index, expect, opaque);
        })?;
        Ok(counts.iter().sum())
    }

    fn run(&mut self) -> Result<ThreadReport, String> {
        let shared = self.shared;
        let profile = &shared.profile;
        let populate_start = shared.now_ns();
        let populated = self.populate();
        let populate_ns = shared.now_ns() - populate_start;
        if let Err(error) = populated {
            shared.fail(error.clone());
            shared.barrier.wait();
            shared.barrier.wait();
            return Err(error);
        }
        shared.barrier.wait();
        shared.barrier.wait();
        self.check_failed()?;
        self.start_ns = *shared.start_ns.lock().unwrap();

        let segments = &shared.segments;
        let end_ns = segments.last().unwrap().end_ns;
        let threads = profile.client.threads as f64;
        let mean_burst = profile.mean_burst();
        let churn_mean_ns = if profile.churn.connections_per_sec > 0 {
            1e9 * threads / profile.churn.connections_per_sec as f64
        } else {
            f64::INFINITY
        };
        let max_outstanding = profile.client.max_outstanding_per_connection;
        let mut segment = usize::MAX;
        let mut next_arrival = f64::INFINITY;
        let mut arrival_mean = f64::INFINITY;
        let mut next_churn = self.rng.exp(churn_mean_ns);

        loop {
            self.check_failed()?;
            let now = self.rel(shared.now_ns());
            if now >= end_ns {
                break;
            }
            let current = segments.iter().position(|s| now < s.end_ns).unwrap();
            if current != segment {
                segment = current;
                let s = &segments[segment];
                match s.mode {
                    Mode::Open { rate } => {
                        arrival_mean = 1e9 * threads * mean_burst / rate as f64;
                        next_arrival = s.start_ns as f64 + self.rng.exp(arrival_mean);
                    }
                    Mode::Closed { .. } => {
                        next_arrival = f64::INFINITY;
                        self.refill.clear();
                        self.refill.extend(0..self.persistent);
                    }
                }
            }
            let s = &segments[segment];

            match s.mode {
                Mode::Open { .. } => {
                    while next_arrival <= now as f64 && next_arrival < s.end_ns as f64 {
                        let intended = next_arrival as u64;
                        let burst = self.w.pick_burst(&mut self.rng);
                        let index = self.rng.below(self.persistent as u64) as usize;
                        let seg = self.measured_segment(intended);
                        for _ in 0..burst {
                            let class = self.w.pick_class(&mut self.rng);
                            if let Some(seg) = seg {
                                self.stats[seg].planned += 1;
                                self.stats[seg].lag.saturating_record(now - intended);
                            }
                            if self.conns[index].as_ref().unwrap().pending.len() >= max_outstanding
                            {
                                if let Some(seg) = seg {
                                    self.stats[seg].dropped += 1;
                                }
                                continue;
                            }
                            self.issue(index, class, intended + self.start_ns);
                        }
                        next_arrival += self.rng.exp(arrival_mean);
                    }
                    self.refill.clear();
                }
                Mode::Closed { depth } => {
                    let seg = self.measured_segment(now);
                    while let Some(index) = self.refill.pop() {
                        while self.conns[index].as_ref().unwrap().pending.len() < depth {
                            let class = self.w.pick_class(&mut self.rng);
                            if let Some(seg) = seg {
                                self.stats[seg].planned += 1;
                            }
                            self.issue(index, class, now + self.start_ns);
                        }
                    }
                }
            }
            while next_churn <= now as f64 {
                self.open_churn(next_churn as u64 + self.start_ns)?;
                next_churn += self.rng.exp(churn_mean_ns);
            }
            self.flush()?;

            let deadline = next_arrival.min(next_churn).min(s.end_ns as f64);
            let wait = deadline - now as f64;
            let timeout = if matches!(s.mode, Mode::Closed { .. }) || wait < SPIN_NS as f64 {
                0
            } else {
                ((wait - 1e6) / 1e6) as i32
            };
            self.poll(timeout)?;
        }

        // Drain: no new work, wait for every outstanding response.
        let drain_start = shared.now_ns();
        let drain_deadline = drain_start + profile.phases.drain_timeout_ms * 1_000_000;
        self.refill.clear();
        while self.outstanding > 0 {
            self.check_failed()?;
            self.flush()?;
            self.poll(1)?;
            self.refill.clear();
            if shared.now_ns() > drain_deadline {
                return Err(format!(
                    "drain timed out with {} requests outstanding",
                    self.outstanding
                ));
            }
        }
        let drain_ns = shared.now_ns() - drain_start;
        let verified = self.verify()?;
        Ok(ThreadReport {
            segments: std::mem::take(&mut self.stats),
            populate_ns,
            drain_ns,
            verified,
        })
    }
}

/// Start a non-blocking connect. Requests may be queued immediately: writes
/// return `WouldBlock` until the handshake completes, which arms `EPOLLOUT`.
///
/// For loopback servers the socket is first bound to `127.0.0.<source>`.
/// Churn leaves one client-side TIME_WAIT socket per connection for 60 s;
/// back-to-back runs would otherwise exhaust the ephemeral ports of a single
/// source address.
fn connect_nonblocking(address: SocketAddr, source: u8) -> io::Result<TcpStream> {
    let SocketAddr::V4(v4) = address else {
        return TcpStream::connect(address);
    };
    let sockaddr = |ip: [u8; 4], port: u16| libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: port.to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(ip),
        },
        sin_zero: [0; 8],
    };
    let len = std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
    // SAFETY: plain socket syscalls on a descriptor we own; `sockaddr_in`
    // is fully initialized.
    unsafe {
        let fd = libc::socket(
            libc::AF_INET,
            libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC,
            0,
        );
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let stream = TcpStream::from_raw_fd(fd);
        if v4.ip().is_loopback() {
            let local = sockaddr([127, 0, 0, source], 0);
            if libc::bind(fd, (&local as *const libc::sockaddr_in).cast(), len) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        let remote = sockaddr(v4.ip().octets(), v4.port());
        let rc = libc::connect(fd, (&remote as *const libc::sockaddr_in).cast(), len);
        if rc < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EINPROGRESS) {
                return Err(error);
            }
        }
        Ok(stream)
    }
}

pub fn pin_current_thread(cpu: usize) -> io::Result<()> {
    // SAFETY: cpu_set_t is plain data; CPU_SET bounds are checked below.
    unsafe {
        if cpu >= libc::CPU_SETSIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cpu out of range",
            ));
        }
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Run one client thread to completion. On failure the shared error is set
/// and the thread still participates in both start barriers.
pub fn run_thread(shared: Arc<Shared>, thread: usize, cpu: Option<usize>) -> Option<ThreadReport> {
    if let Some(cpu) = cpu {
        if let Err(e) = pin_current_thread(cpu) {
            shared.fail(format!("pin client thread {thread} to CPU {cpu}: {e}"));
        }
    }
    let mut worker = match Worker::new(&shared, thread) {
        Ok(worker) => worker,
        Err(error) => {
            shared.fail(error);
            shared.barrier.wait();
            shared.barrier.wait();
            return None;
        }
    };
    match worker.run() {
        Ok(report) => Some(report),
        Err(error) => {
            shared.fail(format!("client thread {thread}: {error}"));
            None
        }
    }
}
