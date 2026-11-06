use std::sync::Arc;

use norn_metrics::{Counter, Gauge, Kind, Metric, Padded, Visit};

/// Counters written by one [`Driver`](super::Driver)'s thread.
#[derive(Debug, Default)]
pub(crate) struct RingCounters {
    pub(crate) enters: Counter,
    pub(crate) waiting_enters: Counter,
    pub(crate) sqes_submitted: Counter,
    pub(crate) cqes: Counter,
    pub(crate) in_flight: Gauge,
    pub(crate) sq_backlog: Gauge,
    pub(crate) sq_backlogged: Counter,
    pub(crate) cq_overflows: Counter,
    pub(crate) cqes_dropped: Gauge,
    pub(crate) enter_retries: Counter,
    pub(crate) unparks: Counter,
    pub(crate) status: Gauge,
    pub(crate) waiting: Gauge,
}

/// Flags a ring as waiting in `io_uring_enter` until dropped.
///
/// It sets a flag instead of timing the wait: two clock reads cost about 250
/// instructions per waiting enter, 14% of a tick in a probe where every op
/// parks. Readers estimate idle time by sampling the flag.
pub(crate) struct Waiting<'a>(&'a RingCounters);

impl<'a> Waiting<'a> {
    #[inline(always)]
    pub(crate) fn start(counters: &'a RingCounters) -> Self {
        counters.waiting.set(1);
        Self(counters)
    }
}

impl Drop for Waiting<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.0.waiting.set(0);
    }
}

/// A read-only view of one ring's counters.
///
/// This is `Send + Sync`: any thread may take a
/// [`snapshot`](RingMetrics::snapshot) at any time, including while the
/// driver's thread is blocked or stuck and after the driver is dropped.
#[derive(Clone)]
pub struct RingMetrics(pub(crate) Arc<Padded<RingCounters>>);

impl std::fmt::Debug for RingMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl Visit for RingMetrics {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        self.snapshot().visit(f);
    }
}

impl RingMetrics {
    /// Read the current values.
    pub fn snapshot(&self) -> RingSnapshot {
        let c = &self.0;
        RingSnapshot {
            enters: c.enters.get(),
            waiting_enters: c.waiting_enters.get(),
            sqes_submitted: c.sqes_submitted.get(),
            cqes: c.cqes.get(),
            in_flight: c.in_flight.get(),
            sq_backlog: c.sq_backlog.get(),
            sq_backlogged: c.sq_backlogged.get(),
            cq_overflows: c.cq_overflows.get(),
            cqes_dropped: c.cqes_dropped.get(),
            enter_retries: c.enter_retries.get(),
            unparks: c.unparks.get(),
            waiting: c.waiting.get() != 0,
            status: match c.status.get() {
                0 => RingStatus::Running,
                1 => RingStatus::Closing,
                2 => RingStatus::Draining,
                _ => RingStatus::Shutdown,
            },
        }
    }
}

/// The lifecycle stage of a ring's driver.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RingStatus {
    /// Accepting requests.
    #[default]
    Running,
    /// No longer accepting requests; cancelling what is outstanding.
    Closing,
    /// Waiting for every outstanding request's final completion.
    Draining,
    /// Shut down.
    Shutdown,
}

/// Values read from a [`RingMetrics`].
///
/// `in_flight` and `cqes_dropped` are published when the driver parks, so
/// while its thread is busy they can trail by one executor tick; `sq_backlog`
/// changes with the backlog. Counts that a submit adds, such as
/// `sqes_submitted`, appear when its enter returns.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RingSnapshot {
    /// `io_uring_enter` system calls the driver made.
    pub enters: u64,
    /// Enters that waited for a completion or a timeout.
    pub waiting_enters: u64,
    /// Submission entries the kernel consumed, added when each enter returns.
    ///
    /// Stays zero on `IORING_SETUP_SQPOLL` rings, whose kernel thread consumes
    /// entries without any enter reporting them.
    pub sqes_submitted: u64,
    /// Completion entries reaped from the ring, including the driver's own.
    pub cqes: u64,
    /// Entries pushed to the submission queue whose final completion has not
    /// been reaped, including the driver's own (its wakeup read,
    /// cancellations and closes). Published after each park reaps
    /// completions and again just before it waits, so it is current while
    /// the driver's thread is blocked.
    pub in_flight: u64,
    /// Entries waiting in the driver's queue for room in the submission queue.
    pub sq_backlog: u64,
    /// Entries that ever had to wait for room in the submission queue.
    pub sq_backlogged: u64,
    /// Times the completion queue fell into overflow: the kernel started
    /// holding completions in its overflow list because the queue was full.
    /// The driver checks at every park, so this counts episodes, not
    /// completions.
    pub cq_overflows: u64,
    /// Completions the kernel dropped because it could not even queue them
    /// for overflow. Any nonzero value means lost completions.
    pub cqes_dropped: u64,
    /// `io_uring_enter` calls that failed with a transient error (`EINTR`,
    /// `EBUSY`, `EAGAIN` or `ENOMEM`) and were retried or deferred.
    pub enter_retries: u64,
    /// Wakeups from another thread that reached a parked, running ring.
    /// Shutdown's own wakeup is not counted.
    pub unparks: u64,
    /// Whether the driver's thread was inside a waiting `io_uring_enter` when
    /// read: idle, waiting for a completion, a timeout or a wakeup. Waiting
    /// for room in an SQPOLL ring's submission queue is not idle and is not
    /// flagged.
    ///
    /// The executor's `running` flag is also false during the work a park
    /// does before it waits, such as reaping completions; this flag is set
    /// only for the wait itself. The fraction of samples that find it set
    /// estimates how idle the thread is: sampling every millisecond for ten
    /// seconds gives the idle fraction to within about one percent.
    pub waiting: bool,
    /// The driver's lifecycle stage.
    pub status: RingStatus,
}

impl Visit for RingSnapshot {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        let counter = |name, help, value| Metric {
            name,
            help,
            kind: Kind::Counter,
            value,
        };
        let gauge = |name, help, value| Metric {
            name,
            help,
            kind: Kind::Gauge,
            value,
        };
        f(counter(
            "norn_uring_enters_total",
            "io_uring_enter calls.",
            self.enters,
        ));
        f(counter(
            "norn_uring_waiting_enters_total",
            "io_uring_enter calls that waited for a completion or timeout.",
            self.waiting_enters,
        ));
        f(counter(
            "norn_uring_sqes_submitted_total",
            "Submission entries consumed by the kernel.",
            self.sqes_submitted,
        ));
        f(counter(
            "norn_uring_cqes_total",
            "Completion entries reaped.",
            self.cqes,
        ));
        f(gauge(
            "norn_uring_in_flight",
            "Pushed entries whose final completion has not been reaped.",
            self.in_flight,
        ));
        f(gauge(
            "norn_uring_sq_backlog",
            "Entries waiting for room in the submission queue.",
            self.sq_backlog,
        ));
        f(counter(
            "norn_uring_sq_backlogged_total",
            "Entries that had to wait for room in the submission queue.",
            self.sq_backlogged,
        ));
        f(counter(
            "norn_uring_cq_overflows_total",
            "Times the completion queue fell into overflow.",
            self.cq_overflows,
        ));
        f(counter(
            "norn_uring_cqes_dropped_total",
            "Completions the kernel dropped.",
            self.cqes_dropped,
        ));
        f(counter(
            "norn_uring_enter_retries_total",
            "io_uring_enter calls that failed transiently and were retried.",
            self.enter_retries,
        ));
        f(counter(
            "norn_uring_unparks_total",
            "Cross-thread wakeups that reached a parked ring.",
            self.unparks,
        ));
        f(gauge(
            "norn_uring_waiting",
            "1 while the driver's thread waits in io_uring_enter.",
            u64::from(self.waiting),
        ));
        f(gauge(
            "norn_uring_status",
            "Driver stage: 0 running, 1 closing, 2 draining, 3 shut down.",
            self.status as u64,
        ));
    }
}
