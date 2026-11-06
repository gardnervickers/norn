use std::sync::Arc;

use norn_metrics::{Counter, Kind, Metric, Padded, Visit};
use norn_task::{TaskMetrics, TaskSnapshot};

/// Counters written by one [`LocalExecutor`](crate::LocalExecutor)'s thread.
#[derive(Debug, Default)]
pub(crate) struct ExecutorCounters {
    pub(crate) ticks: Counter,
    /// Incremented each time the executor starts or stops running the root
    /// future, tasks, or the wakers and destructors they trigger, so it is
    /// odd while running. Keeping both facts in one word lets a reader on
    /// another thread see them consistently.
    pub(crate) activity: Counter,
    pub(crate) polls: Counter,
    pub(crate) budget_exhausted: Counter,
    pub(crate) waiting_parks: Counter,
}

/// Flips the activity word when created and again when dropped, unwinding
/// included, so a panic cannot leave the running state wrong.
pub(crate) struct Activity<'a>(&'a Counter);

impl<'a> Activity<'a> {
    #[inline(always)]
    pub(crate) fn flip(activity: &'a Counter) -> Self {
        activity.incr();
        Self(activity)
    }
}

impl Drop for Activity<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.0.incr();
    }
}

/// Publishes a tick's poll count when dropped, so the polls of a tick that
/// unwinds are counted too.
pub(crate) struct TickPolls<'a> {
    pub(crate) counter: &'a Counter,
    pub(crate) polled: usize,
}

impl Drop for TickPolls<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        self.counter.add(self.polled as u64);
    }
}

/// A read-only view of a [`LocalExecutor`](crate::LocalExecutor)'s scheduling
/// counters and task counts.
///
/// This is `Send + Sync`: any thread may take a
/// [`snapshot`](ExecutorMetrics::snapshot) at any time, including while the
/// executor is stuck in a poll and after it is dropped.
#[derive(Clone)]
pub struct ExecutorMetrics {
    pub(crate) counters: Arc<Padded<ExecutorCounters>>,
    pub(crate) tasks: TaskMetrics,
}

impl std::fmt::Debug for ExecutorMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl Visit for ExecutorMetrics {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        self.snapshot().visit(f);
    }
}

impl ExecutorMetrics {
    /// Read the current values.
    pub fn snapshot(&self) -> ExecutorSnapshot {
        let c = &self.counters;
        let activity = c.activity.get();
        ExecutorSnapshot {
            ticks: c.ticks.get(),
            polls: c.polls.get(),
            budget_exhausted: c.budget_exhausted.get(),
            waiting_parks: c.waiting_parks.get(),
            running: activity % 2 == 1,
            activity,
            tasks: self.tasks.snapshot(),
        }
    }
}

/// Values read from an [`ExecutorMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExecutorSnapshot {
    /// Scheduler loop iterations that parked. Each polls the root future,
    /// runs up to the task poll budget, and then calls
    /// [`Park::park`](crate::park::Park::park) once. The final iteration, in
    /// which the root future completes, does not park and is not counted.
    pub ticks: u64,
    /// Task runs: one per `Runnable::run`, which polls the task's future
    /// unless the task was cancelled first.
    pub polls: u64,
    /// Ticks that ended because the task poll budget ran out while tasks were
    /// still runnable.
    pub budget_exhausted: u64,
    /// Ticks that parked waiting for the next completion because nothing was
    /// runnable.
    pub waiting_parks: u64,
    /// Whether the executor was running when read: polling its root future or
    /// a task, or running a waker or destructor they triggered, inside
    /// `block_on` or while dropping its tasks. It is false while parked, while
    /// the park layer shuts down, and whenever no `block_on` is in progress.
    pub running: bool,
    /// A word that changes every time the executor starts or stops running;
    /// it is odd exactly when [`running`](Self::running) is true.
    ///
    /// Both come from one atomic word, so they are consistent with each other.
    /// A watchdog uses them to tell a stuck worker from an idle or finished
    /// one: if two samples an interval apart show the same `activity` and
    /// `running` is true, the executor has run for the whole interval without
    /// parking. Reads are relaxed. The memory model does not bound how stale
    /// a relaxed load may be, but on real hardware a store reaches other
    /// cores within microseconds.
    pub activity: u64,
    /// Task counts of the executor's queue.
    pub tasks: TaskSnapshot,
}

impl Visit for ExecutorSnapshot {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        f(Metric {
            name: "norn_executor_ticks_total",
            help: "Scheduler loop iterations that parked.",
            kind: Kind::Counter,
            value: self.ticks,
        });
        f(Metric {
            name: "norn_executor_polls_total",
            help: "Task runs.",
            kind: Kind::Counter,
            value: self.polls,
        });
        f(Metric {
            name: "norn_executor_budget_exhausted_total",
            help: "Ticks cut short by the task poll budget with tasks still runnable.",
            kind: Kind::Counter,
            value: self.budget_exhausted,
        });
        f(Metric {
            name: "norn_executor_waiting_parks_total",
            help: "Ticks that parked waiting because nothing was runnable.",
            kind: Kind::Counter,
            value: self.waiting_parks,
        });
        f(Metric {
            name: "norn_executor_running",
            help: "1 while the executor runs its root future, tasks, or their wakers.",
            kind: Kind::Gauge,
            value: u64::from(self.running),
        });
        self.tasks.visit(f);
    }
}

/// Poll durations recorded when poll timing is enabled.
#[derive(Debug, Default)]
pub(crate) struct PollTimeCounters {
    buckets: [Counter; 5],
    total_ns: Counter,
    max_ns: norn_metrics::Gauge,
}

/// The upper bounds of the first four poll-time buckets, in nanoseconds; the
/// fifth holds every longer poll.
const POLL_TIME_BOUNDS_NS: [u64; 4] = [10_000, 100_000, 1_000_000, 10_000_000];

impl PollTimeCounters {
    /// Record one poll. Owner thread only.
    #[inline]
    pub(crate) fn record(&self, elapsed: std::time::Duration) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        let bucket = POLL_TIME_BOUNDS_NS
            .iter()
            .position(|&bound| ns <= bound)
            .unwrap_or(POLL_TIME_BOUNDS_NS.len());
        self.buckets[bucket].incr();
        self.total_ns.add(ns);
        if ns > self.max_ns.get() {
            self.max_ns.set(ns);
        }
    }
}

/// A read-only view of an executor's poll durations, present once
/// [`LocalExecutor::enable_poll_timing`](crate::LocalExecutor::enable_poll_timing)
/// has been called.
#[derive(Clone)]
pub struct PollTimeMetrics(pub(crate) Arc<Padded<PollTimeCounters>>);

impl std::fmt::Debug for PollTimeMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl Visit for PollTimeMetrics {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        self.snapshot().visit(f);
    }
}

impl PollTimeMetrics {
    /// Read the current values.
    pub fn snapshot(&self) -> PollTimeSnapshot {
        let c = &self.0;
        PollTimeSnapshot {
            up_to_10us: c.buckets[0].get(),
            up_to_100us: c.buckets[1].get(),
            up_to_1ms: c.buckets[2].get(),
            up_to_10ms: c.buckets[3].get(),
            over_10ms: c.buckets[4].get(),
            total_ns: c.total_ns.get(),
            max_ns: c.max_ns.get(),
        }
    }
}

/// Poll durations read from a [`PollTimeMetrics`].
///
/// The five buckets do not overlap; together they count every timed poll,
/// including one that unwinds. The root future's polls are not timed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PollTimeSnapshot {
    /// Polls that took at most 10 µs.
    pub up_to_10us: u64,
    /// Polls that took more than 10 µs and at most 100 µs.
    pub up_to_100us: u64,
    /// Polls that took more than 100 µs and at most 1 ms.
    pub up_to_1ms: u64,
    /// Polls that took more than 1 ms and at most 10 ms.
    pub up_to_10ms: u64,
    /// Polls that took more than 10 ms.
    pub over_10ms: u64,
    /// Total time spent in timed polls, in nanoseconds.
    pub total_ns: u64,
    /// The longest timed poll, in nanoseconds.
    pub max_ns: u64,
}

impl PollTimeSnapshot {
    /// The number of timed polls.
    pub fn count(&self) -> u64 {
        self.up_to_10us + self.up_to_100us + self.up_to_1ms + self.up_to_10ms + self.over_10ms
    }
}

impl Visit for PollTimeSnapshot {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        let counter = |name, help, value| Metric {
            name,
            help,
            kind: Kind::Counter,
            value,
        };
        f(counter(
            "norn_executor_poll_time_le_10us_total",
            "Timed polls that took at most 10 us.",
            self.up_to_10us,
        ));
        f(counter(
            "norn_executor_poll_time_le_100us_total",
            "Timed polls that took more than 10 us and at most 100 us.",
            self.up_to_100us,
        ));
        f(counter(
            "norn_executor_poll_time_le_1ms_total",
            "Timed polls that took more than 100 us and at most 1 ms.",
            self.up_to_1ms,
        ));
        f(counter(
            "norn_executor_poll_time_le_10ms_total",
            "Timed polls that took more than 1 ms and at most 10 ms.",
            self.up_to_10ms,
        ));
        f(counter(
            "norn_executor_poll_time_gt_10ms_total",
            "Timed polls that took more than 10 ms.",
            self.over_10ms,
        ));
        f(counter(
            "norn_executor_poll_time_nanoseconds_total",
            "Time spent in timed polls.",
            self.total_ns,
        ));
        f(Metric {
            name: "norn_executor_poll_time_max_nanoseconds",
            help: "The longest timed poll.",
            kind: Kind::Gauge,
            value: self.max_ns,
        });
    }
}
