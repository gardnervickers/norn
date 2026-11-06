use std::sync::Arc;

use norn_metrics::{Counter, Kind, Metric, Padded, Visit};
use norn_task::{TaskMetrics, TaskSnapshot};

/// Counters written by one [`LocalExecutor`](crate::LocalExecutor)'s thread.
#[derive(Debug, Default)]
pub(crate) struct ExecutorCounters {
    /// Incremented on entering and on leaving `Park::park`, so it is odd while
    /// parked. Keeping both facts in one word lets a reader on another thread
    /// see them consistently.
    pub(crate) park_epoch: Counter,
    pub(crate) polls: Counter,
    pub(crate) budget_exhausted: Counter,
    pub(crate) waiting_parks: Counter,
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

impl ExecutorMetrics {
    /// Read the current values.
    pub fn snapshot(&self) -> ExecutorSnapshot {
        let c = &self.counters;
        let epoch = c.park_epoch.get();
        ExecutorSnapshot {
            ticks: epoch.div_ceil(2),
            polls: c.polls.get(),
            budget_exhausted: c.budget_exhausted.get(),
            waiting_parks: c.waiting_parks.get(),
            parked: epoch % 2 == 1,
            tasks: self.tasks.snapshot(),
        }
    }
}

/// Values read from an [`ExecutorMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ExecutorSnapshot {
    /// Scheduler loop iterations. Each polls the root future, runs up to the
    /// task poll budget, and then calls [`Park::park`](crate::park::Park::park)
    /// once.
    pub ticks: u64,
    /// Task polls.
    pub polls: u64,
    /// Ticks that ended because the task poll budget ran out while tasks were
    /// still runnable.
    pub budget_exhausted: u64,
    /// Ticks that parked waiting for the next completion because nothing was
    /// runnable.
    pub waiting_parks: u64,
    /// Whether the executor was inside [`Park::park`](crate::park::Park::park)
    /// when read.
    ///
    /// `ticks` and `parked` come from one word, so they are consistent with
    /// each other. Together they tell a watchdog whether a worker is stuck: if
    /// two samples an interval apart have the same `ticks` and neither is
    /// parked, one tick has been running for the whole interval, because a
    /// tick counts itself before it parks.
    pub parked: bool,
    /// Task counts of the executor's queue.
    pub tasks: TaskSnapshot,
}

impl Visit for ExecutorSnapshot {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        f(Metric {
            name: "norn_executor_ticks_total",
            help: "Scheduler loop iterations; each ends in one park call.",
            kind: Kind::Counter,
            value: self.ticks,
        });
        f(Metric {
            name: "norn_executor_polls_total",
            help: "Task polls.",
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
            name: "norn_executor_parked",
            help: "1 while the executor is inside its park layer.",
            kind: Kind::Gauge,
            value: u64::from(self.parked),
        });
        self.tasks.visit(f);
    }
}
