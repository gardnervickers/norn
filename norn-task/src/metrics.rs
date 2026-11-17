use std::sync::Arc;

use norn_metrics::{Counter, Kind, Metric, Padded, Visit};

/// Counters written by one [`TaskQueue`](crate::TaskQueue)'s thread.
#[derive(Debug, Default)]
pub(crate) struct TaskCounters {
    pub(crate) spawned: Counter,
    pub(crate) finished: Counter,
}

/// A read-only view of a [`TaskQueue`](crate::TaskQueue)'s task counts.
///
/// Unlike the queue, this is `Send + Sync`: any thread may take a
/// [`snapshot`](TaskMetrics::snapshot) at any time, including after the queue
/// is gone.
#[derive(Clone)]
pub struct TaskMetrics(pub(crate) Arc<Padded<TaskCounters>>);

impl std::fmt::Debug for TaskMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl Visit for TaskMetrics {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        self.snapshot().visit(f);
    }
}

impl TaskMetrics {
    /// Read the current counts.
    pub fn snapshot(&self) -> TaskSnapshot {
        TaskSnapshot {
            spawned: self.0.spawned.get(),
            finished: self.0.finished.get(),
        }
    }
}

/// Task counts read from a [`TaskMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TaskSnapshot {
    /// Tasks accepted by the queue.
    pub spawned: u64,
    /// Tasks that left the queue: completed, panicked or cancelled.
    pub finished: u64,
}

impl TaskSnapshot {
    /// Tasks spawned but not yet finished.
    ///
    /// The two counts are read separately, so this saturates at zero.
    pub fn alive(&self) -> u64 {
        self.spawned.saturating_sub(self.finished)
    }
}

impl Visit for TaskSnapshot {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        f(Metric {
            name: "norn_tasks_spawned_total",
            help: "Tasks accepted by the task queue.",
            kind: Kind::Counter,
            value: self.spawned,
        });
        f(Metric {
            name: "norn_tasks_finished_total",
            help: "Tasks that completed, panicked or were cancelled.",
            kind: Kind::Counter,
            value: self.finished,
        });
        f(Metric {
            name: "norn_tasks_alive",
            help: "Tasks spawned and not yet finished.",
            kind: Kind::Gauge,
            value: self.alive(),
        });
    }
}
