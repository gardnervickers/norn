use std::sync::Arc;

use norn_metrics::{Counter, Kind, Metric, Padded, Visit};

/// Counters written by one timer [`Driver`](crate::Driver)'s thread.
#[derive(Debug, Default)]
pub(crate) struct TimerCounters {
    pub(crate) registered: Counter,
    pub(crate) fired: Counter,
    pub(crate) cancelled: Counter,
    pub(crate) shut_down: Counter,
}

/// A read-only view of a timer wheel's counters.
///
/// This is `Send + Sync`: any thread may take a
/// [`snapshot`](TimerMetrics::snapshot) at any time.
#[derive(Clone)]
pub struct TimerMetrics(pub(crate) Arc<Padded<TimerCounters>>);

impl std::fmt::Debug for TimerMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl Visit for TimerMetrics {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        self.snapshot().visit(f);
    }
}

impl TimerMetrics {
    /// Read the current counts.
    pub fn snapshot(&self) -> TimerSnapshot {
        TimerSnapshot {
            registered: self.0.registered.get(),
            fired: self.0.fired.get(),
            cancelled: self.0.cancelled.get(),
            shut_down: self.0.shut_down.get(),
        }
    }
}

/// Timer counts read from a [`TimerMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TimerSnapshot {
    /// Sleeps inserted into the wheel. A sleep that is already due when first
    /// polled completes without being inserted.
    pub registered: u64,
    /// Inserted sleeps that reached their deadline.
    pub fired: u64,
    /// Inserted sleeps removed before their deadline, by drop or reset.
    pub cancelled: u64,
    /// Inserted sleeps completed with an error by driver shutdown.
    pub shut_down: u64,
}

impl TimerSnapshot {
    /// Sleeps in the wheel. The counts are read separately, so this
    /// saturates at zero.
    pub fn pending(&self) -> u64 {
        self.registered
            .saturating_sub(self.fired)
            .saturating_sub(self.cancelled)
            .saturating_sub(self.shut_down)
    }
}

impl Visit for TimerSnapshot {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        f(Metric {
            name: "norn_timer_registered_total",
            help: "Sleeps inserted into the timer wheel.",
            kind: Kind::Counter,
            value: self.registered,
        });
        f(Metric {
            name: "norn_timer_fired_total",
            help: "Sleeps that reached their deadline.",
            kind: Kind::Counter,
            value: self.fired,
        });
        f(Metric {
            name: "norn_timer_cancelled_total",
            help: "Sleeps removed from the wheel before their deadline.",
            kind: Kind::Counter,
            value: self.cancelled,
        });
        f(Metric {
            name: "norn_timer_pending",
            help: "Sleeps waiting in the timer wheel.",
            kind: Kind::Gauge,
            value: self.pending(),
        });
    }
}
