use std::sync::Arc;

use norn_metrics::{Counter, Kind, Metric, Padded, Visit};

/// Counters written by one channel [`Driver`](crate::Driver)'s thread.
#[derive(Debug, Default)]
pub(crate) struct ChannelCounters {
    pub(crate) deliveries: Counter,
    pub(crate) receivers_woken: Counter,
}

/// A read-only view of a channel driver's counters.
///
/// This is `Send + Sync`: any thread may take a
/// [`snapshot`](ChannelMetrics::snapshot) at any time.
#[derive(Clone)]
pub struct ChannelMetrics(pub(crate) Arc<Padded<ChannelCounters>>);

impl std::fmt::Debug for ChannelMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.snapshot().fmt(f)
    }
}

impl Visit for ChannelMetrics {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        self.snapshot().visit(f);
    }
}

impl ChannelMetrics {
    /// Read the current counts.
    pub fn snapshot(&self) -> ChannelSnapshot {
        ChannelSnapshot {
            deliveries: self.0.deliveries.get(),
            receivers_woken: self.0.receivers_woken.get(),
        }
    }
}

/// Counts read from a [`ChannelMetrics`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ChannelSnapshot {
    /// Times a park found the driver's pending flag set and woke the ready
    /// receivers. A send from any thread sets the flag, and so do a sender's
    /// drop and a receiver's attach; sends that arrive together share one
    /// delivery, so this counts deliveries, not messages.
    pub deliveries: u64,
    /// Receiver wakers those deliveries invoked.
    pub receivers_woken: u64,
}

impl Visit for ChannelSnapshot {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        f(Metric {
            name: "norn_channel_deliveries_total",
            help: "Parks that delivered pending channel messages to receivers.",
            kind: Kind::Counter,
            value: self.deliveries,
        });
        f(Metric {
            name: "norn_channel_receivers_woken_total",
            help: "Receiver wakers woken by those deliveries.",
            kind: Kind::Counter,
            value: self.receivers_woken,
        });
    }
}
