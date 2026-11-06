//! Single-writer counters for Norn runtime metrics.
//!
//! Every Norn runtime layer is owned by one thread, so each of its counters
//! has exactly one writer. [`Counter`] and [`Gauge`] use that: the owner
//! updates them with a relaxed load and store rather than an atomic
//! read-modify-write. On x86-64 an increment compiles to the same single
//! `add qword ptr [mem], 1` as a plain integer, with no `lock` prefix, yet
//! any thread may read the value at any time with a relaxed load.
//!
//! # Semantics
//!
//! - **Exact counts.** A single writer cannot lose an update. A second
//!   writing thread would lose updates (but never cause undefined behaviour),
//!   so layers keep their counters private and hand out read-only views.
//! - **Monotonic reads.** One reader never sees a [`Counter`] go backwards:
//!   reads of a single atomic location are coherent even when relaxed.
//! - **No cross-counter atomicity.** A snapshot reads each value at some
//!   instant during the read. Values derived from two counters, such as
//!   `spawned - finished`, can be off by events racing with the read, so
//!   derive them with saturating arithmetic.
//!
//! Layers group their counters in one [`Padded`] block per runtime instance,
//! shared with readers through an [`Arc`]. The block is the only memory the
//! owner writes that readers touch, and padding keeps it off cache lines that
//! belong to other data.
//!
//! # Reading
//!
//! Each layer hands out a `Send + Sync` view of its block. Its snapshot
//! implements [`Visit`], which lets exporters enumerate the values without
//! knowing the layer. [`Sources`] collects the views of every layer in one
//! runtime, and [`Sources::get`] recovers a typed view for typed snapshots.
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub
)]
use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

/// A count kept by one thread and readable from any thread.
///
/// Only the owning thread may call [`Counter::add`] or [`Counter::incr`];
/// see the [crate documentation](crate).
#[derive(Default)]
#[repr(transparent)]
pub struct Counter(AtomicU64);

impl Counter {
    /// A counter at zero.
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Add one. Owner thread only.
    #[inline(always)]
    pub fn incr(&self) {
        self.add(1);
    }

    /// Add `n`. Owner thread only.
    #[inline(always)]
    pub fn add(&self, n: u64) {
        // Not a read-modify-write: with one writer, the load always sees the
        // last store, so no update can be lost.
        self.0.store(self.0.load(Relaxed).wrapping_add(n), Relaxed);
    }

    /// The current count.
    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

impl fmt::Debug for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

/// A level set by one thread and readable from any thread.
///
/// Only the owning thread may call [`Gauge::set`].
#[derive(Default)]
#[repr(transparent)]
pub struct Gauge(AtomicU64);

impl Gauge {
    /// A gauge at zero.
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Set the level. Owner thread only.
    #[inline(always)]
    pub fn set(&self, value: u64) {
        self.0.store(value, Relaxed);
    }

    /// The current level.
    #[inline]
    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

impl fmt::Debug for Gauge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.get().fmt(f)
    }
}

/// Aligns a block of counters to 128 bytes, so that it shares no cache line,
/// or adjacent-line prefetch pair, with unrelated data.
#[derive(Debug, Default)]
#[repr(align(128))]
pub struct Padded<T>(pub T);

impl<T> std::ops::Deref for Padded<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &T {
        &self.0
    }
}

/// Whether a metric only grows or can move both ways.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A monotonic count of events.
    Counter,
    /// A level that can rise and fall.
    Gauge,
}

/// One named value of a snapshot, as handed to exporters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Metric {
    /// A Prometheus-style name, such as `norn_tasks_spawned_total`.
    pub name: &'static str,
    /// One line describing the value.
    pub help: &'static str,
    /// Whether the value is a counter or a gauge.
    pub kind: Kind,
    /// The value.
    pub value: u64,
}

/// A snapshot whose values exporters can enumerate without knowing its type.
pub trait Visit {
    /// Call `f` once per value, in a stable order.
    fn visit(&self, f: &mut dyn FnMut(Metric));
}

/// One layer's view of its counters, readable from any thread.
///
/// Every [`Visit`] type that is `Send + Sync + 'static` is a source; layers
/// implement [`Visit`] on their views by taking a snapshot.
pub trait Source: Visit + Any + Send + Sync {}

impl<T: Visit + Any + Send + Sync> Source for T {}

/// The metric sources of one runtime, one per layer that keeps counters.
///
/// [`Sources::visit`] enumerates every value of every layer for exporters.
/// [`Sources::get`] returns one layer's typed view.
#[derive(Clone, Default)]
pub struct Sources(Vec<Arc<dyn Source>>);

impl Sources {
    /// An empty collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add one layer's source.
    pub fn push<S: Source>(&mut self, source: S) {
        self.0.push(Arc::new(source));
    }

    /// The first source of type `S`.
    pub fn get<S: Source>(&self) -> Option<&S> {
        self.0.iter().find_map(|source| {
            let source: &dyn Any = source.as_ref();
            source.downcast_ref::<S>()
        })
    }

    /// The number of sources.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether there are no sources.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Visit for Sources {
    /// Visit every value of every source, in the order they were added.
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        for source in &self.0 {
            source.visit(f);
        }
    }
}

impl fmt::Debug for Sources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map = f.debug_map();
        self.visit(&mut |metric| {
            map.entry(&metric.name, &metric.value);
        });
        map.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn counter_adds_and_reads() {
        let c = Counter::new();
        c.incr();
        c.add(41);
        assert_eq!(c.get(), 42);
    }

    #[test]
    fn gauge_sets_and_reads() {
        let g = Gauge::new();
        g.set(7);
        g.set(3);
        assert_eq!(g.get(), 3);
    }

    struct Fixed(u64);

    impl Visit for Fixed {
        fn visit(&self, f: &mut dyn FnMut(Metric)) {
            f(Metric {
                name: "fixed",
                help: "A fixed value.",
                kind: Kind::Gauge,
                value: self.0,
            });
        }
    }

    struct Other;

    impl Visit for Other {
        fn visit(&self, f: &mut dyn FnMut(Metric)) {
            f(Metric {
                name: "other",
                help: "Another value.",
                kind: Kind::Counter,
                value: 1,
            });
        }
    }

    #[test]
    fn sources_visit_in_order_and_downcast() {
        let mut sources = Sources::new();
        assert!(sources.is_empty());
        sources.push(Fixed(7));
        sources.push(Other);
        assert_eq!(sources.len(), 2);
        let mut names = Vec::new();
        sources.visit(&mut |metric| names.push((metric.name, metric.value)));
        assert_eq!(names, [("fixed", 7), ("other", 1)]);
        assert_eq!(sources.get::<Fixed>().map(|fixed| fixed.0), Some(7));
        assert!(Sources::new().get::<Fixed>().is_none());
    }

    #[test]
    fn padded_blocks_do_not_share_lines() {
        assert_eq!(std::mem::align_of::<Padded<Counter>>(), 128);
        assert_eq!(std::mem::size_of::<Padded<Counter>>(), 128);
    }

    /// A reader on another thread sees a monotonic sequence ending at the
    /// writer's exact total.
    #[test]
    fn concurrent_reader_sees_monotonic_exact_counts() {
        // Miri runs this about 10,000 times slower.
        const N: u64 = if cfg!(miri) { 1_000 } else { 1_000_000 };
        let block = Arc::new(Padded(Counter::new()));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader = {
            let block = Arc::clone(&block);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                let mut last = 0;
                while !done.load(std::sync::atomic::Ordering::Acquire) {
                    let now = block.get();
                    assert!(now >= last, "counter went backwards: {now} < {last}");
                    last = now;
                }
                block.get()
            })
        };
        for _ in 0..N {
            block.incr();
        }
        done.store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(reader.join().unwrap(), N);
    }
}
