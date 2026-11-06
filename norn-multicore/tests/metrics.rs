//! Every worker's metric sources are collected at startup, include each
//! layer of its park stack, and stay readable after the workers exit.

use std::io;
use std::sync::mpsc;

use norn_executor::park::{Park, ParkMode, ThreadPark};
use norn_executor::ExecutorMetrics;
use norn_metrics::{Kind, Metric, Sources, Visit};
use norn_multicore::{RuntimeGroup, WorkerId};

/// A park layer that keeps one fixed value, standing in for a driver with
/// counters of its own.
struct Labelled {
    inner: ThreadPark,
    label: u64,
}

#[derive(Debug)]
struct Label(u64);

impl Visit for Label {
    fn visit(&self, f: &mut dyn FnMut(Metric)) {
        f(Metric {
            name: "test_label",
            help: "A value set by the test park layer.",
            kind: Kind::Gauge,
            value: self.0,
        });
    }
}

impl Park for Labelled {
    type Unparker = <ThreadPark as Park>::Unparker;
    type Guard = <ThreadPark as Park>::Guard;

    fn park(&mut self, mode: ParkMode) -> io::Result<()> {
        self.inner.park(mode)
    }

    fn enter(&self) -> Self::Guard {
        self.inner.enter()
    }

    fn unparker(&self) -> Self::Unparker {
        self.inner.unparker()
    }

    fn needs_park(&self) -> bool {
        self.inner.needs_park()
    }

    fn shutdown(&mut self) {
        self.inner.shutdown();
    }

    fn metric_sources(&self, sources: &mut Sources) {
        sources.push(Label(self.label));
        self.inner.metric_sources(sources);
    }
}

#[test]
fn group_collects_every_layer_of_every_worker() {
    let mut builder = RuntimeGroup::builder(2);
    let (seen_tx, seen_rx) = mpsc::channel();
    for index in 0..2 {
        let seen_tx = seen_tx.clone();
        builder
            .worker_with(
                WorkerId::new(index),
                move || {
                    Ok::<_, io::Error>(Labelled {
                        inner: ThreadPark::default(),
                        label: 10 + index as u64,
                    })
                },
                move |context| async move {
                    for _ in 0..=index {
                        norn_executor::spawn(async {}).await.unwrap();
                    }
                    let label = context.metrics().get::<Label>().map(|label| label.0);
                    seen_tx.send((context.id(), label)).unwrap();
                },
            )
            .unwrap();
    }
    drop(seen_tx);

    let group = builder.start().unwrap();
    let metrics = group.metrics();
    group.join().unwrap();

    let mut seen: Vec<_> = seen_rx.iter().collect();
    seen.sort_by_key(|(id, _)| id.index());
    assert_eq!(
        seen,
        [(WorkerId::new(0), Some(10)), (WorkerId::new(1), Some(11))]
    );

    assert_eq!(metrics.len(), 2);
    for (index, (id, sources)) in metrics.iter().enumerate() {
        assert_eq!(id.index(), index);
        // The channel driver forwards to the park stack below it.
        assert_eq!(
            sources.get::<Label>().map(|label| label.0),
            Some(10 + index as u64)
        );
        // Counts stay readable after the worker has exited.
        let executor = sources.get::<ExecutorMetrics>().unwrap().snapshot();
        assert_eq!(executor.tasks.spawned, index as u64 + 1);
        assert_eq!(executor.tasks.alive(), 0);
        let mut names = Vec::new();
        sources.visit(&mut |metric| names.push(metric.name));
        assert_eq!(names.first(), Some(&"norn_executor_ticks_total"));
        assert!(names.contains(&"test_label"));
    }
}

#[test]
fn poll_timing_reaches_every_worker() {
    let mut builder = RuntimeGroup::builder(2);
    builder.enable_poll_timing();
    for index in 0..2 {
        builder
            .worker(WorkerId::new(index), |_| async {
                norn_executor::spawn(async {}).await.unwrap();
            })
            .unwrap();
    }
    let group = builder.start().unwrap();
    let metrics = group.metrics();
    group.join().unwrap();
    for (_, sources) in &metrics {
        let times = sources
            .get::<norn_executor::PollTimeMetrics>()
            .unwrap()
            .snapshot();
        assert!(times.count() >= 1, "{times:?}");
    }
}
