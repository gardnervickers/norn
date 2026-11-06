//! Swappable counter backends for measuring the cost of runtime metrics on
//! norn's hot paths. Each runtime layer keeps its counters in a [`Block`] and
//! increments them at the same sites whatever the backend.

#[cfg(any(feature = "sw", feature = "rmw", feature = "sw-inline"))]
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// Storage for a layer's counters: shared with readers through an `Arc`,
/// except for the `cell` backend, whose counters stay inline and owner-only.
#[cfg(not(any(feature = "cell", feature = "sw-inline", feature = "cell-arc")))]
pub struct Block<T>(std::sync::Arc<T>);
#[cfg(any(feature = "cell", feature = "sw-inline"))]
pub struct Block<T>(T);
#[cfg(feature = "cell-arc")]
pub struct Block<T>(Box<T>);

impl<T> Block<T> {
    pub fn new(value: T) -> Self {
        #[cfg(not(any(feature = "cell", feature = "sw-inline", feature = "cell-arc")))]
        return Self(std::sync::Arc::new(value));
        #[cfg(any(feature = "cell", feature = "sw-inline"))]
        return Self(value);
        #[cfg(feature = "cell-arc")]
        return Self(Box::new(value));
    }
}

impl<T> std::ops::Deref for Block<T> {
    type Target = T;
    #[inline(always)]
    fn deref(&self) -> &T {
        &self.0
    }
}

pub struct Counter {
    #[cfg(any(feature = "sw", feature = "rmw", feature = "sw-inline"))]
    value: AtomicU64,
    #[cfg(any(feature = "cell", feature = "cell-arc"))]
    value: std::cell::Cell<u64>,
    #[cfg(feature = "facade")]
    name: &'static str,
    #[cfg(feature = "facade-cached")]
    handle: metrics::Counter,
    #[cfg(feature = "otel")]
    handle: opentelemetry::metrics::Counter<u64>,
    #[cfg(feature = "otel-attr")]
    handle: (
        opentelemetry::metrics::Counter<u64>,
        [opentelemetry::KeyValue; 1],
    ),
    #[cfg(feature = "otel-bound")]
    handle: opentelemetry::metrics::BoundCounter<u64>,
}

impl Counter {
    #[allow(unused_variables)]
    pub fn new(name: &'static str) -> Self {
        Self {
            #[cfg(any(feature = "sw", feature = "rmw", feature = "sw-inline"))]
            value: AtomicU64::new(0),
            #[cfg(any(feature = "cell", feature = "cell-arc"))]
            value: std::cell::Cell::new(0),
            #[cfg(feature = "facade")]
            name,
            #[cfg(feature = "facade-cached")]
            handle: metrics::counter!(name, "worker" => "0"),
            #[cfg(feature = "otel")]
            handle: otel_counter(name),
            #[cfg(feature = "otel-attr")]
            handle: (
                otel_counter(name),
                [opentelemetry::KeyValue::new("worker", 0i64)],
            ),
            #[cfg(feature = "otel-bound")]
            handle: otel_counter(name).bind(&[opentelemetry::KeyValue::new("worker", 0i64)]),
        }
    }

    #[inline(always)]
    pub fn incr(&self) {
        self.add(1);
    }

    #[inline(always)]
    #[allow(unused_variables)]
    pub fn add(&self, n: u64) {
        #[cfg(any(feature = "sw", feature = "sw-inline"))]
        self.value
            .store(self.value.load(Relaxed).wrapping_add(n), Relaxed);
        #[cfg(feature = "rmw")]
        self.value.fetch_add(n, Relaxed);
        #[cfg(any(feature = "cell", feature = "cell-arc"))]
        self.value.set(self.value.get().wrapping_add(n));
        #[cfg(feature = "facade")]
        metrics::counter!(self.name, "worker" => "0").increment(n);
        #[cfg(feature = "facade-cached")]
        self.handle.increment(n);
        #[cfg(feature = "otel")]
        self.handle.add(n, &[]);
        #[cfg(feature = "otel-attr")]
        self.handle.0.add(n, &self.handle.1);
        #[cfg(feature = "otel-bound")]
        self.handle.add(n);
    }

    /// The count, where the backend can report it.
    pub fn get(&self) -> u64 {
        #[cfg(any(feature = "sw", feature = "rmw", feature = "sw-inline"))]
        return self.value.load(Relaxed);
        #[cfg(any(feature = "cell", feature = "cell-arc"))]
        return self.value.get();
        #[allow(unreachable_code)]
        0
    }
}

#[cfg(any(feature = "otel", feature = "otel-attr", feature = "otel-bound"))]
fn otel_counter(name: &'static str) -> opentelemetry::metrics::Counter<u64> {
    opentelemetry::global::meter("norn")
        .u64_counter(name)
        .build()
}

/// Install the recorder or meter provider the backend reports to. Call once
/// before building a runtime.
pub fn install() {
    #[cfg(any(feature = "facade", feature = "facade-cached"))]
    {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
        let _ = PROMETHEUS.set(recorder.handle());
        metrics::set_global_recorder(recorder).unwrap();
    }
    #[cfg(any(feature = "otel", feature = "otel-attr", feature = "otel-bound"))]
    {
        let reader = otel::SharedReader(std::sync::Arc::new(
            opentelemetry_sdk::metrics::ManualReader::builder().build(),
        ));
        let _ = otel::READER.set(reader.clone());
        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_reader(reader)
            .build();
        opentelemetry::global::set_meter_provider(provider);
    }
}

/// A rendering of what the backend recorded, to confirm the counters ran.
pub fn report() -> String {
    #[cfg(any(feature = "facade", feature = "facade-cached"))]
    return PROMETHEUS
        .get()
        .map(|h| h.render())
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.starts_with('#'))
        .collect::<Vec<_>>()
        .join("\n");
    #[cfg(any(feature = "otel", feature = "otel-attr", feature = "otel-bound"))]
    return otel::report();
    #[allow(unreachable_code)]
    String::new()
}

#[cfg(any(feature = "facade", feature = "facade-cached"))]
static PROMETHEUS: std::sync::OnceLock<metrics_exporter_prometheus::PrometheusHandle> =
    std::sync::OnceLock::new();

#[cfg(any(feature = "otel", feature = "otel-attr", feature = "otel-bound"))]
mod otel {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use std::sync::Arc;

    pub(crate) static READER: std::sync::OnceLock<SharedReader> = std::sync::OnceLock::new();

    #[derive(Debug, Clone)]
    pub(crate) struct SharedReader(pub(crate) Arc<opentelemetry_sdk::metrics::ManualReader>);

    impl MetricReader for SharedReader {
        fn register_pipeline(
            &self,
            pipeline: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>,
        ) {
            self.0.register_pipeline(pipeline)
        }
        fn collect(&self, rm: &mut ResourceMetrics) -> opentelemetry_sdk::error::OTelSdkResult {
            self.0.collect(rm)
        }
        fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
            self.0.force_flush()
        }
        fn shutdown_with_timeout(
            &self,
            timeout: std::time::Duration,
        ) -> opentelemetry_sdk::error::OTelSdkResult {
            self.0.shutdown_with_timeout(timeout)
        }
        fn temporality(
            &self,
            kind: opentelemetry_sdk::metrics::InstrumentKind,
        ) -> opentelemetry_sdk::metrics::Temporality {
            self.0.temporality(kind)
        }
    }

    pub(crate) fn report() -> String {
        let mut rm = ResourceMetrics::default();
        READER.get().unwrap().collect(&mut rm).unwrap();
        let mut out = Vec::new();
        for scope in rm.scope_metrics() {
            for metric in scope.metrics() {
                if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                    let total: u64 = sum.data_points().map(|p| p.value()).sum();
                    out.push(format!("{} {}", metric.name(), total));
                }
            }
        }
        out.join("\n")
    }
}
