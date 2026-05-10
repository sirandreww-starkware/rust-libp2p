//! `tracing_subscriber::Layer` that records the wall-clock duration of every
//! poll on a named `#[tracing::instrument]` span into a shared
//! `hdrhistogram`. Used by the central role to time both `Connection::poll`
//! (span name `"Connection::poll"` from `swarm/src/connection.rs`) and
//! `Swarm::poll_next_event` (span name `"Swarm::poll"` from
//! `swarm/src/lib.rs`).

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use hdrhistogram::Histogram;
use tracing::Subscriber;
use tracing_subscriber::{Layer, layer::Context, registry::LookupSpan};

/// Tokio's cooperative-scheduling guideline: a single poll should return
/// within ~50 µs so the worker can drive other tasks. Polls exceeding
/// this are counted as slow polls — same threshold tokio_metrics ships
/// for `TaskMonitor::with_slow_poll_threshold`.
pub const SLOW_POLL_THRESHOLD_NANOS: u64 = 50_000;

#[derive(Clone)]
pub struct PollDurations {
    histogram: Arc<Mutex<Histogram<u64>>>,
    slow_count: Arc<AtomicU64>,
}

impl PollDurations {
    pub fn new() -> Self {
        Self {
            histogram: Arc::new(Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                    .expect("hdrhistogram bounds valid"),
            )),
            slow_count: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn record_nanos(&self, nanos: u64) {
        if nanos > SLOW_POLL_THRESHOLD_NANOS {
            self.slow_count.fetch_add(1, Ordering::Relaxed);
        }
        let mut h = self.histogram.lock().expect("poll histogram mutex poisoned");
        let high = h.high();
        let _ = h.record(nanos.clamp(1, high));
    }

    pub fn snapshot(&self) -> Histogram<u64> {
        self.histogram
            .lock()
            .expect("poll histogram mutex poisoned")
            .clone()
    }

    pub fn slow_count_snapshot(&self) -> u64 {
        self.slow_count.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.histogram
            .lock()
            .expect("poll histogram mutex poisoned")
            .reset();
        self.slow_count.store(0, Ordering::Relaxed);
    }
}

impl Default for PollDurations {
    fn default() -> Self {
        Self::new()
    }
}

// Helper extension type. Uniquified per layer by the layer's distinct span
// name — two PollTimer layers with different `span_name` fields will store
// independent extensions on the same span (the extension key is the
// `TypeId`, which is the same here, but we filter by span name before
// touching the extensions so the layers don't collide on shared spans).
struct Timestamp(Instant);

pub struct PollTimer {
    span_name: &'static str,
    durations: PollDurations,
}

impl PollTimer {
    pub fn new(span_name: &'static str, durations: PollDurations) -> Self {
        Self {
            span_name,
            durations,
        }
    }
}

impl<S> Layer<S> for PollTimer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_enter(&self, id: &tracing::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if span.name() != self.span_name {
            return;
        }
        let mut ext = span.extensions_mut();
        if ext.get_mut::<Timestamp>().is_none() {
            ext.insert(Timestamp(Instant::now()));
        }
    }

    fn on_exit(&self, id: &tracing::Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        if span.name() != self.span_name {
            return;
        }
        let started = {
            let mut ext = span.extensions_mut();
            ext.remove::<Timestamp>()
        };
        if let Some(Timestamp(start)) = started {
            let elapsed = start.elapsed();
            self.durations
                .record_nanos(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
        }
    }
}
