//! Serializable metric types shared between the central node and the orchestrator.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

/// Summary statistics extracted from an `hdrhistogram::Histogram<u64>` of microseconds.
///
/// `slow_count` is only populated for the poll-duration histograms
/// (`connection_poll`, `swarm_poll`) — counts samples that exceeded the
/// 50 µs cooperative-scheduling threshold. Defaults to 0 for histograms
/// where the concept doesn't apply (e.g. per-peer e2e latency).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HistSummary {
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub samples: u64,
    #[serde(default)]
    pub slow_count: u64,
}

impl HistSummary {
    pub fn from_hist_nanos(h: &Histogram<u64>) -> Self {
        let to_us = |ns: u64| ns / 1000;
        Self {
            p50_us: to_us(h.value_at_quantile(0.50)),
            p95_us: to_us(h.value_at_quantile(0.95)),
            p99_us: to_us(h.value_at_quantile(0.99)),
            max_us: to_us(h.max()),
            samples: h.len(),
            slow_count: 0,
        }
    }

    pub fn with_slow_count(mut self, slow_count: u64) -> Self {
        self.slow_count = slow_count;
        self
    }
}

/// Selected fields from `tokio_metrics::TaskMetrics` for the connection tasks
/// the central swarm spawned during the measurement window. Captured via
/// `TaskMonitor::intervals().next()` (interval-aggregated, not cumulative).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TaskMonitorSummary {
    /// Number of times an instrumented task was polled in the window.
    pub total_poll_count: u64,
    /// Total wall-clock time the runtime spent polling the instrumented tasks.
    pub total_poll_duration_us: u64,
    /// Mean polling duration (== total_poll_duration / total_poll_count).
    pub mean_poll_duration_us: u64,
    /// Polls whose duration exceeded TaskMonitor's slow-poll threshold
    /// (`tokio_metrics::TaskMonitor::DEFAULT_SLOW_POLL_THRESHOLD` = 50 µs).
    pub total_slow_poll_count: u64,
    /// Mean duration of slow polls only.
    pub mean_slow_poll_duration_us: u64,
    /// Total time tasks spent in the "scheduled" state (woken-but-not-yet-polled).
    /// This is the canonical scheduler-latency signal.
    pub total_scheduled_duration_us: u64,
    /// Mean scheduled duration.
    pub mean_scheduled_duration_us: u64,
    /// Number of times tasks entered the scheduled state.
    pub total_scheduled_count: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CentralMetrics {
    /// Histogram of `Connection::poll` invocation durations (one per
    /// per-connection task, aggregated across all connections on the
    /// central side). From a tracing-layer hook on the
    /// `#[tracing::instrument(name = "Connection::poll")]` span in
    /// `swarm/src/connection.rs`.
    pub connection_poll: HistSummary,
    /// Histogram of `Swarm::poll_next_event` invocation durations on the
    /// central swarm. From a tracing-layer hook on the
    /// `#[tracing::instrument(name = "Swarm::poll")]` span in
    /// `swarm/src/lib.rs`. This measures the swarm event-loop's poll
    /// cost — the outer driver that calls into `Connection::poll` etc.
    #[serde(default)]
    pub swarm_poll: HistSummary,
    /// TaskMonitor stats for the central process's own main task — the
    /// future polled by `rt.block_on`, containing the swarm event loop,
    /// dial bookkeeping, and the closed-loop request driver.
    pub central_task: TaskMonitorSummary,
    /// TaskMonitor stats aggregated across every per-connection task
    /// that libp2p-swarm spawned during the run (one per peer).
    pub connection_tasks: TaskMonitorSummary,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerOutcome {
    /// 0..N − 1 peer index as the orchestrator assigned it.
    pub index: usize,
    /// "fast" or "slow"; the fast peer always has index 0 in mixed-RTT mode.
    pub tag: String,
    pub e2e_latency: HistSummary,
    pub completed: u64,
    pub throughput_rps: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunConfig {
    pub n_peers: usize,
    pub payload_bytes: usize,
    pub concurrency_per_peer: usize,
    pub warmup_secs: f64,
    pub measurement_secs: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunMetrics {
    pub config: RunConfig,
    pub central: CentralMetrics,
    pub peers: Vec<PeerOutcome>,
}
