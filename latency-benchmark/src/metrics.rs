//! Serializable metric types: the per-scenario record the client emits and the
//! runner collects.

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

/// Summary statistics extracted from an `hdrhistogram::Histogram<u64>` of poll
/// **thread-CPU** times in nanos, reported in microseconds as `f64` (sub-µs
/// precision — the poll medians sit around single-digit µs, where integer
/// rounding would quantise the signal away). `slow_count` counts samples over
/// the 50 µs cooperative-scheduling threshold (informational; the percentiles
/// are the headline).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct HistSummary {
    pub mean_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p95_us: f64,
    pub p99_us: f64,
    pub max_us: f64,
    pub samples: u64,
    #[serde(default)]
    pub slow_count: u64,
}

impl HistSummary {
    pub fn from_hist_nanos(h: &Histogram<u64>) -> Self {
        let to_us = |ns: f64| ns / 1000.0;
        Self {
            mean_us: to_us(h.mean()),
            p50_us: to_us(h.value_at_quantile(0.50) as f64),
            p90_us: to_us(h.value_at_quantile(0.90) as f64),
            p95_us: to_us(h.value_at_quantile(0.95) as f64),
            p99_us: to_us(h.value_at_quantile(0.99) as f64),
            max_us: to_us(h.max() as f64),
            samples: h.len(),
            slow_count: 0,
        }
    }

    pub fn with_slow_count(mut self, slow_count: u64) -> Self {
        self.slow_count = slow_count;
        self
    }
}

/// One row of the benchmark: a single (rtt, msg_size, heartbeat) point. The
/// client emits this as JSON; the runner collects all of them.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScenarioRecord {
    pub rtt_ms: u64,
    pub msg_size: usize,
    pub heartbeat_ms: u64,
    /// `Connection::poll` **thread-CPU time** distribution — the work a poll
    /// does. The headline.
    pub connection_poll: HistSummary,
    /// `Swarm::poll` thread-CPU time (the outer event loop; for context).
    pub swarm_poll: HistSummary,
    /// CPU time of the `handler.poll` sub-phase inside `Connection::poll`.
    #[serde(default)]
    pub handler_poll: HistSummary,
    /// CPU time of the `muxer.poll` sub-phase inside `Connection::poll`.
    #[serde(default)]
    pub muxer_poll: HistSummary,
    /// Achieved round-trip goodput (echoed payload received) over the
    /// measurement window, in MB/s.
    #[serde(default)]
    pub recv_mbps: f64,
    /// Application round-trip latency: each message carries an 8-byte send
    /// timestamp; the client records `now - timestamp` when the echo returns.
    /// Wall-clock real on-wire latency (incl. queueing under load), unlike the
    /// CPU-time poll metrics above.
    #[serde(default)]
    pub wire_rtt: HistSummary,
}
