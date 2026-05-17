//! Central role: dial all peers, run closed-loop request-response, record per-peer
//! e2e latency + Connection::poll p99 + tokio TaskMonitor stats. Write JSON metrics on exit.

use std::{
    collections::HashMap,
    error::Error,
    path::PathBuf,
    time::{Duration, Instant},
};

use futures::{FutureExt, StreamExt, future::BoxFuture};
use hdrhistogram::Histogram;
use libp2p_core::Multiaddr;
use libp2p_identity::PeerId;
use libp2p_request_response as request_response;
use libp2p_swarm::SwarmEvent;
use tokio_metrics::TaskMonitor;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

use crate::{
    metrics::{
        CentralMetrics, HistSummary, PeerOutcome, RunConfig, RunMetrics, TaskMonitorSummary,
    },
    poll_timer::{PollDurations, PollTimer},
    swarm::build_swarm_with_executor,
};

#[derive(Debug, clap::Args)]
pub struct CentralArgs {
    /// Multiaddr of a peer to dial. Repeat once per peer; order matters
    /// because peer index 0 is treated as the "fast" peer (see `--fast-peer-tag`).
    #[arg(long = "peer", value_name = "MULTIADDR")]
    pub peers: Vec<String>,

    /// Payload size in bytes for every request and echoed response.
    #[arg(long, default_value_t = 4096)]
    pub payload: usize,

    /// Outstanding requests per peer (closed-loop concurrency).
    #[arg(long, default_value_t = 2)]
    pub concurrency_per_peer: usize,

    /// Warmup duration in seconds (samples discarded).
    #[arg(long, default_value_t = 2.0)]
    pub warmup_secs: f64,

    /// Measurement window in seconds.
    #[arg(long, default_value_t = 20.0)]
    pub measurement_secs: f64,

    /// Path to write the run's JSON metrics to.
    #[arg(long)]
    pub metrics_out: PathBuf,

    /// Tag for peer index 0. Anything else is tagged "slow".
    #[arg(long, default_value = "fast")]
    pub fast_peer_tag: String,

    /// Tokio worker thread count for this process. Default 1 forces every
    /// per-connection task to time-share a single worker — surfaces
    /// head-of-line blocking on the runtime that 16+-core multi-threaded
    /// runtimes would otherwise hide.
    #[arg(long, default_value_t = 1)]
    pub worker_threads: usize,
}

/// `libp2p_swarm::Executor` that wraps every spawned connection task with a shared
/// `TaskMonitor`. Aggregating monitor stats over the measurement window gives us
/// the per-connection-task scheduler/poll behavior with no privileges, no
/// `tokio_unstable` cfg, and no ad-hoc sleep-based probes.
#[derive(Clone)]
struct InstrumentingExecutor {
    monitor: TaskMonitor,
}

impl libp2p_swarm::Executor for InstrumentingExecutor {
    fn exec(&self, future: BoxFuture<'static, ()>) {
        // `TaskMonitor::instrument` accepts any `Future`; box-pinning around it
        // keeps the spawned future `'static + Send`.
        let instrumented = self.monitor.instrument(future).boxed();
        tokio::spawn(instrumented);
    }
}

pub async fn run(
    args: CentralArgs,
    central_monitor: TaskMonitor,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if args.peers.is_empty() {
        return Err("--peer must be specified at least once".into());
    }

    let connection_poll_durations = PollDurations::new();
    let swarm_poll_durations = PollDurations::new();
    install_subscriber(
        connection_poll_durations.clone(),
        swarm_poll_durations.clone(),
    );

    // `connection_monitor` wraps every libp2p-spawned task (the per-peer
    // connection tasks). `central_monitor` is created in `main.rs` and wraps
    // the future containing this very function — the swarm event loop +
    // closed-loop driver — so we can compare per-task vs per-central-loop
    // poll/scheduler latencies separately.
    let connection_monitor = TaskMonitor::new();
    let executor = InstrumentingExecutor {
        monitor: connection_monitor.clone(),
    };

    let peer_addrs: Vec<Multiaddr> = args
        .peers
        .iter()
        .map(|s| s.parse::<Multiaddr>())
        .collect::<Result<_, _>>()
        .map_err(|e| format!("invalid --peer multiaddr: {e}"))?;

    let peer_ids: Vec<PeerId> = peer_addrs
        .iter()
        .map(|addr| {
            addr.iter()
                .find_map(|p| match p {
                    libp2p_core::multiaddr::Protocol::P2p(id) => Some(id),
                    _ => None,
                })
                .ok_or_else(|| format!("multiaddr missing /p2p/<peer_id>: {addr}"))
        })
        .collect::<Result<_, _>>()?;

    let mut swarm = build_swarm_with_executor(executor);

    let mut pending: usize = peer_addrs.len();
    for (peer_id, addr) in peer_ids.iter().zip(peer_addrs.iter()) {
        let opts = libp2p_swarm::dial_opts::DialOpts::peer_id(*peer_id)
            .addresses(vec![addr.clone()])
            .build();
        swarm.dial(opts)?;
    }
    while pending > 0 {
        match swarm.next().await {
            Some(SwarmEvent::ConnectionEstablished { peer_id, .. }) => {
                if peer_ids.contains(&peer_id) {
                    pending -= 1;
                }
            }
            Some(SwarmEvent::OutgoingConnectionError { error, .. }) => {
                return Err(format!("dial failed: {error}").into());
            }
            Some(_) => {}
            None => return Err("swarm terminated".into()),
        }
    }

    let payload = vec![0u8; args.payload];
    let warmup = Duration::from_secs_f64(args.warmup_secs);
    let measurement = Duration::from_secs_f64(args.measurement_secs);
    let warmup_until = Instant::now() + warmup;
    let measurement_until = warmup_until + measurement;
    let concurrency = args.concurrency_per_peer;

    let mut outstanding: HashMap<PeerId, usize> = peer_ids.iter().map(|p| (*p, 0)).collect();
    let mut peer_e2e: HashMap<PeerId, Histogram<u64>> = peer_ids
        .iter()
        .map(|p| {
            (
                *p,
                Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)
                    .expect("hdrhistogram bounds valid"),
            )
        })
        .collect();
    let mut peer_completed: HashMap<PeerId, u64> = peer_ids.iter().map(|p| (*p, 0)).collect();
    let mut sent_at: HashMap<request_response::OutboundRequestId, (PeerId, Instant)> =
        HashMap::new();

    // Acquire one `intervals()` iterator per monitor before the run starts.
    // `next()` at warmup_end demarcates the boundary (and discards the
    // warmup-window sample); `next()` at measurement_end captures
    // measurement-window-only stats.
    let mut conn_intervals = connection_monitor.intervals();
    let mut central_intervals = central_monitor.intervals();
    let mut warmup_marked = false;

    for peer_id in &peer_ids {
        for _ in 0..concurrency {
            let id = swarm.behaviour_mut().send_request(peer_id, payload.clone());
            sent_at.insert(id, (*peer_id, Instant::now()));
            *outstanding.get_mut(peer_id).unwrap() += 1;
        }
    }

    loop {
        let now = Instant::now();
        if now >= measurement_until {
            break;
        }
        if !warmup_marked && now >= warmup_until {
            warmup_marked = true;
            connection_poll_durations.reset();
            swarm_poll_durations.reset();
            // Discard the warmup-window TaskMonitor samples so the next
            // `next()` returns only measurement-window stats.
            let _ = conn_intervals.next();
            let _ = central_intervals.next();
            sent_at.clear();
            for v in outstanding.values_mut() {
                *v = 0;
            }
            for h in peer_e2e.values_mut() {
                h.reset();
            }
            for c in peer_completed.values_mut() {
                *c = 0;
            }
            for peer_id in &peer_ids {
                for _ in 0..concurrency {
                    let id = swarm.behaviour_mut().send_request(peer_id, payload.clone());
                    sent_at.insert(id, (*peer_id, Instant::now()));
                    *outstanding.get_mut(peer_id).unwrap() += 1;
                }
            }
        }

        let remaining = measurement_until.saturating_duration_since(now);
        let event = match tokio::time::timeout(remaining, swarm.next()).await {
            Ok(Some(ev)) => ev,
            Ok(None) | Err(_) => break,
        };

        if let SwarmEvent::Behaviour(request_response::Event::Message {
            message: request_response::Message::Response { request_id, .. },
            peer,
            ..
        }) = event
        {
            if let Some((sent_peer, t)) = sent_at.remove(&request_id) {
                debug_assert_eq!(sent_peer, peer);
                if warmup_marked {
                    if let Some(h) = peer_e2e.get_mut(&peer) {
                        let elapsed = t.elapsed();
                        let high = h.high();
                        let _ = h.record(
                            u64::try_from(elapsed.as_nanos())
                                .unwrap_or(u64::MAX)
                                .clamp(1, high),
                        );
                    }
                    if let Some(c) = peer_completed.get_mut(&peer) {
                        *c += 1;
                    }
                }
                if let Some(c) = outstanding.get_mut(&peer) {
                    *c = c.saturating_sub(1);
                }
            }
            if let Some(c) = outstanding.get_mut(&peer) {
                while *c < concurrency && Instant::now() < measurement_until {
                    let id = swarm.behaviour_mut().send_request(&peer, payload.clone());
                    sent_at.insert(id, (peer, Instant::now()));
                    *c += 1;
                }
            }
        }
    }

    // Capture TaskMonitor stats for the measurement window only.
    let connection_summary = conn_intervals
        .next()
        .map(summarize_task_monitor)
        .unwrap_or_default();
    let central_summary = central_intervals
        .next()
        .map(summarize_task_monitor)
        .unwrap_or_default();

    let measurement_secs = measurement.as_secs_f64();
    let peers_out: Vec<PeerOutcome> = peer_ids
        .iter()
        .enumerate()
        .map(|(idx, p)| {
            let completed = *peer_completed.get(p).unwrap_or(&0);
            PeerOutcome {
                index: idx,
                tag: if idx == 0 {
                    args.fast_peer_tag.clone()
                } else {
                    "slow".to_string()
                },
                e2e_latency: HistSummary::from_hist_nanos(&peer_e2e[p]),
                completed,
                throughput_rps: completed as f64 / measurement_secs,
            }
        })
        .collect();

    let out = RunMetrics {
        config: RunConfig {
            n_peers: peer_ids.len(),
            payload_bytes: args.payload,
            concurrency_per_peer: concurrency,
            warmup_secs: args.warmup_secs,
            measurement_secs: args.measurement_secs,
        },
        central: CentralMetrics {
            connection_poll: HistSummary::from_hist_nanos(&connection_poll_durations.snapshot())
                .with_slow_count(connection_poll_durations.slow_count_snapshot()),
            swarm_poll: HistSummary::from_hist_nanos(&swarm_poll_durations.snapshot())
                .with_slow_count(swarm_poll_durations.slow_count_snapshot()),
            central_task: central_summary,
            connection_tasks: connection_summary,
        },
        peers: peers_out,
    };

    let json = serde_json::to_string_pretty(&out)?;
    std::fs::write(&args.metrics_out, json)?;
    Ok(())
}

fn summarize_task_monitor(m: tokio_metrics::TaskMetrics) -> TaskMonitorSummary {
    let to_us = |d: Duration| u64::try_from(d.as_micros()).unwrap_or(u64::MAX);
    TaskMonitorSummary {
        total_poll_count: m.total_poll_count,
        total_poll_duration_us: to_us(m.total_poll_duration),
        mean_poll_duration_us: to_us(m.mean_poll_duration()),
        total_slow_poll_count: m.total_slow_poll_count,
        mean_slow_poll_duration_us: to_us(m.mean_slow_poll_duration()),
        total_scheduled_duration_us: to_us(m.total_scheduled_duration),
        mean_scheduled_duration_us: to_us(m.mean_scheduled_duration()),
        total_scheduled_count: m.total_scheduled_count,
    }
}

fn install_subscriber(connection_poll: PollDurations, swarm_poll: PollDurations) {
    use tracing_subscriber::filter::Targets;
    let filter = Targets::new()
        .with_target("libp2p_swarm", tracing::Level::DEBUG)
        .with_default(tracing::Level::WARN);
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(PollTimer::new("Connection::poll", connection_poll))
        .with(PollTimer::new("Swarm::poll", swarm_poll))
        .try_init();
}
