//! Client role: dial one server, open **one** long-lived `libp2p-stream`, send a
//! fixed-size message every `heartbeat_ms` (open-loop), drain the echoes, and
//! record `Connection::poll` / `Swarm::poll` durations over the measurement
//! window. Writes a single [`ScenarioRecord`] JSON file on exit.
//!
//! This is the half of the 2-node workload that does the measuring; the server
//! ([`crate::peer`]) just echoes. The headline metric is `Connection::poll`
//! duration — how long a single poll monopolises the worker under a high-RTT
//! inbound burst.

use std::{
    error::Error,
    path::PathBuf,
    time::{Duration, Instant},
};

use futures::{AsyncReadExt as _, StreamExt};
use libp2p_core::Multiaddr;
use libp2p_identity::PeerId;
use libp2p_swarm::{SwarmEvent, dial_opts::DialOpts};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _};

use crate::{
    metrics::{HistSummary, ScenarioRecord},
    poll_timer::{PollDurations, PollTimer},
    swarm::{STREAM_PROTOCOL, build_stream_swarm, read_framed, write_framed},
};

#[derive(Debug, clap::Args)]
pub struct ClientArgs {
    /// Multiaddr of the server to dial (one per scenario).
    #[arg(long)]
    pub peer: String,

    /// Message size in bytes sent every heartbeat (and echoed back).
    #[arg(long, default_value_t = 262144)]
    pub msg_size: usize,

    /// Send one message every this many milliseconds (open-loop). `0` = send as
    /// fast as the link/flow-control allows (saturating).
    #[arg(long, default_value_t = 10)]
    pub heartbeat_ms: u64,

    /// Warmup duration in seconds (samples discarded before measuring).
    #[arg(long, default_value_t = 0.5)]
    pub warmup_secs: f64,

    /// Measurement window in seconds.
    #[arg(long, default_value_t = 2.0)]
    pub measurement_secs: f64,

    /// One-way link RTT in ms — metadata only, copied into the record so the
    /// runner can label the scenario.
    #[arg(long, default_value_t = 0)]
    pub rtt_ms: u64,

    /// Path to write the scenario's JSON record to.
    #[arg(long)]
    pub metrics_out: PathBuf,

    /// Tokio worker thread count. Default 1 keeps the runtime single-threaded so
    /// the process can be pinned to one core and poll durations describe a
    /// single worker's behaviour.
    #[arg(long, default_value_t = 1)]
    pub worker_threads: usize,
}

pub async fn run(args: ClientArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    let connection_poll = PollDurations::new();
    let swarm_poll = PollDurations::new();
    // Per-phase CPU breakdown inside Connection::poll.
    let handler_poll = PollDurations::new();
    let muxer_poll = PollDurations::new();
    install_subscriber(SubscriberPolls {
        connection_poll: connection_poll.clone(),
        swarm_poll: swarm_poll.clone(),
        handler_poll: handler_poll.clone(),
        muxer_poll: muxer_poll.clone(),
    });

    let addr: Multiaddr = args
        .peer
        .parse()
        .map_err(|e| format!("invalid --peer multiaddr: {e}"))?;
    let peer_id: PeerId = addr
        .iter()
        .find_map(|p| match p {
            libp2p_core::multiaddr::Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .ok_or_else(|| format!("multiaddr missing /p2p/<peer_id>: {addr}"))?;

    let mut swarm = build_stream_swarm();
    let mut control = swarm.behaviour().new_control();

    swarm.dial(DialOpts::peer_id(peer_id).addresses(vec![addr]).build())?;
    loop {
        match swarm.next().await {
            Some(SwarmEvent::ConnectionEstablished { peer_id: p, .. }) if p == peer_id => break,
            Some(SwarmEvent::OutgoingConnectionError { error, .. }) => {
                return Err(format!("dial failed: {error}").into());
            }
            Some(_) => {}
            None => return Err("swarm terminated before connecting".into()),
        }
    }

    // Drive the swarm in the background so the connection task (whose
    // `Connection::poll` the PollTimer instruments) keeps running while we do
    // stream I/O directly.
    tokio::spawn(async move { while swarm.next().await.is_some() {} });

    // Open exactly one long-lived stream (negotiated once, here).
    let stream = control
        .open_stream(peer_id, STREAM_PROTOCOL)
        .await
        .map_err(|e| format!("open_stream({peer_id}) failed: {e}"))?;
    let (mut read_half, mut write_half) = stream.split();

    let warmup = Duration::from_secs_f64(args.warmup_secs);
    let measurement = Duration::from_secs_f64(args.measurement_secs);
    // Reference instant for both the measurement window and the per-message
    // round-trip timestamps the sender embeds and the reader reads back.
    let t0 = Instant::now();
    let measure_start = t0 + warmup;
    let until = measure_start + measurement;

    // Application round-trip latency (wall-clock): the sender stamps each
    // message with `t0.elapsed()`, the reader computes `now - stamp` on echo.
    let wire_rtt = PollDurations::new();

    // Sender: one `msg_size` message every `heartbeat_ms` (open-loop). On a slow
    // link the writes self-throttle to capacity (flow control), which is what
    // produces the bursty inbound echoes that stress `Connection::poll`.
    let mut msg = vec![0u8; args.msg_size];
    let heartbeat_ms = args.heartbeat_ms;
    let sender = tokio::spawn(async move {
        // Stamp the send time into the first 8 bytes so the reader can measure
        // the round trip when this message echoes back.
        let stamp = |buf: &mut [u8]| {
            if buf.len() >= 8 {
                buf[..8].copy_from_slice(&(t0.elapsed().as_nanos() as u64).to_be_bytes());
            }
        };
        if heartbeat_ms == 0 {
            while Instant::now() < until {
                stamp(&mut msg);
                if write_framed(&mut write_half, &msg).await.is_err() {
                    break;
                }
            }
        } else {
            let mut interval = tokio::time::interval(Duration::from_millis(heartbeat_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                if Instant::now() >= until {
                    break;
                }
                stamp(&mut msg);
                if write_framed(&mut write_half, &msg).await.is_err() {
                    break;
                }
            }
        }
    });

    // Reader: drain echoes so the receive window keeps opening, and tally the
    // bytes that land inside the measurement window (achieved goodput). Deadline-
    // bound each read so a stalled stream can never block past the window.
    let recv_bytes = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let reader_bytes = recv_bytes.clone();
    let reader_wire = wire_rtt.clone();
    let reader = tokio::spawn(async move {
        loop {
            let Some(remaining) = until.checked_duration_since(Instant::now()) else {
                break;
            };
            match tokio::time::timeout(remaining, read_framed(&mut read_half)).await {
                Ok(Ok(buf)) => {
                    if Instant::now() >= measure_start {
                        reader_bytes
                            .fetch_add(buf.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        // Recover the send timestamp from the echoed payload and
                        // record the (wall-clock) round-trip latency.
                        if buf.len() >= 8 {
                            let sent = u64::from_be_bytes(buf[..8].try_into().unwrap());
                            let now = t0.elapsed().as_nanos() as u64;
                            reader_wire.record_nanos(now.saturating_sub(sent));
                        }
                    }
                }
                Ok(Err(_)) | Err(_) => break, // stream error/EOF, or window elapsed
            }
        }
    });

    // Windowing: discard warmup-window poll samples, then measure.
    tokio::time::sleep(warmup).await;
    for d in [&connection_poll, &swarm_poll, &handler_poll, &muxer_poll] {
        d.reset();
    }
    tokio::time::sleep(measurement).await;

    // Print the per-phase CPU breakdown of Connection::poll. `samples` ratios
    // reveal how many handler/muxer polls happen per Connection::poll (i.e.
    // whether a long poll is one heavy call or the loop iterating many times).
    // Enable with LATB_PHASES=1.
    if std::env::var_os("LATB_PHASES").is_some() {
        let line = |name: &str, d: &PollDurations| {
            let h = HistSummary::from_hist_nanos(&d.snapshot());
            eprintln!(
                "  PHASE {name:>14}: samples={:>7}  p50={:>7.1}  p90={:>7.1}  p99={:>8.1}  max={:>9.1} µs",
                h.samples, h.p50_us, h.p90_us, h.p99_us, h.max_us,
            );
        };
        eprintln!(
            "PHASE breakdown (CPU µs) — rtt={}ms size={} hb={}ms:",
            args.rtt_ms, args.msg_size, args.heartbeat_ms
        );
        line("Connection::poll", &connection_poll);
        line("handler_poll", &handler_poll);
        line("muxer_poll", &muxer_poll);
    }

    let bytes = recv_bytes.load(std::sync::atomic::Ordering::Relaxed);
    let recv_mbps = bytes as f64 / 1e6 / measurement.as_secs_f64();

    let record = ScenarioRecord {
        rtt_ms: args.rtt_ms,
        msg_size: args.msg_size,
        heartbeat_ms: args.heartbeat_ms,
        connection_poll: HistSummary::from_hist_nanos(&connection_poll.snapshot())
            .with_slow_count(connection_poll.slow_count_snapshot()),
        swarm_poll: HistSummary::from_hist_nanos(&swarm_poll.snapshot())
            .with_slow_count(swarm_poll.slow_count_snapshot()),
        handler_poll: HistSummary::from_hist_nanos(&handler_poll.snapshot()),
        muxer_poll: HistSummary::from_hist_nanos(&muxer_poll.snapshot()),
        recv_mbps,
        wire_rtt: HistSummary::from_hist_nanos(&wire_rtt.snapshot()),
    };

    sender.abort();
    reader.abort();

    std::fs::write(&args.metrics_out, serde_json::to_string_pretty(&record)?)?;
    Ok(())
}

/// The set of histograms the tracing subscriber feeds (the headline poll timers
/// plus the sub-phase timers).
pub(crate) struct SubscriberPolls {
    pub connection_poll: PollDurations,
    pub swarm_poll: PollDurations,
    pub handler_poll: PollDurations,
    pub muxer_poll: PollDurations,
}

pub(crate) fn install_subscriber(p: SubscriberPolls) {
    use tracing_subscriber::filter::Targets;
    // `Connection::poll`/`handler_poll`/`muxer_poll` are in libp2p_swarm.
    let filter = Targets::new()
        .with_target("libp2p_swarm", tracing::Level::DEBUG)
        .with_default(tracing::Level::WARN);
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(PollTimer::new("Connection::poll", p.connection_poll))
        .with(PollTimer::new("Swarm::poll", p.swarm_poll))
        // Sub-phase CPU timers.
        .with(PollTimer::new("handler_poll", p.handler_poll))
        .with(PollTimer::new("muxer_poll", p.muxer_poll))
        .try_init();
}
