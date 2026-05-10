//! Orchestrator (`run`): build a grid of scenarios
//! (`rtt × msg_size × heartbeat`) and run them **in parallel** over real
//! netns/veth/`tc netem`, then print a `Connection::poll` **CPU-time**
//! percentile table and write `per_run.csv`.
//!
//! Each scenario is two nodes — a server (echo) in its own netns and a client
//! (heartbeat sender) in the host netns — talking over one long-lived
//! `libp2p-stream`. Scenarios run concurrently, each on a **disjoint netns
//! index** and a **disjoint pair of CPU cores** (server core + client core), so
//! their poll timings don't contaminate each other. Concurrency is capped at
//! `floor(available_parallelism / 2)` (2 cores per scenario); larger grids run
//! in successive waves.
//!
//! Requires root (or CAP_NET_ADMIN + CAP_SYS_ADMIN). Per slot `i` it creates:
//! - `latb-peer-{i}` netns
//! - `latb-c{i}` host-side veth + `latb-p{i}` peer-side veth
//! - `10.99.{i+1}.0/24` subnet (host .1, peer .2)
//! - `tc netem delay` on both veths' egress (half the RTT each way)

use std::{
    error::Error,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

use crate::metrics::{HistSummary, ScenarioRecord};

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    /// One-way link RTTs in ms (applied via `tc netem`). Comma-separated. The
    /// default spans loopback → LAN → regional → cross-continent.
    #[arg(
        long = "rtt-ms",
        value_delimiter = ',',
        default_value = "0,10,50,100,200"
    )]
    pub rtt_ms: Vec<u64>,

    /// Message sizes in bytes the client sends each heartbeat. Comma-separated.
    /// The default spans 4 / 16 / 64 / 256 KiB — small control messages to large
    /// blocks (small to large inbound bursts).
    #[arg(
        long = "msg-size",
        value_delimiter = ',',
        default_value = "1024,4096,16384,65536,262144"
    )]
    pub msg_size: Vec<usize>,

    /// Heartbeat intervals in ms (`0` = saturate) — the throughput knob, since
    /// offered load ≈ msg_size / heartbeat. Comma-separated. Combos whose offered
    /// load exceeds `--max-offered-mbps` are pruned, so a wide heartbeat list
    /// only adds the *realistic* (size, rate) cells.
    #[arg(
        long = "heartbeat-ms",
        value_delimiter = ',',
        default_value = "5,10,20,50,100,200,500"
    )]
    pub heartbeat_ms: Vec<u64>,

    /// Skip any (size, heartbeat) whose offered load `size / heartbeat` exceeds
    /// this many MB/s. A real per-stream TCP flow over a lossy/high-RTT WAN can't
    /// sustain very high rates regardless of the muxer, so those cells are both
    /// unrealistic and the noisiest (saturation, flow-control stalls). Pruning
    /// them frees the grid to cover more heartbeats.
    ///
    /// `0` (the default) disables the cap — every (size, heartbeat) cell runs,
    /// including ones that saturate the link.
    #[arg(long, default_value_t = 0.0)]
    pub max_offered_mbps: f64,

    /// Warmup per scenario (samples discarded before measuring). Default 0: the
    /// long measurement window below dominates over the one-off handshake/cold
    /// ramp, and a zero warmup avoids building an open-loop backlog before
    /// measuring (which was wedging high-offered cells to zero I/O).
    #[arg(long, default_value_t = 2.0)]
    pub warmup_secs: f64,

    /// Measurement window per scenario. Long by default so each cell sees many
    /// messages and per-message slow-start ramps are amortized into a stable
    /// median (we run a single window per cell rather than repeating — see below).
    #[arg(long, default_value_t = 20.0)]
    pub measurement_secs: f64,

    /// How many independent runs per cell. Default 1: with the long measurement
    /// window above, a single window already gives a stable median, so repeats
    /// (which 3×'d wall time once the grid exceeds the parallelism cap) aren't
    /// worth it for the p50 target. Raise it for a final low-noise confirmation
    /// run — each repeat is its own netns + connection + window, and the table
    /// medians across them (the CSV keeps every repeat so `plot.py` medians too).
    #[arg(long, default_value_t = 1)]
    pub runs_per_cell: usize,

    /// Where to write the per-scenario JSON + `per_run.csv`.
    #[arg(long, default_value = "latency-benchmark/data")]
    pub out_dir: PathBuf,

    /// Max scenarios to run concurrently. Default = `floor(ncores / 2)`. This is
    /// also a hard ceiling: each scenario is two processes needing a dedicated
    /// core each, so a larger value is clamped down to `floor(ncores / 2)`.
    #[arg(long)]
    pub max_parallel: Option<usize>,

    /// Tokio worker threads inside each server/client process. Default 1 keeps
    /// each runtime single-threaded so it can be pinned to one core.
    #[arg(long, default_value_t = 1)]
    pub worker_threads: usize,

    /// Pin each process to its scenario's core with `taskset` (default on).
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub pin_cores: bool,

    /// Capture TCP packets per scenario with `tcpdump` on BOTH ends of the veth
    /// pair, writing `<cell>_{host,peer}_r<n>.pcap` next to the metrics JSON. For
    /// diagnosis only; use with a tiny focused grid. tcpdump runs as root.
    #[arg(long, default_value_t = false)]
    pub pcap: bool,
}

#[derive(Clone, Copy, Debug)]
struct Scenario {
    rtt_ms: u64,
    msg_size: usize,
    heartbeat_ms: u64,
}

pub fn run(args: RunArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    if args.rtt_ms.is_empty() || args.msg_size.is_empty() || args.heartbeat_ms.is_empty() {
        return Err("each grid axis must have at least one value".into());
    }

    // Cartesian product, ordered (rtt, size, heartbeat). Prune (size, heartbeat)
    // combos whose offered load exceeds the cap — unrealistic for a single TCP
    // stream and the source of saturation noise.
    let mut scenarios = Vec::new();
    let mut pruned = 0usize;
    for &rtt_ms in &args.rtt_ms {
        for &msg_size in &args.msg_size {
            for &heartbeat_ms in &args.heartbeat_ms {
                if args.max_offered_mbps > 0.0 {
                    // offered MB/s = size_bytes / (heartbeat_ms / 1000) / 1e6.
                    // heartbeat 0 = saturate = unbounded → always pruned by a cap.
                    let offered = if heartbeat_ms == 0 {
                        f64::INFINITY
                    } else {
                        msg_size as f64 / (heartbeat_ms as f64) / 1000.0
                    };
                    if offered > args.max_offered_mbps {
                        pruned += 1;
                        continue;
                    }
                }
                scenarios.push(Scenario {
                    rtt_ms,
                    msg_size,
                    heartbeat_ms,
                });
            }
        }
    }
    if scenarios.is_empty() {
        return Err(
            "every grid cell was pruned by --max-offered-mbps; raise it or \
                    use larger heartbeats / smaller messages"
                .into(),
        );
    }

    // Expand each cell into `runs_per_cell` independent jobs (carry the repeat
    // index so each gets its own metrics file). Repeats are medianed at report
    // time, so running them as separate scenarios — possibly in different waves
    // — folds machine variance into the estimate.
    let runs_per_cell = args.runs_per_cell.max(1);
    let cells = scenarios.len();
    let jobs: Vec<(Scenario, usize)> = scenarios
        .into_iter()
        .flat_map(|s| (0..runs_per_cell).map(move |run| (s, run)))
        .collect();

    let ncores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(2);
    // Each scenario is two processes (server + client) pinned to a core each, so
    // never run more than floor(ncores/2) at once — this is a hard ceiling even
    // when --max-parallel asks for more.
    let core_cap = (ncores / 2).max(1);
    let cap = args
        .max_parallel
        .unwrap_or(core_cap)
        .clamp(1, core_cap)
        .min(jobs.len().max(1))
        // We index netns subnets as `10.99.{slot+1}.0/24`, so slot must stay < 254.
        .min(254);
    let waves = jobs.len().div_ceil(cap);

    fs::create_dir_all(&args.out_dir)?;
    let metrics_dir = args.out_dir.join("runs");
    fs::create_dir_all(&metrics_dir)?;

    let exe = std::env::current_exe()?;
    let prune_note = if pruned > 0 {
        format!(
            " ({pruned} pruned > {} MB/s offered)",
            args.max_offered_mbps
        )
    } else {
        String::new()
    };
    eprintln!(
        "grid: {} cell(s){} × {} run(s) = {} job(s) \
         ({} rtt × {} size × {} heartbeat), \
         cap={} → {} wave(s); ncores={} pin_cores={} warmup={}s measure={}s",
        cells,
        prune_note,
        runs_per_cell,
        jobs.len(),
        args.rtt_ms.len(),
        args.msg_size.len(),
        args.heartbeat_ms.len(),
        cap,
        waves,
        ncores,
        args.pin_cores,
        args.warmup_secs,
        args.measurement_secs,
    );

    // Clear anything left over from a crashed run on the slots we'll use.
    for idx in 0..cap {
        cleanup_idx(idx);
    }

    let mut records: Vec<ScenarioRecord> = Vec::with_capacity(jobs.len());
    for (wave_idx, chunk) in jobs.chunks(cap).enumerate() {
        eprintln!(
            "── wave {}/{}: {} job(s) ──",
            wave_idx + 1,
            waves,
            chunk.len()
        );
        let exe = &exe;
        let args = &args;
        let metrics_dir = &metrics_dir;
        let wave_records: Vec<ScenarioRecord> = std::thread::scope(|s| {
            let handles: Vec<_> = chunk
                .iter()
                .enumerate()
                .map(|(slot, (scenario, run_idx))| {
                    let (scenario, run_idx) = (*scenario, *run_idx);
                    s.spawn(move || {
                        run_scenario(exe, scenario, run_idx, slot, args, ncores, metrics_dir)
                    })
                })
                .collect();
            handles
                .into_iter()
                .filter_map(|h| match h.join() {
                    Ok(Ok(rec)) => Some(rec),
                    Ok(Err(e)) => {
                        eprintln!("  scenario failed: {e}");
                        None
                    }
                    Err(_) => {
                        eprintln!("  scenario thread panicked");
                        None
                    }
                })
                .collect()
        });
        records.extend(wave_records);
    }

    // Final best-effort sweep of the slots we touched.
    for idx in 0..cap {
        cleanup_idx(idx);
    }

    if records.is_empty() {
        return Err("every scenario failed; no records to report".into());
    }

    // Table shows the per-cell median across repeats; CSV keeps every repeat.
    let aggregated = aggregate_runs(&records);
    print_table(&aggregated, &args, waves, cells, runs_per_cell);
    let csv_path = args.out_dir.join("per_run.csv");
    write_csv(&records, &csv_path)?;
    eprintln!("\nper_run.csv → {}", csv_path.display());
    Ok(())
}

/// Set up the netns, run the 2-node scenario (repeat `run_idx`) on slot `slot`,
/// tear the netns down. Returns the client's recorded scenario metrics.
fn run_scenario(
    exe: &Path,
    scenario: Scenario,
    run_idx: usize,
    slot: usize,
    args: &RunArgs,
    ncores: usize,
    metrics_dir: &Path,
) -> Result<ScenarioRecord, Box<dyn Error + Send + Sync>> {
    let idx = slot;
    cleanup_idx(idx); // clear any leftover before we (re)create
    let result = (|| -> Result<ScenarioRecord, Box<dyn Error + Send + Sync>> {
        netns_setup(idx, scenario.rtt_ms)?;

        let (server_core, client_core) = if args.pin_cores {
            (Some((2 * slot) % ncores), Some((2 * slot + 1) % ncores))
        } else {
            (None, None)
        };

        let label = cell_label(&scenario);

        // Optional packet capture for this scenario, on BOTH ends of the veth
        // pair (host side = client, netns side = server), so we can line up each
        // side's segment sizes / ACK timing / advertised windows and the on-wire
        // delay. tcpdump runs as root (the orchestrator is already under sudo).
        let mut pcaps: Vec<Child> = Vec::new();
        if args.pcap {
            let host_veth = format!("latb-c{idx}");
            let peer_veth = format!("latb-p{idx}");
            let ns = format!("latb-peer-{idx}");
            pcaps.push(
                Command::new("tcpdump")
                    .args(["-i", &host_veth, "-s", "256", "-U", "-w"])
                    .arg(metrics_dir.join(format!("{label}_host_r{run_idx}.pcap")))
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?,
            );
            pcaps.push(
                Command::new("ip")
                    .args([
                        "netns", "exec", &ns, "tcpdump", "-i", &peer_veth, "-s", "256", "-U", "-w",
                    ])
                    .arg(metrics_dir.join(format!("{label}_peer_r{run_idx}.pcap")))
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()?,
            );
            // Let tcpdump attach to the interfaces before traffic starts.
            std::thread::sleep(std::time::Duration::from_millis(300));
        }

        let (mut server, addr) = spawn_server(exe, idx, server_core, args.worker_threads)?;

        let metrics_path = metrics_dir.join(format!("{label}_r{run_idx}.json"));
        let client_result = run_client(exe, &addr, &scenario, args, &metrics_path, client_core);

        for mut td in pcaps.drain(..) {
            let _ = td.kill();
            let _ = td.wait();
        }
        let _ = server.kill();
        let _ = server.wait();
        client_result?;

        let body = fs::read_to_string(&metrics_path)?;
        let record: ScenarioRecord = serde_json::from_str(&body)?;
        // A 0-sample record means the stream produced no I/O in the window (a
        // transient stall / reset). Drop it: a fake 0 would read as a +100%
        // "win" / -100% throughput in the charts.
        if record.connection_poll.samples == 0 {
            return Err(format!(
                "rtt={}ms size={} hb={}ms: 0 poll samples (stream stalled) — dropped",
                scenario.rtt_ms,
                fmt_bytes(scenario.msg_size),
                scenario.heartbeat_ms,
            )
            .into());
        }
        let thin = if record.connection_poll.samples < LOW_SAMPLE_THRESHOLD {
            " ⚠ thin"
        } else {
            ""
        };
        eprintln!(
            "  ✓ rtt={}ms size={} hb={}ms: poll p50={:.1}µs p99={:.1}µs max={:.1}µs \
             ({} samples, {:.1} MB/s){}",
            scenario.rtt_ms,
            fmt_bytes(scenario.msg_size),
            scenario.heartbeat_ms,
            record.connection_poll.p50_us,
            record.connection_poll.p99_us,
            record.connection_poll.max_us,
            record.connection_poll.samples,
            record.recv_mbps,
            thin,
        );
        Ok(record)
    })();
    cleanup_idx(idx);
    result
}

fn cell_label(s: &Scenario) -> String {
    format!(
        "rtt{:03}_sz{:08}_hb{:04}",
        s.rtt_ms, s.msg_size, s.heartbeat_ms
    )
}

// ─── netns / tc setup ────────────────────────────────────────────────────────

fn cmd_status(cmd: &mut Command) -> Result<(), Box<dyn Error + Send + Sync>> {
    let out = cmd.stdout(Stdio::null()).stderr(Stdio::piped()).output()?;
    if !out.status.success() {
        return Err(format!(
            "command failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        )
        .into());
    }
    Ok(())
}

fn netns_setup(idx: usize, rtt_ms: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ns = format!("latb-peer-{idx}");
    let host_veth = format!("latb-c{idx}");
    let peer_veth = format!("latb-p{idx}");
    let host_ip = format!("10.99.{}.1/24", idx + 1);
    let peer_ip = format!("10.99.{}.2/24", idx + 1);

    cmd_status(Command::new("ip").args(["netns", "add", &ns]))?;
    cmd_status(Command::new("ip").args([
        "link", "add", &host_veth, "type", "veth", "peer", "name", &peer_veth,
    ]))?;
    cmd_status(Command::new("ip").args(["link", "set", &peer_veth, "netns", &ns]))?;
    cmd_status(Command::new("ip").args(["addr", "add", &host_ip, "dev", &host_veth]))?;
    cmd_status(Command::new("ip").args(["-n", &ns, "addr", "add", &peer_ip, "dev", &peer_veth]))?;
    cmd_status(Command::new("ip").args(["link", "set", &host_veth, "up"]))?;
    cmd_status(Command::new("ip").args(["-n", &ns, "link", "set", &peer_veth, "up"]))?;
    cmd_status(Command::new("ip").args(["-n", &ns, "link", "set", "lo", "up"]))?;

    // Symmetric egress: half the configured RTT on each veth's egress qdisc.
    // Real WAN links have ~equal propagation delay both ways. We apply `tc
    // netem` even at rtt_ms=0 (delay 0ms) so every cell traverses the same
    // qdisc shape — otherwise RTT=0 would compare a `pfifo_fast` baseline
    // against `netem` and contaminate per-RTT comparisons.
    let peer_half = rtt_ms / 2;
    let host_half = rtt_ms - peer_half; // round-up the odd-ms case
    cmd_status(Command::new("ip").args([
        "netns",
        "exec",
        &ns,
        "tc",
        "qdisc",
        "add",
        "dev",
        &peer_veth,
        "root",
        "netem",
        "delay",
        &format!("{peer_half}ms"),
        // Size netem's OWN queue so it never tail-drops (its default is 1000
        // packets). A faithful delay line must not inject loss — this is
        // link-emulation fidelity, NOT TCP/kernel tuning, and is result-neutral.
        "limit",
        "1000000",
    ]))?;
    cmd_status(Command::new("tc").args([
        "qdisc",
        "add",
        "dev",
        &host_veth,
        "root",
        "netem",
        "delay",
        &format!("{host_half}ms"),
        "limit",
        "1000000",
    ]))?;
    Ok(())
}

/// Best-effort teardown of one slot's veth + netns. ENOENT is normal.
fn cleanup_idx(idx: usize) {
    let host_veth = format!("latb-c{idx}");
    let _ = Command::new("ip")
        .args(["link", "del", &host_veth])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let ns = format!("latb-peer-{idx}");
    let _ = Command::new("ip")
        .args(["netns", "del", &ns])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

// ─── process spawn ───────────────────────────────────────────────────────────

fn spawn_server(
    exe: &Path,
    idx: usize,
    pin_core: Option<usize>,
    worker_threads: usize,
) -> Result<(Child, String), Box<dyn Error + Send + Sync>> {
    // Listen on `tcp/0` for an ephemeral port (avoids TIME_WAIT / cross-scenario
    // port collisions). The peer side of the veth pair owns `10.99.{idx+1}.2`.
    let listen = format!("/ip4/10.99.{}.2/tcp/0", idx + 1);
    let ns = format!("latb-peer-{idx}");

    // `ip netns exec NS [taskset -c C] exe peer …` — both `ip` and `taskset`
    // execvp into the next program, so the spawned Child's PID ends up being the
    // server binary itself (killing the Child kills the server).
    let mut cmd = Command::new("ip");
    cmd.args(["netns", "exec", &ns]);
    if let Some(c) = pin_core {
        cmd.args(["taskset", "-c", &c.to_string()]);
    }
    cmd.arg(exe);
    cmd.args([
        "peer",
        "--listen",
        &listen,
        "--worker-threads",
        &worker_threads.to_string(),
    ]);

    let mut child = cmd.stdout(Stdio::piped()).stderr(Stdio::null()).spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or("server child has no stdout pipe")?;
    // Read the multiaddr line on a helper thread with a timeout so a server that
    // never starts can't hang the whole wave; the thread then drains the rest of
    // stdout so the pipe never blocks the server.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let n = reader.read_line(&mut line).unwrap_or(0);
        let _ = tx.send(if n == 0 {
            None
        } else {
            Some(line.trim().to_string())
        });
        let mut buf = String::new();
        while reader.read_line(&mut buf).unwrap_or(0) > 0 {
            buf.clear();
        }
    });
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(Some(addr)) => Ok((child, addr)),
        Ok(None) => {
            let _ = child.kill();
            Err("server exited before printing multiaddr".into())
        }
        Err(_) => {
            let _ = child.kill();
            Err("server did not print its multiaddr within 10s".into())
        }
    }
}

fn run_client(
    exe: &Path,
    server_addr: &str,
    scenario: &Scenario,
    args: &RunArgs,
    metrics_out: &Path,
    pin_core: Option<usize>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Client runs in the host netns (it must reach the peer-side veth IP), so no
    // `ip netns exec` — just `taskset` to pin it, then the binary.
    let mut cmd = if let Some(c) = pin_core {
        let mut cmd = Command::new("taskset");
        cmd.args(["-c", &c.to_string()]);
        cmd.arg(exe);
        cmd
    } else {
        Command::new(exe)
    };
    cmd.args([
        "client",
        "--peer",
        server_addr,
        "--msg-size",
        &scenario.msg_size.to_string(),
        "--heartbeat-ms",
        &scenario.heartbeat_ms.to_string(),
        "--rtt-ms",
        &scenario.rtt_ms.to_string(),
        "--warmup-secs",
        &args.warmup_secs.to_string(),
        "--measurement-secs",
        &args.measurement_secs.to_string(),
        "--worker-threads",
        &args.worker_threads.to_string(),
        "--metrics-out",
        metrics_out.to_str().ok_or("non-utf8 metrics path")?,
    ]);

    let out = cmd.stdout(Stdio::null()).stderr(Stdio::piped()).output()?;
    if !out.status.success() {
        return Err(format!(
            "client exit {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).trim()
        )
        .into());
    }
    Ok(())
}

// ─── reporting ────────────────────────────────────────────────────────────────

/// Cells with fewer than this many `Connection::poll` samples have statistically
/// thin tail percentiles (p99/max ≈ a single observation); they're marked `*`.
const LOW_SAMPLE_THRESHOLD: u64 = 200;

fn median_f64(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// Collapse repeat runs into one record per (rtt, size, heartbeat): each
/// percentile / goodput is the **median** across repeats (robust to the
/// single-run tail noise), and samples / slow_count are summed. The raw repeats
/// stay in the CSV so `plot.py` medians them too.
fn aggregate_runs(records: &[ScenarioRecord]) -> Vec<ScenarioRecord> {
    use std::collections::BTreeMap;

    let mut groups: BTreeMap<(u64, usize, u64), Vec<&ScenarioRecord>> = BTreeMap::new();
    for r in records {
        groups
            .entry((r.rtt_ms, r.msg_size, r.heartbeat_ms))
            .or_default()
            .push(r);
    }
    let mut out = Vec::with_capacity(groups.len());
    for ((rtt_ms, msg_size, heartbeat_ms), g) in groups {
        let summarize = |pick: fn(&ScenarioRecord) -> &HistSummary| {
            let med = |field: fn(&HistSummary) -> f64| {
                median_f64(g.iter().map(|r| field(pick(r))).collect())
            };
            HistSummary {
                mean_us: med(|h| h.mean_us),
                p50_us: med(|h| h.p50_us),
                p90_us: med(|h| h.p90_us),
                p95_us: med(|h| h.p95_us),
                p99_us: med(|h| h.p99_us),
                max_us: med(|h| h.max_us),
                samples: g.iter().map(|r| pick(r).samples).sum(),
                slow_count: g.iter().map(|r| pick(r).slow_count).sum(),
            }
        };
        out.push(ScenarioRecord {
            rtt_ms,
            msg_size,
            heartbeat_ms,
            connection_poll: summarize(|r| &r.connection_poll),
            swarm_poll: HistSummary::default(),
            handler_poll: summarize(|r| &r.handler_poll),
            muxer_poll: summarize(|r| &r.muxer_poll),
            recv_mbps: median_f64(g.iter().map(|r| r.recv_mbps).collect()),
            wire_rtt: summarize(|r| &r.wire_rtt),
        });
    }
    out
}

fn fmt_bytes(n: usize) -> String {
    if n >= 1024 * 1024 && n.is_multiple_of(1024 * 1024) {
        format!("{}MiB", n / (1024 * 1024))
    } else if n >= 1024 && n.is_multiple_of(1024) {
        format!("{}KiB", n / 1024)
    } else {
        format!("{n}B")
    }
}

/// Print the headline: `Connection::poll` percentiles + goodput per cell.
/// `records` are already medianed across repeats; cells are sorted by
/// (rtt, size, heartbeat).
fn print_table(
    records: &[ScenarioRecord],
    args: &RunArgs,
    waves: usize,
    cells: usize,
    runs: usize,
) {
    const WIDTH: usize = 72;
    let runs_note = if runs > 1 {
        format!(", median of {runs} runs")
    } else {
        String::new()
    };
    println!();
    println!(
        "Connection::poll CPU time (µs) + goodput  \
         ({cells} cell(s), {waves} wave(s), {}s measure{runs_note})",
        args.measurement_secs,
    );
    // connP99 ≈ muxP99 + ~25µs loop/handler. If muxP99 dominates connP99, the
    // poll floor is yamux-internal.
    println!(
        "{:>5}  {:>7}  {:>4}  {:>8}  {:>8}  {:>8}  | {:>6}",
        "rtt", "size", "hb", "samples", "connP99", "muxP99", "MB/s",
    );
    println!("{}", "-".repeat(WIDTH));

    // Cells sorted by (rtt, size, heartbeat).
    let mut sorted: Vec<&ScenarioRecord> = records.iter().collect();
    sorted.sort_by_key(|r| (r.rtt_ms, r.msg_size, r.heartbeat_ms));

    let mut any_thin = false;
    for r in sorted {
        let c = &r.connection_poll;
        let thin = c.samples < LOW_SAMPLE_THRESHOLD;
        any_thin |= thin;
        let samples = format!("{}{}", c.samples, if thin { "*" } else { "" });
        println!(
            "{:>5}  {:>7}  {:>4}  {:>8}  {:>8.1}  {:>8.1}  | {:>6.1}",
            r.rtt_ms,
            fmt_bytes(r.msg_size),
            r.heartbeat_ms,
            samples,
            c.p99_us,
            r.muxer_poll.p99_us,
            r.recv_mbps,
        );
    }
    println!("{}", "-".repeat(WIDTH));
    if any_thin {
        println!(
            "* fewer than {} samples — p99/max are statistically thin; raise \
             --measurement-secs or --heartbeat-ms (more I/O) for those cells.",
            LOW_SAMPLE_THRESHOLD
        );
    }
}

fn write_csv(records: &[ScenarioRecord], path: &Path) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut s = String::new();
    s.push_str(
        "rtt_ms,msg_size,heartbeat_ms,recv_mbps,\
         poll_mean_us,poll_p50_us,poll_p90_us,poll_p95_us,poll_p99_us,poll_max_us,\
         poll_samples,poll_slow_count,\
         muxer_p50_us,muxer_p90_us,muxer_p99_us,muxer_max_us,muxer_samples,\
         handler_p50_us,handler_p90_us,handler_p99_us,handler_max_us,handler_samples,\
         swarm_poll_mean_us,swarm_poll_p50_us,swarm_poll_p90_us,swarm_poll_p95_us,\
         swarm_poll_p99_us,swarm_poll_max_us,swarm_poll_samples,\
         wire_rtt_p50_us,wire_rtt_p99_us,wire_rtt_max_us,wire_rtt_samples\n",
    );
    for r in records {
        let c = &r.connection_poll;
        let mx = &r.muxer_poll;
        let hd = &r.handler_poll;
        let sw = &r.swarm_poll;
        let wr = &r.wire_rtt;
        s.push_str(&format!(
            "{},{},{},{:.3},\
             {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},\
             {:.3},{:.3},{:.3},{:.3},{},\
             {:.3},{:.3},{:.3},{:.3},{},\
             {:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},\
             {:.3},{:.3},{:.3},{}\n",
            r.rtt_ms,
            r.msg_size,
            r.heartbeat_ms,
            r.recv_mbps,
            c.mean_us,
            c.p50_us,
            c.p90_us,
            c.p95_us,
            c.p99_us,
            c.max_us,
            c.samples,
            c.slow_count,
            mx.p50_us,
            mx.p90_us,
            mx.p99_us,
            mx.max_us,
            mx.samples,
            hd.p50_us,
            hd.p90_us,
            hd.p99_us,
            hd.max_us,
            hd.samples,
            sw.mean_us,
            sw.p50_us,
            sw.p90_us,
            sw.p95_us,
            sw.p99_us,
            sw.max_us,
            sw.samples,
            wr.p50_us,
            wr.p99_us,
            wr.max_us,
            wr.samples,
        ));
    }
    fs::write(path, s)?;
    Ok(())
}
