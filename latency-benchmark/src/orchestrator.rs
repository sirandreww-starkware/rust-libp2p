//! Orchestrator: stand up `n_peers` Linux netns/veth/tc setups, spawn one
//! peer process per netns, spawn the central process in the host netns,
//! collect JSON metrics, write CSVs, tear everything down.
//!
//! Requires root (or CAP_NET_ADMIN + CAP_SYS_ADMIN). Will create:
//! - `latb-peer-{0..N-1}` netns (cleaned up between runs)
//! - `latb-c{0..N-1}` host-side veth + `latb-p{0..N-1}` peer-side veth
//! - `10.99.{i}.0/24` per-peer subnet (host .1, peer .2)
//! - tc netem delay on slow peers' egress (default: peer 0 is fast, 1..N-1 slow)

use std::{
    error::Error,
    fs,
    io::{BufRead, BufReader},
    os::unix::process::CommandExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

use crate::metrics::RunMetrics;

#[derive(Debug, clap::Args)]
pub struct RunArgs {
    /// Total peers (1 fast at index 0 + N-1 slow in mixed topology).
    #[arg(long, default_value_t = 10)]
    pub n_peers: usize,

    /// One-way latency in ms applied to slow peers' egress (tc netem delay).
    #[arg(long, default_value_t = 50)]
    pub rtt_ms: u64,

    /// Payload bytes per request.
    #[arg(long, default_value_t = 4096)]
    pub payload: usize,

    #[arg(long, default_value_t = 2.0)]
    pub warmup_secs: f64,

    #[arg(long, default_value_t = 20.0)]
    pub measurement_secs: f64,

    #[arg(long, default_value_t = 2)]
    pub concurrency_per_peer: usize,

    /// Number of runs to perform (full setup + teardown per run).
    #[arg(long, default_value_t = 5)]
    pub n_runs: usize,

    /// Where to write metrics JSON + the aggregated CSVs.
    #[arg(long, default_value = "target/latency-benchmark")]
    pub out_dir: PathBuf,

    /// "homogeneous" (all peers at rtt_ms) or "mixed" (peer 0 fast, 1..N slow).
    /// Homogeneous is the right choice when the headline metric is
    /// Connection::poll behaviour at a given RTT — mixed contaminates the
    /// per-RTT attribution because peer 0 always runs at RTT=0 regardless of
    /// the axis value. Mixed remains as an opt-in for head-of-line-style
    /// experiments.
    #[arg(long, default_value = "homogeneous")]
    pub topology: String,

    /// Tokio worker threads inside each peer/central process. Default 1 keeps
    /// each tokio runtime single-threaded so we can pin it to one core.
    #[arg(long, default_value_t = 1)]
    pub worker_threads: usize,

    /// If true (default), use `taskset -c <core>` to pin each spawned process
    /// to a specific physical core. Central → core 0, peer i → core
    /// `(i + 1) mod ncores`. Disable with `--no-pin-cores`.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub pin_cores: bool,
}

pub fn run(args: RunArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    let topology = match args.topology.as_str() {
        "mixed" | "homogeneous" => args.topology.clone(),
        other => return Err(format!("--topology must be mixed|homogeneous, got {other}").into()),
    };
    if args.n_peers == 0 || args.n_peers > 254 {
        return Err("n_peers must be in 1..=254".into());
    }

    fs::create_dir_all(&args.out_dir)?;
    let metrics_dir = args.out_dir.join("runs");
    fs::create_dir_all(&metrics_dir)?;

    // Every cell — including rtt_ms == 0 — goes through the same per-peer
    // netns + veth + `tc netem` path. Skipping netns at RTT=0 would measure
    // a different network stack (loopback) than the rest of the grid and
    // make per-RTT comparisons unsound. Requires root throughout.
    let use_netns = true;
    // Best-effort cleanup of anything left over from a prior crash.
    cleanup_all(args.n_peers);

    let exe = std::env::current_exe()?;
    let ncores: usize = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    if args.pin_cores && args.n_peers + 1 > ncores {
        eprintln!(
            "warning: n_peers+1 ({}) > available cores ({}); some peers will share a core",
            args.n_peers + 1,
            ncores
        );
    }
    eprintln!(
        "settings: worker_threads={} pin_cores={} ncores={} use_netns={} (rtt_ms={})",
        args.worker_threads, args.pin_cores, ncores, use_netns, args.rtt_ms
    );
    let mut all_metrics: Vec<(usize, RunMetrics)> = Vec::new();

    for run_idx in 0..args.n_runs {
        eprintln!(
            "[run {}/{}] n_peers={} rtt_ms={} payload={} topology={}",
            run_idx + 1,
            args.n_runs,
            args.n_peers,
            args.rtt_ms,
            args.payload,
            topology,
        );

        // Per-peer setup. Peer 0 is fast in mixed topology.
        if use_netns {
            for i in 0..args.n_peers {
                let rtt = if topology == "mixed" && i == 0 {
                    0
                } else {
                    args.rtt_ms
                };
                netns_setup(i, rtt)?;
            }
        }

        let mut peer_children: Vec<Child> = Vec::with_capacity(args.n_peers);
        let mut peer_addrs: Vec<String> = Vec::with_capacity(args.n_peers);

        let bail = |err: Box<dyn Error + Send + Sync>,
                    children: &mut Vec<Child>|
         -> Box<dyn Error + Send + Sync> {
            for c in children.iter_mut() {
                let _ = c.kill();
            }
            for c in children.iter_mut() {
                let _ = c.wait();
            }
            if use_netns {
                cleanup_all(args.n_peers);
            }
            err
        };

        for i in 0..args.n_peers {
            let pin_core = if args.pin_cores {
                Some((i + 1) % ncores)
            } else {
                None
            };
            match spawn_peer(&exe, i, pin_core, args.worker_threads, use_netns) {
                Ok((child, addr)) => {
                    peer_children.push(child);
                    peer_addrs.push(addr);
                }
                Err(e) => {
                    return Err(bail(
                        format!("spawn_peer({i}) failed: {e}").into(),
                        &mut peer_children,
                    ));
                }
            }
        }

        let metrics_path = metrics_dir.join(format!("run-{run_idx}.json"));
        let central_pin = if args.pin_cores { Some(0usize) } else { None };
        match spawn_central(&exe, &peer_addrs, &args, &metrics_path, central_pin) {
            Ok(()) => {}
            Err(e) => {
                return Err(bail(
                    format!("central failed: {e}").into(),
                    &mut peer_children,
                ));
            }
        }

        // Tear down: peers, then netns (if any).
        for c in peer_children.iter_mut() {
            let _ = c.kill();
        }
        for c in peer_children.iter_mut() {
            let _ = c.wait();
        }
        if use_netns {
            cleanup_all(args.n_peers);
        }

        // Read metrics.
        let body = fs::read_to_string(&metrics_path)?;
        let metrics: RunMetrics = serde_json::from_str(&body)?;
        eprintln!(
            "    Connection::poll p99={} µs  Swarm::poll p99={} µs  \
             central_task: mean_poll={} µs mean_sched={} µs  \
             conn_tasks: mean_poll={} µs mean_sched={} µs  slow={}",
            metrics.central.connection_poll.p99_us,
            metrics.central.swarm_poll.p99_us,
            metrics.central.central_task.mean_poll_duration_us,
            metrics.central.central_task.mean_scheduled_duration_us,
            metrics.central.connection_tasks.mean_poll_duration_us,
            metrics.central.connection_tasks.mean_scheduled_duration_us,
            metrics.central.connection_tasks.total_slow_poll_count,
        );
        for p in &metrics.peers {
            eprintln!(
                "    peer[{}] tag={:>5} e2e p99={} µs  rps={:.1}",
                p.index, p.tag, p.e2e_latency.p99_us, p.throughput_rps
            );
        }
        all_metrics.push((run_idx, metrics));
    }

    write_csvs(&args, &all_metrics, &topology)?;
    eprintln!("done. CSVs in {}", args.out_dir.display());
    Ok(())
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
    // Symmetric egress: half the configured RTT on each veth's egress
    // qdisc. Real WAN links have ~equal propagation delay both directions;
    // modelling it asymmetrically would skew TCP-stack behaviour on the
    // central side (no qdisc-induced backpressure on its writes).
    // We apply `tc netem` even at rtt_ms=0 (with delay 0ms) so that every
    // cell on the grid traverses the same qdisc shape — without this, the
    // RTT=0 column would compare against a `pfifo_fast` baseline and the
    // others against `netem`, contaminating per-RTT comparisons.
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
    ]))?;
    Ok(())
}

fn cleanup_all(n_peers: usize) {
    // Cleanup is best-effort. Ignore errors; ENOENT is normal when interfaces
    // / netns don't exist yet.
    for idx in 0..n_peers {
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
}

// ─── process spawn ───────────────────────────────────────────────────────────

fn spawn_peer(
    exe: &std::path::Path,
    idx: usize,
    pin_core: Option<usize>,
    worker_threads: usize,
    use_netns: bool,
) -> Result<(Child, String), Box<dyn Error + Send + Sync>> {
    let workers_str = worker_threads.to_string();

    // Listen on `tcp/0` to let the OS pick an ephemeral port. Avoids
    // collisions with leftover sockets in `TIME_WAIT` from prior runs,
    // collisions with anything else on the host listening on a fixed
    // port, and collisions between peers (which would otherwise need
    // mutually-exclusive port assignment).
    let listen = if use_netns {
        format!("/ip4/10.99.{}.2/tcp/0", idx + 1)
    } else {
        "/ip4/127.0.0.1/tcp/0".to_string()
    };

    // Layered wrapping: outer wrappers (when used) come first, then taskset,
    // then the actual binary. The leading program is whatever wrapper we need;
    // if there are no wrappers at all, run the binary directly.
    let pin = pin_core.map(|c| c.to_string());
    let mut cmd = match (use_netns, pin.as_deref()) {
        (true, Some(c)) => {
            let mut cmd = Command::new("ip");
            let ns = format!("latb-peer-{idx}");
            cmd.args(["netns", "exec", &ns, "taskset", "-c", c]);
            cmd.arg(exe);
            cmd
        }
        (true, None) => {
            let mut cmd = Command::new("ip");
            let ns = format!("latb-peer-{idx}");
            cmd.args(["netns", "exec", &ns]);
            cmd.arg(exe);
            cmd
        }
        (false, Some(c)) => {
            let mut cmd = Command::new("taskset");
            cmd.args(["-c", c]);
            cmd.arg(exe);
            cmd
        }
        (false, None) => Command::new(exe),
    };
    cmd.args([
        "peer",
        "--listen",
        &listen,
        "--worker-threads",
        &workers_str,
    ]);

    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0)
        .spawn()?;

    let stdout = child.stdout.take().ok_or("peer child has no stdout pipe")?;
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    // Block until peer prints its multiaddr.
    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Err("peer exited before printing multiaddr".into());
    }
    // Put the pipe back so it doesn't fill up and block the peer.
    let _ = std::thread::Builder::new()
        .name(format!("latb-peer-{idx}-stdout"))
        .spawn(move || {
            let mut sink = reader;
            let mut buf = String::new();
            while sink.read_line(&mut buf).unwrap_or(0) > 0 {
                buf.clear();
            }
        });
    Ok((child, line.trim().to_string()))
}

fn spawn_central(
    exe: &std::path::Path,
    peer_addrs: &[String],
    args: &RunArgs,
    metrics_out: &std::path::Path,
    pin_core: Option<usize>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // If pinning, the front of the command is `taskset -c <core>` and the
    // real binary follows; otherwise just invoke the binary directly.
    let mut cmd = if let Some(c) = pin_core {
        let mut cmd = Command::new("taskset");
        cmd.args(["-c", &c.to_string()]);
        cmd.arg(exe);
        cmd
    } else {
        Command::new(exe)
    };
    cmd.arg("central");
    for addr in peer_addrs {
        cmd.args(["--peer", addr]);
    }
    cmd.args(["--payload", &args.payload.to_string()]);
    cmd.args([
        "--concurrency-per-peer",
        &args.concurrency_per_peer.to_string(),
    ]);
    cmd.args(["--warmup-secs", &args.warmup_secs.to_string()]);
    cmd.args(["--measurement-secs", &args.measurement_secs.to_string()]);
    cmd.args(["--worker-threads", &args.worker_threads.to_string()]);
    cmd.args([
        "--metrics-out",
        metrics_out.to_str().ok_or("non-utf8 metrics path")?,
    ]);
    cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());

    let status = cmd.status()?;
    if !status.success() {
        return Err(format!("central exit {}", status).into());
    }
    // Generous wall-clock safeguard so we don't hang forever if something is off.
    let _ = Duration::from_secs_f64(args.warmup_secs + args.measurement_secs + 30.0);
    Ok(())
}

// ─── CSV emit ────────────────────────────────────────────────────────────────

fn write_csvs(
    args: &RunArgs,
    all_metrics: &[(usize, RunMetrics)],
    topology: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let per_peer = args.out_dir.join("per_peer.csv");
    let per_run = args.out_dir.join("per_run.csv");

    // per_peer.csv: row per (run, peer).
    let mut pp = String::new();
    pp.push_str(
        "run_idx,n_peers,rtt_ms,payload,topology,peer_idx,peer_tag,\
         e2e_p50_us,e2e_p95_us,e2e_p99_us,e2e_max_us,completed,throughput_rps\n",
    );
    for (run_idx, m) in all_metrics {
        for p in &m.peers {
            pp.push_str(&format!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{:.2}\n",
                run_idx,
                args.n_peers,
                args.rtt_ms,
                args.payload,
                topology,
                p.index,
                p.tag,
                p.e2e_latency.p50_us,
                p.e2e_latency.p95_us,
                p.e2e_latency.p99_us,
                p.e2e_latency.max_us,
                p.completed,
                p.throughput_rps,
            ));
        }
    }
    fs::write(&per_peer, pp)?;

    // per_run.csv: one row per run. Columns:
    //   poll_*        = Connection::poll tracing histogram (per-connection-task)
    //   swarm_poll_*  = Swarm::poll tracing histogram (central event loop)
    //   ct_*          = central task TaskMonitor (means only)
    //   cn_*          = connection tasks TaskMonitor (means only, aggregated)
    let mut pr = String::new();
    pr.push_str(
        "run_idx,n_peers,rtt_ms,payload,topology,\
         poll_p50_us,poll_p95_us,poll_p99_us,poll_max_us,poll_samples,poll_slow_count,\
         swarm_poll_p50_us,swarm_poll_p95_us,swarm_poll_p99_us,\
         swarm_poll_max_us,swarm_poll_samples,swarm_poll_slow_count,\
         ct_mean_poll_us,ct_mean_scheduled_us,ct_total_poll_count,\
         ct_total_slow_poll_count,ct_mean_slow_poll_us,\
         cn_mean_poll_us,cn_mean_scheduled_us,cn_total_poll_count,\
         cn_total_slow_poll_count,cn_mean_slow_poll_us,total_rps\n",
    );
    for (run_idx, m) in all_metrics {
        let total_rps: f64 = m.peers.iter().map(|p| p.throughput_rps).sum();
        pr.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.2}\n",
            run_idx,
            args.n_peers,
            args.rtt_ms,
            args.payload,
            topology,
            m.central.connection_poll.p50_us,
            m.central.connection_poll.p95_us,
            m.central.connection_poll.p99_us,
            m.central.connection_poll.max_us,
            m.central.connection_poll.samples,
            m.central.connection_poll.slow_count,
            m.central.swarm_poll.p50_us,
            m.central.swarm_poll.p95_us,
            m.central.swarm_poll.p99_us,
            m.central.swarm_poll.max_us,
            m.central.swarm_poll.samples,
            m.central.swarm_poll.slow_count,
            m.central.central_task.mean_poll_duration_us,
            m.central.central_task.mean_scheduled_duration_us,
            m.central.central_task.total_poll_count,
            m.central.central_task.total_slow_poll_count,
            m.central.central_task.mean_slow_poll_duration_us,
            m.central.connection_tasks.mean_poll_duration_us,
            m.central.connection_tasks.mean_scheduled_duration_us,
            m.central.connection_tasks.total_poll_count,
            m.central.connection_tasks.total_slow_poll_count,
            m.central.connection_tasks.mean_slow_poll_duration_us,
            total_rps,
        ));
    }
    fs::write(&per_run, pr)?;
    Ok(())
}
