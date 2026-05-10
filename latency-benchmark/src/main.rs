use std::error::Error;

use clap::{Parser, Subcommand};
use latency_benchmark::{central, orchestrator, peer};
use tokio_metrics::TaskMonitor;

#[derive(Parser)]
#[command(
    name = "latency-benchmark",
    about = "Real-network bench for libp2p Connection::poll latency under high RTT."
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run as a peer: listen on TCP, echo every request, print listen multiaddr to stdout.
    Peer(peer::PeerArgs),
    /// Run as the central node: dial peers, run closed-loop workload, emit metrics JSON.
    Central(central::CentralArgs),
    /// Orchestrator: set up netns + tc + spawn peers + central, collect metrics, tear down.
    Run(orchestrator::RunArgs),
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Peer(args) => {
            let threads = args.worker_threads;
            async_run(threads, || peer::run(args))
        }
        Cmd::Central(args) => {
            let threads = args.worker_threads;
            // Wrap the entire central run with its own TaskMonitor so we can
            // measure the main task's poll/scheduler stats alongside the
            // per-connection-task stats that central.rs collects internally.
            let central_monitor = TaskMonitor::new();
            let monitor = central_monitor.clone();
            async_run(threads, move || {
                monitor.instrument(central::run(args, central_monitor))
            })
        }
        Cmd::Run(args) => orchestrator::run(args),
    }
}

fn async_run<F, Fut>(worker_threads: usize, make_fut: F) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn Error + Send + Sync>>>,
{
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    if worker_threads > 0 {
        builder.worker_threads(worker_threads);
    }
    let rt = builder.enable_all().build()?;
    rt.block_on(make_fut())
}
