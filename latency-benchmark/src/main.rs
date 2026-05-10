use std::error::Error;

use clap::{Parser, Subcommand};
use latency_benchmark::{client, orchestrator, peer};

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
    /// Run as the server: listen on TCP, echo every message, print listen multiaddr to stdout.
    Peer(peer::PeerArgs),
    /// Run as the client: dial the server, drive the heartbeat workload, emit a scenario JSON
    /// record.
    Client(client::ClientArgs),
    /// Orchestrator: build the grid, run scenarios in parallel over netns + tc, collect + report.
    Run(orchestrator::RunArgs),
}

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Peer(args) => {
            let threads = args.worker_threads;
            async_run(threads, || peer::run(args))
        }
        Cmd::Client(args) => {
            let threads = args.worker_threads;
            async_run(threads, || client::run(args))
        }
        Cmd::Run(args) => orchestrator::run(args),
    }
}

fn async_run<F, Fut>(worker_threads: usize, make_fut: F) -> Result<(), Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), Box<dyn Error + Send + Sync>>>,
{
    // `worker_threads == 1` (the default for both nodes) builds a *current-thread*
    // runtime: every task, the I/O reactor and timers all run on ONE thread, so a
    // long `Connection::poll` blocks the sender, reader and timers too — exactly
    // the single-core contention that makes scheduling issues obvious.
    // `0` = multi-threaded over all cores; `> 1` = that many worker threads.
    let rt = if worker_threads == 1 {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if worker_threads > 0 {
            builder.worker_threads(worker_threads);
        }
        builder.enable_all().build()?
    };
    rt.block_on(make_fut())
}
