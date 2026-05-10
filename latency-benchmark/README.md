# `latency-benchmark`

A real-network measurement tool for `libp2p`'s `Connection::poll`
cooperative-scheduling behaviour under network latency. It runs a single
long-lived-stream workload over a real TCP + `tc netem` stack and measures how
long a single `Connection::poll` monopolises the worker as RTT, message size,
and throughput vary.

The default `bench.sh` run is **comprehensive** — it sweeps the full
RTT × message-size × throughput grid, repeats each cell 3× and reports the
median, and renders the whole multi-dimensional picture. Cells whose offered
load (`msg_size / heartbeat`) exceeds a realistic single-stream rate are pruned
(see `--max-offered-mbps`), so the grid spends its time on cells that matter. For
a tight edit → measure → look loop, narrow any axis with flags (e.g.
`--rtt-ms 100 --msg-size 262144 --heartbeat-ms 10 --runs-per-cell 1` is 1 cell, a
few seconds).

## The workload (one scenario)

Two nodes, **one long-lived `libp2p-stream`**, over a real TCP + `tc netem`
stack:

- **Server** (`peer`, in its own netns): accepts the stream and echoes framed
  messages.
- **Client** (`client`, in the host netns): dials the server, opens one stream,
  sends a `msg_size` message every `heartbeat_ms` (open-loop), drains the
  echoes, and records `Connection::poll` durations over the measurement window.

Each process runs a single-worker `tokio` runtime, `taskset`-pinned to its own
core, so poll durations describe one worker's behaviour without runtime
parallelism washing out the signal.

## The metrics

The tool measures two things per cell:

- **`Connection::poll` CPU time** — mean / p50 / p90 / p95 / p99 / max of the
  **thread CPU time** a single poll consumes, in µs at sub-µs resolution. CPU
  time (not wall-clock) is the work a poll actually does: a poll is synchronous,
  so the CPU it burns is the work it does; off-CPU time (preemption, softirqs,
  faults) isn't work the muxer can shorten and is excluded on purpose. A long
  poll monopolises the worker when yamux drains a whole network burst in one
  unbounded poll, and that cost **grows with message size**: large bursts see
  high p99. `Swarm::poll` (and the `handler` / `muxer` sub-phases of
  `Connection::poll`) are recorded too, for context.
- **Achieved goodput** (`recv_mbps`) — echoed payload received per second over
  the measurement window.

## How to run

```bash
./latency-benchmark/bench.sh            # full default grid (60 cells), ~45 s
```

`bench.sh` builds the binary (un-elevated), prompts for sudo once, runs the
parallel scenario grid, `chown`s the output back to you, writes
`data/per_run.csv`, and renders charts (using the matplotlib venv at
`/tmp/lbplot` if present, else the active `python3`).

It prints a per-cell table of `Connection::poll` percentiles and goodput:

```
Connection::poll CPU time (µs) + goodput  (60 cell(s), 8 wave(s), 4s measure)
  rtt      size    hb   samples   connP99    muxP99  |   MB/s
------------------------------------------------------------------------
  100   256KiB    10       447     541.0     512.0  |   24.1
  ...
------------------------------------------------------------------------
```

## The grid (all flags pass straight through `bench.sh`)

The grid + parallelism live in the Rust `run` command. The default sweeps the
full **RTT × message size × throughput** product, then prunes unrealistic cells:

- `--rtt-ms 0,10,50,100,200` — loopback → LAN → regional → cross-continent.
- `--msg-size 4096,16384,65536,262144` — 4 / 16 / 64 / 256 KiB.
- `--heartbeat-ms 5,10,20,50` — the throughput knob (offered load ≈ size /
  heartbeat).
- `--max-offered-mbps 20` — **prune** any (size, heartbeat) whose offered load
  exceeds 20 MB/s. A single TCP stream over a lossy/high-RTT WAN can't sustain
  much more regardless of the muxer, and those saturating cells are the noisiest
  (flow-control stalls, throughput collapse). Pruning frees the grid to cover a
  wider heartbeat list cheaply. `0` disables the cap.
- `--runs-per-cell 3` — repeat each cell 3× and report the **median** (single-run
  poll tails are noisy for medium message sizes). Repeats fill idle slots in a
  wave, so they're ~free until total jobs exceed the parallelism cap. Drop to 1
  for the quickest pass.
- `--warmup-secs 0.5 --measurement-secs 3.0`

Scenarios run **concurrently**, each on a disjoint netns index and a disjoint
pair of cores (server core + client core). Concurrency is capped at
`floor(available_parallelism / 2)` — a **hard ceiling**, since each scenario is
two processes needing a dedicated core each (`--max-parallel` can only lower it).
Larger grids run in successive waves. On a 16-core box the pruned default is
~140 cells / ~18 waves, ~75 s.

Data quality is enforced three ways: **repeats are medianed** per cell (the table
shows the median of the N runs; the CSV keeps every repeat and `plot.py` medians
across them); a scenario that stalls to **0 poll samples** (a transient reset) is
dropped with a warning rather than recorded as a fake zero; and cells whose
summed samples fall below 200 are marked `*` in the table (and hidden from the
charts below 50) — their p99/max are statistically thin. Raise `--runs-per-cell`
or `--measurement-secs`, or use a faster `--heartbeat-ms`, to firm those up.

```bash
# fast iteration loop: one cell, single run — a few seconds
./latency-benchmark/bench.sh --rtt-ms 100 --msg-size 262144 --heartbeat-ms 10 --runs-per-cell 1

# even wider survey (more heartbeats; the cap keeps it realistic)
./latency-benchmark/bench.sh --rtt-ms 0,25,50,100,200 \
  --msg-size 16384,65536,262144 --heartbeat-ms 5,10,20,50,100

# allow saturation (disable the cap) to see the high-throughput regime
./latency-benchmark/bench.sh --max-offered-mbps 0 --heartbeat-ms 1,10
```

## The charts

`plot.py` renders the multi-dimensional picture — every axis at once:

- **`grid_poll_p99.png`** — the master view. Small-multiples (one subplot per
  `(size, heartbeat)`): `Connection::poll` p99 (solid) and p50 (dashed) vs RTT.
  All three axes in one figure.
- **`grid_throughput.png`** — same layout for achieved goodput.

Plus per-cell drill-downs (`poll_vs_rtt_sz<size>_hb<hb>.png` and
`poll_percentiles_sz<size>_hb<hb>_rtt<rtt>.png`) for zooming into a single cell.

## `per_run.csv`

One row per scenario:

```
rtt_ms,msg_size,heartbeat_ms,recv_mbps,
poll_mean_us,poll_p50_us,poll_p90_us,poll_p95_us,poll_p99_us,poll_max_us,poll_samples,poll_slow_count,
muxer_p50_us,muxer_p90_us,muxer_p99_us,muxer_max_us,muxer_samples,
handler_p50_us,handler_p90_us,handler_p99_us,handler_max_us,handler_samples,
swarm_poll_mean_us,swarm_poll_p50_us,swarm_poll_p90_us,swarm_poll_p95_us,swarm_poll_p99_us,swarm_poll_max_us,swarm_poll_samples
```

## How the measurement works

`Connection::poll` / `Swarm::poll` CPU times come from a
`tracing_subscriber::Layer` that reads the calling thread's CPU clock
(`CLOCK_THREAD_CPUTIME_ID`) on entry/exit of the existing
`#[tracing::instrument(name = "Connection::poll")]` / `"Swarm::poll"` spans into
`hdrhistogram`s; goodput is the client tallying echoed bytes received inside the
measurement window. No changes to published crates, no `tokio_unstable`, no
custom kernel.

## Privileges

`sudo` (or `CAP_NET_ADMIN` + `CAP_SYS_ADMIN`) is required — every scenario gets
its own netns + veth + `tc netem` qdisc, including at RTT=0 (so every cell
traverses the same qdisc shape). Won't work in CI; this is a local/manual tool.

## Layout

```
latency-benchmark/
├── README.md           — this file
├── bench.sh            — thin wrapper: build, sudo run, chown, plot
├── plot.py             — chart renderer (matplotlib): grids + per-cell drill-downs
└── src/
    ├── main.rs         — clap dispatch: peer / client / run
    ├── swarm.rs         — TCP+plaintext+yamux transport + stream swarm + framed I/O
    ├── peer.rs          — server role (echo over one long-lived stream)
    ├── client.rs        — client role (heartbeat sender + poll-time JSON record)
    ├── orchestrator.rs  — run role (parallel scenario grid over netns + tc, table + CSV)
    ├── metrics.rs       — serde types (HistSummary + ScenarioRecord)
    └── poll_timer.rs    — tracing-layer Connection::poll / Swarm::poll histograms
```

## Cleanup if a previous run left state behind

```bash
sudo pkill -9 -f latency-benchmark || true
sudo bash -c 'for i in $(seq 0 63); do ip link del "latb-c$i" 2>/dev/null; ip netns del "latb-peer-$i" 2>/dev/null; done'
```
