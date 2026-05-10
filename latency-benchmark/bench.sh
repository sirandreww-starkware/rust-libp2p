#!/usr/bin/env bash
# Thin wrapper around the Rust `run` orchestrator: build (un-elevated), run the
# parallel scenario grid under sudo (netns + tc need root), hand the root-owned
# output back to the invoking user, and render charts.
#
# The grid + parallelism live in the Rust `run` command — every flag is passed
# straight through, so:
#
#   ./latency-benchmark/bench.sh                       # default grid (~10–20 s)
#   ./latency-benchmark/bench.sh --rtt-ms 0,50,100,200 # widen the RTT axis
#   ./latency-benchmark/bench.sh --msg-size 4096,262144 --heartbeat-ms 1,10
#   ./latency-benchmark/bench.sh --measurement-secs 4  # longer windows
#
# Run `./latency-benchmark/bench.sh --help`* for the full flag list.
# (* via the built binary: target/release/latency-benchmark run --help)

set -euo pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
BIN="${BIN:-target/release/latency-benchmark}"
OUT_DIR="$SCRIPT_DIR"   # data/ and charts/ live under the crate dir

echo "==> cargo build --release --package latency-benchmark"
cargo build --release --package latency-benchmark
BIN_ABS="$(realpath "$BIN")"

# Every scenario goes through netns + veth + `tc netem`, so the whole run needs
# root. Prompt once up front.
sudo -v

# Clean stale outputs from a previous (possibly different-shaped) grid so the
# charts/CSV only reflect this run. `data/` may be root-owned from a prior sudo
# run, so clean it with sudo; `charts/` is user-owned.
sudo rm -rf "$OUT_DIR/data"
rm -rf "$OUT_DIR/charts"

start_ts="$(date +%s)"
sudo "$BIN_ABS" run --out-dir "$OUT_DIR/data" "$@"

# The orchestrator runs under sudo, so its output is root-owned. Hand it back so
# plotting / git / rm don't trip over it.
sudo chown -R "$(id -u):$(id -g)" "$OUT_DIR/data" 2>/dev/null || true
mkdir -p "$OUT_DIR/charts"

# Render charts with the matplotlib venv if present, else the active python3.
echo
PY=""
if [[ -x /tmp/lbplot/bin/python3 ]]; then
    PY=/tmp/lbplot/bin/python3
elif python3 -c 'import matplotlib' 2>/dev/null; then
    PY=python3
fi
if [[ -n "$PY" ]]; then
    echo "==> Rendering charts into $OUT_DIR/charts/"
    "$PY" "$SCRIPT_DIR/plot.py" "$OUT_DIR"
else
    echo "==> matplotlib not available; skipping charts."
    echo "    Install it and run: python3 $SCRIPT_DIR/plot.py $OUT_DIR"
fi

elapsed=$(( $(date +%s) - start_ts ))
echo
echo "Done in ${elapsed}s."
echo "  data:   $OUT_DIR/data/"
echo "  charts: $OUT_DIR/charts/"
