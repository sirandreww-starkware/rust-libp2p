#!/usr/bin/env bash
# Sweep `n_peers` × `rtt_ms`, aggregate all per_peer.csv / per_run.csv rows
# into a single top-level CSV, render plots. Designed for one-shot invocation.
#
# Knobs (all env-var overridable):
#   N_PEERS_LIST     space-separated list of N values   (default "1 2 5 10 15")
#   RTT_MS_LIST      space-separated list of RTT values
#                    (default "0 5 10 25 50 100 200"). Every cell — including
#                    RTT=0 — goes through the same netns + veth + `tc netem`
#                    stack, so all cells need root (no smoke-test shortcut).
#   RTT_MS           single-RTT shim; expands to RTT_MS_LIST="$RTT_MS"
#                    (only honoured when RTT_MS_LIST is unset)
#   PAYLOAD_LIST     space-separated request-size list
#                    (default "4096 16384 65536 262144" = 4/16/64/256 KiB)
#   PAYLOAD          single-payload shim; expands to PAYLOAD_LIST="$PAYLOAD"
#                    (only honoured when PAYLOAD_LIST is unset)
#   MEASUREMENT_SECS measurement window per run         (default 15)
#   WARMUP_SECS      warmup per run                     (default 2)
#   N_RUNS           runs per cell                      (default 1)
#   TOPOLOGY         "homogeneous" or "mixed"           (default homogeneous)
#                    - homogeneous: every peer uses --rtt-ms (the RTT axis
#                      value is what each connection's link is actually set
#                      to; histogram numbers attribute cleanly to that RTT).
#                    - mixed: peer 0 is held at RTT=0, peers 1..N at
#                      --rtt-ms. Useful for head-of-line experiments;
#                      contaminates the per-RTT attribution otherwise.
#   OUT_DIR          output base dir                    (default = the crate dir)
#                    => CSVs land in $OUT_DIR/data/, PNGs in $OUT_DIR/charts/.
#                    Set OUT_DIR=/tmp/foo for a non-destructive run that
#                    doesn't touch the committed evidence.
#   BIN              path to the binary                 (default target/release/latency-benchmark)
#
# Usage:
#   ./latency-benchmark/sweep.sh             # default grid, ~15 cells, ~20 min
#   RTT_MS_LIST=0 ./latency-benchmark/sweep.sh   # no-sudo sanity sweep
#   N_PEERS_LIST="1 5" RTT_MS_LIST=50 ./latency-benchmark/sweep.sh   # quick smoke
#   OUT_DIR=/tmp/runX ./latency-benchmark/sweep.sh   # write somewhere else

set -euo pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

N_PEERS_LIST="${N_PEERS_LIST:-1 2 5 10 15}"
# Survey-style default: many RTT values, single run per cell. With a
# longer measurement window per cell, the curve-across-RTT trend
# absorbs single-run noise better than dense per-cell averaging would.
# After seeing the survey, rerun any interesting cell with N_RUNS=5.
RTT_MS_LIST="${RTT_MS_LIST:-${RTT_MS:-0 5 10 25 50 100 200}}"
PAYLOAD_LIST="${PAYLOAD_LIST:-${PAYLOAD:-4096 16384 65536 262144}}"
MEASUREMENT_SECS="${MEASUREMENT_SECS:-15}"
WARMUP_SECS="${WARMUP_SECS:-2}"
N_RUNS="${N_RUNS:-1}"
# Output structure: $OUT_DIR/data/ for CSVs, $OUT_DIR/charts/ for PNGs.
# Default OUT_DIR is the crate dir itself so a fresh sweep refreshes the
# committed evidence in latency-benchmark/{data,charts}.
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR}"
DATA_DIR="$OUT_DIR/data"
CHARTS_DIR="$OUT_DIR/charts"
TOPOLOGY="${TOPOLOGY:-homogeneous}"
BIN="${BIN:-target/release/latency-benchmark}"

# Always rebuild — cargo is a no-op if nothing changed, so this only
# costs something when source has actually moved. Avoids the "stale
# binary after edits" footgun.
echo "==> cargo build --release --package latency-benchmark"
cargo build --release --package latency-benchmark
BIN_ABS="$(realpath "$BIN")"

# Every cell goes through netns + veth + `tc netem`, including RTT=0.
# Prompt for sudo up front so the inner invocations don't have to.
SUDO="sudo"
sudo -v

mkdir -p "$DATA_DIR" "$CHARTS_DIR"
# Stale files from a previous sweep with RTT>0 are owned by root (the
# binary runs under sudo for tc/netns), so a plain `rm` fails with
# permission-denied. Reuse $SUDO when present so we can clean either case.
$SUDO rm -rf "${DATA_DIR:?}"/* "${CHARTS_DIR:?}"/*
start_ts="$(date +%s)"

# Cell labels: pl{payload}_rtt{rtt}_n{n} so subdirs sort sensibly by payload,
# then RTT, then N.
fmt_cell() { printf 'pl%06d_rtt%03d_n%02d' "$1" "$2" "$3"; }

for PAYLOAD_VAL in $PAYLOAD_LIST; do
    for RTT in $RTT_MS_LIST; do
        for N in $N_PEERS_LIST; do
            SUB_DIR="$DATA_DIR/$(fmt_cell "$PAYLOAD_VAL" "$RTT" "$N")"
            echo
            echo "==> payload=$PAYLOAD_VAL bytes, RTT=$RTT ms, N=$N peers, runs=$N_RUNS"
            $SUDO "$BIN_ABS" run \
                --n-peers "$N" \
                --rtt-ms "$RTT" \
                --payload "$PAYLOAD_VAL" \
                --warmup-secs "$WARMUP_SECS" \
                --measurement-secs "$MEASUREMENT_SECS" \
                --n-runs "$N_RUNS" \
                --topology "$TOPOLOGY" \
                --out-dir "$SUB_DIR"
        done
    done
done

echo
echo "==> Aggregating CSVs into $DATA_DIR/{per_peer,per_run}.csv"
first=1
for PAYLOAD_VAL in $PAYLOAD_LIST; do
    for RTT in $RTT_MS_LIST; do
        for N in $N_PEERS_LIST; do
            SUB_DIR="$DATA_DIR/$(fmt_cell "$PAYLOAD_VAL" "$RTT" "$N")"
            if [[ $first -eq 1 ]]; then
                cp "$SUB_DIR/per_peer.csv" "$DATA_DIR/per_peer.csv"
                cp "$SUB_DIR/per_run.csv"  "$DATA_DIR/per_run.csv"
                first=0
            else
                tail -n +2 "$SUB_DIR/per_peer.csv" >> "$DATA_DIR/per_peer.csv"
                tail -n +2 "$SUB_DIR/per_run.csv"  >> "$DATA_DIR/per_run.csv"
            fi
        done
    done
done

# Plot if matplotlib is around. Don't fail the script if it isn't.
echo
if python3 -c 'import matplotlib' 2>/dev/null; then
    echo "==> Rendering charts into $CHARTS_DIR/"
    python3 "$SCRIPT_DIR/plot.py" "$OUT_DIR"
else
    echo "==> python3 + matplotlib not available; skipping charts."
    echo "    Install matplotlib and run: python3 $SCRIPT_DIR/plot.py $OUT_DIR"
fi

elapsed=$(( $(date +%s) - start_ts ))
echo
echo "Done in ${elapsed}s."
echo "  data:   $DATA_DIR/"
echo "  charts: $CHARTS_DIR/"
