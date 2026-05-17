#!/usr/bin/env python3
"""Render headline charts from `data/per_run.csv`.

Usage:
    python3 plot.py <base_dir>   # e.g. latency-benchmark/

Reads:
  - <base>/data/per_run.csv: row per run with central poll / wake / total_rps.

Writes PNGs into <base>/charts/. Aggregates across runs with median.
"""
import csv
import os
import statistics
import sys


def load(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def setup_plt():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def median_or_nan(values):
    return statistics.median(values) if values else float("nan")


def annotate_slow_poll_ratios(rows):
    """Add slow-poll-ratio columns (in %) to each row.

    `cn_slow_poll_ratio_pct` / `ct_slow_poll_ratio_pct` come from the
    TaskMonitor counters (`{prefix}_total_slow_poll_count` /
    `{prefix}_total_poll_count`).

    `poll_slow_ratio_pct` / `swarm_poll_slow_ratio_pct` come from the
    tracing-layer histograms (`{prefix}_slow_count` / `{prefix}_samples`).
    The tracing-layer numbers are the only ones available for
    `Swarm::poll`, since the central task's TaskMonitor wraps the entire
    central loop (Swarm::poll + dial bookkeeping + request driver) and
    doesn't isolate Swarm::poll specifically.
    """
    for r in rows:
        for prefix in ("cn", "ct"):
            total = int(r.get(f"{prefix}_total_poll_count", 0) or 0)
            slow = int(r.get(f"{prefix}_total_slow_poll_count", 0) or 0)
            r[f"{prefix}_slow_poll_ratio_pct"] = (100.0 * slow / total) if total else 0.0
        for prefix in ("poll", "swarm_poll"):
            samples = int(r.get(f"{prefix}_samples", 0) or 0)
            slow = int(r.get(f"{prefix}_slow_count", 0) or 0)
            r[f"{prefix}_slow_ratio_pct"] = (100.0 * slow / samples) if samples else 0.0


def _heatmap(per_run_rows, *, metric, value_fmt, vmax, title, cbar_label, out_path):
    """Render an (N × RTT) heatmap for a single payload, colouring `metric`."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    rtts = sorted({int(r["rtt_ms"]) for r in per_run_rows})
    ns = sorted({int(r["n_peers"]) for r in per_run_rows})
    if not rtts or not ns:
        return
    grid = np.full((len(ns), len(rtts)), np.nan)
    for r in per_run_rows:
        i = ns.index(int(r["n_peers"]))
        j = rtts.index(int(r["rtt_ms"]))
        grid[i, j] = float(r[metric])
    fig, ax = plt.subplots(figsize=(1.2 * len(rtts) + 3, 0.7 * len(ns) + 3))
    cmap = plt.get_cmap("YlOrRd")
    im = ax.imshow(grid, aspect="auto", origin="lower", cmap=cmap, vmin=0, vmax=vmax)
    ax.set_xticks(range(len(rtts)))
    ax.set_xticklabels([f"{r}ms" for r in rtts])
    ax.set_yticks(range(len(ns)))
    ax.set_yticklabels([str(n) for n in ns])
    ax.set_xlabel("RTT")
    ax.set_ylabel("N peers")
    ax.set_title(title)
    white_text_above = vmax * 0.4
    for i in range(len(ns)):
        for j in range(len(rtts)):
            v = grid[i, j]
            if not np.isnan(v):
                ax.text(
                    j,
                    i,
                    value_fmt.format(v),
                    ha="center",
                    va="center",
                    color="white" if v > white_text_above else "black",
                    fontsize=9,
                )
    fig.colorbar(im, ax=ax, label=cbar_label)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def slow_poll_heatmap(per_run_rows, out_path):
    """Heatmap of poll_slow_ratio_pct over (N × RTT) for a single payload.

    `Connection::poll` slow-poll ratio from the tracing-layer histogram
    (`poll_slow_count / poll_samples  8. metrics.rs:53 doc comment: "default 50 ms" — should be 50 µs.
`). Each sample is one invocation of
    the `#[tracing::instrument(name = "Connection::poll")]` span, so this
    is *the* metric the README's narrative is about — apples-to-apples
    with the Swarm::poll heatmap that uses the same instrumentation.
    """
    if not per_run_rows:
        return
    payload = int(per_run_rows[0]["payload"])
    _heatmap(
        per_run_rows,
        metric="poll_slow_ratio_pct",
        value_fmt="{:.1f}%",
        vmax=30,
        title=(
            f"Connection::poll slow-poll ratio (%) — payload {payload} B\n"
            "fraction of polls > 50 µs (tokio_metrics' DEFAULT_SLOW_POLL_THRESHOLD)"
        ),
        cbar_label="slow-poll ratio (%)",
        out_path=out_path,
    )


def swarm_slow_poll_heatmap(per_run_rows, out_path):
    """Heatmap of swarm_poll_slow_ratio_pct over (N × RTT) for a single payload.

    Swarm::poll slow-poll ratio from the tracing-layer histogram:
    `swarm_poll_slow_count / swarm_poll_samples`. Same axes, scale, and
    threshold framing as `slow_poll_heatmap` so the two can be read
    side-by-side; the contrast is the headline.
    """
    if not per_run_rows:
        return
    payload = int(per_run_rows[0]["payload"])
    _heatmap(
        per_run_rows,
        metric="swarm_poll_slow_ratio_pct",
        value_fmt="{:.1f}%",
        vmax=30,
        title=(
            f"Swarm::poll slow-poll ratio (%) — payload {payload} B\n"
            "fraction of polls > 50 µs threshold "
            "(same scale as the Connection::poll heatmap — for contrast)"
        ),
        cbar_label="slow-poll ratio (%)",
        out_path=out_path,
    )


def plot_vs_rtt(per_run, metric, ylabel, title, out_path, fixed_n, hlines=None):
    """X=RTT, lines per payload, at fixed N. Isolates RTT's effect."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    payloads = sorted({int(r["payload"]) for r in per_run})
    rtts = sorted({int(r["rtt_ms"]) for r in per_run})
    for pl in payloads:
        xs, ys = [], []
        for rtt in rtts:
            matching = [
                r
                for r in per_run
                if int(r["payload"]) == pl
                and int(r["rtt_ms"]) == rtt
                and int(r["n_peers"]) == fixed_n
            ]
            if matching:
                xs.append(rtt)
                ys.append(float(matching[0][metric]))
        ax.plot(xs, ys, marker="o", label=f"payload={pl} B")
    if hlines:
        for y, lbl in hlines:
            ax.axhline(y, color="grey", linestyle=":", alpha=0.6, label=lbl)
    ax.set_xlabel("RTT (ms)")
    ax.set_ylabel(ylabel)
    ax.set_title(f"{title}\n(at N = {fixed_n} peers, lines per payload)")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_vs_payload(per_run, metric, ylabel, title, out_path, fixed_n, hlines=None):
    """X=payload (log scale), lines per RTT, at fixed N. Isolates payload's effect."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 5))
    payloads = sorted({int(r["payload"]) for r in per_run})
    rtts = sorted({int(r["rtt_ms"]) for r in per_run})
    for rtt in rtts:
        xs, ys = [], []
        for pl in payloads:
            matching = [
                r
                for r in per_run
                if int(r["payload"]) == pl
                and int(r["rtt_ms"]) == rtt
                and int(r["n_peers"]) == fixed_n
            ]
            if matching:
                xs.append(pl)
                ys.append(float(matching[0][metric]))
        ax.plot(xs, ys, marker="o", label=f"RTT={rtt}ms")
    if hlines:
        for y, lbl in hlines:
            ax.axhline(y, color="grey", linestyle=":", alpha=0.6, label=lbl)
    ax.set_xscale("log", base=2)
    ax.set_xlabel("Payload (bytes, log₂)")
    ax.set_ylabel(ylabel)
    ax.set_title(f"{title}\n(at N = {fixed_n} peers, lines per RTT)")
    ax.grid(True, which="both", linestyle="--", alpha=0.5)
    ax.legend(fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    base = sys.argv[1]
    data_dir = os.path.join(base, "data")
    out_dir = os.path.join(base, "charts")
    os.makedirs(out_dir, exist_ok=True)

    per_run = load(os.path.join(data_dir, "per_run.csv"))
    if not per_run:
        return
    annotate_slow_poll_ratios(per_run)
    payloads = sorted({int(r["payload"]) for r in per_run})

    # Headline evidence: 2D (N × RTT) slow-poll-ratio landscape per payload.
    # The Swarm::poll heatmap renders on the same scale (same metric, same
    # threshold) so the two can be read side-by-side. The Swarm::poll
    # variant only renders when `swarm_poll_slow_count` is present in the
    # CSV (older bench runs predate that column).
    for pl in payloads:
        sub = [r for r in per_run if int(r["payload"]) == pl]
        suffix = f"_payload{pl}" if len(payloads) > 1 else ""
        slow_poll_heatmap(
            sub, os.path.join(out_dir, f"slow_poll_ratio_heatmap{suffix}.png")
        )
        if sub and "swarm_poll_slow_count" in sub[0]:
            swarm_slow_poll_heatmap(
                sub,
                os.path.join(out_dir, f"swarm_slow_poll_ratio_heatmap{suffix}.png"),
            )

    # Factor isolation at N=10: how each axis shifts the slow-poll ratio.
    fixed_n = 10
    plot_vs_rtt(
        per_run,
        "poll_slow_ratio_pct",
        "Connection::poll slow-poll ratio (%)",
        "Effect of RTT on Connection::poll slow-poll ratio",
        os.path.join(out_dir, "effect_on_slow_ratio_vs_rtt.png"),
        fixed_n,
    )
    plot_vs_payload(
        per_run,
        "poll_slow_ratio_pct",
        "Connection::poll slow-poll ratio (%)",
        "Effect of payload on Connection::poll slow-poll ratio",
        os.path.join(out_dir, "effect_on_slow_ratio_vs_payload.png"),
        fixed_n,
    )


if __name__ == "__main__":
    main()
