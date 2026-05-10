#!/usr/bin/env python3
"""Render multi-dimensional charts from `data/per_run.csv`.

The benchmark sweeps three axes — **RTT × message size × throughput
(heartbeat)** — and measures two things per cell: `Connection::poll` CPU-time
percentiles and achieved goodput. These charts are built to show all three axes
at once so the takeaways are visible without reading the raw table:

  grid_poll_p99.png            small-multiples: poll p99 (& p50) vs RTT,
                               one subplot per (size, heartbeat).
                               The master view — every axis in one figure.
  grid_throughput.png          same layout for goodput.
  poll_vs_rtt_*.png / poll_percentiles_*.png   per-cell drill-downs (as before).

Usage:
    python3 plot.py <base_dir>     # e.g. latency-benchmark/
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


def median_for(rows, key, metric):
    """Median of `metric` over rows matching every (col, value) in `key`."""
    vals = []
    for r in rows:
        if all(str(r[k]) == str(v) for k, v in key.items()):
            try:
                vals.append(float(r[metric]))
            except (KeyError, ValueError):
                pass
    return statistics.median(vals) if vals else None


def cell(rows, size, hb, rtt, metric):
    return median_for(
        rows,
        {"msg_size": size, "heartbeat_ms": hb, "rtt_ms": rtt},
        metric,
    )


def fmt_size(n):
    n = int(n)
    if n % (1024 * 1024) == 0:
        return f"{n // (1024 * 1024)} MiB"
    if n % 1024 == 0:
        return f"{n // 1024} KiB"
    return f"{n} B"


def axes_vals(rows):
    sizes = sorted({int(r["msg_size"]) for r in rows})
    hbs = sorted({int(r["heartbeat_ms"]) for r in rows})
    rtts = sorted({int(r["rtt_ms"]) for r in rows})
    return sizes, hbs, rtts


# ── small-multiples line grids (rows = size, cols = heartbeat) ────────────────


def plot_line_grid(rows, series, ylabel, title, out_path):
    """Grid of subplots (row per msg_size, col per heartbeat). Each subplot plots
    `series` vs RTT. `series` = list of (metric, linestyle, label_suffix)."""
    plt = setup_plt()
    sizes, hbs, rtts = axes_vals(rows)
    if not (sizes and hbs and rtts):
        return
    fig, axes = plt.subplots(
        len(sizes), len(hbs),
        figsize=(max(4.5 * len(hbs), 5), max(3.2 * len(sizes), 3.5)),
        squeeze=False, sharex=True,
    )
    handles = {}
    for i, size in enumerate(sizes):
        for j, hb in enumerate(hbs):
            ax = axes[i][j]
            for si, (metric, style, suf) in enumerate(series):
                xs, ys = [], []
                for rtt in rtts:
                    v = cell(rows, size, hb, rtt, metric)
                    if v is not None:
                        xs.append(rtt)
                        ys.append(v)
                if xs:
                    lbl = suf if suf else metric
                    (line,) = ax.plot(
                        xs, ys, marker="o", linestyle=style, color=f"C{si}", label=lbl
                    )
                    handles.setdefault(lbl, line)
            ax.set_title(f"{fmt_size(size)}, hb={hb} ms", fontsize=9)
            ax.grid(True, linestyle="--", alpha=0.4)
            if i == len(sizes) - 1:
                ax.set_xlabel("RTT (ms)")
            if j == 0:
                ax.set_ylabel(ylabel, fontsize=8)
    fig.suptitle(title, y=0.995, fontsize=10)
    if handles:
        fig.legend(handles.values(), handles.keys(), loc="upper center",
                   bbox_to_anchor=(0.5, 0.955), ncol=min(4, len(handles)), fontsize=8)
    fig.tight_layout(rect=(0, 0, 1, 0.90))
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


# ── per-cell drill-downs (kept for zooming into a single size/heartbeat) ──────


def plot_poll_vs_rtt(rows, size, hb, out_path):
    plt = setup_plt()
    _, _, rtts = axes_vals(rows)
    fig, ax = plt.subplots(figsize=(8, 5))
    drew = False
    for mi, (metric, style) in enumerate([("poll_p50_us", "-"), ("poll_p99_us", "--")]):
        xs, ys = [], []
        for rtt in rtts:
            v = cell(rows, size, hb, rtt, metric)
            if v is not None:
                xs.append(rtt)
                ys.append(v)
        if xs:
            drew = True
            ax.plot(xs, ys, marker="o", linestyle=style, color=f"C{mi}",
                    label=metric.split("_")[1])
    if not drew:
        plt.close(fig)
        return
    ax.set_xlabel("RTT (ms)")
    ax.set_ylabel("Connection::poll CPU time (µs)")
    ax.set_title(f"Connection::poll vs RTT — msg {fmt_size(size)}, heartbeat {hb} ms\n"
                 "solid = p50, dashed = p99")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(fontsize=8)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_poll_percentiles_bars(rows, size, hb, rtt, out_path):
    plt = setup_plt()
    import numpy as np

    pcts = [("poll_mean_us", "mean"), ("poll_p50_us", "p50"), ("poll_p90_us", "p90"),
            ("poll_p95_us", "p95"), ("poll_p99_us", "p99")]
    fig, ax = plt.subplots(figsize=(8, 5))
    x = np.arange(len(pcts))
    ys = [cell(rows, size, hb, rtt, m) or 0.0 for m, _ in pcts]
    if not any(ys):
        plt.close(fig)
        return
    ax.bar(x, ys, 0.6)
    ax.set_xticks(x)
    ax.set_xticklabels([lbl for _, lbl in pcts])
    ax.set_ylabel("Connection::poll CPU time (µs)")
    ax.set_title(f"Connection::poll percentiles — msg {fmt_size(size)}, "
                 f"heartbeat {hb} ms, RTT={rtt} ms")
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_poll_heatmaps(rows, metric, label, title, out_path, scale=1.0):
    """Faceted heatmaps comparing one metric across heartbeats. One subplot per
    heartbeat; within each, y = message size, x = RTT, cell = median `metric`
    over reps (× `scale`, annotated, so it reads as a table). All subplots share
    one colour scale + colorbar, so the same colour means the same value across
    heartbeats."""
    plt = setup_plt()
    import numpy as np

    sizes, hbs, rtts = axes_vals(rows)
    if not (sizes and hbs and rtts):
        return

    grids = {}
    vmin = vmax = None
    for hb in hbs:
        g = np.full((len(sizes), len(rtts)), np.nan)
        for i, size in enumerate(sizes):
            for j, rtt in enumerate(rtts):
                v = cell(rows, size, hb, rtt, metric)
                if v is not None:
                    v *= scale
                    g[i, j] = v
                    vmin = v if vmin is None else min(vmin, v)
                    vmax = v if vmax is None else max(vmax, v)
        grids[hb] = g
    if vmin is None:
        return
    span = (vmax - vmin) or 1.0

    n = len(hbs)
    ncols = min(n, 4)
    nrows = -(-n // ncols)  # ceil — wrap to a grid so many heartbeats stay legible
    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(max(2.8 * ncols + 1.4, 5), max(2.4 * nrows + 1.2, 3.2)),
        squeeze=False, constrained_layout=True,
    )
    axflat = axes.ravel()
    cmap = plt.get_cmap("viridis").copy()
    cmap.set_bad("0.85")  # missing/too-thin cells in light grey
    im = None
    for idx, hb in enumerate(hbs):
        ax = axflat[idx]
        g = grids[hb]
        im = ax.imshow(np.ma.masked_invalid(g), aspect="auto", cmap=cmap,
                       vmin=vmin, vmax=vmax, origin="upper")
        ax.set_xticks(range(len(rtts)))
        ax.set_xticklabels([str(r) for r in rtts])
        ax.set_xlabel("RTT (ms)")
        ax.set_title(f"hb = {hb} ms", fontsize=9)
        ax.set_yticks(range(len(sizes)))
        if idx % ncols == 0:
            ax.set_yticklabels([fmt_size(s) for s in sizes])
            ax.set_ylabel("message size")
        else:
            ax.set_yticklabels([])
        for i in range(len(sizes)):
            for k in range(len(rtts)):
                val = g[i, k]
                if not np.isnan(val):
                    norm = (val - vmin) / span
                    ax.text(k, i, f"{val:.0f}", ha="center", va="center",
                            fontsize=7, color=("black" if norm > 0.55 else "white"))
    for j in range(n, len(axflat)):
        axflat[j].axis("off")  # hide unused grid slots
    fig.suptitle(title, fontsize=11)
    cbar = fig.colorbar(im, ax=axflat.tolist(), shrink=0.9, pad=0.02)
    cbar.set_label(label)
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

    all_rows = load(os.path.join(data_dir, "per_run.csv"))
    if not all_rows or "rtt_ms" not in all_rows[0]:
        print("per_run.csv missing or lacks the rtt_ms column", file=sys.stderr)
        return
    # No sample-count filtering: chart every cell that produced data. The
    # orchestrator already drops genuinely-stalled 0-sample cells upstream, so
    # everything in per_run.csv is real, and the long measurement window means
    # every cell has plenty of samples.
    rows = all_rows
    sizes, hbs, rtts = axes_vals(rows)
    hi_rtt = max(rtts)
    has_tp = "recv_mbps" in rows[0]

    # Master comprehensive views.
    plot_line_grid(
        rows,
        [("poll_p99_us", "-", "p99"), ("poll_p50_us", "--", "p50")],
        "poll (µs)",
        "Connection::poll latency vs RTT — solid = p99, dashed = p50",
        os.path.join(out_dir, "grid_poll_p99.png"),
    )
    if has_tp:
        plot_line_grid(
            rows, [("recv_mbps", "-", "goodput")], "goodput (MB/s)",
            "Achieved goodput vs RTT",
            os.path.join(out_dir, "grid_throughput.png"),
        )

    # Consolidated p99 view: RTT × message-size tables, one per heartbeat,
    # shared colour scale — replaces the per-(size,heartbeat) drill-down charts.
    plot_poll_heatmaps(
        rows, "poll_p99_us", "poll p99 (µs)",
        "Connection::poll p99 latency — RTT × message size, faceted by heartbeat",
        os.path.join(out_dir, "heatmap_poll_p99.png"),
    )
    plot_poll_heatmaps(
        rows, "poll_max_us", "poll max (µs)",
        "Connection::poll max latency — RTT × message size, faceted by heartbeat",
        os.path.join(out_dir, "heatmap_poll_max.png"),
    )
    if "wire_rtt_max_us" in all_rows[0]:
        wire_rows = all_rows
        for stat in ("p50", "p99", "max"):
            plot_poll_heatmaps(
                wire_rows, f"wire_rtt_{stat}_us", f"{stat} round-trip (ms)",
                f"Application round-trip latency ({stat}) — RTT × message size, "
                "faceted by heartbeat",
                os.path.join(out_dir, f"heatmap_wire_rtt_{stat}.png"), scale=0.001,
            )


if __name__ == "__main__":
    main()
