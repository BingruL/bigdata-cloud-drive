#!/usr/bin/env python3
"""
读取 reports/*.csv 实验数据，生成实验报告里要用的图表。

输出到 reports/figures/ 下：
  - scale_step_seconds.png       规模实验各步骤耗时柱状图
  - scale_step_throughput.png    规模实验吞吐对比
  - batch_size_throughput.png    batch_size 与吞吐折线图
  - batch_size_speedup.png       batch_size 加速比柱状图
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
FIG_DIR = ROOT / "reports" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

plt.rcParams["axes.unicode_minus"] = False
for family in ("Noto Sans CJK SC", "WenQuanYi Zen Hei", "Source Han Sans SC",
               "PingFang SC", "Microsoft YaHei", "Arial Unicode MS"):
    if any(family.lower() in f.name.lower()
           for f in matplotlib.font_manager.fontManager.ttflist):
        plt.rcParams["font.sans-serif"] = [family]
        break


def read_csv(path):
    rows = []
    with Path(path).open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def plot_scale_step_seconds(rows):
    target_label = "scale_20260512_121734"
    subset = [r for r in rows if r["label"] == target_label and r["status"] == "ok"]
    if not subset:
        print("scale benchmark target label not found; skipped")
        return
    name_map = {
        "hbase_file_metadata": "HBase metadata\n(100K rows / 1GB)",
        "hbase_operation_logs": "HBase logs\n(1M rows)",
        "hdfs_payload": "HDFS payload\n(1GB / 16 files)",
        "spark_file_stats": "Spark file_stats\n(100K + 1M)",
        "spark_tag_index": "Spark tag_index\n(100K files)",
    }
    items = [(name_map.get(r["step"], r["step"]), float(r["seconds"])) for r in subset]
    labels = [x[0] for x in items]
    seconds = [x[1] for x in items]
    colors = ["#0B6FD8", "#0B6FD8", "#22B07A", "#F0843B", "#F0843B"]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    bars = ax.bar(labels, seconds, color=colors, edgecolor="white")
    for bar, val in zip(bars, seconds):
        ax.text(bar.get_x() + bar.get_width() / 2, val,
                f"{val:.2f}s", ha="center", va="bottom", fontsize=10)
    ax.set_ylabel("Elapsed time (s)")
    ax.set_title("Scale benchmark: per-step elapsed time")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(top=max(seconds) * 1.18)
    fig.tight_layout()
    out = FIG_DIR / "scale_step_seconds.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def plot_scale_throughput(rows):
    target_label = "scale_20260512_121734"
    subset = [r for r in rows if r["label"] == target_label and r["status"] == "ok"
              and float(r.get("rows_per_sec") or 0) > 0]
    if not subset:
        print("scale throughput data missing; skipped")
        return
    name_map = {
        "hbase_file_metadata": "HBase metadata write",
        "hbase_operation_logs": "HBase logs write",
        "hdfs_payload": "HDFS payload write",
    }
    items = []
    for r in subset:
        label = name_map.get(r["step"])
        if not label:
            continue
        rows_per_sec = float(r.get("rows_per_sec") or 0)
        mb_per_sec = float(r.get("mb_per_sec") or 0)
        items.append((label, rows_per_sec, mb_per_sec))
    labels = [x[0] for x in items]
    rps = [x[1] for x in items]
    mbps = [x[2] for x in items]

    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    x = range(len(labels))
    width = 0.35
    bar1 = ax1.bar([i - width / 2 for i in x], rps, width=width,
                   color="#0B6FD8", label="rows / second")
    ax1.set_ylabel("rows / second", color="#0B6FD8")
    ax1.tick_params(axis="y", labelcolor="#0B6FD8")
    ax2 = ax1.twinx()
    bar2 = ax2.bar([i + width / 2 for i in x], mbps, width=width,
                   color="#F0843B", label="MB / second")
    ax2.set_ylabel("MB / second", color="#F0843B")
    ax2.tick_params(axis="y", labelcolor="#F0843B")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(labels, rotation=10)
    ax1.set_title("Scale benchmark: write throughput")
    for bar, val in zip(bar1, rps):
        ax1.text(bar.get_x() + bar.get_width() / 2, val,
                 f"{val:,.0f}", ha="center", va="bottom", fontsize=9, color="#0B6FD8")
    for bar, val in zip(bar2, mbps):
        if val > 0:
            ax2.text(bar.get_x() + bar.get_width() / 2, val,
                     f"{val:,.1f}", ha="center", va="bottom", fontsize=9, color="#F0843B")
    ax1.spines["top"].set_visible(False)
    ax2.spines["top"].set_visible(False)
    fig.tight_layout()
    out = FIG_DIR / "scale_step_throughput.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def plot_batch_size_throughput(rows):
    if not rows:
        print("batch size data missing; skipped")
        return
    # Use the last label group only (latest run)
    last_label = rows[-1]["label"]
    subset = [r for r in rows if r["label"] == last_label]
    subset.sort(key=lambda r: int(r["batch_size"]))
    sizes = [int(r["batch_size"]) for r in subset]
    rps = [float(r["rows_per_sec"]) for r in subset]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.plot(sizes, rps, marker="o", color="#0B6FD8", linewidth=2)
    for x, y in zip(sizes, rps):
        ax.text(x, y, f"{y:,.0f}", ha="center", va="bottom", fontsize=9, color="#0B6FD8")
    ax.set_xscale("log")
    ax.set_xlabel("batch_size (log scale)")
    ax.set_ylabel("rows / second")
    ax.set_title("HBase put throughput vs batch_size")
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(bottom=0, top=max(rps) * 1.18)
    fig.tight_layout()
    out = FIG_DIR / "batch_size_throughput.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def plot_batch_size_speedup(rows):
    if not rows:
        print("batch size data missing; skipped")
        return
    last_label = rows[-1]["label"]
    subset = [r for r in rows if r["label"] == last_label]
    subset.sort(key=lambda r: int(r["batch_size"]))
    sizes = [str(r["batch_size"]) for r in subset]
    speedups = [float(r["speedup_vs_b1"]) if r["speedup_vs_b1"] else 0 for r in subset]

    fig, ax = plt.subplots(figsize=(8, 4.5))
    colors = ["#94A3B8" if int(s) == 1 else "#0B6FD8" for s in sizes]
    bars = ax.bar(sizes, speedups, color=colors, edgecolor="white")
    for bar, val in zip(bars, speedups):
        ax.text(bar.get_x() + bar.get_width() / 2, val,
                f"{val:.2f}x", ha="center", va="bottom", fontsize=10)
    ax.set_xlabel("batch_size")
    ax.set_ylabel("speedup vs batch_size=1")
    ax.set_title("HBase batch put: speedup over single-row put")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_ylim(top=max(speedups) * 1.15)
    fig.tight_layout()
    out = FIG_DIR / "batch_size_speedup.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def main():
    scale = read_csv(ROOT / "reports" / "scale-benchmark.csv")
    batch = read_csv(ROOT / "reports" / "batch-size-benchmark.csv")
    plot_scale_step_seconds(scale)
    plot_scale_throughput(scale)
    plot_batch_size_throughput(batch)
    plot_batch_size_speedup(batch)


if __name__ == "__main__":
    main()
