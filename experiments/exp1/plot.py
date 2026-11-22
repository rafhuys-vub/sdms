
# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib"]
# ///

"""
E1 - Plot bucket count experiment results.

Usage:
    uv run experiments/exp1/plot.py experiments/exp1/results/20261122-230653

Generated figures are saved in the selected results directory.
"""

import csv
import sys
from pathlib import Path

import matplotlib.pyplot as plt


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

if len(sys.argv) != 2:
    raise SystemExit(
        "Usage: uv run experiments/exp1/plot.py <results_directory>"
    )

RUN_DIR = Path(sys.argv[1]).resolve()

if not RUN_DIR.is_dir():
    raise SystemExit(f"Results directory does not exist: {RUN_DIR}")

LOAD_FILE = RUN_DIR / "load_times.csv"
SUMMARY_FILE = RUN_DIR / "summary.csv"
BENCHMARK_FILE = RUN_DIR / "benchmark.csv"
TABLET_FILE = RUN_DIR / "tablet_stats.csv"


# ---------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------

def read_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")

    with path.open("r", newline="", encoding="utf-8-sig") as file:
        return list(csv.DictReader(file))


def save_plot(fig, filename):
    output = RUN_DIR / filename
    fig.savefig(output, dpi=160, bbox_inches="tight")
    plt.close(fig)
    print(f"Generated: {output}")


# ---------------------------------------------------------
# Load data
# ---------------------------------------------------------

load_rows = read_csv(LOAD_FILE)
summary_rows = read_csv(SUMMARY_FILE)
benchmark_rows = read_csv(BENCHMARK_FILE)
tablet_rows = read_csv(TABLET_FILE)


# Sort configurations numerically by bucket count.
bucket_counts = sorted({
    int(row["bucket_count"]) for row in load_rows
})

if not bucket_counts:
    raise ValueError("No bucket configurations found.")

# Load times.
load_times = {
    int(row["bucket_count"]): float(row["load_time_seconds"])
    for row in load_rows
}

# Summary statistics indexed by query and bucket count.
summary = {
    (int(row["bucket_count"]), row["query"]): row
    for row in summary_rows
}

# Benchmark measurements indexed by query and bucket count.
measurements = {}

for row in benchmark_rows:
    if int(row["ok"]) != 1:
        continue

    key = (int(row["bucket_count"]), row["query"])
    measurements.setdefault(key, []).append(float(row["latency_ms"]))


# Tablet statistics: one record per backend and bucket configuration.
tablet_by_bucket = {}

for row in tablet_rows:
    bucket = int(row["bucket_count"])

    tablet_by_bucket[bucket] = {
        "logical_tablets": int(row["logical_tablets"]),
        "total_replicas": int(row["total_replicas"]),
    }


# ---------------------------------------------------------
# Common plotting configuration
# ---------------------------------------------------------

plt.rcParams.update({
    "figure.figsize": (10, 6),
    "axes.grid": True,
    "grid.alpha": 0.3,
    "font.size": 10,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
})


# ---------------------------------------------------------
# 1. Load time vs bucket count
# ---------------------------------------------------------

fig, ax = plt.subplots()

times = [load_times[b] for b in bucket_counts]

ax.plot(
    bucket_counts,
    times,
    marker="o",
    linewidth=2,
)

ax.set_xscale("log", base=2)
ax.set_xticks(bucket_counts)
ax.set_xticklabels(bucket_counts)

ax.set_xlabel("Bucket count")
ax.set_ylabel("Load time (seconds)")
ax.set_title("E1 - Data Loading Time vs Bucket Count")

save_plot(fig, "load_time.png")


# ---------------------------------------------------------
# 2. Full scan median and P95 latency
# ---------------------------------------------------------

fig, ax = plt.subplots()

for query, label in [
    ("full_scan", "Median"),
    ("full_scan_p95", "P95"),
]:
    if query == "full_scan_p95":
        continue

median_values = []
p95_values = []

for bucket in bucket_counts:
    row = summary.get((bucket, "full_scan"))

    if row is None:
        median_values.append(float("nan"))
        p95_values.append(float("nan"))
    else:
        median_values.append(float(row["median_ms"]))
        p95_values.append(float(row["p95_ms"]))

ax.plot(
    bucket_counts,
    median_values,
    marker="o",
    linewidth=2,
    label="Median",
)

ax.plot(
    bucket_counts,
    p95_values,
    marker="s",
    linewidth=2,
    linestyle="--",
    label="P95",
)

ax.set_xscale("log", base=2)
ax.set_xticks(bucket_counts)
ax.set_xticklabels(bucket_counts)

ax.set_xlabel("Bucket count")
ax.set_ylabel("Latency (ms)")
ax.set_title("E1 - Full Scan Latency vs Bucket Count")
ax.legend()

save_plot(fig, "full_scan_latency.png")


# ---------------------------------------------------------
# 3. Point query median and P95 latency
# ---------------------------------------------------------

fig, ax = plt.subplots()

median_values = []
p95_values = []

for bucket in bucket_counts:
    row = summary.get((bucket, "point_query"))

    if row is None:
        median_values.append(float("nan"))
        p95_values.append(float("nan"))
    else:
        median_values.append(float(row["median_ms"]))
        p95_values.append(float(row["p95_ms"]))

ax.plot(
    bucket_counts,
    median_values,
    marker="o",
    linewidth=2,
    label="Median",
)

ax.plot(
    bucket_counts,
    p95_values,
    marker="s",
    linewidth=2,
    linestyle="--",
    label="P95",
)

ax.set_xscale("log", base=2)
ax.set_xticks(bucket_counts)
ax.set_xticklabels(bucket_counts)

ax.set_xlabel("Bucket count")
ax.set_ylabel("Latency (ms)")
ax.set_title("E1 - Point Query Latency vs Bucket Count")
ax.legend()

save_plot(fig, "point_query_latency.png")


# ---------------------------------------------------------
# 4. Full scan vs point query comparison
# ---------------------------------------------------------

fig, ax = plt.subplots()

for query, label, marker in [
    ("full_scan", "Full scan", "o"),
    ("point_query", "Point query", "s"),
]:
    medians = []

    for bucket in bucket_counts:
        row = summary.get((bucket, query))

        if row is None:
            medians.append(float("nan"))
        else:
            medians.append(float(row["median_ms"]))

    ax.plot(
        bucket_counts,
        medians,
        marker=marker,
        linewidth=2,
        label=label,
    )

ax.set_xscale("log", base=2)
ax.set_xticks(bucket_counts)
ax.set_xticklabels(bucket_counts)

ax.set_xlabel("Bucket count")
ax.set_ylabel("Median latency (ms)")
ax.set_title("E1 - Query Performance Comparison")
ax.legend()

save_plot(fig, "query_latency_comparison.png")


# ---------------------------------------------------------
# 5. Logical tablets and physical replicas
# ---------------------------------------------------------

fig, ax = plt.subplots()

logical_tablets = [
    tablet_by_bucket[b]["logical_tablets"]
    for b in bucket_counts
]

total_replicas = [
    tablet_by_bucket[b]["total_replicas"]
    for b in bucket_counts
]

ax.plot(
    bucket_counts,
    logical_tablets,
    marker="o",
    linewidth=2,
    label="Logical tablets",
)

ax.plot(
    bucket_counts,
    total_replicas,
    marker="s",
    linewidth=2,
    linestyle="--",
    label="Physical replicas",
)

ax.set_xscale("log", base=2)
ax.set_yscale("log", base=2)

ax.set_xticks(bucket_counts)
ax.set_xticklabels(bucket_counts)

ax.set_xlabel("Bucket count")
ax.set_ylabel("Count")
ax.set_title("E1 - Tablet and Replica Counts")
ax.legend()

save_plot(fig, "tablet_counts.png")


# ---------------------------------------------------------
# 6. Individual query measurement distributions
# ---------------------------------------------------------

fig, ax = plt.subplots()

for query, label in [
    ("full_scan", "Full scan"),
    ("point_query", "Point query"),
]:
    data = [
        measurements.get((bucket, query), [])
        for bucket in bucket_counts
    ]

    positions = [
        bucket * (0.94 if query == "full_scan" else 1.06)
        for bucket in bucket_counts
    ]

    valid = [
        (position, values)
        for position, values in zip(positions, data)
        if values
    ]

    if valid:
        positions, values = zip(*valid)

        ax.boxplot(
            values,
            positions=positions,
            widths=[
                position * 0.07 for position in positions
            ],
            manage_ticks=False,
            showfliers=True,
            patch_artist=False,
            label=label,
        )

ax.set_xscale("log", base=2)
ax.set_xticks(bucket_counts)
ax.set_xticklabels(bucket_counts)

ax.set_xlabel("Bucket count")
ax.set_ylabel("Latency (ms)")
ax.set_title("E1 - Distribution of Query Latencies")

save_plot(fig, "query_latency_distribution.png")


# ---------------------------------------------------------
# 7. Backend replica distribution
# ---------------------------------------------------------

backend_counts = {}

for row in tablet_rows:
    bucket = int(row["bucket_count"])
    backend = row["backend_id"]
    count = int(row["replica_count"])

    backend_counts.setdefault(backend, {})[bucket] = count

fig, ax = plt.subplots()

for backend, values in sorted(backend_counts.items()):
    counts = [
        values.get(bucket, 0)
        for bucket in bucket_counts
    ]

    ax.plot(
        bucket_counts,
        counts,
        marker="o",
        linewidth=2,
        label=f"Backend {backend}",
    )

ax.set_xscale("log", base=2)
ax.set_xticks(bucket_counts)
ax.set_xticklabels(bucket_counts)

ax.set_xlabel("Bucket count")
ax.set_ylabel("Replica count")
ax.set_title("E1 - Replica Distribution Across Backends")
ax.legend()

save_plot(fig, "replica_distribution.png")


# ---------------------------------------------------------
# Finish
# ---------------------------------------------------------

print()
print("E1 plotting completed successfully.")
print(f"Output directory: {RUN_DIR}")
