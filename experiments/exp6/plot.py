# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib"]
# ///
"""Plot one E6 run: uv run plot_e6.py results/<run> -> results/<run>/timeline.png"""
import csv
import re
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent

run = Path(sys.argv[1])
samples_path = ROOT / "results" / run / "samples.csv"
queries_path = ROOT / "results" /run / "queries.csv"
events_path = ROOT / "results" / run / "events.txt"
out = ROOT / "results" / run / "timeline.png"

samples = list(csv.reader(samples_path.open())) if samples_path.exists() else []
queries = list(csv.DictReader(queries_path.open())) if queries_path.exists() else []
events = events_path.read_text().splitlines() if events_path.exists() else []

# The run logs the kill command immediately before the kill is issued.
kill_time = None
for line in events:
    if "docker compose kill" in line:
        match = re.match(r"\[([0-9.]+)s\]", line)
        if match:
            kill_time = float(match.group(1))
            break

replicas = defaultdict(list)
health = []
scheduler = defaultdict(list)
for row in samples:
    if len(row) != 4:
        continue
    t, kind, name, value = row
    t, value = float(t), int(value)
    if kind == "replicas":
        replicas[name].append((t, value))
    elif kind == "health":
        health.append((t, value))
    elif kind == "scheduler":
        scheduler[name].append((t, value))

fig, axes = plt.subplots(4, 1, figsize=(11, 11), sharex=True)

for be, points in sorted(replicas.items()):
    axes[0].step([p[0] for p in points], [p[1] for p in points], where="post", label=be)
axes[0].set_ylabel("replicas per BE")
axes[0].legend(loc="best", ncol=2)

if health:
    axes[1].step([p[0] for p in health], [p[1] for p in health], where="post", color="tab:red")
axes[1].set_ylabel("unhealthy tablets")

for name in ("pending", "running"):
    points = scheduler[name]
    if points:
        axes[2].step([p[0] for p in points], [p[1] for p in points], where="post", label=name)
axes[2].set_ylabel("scheduler tablets")
if scheduler:
    axes[2].legend(loc="best")

per_query = defaultdict(list)
for row in queries:
    per_query[row["query"]].append((float(row["t"]), int(row["latency_ms"]), row["ok"] == "1"))
for query, points in sorted(per_query.items()):
    axes[3].plot([p[0] for p in points], [p[1] for p in points], ".", ms=3, label=query)
    failed = [p for p in points if not p[2]]
    if failed:
        axes[3].plot([p[0] for p in failed], [p[1] for p in failed], "rx", ms=6)
axes[3].set_ylabel("query latency (ms)")
axes[3].set_xlabel("elapsed time (s)")
if per_query:
    axes[3].legend(loc="best")

if kill_time is not None:
    for ax in axes:
        ax.axvline(kill_time, color="black", linestyle="--", linewidth=1)
    axes[0].text(kill_time, 0.98, " BE killed", transform=axes[0].get_xaxis_transform(),
                 ha="left", va="top", fontsize=8)

fig.suptitle(f"E6 failure and replica repair — {run.name}")
fig.tight_layout()
fig.savefig(out, dpi=140)
print(out)
