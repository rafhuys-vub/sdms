# /// script
# requires-python = ">=3.10"
# dependencies = ["matplotlib"]
# ///
"""Plot one E5 run:  uv run plot.py results/<run>   ->  results/<run>/timeline.png"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
run = Path(sys.argv[1])
if not run.is_absolute():
    run = ROOT / "results" / run
rows = lambda name: list(csv.DictReader(open(run / name))) if (run / name).exists() else []

t_add = next(float(l.split("s]")[0][1:]) for l in open(run / "events.txt") if "ADD" in l)

fig, axes = plt.subplots(3, 1, figsize=(10, 9), sharex=True)

per_be = defaultdict(list)
for r in rows("replicas.csv"):
    per_be[r["backend"]].append((float(r["t"]), int(r["replicas"])))
for be, pts in sorted(per_be.items()):
    axes[0].step(*zip(*pts), where="post", label=be)
axes[0].set_ylabel("replicas of e5.events")
axes[0].legend()

sched = rows("sched.csv")
ts = [float(r["t"]) for r in sched]
axes[1].step(ts, [int(r["pending"]) for r in sched], where="post", label="pending")
axes[1].step(ts, [int(r["running"]) for r in sched], where="post", label="running")
axes[1].set_ylabel("scheduler tablets")
axes[1].legend()

per_q = defaultdict(list)
for r in rows("queries.csv"):
    per_q[r["query"]].append((float(r["t"]), int(r["latency_ms"]), r["ok"] == "1"))
for q, pts in sorted(per_q.items()):
    axes[2].plot([p[0] for p in pts], [p[1] for p in pts], ".", ms=3, label=q)
    bad = [p for p in pts if not p[2]]
    if bad:
        axes[2].plot([p[0] for p in bad], [p[1] for p in bad], "rx")
axes[2].set_ylabel("query latency (ms)")
axes[2].set_xlabel("t (s)")
if per_q:
    axes[2].legend()

for ax in axes:
    ax.axvline(t_add, color="k", ls="--", lw=1)
axes[0].set_title(f"{run.name}  (dashed: BE added)")
fig.tight_layout()
fig.savefig(run / "timeline.png", dpi=120)
print(run / "timeline.png")
