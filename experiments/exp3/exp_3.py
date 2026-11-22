import csv
import re
import statistics
import subprocess
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULTS_PATH = ROOT / "experiments" / "exp3" / "results" / datetime.now().strftime("%Y%m%d-%H%M%S")
RESULTS_PATH.mkdir(parents=True, exist_ok=True)

QUERIES = [
    ("1_baseline", "SELECT count(*), sum(amount) FROM e3.events"),
    ("2_bucket_eq", "SELECT count(*), sum(amount) FROM e3.events WHERE user_id = 42"),
    ("3_non_bucket", "SELECT count(*), sum(amount) FROM e3.events WHERE category = 'cat_3'"),
    ("4_date_1part", "SELECT count(*), sum(amount) FROM e3.events WHERE event_date >= '2024-01-03' AND event_date < '2024-01-04'"),
    ("5_combo", "SELECT count(*), sum(amount) FROM e3.events WHERE event_date >= '2024-01-03' AND event_date < '2024-01-04' AND user_id = 42"),
]


def sql(statement: str) -> str:
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "starrocks-fe",
            "mysql", "-h127.0.0.1", "-P9030", "-uroot",
            "--batch", "--skip-column-names", "-e", statement,
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(f"SQL failed: {statement}\n{result.stderr.strip()}")
    return result.stdout.strip()


def parse_pruning(explain_text: str) -> tuple[str, str]:
    part_match = re.search(r"partitions=(\d+/\d+)", explain_text)
    ratio_match = re.search(r"tabletRatio=(\d+/\d+)", explain_text)
    partitions_kept = part_match.group(1) if part_match else "unknown"
    tablet_ratio = ratio_match.group(1) if ratio_match else "unknown"
    return partitions_kept, tablet_ratio


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(0.95 * len(ordered) + 0.999999) - 1))
    return ordered[idx]


def main() -> None:
    explain_log = RESULTS_PATH / "explain.txt"
    summary_csv = RESULTS_PATH / "summary.csv"

    with explain_log.open("w", encoding="utf-8") as ef, summary_csv.open("w", newline="", encoding="utf-8") as sf:
        writer = csv.writer(sf)
        writer.writerow(["query", "partitions_kept", "tablet_ratio", "median_ms", "p95_ms"])

        for name, query in QUERIES:
            plan = sql(f"EXPLAIN {query}")
            ef.write(f"================ {name} ================\n{plan}\n\n")
            partitions_kept, tablet_ratio = parse_pruning(plan)

            # Warm-up with query cache disabled
            sql(f"SET enable_query_cache = false; {query}")

            samples_ms: list[float] = []
            for _ in range(6):
                t0 = time.perf_counter()
                sql(f"SET enable_query_cache = false; {query}")
                samples_ms.append((time.perf_counter() - t0) * 1000.0)

            med = round(statistics.median(samples_ms), 2)
            p95_val = round(p95(samples_ms), 2)
            writer.writerow([name, partitions_kept, tablet_ratio, f"{med:.2f}", f"{p95_val:.2f}"])
            sf.flush()
            print(f"{name}: partitions={partitions_kept}, tabletRatio={tablet_ratio}, median={med:.2f}ms, p95={p95_val:.2f}ms")

    print(f"Saved E3 results to {RESULTS_PATH}")


if __name__ == "__main__":
    main()
