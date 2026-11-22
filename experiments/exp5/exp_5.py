import csv
import os
import re
import statistics
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BACKENDS = ["starrocks-be-0", "starrocks-be-1", "starrocks-be-2"]
DB_NAME = "e5"
EXP_START = time.monotonic()
RESTORE_DEADLINE = 1800
RESULTS_PATH = ROOT / "experiments"/ "exp5" / "results" / datetime.now().strftime("%Y%m%d-%H%M%S")
RESULTS_PATH.mkdir(parents=True, exist_ok=True)
STOP_QUERIES = threading.Event()
TABLE = "e5.events"
NEW_BE = "starrocks-be-3"


def sql(statement):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "starrocks-fe", "mysql", "-h127.0.0.1",
         "-P9030", "-uroot", "--batch", "--skip-column-names", "-e", statement],
        cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(f"SQL failed: {statement}\n{result.stderr.strip()}")
    return result.stdout.strip()


def backends():
    """Return the FE's backend list, keyed by hostname and backend ID."""
    result = {}
    for line in sql("SHOW BACKENDS").splitlines():
        cols = line.split("\t")
        if len(cols) >= 9 and cols[0].isdigit():
            backend = {"id": cols[0], "host": cols[1], "alive": cols[8].lower() == "true"}
            result[cols[0]] = backend
            result[cols[1]] = backend
    return result


def docker(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def query_loop():
    queries = [
        f"SELECT category, count(*), sum(amount) FROM {TABLE} GROUP BY category",
        f"SELECT event_date, count(DISTINCT user_id) FROM {TABLE} GROUP BY event_date",
        f"SELECT count(*) FROM {TABLE} WHERE user_id = 42",
    ]
    with (RESULTS_PATH / "queries.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["t", "query", "ok", "latency_ms"])
        while not STOP_QUERIES.is_set():
            for i, query in enumerate(queries):
                if STOP_QUERIES.is_set():
                    break
                start = time.monotonic()
                try:
                    sql(query)
                    ok = 1
                except RuntimeError:
                    ok = 0
                writer.writerow([f"{start - EXP_START:.2f}", f"q{i}", ok,
                                 round((time.monotonic() - start) * 1000)])
                f.flush()


def elapsed():
    return time.monotonic() - EXP_START


def wait_until(predicate, timeout, description):
    start = time.monotonic()
    while time.monotonic() < start + timeout:
        if predicate():
            return elapsed()
        time.sleep(1)
    raise TimeoutError(description)


def log(message):
    line = f"[{elapsed():.2f}s] {message}"
    print(line, flush=True)
    with (RESULTS_PATH / "events.txt").open("a") as f:
        f.write(line + "\n")


def query_cluster_balance():
    scheduler_text = sql("SHOW PROC '/cluster_balance'")
    result = {}
    for line in scheduler_text.splitlines():
        cols = line.split("\t")
        result[cols[0]] = int(cols[1])
    return result


def query_replica_distribution():
    result = {}
    nodes_by_id = backends()
    for line in sql(f"ADMIN SHOW REPLICA DISTRIBUTION FROM {TABLE}").splitlines():
        cols = line.split("\t")
        backend = nodes_by_id.get(cols[0])
        if backend:
            result[backend["host"]] = int(cols[1])
    return result


def sample(writer, replica_file, scheduler_writer, scheduler_file):
    now = elapsed()
    balance = query_cluster_balance()
    distribution = query_replica_distribution()
    for host, replicas in distribution.items():
        writer.writerow([f"{now:.2f}", host, replicas])
    replica_file.flush()

    scheduler_writer.writerow([f"{now:.2f}", balance["pending_tablets"], balance["running_tablets"]])
    scheduler_file.flush()

    text = " ".join(f"{host}={count}" for host, count in sorted(distribution.items()))
    return text, distribution, balance["pending_tablets"] == 0 and balance["running_tablets"] == 0


def write_config():
    parts = [
        f"date: {datetime.now().astimezone().isoformat()}",
        "experiment: E5 scale-out; add one BE and wait for replica redistribution",
        "--- table", sql(f"SHOW CREATE TABLE {TABLE}"),
        "--- partitions", sql("SELECT count(*) FROM information_schema.partitions_meta WHERE DB_NAME='e5' AND TABLE_NAME='events'"),
        "--- scheduler config", sql("ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_sched%'")
    ]
    (RESULTS_PATH / "config.txt").write_text("\n".join(parts) + "\n")


def main():
    query_thread = None

    bs = backends()
    alive_backends = sorted({node["host"] for node in bs.values() if node["alive"]})
    if alive_backends != BACKENDS:
        raise RuntimeError(f"unexpected live backends: {alive_backends}; expected {BACKENDS}")
    # A registered but stopped scale-out BE is safe: Compose can start it.
    if NEW_BE in bs and bs[NEW_BE]["alive"]:
        raise RuntimeError(f"{NEW_BE} is already alive; start with a fresh 3-BE baseline")
    sql(f"SELECT count(*) FROM {TABLE}")
    log(f"start experiment with backends: {alive_backends}")

    write_config()
    (RESULTS_PATH / "replicas.csv").write_text("t,backend,replicas\n")
    (RESULTS_PATH / "sched.csv").write_text("t,pending,running\n")
    (RESULTS_PATH / "events.txt").write_text("")

    query_thread = threading.Thread(target=query_loop, daemon=True)
    query_thread.start()

    try:
        with ((RESULTS_PATH / "replicas.csv").open("a", newline="") as replica_file,
              (RESULTS_PATH / "sched.csv").open("a", newline="") as scheduler_file):
            writer = csv.writer(replica_file)
            scheduler_writer = csv.writer(scheduler_file)
            distribution = query_replica_distribution()
            total_replicas = sum(distribution.values())
            initial_text, _, _ = sample(writer, replica_file, scheduler_writer, scheduler_file)
            log(f"baseline for 30s: {initial_text}")
            baseline_end = time.monotonic() + 30
            while time.monotonic() < baseline_end:
                time.sleep(min(5, max(0, baseline_end - time.monotonic())))
                sample(writer, replica_file, scheduler_writer, scheduler_file)

            add_time = elapsed()
            log(f"ADD: docker compose --profile scaleout up -d --no-deps {NEW_BE}")
            docker("--profile", "scaleout", "up", "-d", "--no-deps", NEW_BE)

            alive_time = first_time = None
            last_dist = ""
            last_change = add_time
            timed_out = False
            while True:
                time.sleep(5)
                final_text, counts, idle = sample(writer, replica_file, scheduler_writer, scheduler_file)
                now = elapsed()
                current_backends = backends()
                if alive_time is None and NEW_BE in current_backends and current_backends[NEW_BE]["alive"]:
                    alive_time = now
                    log(f"{NEW_BE} alive")
                if first_time is None and counts.get(NEW_BE, 0) > 0:
                    first_time = now
                    log(f"first replica on {NEW_BE}")
                if final_text != last_dist:
                    last_dist = final_text
                    last_change = now
                    print(f"[{now:.2f}s] {final_text}", flush=True)
                if first_time is not None and idle and now - last_change >= 60:
                    log(f"converged (no change since {last_change:.2f}s)")
                    break
                if now - add_time >= RESTORE_DEADLINE:
                    raise TimeoutError("Timed out waiting for scale-out to converge")

        STOP_QUERIES.set()
        query_thread.join(timeout=10)
        clones = sql("SHOW PROC '/cluster_balance/history_tablets'")
        (RESULTS_PATH / "clones.tsv").write_text(clones)
        print(f"results in {RESULTS_PATH}")
    finally:
        STOP_QUERIES.set()
        if query_thread:
            query_thread.join(timeout=10)

if __name__ == "__main__":
    main()
