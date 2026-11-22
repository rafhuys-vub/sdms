
# /// script
# requires-python = ">=3.10"
# dependencies = []
# ///

"""
E2 - HASH vs RANDOM bucketing under data skew.

Usage:
    python experiments/exp2/exp_2.py

Experiment:
    Compare HASH(region_id) and RANDOM bucketing
    using a deterministic dataset with a skewed region distribution.

Configuration:
    Rows: 1,500,000
    Buckets: 24
    Replication factor: 3
    Warmups: 2
    Measured runs: 10
"""

import csv
import statistics
import subprocess
import time

from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]

DB = "e2"
SOURCE = "source_events"

TABLES = {
    "hash": "events_hash",
    "random": "events_random",
}

ROWS = 1_500_000
BUCKETS = 24
REPLICATION = 3

WARMUPS = 2
RUNS = 10

REPLICA_TIMEOUT = 300
POLL_INTERVAL = 5

RESULTS = (
    ROOT
    / "experiments"
    / "exp2"
    / "results"
    / datetime.now().strftime("%Y%m%d-%H%M%S")
)

RESULTS.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# Logging and SQL
# ---------------------------------------------------------

def log(message):
    line = (
        f"[{datetime.now().isoformat(timespec='seconds')}] "
        f"{message}"
    )

    print(line, flush=True)

    with (RESULTS / "events.txt").open(
        "a",
        encoding="utf-8",
    ) as f:
        f.write(line + "\n")


def sql(statement):
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "starrocks-fe",
            "mysql",
            "-h",
            "127.0.0.1",
            "-P",
            "9030",
            "-u",
            "root",
            "--batch",
            "-e",
            statement,
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
    )

    if result.returncode:
        raise RuntimeError(
            f"SQL failed:\n"
            f"{statement}\n"
            f"{result.stderr.strip()}"
        )

    return result.stdout.strip()


def query_rows(statement):
    """
    Execute a SQL query and return its rows as dictionaries.

    The first output line is treated as the column header.
    """

    output = sql(statement)
    lines = output.splitlines()

    if len(lines) < 2:
        return []

    return list(
        csv.DictReader(
            lines,
            delimiter="\t",
        )
    )


def sql_value(statement):
    """
    Execute a scalar SQL query and return its value.

    Example:
        COUNT(*)
        1500000

    Returns:
        1500000
    """

    output = sql(statement)
    lines = output.splitlines()

    if len(lines) < 2:
        raise RuntimeError(
            f"Expected a scalar SQL result, got: {output}"
        )

    return lines[-1].strip()


def write_csv(filename, headers, rows):
    path = RESULTS / filename

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


# ---------------------------------------------------------
# Cluster validation
# ---------------------------------------------------------

def check_cluster():
    log("Checking StarRocks cluster")

    rows = query_rows("SHOW BACKENDS")

    alive = [
        row
        for row in rows
        if row.get("Alive", "").lower() == "true"
    ]

    if len(alive) != 3:
        raise RuntimeError(
            f"Expected 3 alive backends, "
            f"found {len(alive)}"
        )

    log(
        f"Cluster ready: "
        f"{len(alive)} alive backends"
    )


# ---------------------------------------------------------
# Source dataset
# ---------------------------------------------------------

def create_source():
    log("Creating E2 database and source table")

    sql(f"CREATE DATABASE IF NOT EXISTS {DB}")

    sql(
        f"DROP TABLE IF EXISTS "
        f"{DB}.{SOURCE}"
    )

    sql(f"""
        CREATE TABLE {DB}.{SOURCE} (
            event_id BIGINT NOT NULL,
            customer_id BIGINT NOT NULL,
            region_id INT NOT NULL,
            event_date DATE NOT NULL,
            amount DECIMAL(12,2) NOT NULL
        )
        DUPLICATE KEY(event_id)
        DISTRIBUTED BY HASH(event_id) BUCKETS {BUCKETS}
        PROPERTIES (
            "replication_num" = "{REPLICATION}",
            "compression" = "LZ4"
        )
    """)

    log(
        f"Generating {ROWS:,} "
        f"deterministic skewed rows"
    )

    # StarRocks 3.3.22:
    # Generate a sequence from 0 to ROWS - 1.

    sql(f"""
        INSERT INTO {DB}.{SOURCE}
        SELECT
            generate_series + 1,

            MOD(generate_series * 37, 100000) + 1,

            CASE
                WHEN MOD(generate_series, 100) < 70
                    THEN 1

                WHEN MOD(generate_series, 100) < 85
                    THEN 2

                WHEN MOD(generate_series, 100) < 95
                    THEN 3

                ELSE 4
            END,

            DATE_ADD(
                '2025-01-01',
                INTERVAL MOD(generate_series, 365) DAY
            ),

            CAST(
                10 + MOD(generate_series, 991)
                AS DECIMAL(12,2)
            )

        FROM TABLE(
            generate_series(0, {ROWS - 1})
        )
    """)

    # Validate total number of source rows.

    count = int(
        sql_value(
            f"SELECT COUNT(*) "
            f"FROM {DB}.{SOURCE}"
        )
    )

    if count != ROWS:
        raise RuntimeError(
            f"Expected {ROWS:,} source rows, "
            f"got {count:,}"
        )

    log(
        f"Source table validated: "
        f"{count:,} rows"
    )

    # Validate region distribution.

    distribution = query_rows(f"""
        SELECT
            region_id,
            COUNT(*) AS row_count
        FROM {DB}.{SOURCE}
        GROUP BY region_id
        ORDER BY region_id
    """)

    expected = {
        1: 1_050_000,
        2: 225_000,
        3: 150_000,
        4: 75_000,
    }

    actual = {
        int(row["region_id"]): int(row["row_count"])
        for row in distribution
    }

    if actual != expected:
        raise RuntimeError(
            f"Unexpected region distribution: {actual}. "
            f"Expected: {expected}"
        )

    write_csv(
        "data_stats.csv",
        [
            "region_id",
            "row_count",
        ],
        [
            [region, actual[region]]
            for region in sorted(expected)
        ],
    )

    log(
        f"Skew validated: {actual}"
    )


# ---------------------------------------------------------
# Replica consistency
# ---------------------------------------------------------

def wait_for_replica_consistency(table):
    log(
        f"{table}: waiting for replica consistency"
    )

    deadline = (
        time.monotonic()
        + REPLICA_TIMEOUT
    )

    while time.monotonic() < deadline:

        rows = query_rows(
            f"SHOW TABLET FROM {DB}.{table}"
        )

        if not rows:
            time.sleep(POLL_INTERVAL)
            continue

        tablets = {}

        for row in rows:
            tablets.setdefault(
                row["TabletId"],
                [],
            ).append(row)

        valid = len(tablets) == BUCKETS

        if valid:

            for replicas in tablets.values():

                if len(replicas) != REPLICATION:
                    valid = False
                    break

                backend_ids = {
                    replica["BackendId"]
                    for replica in replicas
                }

                row_counts = {
                    int(replica["RowCount"])
                    for replica in replicas
                }

                states = {
                    replica["State"]
                    for replica in replicas
                }

                if (
                    len(backend_ids) != REPLICATION
                    or len(row_counts) != 1
                    or states != {"NORMAL"}
                ):
                    valid = False
                    break

        if valid:

            logical_rows = sum(
                int(replicas[0]["RowCount"])
                for replicas in tablets.values()
            )

            if logical_rows == ROWS:

                log(
                    f"{table}: replica consistency confirmed "
                    f"({BUCKETS} tablets, "
                    f"{REPLICATION} replicas each)"
                )

                return

        time.sleep(POLL_INTERVAL)

    raise TimeoutError(
        f"Replica consistency timeout for {table} "
        f"after {REPLICA_TIMEOUT} seconds"
    )


# ---------------------------------------------------------
# Target tables
# ---------------------------------------------------------

def create_and_load_tables():

    load_results = []

    for strategy, table in TABLES.items():

        log(
            f"Creating {strategy.upper()} table: {table}"
        )

        sql(
            f"DROP TABLE IF EXISTS "
            f"{DB}.{table}"
        )

        if strategy == "hash":

            distribution = (
                f"HASH(region_id) BUCKETS {BUCKETS}"
            )

        else:

            distribution = (
                f"RANDOM BUCKETS {BUCKETS}"
            )

        sql(f"""
            CREATE TABLE {DB}.{table} (
                event_id BIGINT NOT NULL,
                customer_id BIGINT NOT NULL,
                region_id INT NOT NULL,
                event_date DATE NOT NULL,
                amount DECIMAL(12,2) NOT NULL
            )
            DUPLICATE KEY(event_id)
            DISTRIBUTED BY {distribution}
            PROPERTIES (
                "replication_num" = "{REPLICATION}",
                "compression" = "LZ4"
            )
        """)

        log(
            f"{table}: loading source data"
        )

        start = time.perf_counter()

        sql(f"""
            INSERT INTO {DB}.{table}
            SELECT
                event_id,
                customer_id,
                region_id,
                event_date,
                amount
            FROM {DB}.{SOURCE}
        """)

        elapsed = (
            time.perf_counter() - start
        )

        # Use sql_value() to remove the COUNT(*) header.

        count = int(
            sql_value(
                f"SELECT COUNT(*) "
                f"FROM {DB}.{table}"
            )
        )

        if count != ROWS:
            raise RuntimeError(
                f"{table}: expected {ROWS:,} rows, "
                f"got {count:,}"
            )

        wait_for_replica_consistency(table)

        load_results.append([
            strategy,
            table,
            round(elapsed, 3),
            count,
        ])

        log(
            f"{table}: loaded {count:,} rows "
            f"in {elapsed:.3f}s"
        )

    write_csv(
        "load_times.csv",
        [
            "strategy",
            "table",
            "load_time_seconds",
            "row_count",
        ],
        load_results,
    )


# ---------------------------------------------------------
# Data validation
# ---------------------------------------------------------

def validate_tables():

    log("Validating data equivalence")

    source = query_rows(f"""
        SELECT
            COUNT(*) AS n,
            SUM(event_id) AS event_sum,
            SUM(amount) AS amount_sum
        FROM {DB}.{SOURCE}
    """)[0]

    for strategy, table in TABLES.items():

        target = query_rows(f"""
            SELECT
                COUNT(*) AS n,
                SUM(event_id) AS event_sum,
                SUM(amount) AS amount_sum
            FROM {DB}.{table}
        """)[0]

        for column in (
            "n",
            "event_sum",
            "amount_sum",
        ):

            if source[column] != target[column]:

                raise RuntimeError(
                    f"{table}: mismatch in {column}: "
                    f"{source[column]} != {target[column]}"
                )

        log(
            f"{table}: data equivalence validated"
        )


# ---------------------------------------------------------
# Query workloads
# ---------------------------------------------------------

QUERIES = {

    "full_scan": """
        SELECT
            COUNT(*),
            SUM(amount)
        FROM {table}
    """,

    "dominant_region": """
        SELECT
            COUNT(*),
            SUM(amount)
        FROM {table}
        WHERE region_id = 1
    """,

    "rare_region": """
        SELECT
            COUNT(*),
            SUM(amount)
        FROM {table}
        WHERE region_id = 4
    """,

    "group_by_region": """
        SELECT
            region_id,
            COUNT(*),
            SUM(amount)
        FROM {table}
        GROUP BY region_id
    """,
}


def percentile_95(values):

    if not values:
        return None

    if len(values) == 1:
        return values[0]

    return statistics.quantiles(
        values,
        n=100,
        method="inclusive",
    )[94]


def benchmark():

    log("Starting query benchmarks")

    raw = []
    summary = []

    for strategy, table in TABLES.items():

        for query_name, template in QUERIES.items():

            statement = template.format(
                table=f"{DB}.{table}"
            )

            values = []

            total_runs = WARMUPS + RUNS

            for run in range(1, total_runs + 1):

                start = time.perf_counter()

                ok = 1
                error = ""

                try:

                    sql(
                        "SET enable_query_cache = false; "
                        + statement
                    )

                except RuntimeError as exc:

                    ok = 0
                    error = str(exc)

                latency = (
                    time.perf_counter() - start
                ) * 1000

                if run > WARMUPS:

                    measured_run = run - WARMUPS

                    raw.append([
                        strategy,
                        table,
                        query_name,
                        measured_run,
                        round(latency, 3),
                        ok,
                        error.replace("\n", " ")[:500],
                    ])

                    if ok:
                        values.append(latency)

                if not ok:

                    log(
                        f"WARNING: {strategy}/{query_name} "
                        f"run {run} failed: "
                        f"{error[:200]}"
                    )

            if not values:

                raise RuntimeError(
                    f"No successful measurements for "
                    f"{strategy}/{query_name}"
                )

            median = statistics.median(values)
            p95 = percentile_95(values)

            summary.append([
                strategy,
                query_name,
                len(values),
                round(median, 3),
                round(p95, 3),
                round(min(values), 3),
                round(max(values), 3),
            ])

            log(
                f"{strategy}/{query_name}: "
                f"{len(values)}/{RUNS} successful, "
                f"median={median:.2f}ms, "
                f"P95={p95:.2f}ms"
            )

    write_csv(
        "benchmark.csv",
        [
            "strategy",
            "table",
            "query",
            "run",
            "latency_ms",
            "ok",
            "error",
        ],
        raw,
    )

    write_csv(
        "summary.csv",
        [
            "strategy",
            "query",
            "runs",
            "median_ms",
            "p95_ms",
            "min_ms",
            "max_ms",
        ],
        summary,
    )


# ---------------------------------------------------------
# Tablet statistics
# ---------------------------------------------------------

def collect_tablet_stats():

    log("Collecting tablet statistics")

    details = []
    stats = []

    for strategy, table in TABLES.items():

        rows = query_rows(
            f"SHOW TABLET FROM {DB}.{table}"
        )

        if not rows:

            raise RuntimeError(
                f"No tablet metadata for {table}"
            )

        required = {
            "TabletId",
            "BackendId",
            "RowCount",
            "State",
        }

        if not required.issubset(rows[0].keys()):

            raise RuntimeError(
                f"Unexpected SHOW TABLET columns: "
                f"{rows[0].keys()}"
            )

        logical = {}
        backend_counts = {}

        for row in rows:

            tablet_id = row["TabletId"]
            backend = row["BackendId"]
            row_count = int(row["RowCount"])

            details.append([
                strategy,
                table,
                tablet_id,
                backend,
                row_count,
                row["State"],
            ])

            logical.setdefault(
                tablet_id,
                [],
            ).append(row_count)

            backend_counts[backend] = (
                backend_counts.get(backend, 0) + 1
            )

        # Use one replica per logical tablet
        # to avoid counting replicated rows three times.

        logical_counts = [
            replica_counts[0]
            for replica_counts in logical.values()
        ]

        nonempty = sum(
            1
            for count in logical_counts
            if count > 0
        )

        for backend, replica_count in sorted(
            backend_counts.items()
        ):

            stats.append([
                strategy,
                table,
                len(logical),
                len(rows),
                nonempty,
                backend,
                replica_count,
            ])

        log(
            f"{table}: "
            f"{len(logical)} logical tablets, "
            f"{len(rows)} physical replicas, "
            f"{nonempty} non-empty tablets"
        )

    write_csv(
        "tablet_details.csv",
        [
            "strategy",
            "table",
            "tablet_id",
            "backend_id",
            "row_count",
            "state",
        ],
        details,
    )

    write_csv(
        "tablet_stats.csv",
        [
            "strategy",
            "table",
            "logical_tablets",
            "total_replicas",
            "nonempty_tablets",
            "backend_id",
            "replica_count",
        ],
        stats,
    )


# ---------------------------------------------------------
# EXPLAIN plans
# ---------------------------------------------------------

def collect_explain_plans():

    log("Collecting EXPLAIN plans")

    with (
        RESULTS / "explain_plans.txt"
    ).open(
        "w",
        encoding="utf-8",
    ) as f:

        for strategy, table in TABLES.items():

            for query_name in (
                "dominant_region",
                "rare_region",
            ):

                statement = QUERIES[
                    query_name
                ].format(
                    table=f"{DB}.{table}"
                )

                plan = sql(
                    "EXPLAIN " + statement
                )

                f.write(
                    f"\n{'=' * 70}\n"
                    f"{strategy.upper()} - {query_name}\n"
                    f"{'=' * 70}\n"
                    f"{plan}\n"
                )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    start = time.perf_counter()

    log(
        "Starting E2 - HASH vs RANDOM under data skew"
    )

    config = f"""Experiment: E2 - HASH vs RANDOM under data skew

StarRocks database: {DB}

Rows: {ROWS}
Buckets: {BUCKETS}
Replication factor: {REPLICATION}

Warmups: {WARMUPS}
Measured runs: {RUNS}

Strategies:
    HASH(region_id)
    RANDOM

Region distribution:
    Region 1: 70%
    Region 2: 15%
    Region 3: 10%
    Region 4: 5%

P95:
    Inclusive percentile interpolation

Query latency:
    Client-observed latency, including Docker/MySQL
    client startup overhead.
"""

    (RESULTS / "config.txt").write_text(
        config,
        encoding="utf-8",
    )

    check_cluster()

    create_source()

    create_and_load_tables()

    validate_tables()

    benchmark()

    collect_tablet_stats()

    collect_explain_plans()

    elapsed = (
        time.perf_counter() - start
    )

    log(
        f"E2 completed successfully in "
        f"{elapsed:.1f}s"
    )

    print(
        f"\nResults directory: {RESULTS}"
    )


if __name__ == "__main__":
    main()
