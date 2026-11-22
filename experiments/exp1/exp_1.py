
#!/usr/bin/env python3
"""
E1 - Bucket count / tablet granularity vs performance.

Usage:
    python experiments/exp1/exp_1.py

Requirements:
    - Docker Compose environment running StarRocks 3.3.22
    - Database e1
    - Source table e1.source_events containing 1.5M rows
    - Three active StarRocks backend nodes

The script reuses e1.source_events and creates nine benchmark tables.

Results:
    experiments/exp1/results/<timestamp>/
"""

import csv
import math
import statistics
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
EXP_DIR = ROOT / "experiments" / "exp1"

DATABASE = "e1"
SOURCE_TABLE = "source_events"

BUCKET_COUNTS = [1, 3, 6, 12, 24, 48, 96, 192, 384]

EXPECTED_ROWS = 1_500_000
REPLICATION_NUM = 3

WARMUP_RUNS = 2
MEASURED_RUNS = 10

QUERY_TYPES = ("full_scan", "point_query")

BACKENDS = (
    "starrocks-be-0",
    "starrocks-be-1",
    "starrocks-be-2",
)

REPLICA_SYNC_TIMEOUT_SECONDS = 180
REPLICA_SYNC_POLL_INTERVAL_SECONDS = 5

RESULT_DIR = (
    EXP_DIR / "results" /
    datetime.now().strftime("%Y%m%d-%H%M%S")
)

RESULT_DIR.mkdir(parents=True, exist_ok=True)

EVENTS_FILE = RESULT_DIR / "events.txt"


# ---------------------------------------------------------------------
# Logging and SQL execution
# ---------------------------------------------------------------------

def log(message):
    """Print and persist experiment events."""

    timestamp = datetime.now().isoformat(timespec="seconds")
    line = f"[{timestamp}] {message}"

    print(line)

    with EVENTS_FILE.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


def sql(statement):
    """Execute SQL through the StarRocks FE container."""

    command = [
        "docker", "compose", "exec", "-T",
        "starrocks-fe",
        "mysql",
        "-h", "127.0.0.1",
        "-P", "9030",
        "-u", "root",
        "--batch",
        "-e", statement,
    ]

    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"SQL execution failed:\n"
            f"{result.stderr}\n"
            f"SQL: {statement}"
        )

    return result.stdout.strip()


def parse_tabular_output(output, description):
    """Parse tab-separated MySQL batch output into dictionaries."""

    lines = [
        line for line in output.splitlines()
        if line.strip()
    ]

    if len(lines) < 2:
        raise RuntimeError(
            f"No data rows returned for {description}."
        )

    headers = lines[0].split("\t")
    records = []

    for line in lines[1:]:
        columns = line.split("\t")

        if len(columns) != len(headers):
            raise RuntimeError(
                f"Unexpected output for {description}.\n"
                f"Expected {len(headers)} columns, "
                f"received {len(columns)}.\n"
                f"Headers: {headers}\n"
                f"Row: {columns}"
            )

        records.append(dict(zip(headers, columns)))

    return records


def scalar(statement):
    """Return the final scalar result from a SQL statement."""

    output = sql(statement)

    if not output:
        raise RuntimeError(
            f"SQL returned no result:\n{statement}"
        )

    return output.splitlines()[-1].strip()


def write_config():
    """Save experiment configuration."""

    config = {
        "experiment": "E1 - Bucket count and tablet granularity",
        "starrocks_version": "3.3.22",
        "database": DATABASE,
        "source_table": SOURCE_TABLE,
        "expected_rows": EXPECTED_ROWS,
        "bucket_counts": BUCKET_COUNTS,
        "distribution": "HASH(user_id)",
        "replication_num": REPLICATION_NUM,
        "warmup_runs": WARMUP_RUNS,
        "measured_runs": MEASURED_RUNS,
        "backend_count": len(BACKENDS),
        "partitioning": "none",
        "query_cache": "disabled per benchmark session",
        "latency_method": "client-observed wall-clock time",
        "latency_includes": "Docker and MySQL client startup overhead",
        "replica_validation":
            "ADMIN SHOW REPLICA STATUS; physical RowCount "
            "is recorded but not used as a synchronization condition",
        "replica_sync_timeout_seconds":
            REPLICA_SYNC_TIMEOUT_SECONDS,
        "replica_sync_poll_interval_seconds":
            REPLICA_SYNC_POLL_INTERVAL_SECONDS,
    }

    with (RESULT_DIR / "config.txt").open(
        "w", encoding="utf-8"
    ) as file:

        for key, value in config.items():
            file.write(f"{key}={value}\n")


# ---------------------------------------------------------------------
# Environment validation
# ---------------------------------------------------------------------

def validate_environment():
    """Check Docker Compose and active StarRocks backends."""

    log("Checking Docker Compose environment")

    compose_files = (
        "docker-compose.yml",
        "docker-compose.yaml",
        "compose.yaml",
        "compose.yml",
    )

    if not any((ROOT / name).exists() for name in compose_files):
        raise RuntimeError(
            "Docker Compose configuration not found."
        )

    backend_output = sql("SHOW BACKENDS;")

    lines = backend_output.splitlines()

    if not lines:
        raise RuntimeError(
            "No backend information returned."
        )

    header = lines[0].split("\t")

    try:
        alive_index = header.index("Alive")
        ip_index = header.index("IP")

    except ValueError as error:
        raise RuntimeError(
            f"Unexpected SHOW BACKENDS columns: {header}"
        ) from error

    active_backends = []

    for line in lines[1:]:
        columns = line.split("\t")

        if len(columns) <= max(alive_index, ip_index):
            continue

        if columns[alive_index].lower() == "true":
            active_backends.append(columns[ip_index])

    if len(active_backends) != len(BACKENDS):
        raise RuntimeError(
            f"Expected {len(BACKENDS)} active backends, "
            f"found {len(active_backends)}: {active_backends}"
        )

    log(f"Active backends: {active_backends}")


def validate_source():
    """Verify that the reference dataset exists and is complete."""

    log("Validating source dataset")

    count = int(
        scalar(
            f"SELECT COUNT(*) FROM "
            f"{DATABASE}.{SOURCE_TABLE};"
        )
    )

    if count != EXPECTED_ROWS:
        raise RuntimeError(
            f"Expected {EXPECTED_ROWS:,} source rows, "
            f"found {count:,}."
        )

    log(f"Source dataset validated: {count:,} rows")


# ---------------------------------------------------------------------
# Table creation and data loading
# ---------------------------------------------------------------------

def create_table(bucket_count):
    """Create a benchmark table with the requested bucket count."""

    table = f"events_b{bucket_count}"

    log(
        f"Creating {table} with "
        f"{bucket_count} buckets"
    )

    sql(f"""
        DROP TABLE IF EXISTS {DATABASE}.{table};

        CREATE TABLE {DATABASE}.{table} (
            event_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            event_date DATE,
            category VARCHAR(20),
            amount DECIMAL(12,2)
        )
        ENGINE=OLAP
        DUPLICATE KEY(event_id)
        DISTRIBUTED BY HASH(user_id) BUCKETS {bucket_count}
        PROPERTIES (
            'replication_num' = '{REPLICATION_NUM}'
        );
    """)

    return table


def load_table(table):
    """
    Load identical records from the source table.

    The INSERT execution time is measured separately from the
    subsequent COUNT(*) verification.
    """

    log(f"Loading data into {table}")

    start = time.perf_counter()

    sql(f"""
        INSERT INTO {DATABASE}.{table}
        SELECT
            event_id,
            user_id,
            event_date,
            category,
            amount
        FROM {DATABASE}.{SOURCE_TABLE};
    """)

    elapsed = time.perf_counter() - start

    log(
        f"INSERT completed for {table} in "
        f"{elapsed:.3f} seconds"
    )

    # Independently verify that the inserted data is query-visible.
    actual_rows = int(
        scalar(
            f"SELECT COUNT(*) FROM {DATABASE}.{table};"
        )
    )

    if actual_rows != EXPECTED_ROWS:
        raise RuntimeError(
            f"Load verification failed for {table}: "
            f"expected {EXPECTED_ROWS:,} rows, "
            f"found {actual_rows:,}."
        )

    log(
        f"Load verification passed for {table}: "
        f"{actual_rows:,} rows"
    )

    return elapsed


# ---------------------------------------------------------------------
# Tablet metadata and replica status
# ---------------------------------------------------------------------

def get_tablet_metadata(table):
    """
    Return one metadata record for every physical tablet replica.

    SHOW TABLET returns one row per physical replica. RowCount and
    DataSize are retained as diagnostic metadata, but RowCount is
    not used alone to determine replica synchronization.
    """

    output = sql(
        f"SHOW TABLET FROM {DATABASE}.{table};"
    )

    records = parse_tabular_output(
        output,
        f"SHOW TABLET for {table}",
    )

    required_columns = (
        "TabletId",
        "ReplicaId",
        "BackendId",
        "RowCount",
        "DataSize",
        "State",
    )

    for record in records:
        missing = [
            column for column in required_columns
            if column not in record
        ]

        if missing:
            raise RuntimeError(
                f"Missing SHOW TABLET columns for {table}: "
                f"{missing}"
            )

    replicas = []

    for record in records:
        replicas.append({
            "tablet_id": record["TabletId"],
            "replica_id": record["ReplicaId"],
            "backend_id": record["BackendId"],
            "row_count": int(record["RowCount"]),
            "data_size": record["DataSize"],
            "state": record["State"],
        })

    if not replicas:
        raise RuntimeError(
            f"No replica records found for {table}"
        )

    return replicas


def get_replica_status(table):
    """
    Return replica health and version metadata.

    ADMIN SHOW REPLICA STATUS provides the replica status used
    for synchronization validation.
    """

    output = sql(
        f"ADMIN SHOW REPLICA STATUS FROM {DATABASE}.{table};"
    )

    records = parse_tabular_output(
        output,
        f"ADMIN SHOW REPLICA STATUS for {table}",
    )

    required_columns = (
        "TabletId",
        "ReplicaId",
        "BackendId",
        "CommittedVersion",
        "LastSuccessVersion",
        "IsBad",
        "State",
        "Status",
    )

    for record in records:
        missing = [
            column for column in required_columns
            if column not in record
        ]

        if missing:
            raise RuntimeError(
                f"Missing replica status columns for {table}: "
                f"{missing}"
            )

    return records


def wait_for_replica_consistency(
    table,
    bucket_count,
    timeout_seconds=REPLICA_SYNC_TIMEOUT_SECONDS,
    poll_interval=REPLICA_SYNC_POLL_INTERVAL_SECONDS,
):
    """
    Wait until all expected tablet replicas are healthy and synchronized.

    Conditions:
        - Expected number of logical tablets is present.
        - Every tablet has REPLICATION_NUM physical replicas.
        - Replicas are placed on distinct backends.
        - Every replica has State=NORMAL.
        - Every replica has Status=OK.
        - No replica is marked IsBad=true.
        - Committed versions match across replicas of each tablet.
        - Last successful versions match across replicas.
        - Committed and last successful versions match.

    Physical SHOW TABLET RowCount values are deliberately not used
    as a synchronization condition.

    Synchronization time is measured separately from INSERT time.
    """

    log(
        f"Waiting for replica consistency for {table} "
        f"(timeout: {timeout_seconds}s)"
    )

    start = time.monotonic()
    deadline = start + timeout_seconds

    last_problems = []
    last_tablet_count = 0

    while time.monotonic() < deadline:

        try:
            replicas = get_replica_status(table)

            tablets = {}

            for replica in replicas:
                tablets.setdefault(
                    replica["TabletId"], []
                ).append(replica)

            last_tablet_count = len(tablets)
            problems = []

            if len(tablets) != bucket_count:
                problems.append(
                    f"Expected {bucket_count} tablets, "
                    f"found {len(tablets)}"
                )

            for tablet_id, tablet_replicas in tablets.items():

                if len(tablet_replicas) != REPLICATION_NUM:
                    problems.append(
                        f"Tablet {tablet_id}: expected "
                        f"{REPLICATION_NUM} replicas, "
                        f"found {len(tablet_replicas)}"
                    )
                    continue

                backend_ids = {
                    replica["BackendId"]
                    for replica in tablet_replicas
                }

                if len(backend_ids) != REPLICATION_NUM:
                    problems.append(
                        f"Tablet {tablet_id}: replicas are not "
                        "placed on distinct backends"
                    )

                for replica in tablet_replicas:

                    if replica["State"].upper() != "NORMAL":
                        problems.append(
                            f"Tablet {tablet_id}, backend "
                            f"{replica['BackendId']}: "
                            f"State={replica['State']}"
                        )

                    if replica["Status"].upper() != "OK":
                        problems.append(
                            f"Tablet {tablet_id}, backend "
                            f"{replica['BackendId']}: "
                            f"Status={replica['Status']}"
                        )

                    if replica["IsBad"].lower() != "false":
                        problems.append(
                            f"Tablet {tablet_id}, backend "
                            f"{replica['BackendId']}: "
                            f"IsBad={replica['IsBad']}"
                        )

                committed_versions = {
                    replica["CommittedVersion"]
                    for replica in tablet_replicas
                }

                successful_versions = {
                    replica["LastSuccessVersion"]
                    for replica in tablet_replicas
                }

                if len(committed_versions) != 1:
                    problems.append(
                        f"Tablet {tablet_id}: committed versions "
                        "differ across replicas"
                    )

                if len(successful_versions) != 1:
                    problems.append(
                        f"Tablet {tablet_id}: last successful "
                        "versions differ across replicas"
                    )

                if committed_versions != successful_versions:
                    problems.append(
                        f"Tablet {tablet_id}: committed and "
                        "last successful versions do not match"
                    )

            last_problems = problems

            if not problems:
                elapsed = time.monotonic() - start

                log(
                    f"Replica consistency confirmed for {table}: "
                    f"{len(tablets)} logical tablets, "
                    f"{len(replicas)} physical replicas, "
                    f"{elapsed:.1f}s"
                )

                return elapsed

            log(
                f"Replica synchronization in progress for {table}: "
                + "; ".join(problems)
            )

        except Exception as error:
            last_problems = [str(error)]

            log(
                f"Replica status check not ready for {table}: "
                f"{error}"
            )

        time.sleep(poll_interval)

    raise RuntimeError(
        f"Replica consistency timeout for {table} "
        f"after {timeout_seconds}s.\n"
        f"Expected {bucket_count} tablets and "
        f"{REPLICATION_NUM} healthy replicas per tablet.\n"
        f"Last observed tablet count: {last_tablet_count}.\n"
        + "\n".join(last_problems)
    )


# ---------------------------------------------------------------------
# Data correctness
# ---------------------------------------------------------------------

def validate_table(table):
    """Compare source and target aggregate checksums."""

    source = scalar(f"""
        SELECT CONCAT(
            COUNT(*), '|',
            SUM(event_id), '|',
            SUM(amount)
        )
        FROM {DATABASE}.{SOURCE_TABLE};
    """)

    target = scalar(f"""
        SELECT CONCAT(
            COUNT(*), '|',
            SUM(event_id), '|',
            SUM(amount)
        )
        FROM {DATABASE}.{table};
    """)

    if source != target:
        raise RuntimeError(
            f"Data validation failed for {table}.\n"
            f"Source: {source}\n"
            f"Target: {target}"
        )

    log(f"Data validation passed for {table}")


# ---------------------------------------------------------------------
# Query benchmarking
# ---------------------------------------------------------------------

def benchmark_query(table, query_type):
    """
    Execute one benchmark query.

    Query-cache configuration and benchmark SQL execute
    in the same MySQL session.

    Latency is client-observed wall-clock time. It includes
    Docker/MySQL client startup and connection overhead.
    """

    if query_type == "full_scan":

        statement = f"""
            SELECT
                category,
                COUNT(*),
                SUM(amount)
            FROM {DATABASE}.{table}
            GROUP BY category;
        """

    elif query_type == "point_query":

        statement = f"""
            SELECT
                COUNT(*),
                SUM(amount)
            FROM {DATABASE}.{table}
            WHERE user_id = 12345;
        """

    else:
        raise ValueError(
            f"Unknown query type: {query_type}"
        )

    command = [
        "docker", "compose", "exec", "-T",
        "starrocks-fe",
        "mysql",
        "-h", "127.0.0.1",
        "-P", "9030",
        "-u", "root",
        "--batch",
        "--skip-column-names",
        "-e",
        f"""
        SET enable_query_cache = false;
        {statement}
        """,
    ]

    start = time.perf_counter()

    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    elapsed_ms = (
        time.perf_counter() - start
    ) * 1000

    if result.returncode != 0:
        raise RuntimeError(
            f"Benchmark failed: {table}, {query_type}\n"
            f"{result.stderr}"
        )

    return elapsed_ms


def run_benchmarks(table, bucket_count, writer):
    """Run warm-ups and measured executions for each query."""

    for query_type in QUERY_TYPES:

        log(
            f"Starting {query_type} benchmarks "
            f"for {table}"
        )

        # Warm-up executions
        for run in range(1, WARMUP_RUNS + 1):

            latency = benchmark_query(
                table,
                query_type
            )

            log(
                f"Warm-up {query_type}: "
                f"{run}/{WARMUP_RUNS}, "
                f"{latency:.3f} ms"
            )

        # Measured executions
        for run in range(1, MEASURED_RUNS + 1):

            latency = benchmark_query(
                table,
                query_type
            )

            writer.writerow([
                bucket_count,
                table,
                query_type,
                run,
                f"{latency:.3f}",
                1,
            ])

            log(
                f"{table} {query_type} "
                f"run={run}/{MEASURED_RUNS}, "
                f"latency={latency:.3f} ms"
            )


# ---------------------------------------------------------------------
# EXPLAIN plans
# ---------------------------------------------------------------------

def collect_explain_plans(table, bucket_count):
    """Save execution plans for the two benchmark queries."""

    output_file = RESULT_DIR / "explain_plans.txt"

    queries = {
        "full_scan": f"""
            SELECT category, COUNT(*), SUM(amount)
            FROM {DATABASE}.{table}
            GROUP BY category;
        """,

        "point_query": f"""
            SELECT COUNT(*), SUM(amount)
            FROM {DATABASE}.{table}
            WHERE user_id = 12345;
        """,
    }

    with output_file.open(
        "a", encoding="utf-8"
    ) as file:

        file.write(
            f"\n\n{'=' * 75}\n"
            f"TABLE: {table}\n"
            f"BUCKETS: {bucket_count}\n"
            f"{'=' * 75}\n"
        )

        for query_type, statement in queries.items():

            file.write(
                f"\n--- {query_type} ---\n"
            )

            try:
                plan = sql(
                    f"EXPLAIN {statement}"
                )

                file.write(plan + "\n")

            except Exception as error:

                file.write(
                    f"EXPLAIN failed: {error}\n"
                )

                log(
                    f"WARNING: EXPLAIN failed "
                    f"for {table}: {error}"
                )

    log(f"EXPLAIN plans collected for {table}")


# ---------------------------------------------------------------------
# Tablet statistics and replica health
# ---------------------------------------------------------------------

def collect_tablet_stats(bucket_count, table, writer):
    """
    Collect logical tablet count and replica distribution.

    tablet_stats.csv:
        One row per backend.

    tablet_details.csv:
        One row per physical tablet replica, including both
        SHOW TABLET metadata and ADMIN SHOW REPLICA STATUS.
    """

    log(f"Collecting tablet statistics for {table}")

    # -------------------------------------------------------------
    # 1. Collect physical tablet metadata
    # -------------------------------------------------------------

    replicas = get_tablet_metadata(table)

    tablets = {}

    for replica in replicas:

        tablet_id = replica["tablet_id"]

        tablets.setdefault(
            tablet_id, []
        ).append(replica)

    logical_tablet_count = len(tablets)
    total_replicas = len(replicas)

    # -------------------------------------------------------------
    # 2. Collect replica health and version information
    # -------------------------------------------------------------

    replica_statuses = get_replica_status(table)

    status_by_replica = {
        (
            status["TabletId"],
            status["BackendId"],
        ): status
        for status in replica_statuses
    }

    # -------------------------------------------------------------
    # 3. Collect replica distribution by backend
    # -------------------------------------------------------------

    distribution_output = sql(
        f"ADMIN SHOW REPLICA DISTRIBUTION "
        f"FROM {DATABASE}.{table};"
    )

    distribution_lines = distribution_output.splitlines()

    if not distribution_lines:
        raise RuntimeError(
            f"No replica distribution returned for {table}"
        )

    distribution_headers = distribution_lines[0].split("\t")

    try:
        backend_index = distribution_headers.index("BackendId")
        replica_index = distribution_headers.index("ReplicaNum")
        percent_index = distribution_headers.index("Percent")

    except ValueError as error:
        raise RuntimeError(
            "Unexpected ADMIN SHOW REPLICA DISTRIBUTION "
            f"columns: {distribution_headers}"
        ) from error

    distribution = {}

    for line in distribution_lines[1:]:

        columns = line.split("\t")

        if len(columns) <= max(
            backend_index,
            replica_index,
            percent_index,
        ):
            continue

        backend_id = columns[backend_index]

        distribution[backend_id] = {
            "replicas": int(columns[replica_index]),
            "percentage": columns[percent_index],
        }

    # -------------------------------------------------------------
    # 4. Save one row per backend
    # -------------------------------------------------------------

    for backend_id, stats in sorted(distribution.items()):

        writer.writerow([
            bucket_count,
            table,
            logical_tablet_count,
            total_replicas,
            backend_id,
            stats["replicas"],
            stats["percentage"],
        ])

    log(
        f"{table}: {logical_tablet_count} logical tablets, "
        f"{total_replicas} physical replicas"
    )

    # -------------------------------------------------------------
    # 5. Save detailed metadata: one row per physical replica
    # -------------------------------------------------------------

    detail_file = RESULT_DIR / "tablet_details.csv"

    write_header = not detail_file.exists()

    with detail_file.open(
        "a",
        newline="",
        encoding="utf-8",
    ) as file:

        detail_writer = csv.writer(file)

        if write_header:

            detail_writer.writerow([
                "bucket_count",
                "table",
                "tablet_id",
                "replica_id",
                "backend_id",
                "replica_count",
                "row_count",
                "data_size",
                "tablet_state",
                "row_count_metadata_consistent",
                "replica_status",
                "committed_version",
                "last_success_version",
                "is_bad",
            ])

        for tablet_id, tablet_replicas in sorted(tablets.items()):

            # This field describes whether SHOW TABLET reports
            # equal RowCount values. It is diagnostic only.
            row_counts = {
                replica["row_count"]
                for replica in tablet_replicas
            }

            row_count_metadata_consistent = (
                len(row_counts) == 1
            )

            for replica in sorted(
                tablet_replicas,
                key=lambda item: item["backend_id"],
            ):

                key = (
                    tablet_id,
                    replica["backend_id"],
                )

                status = status_by_replica.get(key, {})

                detail_writer.writerow([
                    bucket_count,
                    table,
                    tablet_id,
                    replica["replica_id"],
                    replica["backend_id"],
                    len(tablet_replicas),
                    replica["row_count"],
                    replica["data_size"],
                    replica["state"],
                    row_count_metadata_consistent,
                    status.get("Status", "MISSING"),
                    status.get("CommittedVersion", ""),
                    status.get("LastSuccessVersion", ""),
                    status.get("IsBad", ""),
                ])

    log(
        f"Detailed tablet metadata and replica health "
        f"saved for {table}"
    )


# ---------------------------------------------------------------------
# Summary statistics
# ---------------------------------------------------------------------

def percentile(values, percentage):
    """Calculate a percentile using linear interpolation."""

    if not values:
        return float("nan")

    ordered = sorted(values)

    if len(ordered) == 1:
        return ordered[0]

    position = (
        len(ordered) - 1
    ) * percentage / 100

    lower = math.floor(position)
    upper = math.ceil(position)

    if lower == upper:
        return ordered[lower]

    return (
        ordered[lower]
        + (ordered[upper] - ordered[lower])
        * (position - lower)
    )


def write_summary():
    """Calculate median, p95, minimum and maximum latency."""

    benchmark_file = RESULT_DIR / "benchmark.csv"

    with benchmark_file.open(
        newline="", encoding="utf-8"
    ) as file:

        rows = list(csv.DictReader(file))

    grouped = {}

    for row in rows:

        key = (
            int(row["bucket_count"]),
            row["query"],
        )

        grouped.setdefault(key, []).append(
            float(row["latency_ms"])
        )

    output_file = RESULT_DIR / "summary.csv"

    with output_file.open(
        "w", newline="", encoding="utf-8"
    ) as file:

        writer = csv.writer(file)

        writer.writerow([
            "bucket_count",
            "query",
            "runs",
            "median_ms",
            "p95_ms",
            "min_ms",
            "max_ms",
        ])

        for (bucket_count, query), values in sorted(
            grouped.items()
        ):

            writer.writerow([
                bucket_count,
                query,
                len(values),
                f"{statistics.median(values):.3f}",
                f"{percentile(values, 95):.3f}",
                f"{min(values):.3f}",
                f"{max(values):.3f}",
            ])

    log("Summary statistics generated")


# ---------------------------------------------------------------------
# Main experiment
# ---------------------------------------------------------------------

def main():
    """Execute the complete E1 experiment."""

    log("Starting E1 - Bucket count experiment")

    write_config()

    validate_environment()

    validate_source()

    load_file = RESULT_DIR / "load_times.csv"
    benchmark_file = RESULT_DIR / "benchmark.csv"
    tablet_file = RESULT_DIR / "tablet_stats.csv"

    with load_file.open(
        "w", newline="", encoding="utf-8"
    ) as load_handle:

        load_writer = csv.writer(load_handle)

        load_writer.writerow([
            "bucket_count",
            "table",
            "load_time_seconds",
        ])

        with benchmark_file.open(
            "w", newline="", encoding="utf-8"
        ) as benchmark_handle:

            benchmark_writer = csv.writer(
                benchmark_handle
            )

            benchmark_writer.writerow([
                "bucket_count",
                "table",
                "query",
                "run",
                "latency_ms",
                "ok",
            ])

            with tablet_file.open(
                "w", newline="", encoding="utf-8"
            ) as tablet_handle:

                tablet_writer = csv.writer(tablet_handle)

                tablet_writer.writerow([
                    "bucket_count",
                    "table",
                    "logical_tablets",
                    "total_replicas",
                    "backend_id",
                    "replica_count",
                    "percentage",
                ])

                for bucket_count in BUCKET_COUNTS:

                    log(
                        f"Starting configuration: "
                        f"{bucket_count} buckets"
                    )

                    # 1. Create the benchmark table.
                    table = create_table(bucket_count)

                    # 2. Load the source dataset and measure INSERT time.
                    load_time = load_table(table)

                    load_writer.writerow([
                        bucket_count,
                        table,
                        f"{load_time:.3f}",
                    ])

                    load_handle.flush()

                    # 3. Wait for replica health and version consistency.
                    # This time is separate from INSERT load time.
                    wait_for_replica_consistency(
                        table,
                        bucket_count,
                    )

                    # 4. Validate the target against the source.
                    validate_table(table)

                    # 5. Save execution plans.
                    collect_explain_plans(
                        table,
                        bucket_count,
                    )

                    # 6. Run query warm-ups and measurements.
                    run_benchmarks(
                        table,
                        bucket_count,
                        benchmark_writer,
                    )

                    benchmark_handle.flush()

                    # 7. Collect tablet and replica statistics.
                    collect_tablet_stats(
                        bucket_count,
                        table,
                        tablet_writer,
                    )

                    tablet_handle.flush()

                    log(
                        f"Completed configuration: "
                        f"{bucket_count} buckets"
                    )

    # 8. Calculate median, p95, min and max query latency.
    write_summary()

    log("E1 experiment completed successfully")

    log(f"Results saved in: {RESULT_DIR}")


# ---------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":

    try:
        main()

    except KeyboardInterrupt:

        log("Experiment interrupted by user")
        sys.exit(130)

    except Exception as error:

        log(f"ERROR: {error}")
        sys.exit(1)
