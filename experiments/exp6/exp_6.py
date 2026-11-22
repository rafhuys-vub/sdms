
import csv
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

# based on https://github.com/StarRocks/starrocks-debug-skills/blob/main/balance/SKILL.md

ROOT = Path(__file__).resolve().parents[2]
BACKENDS = ["starrocks-be-0", "starrocks-be-1", "starrocks-be-2", "starrocks-be-3"]
DB_NAME = "e5"
EXP_START = time.monotonic()
RESTORE_DEADLINE = 1800
RESULTS_PATH =  ROOT / "experiments"/ "exp6" /  "results" / datetime.now().strftime("%Y%m%d-%H%M%S")
RESULTS_PATH.mkdir(parents=True, exist_ok=True)
STOP_QUERIES = threading.Event()
TABLE = "e5.events"
VICTIM = "starrocks-be-3"

def sql(statement):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "starrocks-fe", "mysql", "-h127.0.0.1",
         "-P9030", "-uroot", "--batch", "--skip-column-names", "-e", statement],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if result.returncode:
        raise RuntimeError(f"SQL failed: {statement}\n{result.stderr.strip()}")
    return result.stdout.strip()

def backends():
    """Return the FE's backend list, keyed by hostname."""
    result = {}
    for line in sql("SHOW BACKENDS").splitlines():
        cols = line.split("\t")
        if len(cols) >= 9 and cols[0].isdigit():
            result[cols[0]] = {"id": cols[0], "host": cols[1], "alive": cols[8].lower() == "true"}
            result[cols[1]] = {"id": cols[0], "host": cols[1], "alive": cols[8].lower() == "true"}
    return result

def set_fe_config(key, value):
    sql(f'ADMIN SET FRONTEND CONFIG ("{key}" = "{value}")')
    
def docker(*args, check=True):
    return subprocess.run(["docker", "compose", *args], check=check, text=True,
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
    while time.monotonic() < start + timeout :
        if predicate():
            return elapsed()
        time.sleep(1)
    raise TimeoutError(description)

def log(message):
    line = f"[{elapsed():.2f}s] {message}"
    print(line, flush=True)
    with (RESULTS_PATH / "events.txt").open("a") as f:
        f.write(line + "\n")
        
def query_statistic():
    # SHOW PROC "/statistic";
    # 
    # DbId |DbName            |TableNum|PartitionNum|IndexNum|TabletNum|ReplicaNum|UnhealthyTabletNum|InconsistentTabletNum|CloningTabletNum|ErrorStateTabletNum|
    # -----+------------------+--------+------------+--------+---------+----------+------------------+---------------------+----------------+-------------------+
    # 11479|_statistics_      |8       |10          |10      |75       |225       |0                 |0                    |0               |0                  |
    # 10004|e5                |1       |30          |30      |360      |1080      |0                 |0                    |0               |0                  |
    # 1    |information_schema|0       |0           |0       |0        |0         |0                 |0                    |0               |0                  |
    # 100  |sys               |0       |0           |0       |0        |0         |0                 |0                    |0               |0                  |
    # Total|4                 |9       |40          |40      |435      |1305      |0                 |0                    |0               |0                  |
    result = {}
    stat = sql("SHOW PROC '/statistic'")
    columns = [
           "DbId", "DbName", "TableNum", "PartitionNum", "IndexNum",
           "TabletNum", "ReplicaNum", "UnhealthyTabletNum",
           "InconsistentTabletNum", "CloningTabletNum", "ErrorStateTabletNum",
       ]
    
    for line in stat.splitlines():
        values = line.split("\t")
        if values[1] == "e5":
            result = dict(zip(columns, values))
            for column in columns[2:]:
                result[column] = int(result[column])
            break
    return result
        
def query_cluster_balance():
    # SHOW PROC '/cluster_balance';
    # 
    # Item             |Number|
    # -----------------+------+
    # cluster_load_stat|1     |
    # working_slots    |4     |
    # sched_stat       |1     |
    # priority_repair  |0     |
    # pending_tablets  |0     |
    # running_tablets  |0     |
    # history_tablets  |1000  |
    # all_tablets      |0     |
    
    scheduler_text = sql("SHOW PROC '/cluster_balance'")
    result = {}
    for line in scheduler_text.splitlines():
        cols = line.split("\t")
        result[cols[0]] = int(cols[1])
    return  result

def query_replica_distribution():
    # ADMIN SHOW REPLICA DISTRIBUTION FROM e5.events;
    # 
    # BackendId|ReplicaNum|Graph|Percent|
    # ---------+----------+-----+-------+
    # 10001    |360       |>>   |33.33 %|
    # 10002    |360       |>>   |33.33 %|
    # 10003    |360       |>>   |33.33 %|
    # 11785    |0         |     |.00 %  |
    result = {}
    nodes_by_id = backends()
    for line in sql(f"ADMIN SHOW REPLICA DISTRIBUTION FROM {TABLE}").splitlines():
        cols = line.split("\t")
        backend = nodes_by_id.get(cols[0])
        result[backend["host"]] = { "replicaNum": int(cols[1]) , "alive": backend["alive"] }
    return result

def sample(writer):
    now = elapsed()
    balance = query_cluster_balance()
    replica_distribution = query_replica_distribution()
    stat = query_statistic()
    
    print(stat)
    
    alive_replicas = 0
    for host, value in replica_distribution.items():
        writer.writerow([f"{now:.2f}", "replicas", host, value["replicaNum"]])
        if value["alive"]:
            alive_replicas += value["replicaNum"]
    writer.writerow([f"{now:.2f}", "health", "unhealthy_tablets", stat["UnhealthyTabletNum"]])
    writer.writerow([f"{now:.2f}", "scheduler", "pending", balance["pending_tablets"]])
    writer.writerow([f"{now:.2f}", "scheduler", "running", balance["running_tablets"]])
    
    return alive_replicas, stat["UnhealthyTabletNum"], balance["pending_tablets"], balance["running_tablets"]

def main():
    # TODO: https://github.com/StarRocks/starrocks-debug-skills/blob/main/balance/references/02-decommission-checklist.md

    lines = sql(f"ADMIN SHOW FRONTEND CONFIG")

    query_thread = None
    
    # is cluster up to start exp?
    bs = backends()
    alive_backends = list({node["host"] for node in bs.values()  if node["alive"]})
    alive_backends.sort()
    if alive_backends != BACKENDS:
        raise RuntimeError(f"unexpected backends: {alive_backends}")
    log(f"start experiment with backends: {bs}")
    
    # shorten cluster config defaults so cluster starts rebalancing quicker
    set_fe_config("tablet_sched_be_down_tolerate_time_s", "30")
    set_fe_config("tablet_sched_repair_delay_factor_second", "60")
    
    # start query loop to have baseline
    query_thread = threading.Thread(target=query_loop, daemon=True)
    query_thread.start()
    
    # calculate total replicas
    total_replicas = 0
    for _, value in query_replica_distribution().items():
        total_replicas += value["replicaNum"]
    
    with (RESULTS_PATH / "samples.csv").open("a", newline="") as f:
        writer = csv.writer(f)
        
        # start sampling for 30s before backend kill to have baseline
        initial_alive_replicas = 0
        baseline_end = time.monotonic() + 6
        while time.monotonic() < baseline_end:
            time.sleep(5)
            alive_replicas, _, _, _ = sample(writer)
            initial_alive_replicas = alive_replicas
            
        
        # kill backend
        log(f"docker compose kill {VICTIM}")
        docker("kill", VICTIM)
        
        
        # wait until backend is gone
        time_killed = wait_until(
            lambda: backends()[VICTIM] and not backends()[VICTIM]["alive"], 300, f"FE did not mark {VICTIM} dead")
        log(f"FE marked {VICTIM} dead after {time_killed}")
            
        # monitor replica repair on surviving backends
        restore_end = time.monotonic() + RESTORE_DEADLINE
        while time.monotonic() < restore_end:
            time.sleep(5)
            alive_replicas, unhealthy, pending, running = sample(writer)
            
            if pending > 0 or running >0 or alive_replicas > initial_alive_replicas:
                log("repair activity observed")
                
            if alive_replicas == total_replicas and unhealthy == 0:
                log(f"fully re-replicated")
                break
            
            log(f"alive replicas {alive_replicas}/{total_replicas}, unhealthy={unhealthy}, scheduler={pending}/{running}")
    
    # clean up
    STOP_QUERIES.set()
    if query_thread:
        query_thread.join(timeout=10)

    (RESULTS_PATH / "clones.tsv").write_text(sql("SHOW PROC '/cluster_balance/history_tablets'"))
    print(f"results in {RESULTS_PATH}")

if __name__ == "__main__":
    main()