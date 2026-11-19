# E5 MVP – scale-out & tablet rebalancing in StarRocks

Minimal version of experiment E5 (see `../task3_proposal.md`; the full toolkit is in `../task3`).
Three files do the work:

| file | what |
|---|---|
| `docker-compose.yml` | 1 FE + 3 BEs (`starrocks-be-0..2`) + a 4th BE `starrocks-be-3` in profile `scaleout` |
| `gen_data.sh` | creates `e5.events` (daily range partitions, `HASH(user_id) BUCKETS n`) and generates rows inside StarRocks |
| `run_e5.sh` | baseline → start `starrocks-be-3` → sample until the scheduler has rebalanced → summary |

`plot.py` draws a timeline of a run. You need Docker and, for the plot only, [uv](https://docs.astral.sh/uv/).
No MySQL client is needed on the host: every command runs `mysql` inside the FE container.

## Cluster

The compose file follows the official StarRocks one
([StarRocks/demo `docker-compose-3BE.yml`](https://github.com/StarRocks/demo/blob/master/deploy/docker-compose/docker-compose-3BE.yml)):
containers start with the image entrypoints `fe_entrypoint.sh` / `be_entrypoint.sh`, and `HOST_TYPE=FQDN`
makes nodes register by hostname. **`be_entrypoint.sh` registers its own BE**: before it starts the BE it runs
`ALTER SYSTEM ADD BACKEND "<hostname>:9050"` against the FE. Starting a BE container is therefore all it
takes to add a node ([Scale in and out](https://docs.starrocks.io/docs/administration/management/Scale_up_down/)).
`conf/*.conf` are the image defaults with lower memory limits (FE heap 2 GB, BE `mem_limit = 3G`).

```bash
docker compose up -d                       # FE + 3 BEs, they register themselves
alias sr='docker compose exec starrocks-fe mysql -h127.0.0.1 -P9030 -uroot'
sr -e "SHOW BACKENDS\G"                    # Alive: true x3
sr                                         # interactive SQL shell
```

### Connect with DBeaver

Create a **MySQL** connection with host `127.0.0.1`, port `9030`, user `root`,
and a blank password. Database `e5` is available after `./gen_data.sh` runs;
leave the database field empty if connecting before then. Download the MySQL
JDBC driver if DBeaver prompts you, and test the connection. The compose ports
are bound to localhost only: the default root account has no password and is
intended for local experiments, not a public network.

Useful statements while experimenting:

```sql
SHOW BACKENDS;                                        -- nodes, TabletNum, DataUsedCapacity
ADMIN SHOW REPLICA DISTRIBUTION FROM e5.events;       -- replicas per BE for one table
SHOW TABLET FROM e5.events;                           -- every replica: tablet -> BackendId, size
SHOW PROC '/cluster_balance';                         -- tablet scheduler: pending/running/history
SHOW PROC '/cluster_balance/history_tablets';         -- finished clone tasks (SrcBe -> DestBe)
ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_sched%';      -- scheduler knobs
ADMIN SET FRONTEND CONFIG ("tablet_sched_max_balancing_tablets" = "100");   -- not persistent
```

Scaling by hand: `docker compose --profile scaleout up -d starrocks-be-3` (add a node),
`ALTER SYSTEM DECOMMISSION BACKEND "starrocks-be-3:9050";` (remove it gracefully).
Start again from scratch: `docker compose --profile scaleout down -v`.

## Data

```bash
./gen_data.sh                                        # 10M rows, 30 days, 12 buckets, 3 replicas
ROWS=20000000 DAYS=30 BUCKETS=48 SKEW=1.1 ./gen_data.sh
```

Partitioning and bucketing are in the `CREATE TABLE` in `gen_data.sh`; edit it to try other keys.
Rows come from `INSERT INTO … SELECT … FROM TABLE(generate_series(1, ROWS))` with hash-derived pseudo-random
columns, so they're deterministic and about 1M rows/s. `SKEW` > 0 draws `user_id` from a Zipf(s)
distribution. `user_id` is the bucket key, so skew makes some tablets bigger than others.
Number of tablets = `DAYS × BUCKETS`, number of replicas = that × `REPLICAS`.

## Experiment

```bash
docker compose --profile scaleout down -v && docker compose up -d     # fresh 3-BE cluster
./gen_data.sh
TAG=b12 ./run_e5.sh
uv run plot.py results/<run>
```

`run_e5.sh` samples every `INTERVAL` s. It reads `ADMIN SHOW REPLICA DISTRIBUTION` (replicas per BE) and
`SHOW PROC '/cluster_balance'` (scheduler queue) and runs a background query loop.
After a `WARMUP` baseline it starts `starrocks-be-3`. The run counts as converged once the distribution
hasn't changed for `STABLE_FOR` s and the scheduler is idle.
The convergence time is when the distribution last changed. Results go to `results/<timestamp>_<TAG>/`:
`replicas.csv`, `sched.csv`, `queries.csv`, `clones.tsv`, `config.txt` (DDL + scheduler config),
`events.txt`, `summary.txt`.

Smoke run (2M rows, 10 partitions × 4 buckets × 3 replicas = 120 replicas):

```
replicas moved to new:  30   (minimum for perfect balance: 30)
BE alive after add:     8.3 s
first move after add:   19.5 s
converged after add:    41.5 s
queries 1_before  n=48   mean=  240 ms  errors=0
queries 2_during  n=177  mean=  231 ms  errors=0
queries 3_after   n=145  mean=  223 ms  errors=0
```

The new BE is filled first: clones bring it to 30 while the old BEs still hold 40 each.
About 10 s later the now-redundant source replicas are deleted (`REPAIR/REDUNDANT` tasks).
The scheduler works in rounds (`tablet_sched_checker_interval_seconds`, default 20 s).
Things to vary: `BUCKETS` (tablet granularity), `ROWS` (tablet size), `SKEW`,
and the scheduler knobs `tablet_sched_max_balancing_tablets`, `tablet_sched_slot_num_per_path`
and `tablet_sched_checker_interval_seconds`.
