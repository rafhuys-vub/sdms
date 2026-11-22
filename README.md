# Starrocks tablet rebalance/restore experiments

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
alias sql='docker compose exec starrocks-fe mysql -h127.0.0.1 -P9030 -uroot'
sql -e "SHOW BACKENDS\G"                    # Alive: true x3
sql                                         # interactive SQL shell
```

### Connect with MySQL client

Create a **MySQL** connection with host `127.0.0.1`, port `9030`, user `root`,
and a blank password.

Useful statements while experimenting (cfr. https://github.com/StarRocks/starrocks-debug-skills):

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
./gen_data.sh # 10M rows, 30 days, 12 buckets, 3 replicas
ROWS=20000000 DAYS=30 BUCKETS=48 SKEW=1.1 ./gen_data.sh
```

Partitioning and bucketing are in the `CREATE TABLE` in `gen_data.sh`; edit it to try other keys.
Rows come from `INSERT INTO … SELECT … FROM TABLE(generate_series(1, ROWS))` (https://docs.starrocks.io/docs/sql-reference/sql-functions/table-functions/generate_series/) 
with hash-derived pseudo-random values (https://docs.starrocks.io/docs/sql-reference/sql-functions/hash-functions/murmur_hash3_32/), so they're deterministic and about 1M rows/s. 
`SKEW` = 0 in this experiment. `user_id` is the bucket key, so skew makes some tablets bigger than others.
Number of tablets = `DAYS × BUCKETS`.

## Experiments

see experiments