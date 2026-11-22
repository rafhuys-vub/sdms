# Experiments

**E1 – Bucket count / tablet granularity vs performance**

1. Create identical datasets with 1.5M rows using nine hash-bucket configurations: 1, 3, 6, 12, 24, 48, 96, 192, and 384 buckets.
   Keep the schema, distribution key, replication factor, and cluster configuration constant.
2. Measure data-loading time for each configuration.
3. Compare full-scan aggregation and point-query latency using warm-up executions and repeated measurements.
4. Record EXPLAIN plans, tablet counts, tablet distribution across BEs, and available storage/segment statistics.
5. Calculate median and p95 query latency and analyze the relationship between bucket count, tablet granularity, and performance.

**E5 – Elastic scale-out and rebalancing**
1. Load data on 3 BEs.
2. Add a 4th BE.
3. Record tablets per BE over time until balanced.
4. Record the amount of data moved, and the time to converge.
5. Measure query latency during rebalancing (run a background query loop).


**E6 – Failure and tablet replication recovery**
1. With 3 replicas on 4 BEs, kill one BE (`docker compose kill`).
2. Check that queries still succeed.
3. After `tablet_repair_delay_factor_second`, measure time to full repair and which BEs act as clone sources and targets. The work should be spread over the surviving BEs.



**E3 – Pruning: partition + bucket**
1. Create a partitioned and hash-bucketed `e3.events` table (`1.5M` rows, `7` daily range partitions, `12` hash buckets = `84` tablets).
2. Compare `EXPLAIN` plans and latencies (`SET enable_query_cache = false;`) across baseline, bucket equality predicate, non-bucket predicate, single-partition date range, and combined date + bucket filter.
3. Record partition pruning (`partitions`), bucket/tablet pruning (`tabletRatio`), and `median` / `p95` query latency.
