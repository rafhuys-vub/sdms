# Experiments

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
