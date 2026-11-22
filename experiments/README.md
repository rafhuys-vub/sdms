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


