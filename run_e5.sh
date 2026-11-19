#!/usr/bin/env bash
# E5 – scale-out: add a 4th BE to a loaded 3-BE cluster and record how the
# tablet scheduler moves replicas onto it.
#
#   ./gen_data.sh                    # load e5.events first
#   ./run_e5.sh                      # then run the experiment
#   TAG=b12 INTERVAL=2 ./run_e5.sh
#
#   INTERVAL    sampling period in s                                  (5)
#   WARMUP      baseline period before the BE is added in s           (30)
#   STABLE_FOR  converged = distribution unchanged + scheduler idle for this long (60)
#   TIMEOUT     give up after this many s after the add               (1800)
#   QUERIES     1 = run a background query loop and record latencies  (1)
#   TAG         suffix for the results directory
#
# Output: results/<timestamp>[_TAG]/
#   replicas.csv  t,backend,replicas         replicas of e5.events per BE
#   sched.csv     t,pending,running          tablet-scheduler queue sizes
#   queries.csv   t,query,ok,latency_ms      background queries (incl. ~0.1 s docker exec overhead)
#   clones.tsv    SHOW PROC '/cluster_balance/history_tablets' at the end
#   events.txt    timestamps of add / first move / convergence
#   summary.txt   human-readable result
# t is seconds since the start of the run.
set -euo pipefail
cd "$(dirname "$0")"

INTERVAL=${INTERVAL:-5}
WARMUP=${WARMUP:-30}
STABLE_FOR=${STABLE_FOR:-60}
TIMEOUT=${TIMEOUT:-1800}
QUERIES=${QUERIES:-1}
NEW_BE=starrocks-be-3
TABLE=e5.events

OUT=results/$(date +%Y%m%d-%H%M%S)${TAG:+_$TAG}
mkdir -p "$OUT"

sql() { docker compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot --batch --skip-column-names -e "$1"; }
T0=$EPOCHREALTIME
t() { awk -v a="$T0" -v b="$EPOCHREALTIME" 'BEGIN { printf "%.2f", b - a }'; }
log() { echo "[$(t)s] $*" | tee -a "$OUT/events.txt"; }

# --- preflight --------------------------------------------------------------
if sql "SHOW BACKENDS" | grep -q "$NEW_BE"; then
  echo "$NEW_BE is already part of the cluster; start from a fresh cluster:"
  echo "  docker compose --profile scaleout down -v && docker compose up -d && ./gen_data.sh"
  exit 1
fi
sql "SELECT count(*) FROM $TABLE" >/dev/null   # table must exist

{ echo "date: $(date -Is)"; echo "INTERVAL=$INTERVAL WARMUP=$WARMUP STABLE_FOR=$STABLE_FOR";
  echo "--- table"; sql "SHOW CREATE TABLE $TABLE" | sed 's/\\n/\n/g';
  echo "--- partitions"; sql "SELECT count(*) FROM information_schema.partitions_meta WHERE DB_NAME='e5' AND TABLE_NAME='events'";
  echo "--- scheduler config"; sql "ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_sched%'" | cut -f1,3;
} > "$OUT/config.txt"

# --- samplers -----------------------------------------------------------------
echo "t,backend,replicas" > "$OUT/replicas.csv"
echo "t,pending,running" > "$OUT/sched.csv"

# one sample: prints "host=replicas ..." (sorted) and appends to the CSVs
sample() {
  local now ids dist sched
  now=$(t)
  ids=$(sql "SHOW BACKENDS" | cut -f1,2)                        # BackendId  Host
  dist=$(sql "ADMIN SHOW REPLICA DISTRIBUTION FROM $TABLE" | cut -f1,2)  # BackendId ReplicaNum
  sched=$(sql "SHOW PROC '/cluster_balance'")
  join -t $'\t' <(sort <<<"$ids") <(sort <<<"$dist") | sort -k2 \
    | awk -F'\t' -v t="$now" -v f="$OUT/replicas.csv" '{ print t "," $2 "," $3 >> f; printf "%s=%s ", $2, $3 }'
  awk -v t="$now" '$1=="pending_tablets"{p=$2} $1=="running_tablets"{r=$2} END{ print t "," p "," r }' \
    <<<"$sched" >> "$OUT/sched.csv"
}
idle() { tail -1 "$OUT/sched.csv" | awk -F, '{ exit !($2 == 0 && $3 == 0) }'; }

query_loop() {
  echo "t,query,ok,latency_ms" > "$OUT/queries.csv"
  local qs=(
    "SELECT category, count(*), sum(amount) FROM $TABLE GROUP BY category"
    "SELECT event_date, count(DISTINCT user_id) FROM $TABLE GROUP BY event_date"
    "SELECT count(*) FROM $TABLE WHERE user_id = 42"
  )
  while :; do
    for i in "${!qs[@]}"; do
      local s=$EPOCHREALTIME ok=1
      sql "${qs[$i]}" >/dev/null 2>&1 || ok=0
      awk -v s="$s" -v e="$EPOCHREALTIME" -v a="$T0" -v q="q$i" -v ok=$ok \
        'BEGIN { printf "%.2f,%s,%d,%.0f\n", s - a, q, ok, (e - s) * 1000 }' >> "$OUT/queries.csv"
    done
  done
}

if [[ "$QUERIES" == "1" ]]; then
  query_loop & QPID=$!
  trap 'kill $QPID 2>/dev/null || true' EXIT
fi

# --- baseline -------------------------------------------------------------------
log "baseline for ${WARMUP}s: $(sample)"
end=$(awk -v a="$(t)" -v w="$WARMUP" 'BEGIN { print a + w }')
while awk -v a="$(t)" -v e="$end" 'BEGIN { exit !(a < e) }'; do sleep "$INTERVAL"; sample >/dev/null; done

# --- scale out -------------------------------------------------------------------
# be_entrypoint.sh in the container first runs
#   ALTER SYSTEM ADD BACKEND "starrocks-be-3:9050";
# against the FE and then starts the BE process (official StarRocks images).
T_ADD=$(t)
log "ADD: docker compose up -d $NEW_BE"
docker compose --profile scaleout up -d --no-deps "$NEW_BE" 2>/dev/null

T_ALIVE= T_FIRST= last_dist= last_change=$T_ADD
while :; do
  sleep "$INTERVAL"
  dist=$(sample); now=$(t)
  if [[ -z "$T_ALIVE" ]] && sql "SHOW BACKENDS" | grep "$NEW_BE" | grep -qw true; then
    T_ALIVE=$now; log "$NEW_BE alive"
  fi
  if [[ -z "$T_FIRST" ]] && grep -Eq "$NEW_BE=[1-9]" <<<"$dist"; then
    T_FIRST=$now; log "first replica on $NEW_BE"
  fi
  [[ "$dist" != "$last_dist" ]] && { last_dist=$dist; last_change=$now; echo "[${now}s] $dist"; }
  if [[ -n "$T_FIRST" ]] && idle && awk -v n="$now" -v l="$last_change" -v s="$STABLE_FOR" 'BEGIN { exit !(n - l >= s) }'; then
    log "converged (no change since ${last_change}s)"; break
  fi
  if awk -v n="$now" -v a="$T_ADD" -v m="$TIMEOUT" 'BEGIN { exit !(n - a > m) }'; then
    log "TIMEOUT"; break
  fi
done

[[ "$QUERIES" == "1" ]] && kill $QPID 2>/dev/null || true
sql "SHOW PROC '/cluster_balance/history_tablets'" > "$OUT/clones.tsv" || true

# --- summary ----------------------------------------------------------------------
total=$(awk -F, -v t="$(tail -1 "$OUT/replicas.csv" | cut -d, -f1)" '$1==t { s += $3 } END { print s }' "$OUT/replicas.csv")
nbe=$(awk -F, -v t="$(tail -1 "$OUT/replicas.csv" | cut -d, -f1)" '$1==t { n++ } END { print n }' "$OUT/replicas.csv")
moved=$(grep -Eo "$NEW_BE=[0-9]+" <<<"$last_dist" | cut -d= -f2)
d() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.1f", a - b }'; }
{
  echo "replicas total:         $total over $nbe BEs"
  echo "final distribution:     $last_dist"
  echo "replicas moved to new:  ${moved:-0}   (minimum for perfect balance: $((total / nbe)))"
  echo "BE alive after add:     $(d "${T_ALIVE:-$T_ADD}" "$T_ADD") s"
  echo "first move after add:   $(d "${T_FIRST:-$T_ADD}" "$T_ADD") s"
  echo "converged after add:    $(d "$last_change" "$T_ADD") s"
  echo "balance tasks (all):    $(grep -c BALANCE "$OUT/clones.tsv" || true)"
  if [[ -f "$OUT/queries.csv" ]]; then
    awk -F, -v a="$T_ADD" -v c="$last_change" 'NR > 1 {
          p = ($1 < a) ? "1_before" : ($1 <= c ? "2_during" : "3_after");
          n[p]++; s[p] += $4; e[p] += !$3 }
        END { for (p in n) printf "queries %-9s n=%-4d mean=%5.0f ms  errors=%d\n", p, n[p], s[p]/n[p], e[p] }' \
      "$OUT/queries.csv" | sort
  fi
} | tee "$OUT/summary.txt"
echo "results in $OUT"
