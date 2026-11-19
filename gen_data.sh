#!/usr/bin/env bash
# Create e5.events and fill it with synthetic rows, generated inside StarRocks
# (INSERT ... SELECT FROM generate_series), so no data files are needed.
#
#   ROWS=10000000 DAYS=30 BUCKETS=12 SKEW=0 ./gen_data.sh
#
#   ROWS     number of rows                                   (default 10M)
#   DAYS     number of daily range partitions, from 2024-01-01 (default 30)
#   BUCKETS  hash buckets (= tablets) per partition            (default 12)
#   REPLICAS replication_num                                   (default 3)
#   USERS    distinct user_id values                           (default 1M)
#   SKEW     0 = uniform user_id, s > 0 = Zipf(s) over USERS   (default 0)
#            (the bucket key is user_id, so skew -> unequal tablet sizes)
set -euo pipefail
cd "$(dirname "$0")"

ROWS=${ROWS:-10000000}
DAYS=${DAYS:-30}
BUCKETS=${BUCKETS:-12}
REPLICAS=${REPLICAS:-3}
USERS=${USERS:-1000000}
SKEW=${SKEW:-0}

sql() { docker compose exec -T starrocks-fe mysql -h127.0.0.1 -P9030 -uroot "$@"; }

END_DATE=$(date -d "2024-01-01 + ${DAYS} days" +%F)

# u_x: deterministic uniform number in [0,1) derived from the row number g
u() { echo "((CAST(murmur_hash3_32(concat('$1', CAST(g AS VARCHAR))) AS DOUBLE) + 2147483648) / 4294967296)"; }

if [[ "$SKEW" == "0" ]]; then
  USER_EXPR="CAST(floor($(u user) * ${USERS}) + 1 AS BIGINT)"
else
  # inverse-CDF sampling of a bounded power law P(k) ~ k^-s, k in 1..USERS
  # (s = 1 needs the logarithmic form)
  if [[ "$SKEW" == "1" || "$SKEW" == "1.0" ]]; then
    RANK="floor(exp($(u user) * ln(${USERS} + 1)))"
  else
    RANK="floor(pow((pow(${USERS} + 1, 1 - ${SKEW}) - 1) * $(u user) + 1, 1 / (1 - ${SKEW})))"
  fi
  USER_EXPR="CAST(least(greatest(${RANK}, 1), ${USERS}) AS BIGINT)"
fi

echo "creating e5.events: ${DAYS} partitions x ${BUCKETS} buckets x ${REPLICAS} replicas"
sql <<SQL
CREATE DATABASE IF NOT EXISTS e5;
DROP TABLE IF EXISTS e5.events FORCE;
CREATE TABLE e5.events (
    event_date DATE         NOT NULL,
    user_id    BIGINT       NOT NULL,
    event_id   BIGINT       NOT NULL,
    category   VARCHAR(16)  NOT NULL,
    amount     DECIMAL(12,2) NOT NULL,
    payload    VARCHAR(128) NOT NULL
)
DUPLICATE KEY (event_date, user_id)
PARTITION BY RANGE (event_date) (
    START ("2024-01-01") END ("${END_DATE}") EVERY (INTERVAL 1 DAY)
)
DISTRIBUTED BY HASH (user_id) BUCKETS ${BUCKETS}
PROPERTIES ("replication_num" = "${REPLICAS}");
SQL

echo "generating ${ROWS} rows (users=${USERS}, skew=${SKEW}) ..."
start=$(date +%s)
sql <<SQL
SET query_timeout = 7200;
INSERT INTO e5.events
SELECT
    date_add('2024-01-01', CAST(floor($(u day) * ${DAYS}) AS INT)),
    ${USER_EXPR},
    g,
    concat('cat_', CAST(CAST(floor($(u cat) * 20) AS INT) AS VARCHAR)),
    CAST($(u amt) * 1000 AS DECIMAL(12,2)),
    repeat(md5(CAST(g AS VARCHAR)), 4)
FROM TABLE(generate_series(1, ${ROWS})) AS t(g);
SQL
echo "done in $(( $(date +%s) - start ))s"

sql -t <<'SQL'
SELECT count(*) AS row_count, count(DISTINCT user_id) AS users FROM e5.events;
ADMIN SHOW REPLICA DISTRIBUTION FROM e5.events;
SQL
