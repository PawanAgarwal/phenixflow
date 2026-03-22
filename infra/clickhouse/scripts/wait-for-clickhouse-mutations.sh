#!/usr/bin/env bash
set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-127.0.0.1}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-options}"
CLICKHOUSE_MUTATION_TIMEOUT_SEC="${CLICKHOUSE_MUTATION_TIMEOUT_SEC:-3600}"
CLICKHOUSE_MUTATION_POLL_MS="${CLICKHOUSE_MUTATION_POLL_MS:-2000}"
CLICKHOUSE_MUTATION_STABLE_POLLS="${CLICKHOUSE_MUTATION_STABLE_POLLS:-2}"

if [[ "$CLICKHOUSE_PORT" == "9000" ]]; then
  CLICKHOUSE_HTTP_PORT="8123"
else
  CLICKHOUSE_HTTP_PORT="$CLICKHOUSE_PORT"
fi

python3 - "$CLICKHOUSE_HOST" "$CLICKHOUSE_HTTP_PORT" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_DATABASE" "$CLICKHOUSE_MUTATION_TIMEOUT_SEC" "$CLICKHOUSE_MUTATION_POLL_MS" "$CLICKHOUSE_MUTATION_STABLE_POLLS" "$@" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


def quote_sql(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


host = sys.argv[1]
port = sys.argv[2]
user = sys.argv[3]
password = sys.argv[4]
database = sys.argv[5]
timeout_sec = int(sys.argv[6])
poll_ms = int(sys.argv[7])
stable_polls = int(sys.argv[8])
tables = [table.strip() for table in sys.argv[9:] if table.strip()]

table_filter = ""
if tables:
    table_filter = " AND table IN ({})".format(", ".join(quote_sql(table) for table in tables))

url = f"http://{host}:{port}/?{urllib.parse.urlencode({'database': database, 'default_format': 'JSONEachRow'})}"
deadline = time.time() + timeout_sec
last_signature = None
zero_polls = 0

print(json.dumps({
    "event": "clickhouse_mutation_wait_start",
    "database": database,
    "tables": tables,
    "timeoutSec": timeout_sec,
    "pollMs": poll_ms,
}), flush=True)

while True:
    sql = f"""
        SELECT
          table,
          count() AS mutation_count,
          sum(parts_to_do) AS parts_to_do,
          max(create_time) AS newest_create_time
        FROM system.mutations
        WHERE database = {quote_sql(database)}
          AND is_done = 0
          {table_filter}
        GROUP BY table
        ORDER BY table
    """
    request = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        headers={
            "X-ClickHouse-User": user,
            "X-ClickHouse-Key": password,
        },
    )
    try:
      with urllib.request.urlopen(request, timeout=120) as response:
        rows = [
            json.loads(line)
            for line in response.read().decode("utf-8").splitlines()
            if line.strip()
        ]
    except urllib.error.HTTPError as exc:
      body = exc.read().decode("utf-8", errors="replace")
      raise SystemExit(f"clickhouse mutation wait query failed: HTTP {exc.code}: {body}")
    except Exception as exc:
      raise SystemExit(f"clickhouse mutation wait query failed: {exc}")

    signature = json.dumps(rows, sort_keys=True)
    if signature != last_signature:
      print(json.dumps({
          "event": "clickhouse_mutation_wait_poll",
          "pendingTables": len(rows),
          "pendingMutations": sum(int(row.get("mutation_count") or 0) for row in rows),
          "pendingParts": sum(int(row.get("parts_to_do") or 0) for row in rows),
          "tables": rows,
      }), flush=True)
      last_signature = signature

    if not rows:
      zero_polls += 1
      if zero_polls >= stable_polls:
        print(json.dumps({
            "event": "clickhouse_mutation_wait_complete",
            "database": database,
            "tables": tables,
        }), flush=True)
        break
    else:
      zero_polls = 0

    if time.time() >= deadline:
      raise SystemExit("clickhouse_mutation_wait_timeout")

    time.sleep(poll_ms / 1000.0)
PY
