#!/usr/bin/env bash
set -euo pipefail

if (( "$#" != 3 )); then
  echo "usage: delete-clickhouse-symbol-days.sh <table_name> <day_expr> <symbol_days_tsv>" >&2
  exit 1
fi

TABLE_NAME="$1"
DAY_EXPR="$2"
SYMBOL_DAYS_TSV="$3"

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-127.0.0.1}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DATABASE="${CLICKHOUSE_DATABASE:-options}"
CLICKHOUSE_DELETE_MUTATION_SYNC="${CLICKHOUSE_DELETE_MUTATION_SYNC:-0}"

if [[ "$CLICKHOUSE_PORT" == "9000" ]]; then
  CLICKHOUSE_HTTP_PORT="8123"
else
  CLICKHOUSE_HTTP_PORT="$CLICKHOUSE_PORT"
fi

if [[ ! -f "$SYMBOL_DAYS_TSV" ]]; then
  echo "symbol-day file not found: $SYMBOL_DAYS_TSV" >&2
  exit 1
fi

python3 - "$TABLE_NAME" "$DAY_EXPR" "$SYMBOL_DAYS_TSV" "$CLICKHOUSE_HOST" "$CLICKHOUSE_HTTP_PORT" "$CLICKHOUSE_USER" "$CLICKHOUSE_PASSWORD" "$CLICKHOUSE_DATABASE" "$CLICKHOUSE_DELETE_MUTATION_SYNC" <<'PY'
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict


def quote_sql(value):
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


table_name = sys.argv[1].strip()
day_expr = sys.argv[2].strip()
tsv_path = sys.argv[3]
host = sys.argv[4]
port = sys.argv[5]
user = sys.argv[6]
password = sys.argv[7]
database = sys.argv[8]
mutation_sync = int(sys.argv[9])

if not re.fullmatch(r"[A-Za-z0-9_]+", table_name):
    raise SystemExit(f"invalid_table_name:{table_name}")

groups = defaultdict(set)
with open(tsv_path, "r", encoding="utf-8") as handle:
    for raw_line in handle:
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        day_iso = parts[0].strip()
        symbol = parts[1].strip().upper()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_iso):
            continue
        if not re.fullmatch(r"[A-Z0-9.\-]+", symbol):
            continue
        groups[day_iso].add(symbol)

if not groups:
    print(json.dumps({
        "event": "clickhouse_symbol_day_delete_skip",
        "table": table_name,
        "reason": "no_symbol_days",
        "path": tsv_path,
    }), flush=True)
    raise SystemExit(0)

base_url = f"http://{host}:{port}/?{urllib.parse.urlencode({'database': database})}"

for day_iso in sorted(groups):
    symbols = sorted(groups[day_iso])
    symbol_sql = ", ".join(quote_sql(symbol) for symbol in symbols)
    sql = f"""
        ALTER TABLE options.{table_name}
        DELETE WHERE {day_expr} = toDate({quote_sql(day_iso)})
          AND symbol IN ({symbol_sql})
        SETTINGS mutations_sync = {mutation_sync}
    """
    print(json.dumps({
        "event": "clickhouse_symbol_day_delete_execute",
        "table": table_name,
        "dayIso": day_iso,
        "symbolCount": len(symbols),
        "mutationSync": mutation_sync,
    }), flush=True)
    request = urllib.request.Request(
        base_url,
        data=sql.encode("utf-8"),
        headers={
            "X-ClickHouse-User": user,
            "X-ClickHouse-Key": password,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=600) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"clickhouse_symbol_day_delete_failed:{table_name}:{day_iso}:{exc.code}:{body}")
    except Exception as exc:
        raise SystemExit(f"clickhouse_symbol_day_delete_failed:{table_name}:{day_iso}:{exc}")

    print(json.dumps({
        "event": "clickhouse_symbol_day_delete_complete",
        "table": table_name,
        "dayIso": day_iso,
        "symbolCount": len(symbols),
        "mutationSync": mutation_sync,
    }), flush=True)
PY
