#!/usr/bin/env bash
set -euo pipefail

CH_ROOT="${CH_ROOT:-/Volumes/Phenix4TB/clickhouse}"
PID_FILE="${PID_FILE:-$CH_ROOT/run/clickhouse-server.pid}"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    echo "ClickHouse running (pid=$pid)."
    clickhouse client --host 127.0.0.1 --port 9000 --query "SELECT version()" || true
    exit 0
  fi
fi

echo "ClickHouse not running."
exit 1
