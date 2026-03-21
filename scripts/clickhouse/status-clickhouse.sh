#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/clickhouse-env.sh"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    echo "ClickHouse running (pid=$pid)."
    if clickhouse client --host 127.0.0.1 --port 9000 --query "SELECT version()"; then
      exit 0
    fi
    echo "ClickHouse process is alive but not accepting connections yet."
    exit 1
  fi
fi

echo "ClickHouse not running."
exit 1
