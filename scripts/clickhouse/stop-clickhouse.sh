#!/usr/bin/env bash
set -euo pipefail

CH_ROOT="${CH_ROOT:-/Volumes/Phenix4TB/clickhouse}"
PID_FILE="${PID_FILE:-$CH_ROOT/run/clickhouse-server.pid}"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    echo "Stopping ClickHouse pid=$pid..."
    kill "$pid"
    for _ in $(seq 1 20); do
      if ! kill -0 "$pid" >/dev/null 2>&1; then
        rm -f "$PID_FILE"
        echo "ClickHouse stopped."
        exit 0
      fi
      sleep 0.5
    done
    echo "Graceful stop timed out, force killing pid=$pid..."
    kill -9 "$pid" >/dev/null 2>&1 || true
    rm -f "$PID_FILE"
    echo "ClickHouse stopped (forced)."
    exit 0
  fi
fi

echo "ClickHouse is not running."
