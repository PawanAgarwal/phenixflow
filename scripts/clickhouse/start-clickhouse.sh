#!/usr/bin/env bash
set -euo pipefail

CH_ROOT="${CH_ROOT:-/Volumes/Phenix4TB/clickhouse}"
CONFIG_FILE="${CONFIG_FILE:-/Users/pawanagarwal/github/phenixflow/config/clickhouse/config.xml}"
PID_FILE="${PID_FILE:-$CH_ROOT/run/clickhouse-server.pid}"

if ! command -v clickhouse >/dev/null 2>&1; then
  echo "clickhouse binary not found. Install first with scripts/clickhouse/install-clickhouse.sh"
  exit 1
fi

if [[ ! -d "/Volumes/Phenix4TB" ]]; then
  echo "Expected external volume not mounted: /Volumes/Phenix4TB"
  exit 1
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "Missing ClickHouse config: $CONFIG_FILE"
  exit 1
fi

mkdir -p \
  "$CH_ROOT/lib" \
  "$CH_ROOT/log" \
  "$CH_ROOT/tmp" \
  "$CH_ROOT/user_files" \
  "$CH_ROOT/format_schemas" \
  "$CH_ROOT/run"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
    echo "ClickHouse already running (pid=$existing_pid)."
    exit 0
  fi
fi

clickhouse server \
  --config-file "$CONFIG_FILE" \
  --daemon \
  --pidfile "$PID_FILE"

sleep 1
if [[ -f "$PID_FILE" ]]; then
  started_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$started_pid" ]] && kill -0 "$started_pid" >/dev/null 2>&1; then
    echo "ClickHouse started (pid=$started_pid)."
    exit 0
  fi
fi

echo "ClickHouse did not start cleanly. Check logs in $CH_ROOT/log."
exit 1
