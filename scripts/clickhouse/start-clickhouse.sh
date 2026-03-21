#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/clickhouse-env.sh"

if ! command -v clickhouse >/dev/null 2>&1; then
  echo "clickhouse binary not found. Install first with scripts/clickhouse/install-clickhouse.sh"
  exit 1
fi

if [[ ! -d "$CH_VOLUME" ]]; then
  echo "Expected external volume not mounted: $CH_VOLUME"
  exit 1
fi

if [[ ! -f "$CLICKHOUSE_USERS_CONFIG" ]]; then
  echo "Missing ClickHouse users config: $CLICKHOUSE_USERS_CONFIG"
  exit 1
fi

bash "$(cd "$(dirname "$0")" && pwd)/prepare-external-volume.sh" >/dev/null
bash "$(cd "$(dirname "$0")" && pwd)/render-clickhouse-config.sh" >/dev/null

max_server_memory_usage="$(compute_clickhouse_memory_limit_bytes)"
mark_cache_size="$(compute_clickhouse_mark_cache_bytes)"
uncompressed_cache_size="$(compute_clickhouse_uncompressed_cache_bytes)"

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

for _ in $(seq 1 20); do
  sleep 0.5
  if [[ ! -f "$PID_FILE" ]]; then
    continue
  fi

  started_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -z "$started_pid" ]] || ! kill -0 "$started_pid" >/dev/null 2>&1; then
    continue
  fi

  if clickhouse client --host 127.0.0.1 --port 9000 --query "SELECT version()" >/dev/null 2>&1; then
    echo "ClickHouse started (pid=$started_pid)."
    echo "  volume: $CH_VOLUME"
    echo "  config: $CONFIG_FILE"
    echo "  memory limit bytes: $max_server_memory_usage"
    echo "  mark cache bytes: $mark_cache_size"
    echo "  uncompressed cache bytes: $uncompressed_cache_size"
    exit 0
  fi
done

echo "ClickHouse did not start cleanly. Check logs in $CH_ROOT/log."
exit 1
