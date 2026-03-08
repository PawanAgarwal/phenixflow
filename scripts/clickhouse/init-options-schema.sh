#!/usr/bin/env bash
set -euo pipefail

SQL_FILE="${1:-$(cd "$(dirname "$0")" && pwd)/init-options-schema.sql}"
CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DB="${CH_DB:-default}"

if ! command -v clickhouse >/dev/null 2>&1; then
  echo "clickhouse binary not found. Install first with scripts/clickhouse/install-clickhouse.sh"
  exit 1
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE"
  exit 1
fi

echo "Applying schema from $SQL_FILE to ${CH_HOST}:${CH_PORT}..."
clickhouse client \
  --host "$CH_HOST" \
  --port "$CH_PORT" \
  --user "$CH_USER" \
  --password "$CH_PASSWORD" \
  --database "$CH_DB" \
  --multiquery \
  --queries-file "$SQL_FILE"

echo "Schema initialized."
