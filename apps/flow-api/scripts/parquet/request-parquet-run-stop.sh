#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"

DEFAULT_LOCAL_PARQUET_ROOT="${HOME}/Library/Caches/phenixflow/parquet"
DEFAULT_EXTERNAL_PARQUET_ROOT="/Volumes/Phenix4TB/phenixflow/parquet"

if [ $# -lt 1 ]; then
  echo "usage: $0 <run-id> [reason]" >&2
  exit 1
fi

RUN_ID="$1"
REASON="${2:-manual_stop}"

if [ -n "${PHENIXFLOW_PARQUET_ROOT:-}" ]; then
  PARQUET_ROOT="$PHENIXFLOW_PARQUET_ROOT"
elif [ -d "/Volumes/Phenix4TB" ]; then
  PARQUET_ROOT="$DEFAULT_EXTERNAL_PARQUET_ROOT"
else
  PARQUET_ROOT="$DEFAULT_LOCAL_PARQUET_ROOT"
fi

RUN_ROOT="$PARQUET_ROOT/runs/$RUN_ID"
STOP_DIR="$RUN_ROOT/state/control"
STOP_PATH="$STOP_DIR/stop-requested.json"

mkdir -p "$STOP_DIR"
cat >"$STOP_PATH" <<EOF
{
  "reason": "$REASON",
  "requestedAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "pid": $$
}
EOF

echo "Stop requested for $RUN_ID -> $STOP_PATH"
