#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

INIT_SCHEMA="${INIT_SCHEMA:-0}"

bash "$SCRIPT_DIR/install-clickhouse.sh" status
bash "$SCRIPT_DIR/prepare-external-volume.sh"
bash "$SCRIPT_DIR/start-clickhouse.sh"

if [[ "$INIT_SCHEMA" == "1" ]]; then
  bash "$SCRIPT_DIR/init-options-schema.sh"
fi

bash "$SCRIPT_DIR/status-clickhouse.sh"
