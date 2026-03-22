#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/clickhouse-env.sh"

if [[ ! -d "$CH_VOLUME" ]]; then
  echo "Volume not mounted: $CH_VOLUME"
  echo "Set CH_VOLUME or CH_VOLUME_NAME if the external drive uses a different mount path."
  exit 1
fi

mkdir -p \
  "$CH_ROOT" \
  "$CH_LIB_DIR" \
  "$CH_LOG_DIR" \
  "$CH_TMP_DIR" \
  "$CH_USER_FILES_DIR" \
  "$CH_FORMAT_SCHEMAS_DIR" \
  "$CH_RUN_DIR"

cat > "$CH_ROOT/CLICKHOUSE_VOLUME_LAYOUT.txt" <<EOF
ClickHouse external volume prepared by PhenixFlow.

Volume path: $CH_VOLUME
ClickHouse root: $CH_ROOT
Data dir: $CH_LIB_DIR
Log dir: $CH_LOG_DIR
Tmp dir: $CH_TMP_DIR
User files dir: $CH_USER_FILES_DIR
Format schemas dir: $CH_FORMAT_SCHEMAS_DIR
Run dir: $CH_RUN_DIR
EOF

echo "Prepared ClickHouse external volume layout:"
echo "  volume: $CH_VOLUME"
echo "  root:   $CH_ROOT"
echo "  data:   $CH_LIB_DIR"
echo "  logs:   $CH_LOG_DIR"
