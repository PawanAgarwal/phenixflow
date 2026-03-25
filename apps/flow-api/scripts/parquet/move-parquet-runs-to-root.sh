#!/usr/bin/env bash
set -euo pipefail

SOURCE_ROOT="${PARQUET_SOURCE_ROOT:-$HOME/Library/Caches/phenixflow/parquet}"
DEST_ROOT="${PHENIXFLOW_PARQUET_ROOT:-/Volumes/Phenix4TB/phenixflow/parquet}"
ACTIVE_RUN_ID="${PARQUET_ACTIVE_RUN_ID:-}"
WAIT_SECONDS="${PARQUET_MIGRATE_WAIT_SECONDS:-60}"

echo "Source root: $SOURCE_ROOT"
echo "Destination root: $DEST_ROOT"

if [ "$SOURCE_ROOT" = "$DEST_ROOT" ]; then
  echo "Source and destination roots are the same; nothing to move."
  exit 0
fi

if [ ! -d "$SOURCE_ROOT" ]; then
  echo "Source root does not exist; nothing to move."
  exit 0
fi

mkdir -p "$DEST_ROOT/runs"

if [ -n "$ACTIVE_RUN_ID" ] && [ -d "$SOURCE_ROOT/runs/$ACTIVE_RUN_ID" ]; then
  SUMMARY_PATH="$SOURCE_ROOT/runs/$ACTIVE_RUN_ID/reports/summary.json"
  echo "Waiting for active run to finish: $ACTIVE_RUN_ID"
  while [ ! -f "$SUMMARY_PATH" ]; do
    sleep "$WAIT_SECONDS"
  done
  echo "Active run completed: $ACTIVE_RUN_ID"
fi

shopt -s nullglob
for entry in "$SOURCE_ROOT"/runs/*; do
  name="$(basename "$entry")"
  if [ -e "$DEST_ROOT/runs/$name" ]; then
    echo "Skipping existing run at destination: $name"
    continue
  fi
  echo "Moving run: $name"
  mv "$entry" "$DEST_ROOT/runs/"
done

echo "Parquet run migration complete."
