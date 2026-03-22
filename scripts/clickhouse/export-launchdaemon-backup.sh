#!/usr/bin/env bash
set -euo pipefail

TS="$(/bin/date +%Y%m%dT%H%M%S)"
LABEL="${CLICKHOUSE_DAEMON_LABEL:-com.phenixflow.clickhouse}"
BACKUP_ROOT="${CLICKHOUSE_BACKUP_ROOT:-$HOME/.config/phenixflow-clickhouse/backups}"
BACKUP_DIR="${CLICKHOUSE_BACKUP_DIR:-$BACKUP_ROOT/${LABEL}-${TS}}"
SYSTEM_DIR="${CLICKHOUSE_SYSTEM_DIR:-/Library/Application Support/PhenixFlow/clickhouse}"
PLIST_PATH="${CLICKHOUSE_DAEMON_PLIST:-/Library/LaunchDaemons/${LABEL}.plist}"
OUT_LOG="${CLICKHOUSE_OUT_LOG:-/Library/Logs/phenixflow-clickhouse.launchd.out.log}"
ERR_LOG="${CLICKHOUSE_ERR_LOG:-/Library/Logs/phenixflow-clickhouse.launchd.err.log}"

/bin/mkdir -p "$BACKUP_DIR"

copy_if_exists() {
  local src="$1"
  local dest="$2"
  if [[ -e "$src" ]]; then
    /bin/cp -R "$src" "$dest"
  fi
}

copy_if_exists "$PLIST_PATH" "$BACKUP_DIR/"
copy_if_exists "$SYSTEM_DIR" "$BACKUP_DIR/"
copy_if_exists "$OUT_LOG" "$BACKUP_DIR/"
copy_if_exists "$ERR_LOG" "$BACKUP_DIR/"

{
  echo "timestamp=$TS"
  echo "label=$LABEL"
  echo "plist=$PLIST_PATH"
  echo "system_dir=$SYSTEM_DIR"
  /bin/launchctl print "system/$LABEL" 2>/dev/null || true
} > "$BACKUP_DIR/launchctl-status.txt"

echo "$BACKUP_DIR"
