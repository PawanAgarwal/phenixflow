#!/bin/bash
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
MONITOR_SCRIPT="$ROOT_DIR/apps/flow-api/scripts/parquet/monitor-parquet-run.js"
RUN_SCRIPT="$ROOT_DIR/apps/flow-api/scripts/parquet/run-jan2025-week-benchmark.sh"

detect_default_parquet_workers() {
  local cpu_count workers
  cpu_count="$(sysctl -n hw.ncpu 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo 8)"
  if ! [[ "$cpu_count" =~ ^[0-9]+$ ]]; then
    cpu_count=8
  fi
  workers=$(( cpu_count > 2 ? cpu_count - 2 : 1 ))
  if (( workers > 8 )); then
    workers=8
  fi
  if (( workers < 1 )); then
    workers=1
  fi
  printf '%s\n' "$workers"
}

DEFAULT_LOCAL_PARQUET_ROOT="${HOME}/Library/Caches/phenixflow/parquet"
DEFAULT_EXTERNAL_PARQUET_ROOT="/Volumes/Phenix4TB/phenixflow/parquet"

if [ -n "${PHENIXFLOW_PARQUET_ROOT:-}" ]; then
  PARQUET_ROOT="$PHENIXFLOW_PARQUET_ROOT"
elif [ -d "/Volumes/Phenix4TB" ]; then
  PARQUET_ROOT="$DEFAULT_EXTERNAL_PARQUET_ROOT"
else
  PARQUET_ROOT="$DEFAULT_LOCAL_PARQUET_ROOT"
fi

RUN_ID="${PARQUET_RUN_ID:-parquet-feb2025-20260324T061500Z}"
RUN_ROOT="$PARQUET_ROOT/runs/$RUN_ID"
START_DATE="${START_DATE:-2025-02-01}"
END_DATE="${END_DATE:-2025-02-28}"
PARQUET_WORKERS="${PARQUET_WORKERS:-$(detect_default_parquet_workers)}"
THETADATA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"
ENSURE_LOG_DIR="$RUN_ROOT/automation"
ENSURE_LOG="$ENSURE_LOG_DIR/ensure-parquet-run.log"
LAUNCH_LOG="$ENSURE_LOG_DIR/ensure-parquet-run-launch.log"

PREFERRED_LOG_ROOT="${HOME}/Library/Logs/phenixflow"
if mkdir -p "$PREFERRED_LOG_ROOT" 2>/dev/null; then
  ENSURE_LOG_DIR="$PREFERRED_LOG_ROOT"
  ENSURE_LOG="$ENSURE_LOG_DIR/ensure-parquet-run.log"
  LAUNCH_LOG="$ENSURE_LOG_DIR/ensure-parquet-run-launch.log"
else
  mkdir -p "$ENSURE_LOG_DIR"
fi

timestamp() {
  date '+%Y-%m-%d %H:%M:%S %Z'
}

STATUS_JSON="$(node "$MONITOR_SCRIPT" "$RUN_ROOT" 2>/dev/null || true)"
STATE="unknown"
TOTAL_JOBS=0
COMPLETED_JOBS=0
FAILED_JOBS=0
if [ -n "$STATUS_JSON" ]; then
  STATE="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const data=JSON.parse(fs.readFileSync(0,"utf8")); process.stdout.write(String(data.state || "unknown"));')"
  TOTAL_JOBS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const data=JSON.parse(fs.readFileSync(0,"utf8")); process.stdout.write(String(Number(data.totalJobs || 0)));')"
  COMPLETED_JOBS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const data=JSON.parse(fs.readFileSync(0,"utf8")); process.stdout.write(String(Number(data.completedJobs || 0)));')"
  FAILED_JOBS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const data=JSON.parse(fs.readFileSync(0,"utf8")); process.stdout.write(String(Number(data.failedJobs || 0)));')"
fi

echo "[$(timestamp)] state=$STATE completed=$COMPLETED_JOBS failed=$FAILED_JOBS total=$TOTAL_JOBS" >>"$ENSURE_LOG"

if [ "$STATE" = "running" ]; then
  exit 0
fi

if [ "$TOTAL_JOBS" -gt 0 ] && [ "$COMPLETED_JOBS" -ge "$TOTAL_JOBS" ]; then
  echo "[$(timestamp)] run complete; nothing to relaunch" >>"$ENSURE_LOG"
  exit 0
fi

echo "[$(timestamp)] relaunching $RUN_ID" >>"$ENSURE_LOG"
nohup env \
  START_DATE="$START_DATE" \
  END_DATE="$END_DATE" \
  PARQUET_RUN_ID="$RUN_ID" \
  PARQUET_WORKERS="$PARQUET_WORKERS" \
  THETADATA_BASE_URL="$THETADATA_BASE_URL" \
  PARQUET_RESUME_EXISTING=1 \
  bash "$RUN_SCRIPT" >>"$LAUNCH_LOG" 2>&1 < /dev/null &
echo "[$(timestamp)] launched pid=$!" >>"$ENSURE_LOG"
