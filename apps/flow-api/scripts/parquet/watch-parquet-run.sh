#!/usr/bin/env bash
set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
MONITOR_SCRIPT="$ROOT_DIR/apps/flow-api/scripts/parquet/monitor-parquet-run.js"
ENSURE_SCRIPT="$ROOT_DIR/apps/flow-api/scripts/parquet/ensure-parquet-run.sh"
STOP_SCRIPT="$ROOT_DIR/apps/flow-api/scripts/parquet/request-parquet-run-stop.sh"

DEFAULT_LOCAL_PARQUET_ROOT="${HOME}/Library/Caches/phenixflow/parquet"
DEFAULT_EXTERNAL_PARQUET_ROOT="/Volumes/Phenix4TB/phenixflow/parquet"

if [ $# -lt 1 ]; then
  echo "usage: $0 <run-id> [interval-seconds]" >&2
  exit 1
fi

RUN_ID="$1"
INTERVAL_SECONDS="${2:-${WATCHDOG_INTERVAL_SECONDS:-600}}"

if [ -n "${PHENIXFLOW_PARQUET_ROOT:-}" ]; then
  PARQUET_ROOT="$PHENIXFLOW_PARQUET_ROOT"
elif [ -d "/Volumes/Phenix4TB" ]; then
  PARQUET_ROOT="$DEFAULT_EXTERNAL_PARQUET_ROOT"
else
  PARQUET_ROOT="$DEFAULT_LOCAL_PARQUET_ROOT"
fi

RUN_ROOT="$PARQUET_ROOT/runs/$RUN_ID"
REPORT_PATH="$RUN_ROOT/reports/watchdog.ndjson"
LOG_PATH="$RUN_ROOT/logs/watchdog.log"
LOCK_DIR="$RUN_ROOT/state/control/watchdog.lock"
STOP_PATH="$RUN_ROOT/state/control/stop-requested.json"

mkdir -p "$(dirname "$REPORT_PATH")" "$(dirname "$LOG_PATH")" "$(dirname "$LOCK_DIR")"

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "watchdog already running for $RUN_ID" >&2
  exit 0
fi

cleanup() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

timestamp() {
  date '+%Y-%m-%d %H:%M:%S %Z'
}

read_stop_reason() {
  if [ ! -f "$STOP_PATH" ]; then
    printf '\n'
    return 0
  fi
  node -e "const fs=require('node:fs'); try { const data=JSON.parse(fs.readFileSync(process.argv[1],'utf8')); process.stdout.write(String(data.reason || '')); } catch { process.stdout.write(''); }" "$STOP_PATH"
}

while true; do
  STATUS_JSON="$(node "$MONITOR_SCRIPT" "$RUN_ROOT" --sample-bandwidth 2>/dev/null || true)"
  if [ -n "$STATUS_JSON" ]; then
    printf '%s\n' "$STATUS_JSON" >>"$REPORT_PATH"
  fi

  STATE="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); process.stdout.write(String(data.state || ""));' 2>/dev/null || true)"
  STOP_REQUESTED="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); process.stdout.write(String(Boolean(data.stopRequested)));' 2>/dev/null || true)"
  TOTAL_JOBS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); process.stdout.write(String(Number(data.totalJobs || 0)));' 2>/dev/null || true)"
  COMPLETE_JOBS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); process.stdout.write(String(Number(data.completeJobs || 0)));' 2>/dev/null || true)"
  THETA_ACTIVE="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); process.stdout.write(String(Number(data.thetaActiveRequests || 0)));' 2>/dev/null || true)"
  THETA_TARGET="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); process.stdout.write(String(Number(data.thetaActiveTarget || 0)));' 2>/dev/null || true)"
  DEGRADED_REASON="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); const value=data.thetaDegradedReason || data.thetaPotentialDegradedReason || ""; process.stdout.write(String(value));' 2>/dev/null || true)"
  BANDWIDTH_MBPS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const raw=fs.readFileSync(0,"utf8").trim(); if(!raw){process.exit(0);} const data=JSON.parse(raw); process.stdout.write(String(Number(data.loopbackNodeInAvgMBps || 0)));' 2>/dev/null || true)"

  echo "[$(timestamp)] state=${STATE:-unknown} complete=${COMPLETE_JOBS:-0}/${TOTAL_JOBS:-0} theta=${THETA_ACTIVE:-0}/${THETA_TARGET:-0} mbps=${BANDWIDTH_MBPS:-0} degraded=${DEGRADED_REASON:-none}" >>"$LOG_PATH"

  STOP_REASON="$(read_stop_reason)"

  if [ "${STATE:-}" = "running" ]; then
    if [ -n "${DEGRADED_REASON:-}" ] && [ "${DEGRADED_REASON:-}" != "null" ]; then
      echo "[$(timestamp)] watchdog restart requested: $DEGRADED_REASON" >>"$LOG_PATH"
      bash "$STOP_SCRIPT" "$RUN_ID" "watchdog_${DEGRADED_REASON}" >>"$LOG_PATH" 2>&1 || true
    fi
  else
    if [ "${TOTAL_JOBS:-0}" -gt 0 ] && [ "${COMPLETE_JOBS:-0}" -lt "${TOTAL_JOBS:-0}" ]; then
      if [ "${STOP_REQUESTED:-false}" = "true" ] && [[ "${STOP_REASON:-}" == watchdog_* ]]; then
        rm -f "$STOP_PATH"
        echo "[$(timestamp)] cleared watchdog stop marker: ${STOP_REASON}" >>"$LOG_PATH"
      fi
      if [ ! -f "$STOP_PATH" ]; then
        echo "[$(timestamp)] invoking ensure-parquet-run for $RUN_ID" >>"$LOG_PATH"
        bash "$ENSURE_SCRIPT" >>"$LOG_PATH" 2>&1 || true
      fi
    fi
  fi

  sleep "$INTERVAL_SECONDS"
done
