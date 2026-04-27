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
  workers=$(( cpu_count + 8 ))
  if (( workers > 18 )); then
    workers=18
  fi
  if (( workers < 4 )); then
    workers=4
  fi
  printf '%s\n' "$workers"
}

derive_download_workers() {
  local total theta_active theta_queued download compute_floor desired_extra
  total="$1"
  theta_active="$2"
  theta_queued="$3"
  compute_floor=2
  if (( total <= 3 )); then
    compute_floor=1
  fi
  desired_extra="$theta_queued"
  if (( desired_extra > theta_active )); then
    desired_extra="$theta_active"
  fi
  download=$(( theta_active + desired_extra ))
  if (( download > total - compute_floor )); then
    download=$(( total > compute_floor ? total - compute_floor : 1 ))
  fi
  if (( download < theta_active )); then
    download="$theta_active"
  fi
  if (( download < 1 )); then
    download=1
  fi
  printf '%s\n' "$download"
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
PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS="${PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS:-8}"
PARQUET_THETA_ACTIVE_TARGET="${PARQUET_THETA_ACTIVE_TARGET:-$PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS}"
PARQUET_THETA_QUEUED_TARGET="${PARQUET_THETA_QUEUED_TARGET:-16}"
PARQUET_THETA_PER_JOB_LIMIT="${PARQUET_THETA_PER_JOB_LIMIT:-1}"
PARQUET_THETA_PER_JOB_BURST_LIMIT="${PARQUET_THETA_PER_JOB_BURST_LIMIT:-2}"
PARQUET_HEAVY_DOWNLOAD_WORKERS="${PARQUET_HEAVY_DOWNLOAD_WORKERS:-0}"
PARQUET_HEAVY_DOWNLOAD_SYMBOLS="${PARQUET_HEAVY_DOWNLOAD_SYMBOLS:-}"
PARQUET_DOWNLOAD_WORKERS="${PARQUET_DOWNLOAD_WORKERS:-$(derive_download_workers "$PARQUET_WORKERS" "$PARQUET_THETA_ACTIVE_TARGET" "$PARQUET_THETA_QUEUED_TARGET")}"
PARQUET_COMPUTE_WORKERS="${PARQUET_COMPUTE_WORKERS:-$(( PARQUET_WORKERS - PARQUET_DOWNLOAD_WORKERS ))}"
if (( PARQUET_COMPUTE_WORKERS < 1 )); then
  PARQUET_COMPUTE_WORKERS=1
fi
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
  COMPLETED_JOBS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const data=JSON.parse(fs.readFileSync(0,"utf8")); process.stdout.write(String(Number(data.completeJobs ?? data.completedJobs ?? 0)));')"
  FAILED_JOBS="$(printf '%s' "$STATUS_JSON" | node -e 'const fs=require("node:fs"); const data=JSON.parse(fs.readFileSync(0,"utf8")); process.stdout.write(String(Number(data.failedJobs || 0)));')"
fi

echo "[$(timestamp)] state=$STATE completed=$COMPLETED_JOBS failed=$FAILED_JOBS total=$TOTAL_JOBS" >>"$ENSURE_LOG"

if [ "$STATE" = "running" ]; then
  exit 0
fi

if [ -f "$RUN_ROOT/state/control/stop-requested.json" ]; then
  echo "[$(timestamp)] stop marker present; not relaunching" >>"$ENSURE_LOG"
  exit 0
fi

if [ "$TOTAL_JOBS" -gt 0 ] && [ "$COMPLETED_JOBS" -ge "$TOTAL_JOBS" ]; then
  echo "[$(timestamp)] run complete; nothing to relaunch" >>"$ENSURE_LOG"
  exit 0
fi

echo "[$(timestamp)] relaunching $RUN_ID" >>"$ENSURE_LOG"
LAUNCH_PID="$(
  python3 - "$RUN_SCRIPT" "$LAUNCH_LOG" "$START_DATE" "$END_DATE" "$RUN_ID" "$PARQUET_WORKERS" "$PARQUET_DOWNLOAD_WORKERS" "$PARQUET_COMPUTE_WORKERS" "$PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS" "$PARQUET_THETA_ACTIVE_TARGET" "$PARQUET_THETA_QUEUED_TARGET" "$PARQUET_THETA_PER_JOB_LIMIT" "$PARQUET_THETA_PER_JOB_BURST_LIMIT" "$THETADATA_BASE_URL" <<'PY'
import os
import subprocess
import sys

(
    run_script,
    launch_log,
    start_date,
    end_date,
    run_id,
    parquet_workers,
    parquet_download_workers,
    parquet_compute_workers,
    theta_max_connections,
    theta_active_target,
    theta_queued_target,
    theta_per_job_limit,
    theta_per_job_burst_limit,
    theta_base_url,
) = sys.argv[1:]

env = os.environ.copy()
env.update({
    "START_DATE": start_date,
    "END_DATE": end_date,
    "PARQUET_RUN_ID": run_id,
    "PARQUET_WORKERS": parquet_workers,
    "PARQUET_DOWNLOAD_WORKERS": parquet_download_workers,
    "PARQUET_COMPUTE_WORKERS": parquet_compute_workers,
    "PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS": theta_max_connections,
    "PARQUET_THETA_ACTIVE_TARGET": theta_active_target,
    "PARQUET_THETA_QUEUED_TARGET": theta_queued_target,
    "PARQUET_THETA_PER_JOB_LIMIT": theta_per_job_limit,
    "PARQUET_THETA_PER_JOB_BURST_LIMIT": theta_per_job_burst_limit,
    "PARQUET_HEAVY_DOWNLOAD_WORKERS": os.environ.get("PARQUET_HEAVY_DOWNLOAD_WORKERS", "0"),
    "PARQUET_HEAVY_DOWNLOAD_SYMBOLS": os.environ.get("PARQUET_HEAVY_DOWNLOAD_SYMBOLS", ""),
    "THETADATA_BASE_URL": theta_base_url,
    "PARQUET_RESUME_EXISTING": "1",
})

with open(launch_log, "ab", buffering=0) as handle:
    process = subprocess.Popen(
        ["bash", run_script],
        stdin=subprocess.DEVNULL,
        stdout=handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        close_fds=True,
        env=env,
    )
    print(process.pid)
PY
)"
echo "[$(timestamp)] launched pid=$LAUNCH_PID" >>"$ENSURE_LOG"
