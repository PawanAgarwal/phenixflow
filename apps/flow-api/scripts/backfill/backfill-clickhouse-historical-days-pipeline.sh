#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CPU_CORES="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
ENRICH_CPU_TARGET_PCT="${ENRICH_CPU_TARGET_PCT:-60}"
MAX_WORKERS="${BACKFILL_MAX_WORKERS:-16}"
REPORT_ROOT="${BACKFILL_REPORT_DIR:-$PROJECT_ROOT/artifacts/reports}"
INPUT_PATH="${BACKFILL_SYMBOL_DAY_LIST_PATH:-}"
PIPELINE_LOOP_SLEEP_MS="${PIPELINE_LOOP_SLEEP_MS:-5000}"
PIPELINE_LOOP_MAX_PASSES="${PIPELINE_LOOP_MAX_PASSES:-400}"
DOWNLOAD_SUPPLEMENTAL_CONCURRENCY="${DOWNLOAD_SUPPLEMENTAL_CONCURRENCY:-2}"
ENRICH_SUPPLEMENTAL_CONCURRENCY="${ENRICH_SUPPLEMENTAL_CONCURRENCY:-2}"
PIPELINE_STAGE_OVERLAP_RAW="${PIPELINE_STAGE_OVERLAP:-1}"
BACKFILL_MEMORY_RESERVE_MB="${BACKFILL_MEMORY_RESERVE_MB:-4096}"
BACKFILL_MEMORY_PER_WORKER_MB="${BACKFILL_MEMORY_PER_WORKER_MB:-2200}"
BACKFILL_NODE_MAX_OLD_SPACE_MB="${BACKFILL_NODE_MAX_OLD_SPACE_MB:-1536}"
DOWNLOAD_NODE_MAX_OLD_SPACE_MB="${DOWNLOAD_NODE_MAX_OLD_SPACE_MB:-$BACKFILL_NODE_MAX_OLD_SPACE_MB}"
ENRICH_NODE_MAX_OLD_SPACE_MB="${ENRICH_NODE_MAX_OLD_SPACE_MB:-$BACKFILL_NODE_MAX_OLD_SPACE_MB}"
BACKFILL_RAM_BUDGET_MB="${BACKFILL_RAM_BUDGET_MB:-8192}"
BACKFILL_WORKER_OVERHEAD_MB="${BACKFILL_WORKER_OVERHEAD_MB:-512}"
BACKFILL_REPORT_INCLUDE_JOBS="${BACKFILL_REPORT_INCLUDE_JOBS:-0}"
THETA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"
FLOW_READ_BACKEND="${PHENIX_FLOW_READ_BACKEND:-clickhouse}"
FLOW_WRITE_BACKEND="${PHENIX_FLOW_WRITE_BACKEND:-clickhouse}"
THETADATA_MAX_CONCURRENT_CONNECTIONS="${THETADATA_MAX_CONCURRENT_CONNECTIONS:-4}"
BACKFILL_DOWNLOAD_WORKER_GUARD="${BACKFILL_DOWNLOAD_WORKER_GUARD:-1}"
BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS="${BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS:-200}"
BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET="${BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET:-$THETADATA_MAX_CONCURRENT_CONNECTIONS}"
CLICKHOUSE_CONNECT_TIMEOUT_SEC="${CLICKHOUSE_CONNECT_TIMEOUT_SEC:-10}"
CLICKHOUSE_SEND_TIMEOUT_SEC="${CLICKHOUSE_SEND_TIMEOUT_SEC:-600}"
CLICKHOUSE_RECEIVE_TIMEOUT_SEC="${CLICKHOUSE_RECEIVE_TIMEOUT_SEC:-600}"
CLICKHOUSE_DELETE_MUTATION_SYNC="${CLICKHOUSE_DELETE_MUTATION_SYNC:-0}"
CLICKHOUSE_ENRICH_GREEKS_SOURCE="${CLICKHOUSE_ENRICH_GREEKS_SOURCE:-calculated_first}"
TS="$(date +%Y%m%dT%H%M%S)"
RUN_DIR="$REPORT_ROOT/clickhouse-historical-pipeline-$TS"
DOWNLOAD_DIR="$RUN_DIR/download"
ENRICH_DIR="$RUN_DIR/enrich"
DOWNLOAD_DONE_FLAG="$RUN_DIR/download.done"

detect_total_memory_mb() {
  local bytes=""
  local kb=""

  bytes="$(sysctl -n hw.memsize 2>/dev/null || true)"
  if [[ "$bytes" =~ ^[0-9]+$ ]] && (( bytes > 0 )); then
    echo $((bytes / 1024 / 1024))
    return
  fi

  if [[ -r /proc/meminfo ]]; then
    kb="$(awk '/MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || true)"
    if [[ "$kb" =~ ^[0-9]+$ ]] && (( kb > 0 )); then
      echo $((kb / 1024))
      return
    fi
  fi

  echo ""
}

build_node_options() {
  local max_old_space_mb="$1"
  local base="--max-old-space-size=${max_old_space_mb}"
  if [[ -n "${NODE_OPTIONS:-}" ]]; then
    echo "${base} ${NODE_OPTIONS}"
  else
    echo "${base}"
  fi
}

estimate_parallel_memory_mb() {
  local download_workers="$1"
  local enrich_workers="$2"
  echo $(( download_workers * DOWNLOAD_WORKER_FOOTPRINT_MB + enrich_workers * ENRICH_WORKER_FOOTPRINT_MB ))
}

if ! [[ "$ENRICH_CPU_TARGET_PCT" =~ ^[0-9]+$ ]] || (( ENRICH_CPU_TARGET_PCT < 10 )) || (( ENRICH_CPU_TARGET_PCT > 95 )); then
  echo "ENRICH_CPU_TARGET_PCT must be an integer in [10,95] (got: $ENRICH_CPU_TARGET_PCT)"
  exit 1
fi

if ! [[ "$MAX_WORKERS" =~ ^[0-9]+$ ]] || (( MAX_WORKERS < 1 )); then
  echo "BACKFILL_MAX_WORKERS must be a positive integer (got: $MAX_WORKERS)"
  exit 1
fi

if ! [[ "$THETADATA_MAX_CONCURRENT_CONNECTIONS" =~ ^[0-9]+$ ]] || (( THETADATA_MAX_CONCURRENT_CONNECTIONS < 1 )); then
  echo "THETADATA_MAX_CONCURRENT_CONNECTIONS must be a positive integer (got: $THETADATA_MAX_CONCURRENT_CONNECTIONS)"
  exit 1
fi

if ! [[ "$BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS" =~ ^[0-9]+$ ]] || (( BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS < 1 )); then
  echo "BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS must be a positive integer (got: $BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS)"
  exit 1
fi

if ! [[ "$BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET" =~ ^[0-9]+$ ]] || (( BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET < 1 )); then
  echo "BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET must be a positive integer (got: $BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET)"
  exit 1
fi

if (( BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET > THETADATA_MAX_CONCURRENT_CONNECTIONS )); then
  BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET="$THETADATA_MAX_CONCURRENT_CONNECTIONS"
fi

if ! [[ "$BACKFILL_MEMORY_RESERVE_MB" =~ ^[0-9]+$ ]]; then
  echo "BACKFILL_MEMORY_RESERVE_MB must be a non-negative integer (got: $BACKFILL_MEMORY_RESERVE_MB)"
  exit 1
fi

if ! [[ "$BACKFILL_MEMORY_PER_WORKER_MB" =~ ^[0-9]+$ ]] || (( BACKFILL_MEMORY_PER_WORKER_MB < 256 )); then
  echo "BACKFILL_MEMORY_PER_WORKER_MB must be an integer >= 256 (got: $BACKFILL_MEMORY_PER_WORKER_MB)"
  exit 1
fi

if ! [[ "$DOWNLOAD_NODE_MAX_OLD_SPACE_MB" =~ ^[0-9]+$ ]] || (( DOWNLOAD_NODE_MAX_OLD_SPACE_MB < 256 )); then
  echo "DOWNLOAD_NODE_MAX_OLD_SPACE_MB must be an integer >= 256 (got: $DOWNLOAD_NODE_MAX_OLD_SPACE_MB)"
  exit 1
fi

if ! [[ "$ENRICH_NODE_MAX_OLD_SPACE_MB" =~ ^[0-9]+$ ]] || (( ENRICH_NODE_MAX_OLD_SPACE_MB < 256 )); then
  echo "ENRICH_NODE_MAX_OLD_SPACE_MB must be an integer >= 256 (got: $ENRICH_NODE_MAX_OLD_SPACE_MB)"
  exit 1
fi

if ! [[ "$BACKFILL_RAM_BUDGET_MB" =~ ^[0-9]+$ ]] || (( BACKFILL_RAM_BUDGET_MB < 1024 )); then
  echo "BACKFILL_RAM_BUDGET_MB must be an integer >= 1024 (got: $BACKFILL_RAM_BUDGET_MB)"
  exit 1
fi

if ! [[ "$BACKFILL_WORKER_OVERHEAD_MB" =~ ^[0-9]+$ ]] || (( BACKFILL_WORKER_OVERHEAD_MB < 64 )); then
  echo "BACKFILL_WORKER_OVERHEAD_MB must be an integer >= 64 (got: $BACKFILL_WORKER_OVERHEAD_MB)"
  exit 1
fi

PIPELINE_STAGE_OVERLAP=0
if [[ "$PIPELINE_STAGE_OVERLAP_RAW" == "1" ]] || [[ "$PIPELINE_STAGE_OVERLAP_RAW" == "true" ]]; then
  PIPELINE_STAGE_OVERLAP=1
fi

DEFAULT_ENRICH_WORKERS=$(( (CPU_CORES * ENRICH_CPU_TARGET_PCT + 99) / 100 ))
if (( DEFAULT_ENRICH_WORKERS < 1 )); then
  DEFAULT_ENRICH_WORKERS=1
fi

DEFAULT_DOWNLOAD_WORKERS="$THETADATA_MAX_CONCURRENT_CONNECTIONS"

ENRICH_WORKERS="${ENRICH_WORKERS:-$DEFAULT_ENRICH_WORKERS}"
DOWNLOAD_WORKERS="${DOWNLOAD_WORKERS:-$DEFAULT_DOWNLOAD_WORKERS}"

if ! [[ "$ENRICH_WORKERS" =~ ^[0-9]+$ ]] || (( ENRICH_WORKERS < 1 )); then
  echo "ENRICH_WORKERS must be a positive integer (got: $ENRICH_WORKERS)"
  exit 1
fi

if ! [[ "$DOWNLOAD_WORKERS" =~ ^[0-9]+$ ]] || (( DOWNLOAD_WORKERS < 1 )); then
  echo "DOWNLOAD_WORKERS must be a positive integer (got: $DOWNLOAD_WORKERS)"
  exit 1
fi

if ! [[ "$DOWNLOAD_SUPPLEMENTAL_CONCURRENCY" =~ ^[0-9]+$ ]] || (( DOWNLOAD_SUPPLEMENTAL_CONCURRENCY < 1 )); then
  echo "DOWNLOAD_SUPPLEMENTAL_CONCURRENCY must be a positive integer (got: $DOWNLOAD_SUPPLEMENTAL_CONCURRENCY)"
  exit 1
fi

if ! [[ "$ENRICH_SUPPLEMENTAL_CONCURRENCY" =~ ^[0-9]+$ ]] || (( ENRICH_SUPPLEMENTAL_CONCURRENCY < 1 )); then
  echo "ENRICH_SUPPLEMENTAL_CONCURRENCY must be a positive integer (got: $ENRICH_SUPPLEMENTAL_CONCURRENCY)"
  exit 1
fi

if (( ENRICH_WORKERS > MAX_WORKERS )); then
  echo "Capping ENRICH_WORKERS from $ENRICH_WORKERS to $MAX_WORKERS"
  ENRICH_WORKERS="$MAX_WORKERS"
fi

if (( DOWNLOAD_WORKERS > MAX_WORKERS )); then
  echo "Capping DOWNLOAD_WORKERS from $DOWNLOAD_WORKERS to $MAX_WORKERS"
  DOWNLOAD_WORKERS="$MAX_WORKERS"
fi

if (( DOWNLOAD_WORKERS > THETADATA_MAX_CONCURRENT_CONNECTIONS )); then
  echo "Capping DOWNLOAD_WORKERS from $DOWNLOAD_WORKERS to THETADATA_MAX_CONCURRENT_CONNECTIONS=$THETADATA_MAX_CONCURRENT_CONNECTIONS"
  DOWNLOAD_WORKERS="$THETADATA_MAX_CONCURRENT_CONNECTIONS"
fi

if (( DOWNLOAD_SUPPLEMENTAL_CONCURRENCY > THETADATA_MAX_CONCURRENT_CONNECTIONS )); then
  echo "Capping DOWNLOAD_SUPPLEMENTAL_CONCURRENCY from $DOWNLOAD_SUPPLEMENTAL_CONCURRENCY to THETADATA_MAX_CONCURRENT_CONNECTIONS=$THETADATA_MAX_CONCURRENT_CONNECTIONS"
  DOWNLOAD_SUPPLEMENTAL_CONCURRENCY="$THETADATA_MAX_CONCURRENT_CONNECTIONS"
fi

TOTAL_MEMORY_MB="$(detect_total_memory_mb)"
MEMORY_WORKER_CAP="$MAX_WORKERS"
if [[ "$TOTAL_MEMORY_MB" =~ ^[0-9]+$ ]] && (( TOTAL_MEMORY_MB > 0 )); then
  if (( TOTAL_MEMORY_MB > BACKFILL_MEMORY_RESERVE_MB )); then
    MEMORY_WORKER_CAP=$(( (TOTAL_MEMORY_MB - BACKFILL_MEMORY_RESERVE_MB) / BACKFILL_MEMORY_PER_WORKER_MB ))
    if (( MEMORY_WORKER_CAP < 1 )); then
      MEMORY_WORKER_CAP=1
    fi
  else
    MEMORY_WORKER_CAP=1
  fi
fi

if (( MEMORY_WORKER_CAP < MAX_WORKERS )); then
  echo "Memory worker cap: $MEMORY_WORKER_CAP (reserve ${BACKFILL_MEMORY_RESERVE_MB}MB, per-worker ${BACKFILL_MEMORY_PER_WORKER_MB}MB)"
fi

DOWNLOAD_WORKER_FOOTPRINT_MB=$(( DOWNLOAD_NODE_MAX_OLD_SPACE_MB + BACKFILL_WORKER_OVERHEAD_MB ))
ENRICH_WORKER_FOOTPRINT_MB=$(( ENRICH_NODE_MAX_OLD_SPACE_MB + BACKFILL_WORKER_OVERHEAD_MB ))
DOWNLOAD_RAM_BUDGET_WORKER_CAP=$(( BACKFILL_RAM_BUDGET_MB / DOWNLOAD_WORKER_FOOTPRINT_MB ))
ENRICH_RAM_BUDGET_WORKER_CAP=$(( BACKFILL_RAM_BUDGET_MB / ENRICH_WORKER_FOOTPRINT_MB ))
if (( DOWNLOAD_RAM_BUDGET_WORKER_CAP < 1 )); then
  DOWNLOAD_RAM_BUDGET_WORKER_CAP=1
fi
if (( ENRICH_RAM_BUDGET_WORKER_CAP < 1 )); then
  ENRICH_RAM_BUDGET_WORKER_CAP=1
fi

ENRICH_STAGE_WORKER_CAP="$MEMORY_WORKER_CAP"
if (( ENRICH_STAGE_WORKER_CAP > ENRICH_RAM_BUDGET_WORKER_CAP )); then
  ENRICH_STAGE_WORKER_CAP="$ENRICH_RAM_BUDGET_WORKER_CAP"
fi

DOWNLOAD_STAGE_WORKER_CAP="$MEMORY_WORKER_CAP"
if (( DOWNLOAD_STAGE_WORKER_CAP > DOWNLOAD_RAM_BUDGET_WORKER_CAP )); then
  DOWNLOAD_STAGE_WORKER_CAP="$DOWNLOAD_RAM_BUDGET_WORKER_CAP"
fi

if (( ENRICH_WORKERS > ENRICH_STAGE_WORKER_CAP )); then
  echo "Capping ENRICH_WORKERS from $ENRICH_WORKERS to stage memory cap $ENRICH_STAGE_WORKER_CAP"
  ENRICH_WORKERS="$ENRICH_STAGE_WORKER_CAP"
fi

if (( DOWNLOAD_WORKERS > DOWNLOAD_STAGE_WORKER_CAP )); then
  echo "Capping DOWNLOAD_WORKERS from $DOWNLOAD_WORKERS to stage memory cap $DOWNLOAD_STAGE_WORKER_CAP"
  DOWNLOAD_WORKERS="$DOWNLOAD_STAGE_WORKER_CAP"
fi

if (( PIPELINE_STAGE_OVERLAP == 1 )); then
  ORIG_DOWNLOAD_WORKERS="$DOWNLOAD_WORKERS"
  ORIG_ENRICH_WORKERS="$ENRICH_WORKERS"
  OVERLAP_ESTIMATED_MEMORY_MB="$(estimate_parallel_memory_mb "$DOWNLOAD_WORKERS" "$ENRICH_WORKERS")"
  if (( OVERLAP_ESTIMATED_MEMORY_MB > BACKFILL_RAM_BUDGET_MB )); then
    while (( OVERLAP_ESTIMATED_MEMORY_MB > BACKFILL_RAM_BUDGET_MB )) && (( DOWNLOAD_WORKERS > 1 || ENRICH_WORKERS > 1 )); do
      if (( DOWNLOAD_WORKERS * DOWNLOAD_WORKER_FOOTPRINT_MB >= ENRICH_WORKERS * ENRICH_WORKER_FOOTPRINT_MB )); then
        if (( DOWNLOAD_WORKERS > 1 )); then
          DOWNLOAD_WORKERS=$(( DOWNLOAD_WORKERS - 1 ))
        else
          ENRICH_WORKERS=$(( ENRICH_WORKERS - 1 ))
        fi
      else
        if (( ENRICH_WORKERS > 1 )); then
          ENRICH_WORKERS=$(( ENRICH_WORKERS - 1 ))
        else
          DOWNLOAD_WORKERS=$(( DOWNLOAD_WORKERS - 1 ))
        fi
      fi
      OVERLAP_ESTIMATED_MEMORY_MB="$(estimate_parallel_memory_mb "$DOWNLOAD_WORKERS" "$ENRICH_WORKERS")"
    done

    if (( OVERLAP_ESTIMATED_MEMORY_MB > BACKFILL_RAM_BUDGET_MB )); then
      echo "Disabling PIPELINE_STAGE_OVERLAP because RAM budget ${BACKFILL_RAM_BUDGET_MB}MB cannot fit download+enrich workers together."
      PIPELINE_STAGE_OVERLAP=0
    elif (( DOWNLOAD_WORKERS != ORIG_DOWNLOAD_WORKERS || ENRICH_WORKERS != ORIG_ENRICH_WORKERS )); then
      echo "Capping overlapping workers to RAM budget: download ${ORIG_DOWNLOAD_WORKERS}->${DOWNLOAD_WORKERS}, enrich ${ORIG_ENRICH_WORKERS}->${ENRICH_WORKERS}"
    fi
  fi
fi

DOWNLOAD_STAGE_ESTIMATED_MEMORY_MB=$(( DOWNLOAD_WORKERS * DOWNLOAD_WORKER_FOOTPRINT_MB ))
ENRICH_STAGE_ESTIMATED_MEMORY_MB=$(( ENRICH_WORKERS * ENRICH_WORKER_FOOTPRINT_MB ))
OVERLAP_ESTIMATED_MEMORY_MB="$(estimate_parallel_memory_mb "$DOWNLOAD_WORKERS" "$ENRICH_WORKERS")"

INPUT_JOB_COUNT=0
if [[ -n "$INPUT_PATH" ]]; then
  if [[ ! -f "$INPUT_PATH" ]]; then
    echo "Input file not found: $INPUT_PATH"
    exit 1
  fi
  INPUT_JOB_COUNT="$(awk 'NF > 0 {count += 1} END {print count + 0}' "$INPUT_PATH")"
fi

DOWNLOAD_WORKER_GUARD_ENABLED=0
if [[ "$BACKFILL_DOWNLOAD_WORKER_GUARD" == "1" ]] || [[ "$BACKFILL_DOWNLOAD_WORKER_GUARD" == "true" ]] || [[ "$BACKFILL_DOWNLOAD_WORKER_GUARD" == "yes" ]]; then
  DOWNLOAD_WORKER_GUARD_ENABLED=1
fi

if (( DOWNLOAD_WORKER_GUARD_ENABLED == 1 )) \
  && (( INPUT_JOB_COUNT >= BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS )) \
  && (( DOWNLOAD_WORKERS < BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET )); then
  echo "Download worker guard failed: $INPUT_JOB_COUNT jobs >= guard threshold ${BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS}, but effective DOWNLOAD_WORKERS=$DOWNLOAD_WORKERS < required target ${BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET}."
  echo "Adjust workers (for example, lower ENRICH_WORKERS or disable overlap) and relaunch."
  exit 1
fi

DOWNLOAD_NODE_OPTIONS="$(build_node_options "$DOWNLOAD_NODE_MAX_OLD_SPACE_MB")"
ENRICH_NODE_OPTIONS="$(build_node_options "$ENRICH_NODE_MAX_OLD_SPACE_MB")"

mkdir -p "$DOWNLOAD_DIR" "$ENRICH_DIR"
rm -f "$DOWNLOAD_DONE_FLAG"

echo "Starting ClickHouse pipeline backfill..."
echo "CPU cores: $CPU_CORES"
echo "Enrichment target CPU: ${ENRICH_CPU_TARGET_PCT}%"
echo "Download workers: $DOWNLOAD_WORKERS (supplemental concurrency: $DOWNLOAD_SUPPLEMENTAL_CONCURRENCY)"
echo "Enrich workers: $ENRICH_WORKERS (supplemental concurrency: $ENRICH_SUPPLEMENTAL_CONCURRENCY)"
echo "Theta max concurrent connections: $THETADATA_MAX_CONCURRENT_CONNECTIONS"
echo "ClickHouse timeouts (sec): connect=${CLICKHOUSE_CONNECT_TIMEOUT_SEC} send=${CLICKHOUSE_SEND_TIMEOUT_SEC} receive=${CLICKHOUSE_RECEIVE_TIMEOUT_SEC}"
echo "ClickHouse delete mutation sync: ${CLICKHOUSE_DELETE_MUTATION_SYNC}"
echo "Enrich greeks source: ${CLICKHOUSE_ENRICH_GREEKS_SOURCE}"
echo "Stage overlap: $([[ "$PIPELINE_STAGE_OVERLAP" == "1" ]] && echo "enabled" || echo "disabled")"
if [[ "$TOTAL_MEMORY_MB" =~ ^[0-9]+$ ]] && (( TOTAL_MEMORY_MB > 0 )); then
  echo "Total memory detected: ${TOTAL_MEMORY_MB}MB"
else
  echo "Total memory detected: unknown"
fi
echo "Memory reserve: ${BACKFILL_MEMORY_RESERVE_MB}MB"
echo "Memory per worker target: ${BACKFILL_MEMORY_PER_WORKER_MB}MB"
echo "Memory worker cap: $MEMORY_WORKER_CAP"
echo "RAM budget: ${BACKFILL_RAM_BUDGET_MB}MB"
echo "Worker overhead estimate: ${BACKFILL_WORKER_OVERHEAD_MB}MB"
echo "Per-worker footprint (download): ${DOWNLOAD_WORKER_FOOTPRINT_MB}MB"
echo "Per-worker footprint (enrich): ${ENRICH_WORKER_FOOTPRINT_MB}MB"
echo "RAM budget worker cap (download): $DOWNLOAD_RAM_BUDGET_WORKER_CAP"
echo "RAM budget worker cap (enrich): $ENRICH_RAM_BUDGET_WORKER_CAP"
echo "Stage memory cap (download): $DOWNLOAD_STAGE_WORKER_CAP"
echo "Stage memory cap (enrich): $ENRICH_STAGE_WORKER_CAP"
echo "Estimated memory at launch (download stage): ${DOWNLOAD_STAGE_ESTIMATED_MEMORY_MB}MB"
echo "Estimated memory at launch (enrich stage): ${ENRICH_STAGE_ESTIMATED_MEMORY_MB}MB"
echo "Estimated memory if overlap is enabled: ${OVERLAP_ESTIMATED_MEMORY_MB}MB"
echo "Input jobs: ${INPUT_JOB_COUNT}"
echo "Download worker guard: $([[ "$DOWNLOAD_WORKER_GUARD_ENABLED" == "1" ]] && echo "enabled" || echo "disabled") (min jobs: ${BACKFILL_DOWNLOAD_WORKER_GUARD_MIN_JOBS}, target workers: ${BACKFILL_DOWNLOAD_WORKER_GUARD_TARGET})"
echo "Node heap cap (download): ${DOWNLOAD_NODE_MAX_OLD_SPACE_MB}MB"
echo "Node heap cap (enrich): ${ENRICH_NODE_MAX_OLD_SPACE_MB}MB"
echo "Detailed job reports: $BACKFILL_REPORT_INCLUDE_JOBS"
echo "Theta base URL: $THETA_BASE_URL"
echo "Flow backends: read=$FLOW_READ_BACKEND write=$FLOW_WRITE_BACKEND"
echo "Reports: $RUN_DIR"
if [[ -n "$INPUT_PATH" ]]; then
  echo "Input: $INPUT_PATH"
fi

DEDUPED_INDICES=()
dedupe_indices() {
  DEDUPED_INDICES=()
  local seen=","
  local idx
  for idx in "$@"; do
    if [[ "$seen" == *",$idx,"* ]]; then
      continue
    fi
    seen="${seen}${idx},"
    DEDUPED_INDICES+=("$idx")
  done
}

worker_report_is_complete() {
  local report_path="$1"
  if [[ ! -s "$report_path" ]]; then
    return 1
  fi
  node - "$report_path" <<'NODE' >/dev/null 2>&1
const fs = require('node:fs');

const reportPath = process.argv[2];
const toNum = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
};

try {
  const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  const totalJobs = toNum(report.totalJobs);
  const completedJobs = toNum(report.completedJobs);
  const failedJobs = toNum(report.failedJobs);
  const loopExitReason = String(report.loopExitReason || '').toLowerCase();
  const completed = totalJobs === 0
    ? loopExitReason === 'completed' || loopExitReason === 'no_jobs'
    : completedJobs >= totalJobs;
  const ok = failedJobs === 0 && completed;
  process.exit(ok ? 0 : 1);
} catch {
  process.exit(2);
}
NODE
}

run_enrich_worker_once() {
  local idx="$1"
  local append_mode="${2:-0}"
  local report_path="$ENRICH_DIR/worker${idx}.json"
  local log_path="$ENRICH_DIR/worker${idx}.log"
  if (( append_mode == 1 )); then
    echo "--- REQUEUE worker ${idx} ---" >> "$log_path"
    (
      cd "$PROJECT_ROOT"
      NODE_OPTIONS="$ENRICH_NODE_OPTIONS" \
      BACKFILL_MODE="enrich" \
      BACKFILL_LOOP_UNTIL_READY="1" \
      BACKFILL_LOOP_SLEEP_MS="$PIPELINE_LOOP_SLEEP_MS" \
      BACKFILL_LOOP_MAX_PASSES="$PIPELINE_LOOP_MAX_PASSES" \
      BACKFILL_DOWNLOAD_DONE_FLAG="$DOWNLOAD_DONE_FLAG" \
      BACKFILL_WORKER_TOTAL="$ENRICH_WORKERS" \
      BACKFILL_WORKER_INDEX="$idx" \
      BACKFILL_REPORT_PATH="$report_path" \
      BACKFILL_SYMBOL_DAY_LIST_PATH="$INPUT_PATH" \
      BACKFILL_REPORT_INCLUDE_JOBS="$BACKFILL_REPORT_INCLUDE_JOBS" \
      PHENIX_FLOW_READ_BACKEND="$FLOW_READ_BACKEND" \
      PHENIX_FLOW_WRITE_BACKEND="$FLOW_WRITE_BACKEND" \
      THETADATA_BASE_URL="$THETA_BASE_URL" \
      THETADATA_SUPPLEMENTAL_CONCURRENCY="$ENRICH_SUPPLEMENTAL_CONCURRENCY" \
      CLICKHOUSE_CONNECT_TIMEOUT_SEC="$CLICKHOUSE_CONNECT_TIMEOUT_SEC" \
      CLICKHOUSE_SEND_TIMEOUT_SEC="$CLICKHOUSE_SEND_TIMEOUT_SEC" \
      CLICKHOUSE_RECEIVE_TIMEOUT_SEC="$CLICKHOUSE_RECEIVE_TIMEOUT_SEC" \
      CLICKHOUSE_DELETE_MUTATION_SYNC="$CLICKHOUSE_DELETE_MUTATION_SYNC" \
      CLICKHOUSE_ENRICH_GREEKS_SOURCE="$CLICKHOUSE_ENRICH_GREEKS_SOURCE" \
      node scripts/backfill/backfill-clickhouse-historical-days.js
    ) >> "$log_path" 2>&1
  else
    (
      cd "$PROJECT_ROOT"
      NODE_OPTIONS="$ENRICH_NODE_OPTIONS" \
      BACKFILL_MODE="enrich" \
      BACKFILL_LOOP_UNTIL_READY="1" \
      BACKFILL_LOOP_SLEEP_MS="$PIPELINE_LOOP_SLEEP_MS" \
      BACKFILL_LOOP_MAX_PASSES="$PIPELINE_LOOP_MAX_PASSES" \
      BACKFILL_DOWNLOAD_DONE_FLAG="$DOWNLOAD_DONE_FLAG" \
      BACKFILL_WORKER_TOTAL="$ENRICH_WORKERS" \
      BACKFILL_WORKER_INDEX="$idx" \
      BACKFILL_REPORT_PATH="$report_path" \
      BACKFILL_SYMBOL_DAY_LIST_PATH="$INPUT_PATH" \
      BACKFILL_REPORT_INCLUDE_JOBS="$BACKFILL_REPORT_INCLUDE_JOBS" \
      PHENIX_FLOW_READ_BACKEND="$FLOW_READ_BACKEND" \
      PHENIX_FLOW_WRITE_BACKEND="$FLOW_WRITE_BACKEND" \
      THETADATA_BASE_URL="$THETA_BASE_URL" \
      THETADATA_SUPPLEMENTAL_CONCURRENCY="$ENRICH_SUPPLEMENTAL_CONCURRENCY" \
      CLICKHOUSE_CONNECT_TIMEOUT_SEC="$CLICKHOUSE_CONNECT_TIMEOUT_SEC" \
      CLICKHOUSE_SEND_TIMEOUT_SEC="$CLICKHOUSE_SEND_TIMEOUT_SEC" \
      CLICKHOUSE_RECEIVE_TIMEOUT_SEC="$CLICKHOUSE_RECEIVE_TIMEOUT_SEC" \
      CLICKHOUSE_DELETE_MUTATION_SYNC="$CLICKHOUSE_DELETE_MUTATION_SYNC" \
      CLICKHOUSE_ENRICH_GREEKS_SOURCE="$CLICKHOUSE_ENRICH_GREEKS_SOURCE" \
      node scripts/backfill/backfill-clickhouse-historical-days.js
    ) > "$log_path" 2>&1
  fi
}

run_download_worker_once() {
  local idx="$1"
  local append_mode="${2:-0}"
  local report_path="$DOWNLOAD_DIR/worker${idx}.json"
  local log_path="$DOWNLOAD_DIR/worker${idx}.log"
  if (( append_mode == 1 )); then
    echo "--- REQUEUE worker ${idx} ---" >> "$log_path"
    (
      cd "$PROJECT_ROOT"
      NODE_OPTIONS="$DOWNLOAD_NODE_OPTIONS" \
      BACKFILL_MODE="download" \
      BACKFILL_WORKER_TOTAL="$DOWNLOAD_WORKERS" \
      BACKFILL_WORKER_INDEX="$idx" \
      BACKFILL_REPORT_PATH="$report_path" \
      BACKFILL_SYMBOL_DAY_LIST_PATH="$INPUT_PATH" \
      BACKFILL_REPORT_INCLUDE_JOBS="$BACKFILL_REPORT_INCLUDE_JOBS" \
      PHENIX_FLOW_READ_BACKEND="$FLOW_READ_BACKEND" \
      PHENIX_FLOW_WRITE_BACKEND="$FLOW_WRITE_BACKEND" \
      THETADATA_BASE_URL="$THETA_BASE_URL" \
      THETADATA_SUPPLEMENTAL_CONCURRENCY="$DOWNLOAD_SUPPLEMENTAL_CONCURRENCY" \
      CLICKHOUSE_CONNECT_TIMEOUT_SEC="$CLICKHOUSE_CONNECT_TIMEOUT_SEC" \
      CLICKHOUSE_SEND_TIMEOUT_SEC="$CLICKHOUSE_SEND_TIMEOUT_SEC" \
      CLICKHOUSE_RECEIVE_TIMEOUT_SEC="$CLICKHOUSE_RECEIVE_TIMEOUT_SEC" \
      CLICKHOUSE_DELETE_MUTATION_SYNC="$CLICKHOUSE_DELETE_MUTATION_SYNC" \
      node scripts/backfill/backfill-clickhouse-historical-days.js
    ) >> "$log_path" 2>&1
  else
    (
      cd "$PROJECT_ROOT"
      NODE_OPTIONS="$DOWNLOAD_NODE_OPTIONS" \
      BACKFILL_MODE="download" \
      BACKFILL_WORKER_TOTAL="$DOWNLOAD_WORKERS" \
      BACKFILL_WORKER_INDEX="$idx" \
      BACKFILL_REPORT_PATH="$report_path" \
      BACKFILL_SYMBOL_DAY_LIST_PATH="$INPUT_PATH" \
      BACKFILL_REPORT_INCLUDE_JOBS="$BACKFILL_REPORT_INCLUDE_JOBS" \
      PHENIX_FLOW_READ_BACKEND="$FLOW_READ_BACKEND" \
      PHENIX_FLOW_WRITE_BACKEND="$FLOW_WRITE_BACKEND" \
      THETADATA_BASE_URL="$THETA_BASE_URL" \
      THETADATA_SUPPLEMENTAL_CONCURRENCY="$DOWNLOAD_SUPPLEMENTAL_CONCURRENCY" \
      CLICKHOUSE_CONNECT_TIMEOUT_SEC="$CLICKHOUSE_CONNECT_TIMEOUT_SEC" \
      CLICKHOUSE_SEND_TIMEOUT_SEC="$CLICKHOUSE_SEND_TIMEOUT_SEC" \
      CLICKHOUSE_RECEIVE_TIMEOUT_SEC="$CLICKHOUSE_RECEIVE_TIMEOUT_SEC" \
      CLICKHOUSE_DELETE_MUTATION_SYNC="$CLICKHOUSE_DELETE_MUTATION_SYNC" \
      node scripts/backfill/backfill-clickhouse-historical-days.js
    ) > "$log_path" 2>&1
  fi
}

launch_enrich_workers() {
  enrich_pids=()
  local idx
  for idx in $(seq 0 $((ENRICH_WORKERS - 1))); do
    run_enrich_worker_once "$idx" 0 &
    enrich_pids+=("$!")
  done
}

launch_download_workers() {
  download_pids=()
  local idx
  for idx in $(seq 0 $((DOWNLOAD_WORKERS - 1))); do
    run_download_worker_once "$idx" 0 &
    download_pids+=("$!")
  done
}

wait_for_download_workers_with_requeue() {
  local stage_failed=0
  local -a failed_indices=()
  local -a missing_indices=()
  local -a requeue_indices=()
  local idx
  local wait_ok
  local report_path
  local report_ok
  for idx in $(seq 0 $((DOWNLOAD_WORKERS - 1))); do
    wait_ok=1
    if ! wait "${download_pids[$idx]}"; then
      wait_ok=0
    fi

    report_path="$DOWNLOAD_DIR/worker${idx}.json"
    report_ok=0
    if worker_report_is_complete "$report_path"; then
      report_ok=1
    fi

    if [[ ! -s "$report_path" ]]; then
      missing_indices+=("$idx")
      stage_failed=1
      if (( wait_ok == 0 )); then
        failed_indices+=("$idx")
      fi
      continue
    fi

    if (( wait_ok == 0 && report_ok == 1 )); then
      echo "Download worker $idx exited non-zero but report is complete; skipping requeue."
      continue
    fi

    if (( wait_ok == 0 || report_ok == 0 )); then
      stage_failed=1
      failed_indices+=("$idx")
    fi
  done
  if (( ${#failed_indices[@]} > 0 || ${#missing_indices[@]} > 0 )); then
    if (( ${#failed_indices[@]} > 0 )); then
      requeue_indices+=("${failed_indices[@]}")
    fi
    if (( ${#missing_indices[@]} > 0 )); then
      requeue_indices+=("${missing_indices[@]}")
    fi
    dedupe_indices "${requeue_indices[@]}"
    echo "Requeueing download shard workers: ${DEDUPED_INDICES[*]}"
    stage_failed=0
    for idx in "${DEDUPED_INDICES[@]}"; do
      if ! run_download_worker_once "$idx" 1; then
        stage_failed=1
      fi
      if [[ ! -s "$DOWNLOAD_DIR/worker${idx}.json" ]]; then
        stage_failed=1
      fi
    done
  fi
  if (( stage_failed != 0 )); then
    return 1
  fi
  return 0
}

wait_for_enrich_workers_with_requeue() {
  local stage_failed=0
  local -a failed_indices=()
  local -a missing_indices=()
  local -a requeue_indices=()
  local idx
  local wait_ok
  local report_path
  local report_ok
  for idx in $(seq 0 $((ENRICH_WORKERS - 1))); do
    wait_ok=1
    if ! wait "${enrich_pids[$idx]}"; then
      wait_ok=0
    fi

    report_path="$ENRICH_DIR/worker${idx}.json"
    report_ok=0
    if worker_report_is_complete "$report_path"; then
      report_ok=1
    fi

    if [[ ! -s "$report_path" ]]; then
      missing_indices+=("$idx")
      stage_failed=1
      if (( wait_ok == 0 )); then
        failed_indices+=("$idx")
      fi
      continue
    fi

    if (( wait_ok == 0 && report_ok == 1 )); then
      echo "Enrich worker $idx exited non-zero but report is complete; skipping requeue."
      continue
    fi

    if (( wait_ok == 0 || report_ok == 0 )); then
      stage_failed=1
      failed_indices+=("$idx")
    fi
  done
  if (( ${#failed_indices[@]} > 0 || ${#missing_indices[@]} > 0 )); then
    if (( ${#failed_indices[@]} > 0 )); then
      requeue_indices+=("${failed_indices[@]}")
    fi
    if (( ${#missing_indices[@]} > 0 )); then
      requeue_indices+=("${missing_indices[@]}")
    fi
    dedupe_indices "${requeue_indices[@]}"
    echo "Requeueing enrich shard workers: ${DEDUPED_INDICES[*]}"
    stage_failed=0
    for idx in "${DEDUPED_INDICES[@]}"; do
      if ! run_enrich_worker_once "$idx" 1; then
        stage_failed=1
      fi
      if [[ ! -s "$ENRICH_DIR/worker${idx}.json" ]]; then
        stage_failed=1
      fi
    done
  fi
  if (( stage_failed != 0 )); then
    return 1
  fi
  return 0
}

download_fail=0
enrich_fail=0

if (( PIPELINE_STAGE_OVERLAP == 1 )); then
  launch_enrich_workers
  launch_download_workers

  if ! wait_for_download_workers_with_requeue; then
    download_fail=1
  fi
  touch "$DOWNLOAD_DONE_FLAG"

  if ! wait_for_enrich_workers_with_requeue; then
    enrich_fail=1
  fi
else
  launch_download_workers
  if ! wait_for_download_workers_with_requeue; then
    download_fail=1
  fi
  touch "$DOWNLOAD_DONE_FLAG"

  if (( download_fail == 0 )); then
    launch_enrich_workers
    if ! wait_for_enrich_workers_with_requeue; then
      enrich_fail=1
    fi
  else
    enrich_fail=1
    echo "Skipping enrich stage because download stage failed."
  fi
fi

SUMMARY_JSON="$RUN_DIR/summary.json"
SUMMARY_TSV="$RUN_DIR/summary.tsv"
FAILURES_TSV="$RUN_DIR/failures.tsv"

node - "$RUN_DIR" "$SUMMARY_JSON" "$SUMMARY_TSV" "$FAILURES_TSV" <<'NODE'
const fs = require('node:fs');
const path = require('node:path');

const [runDir, summaryJson, summaryTsv, failuresTsv] = process.argv.slice(2);
const stageDirs = [
  { stage: 'download', dir: path.join(runDir, 'download') },
  { stage: 'enrich', dir: path.join(runDir, 'enrich') },
];

const reports = [];
stageDirs.forEach(({ stage, dir }) => {
  if (!fs.existsSync(dir)) return;
  fs.readdirSync(dir)
    .filter((entry) => /^worker\d+\.json$/.test(entry))
    .sort()
    .forEach((entry) => {
      const payload = JSON.parse(fs.readFileSync(path.join(dir, entry), 'utf8'));
      reports.push({
        stage,
        file: entry,
        ...payload,
      });
    });
});

const summary = reports.reduce((acc, report) => {
  const stage = report.stage;
  acc.stages[stage] = acc.stages[stage] || {
    workers: 0,
    totalJobs: 0,
    skippedJobs: 0,
    completedJobs: 0,
    pendingJobs: 0,
    failedJobs: 0,
    retriedJobs: 0,
    retryCount: 0,
    noDataJobs: 0,
    totalFetchedRows: 0,
    totalEnrichedRows: 0,
    totalRawTradeRows: 0,
    totalRawStockRows: 0,
    totalRawOiRows: 0,
    totalRawQuoteRows: 0,
    totalRawGreeksRows: 0,
  };
  const s = acc.stages[stage];
  s.workers += 1;
  s.totalJobs += Number(report.totalJobs || 0);
  s.skippedJobs += Number(report.skippedJobs || 0);
  s.completedJobs += Number(report.completedJobs || 0);
  s.pendingJobs += Number(report.pendingJobs || 0);
  s.failedJobs += Number(report.failedJobs || 0);
  s.retriedJobs += Number(report.retriedJobs || 0);
  s.retryCount += Number(report.retryCount || 0);
  s.noDataJobs += Number(report.noDataJobs || 0);
  s.totalFetchedRows += Number(report.totalFetchedRows || 0);
  s.totalEnrichedRows += Number(report.totalEnrichedRows || 0);
  s.totalRawTradeRows += Number(report.totalRawTradeRows || 0);
  s.totalRawStockRows += Number(report.totalRawStockRows || 0);
  s.totalRawOiRows += Number(report.totalRawOiRows || 0);
  s.totalRawQuoteRows += Number(report.totalRawQuoteRows || 0);
  s.totalRawGreeksRows += Number(report.totalRawGreeksRows || 0);
  acc.failures.push(
    ...(report.failures || []).map((failure) => ({
      stage,
      worker: report.workerIndex,
      ...failure,
    })),
  );
  return acc;
}, {
  generatedAt: new Date().toISOString(),
  stages: {},
  failures: [],
});

fs.writeFileSync(summaryJson, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');

const lines = ['stage\tmetric\tvalue'];
Object.entries(summary.stages).forEach(([stage, metrics]) => {
  Object.entries(metrics).forEach(([metric, value]) => {
    lines.push(`${stage}\t${metric}\t${value}`);
  });
});
fs.writeFileSync(summaryTsv, `${lines.join('\n')}\n`, 'utf8');

fs.writeFileSync(
  failuresTsv,
  ['stage\tworker\tsymbol\tdayIso\terror']
    .concat(summary.failures.map((failure) => [failure.stage, failure.worker, failure.symbol, failure.dayIso, failure.error].join('\t')))
    .join('\n') + '\n',
  'utf8',
);
NODE

echo "Pipeline summary: $SUMMARY_JSON"
if (( download_fail != 0 )) || (( enrich_fail != 0 )); then
  echo "Pipeline failed (download_fail=$download_fail enrich_fail=$enrich_fail)."
  exit 1
fi

echo "ClickHouse pipeline backfill complete."
