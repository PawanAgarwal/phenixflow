#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CPU_CORES="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
CPU_TARGET_PCT="${BACKFILL_CPU_TARGET_PCT:-70}"
if ! [[ "$CPU_TARGET_PCT" =~ ^[0-9]+$ ]] || (( CPU_TARGET_PCT < 10 )) || (( CPU_TARGET_PCT > 95 )); then
  echo "BACKFILL_CPU_TARGET_PCT must be an integer in [10,95] (got: $CPU_TARGET_PCT)"
  exit 1
fi
DEFAULT_WORKERS=$(( (CPU_CORES * CPU_TARGET_PCT + 99) / 100 ))
if (( DEFAULT_WORKERS < 1 )); then
  DEFAULT_WORKERS=1
fi
BACKFILL_WORKERS="${BACKFILL_WORKERS:-$DEFAULT_WORKERS}"
MAX_WORKERS="${BACKFILL_MAX_WORKERS:-16}"
BACKFILL_MEMORY_RESERVE_MB="${BACKFILL_MEMORY_RESERVE_MB:-4096}"
BACKFILL_MEMORY_PER_WORKER_MB="${BACKFILL_MEMORY_PER_WORKER_MB:-2200}"
BACKFILL_NODE_MAX_OLD_SPACE_MB="${BACKFILL_NODE_MAX_OLD_SPACE_MB:-1024}"
BACKFILL_RAM_BUDGET_MB="${BACKFILL_RAM_BUDGET_MB:-8192}"
BACKFILL_WORKER_OVERHEAD_MB="${BACKFILL_WORKER_OVERHEAD_MB:-512}"
BACKFILL_REPORT_INCLUDE_JOBS="${BACKFILL_REPORT_INCLUDE_JOBS:-0}"
REPORT_ROOT="${BACKFILL_REPORT_DIR:-$PROJECT_ROOT/artifacts/reports}"
INPUT_PATH="${BACKFILL_SYMBOL_DAY_LIST_PATH:-}"
THETA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"
THETADATA_HISTORICAL_OPTION_FORMAT="${THETADATA_HISTORICAL_OPTION_FORMAT:-ndjson}"
THETADATA_OPTION_QUOTE_FORMAT="${THETADATA_OPTION_QUOTE_FORMAT:-ndjson}"
TS="$(date +%Y%m%dT%H%M%S)"
RUN_DIR="$REPORT_ROOT/clickhouse-last-week-backfill-$TS"

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

if ! [[ "$BACKFILL_WORKERS" =~ ^[0-9]+$ ]] || (( BACKFILL_WORKERS < 1 )); then
  echo "BACKFILL_WORKERS must be a positive integer (got: $BACKFILL_WORKERS)"
  exit 1
fi

if ! [[ "$MAX_WORKERS" =~ ^[0-9]+$ ]] || (( MAX_WORKERS < 1 )); then
  echo "BACKFILL_MAX_WORKERS must be a positive integer (got: $MAX_WORKERS)"
  exit 1
fi

if ! [[ "$BACKFILL_MEMORY_RESERVE_MB" =~ ^[0-9]+$ ]]; then
  echo "BACKFILL_MEMORY_RESERVE_MB must be a non-negative integer (got: $BACKFILL_MEMORY_RESERVE_MB)"
  exit 1
fi

if ! [[ "$BACKFILL_MEMORY_PER_WORKER_MB" =~ ^[0-9]+$ ]] || (( BACKFILL_MEMORY_PER_WORKER_MB < 256 )); then
  echo "BACKFILL_MEMORY_PER_WORKER_MB must be an integer >= 256 (got: $BACKFILL_MEMORY_PER_WORKER_MB)"
  exit 1
fi

if ! [[ "$BACKFILL_NODE_MAX_OLD_SPACE_MB" =~ ^[0-9]+$ ]] || (( BACKFILL_NODE_MAX_OLD_SPACE_MB < 256 )); then
  echo "BACKFILL_NODE_MAX_OLD_SPACE_MB must be an integer >= 256 (got: $BACKFILL_NODE_MAX_OLD_SPACE_MB)"
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

if (( BACKFILL_WORKERS > MAX_WORKERS )); then
  echo "Capping workers from $BACKFILL_WORKERS to BACKFILL_MAX_WORKERS=$MAX_WORKERS"
  BACKFILL_WORKERS="$MAX_WORKERS"
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

if (( BACKFILL_WORKERS > MEMORY_WORKER_CAP )); then
  echo "Capping workers from $BACKFILL_WORKERS to memory cap $MEMORY_WORKER_CAP"
  BACKFILL_WORKERS="$MEMORY_WORKER_CAP"
fi

BACKFILL_WORKER_FOOTPRINT_MB=$(( BACKFILL_NODE_MAX_OLD_SPACE_MB + BACKFILL_WORKER_OVERHEAD_MB ))
RAM_BUDGET_WORKER_CAP=$(( BACKFILL_RAM_BUDGET_MB / BACKFILL_WORKER_FOOTPRINT_MB ))
if (( RAM_BUDGET_WORKER_CAP < 1 )); then
  RAM_BUDGET_WORKER_CAP=1
fi

if (( BACKFILL_WORKERS > RAM_BUDGET_WORKER_CAP )); then
  echo "Capping workers from $BACKFILL_WORKERS to RAM budget cap $RAM_BUDGET_WORKER_CAP"
  BACKFILL_WORKERS="$RAM_BUDGET_WORKER_CAP"
fi

ESTIMATED_WORKER_MEMORY_MB=$(( BACKFILL_WORKERS * BACKFILL_WORKER_FOOTPRINT_MB ))

WORKER_NODE_OPTIONS="--max-old-space-size=${BACKFILL_NODE_MAX_OLD_SPACE_MB}"
if [[ -n "${NODE_OPTIONS:-}" ]]; then
  WORKER_NODE_OPTIONS="${WORKER_NODE_OPTIONS} ${NODE_OPTIONS}"
fi

mkdir -p "$RUN_DIR"

echo "Starting ClickHouse historical backfill with $BACKFILL_WORKERS worker(s)..."
echo "CPU cores: $CPU_CORES"
echo "CPU target: ${CPU_TARGET_PCT}%"
if [[ "$TOTAL_MEMORY_MB" =~ ^[0-9]+$ ]] && (( TOTAL_MEMORY_MB > 0 )); then
  echo "Total memory detected: ${TOTAL_MEMORY_MB}MB"
else
  echo "Total memory detected: unknown"
fi
echo "Memory reserve: ${BACKFILL_MEMORY_RESERVE_MB}MB"
echo "Memory per worker target: ${BACKFILL_MEMORY_PER_WORKER_MB}MB"
echo "Memory worker cap: $MEMORY_WORKER_CAP"
echo "RAM budget: ${BACKFILL_RAM_BUDGET_MB}MB"
echo "Per-worker footprint estimate: ${BACKFILL_WORKER_FOOTPRINT_MB}MB (heap ${BACKFILL_NODE_MAX_OLD_SPACE_MB}MB + overhead ${BACKFILL_WORKER_OVERHEAD_MB}MB)"
echo "RAM budget worker cap: $RAM_BUDGET_WORKER_CAP"
echo "Estimated worker memory at launch: ${ESTIMATED_WORKER_MEMORY_MB}MB"
echo "Node heap cap: ${BACKFILL_NODE_MAX_OLD_SPACE_MB}MB"
echo "Detailed job reports: $BACKFILL_REPORT_INCLUDE_JOBS"
echo "Theta base URL: $THETA_BASE_URL"
echo "Theta formats: trade=${THETADATA_HISTORICAL_OPTION_FORMAT} quote=${THETADATA_OPTION_QUOTE_FORMAT}"
echo "Reports: $RUN_DIR"
if [[ -n "$INPUT_PATH" ]]; then
  echo "Input: $INPUT_PATH"
fi

pids=()
for idx in $(seq 0 $((BACKFILL_WORKERS - 1))); do
  report_path="$RUN_DIR/worker${idx}.json"
  log_path="$RUN_DIR/worker${idx}.log"
  (
    cd "$PROJECT_ROOT"
    NODE_OPTIONS="$WORKER_NODE_OPTIONS" \
    BACKFILL_WORKER_TOTAL="$BACKFILL_WORKERS" \
    BACKFILL_WORKER_INDEX="$idx" \
    BACKFILL_REPORT_PATH="$report_path" \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$INPUT_PATH" \
    BACKFILL_REPORT_INCLUDE_JOBS="$BACKFILL_REPORT_INCLUDE_JOBS" \
    THETADATA_BASE_URL="$THETA_BASE_URL" \
    THETADATA_HISTORICAL_OPTION_FORMAT="$THETADATA_HISTORICAL_OPTION_FORMAT" \
    THETADATA_OPTION_QUOTE_FORMAT="$THETADATA_OPTION_QUOTE_FORMAT" \
    node scripts/backfill/backfill-clickhouse-historical-days.js
  ) > "$log_path" 2>&1 &
  pids+=("$!")
done

fail=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    fail=1
  fi
done

SUMMARY_JSON="$RUN_DIR/summary.json"
SUMMARY_TSV="$RUN_DIR/summary.tsv"
FAILURES_TSV="$RUN_DIR/failures.tsv"

node - "$RUN_DIR" "$SUMMARY_JSON" "$SUMMARY_TSV" "$FAILURES_TSV" <<'NODE'
const fs = require('node:fs');
const path = require('node:path');

const [runDir, summaryJson, summaryTsv, failuresTsv] = process.argv.slice(2);
const workerFiles = fs.readdirSync(runDir)
  .filter((entry) => /^worker\d+\.json$/.test(entry))
  .sort();

const reports = workerFiles.map((entry) => {
  const filePath = path.join(runDir, entry);
  return {
    file: entry,
    ...JSON.parse(fs.readFileSync(filePath, 'utf8')),
  };
});

const summary = reports.reduce((acc, report) => {
  acc.totalJobs += Number(report.totalJobs || 0);
  acc.skippedJobs += Number(report.skippedJobs || 0);
  acc.completedJobs += Number(report.completedJobs || 0);
  acc.noDataJobs += Number(report.noDataJobs || 0);
  acc.retriedJobs += Number(report.retriedJobs || 0);
  acc.retryCount += Number(report.retryCount || 0);
  acc.failedJobs += Number(report.failedJobs || 0);
  acc.totalFetchedRows += Number(report.totalFetchedRows || 0);
  acc.totalEnrichedRows += Number(report.totalEnrichedRows || 0);
  acc.failures.push(...(report.failures || []).map((failure) => ({ worker: report.workerIndex, ...failure })));
  return acc;
}, {
  generatedAt: new Date().toISOString(),
  totalJobs: 0,
  skippedJobs: 0,
  completedJobs: 0,
  noDataJobs: 0,
  retriedJobs: 0,
  retryCount: 0,
  failedJobs: 0,
  totalFetchedRows: 0,
  totalEnrichedRows: 0,
  workerReports: reports.map((report) => ({
    file: report.file,
    workerIndex: report.workerIndex,
    totalJobs: report.totalJobs,
    skippedJobs: report.skippedJobs,
    completedJobs: report.completedJobs,
    noDataJobs: report.noDataJobs,
    failedJobs: report.failedJobs,
    totalFetchedRows: report.totalFetchedRows,
    totalEnrichedRows: report.totalEnrichedRows,
  })),
  failures: [],
});

fs.writeFileSync(summaryJson, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
fs.writeFileSync(
  summaryTsv,
  [
    'metric\tvalue',
    `totalJobs\t${summary.totalJobs}`,
    `skippedJobs\t${summary.skippedJobs}`,
    `completedJobs\t${summary.completedJobs}`,
    `noDataJobs\t${summary.noDataJobs}`,
    `retriedJobs\t${summary.retriedJobs}`,
    `retryCount\t${summary.retryCount}`,
    `failedJobs\t${summary.failedJobs}`,
    `totalFetchedRows\t${summary.totalFetchedRows}`,
    `totalEnrichedRows\t${summary.totalEnrichedRows}`,
  ].join('\n') + '\n',
  'utf8',
);
fs.writeFileSync(
  failuresTsv,
  ['worker\tsymbol\tdayIso\terror']
    .concat(summary.failures.map((failure) => [failure.worker, failure.symbol, failure.dayIso, failure.error].join('\t')))
    .join('\n') + '\n',
  'utf8',
);
NODE

if (( fail != 0 )); then
  echo "One or more backfill workers failed."
  exit 1
fi

echo "Parallel ClickHouse historical backfill complete."
echo "Summary: $SUMMARY_JSON"
