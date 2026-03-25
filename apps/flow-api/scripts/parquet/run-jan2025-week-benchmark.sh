#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT_DIR"

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

derive_download_workers() {
  local total theta_cap download
  total="$1"
  theta_cap="$2"
  download=$theta_cap
  if (( download > total - 1 )); then
    download=$(( total > 1 ? total - 1 : 1 ))
  fi
  if (( download < 1 )); then
    download=1
  fi
  printf '%s\n' "$download"
}

START_DATE="${START_DATE:-2025-01-02}"
END_DATE="${END_DATE:-2025-01-08}"
SYMBOL_FILE="${SYMBOL_FILE:-$ROOT_DIR/apps/flow-api/config/top100-universe.json}"
SYMBOL_LIMIT="${SYMBOL_LIMIT:-100}"
EXTRA_SYMBOLS="${EXTRA_SYMBOLS:-SPX,SPXW,SPY,QQQ,VIX,VIXW,RUT,RUTW,XSP}"
PARQUET_WORKERS="${PARQUET_WORKERS:-$(detect_default_parquet_workers)}"
THETADATA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"
PARQUET_RUN_ID="${PARQUET_RUN_ID:-parquet-jan2025-week-$(date -u +%Y%m%dT%H%M%SZ)}"
PARQUET_RESUME_EXISTING="${PARQUET_RESUME_EXISTING:-1}"
PARQUET_HEAVY_RAW_INDEX_QUOTE_CONCURRENCY="${PARQUET_HEAVY_RAW_INDEX_QUOTE_CONCURRENCY:-4}"
PARQUET_HEAVY_RAW_INDEX_GREEKS_CONCURRENCY="${PARQUET_HEAVY_RAW_INDEX_GREEKS_CONCURRENCY:-4}"
PARQUET_HEAVY_RAW_INDEX_EXPIRATION_CONCURRENCY="${PARQUET_HEAVY_RAW_INDEX_EXPIRATION_CONCURRENCY:-4}"
PARQUET_THETA_RETRY_ATTEMPTS="${PARQUET_THETA_RETRY_ATTEMPTS:-8}"
PARQUET_THETA_RETRY_BASE_DELAY_MS="${PARQUET_THETA_RETRY_BASE_DELAY_MS:-2000}"
PARQUET_THETA_RETRY_MAX_DELAY_MS="${PARQUET_THETA_RETRY_MAX_DELAY_MS:-60000}"
PARQUET_THETA_GLOBAL_COOLDOWN_MS="${PARQUET_THETA_GLOBAL_COOLDOWN_MS:-30000}"
PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS="${PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS:-4}"
PARQUET_DOWNLOAD_WORKERS="${PARQUET_DOWNLOAD_WORKERS:-$(derive_download_workers "$PARQUET_WORKERS" "$PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS")}"
PARQUET_COMPUTE_WORKERS="${PARQUET_COMPUTE_WORKERS:-$(( PARQUET_WORKERS - PARQUET_DOWNLOAD_WORKERS ))}"
if (( PARQUET_COMPUTE_WORKERS < 1 )); then
  PARQUET_COMPUTE_WORKERS=1
fi
PARQUET_STAGE_MAX_ATTEMPTS="${PARQUET_STAGE_MAX_ATTEMPTS:-3}"

export START_DATE END_DATE SYMBOL_FILE SYMBOL_LIMIT EXTRA_SYMBOLS THETADATA_BASE_URL PARQUET_RUN_ID
export PARQUET_RESUME_EXISTING PARQUET_STAGE_MAX_ATTEMPTS
export PARQUET_HEAVY_RAW_INDEX_QUOTE_CONCURRENCY PARQUET_HEAVY_RAW_INDEX_GREEKS_CONCURRENCY PARQUET_HEAVY_RAW_INDEX_EXPIRATION_CONCURRENCY
export PARQUET_THETA_RETRY_ATTEMPTS PARQUET_THETA_RETRY_BASE_DELAY_MS PARQUET_THETA_RETRY_MAX_DELAY_MS PARQUET_THETA_GLOBAL_COOLDOWN_MS PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS

DEFAULT_LOCAL_PARQUET_ROOT="${HOME}/Library/Caches/phenixflow/parquet"
DEFAULT_EXTERNAL_PARQUET_ROOT="/Volumes/Phenix4TB/phenixflow/parquet"
if [ -n "${PHENIXFLOW_PARQUET_ROOT:-}" ]; then
  EFFECTIVE_PARQUET_ROOT="$PHENIXFLOW_PARQUET_ROOT"
elif [ -d "/Volumes/Phenix4TB" ]; then
  EFFECTIVE_PARQUET_ROOT="$DEFAULT_EXTERNAL_PARQUET_ROOT"
else
  EFFECTIVE_PARQUET_ROOT="$DEFAULT_LOCAL_PARQUET_ROOT"
fi

RUN_ROOT="$EFFECTIVE_PARQUET_ROOT/runs/$PARQUET_RUN_ID"
LOG_ROOT="$RUN_ROOT/logs"
REPORT_ROOT="$RUN_ROOT/reports"
STATE_CONTROL_ROOT="$RUN_ROOT/state/control"
STOP_PATH="$STATE_CONTROL_ROOT/stop-requested.json"
mkdir -p "$LOG_ROOT" "$REPORT_ROOT" "$STATE_CONTROL_ROOT"
rm -f "$REPORT_ROOT/summary.json"
rm -f "$STOP_PATH"

RUN_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "Parquet benchmark run: $PARQUET_RUN_ID"
echo "Run root: $RUN_ROOT"
echo "Theta base URL: $THETADATA_BASE_URL"
echo "Date range: $START_DATE -> $END_DATE"
echo "Worker budget: $PARQUET_WORKERS"
echo "Download workers: $PARQUET_DOWNLOAD_WORKERS"
echo "Compute workers: $PARQUET_COMPUTE_WORKERS"
echo "Theta connection cap: $PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS"
echo "Started at: $RUN_STARTED_AT"

request_stop() {
  local reason="${1:-signal}"
  mkdir -p "$STATE_CONTROL_ROOT"
  cat >"$STOP_PATH" <<EOF
{
  "reason": "$reason",
  "requestedAt": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "pid": $$
}
EOF
}

pids=()

cleanup_on_signal() {
  echo "Stop requested; waiting for parquet workers to finish current tasks..."
  request_stop "launcher_signal"
}

trap cleanup_on_signal INT TERM

for (( idx=0; idx<PARQUET_DOWNLOAD_WORKERS; idx++ )); do
  log_file="$LOG_ROOT/download-worker-$idx.log"
  echo "Launching download worker $idx -> $log_file"
  PARQUET_WORKER_ROLE="download" \
  PARQUET_DOWNLOAD_WORKER_TOTAL="$PARQUET_DOWNLOAD_WORKERS" \
  PARQUET_DOWNLOAD_WORKER_INDEX="$idx" \
  node "$ROOT_DIR/apps/flow-api/scripts/parquet/backfill-parquet-greeks.js" \
    >"$log_file" 2>&1 &
  pids+=("$!")
done

for (( idx=0; idx<PARQUET_COMPUTE_WORKERS; idx++ )); do
  log_file="$LOG_ROOT/compute-worker-$idx.log"
  echo "Launching compute worker $idx -> $log_file"
  PARQUET_WORKER_ROLE="compute" \
  PARQUET_COMPUTE_WORKER_TOTAL="$PARQUET_COMPUTE_WORKERS" \
  PARQUET_COMPUTE_WORKER_INDEX="$idx" \
  node "$ROOT_DIR/apps/flow-api/scripts/parquet/backfill-parquet-greeks.js" \
    >"$log_file" 2>&1 &
  pids+=("$!")
done

printf "%s\n" "${pids[@]}" >"$RUN_ROOT/worker-pids.txt"

status=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    status=1
  fi
done

node - <<'NODE' "$RUN_ROOT" "$RUN_STARTED_AT" "$PARQUET_DOWNLOAD_WORKERS" "$PARQUET_COMPUTE_WORKERS"
const fs = require('node:fs');
const path = require('node:path');

const runRoot = process.argv[2];
const runStartedAt = process.argv[3];
const downloadWorkers = Number(process.argv[4]);
const computeWorkers = Number(process.argv[5]);
const reportsRoot = path.join(runRoot, 'reports');
const jobStateRoot = path.join(runRoot, 'state', 'jobs');
const controlRoot = path.join(runRoot, 'state', 'control');
const generatedAt = new Date().toISOString();
const durationMs = runStartedAt ? (Date.parse(generatedAt) - Date.parse(runStartedAt)) : null;
const summary = {
  runRoot,
  generatedAt,
  startedAt: runStartedAt || null,
  durationMs: Number.isFinite(durationMs) ? durationMs : null,
  downloadWorkers,
  computeWorkers,
  stopRequested: fs.existsSync(path.join(controlRoot, 'stop-requested.json')),
  totalJobs: 0,
  completedJobs: 0,
  failedJobs: 0,
  runningJobs: 0,
  totalStockRows: 0,
  totalQuoteRows: 0,
  totalRawGreekRows: 0,
  totalFinalGreekRows: 0,
  stockMs: 0,
  quoteMs: 0,
  rawGreekMs: 0,
  calcGreekMs: 0,
  workers: [],
};

const jobFiles = fs.existsSync(jobStateRoot)
  ? fs.readdirSync(jobStateRoot).filter((name) => name.endsWith('.json')).sort()
  : [];

for (const jobFile of jobFiles) {
  const job = JSON.parse(fs.readFileSync(path.join(jobStateRoot, jobFile), 'utf8'));
  summary.totalJobs += 1;
  if (job.status === 'complete') summary.completedJobs += 1;
  if (job.status === 'failed') summary.failedJobs += 1;
  if (job.status === 'running') summary.runningJobs += 1;
  summary.totalStockRows += Number(job?.stages?.stock?.rowCount || 0);
  summary.totalQuoteRows += Number(job?.stages?.quotes?.rowCount || 0);
  summary.totalRawGreekRows += job.greekMode === 'raw' ? Number(job?.stages?.greeks?.rowCount || 0) : 0;
  summary.totalFinalGreekRows += Number(job?.stages?.greeks?.rowCount || 0);
  summary.stockMs += Number(job?.stages?.stock?.elapsedMs || 0);
  summary.quoteMs += Number(job?.stages?.quotes?.elapsedMs || 0);
  if (job.greekMode === 'raw') {
    summary.rawGreekMs += Number(job?.stages?.greeks?.elapsedMs || 0);
  } else {
    summary.calcGreekMs += Number(job?.stages?.greeks?.elapsedMs || 0);
  }
}

const workerFiles = fs.existsSync(reportsRoot)
  ? fs.readdirSync(reportsRoot).filter((name) => /(download|compute)-worker-\d+\.json$/.test(name)).sort()
  : [];
for (const workerFile of workerFiles) {
  const report = JSON.parse(fs.readFileSync(path.join(reportsRoot, workerFile), 'utf8'));
  summary.workers.push({
    role: report.role,
    workerIndex: report.workerIndex,
    tasksCompleted: Number(report?.counters?.tasksCompleted || 0),
    tasksFailed: Number(report?.counters?.tasksFailed || 0),
    currentTask: report.currentTask || null,
  });
}

const summaryPath = path.join(reportsRoot, 'summary.json');
fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
console.log(JSON.stringify(summary, null, 2));
NODE

exit "$status"
