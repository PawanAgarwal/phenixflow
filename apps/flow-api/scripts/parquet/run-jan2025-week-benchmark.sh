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

export START_DATE END_DATE SYMBOL_FILE SYMBOL_LIMIT EXTRA_SYMBOLS THETADATA_BASE_URL PARQUET_RUN_ID
export PARQUET_RESUME_EXISTING
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
mkdir -p "$LOG_ROOT" "$REPORT_ROOT"
rm -f "$REPORT_ROOT/summary.json"

RUN_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "Parquet benchmark run: $PARQUET_RUN_ID"
echo "Run root: $RUN_ROOT"
echo "Theta base URL: $THETADATA_BASE_URL"
echo "Date range: $START_DATE -> $END_DATE"
echo "Workers: $PARQUET_WORKERS"
echo "Theta connection cap: $PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS"
echo "Started at: $RUN_STARTED_AT"

pids=()
for (( idx=0; idx<PARQUET_WORKERS; idx++ )); do
  log_file="$LOG_ROOT/worker-$idx.log"
  echo "Launching worker $idx -> $log_file"
  PARQUET_WORKER_TOTAL="$PARQUET_WORKERS" \
  PARQUET_WORKER_INDEX="$idx" \
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

node - <<'NODE' "$RUN_ROOT" "$PARQUET_WORKERS" "$RUN_STARTED_AT"
const fs = require('node:fs');
const path = require('node:path');

const runRoot = process.argv[2];
const workerTotal = Number(process.argv[3]);
const runStartedAt = process.argv[4];
const reportsRoot = path.join(runRoot, 'reports');
const generatedAt = new Date().toISOString();
const durationMs = runStartedAt ? (Date.parse(generatedAt) - Date.parse(runStartedAt)) : null;
const summary = {
  runRoot,
  generatedAt,
  startedAt: runStartedAt || null,
  durationMs: Number.isFinite(durationMs) ? durationMs : null,
  workerTotal,
  totalJobs: 0,
  completedJobs: 0,
  failedJobs: 0,
  totalStockRows: 0,
  totalQuoteRows: 0,
  totalRawGreekRows: 0,
  totalFinalGreekRows: 0,
  stockMs: 0,
  quoteMs: 0,
  indexGreekMs: 0,
  calcGreekMs: 0,
  workers: [],
};

const workerFiles = fs.existsSync(reportsRoot)
  ? fs.readdirSync(reportsRoot).filter((name) => /^worker-\d+\.json$/.test(name)).sort()
  : [];
const jobsByKey = new Map();

for (const workerFile of workerFiles) {
  const reportPath = path.join(reportsRoot, workerFile);
  const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  for (const job of Array.isArray(report.jobs) ? report.jobs : []) {
    if (!job?.symbol || !job?.dayIso) continue;
    const key = `${job.symbol}::${job.dayIso}`;
    const existing = jobsByKey.get(key);
    if (!existing || existing.status !== 'complete' || job.status === 'complete') {
      jobsByKey.set(key, job);
    }
  }
  summary.workers.push({
    workerIndex: report.workerIndex,
    totalJobs: report.totalJobs,
    completedJobs: report.completedJobs,
    failedJobs: report.failedJobs,
  });
}

summary.totalJobs = jobsByKey.size;
for (const job of jobsByKey.values()) {
  if (job.status === 'complete') summary.completedJobs += 1;
  if (job.status === 'failed') summary.failedJobs += 1;
  summary.totalStockRows += Number(job.stockRows || 0);
  summary.totalQuoteRows += Number(job.quoteRows || 0);
  summary.totalRawGreekRows += Number(job.rawGreekRows || 0);
  summary.totalFinalGreekRows += Number(job.finalGreekRows || 0);
  summary.stockMs += Number(job.stockMs || 0);
  summary.quoteMs += Number(job.quoteMs || 0);
  summary.indexGreekMs += Number(job.indexGreekMs || 0);
  summary.calcGreekMs += Number(job.calcGreekMs || 0);
}

const summaryPath = path.join(reportsRoot, 'summary.json');
fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
console.log(JSON.stringify(summary, null, 2));
NODE

exit "$status"
