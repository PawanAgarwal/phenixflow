#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$ROOT_DIR"

START_DATE="${START_DATE:-2025-01-02}"
END_DATE="${END_DATE:-2025-01-08}"
SYMBOL_FILE="${SYMBOL_FILE:-$ROOT_DIR/apps/flow-api/config/top100-universe.json}"
SYMBOL_LIMIT="${SYMBOL_LIMIT:-100}"
EXTRA_SYMBOLS="${EXTRA_SYMBOLS:-SPX,SPXW,SPY,QQQ,VIX,VIXW,RUT,RUTW,XSP}"
PARQUET_WORKERS="${PARQUET_WORKERS:-6}"
THETADATA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"
PARQUET_RUN_ID="${PARQUET_RUN_ID:-parquet-jan2025-week-$(date -u +%Y%m%dT%H%M%SZ)}"

export START_DATE END_DATE SYMBOL_FILE SYMBOL_LIMIT EXTRA_SYMBOLS THETADATA_BASE_URL PARQUET_RUN_ID

RUN_ROOT="${PHENIXFLOW_PARQUET_ROOT:-$HOME/Library/Caches/phenixflow/parquet}/runs/$PARQUET_RUN_ID"
LOG_ROOT="$RUN_ROOT/logs"
REPORT_ROOT="$RUN_ROOT/reports"
mkdir -p "$LOG_ROOT" "$REPORT_ROOT"

RUN_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "Parquet benchmark run: $PARQUET_RUN_ID"
echo "Run root: $RUN_ROOT"
echo "Theta base URL: $THETADATA_BASE_URL"
echo "Date range: $START_DATE -> $END_DATE"
echo "Workers: $PARQUET_WORKERS"
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

for (let idx = 0; idx < workerTotal; idx += 1) {
  const reportPath = path.join(reportsRoot, `worker-${idx}.json`);
  if (!fs.existsSync(reportPath)) continue;
  const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  summary.totalJobs += Number(report.totalJobs || 0);
  summary.completedJobs += Number(report.completedJobs || 0);
  summary.failedJobs += Number(report.failedJobs || 0);
  summary.totalStockRows += Number(report.totalStockRows || 0);
  summary.totalQuoteRows += Number(report.totalQuoteRows || 0);
  summary.totalRawGreekRows += Number(report.totalRawGreekRows || 0);
  summary.totalFinalGreekRows += Number(report.totalFinalGreekRows || 0);
  summary.stockMs += Number(report.stockMs || 0);
  summary.quoteMs += Number(report.quoteMs || 0);
  summary.indexGreekMs += Number(report.indexGreekMs || 0);
  summary.calcGreekMs += Number(report.calcGreekMs || 0);
  summary.workers.push({
    workerIndex: report.workerIndex,
    totalJobs: report.totalJobs,
    completedJobs: report.completedJobs,
    failedJobs: report.failedJobs,
  });
}

const summaryPath = path.join(reportsRoot, 'summary.json');
fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
console.log(JSON.stringify(summary, null, 2));
NODE

exit "$status"
