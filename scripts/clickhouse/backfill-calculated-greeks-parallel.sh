#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_ROOT"

WORKERS="${CALC_GREEKS_WORKERS:-4}"
if ! [[ "$WORKERS" =~ ^[0-9]+$ ]] || (( WORKERS < 1 )); then
  echo "CALC_GREEKS_WORKERS must be a positive integer (got: $WORKERS)"
  exit 1
fi

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${CALC_GREEKS_RUN_ID:-calc-greeks-$TS}"
REPORT_ROOT="${CALC_GREEKS_REPORT_DIR:-$PROJECT_ROOT/artifacts/reports}"
RUN_DIR="$REPORT_ROOT/$RUN_ID"
mkdir -p "$RUN_DIR"

echo "Starting calculated-greeks backfill"
echo "run_id: $RUN_ID"
echo "workers: $WORKERS"
echo "run_dir: $RUN_DIR"
echo "range: ${CALC_GREEKS_START_DATE:-auto} -> ${CALC_GREEKS_END_DATE:-auto}"
echo "symbol-day list: ${CALC_GREEKS_SYMBOL_DAY_LIST_PATH:-auto(option_trade_day_cache)}"

pids=()
for idx in $(seq 0 $((WORKERS - 1))); do
  report_path="$RUN_DIR/worker-$idx.json"
  log_path="$RUN_DIR/worker-$idx.log"
  (
    CALC_GREEKS_RUN_ID="$RUN_ID" \
    CALC_GREEKS_WORKER_TOTAL="$WORKERS" \
    CALC_GREEKS_WORKER_INDEX="$idx" \
    CALC_GREEKS_REPORT_PATH="$report_path" \
    node scripts/clickhouse/backfill-calculated-greeks.js
  ) >"$log_path" 2>&1 &
  pids+=("$!")
done

overall_exit=0
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    overall_exit=1
  fi
done

node - "$RUN_DIR" "$RUN_ID" <<'NODE'
const fs = require('node:fs');
const path = require('node:path');
const [runDir, runId] = process.argv.slice(2);
const files = fs.readdirSync(runDir)
  .filter((name) => name.startsWith('worker-') && name.endsWith('.json'))
  .map((name) => path.join(runDir, name));

const totals = {
  assignedJobs: 0,
  completedJobs: 0,
  failedJobs: 0,
  skippedJobs: 0,
  insertedRowsTotal: 0,
  solvedRowsTotal: 0,
};
const workers = [];
for (const file of files) {
  const payload = JSON.parse(fs.readFileSync(file, 'utf8'));
  const t = payload.totals || {};
  totals.assignedJobs += Number(t.assignedJobs || 0);
  totals.completedJobs += Number(t.completedJobs || 0);
  totals.failedJobs += Number(t.failedJobs || 0);
  totals.skippedJobs += Number(t.skippedJobs || 0);
  totals.insertedRowsTotal += Number(t.insertedRowsTotal || 0);
  totals.solvedRowsTotal += Number(t.solvedRowsTotal || 0);
  workers.push({
    workerIndex: payload.workerIndex,
    reportPath: file,
    assignedJobs: Number(t.assignedJobs || 0),
    completedJobs: Number(t.completedJobs || 0),
    failedJobs: Number(t.failedJobs || 0),
    insertedRowsTotal: Number(t.insertedRowsTotal || 0),
    solvedRowsTotal: Number(t.solvedRowsTotal || 0),
  });
}

workers.sort((a, b) => a.workerIndex - b.workerIndex);
const summary = {
  runId,
  generatedAt: new Date().toISOString(),
  totals,
  workers,
};
const outPath = path.join(runDir, 'summary.json');
fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
console.log(JSON.stringify({ summaryPath: outPath, totals }, null, 2));
NODE

if (( overall_exit != 0 )); then
  echo "One or more calculated-greeks workers failed."
  exit 1
fi

echo "Calculated-greeks backfill complete."
echo "Summary: $RUN_DIR/summary.json"

