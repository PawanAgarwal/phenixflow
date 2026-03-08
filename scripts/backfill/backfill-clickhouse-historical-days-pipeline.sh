#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CPU_CORES="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"
ENRICH_CPU_TARGET_PCT="${ENRICH_CPU_TARGET_PCT:-70}"
MAX_WORKERS="${BACKFILL_MAX_WORKERS:-16}"
REPORT_ROOT="${BACKFILL_REPORT_DIR:-$PROJECT_ROOT/artifacts/reports}"
INPUT_PATH="${BACKFILL_SYMBOL_DAY_LIST_PATH:-}"
PIPELINE_LOOP_SLEEP_MS="${PIPELINE_LOOP_SLEEP_MS:-5000}"
PIPELINE_LOOP_MAX_PASSES="${PIPELINE_LOOP_MAX_PASSES:-400}"
DOWNLOAD_SUPPLEMENTAL_CONCURRENCY="${DOWNLOAD_SUPPLEMENTAL_CONCURRENCY:-4}"
ENRICH_SUPPLEMENTAL_CONCURRENCY="${ENRICH_SUPPLEMENTAL_CONCURRENCY:-2}"
PIPELINE_STAGE_OVERLAP_RAW="${PIPELINE_STAGE_OVERLAP:-0}"
BACKFILL_MEMORY_RESERVE_MB="${BACKFILL_MEMORY_RESERVE_MB:-4096}"
BACKFILL_MEMORY_PER_WORKER_MB="${BACKFILL_MEMORY_PER_WORKER_MB:-2200}"
BACKFILL_NODE_MAX_OLD_SPACE_MB="${BACKFILL_NODE_MAX_OLD_SPACE_MB:-1024}"
DOWNLOAD_NODE_MAX_OLD_SPACE_MB="${DOWNLOAD_NODE_MAX_OLD_SPACE_MB:-$BACKFILL_NODE_MAX_OLD_SPACE_MB}"
ENRICH_NODE_MAX_OLD_SPACE_MB="${ENRICH_NODE_MAX_OLD_SPACE_MB:-$BACKFILL_NODE_MAX_OLD_SPACE_MB}"
BACKFILL_REPORT_INCLUDE_JOBS="${BACKFILL_REPORT_INCLUDE_JOBS:-0}"
THETA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"
FLOW_READ_BACKEND="${PHENIX_FLOW_READ_BACKEND:-clickhouse}"
FLOW_WRITE_BACKEND="${PHENIX_FLOW_WRITE_BACKEND:-clickhouse}"
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

if ! [[ "$ENRICH_CPU_TARGET_PCT" =~ ^[0-9]+$ ]] || (( ENRICH_CPU_TARGET_PCT < 10 )) || (( ENRICH_CPU_TARGET_PCT > 95 )); then
  echo "ENRICH_CPU_TARGET_PCT must be an integer in [10,95] (got: $ENRICH_CPU_TARGET_PCT)"
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

if ! [[ "$DOWNLOAD_NODE_MAX_OLD_SPACE_MB" =~ ^[0-9]+$ ]] || (( DOWNLOAD_NODE_MAX_OLD_SPACE_MB < 256 )); then
  echo "DOWNLOAD_NODE_MAX_OLD_SPACE_MB must be an integer >= 256 (got: $DOWNLOAD_NODE_MAX_OLD_SPACE_MB)"
  exit 1
fi

if ! [[ "$ENRICH_NODE_MAX_OLD_SPACE_MB" =~ ^[0-9]+$ ]] || (( ENRICH_NODE_MAX_OLD_SPACE_MB < 256 )); then
  echo "ENRICH_NODE_MAX_OLD_SPACE_MB must be an integer >= 256 (got: $ENRICH_NODE_MAX_OLD_SPACE_MB)"
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

DEFAULT_DOWNLOAD_WORKERS=2

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

if (( ENRICH_WORKERS > MAX_WORKERS )); then
  echo "Capping ENRICH_WORKERS from $ENRICH_WORKERS to $MAX_WORKERS"
  ENRICH_WORKERS="$MAX_WORKERS"
fi

if (( DOWNLOAD_WORKERS > MAX_WORKERS )); then
  echo "Capping DOWNLOAD_WORKERS from $DOWNLOAD_WORKERS to $MAX_WORKERS"
  DOWNLOAD_WORKERS="$MAX_WORKERS"
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

if (( ENRICH_WORKERS > MEMORY_WORKER_CAP )); then
  echo "Capping ENRICH_WORKERS from $ENRICH_WORKERS to memory cap $MEMORY_WORKER_CAP"
  ENRICH_WORKERS="$MEMORY_WORKER_CAP"
fi

if (( DOWNLOAD_WORKERS > MEMORY_WORKER_CAP )); then
  echo "Capping DOWNLOAD_WORKERS from $DOWNLOAD_WORKERS to memory cap $MEMORY_WORKER_CAP"
  DOWNLOAD_WORKERS="$MEMORY_WORKER_CAP"
fi

if (( PIPELINE_STAGE_OVERLAP == 1 )); then
  TOTAL_PARALLEL_WORKERS=$(( ENRICH_WORKERS + DOWNLOAD_WORKERS ))
  if (( TOTAL_PARALLEL_WORKERS > MEMORY_WORKER_CAP )); then
    if (( MEMORY_WORKER_CAP < 2 )); then
      echo "Disabling PIPELINE_STAGE_OVERLAP because memory cap is $MEMORY_WORKER_CAP worker."
      PIPELINE_STAGE_OVERLAP=0
    else
      NEW_DOWNLOAD_WORKERS=$(( (DOWNLOAD_WORKERS * MEMORY_WORKER_CAP) / TOTAL_PARALLEL_WORKERS ))
      if (( NEW_DOWNLOAD_WORKERS < 1 )); then
        NEW_DOWNLOAD_WORKERS=1
      fi
      NEW_ENRICH_WORKERS=$(( MEMORY_WORKER_CAP - NEW_DOWNLOAD_WORKERS ))
      if (( NEW_ENRICH_WORKERS < 1 )); then
        NEW_ENRICH_WORKERS=1
        NEW_DOWNLOAD_WORKERS=$(( MEMORY_WORKER_CAP - 1 ))
      fi
      echo "Capping overlapping workers to memory budget: download ${DOWNLOAD_WORKERS}->${NEW_DOWNLOAD_WORKERS}, enrich ${ENRICH_WORKERS}->${NEW_ENRICH_WORKERS}"
      DOWNLOAD_WORKERS="$NEW_DOWNLOAD_WORKERS"
      ENRICH_WORKERS="$NEW_ENRICH_WORKERS"
    fi
  fi
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
echo "Stage overlap: $([[ "$PIPELINE_STAGE_OVERLAP" == "1" ]] && echo "enabled" || echo "disabled")"
if [[ "$TOTAL_MEMORY_MB" =~ ^[0-9]+$ ]] && (( TOTAL_MEMORY_MB > 0 )); then
  echo "Total memory detected: ${TOTAL_MEMORY_MB}MB"
else
  echo "Total memory detected: unknown"
fi
echo "Memory reserve: ${BACKFILL_MEMORY_RESERVE_MB}MB"
echo "Memory per worker target: ${BACKFILL_MEMORY_PER_WORKER_MB}MB"
echo "Memory worker cap: $MEMORY_WORKER_CAP"
echo "Node heap cap (download): ${DOWNLOAD_NODE_MAX_OLD_SPACE_MB}MB"
echo "Node heap cap (enrich): ${ENRICH_NODE_MAX_OLD_SPACE_MB}MB"
echo "Detailed job reports: $BACKFILL_REPORT_INCLUDE_JOBS"
echo "Theta base URL: $THETA_BASE_URL"
echo "Flow backends: read=$FLOW_READ_BACKEND write=$FLOW_WRITE_BACKEND"
echo "Reports: $RUN_DIR"
if [[ -n "$INPUT_PATH" ]]; then
  echo "Input: $INPUT_PATH"
fi

launch_enrich_workers() {
  enrich_pids=()
  for idx in $(seq 0 $((ENRICH_WORKERS - 1))); do
    report_path="$ENRICH_DIR/worker${idx}.json"
    log_path="$ENRICH_DIR/worker${idx}.log"
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
      node scripts/backfill/backfill-clickhouse-historical-days.js
    ) > "$log_path" 2>&1 &
    enrich_pids+=("$!")
  done
}

launch_download_workers() {
  download_pids=()
  for idx in $(seq 0 $((DOWNLOAD_WORKERS - 1))); do
    report_path="$DOWNLOAD_DIR/worker${idx}.json"
    log_path="$DOWNLOAD_DIR/worker${idx}.log"
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
      node scripts/backfill/backfill-clickhouse-historical-days.js
    ) > "$log_path" 2>&1 &
    download_pids+=("$!")
  done
}

download_fail=0
enrich_fail=0

if (( PIPELINE_STAGE_OVERLAP == 1 )); then
  launch_enrich_workers
  launch_download_workers

  for pid in "${download_pids[@]}"; do
    if ! wait "$pid"; then
      download_fail=1
    fi
  done
  touch "$DOWNLOAD_DONE_FLAG"

  for pid in "${enrich_pids[@]}"; do
    if ! wait "$pid"; then
      enrich_fail=1
    fi
  done
else
  launch_download_workers
  for pid in "${download_pids[@]}"; do
    if ! wait "$pid"; then
      download_fail=1
    fi
  done
  touch "$DOWNLOAD_DONE_FLAG"

  if (( download_fail == 0 )); then
    launch_enrich_workers
    for pid in "${enrich_pids[@]}"; do
      if ! wait "$pid"; then
        enrich_fail=1
      fi
    done
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
