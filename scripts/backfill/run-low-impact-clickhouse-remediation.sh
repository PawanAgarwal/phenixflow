#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_ROOT"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="${LOW_IMPACT_REMEDIATION_RUN_ROOT:-$PROJECT_ROOT/artifacts/reports/clickhouse-remediation-$TS}"

THETA_WORKERS="${LOW_IMPACT_THETA_WORKERS:-2}"
THETA_CONNECTIONS="${LOW_IMPACT_THETA_CONNECTIONS:-2}"
WORKER_HEAP_MB="${LOW_IMPACT_NODE_HEAP_MB:-1024}"
WORKER_OVERHEAD_MB="${LOW_IMPACT_WORKER_OVERHEAD_MB:-384}"
RAM_BUDGET_MB="${LOW_IMPACT_RAM_BUDGET_MB:-3072}"
CPU_TARGET_PCT="${LOW_IMPACT_CPU_TARGET_PCT:-20}"
CALC_GREEKS_WORKERS="${LOW_IMPACT_CALC_GREEKS_WORKERS:-1}"

mkdir -p "$RUN_ROOT"

run_low_priority() {
  if command -v taskpolicy >/dev/null 2>&1; then
    taskpolicy -b nice -n 20 "$@"
  else
    nice -n 20 "$@"
  fi
}

deprioritize_clickhouse() {
  local pids
  pids="$(pgrep -f "clickhouse server --config-file" || true)"
  if [[ -z "$pids" ]]; then
    return 0
  fi

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    renice 10 -p "$pid" >/dev/null 2>&1 || true
    if command -v taskpolicy >/dev/null 2>&1; then
      taskpolicy -b -p "$pid" >/dev/null 2>&1 || true
    fi
  done <<< "$pids"
}

write_tsv() {
  local query="$1"
  local output_path="$2"
  clickhouse client --host 127.0.0.1 --port 9000 --query "$query" > "$output_path"
}

count_lines() {
  local file_path="$1"
  awk 'NF > 0 { count += 1 } END { print count + 0 }' "$file_path"
}

echo "Low-impact remediation run root: $RUN_ROOT"
echo "Theta workers: $THETA_WORKERS"
echo "Theta max connections: $THETA_CONNECTIONS"
echo "Node heap MB: $WORKER_HEAP_MB"
echo "Worker overhead MB: $WORKER_OVERHEAD_MB"
echo "Worker RAM budget MB: $RAM_BUDGET_MB"
echo "CPU target pct: $CPU_TARGET_PCT"
echo "Calculated greeks workers: $CALC_GREEKS_WORKERS"

deprioritize_clickhouse

RAW_STOCK_GAPS_TSV="$RUN_ROOT/raw-stock-gaps.tsv"
ENRICH_GAPS_TSV="$RUN_ROOT/enrich-gaps.tsv"
CALC_GREEKS_GAPS_TSV="$RUN_ROOT/calc-greeks-gaps.tsv"

write_tsv "
  SELECT
    toString(trade_date_utc),
    symbol
  FROM options.option_download_chunk_status FINAL
  WHERE stream_name = 'stock_price_1m'
    AND status = 'missing'
  GROUP BY trade_date_utc, symbol
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$RAW_STOCK_GAPS_TSV"

write_tsv "
  SELECT
    toString(trade_date_utc),
    symbol
  FROM options.option_enrich_chunk_status FINAL
  WHERE status IN ('partial', 'missing')
  GROUP BY trade_date_utc, symbol
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$ENRICH_GAPS_TSV"

write_tsv "
  SELECT
    symbol,
    toString(trade_date_utc)
  FROM options.option_trade_day_cache
  WHERE trade_date_utc >= toDate('2025-01-01')
    AND row_count > 0
    AND cache_status = 'full'
    AND (symbol, trade_date_utc) NOT IN (
      SELECT symbol, trade_date_utc
      FROM options.option_calculated_greeks_day_status FINAL
      WHERE status = 'complete'
    )
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$CALC_GREEKS_GAPS_TSV"

RAW_STOCK_GAP_COUNT="$(count_lines "$RAW_STOCK_GAPS_TSV")"
ENRICH_GAP_COUNT="$(count_lines "$ENRICH_GAPS_TSV")"
CALC_GREEKS_GAP_COUNT="$(count_lines "$CALC_GREEKS_GAPS_TSV")"

cat > "$RUN_ROOT/summary.txt" <<EOF
run_root=$RUN_ROOT
raw_stock_gap_count=$RAW_STOCK_GAP_COUNT
enrich_gap_count=$ENRICH_GAP_COUNT
calc_greeks_gap_count=$CALC_GREEKS_GAP_COUNT
theta_workers=$THETA_WORKERS
theta_connections=$THETA_CONNECTIONS
node_heap_mb=$WORKER_HEAP_MB
worker_overhead_mb=$WORKER_OVERHEAD_MB
ram_budget_mb=$RAM_BUDGET_MB
cpu_target_pct=$CPU_TARGET_PCT
calc_greeks_workers=$CALC_GREEKS_WORKERS
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EOF

echo "Gap lists written:"
echo "  raw stock gaps: $RAW_STOCK_GAPS_TSV ($RAW_STOCK_GAP_COUNT symbol-days)"
echo "  enrich gaps: $ENRICH_GAPS_TSV ($ENRICH_GAP_COUNT symbol-days)"
echo "  calc greeks gaps: $CALC_GREEKS_GAPS_TSV ($CALC_GREEKS_GAP_COUNT symbol-days)"

if (( RAW_STOCK_GAP_COUNT > 0 )); then
  echo "Starting raw stock remediation..."
  run_low_priority env \
    BACKFILL_MODE=download \
    BACKFILL_FORCE=1 \
    BACKFILL_RAW_COMPONENTS=stock \
    BACKFILL_GAP_TELEMETRY=1 \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$RAW_STOCK_GAPS_TSV" \
    BACKFILL_WORKERS="$THETA_WORKERS" \
    BACKFILL_DOWNLOAD_WORKER_GUARD=0 \
    THETADATA_MAX_CONCURRENT_CONNECTIONS="$THETA_CONNECTIONS" \
    THETADATA_DOWNLOAD_CONCURRENCY="$THETA_CONNECTIONS" \
    BACKFILL_RAM_BUDGET_MB="$RAM_BUDGET_MB" \
    BACKFILL_NODE_MAX_OLD_SPACE_MB="$WORKER_HEAP_MB" \
    BACKFILL_WORKER_OVERHEAD_MB="$WORKER_OVERHEAD_MB" \
    BACKFILL_CPU_TARGET_PCT="$CPU_TARGET_PCT" \
    CLICKHOUSE_DELETE_MUTATION_SYNC=1 \
    THETADATA_HISTORICAL_OPTION_FORMAT=ndjson \
    THETADATA_OPTION_QUOTE_FORMAT=ndjson \
    THETADATA_STREAM_HEARTBEAT_EVERY_ROWS=250000 \
    THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
    bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh \
    > "$RUN_ROOT/raw-stock-remediation.log" 2>&1
fi

if (( ENRICH_GAP_COUNT > 0 )); then
  echo "Starting enrich remediation..."
  run_low_priority env \
    BACKFILL_MODE=enrich \
    BACKFILL_FORCE=1 \
    BACKFILL_LOOP_UNTIL_READY=1 \
    BACKFILL_LOOP_MAX_PASSES=200 \
    BACKFILL_LOOP_SLEEP_MS=5000 \
    BACKFILL_GAP_TELEMETRY=1 \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$ENRICH_GAPS_TSV" \
    BACKFILL_WORKERS="$THETA_WORKERS" \
    BACKFILL_RAM_BUDGET_MB="$RAM_BUDGET_MB" \
    BACKFILL_NODE_MAX_OLD_SPACE_MB="$WORKER_HEAP_MB" \
    BACKFILL_WORKER_OVERHEAD_MB="$WORKER_OVERHEAD_MB" \
    BACKFILL_CPU_TARGET_PCT="$CPU_TARGET_PCT" \
    CLICKHOUSE_ENRICH_STREAM_READ=1 \
    CLICKHOUSE_ENRICH_STREAM_WRITE=1 \
    CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE=5000 \
    CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES=10 \
    CLICKHOUSE_ENRICH_GREEKS_SOURCE=calculated_first \
    bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh \
    > "$RUN_ROOT/enrich-remediation.log" 2>&1
fi

if (( CALC_GREEKS_GAP_COUNT > 0 )); then
  echo "Starting calculated-greeks remediation..."
  run_low_priority env \
    CALC_GREEKS_WORKERS="$CALC_GREEKS_WORKERS" \
    CALC_GREEKS_SYMBOL_DAY_LIST_PATH="$CALC_GREEKS_GAPS_TSV" \
    CALC_GREEKS_START_DATE=2025-01-01 \
    CALC_GREEKS_END_DATE=2026-03-13 \
    bash scripts/clickhouse/backfill-calculated-greeks-parallel.sh \
    > "$RUN_ROOT/calc-greeks-remediation.log" 2>&1
fi

echo "Low-impact remediation finished."
echo "Summary: $RUN_ROOT/summary.txt"
