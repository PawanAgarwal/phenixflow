#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

RUNTIME_ROOT="${PHENIXFLOW_RUNTIME_ROOT:-$HOME/Library/Caches/phenixflow}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="${AUDITED_GAP_PHASE2_RUN_ROOT:-$RUNTIME_ROOT/reports/audited-gap-phase2-$TS}"
REPORT_ROOT="${AUDITED_GAP_PHASE2_REPORT_ROOT:-$RUNTIME_ROOT/reports}"
DELETE_SYMBOL_DAYS_SCRIPT="$PROJECT_ROOT/scripts/clickhouse/delete-clickhouse-symbol-days.sh"
WAIT_FOR_MUTATIONS_SCRIPT="$PROJECT_ROOT/scripts/clickhouse/wait-for-clickhouse-mutations.sh"

PRIORITY_CALC_WORKERS="${PRIORITY_CALC_WORKERS:-16}"
BACKLOG_CALC_WORKERS="${BACKLOG_CALC_WORKERS:-8}"
PRIORITY_ENRICH_WORKERS="${PRIORITY_ENRICH_WORKERS:-16}"
PRIORITY_CALC_QUERY_MAX_THREADS="${PRIORITY_CALC_QUERY_MAX_THREADS:-2}"
PRIORITY_CALC_QUERY_MAX_MEMORY_BYTES="${PRIORITY_CALC_QUERY_MAX_MEMORY_BYTES:-2147483648}"
BACKLOG_CALC_QUERY_MAX_THREADS="${BACKLOG_CALC_QUERY_MAX_THREADS:-2}"
BACKLOG_CALC_QUERY_MAX_MEMORY_BYTES="${BACKLOG_CALC_QUERY_MAX_MEMORY_BYTES:-2147483648}"
CLICKHOUSE_MUTATION_TIMEOUT_SEC="${CLICKHOUSE_MUTATION_TIMEOUT_SEC:-7200}"
CLICKHOUSE_MUTATION_POLL_MS="${CLICKHOUSE_MUTATION_POLL_MS:-2000}"
RUN_BACKLOG_CALC="${RUN_BACKLOG_CALC:-1}"

mkdir -p "$RUN_ROOT" "$REPORT_ROOT"

if [[ ! -x "$DELETE_SYMBOL_DAYS_SCRIPT" ]]; then
  chmod +x "$DELETE_SYMBOL_DAYS_SCRIPT"
fi
if [[ ! -x "$WAIT_FOR_MUTATIONS_SCRIPT" ]]; then
  chmod +x "$WAIT_FOR_MUTATIONS_SCRIPT"
fi

echo "Phase 2 run root: $RUN_ROOT"
echo "Runtime report root: $REPORT_ROOT"
echo "Priority calc workers: $PRIORITY_CALC_WORKERS"
echo "Priority enrich workers: $PRIORITY_ENRICH_WORKERS"
echo "Backlog calc workers: $BACKLOG_CALC_WORKERS"

write_query() {
  local query="$1"
  local output_path="$2"
  clickhouse client --host 127.0.0.1 --port 9000 --query "$query" > "$output_path"
}

count_lines() {
  awk 'NF > 0 { count += 1 } END { print count + 0 }' "$1"
}

ENRICH_DAYS_SQL="toDate('2025-03-13'),toDate('2025-03-14'),toDate('2025-03-17'),toDate('2025-03-18'),toDate('2025-03-19'),toDate('2025-03-20'),toDate('2025-03-21'),toDate('2025-03-24'),toDate('2025-03-25'),toDate('2025-03-26'),toDate('2025-03-27'),toDate('2025-03-28'),toDate('2025-03-31'),toDate('2025-04-01'),toDate('2025-04-02'),toDate('2025-04-03'),toDate('2025-04-04'),toDate('2025-04-07'),toDate('2025-04-08'),toDate('2025-04-09'),toDate('2025-04-10'),toDate('2025-04-11'),toDate('2025-04-14'),toDate('2025-04-15'),toDate('2025-04-16'),toDate('2025-04-17'),toDate('2025-04-21'),toDate('2025-04-22'),toDate('2025-04-23'),toDate('2025-04-24'),toDate('2025-04-25'),toDate('2025-04-28'),toDate('2025-04-29'),toDate('2025-04-30'),toDate('2025-05-01'),toDate('2025-05-02'),toDate('2025-05-05'),toDate('2025-05-06'),toDate('2025-05-07'),toDate('2025-05-08'),toDate('2025-05-09'),toDate('2025-05-12'),toDate('2025-05-13'),toDate('2025-05-14'),toDate('2025-05-15'),toDate('2025-05-16'),toDate('2025-05-19'),toDate('2025-05-20'),toDate('2025-05-21'),toDate('2025-05-22'),toDate('2025-05-23'),toDate('2025-05-27'),toDate('2025-05-28'),toDate('2025-05-29'),toDate('2025-05-30'),toDate('2025-06-02'),toDate('2025-06-03'),toDate('2025-06-04'),toDate('2025-06-05'),toDate('2025-06-06'),toDate('2025-06-09'),toDate('2025-06-10'),toDate('2025-06-11'),toDate('2025-06-12'),toDate('2025-06-13'),toDate('2025-06-16'),toDate('2025-06-17'),toDate('2025-06-18'),toDate('2025-06-20'),toDate('2025-06-23'),toDate('2025-06-24'),toDate('2025-06-25'),toDate('2025-06-26'),toDate('2025-06-27'),toDate('2025-06-30'),toDate('2025-07-01'),toDate('2025-07-02'),toDate('2025-07-03'),toDate('2025-07-07'),toDate('2025-07-08'),toDate('2025-07-09'),toDate('2025-07-10'),toDate('2025-07-11'),toDate('2025-07-14'),toDate('2025-07-15'),toDate('2025-07-16'),toDate('2025-07-17'),toDate('2025-07-18'),toDate('2025-07-21'),toDate('2025-07-22'),toDate('2025-07-23'),toDate('2025-07-24'),toDate('2025-07-25'),toDate('2025-07-28'),toDate('2025-07-29'),toDate('2025-07-30'),toDate('2025-07-31'),toDate('2025-10-22'),toDate('2026-01-09'),toDate('2026-01-12'),toDate('2026-01-13'),toDate('2026-01-14'),toDate('2026-01-15'),toDate('2026-01-16'),toDate('2026-01-20'),toDate('2026-01-21'),toDate('2026-01-22'),toDate('2026-01-23'),toDate('2026-01-26'),toDate('2026-01-27'),toDate('2026-01-28'),toDate('2026-01-29'),toDate('2026-01-30'),toDate('2026-02-02'),toDate('2026-02-03'),toDate('2026-02-04'),toDate('2026-02-05'),toDate('2026-02-06'),toDate('2026-02-09'),toDate('2026-02-10'),toDate('2026-02-11'),toDate('2026-02-12'),toDate('2026-02-13'),toDate('2026-02-17'),toDate('2026-02-18'),toDate('2026-03-16'),toDate('2026-03-17'),toDate('2026-03-18'),toDate('2026-03-19'),toDate('2026-03-20')"

PRIORITY_CALC_TSV="$RUN_ROOT/calc-greeks-priority.tsv"
BACKLOG_CALC_TSV="$RUN_ROOT/calc-greeks-backlog.tsv"
ENRICH_TSV="$RUN_ROOT/enrich-gaps.tsv"

echo "Generating audited-day priority greeks list..."
write_query "
  WITH days AS (SELECT arrayJoin([$ENRICH_DAYS_SQL]) AS day)
  SELECT
    symbol,
    toString(trade_date_utc)
  FROM options.option_trade_day_cache
  WHERE trade_date_utc IN (SELECT day FROM days)
    AND row_count > 0
    AND cache_status = 'full'
    AND (symbol, trade_date_utc) NOT IN (
      SELECT symbol, trade_date_utc
      FROM options.option_calculated_greeks_day_status FINAL
      WHERE status = 'complete'
    )
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$PRIORITY_CALC_TSV"

echo "Generating deferred greeks backlog list..."
write_query "
  WITH days AS (SELECT arrayJoin([$ENRICH_DAYS_SQL]) AS day)
  SELECT
    symbol,
    toString(trade_date_utc)
  FROM options.option_trade_day_cache
  WHERE trade_date_utc >= toDate('2025-01-02')
    AND trade_date_utc <= toDate('2026-03-20')
    AND trade_date_utc NOT IN (SELECT day FROM days)
    AND row_count > 0
    AND cache_status = 'full'
    AND (symbol, trade_date_utc) NOT IN (
      SELECT symbol, trade_date_utc
      FROM options.option_calculated_greeks_day_status FINAL
      WHERE status = 'complete'
    )
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$BACKLOG_CALC_TSV"

echo "Generating enrichment gap list..."
write_query "
  WITH
    days AS (SELECT arrayJoin([$ENRICH_DAYS_SQL]) AS day),
    expected AS (
      SELECT symbol, trade_date AS day, max(toStartOfMinute(trade_ts_utc)) AS trade_max_minute
      FROM options.option_trades
      WHERE trade_date IN (SELECT day FROM days)
      GROUP BY symbol, trade_date
    ),
    enriched AS (
      SELECT symbol, trade_date AS day, max(toStartOfMinute(trade_ts_utc)) AS enriched_max_minute
      FROM options.option_trade_enriched
      WHERE trade_date IN (SELECT day FROM days)
      GROUP BY symbol, trade_date
    ),
    chunk_gaps AS (
      SELECT symbol, trade_date_utc AS day
      FROM options.option_enrich_chunk_status FINAL
      WHERE status IN ('partial', 'missing')
        AND trade_date_utc IN (SELECT day FROM days)
      GROUP BY symbol, trade_date_utc
    )
  SELECT toString(day), symbol
  FROM (
    SELECT e.day, e.symbol
    FROM expected e
    LEFT JOIN enriched r USING (symbol, day)
    WHERE r.symbol IS NULL OR r.enriched_max_minute < e.trade_max_minute
    UNION DISTINCT
    SELECT day, symbol
    FROM chunk_gaps
  )
  ORDER BY day, symbol
  FORMAT TabSeparated
" "$ENRICH_TSV"

echo "Priority calc jobs: $(count_lines "$PRIORITY_CALC_TSV")"
echo "Backlog calc jobs: $(count_lines "$BACKLOG_CALC_TSV")"
echo "Enrichment jobs: $(count_lines "$ENRICH_TSV")"

if [[ -s "$ENRICH_TSV" ]]; then
  echo "Pre-clearing option_trade_enriched for targeted symbol-days..."
  CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
    bash "$DELETE_SYMBOL_DAYS_SCRIPT" option_trade_enriched trade_date "$ENRICH_TSV" \
    > "$RUN_ROOT/enrich-preclear.log" 2>&1
  CLICKHOUSE_MUTATION_TIMEOUT_SEC="$CLICKHOUSE_MUTATION_TIMEOUT_SEC" \
  CLICKHOUSE_MUTATION_POLL_MS="$CLICKHOUSE_MUTATION_POLL_MS" \
    bash "$WAIT_FOR_MUTATIONS_SCRIPT" option_trade_enriched \
    > "$RUN_ROOT/enrich-preclear-wait.log" 2>&1
fi

priority_calc_pid=""
if [[ -s "$PRIORITY_CALC_TSV" ]]; then
  echo "Starting priority calculated-greeks backfill in background..."
  (
    cd "$PROJECT_ROOT"
    env \
      NODE_OPTIONS="--max-old-space-size=1024" \
      CALC_GREEKS_WORKERS="$PRIORITY_CALC_WORKERS" \
      CALC_GREEKS_REPORT_DIR="$REPORT_ROOT" \
      CALC_GREEKS_SYMBOL_DAY_LIST_PATH="$PRIORITY_CALC_TSV" \
      CALC_GREEKS_SKIP_COMPLETED=0 \
      CALC_GREEKS_START_DATE=2025-01-02 \
      CALC_GREEKS_END_DATE=2026-03-20 \
      CALC_GREEKS_SOURCE=quote_minute \
      CALC_GREEKS_QUERY_MAX_THREADS="$PRIORITY_CALC_QUERY_MAX_THREADS" \
      CALC_GREEKS_QUERY_MAX_MEMORY_BYTES="$PRIORITY_CALC_QUERY_MAX_MEMORY_BYTES" \
      bash scripts/clickhouse/backfill-calculated-greeks-parallel.sh
  ) > "$RUN_ROOT/calc-priority.log" 2>&1 &
  priority_calc_pid="$!"
  echo "Priority calc pid: $priority_calc_pid"
fi

if [[ -s "$ENRICH_TSV" ]]; then
  echo "Starting enrichment with loop-until-ready..."
  env \
    NODE_OPTIONS="--max-old-space-size=768" \
    BACKFILL_MODE=enrich \
    BACKFILL_FORCE=1 \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$ENRICH_TSV" \
    BACKFILL_REPORT_DIR="$REPORT_ROOT" \
    BACKFILL_WORKERS="$PRIORITY_ENRICH_WORKERS" \
    BACKFILL_MAX_WORKERS=20 \
    BACKFILL_CPU_TARGET_PCT=90 \
    BACKFILL_MEMORY_RESERVE_MB=3072 \
    BACKFILL_MEMORY_PER_WORKER_MB=1024 \
    BACKFILL_RAM_BUDGET_MB=20480 \
    BACKFILL_NODE_MAX_OLD_SPACE_MB=768 \
    BACKFILL_WORKER_OVERHEAD_MB=256 \
    BACKFILL_SHARD_STRATEGY=balanced \
    BACKFILL_LOOP_UNTIL_READY=1 \
    BACKFILL_LOOP_MAX_PASSES=2000 \
    BACKFILL_LOOP_SLEEP_MS=5000 \
    CLICKHOUSE_ENRICH_STREAM_READ=1 \
    CLICKHOUSE_ENRICH_STREAM_WRITE=1 \
    CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE=5000 \
    CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES=10 \
    CLICKHOUSE_ENRICH_GREEKS_SOURCE=calculated \
    CLICKHOUSE_ENRICH_SKIP_DELETE=1 \
    bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh \
    > "$RUN_ROOT/enrich-priority.log" 2>&1
fi

if [[ -n "$priority_calc_pid" ]]; then
  echo "Waiting for priority calculated-greeks background job..."
  wait "$priority_calc_pid"
fi

if [[ "$RUN_BACKLOG_CALC" != "0" ]] && [[ "$RUN_BACKLOG_CALC" != "false" ]] && [[ -s "$BACKLOG_CALC_TSV" ]]; then
  echo "Starting deferred calculated-greeks backlog..."
  env \
    NODE_OPTIONS="--max-old-space-size=1024" \
    CALC_GREEKS_WORKERS="$BACKLOG_CALC_WORKERS" \
    CALC_GREEKS_REPORT_DIR="$REPORT_ROOT" \
    CALC_GREEKS_SYMBOL_DAY_LIST_PATH="$BACKLOG_CALC_TSV" \
    CALC_GREEKS_SKIP_COMPLETED=0 \
    CALC_GREEKS_START_DATE=2025-01-02 \
    CALC_GREEKS_END_DATE=2026-03-20 \
    CALC_GREEKS_SOURCE=quote_minute \
    CALC_GREEKS_QUERY_MAX_THREADS="$BACKLOG_CALC_QUERY_MAX_THREADS" \
    CALC_GREEKS_QUERY_MAX_MEMORY_BYTES="$BACKLOG_CALC_QUERY_MAX_MEMORY_BYTES" \
    bash scripts/clickhouse/backfill-calculated-greeks-parallel.sh \
    > "$RUN_ROOT/calc-backlog.log" 2>&1
fi

echo "Accelerated phase 2 finished."
echo "Artifacts: $RUN_ROOT"
