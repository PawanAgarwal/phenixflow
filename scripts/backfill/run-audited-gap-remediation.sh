#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

export THETADATA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"

TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="${AUDITED_GAP_RUN_ROOT:-$PROJECT_ROOT/artifacts/reports/audited-gap-remediation-$TS}"
DELETE_SYMBOL_DAYS_SCRIPT="$PROJECT_ROOT/scripts/clickhouse/delete-clickhouse-symbol-days.sh"
WAIT_FOR_MUTATIONS_SCRIPT="$PROJECT_ROOT/scripts/clickhouse/wait-for-clickhouse-mutations.sh"

TRADE_WORKERS="${TRADE_WORKERS:-6}"
QUOTE_WORKERS="${QUOTE_WORKERS:-8}"
STOCK_WORKERS="${STOCK_WORKERS:-6}"
OI_WORKERS="${OI_WORKERS:-6}"
ENRICH_WORKERS="${ENRICH_WORKERS:-10}"
CALC_GREEKS_WORKERS="${CALC_GREEKS_WORKERS:-10}"

RAW_CONNECTIONS="${RAW_CONNECTIONS:-8}"
BACKFILL_NODE_MAX_OLD_SPACE_MB="${BACKFILL_NODE_MAX_OLD_SPACE_MB:-1024}"
BACKFILL_WORKER_OVERHEAD_MB="${BACKFILL_WORKER_OVERHEAD_MB:-512}"
BACKFILL_RAM_BUDGET_MB="${BACKFILL_RAM_BUDGET_MB:-16384}"
BACKFILL_MEMORY_RESERVE_MB="${BACKFILL_MEMORY_RESERVE_MB:-4096}"
BACKFILL_MEMORY_PER_WORKER_MB="${BACKFILL_MEMORY_PER_WORKER_MB:-1536}"
BACKFILL_CPU_TARGET_PCT="${BACKFILL_CPU_TARGET_PCT:-85}"
CALC_GREEKS_QUERY_MAX_THREADS="${CALC_GREEKS_QUERY_MAX_THREADS:-1}"
CALC_GREEKS_QUERY_MAX_MEMORY_BYTES="${CALC_GREEKS_QUERY_MAX_MEMORY_BYTES:-1610612736}"
CLICKHOUSE_MUTATION_TIMEOUT_SEC="${CLICKHOUSE_MUTATION_TIMEOUT_SEC:-7200}"
CLICKHOUSE_MUTATION_POLL_MS="${CLICKHOUSE_MUTATION_POLL_MS:-2000}"

mkdir -p "$RUN_ROOT"

if [[ ! -d "$PROJECT_ROOT/node_modules" ]]; then
  echo "Installing npm dependencies..."
  npm ci
fi

if [[ ! -x "$DELETE_SYMBOL_DAYS_SCRIPT" ]]; then
  chmod +x "$DELETE_SYMBOL_DAYS_SCRIPT"
fi
if [[ ! -x "$WAIT_FOR_MUTATIONS_SCRIPT" ]]; then
  chmod +x "$WAIT_FOR_MUTATIONS_SCRIPT"
fi

echo "Run root: $RUN_ROOT"
echo "Theta base URL: $THETADATA_BASE_URL"
echo "Workers trade/quote/stock/oi/enrich/calc: $TRADE_WORKERS/$QUOTE_WORKERS/$STOCK_WORKERS/$OI_WORKERS/$ENRICH_WORKERS/$CALC_GREEKS_WORKERS"

write_query() {
  local query="$1"
  local output_path="$2"
  clickhouse client --host 127.0.0.1 --port 9000 --query "$query" > "$output_path"
}

count_lines() {
  awk 'NF > 0 { count += 1 } END { print count + 0 }' "$1"
}

run_backfill_parallel() {
  local mode="$1"
  local components="$2"
  local workers="$3"
  local input_path="$4"
  local log_path="$5"
  shift 5

  env \
    BACKFILL_MODE="$mode" \
    BACKFILL_FORCE=1 \
    BACKFILL_RAW_COMPONENTS="$components" \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$input_path" \
    BACKFILL_WORKERS="$workers" \
    BACKFILL_MAX_WORKERS=12 \
    BACKFILL_CPU_TARGET_PCT="$BACKFILL_CPU_TARGET_PCT" \
    BACKFILL_MEMORY_RESERVE_MB="$BACKFILL_MEMORY_RESERVE_MB" \
    BACKFILL_MEMORY_PER_WORKER_MB="$BACKFILL_MEMORY_PER_WORKER_MB" \
    BACKFILL_RAM_BUDGET_MB="$BACKFILL_RAM_BUDGET_MB" \
    BACKFILL_NODE_MAX_OLD_SPACE_MB="$BACKFILL_NODE_MAX_OLD_SPACE_MB" \
    BACKFILL_WORKER_OVERHEAD_MB="$BACKFILL_WORKER_OVERHEAD_MB" \
    BACKFILL_DOWNLOAD_WORKER_GUARD=0 \
    BACKFILL_SHARD_STRATEGY=balanced \
    THETADATA_MAX_CONCURRENT_CONNECTIONS="$RAW_CONNECTIONS" \
    THETADATA_DOWNLOAD_CONCURRENCY="$RAW_CONNECTIONS" \
    THETADATA_HISTORICAL_OPTION_FORMAT=ndjson \
    THETADATA_OPTION_QUOTE_FORMAT=ndjson \
    THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
    THETADATA_STREAM_HEARTBEAT_EVERY_ROWS=250000 \
    CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
    "$@" \
    bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh \
    > "$log_path" 2>&1
}

RAW_DAYS_SQL="toDate('2025-01-09'),toDate('2025-04-07'),toDate('2025-07-30'),toDate('2025-10-22'),toDate('2026-02-06'),toDate('2026-02-09'),toDate('2026-02-10'),toDate('2026-02-11'),toDate('2026-02-12'),toDate('2026-02-13'),toDate('2026-02-17'),toDate('2026-02-18'),toDate('2026-03-10'),toDate('2026-03-11'),toDate('2026-03-12'),toDate('2026-03-13'),toDate('2026-03-16'),toDate('2026-03-17'),toDate('2026-03-18'),toDate('2026-03-19'),toDate('2026-03-20')"
OI_DAYS_SQL="toDate('2025-01-09'),toDate('2025-12-29'),toDate('2026-01-02'),toDate('2026-01-05'),toDate('2026-01-06'),toDate('2026-01-07'),toDate('2026-01-08'),toDate('2026-01-09'),toDate('2026-01-12'),toDate('2026-01-13'),toDate('2026-01-14'),toDate('2026-01-15'),toDate('2026-01-16'),toDate('2026-01-20'),toDate('2026-01-21'),toDate('2026-01-22'),toDate('2026-01-23'),toDate('2026-01-26'),toDate('2026-01-27'),toDate('2026-01-28'),toDate('2026-01-29'),toDate('2026-01-30'),toDate('2026-02-02'),toDate('2026-03-13'),toDate('2026-03-16'),toDate('2026-03-17'),toDate('2026-03-18'),toDate('2026-03-19'),toDate('2026-03-20')"
ENRICH_DAYS_SQL="toDate('2025-03-13'),toDate('2025-03-14'),toDate('2025-03-17'),toDate('2025-03-18'),toDate('2025-03-19'),toDate('2025-03-20'),toDate('2025-03-21'),toDate('2025-03-24'),toDate('2025-03-25'),toDate('2025-03-26'),toDate('2025-03-27'),toDate('2025-03-28'),toDate('2025-03-31'),toDate('2025-04-01'),toDate('2025-04-02'),toDate('2025-04-03'),toDate('2025-04-04'),toDate('2025-04-07'),toDate('2025-04-08'),toDate('2025-04-09'),toDate('2025-04-10'),toDate('2025-04-11'),toDate('2025-04-14'),toDate('2025-04-15'),toDate('2025-04-16'),toDate('2025-04-17'),toDate('2025-04-21'),toDate('2025-04-22'),toDate('2025-04-23'),toDate('2025-04-24'),toDate('2025-04-25'),toDate('2025-04-28'),toDate('2025-04-29'),toDate('2025-04-30'),toDate('2025-05-01'),toDate('2025-05-02'),toDate('2025-05-05'),toDate('2025-05-06'),toDate('2025-05-07'),toDate('2025-05-08'),toDate('2025-05-09'),toDate('2025-05-12'),toDate('2025-05-13'),toDate('2025-05-14'),toDate('2025-05-15'),toDate('2025-05-16'),toDate('2025-05-19'),toDate('2025-05-20'),toDate('2025-05-21'),toDate('2025-05-22'),toDate('2025-05-23'),toDate('2025-05-27'),toDate('2025-05-28'),toDate('2025-05-29'),toDate('2025-05-30'),toDate('2025-06-02'),toDate('2025-06-03'),toDate('2025-06-04'),toDate('2025-06-05'),toDate('2025-06-06'),toDate('2025-06-09'),toDate('2025-06-10'),toDate('2025-06-11'),toDate('2025-06-12'),toDate('2025-06-13'),toDate('2025-06-16'),toDate('2025-06-17'),toDate('2025-06-18'),toDate('2025-06-20'),toDate('2025-06-23'),toDate('2025-06-24'),toDate('2025-06-25'),toDate('2025-06-26'),toDate('2025-06-27'),toDate('2025-06-30'),toDate('2025-07-01'),toDate('2025-07-02'),toDate('2025-07-03'),toDate('2025-07-07'),toDate('2025-07-08'),toDate('2025-07-09'),toDate('2025-07-10'),toDate('2025-07-11'),toDate('2025-07-14'),toDate('2025-07-15'),toDate('2025-07-16'),toDate('2025-07-17'),toDate('2025-07-18'),toDate('2025-07-21'),toDate('2025-07-22'),toDate('2025-07-23'),toDate('2025-07-24'),toDate('2025-07-25'),toDate('2025-07-28'),toDate('2025-07-29'),toDate('2025-07-30'),toDate('2025-07-31'),toDate('2025-10-22'),toDate('2026-01-09'),toDate('2026-01-12'),toDate('2026-01-13'),toDate('2026-01-14'),toDate('2026-01-15'),toDate('2026-01-16'),toDate('2026-01-20'),toDate('2026-01-21'),toDate('2026-01-22'),toDate('2026-01-23'),toDate('2026-01-26'),toDate('2026-01-27'),toDate('2026-01-28'),toDate('2026-01-29'),toDate('2026-01-30'),toDate('2026-02-02'),toDate('2026-02-03'),toDate('2026-02-04'),toDate('2026-02-05'),toDate('2026-02-06'),toDate('2026-02-09'),toDate('2026-02-10'),toDate('2026-02-11'),toDate('2026-02-12'),toDate('2026-02-13'),toDate('2026-02-17'),toDate('2026-02-18'),toDate('2026-03-16'),toDate('2026-03-17'),toDate('2026-03-18'),toDate('2026-03-19'),toDate('2026-03-20')"

RAW_TRADE_TSV="$RUN_ROOT/raw-trade-gaps.tsv"
RAW_QUOTE_TSV="$RUN_ROOT/raw-quote-gaps.tsv"
RAW_STOCK_TSV="$RUN_ROOT/raw-stock-gaps.tsv"
RAW_OI_TSV="$RUN_ROOT/raw-oi-gaps.tsv"
CALC_GREEKS_TSV="$RUN_ROOT/calc-greeks-gaps.tsv"
ENRICH_TSV="$RUN_ROOT/enrich-gaps.tsv"

echo "Refreshing SOFR reference data..."
node scripts/clickhouse/sync-sofr-daily.js --refresh 1 --years 3 \
  > "$RUN_ROOT/sofr-sync.log" 2>&1

echo "Generating raw gap lists..."
write_query "
  WITH
    days AS (SELECT arrayJoin([$RAW_DAYS_SQL]) AS day),
    syms AS (
      SELECT arrayJoin(groupArray(symbol)) AS symbol
      FROM (
        SELECT symbol
        FROM options.option_trade_day_cache
        WHERE cache_status = 'full' AND row_count > 0
        GROUP BY symbol
        ORDER BY symbol
      )
    ),
    trade_actual AS (
      SELECT symbol, trade_date AS day, max(toStartOfMinute(trade_ts_utc)) AS max_minute
      FROM options.option_trades
      WHERE trade_date IN (SELECT day FROM days)
      GROUP BY symbol, trade_date
    )
  SELECT toString(day), symbol
  FROM days
  CROSS JOIN syms
  LEFT JOIN trade_actual t USING (symbol, day)
  WHERE t.symbol IS NULL
     OR t.max_minute < toDateTime64(concat(toString(day), ' 16:14:00'), 3, 'UTC')
  ORDER BY day, symbol
  FORMAT TabSeparated
" "$RAW_TRADE_TSV"

write_query "
  WITH
    days AS (SELECT arrayJoin([$RAW_DAYS_SQL]) AS day),
    syms AS (
      SELECT arrayJoin(groupArray(symbol)) AS symbol
      FROM (
        SELECT symbol
        FROM options.option_trade_day_cache
        WHERE cache_status = 'full' AND row_count > 0
        GROUP BY symbol
        ORDER BY symbol
      )
    ),
    quote_actual AS (
      SELECT symbol, trade_date_utc AS day, max(minute_bucket_utc) AS max_minute
      FROM options.option_quote_minute_raw
      WHERE trade_date_utc IN (SELECT day FROM days)
      GROUP BY symbol, trade_date_utc
    )
  SELECT toString(day), symbol
  FROM days
  CROSS JOIN syms
  LEFT JOIN quote_actual q USING (symbol, day)
  WHERE q.symbol IS NULL
     OR q.max_minute < toDateTime64(concat(toString(day), ' 15:59:00'), 3, 'UTC')
  ORDER BY day, symbol
  FORMAT TabSeparated
" "$RAW_QUOTE_TSV"

write_query "
  WITH
    days AS (SELECT arrayJoin([$RAW_DAYS_SQL]) AS day),
    syms AS (
      SELECT arrayJoin(groupArray(symbol)) AS symbol
      FROM (
        SELECT symbol
        FROM options.option_trade_day_cache
        WHERE cache_status = 'full' AND row_count > 0
        GROUP BY symbol
        ORDER BY symbol
      )
    ),
    stock_actual AS (
      SELECT symbol, trade_date_utc AS day, uniqExact(minute_bucket_utc) AS minute_count
      FROM options.stock_ohlc_minute_raw
      WHERE trade_date_utc IN (SELECT day FROM days)
      GROUP BY symbol, trade_date_utc
    )
  SELECT toString(day), symbol
  FROM days
  CROSS JOIN syms
  LEFT JOIN stock_actual s USING (symbol, day)
  WHERE s.symbol IS NULL OR s.minute_count < 406
  ORDER BY day, symbol
  FORMAT TabSeparated
" "$RAW_STOCK_TSV"

write_query "
  WITH
    days AS (SELECT arrayJoin([$OI_DAYS_SQL]) AS day),
    syms AS (
      SELECT arrayJoin(groupArray(symbol)) AS symbol
      FROM (
        SELECT symbol
        FROM options.option_trade_day_cache
        WHERE cache_status = 'full' AND row_count > 0
        GROUP BY symbol
        ORDER BY symbol
      )
    )
  SELECT toString(day), symbol
  FROM days
  CROSS JOIN syms
  ORDER BY day, symbol
  FORMAT TabSeparated
" "$RAW_OI_TSV"

echo "Raw gap counts trade/quote/stock/oi: $(count_lines "$RAW_TRADE_TSV")/$(count_lines "$RAW_QUOTE_TSV")/$(count_lines "$RAW_STOCK_TSV")/$(count_lines "$RAW_OI_TSV")"

if [[ -s "$RAW_TRADE_TSV" ]]; then
  echo "Pre-clearing option_trades for targeted symbol-days..."
  CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
    bash "$DELETE_SYMBOL_DAYS_SCRIPT" option_trades trade_date "$RAW_TRADE_TSV" \
    > "$RUN_ROOT/raw-trade-preclear.log" 2>&1
  CLICKHOUSE_MUTATION_TIMEOUT_SEC="$CLICKHOUSE_MUTATION_TIMEOUT_SEC" \
  CLICKHOUSE_MUTATION_POLL_MS="$CLICKHOUSE_MUTATION_POLL_MS" \
    bash "$WAIT_FOR_MUTATIONS_SCRIPT" option_trades \
    > "$RUN_ROOT/raw-trade-preclear-wait.log" 2>&1

  echo "Running trade-only remediation..."
  run_backfill_parallel download tradequote "$TRADE_WORKERS" "$RAW_TRADE_TSV" "$RUN_ROOT/raw-trade-remediation.log"
fi

if [[ -s "$RAW_QUOTE_TSV" ]]; then
  echo "Running quote-only remediation..."
  run_backfill_parallel download quote "$QUOTE_WORKERS" "$RAW_QUOTE_TSV" "$RUN_ROOT/raw-quote-remediation.log"
fi

if [[ -s "$RAW_STOCK_TSV" ]]; then
  echo "Running stock-only remediation..."
  run_backfill_parallel download stock "$STOCK_WORKERS" "$RAW_STOCK_TSV" "$RUN_ROOT/raw-stock-remediation.log"
fi

if [[ -s "$RAW_OI_TSV" ]]; then
  echo "Running OI remediation..."
  run_backfill_parallel download oi "$OI_WORKERS" "$RAW_OI_TSV" "$RUN_ROOT/raw-oi-remediation.log"
fi

echo "Generating calculated-greeks gap list..."
write_query "
  SELECT
    symbol,
    toString(trade_date_utc)
  FROM options.option_trade_day_cache
  WHERE trade_date_utc >= toDate('2025-01-02')
    AND trade_date_utc <= toDate('2026-03-20')
    AND row_count > 0
    AND cache_status = 'full'
    AND (symbol, trade_date_utc) NOT IN (
      SELECT symbol, trade_date_utc
      FROM options.option_calculated_greeks_day_status FINAL
      WHERE status = 'complete'
    )
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$CALC_GREEKS_TSV"
echo "Calculated-greeks gaps: $(count_lines "$CALC_GREEKS_TSV")"

if [[ -s "$CALC_GREEKS_TSV" ]]; then
  echo "Running calculated-greeks remediation..."
  env \
    CALC_GREEKS_WORKERS="$CALC_GREEKS_WORKERS" \
    CALC_GREEKS_SYMBOL_DAY_LIST_PATH="$CALC_GREEKS_TSV" \
    CALC_GREEKS_SKIP_COMPLETED=0 \
    CALC_GREEKS_START_DATE=2025-01-02 \
    CALC_GREEKS_END_DATE=2026-03-20 \
    CALC_GREEKS_SOURCE=quote_minute \
    CALC_GREEKS_QUERY_MAX_THREADS="$CALC_GREEKS_QUERY_MAX_THREADS" \
    CALC_GREEKS_QUERY_MAX_MEMORY_BYTES="$CALC_GREEKS_QUERY_MAX_MEMORY_BYTES" \
    bash scripts/clickhouse/backfill-calculated-greeks-parallel.sh \
    > "$RUN_ROOT/calc-greeks-remediation.log" 2>&1
fi

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
echo "Enrichment gaps: $(count_lines "$ENRICH_TSV")"

if [[ -s "$ENRICH_TSV" ]]; then
  echo "Pre-clearing option_trade_enriched for targeted symbol-days..."
  CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
    bash "$DELETE_SYMBOL_DAYS_SCRIPT" option_trade_enriched trade_date "$ENRICH_TSV" \
    > "$RUN_ROOT/enrich-preclear.log" 2>&1
  CLICKHOUSE_MUTATION_TIMEOUT_SEC="$CLICKHOUSE_MUTATION_TIMEOUT_SEC" \
  CLICKHOUSE_MUTATION_POLL_MS="$CLICKHOUSE_MUTATION_POLL_MS" \
    bash "$WAIT_FOR_MUTATIONS_SCRIPT" option_trade_enriched \
    > "$RUN_ROOT/enrich-preclear-wait.log" 2>&1

  echo "Running enrichment remediation..."
  env \
    BACKFILL_MODE=enrich \
    BACKFILL_FORCE=1 \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$ENRICH_TSV" \
    BACKFILL_WORKERS="$ENRICH_WORKERS" \
    BACKFILL_MAX_WORKERS=12 \
    BACKFILL_CPU_TARGET_PCT="$BACKFILL_CPU_TARGET_PCT" \
    BACKFILL_MEMORY_RESERVE_MB="$BACKFILL_MEMORY_RESERVE_MB" \
    BACKFILL_MEMORY_PER_WORKER_MB="$BACKFILL_MEMORY_PER_WORKER_MB" \
    BACKFILL_RAM_BUDGET_MB="$BACKFILL_RAM_BUDGET_MB" \
    BACKFILL_NODE_MAX_OLD_SPACE_MB="$BACKFILL_NODE_MAX_OLD_SPACE_MB" \
    BACKFILL_WORKER_OVERHEAD_MB="$BACKFILL_WORKER_OVERHEAD_MB" \
    BACKFILL_SHARD_STRATEGY=balanced \
    BACKFILL_LOOP_UNTIL_READY=1 \
    BACKFILL_LOOP_MAX_PASSES=100 \
    BACKFILL_LOOP_SLEEP_MS=2000 \
    CLICKHOUSE_ENRICH_STREAM_READ=1 \
    CLICKHOUSE_ENRICH_STREAM_WRITE=1 \
    CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE=5000 \
    CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES=10 \
    CLICKHOUSE_ENRICH_GREEKS_SOURCE=calculated \
    CLICKHOUSE_ENRICH_SKIP_DELETE=1 \
    bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh \
    > "$RUN_ROOT/enrich-remediation.log" 2>&1
fi

echo "Audited gap remediation finished."
echo "Artifacts: $RUN_ROOT"
