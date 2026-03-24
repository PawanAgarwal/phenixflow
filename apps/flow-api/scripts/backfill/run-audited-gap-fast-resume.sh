#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
cd "$APP_ROOT"

export THETADATA_BASE_URL="${THETADATA_BASE_URL:-http://127.0.0.1:25503}"

RUNTIME_ROOT="${PHENIXFLOW_RUNTIME_ROOT:-$HOME/Library/Caches/phenixflow}"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="${AUDITED_GAP_FAST_RUN_ROOT:-$RUNTIME_ROOT/reports/audited-gap-fast-resume-$TS}"
REPORT_ROOT="${AUDITED_GAP_FAST_REPORT_ROOT:-$RUNTIME_ROOT/reports}"
DELETE_SYMBOL_DAYS_SCRIPT="$REPO_ROOT/infra/clickhouse/scripts/delete-clickhouse-symbol-days.sh"
WAIT_FOR_MUTATIONS_SCRIPT="$REPO_ROOT/infra/clickhouse/scripts/wait-for-clickhouse-mutations.sh"
CALC_GREEKS_SCRIPT="$REPO_ROOT/infra/clickhouse/scripts/backfill-calculated-greeks-parallel.sh"
SOFR_SYNC_SCRIPT="$REPO_ROOT/infra/clickhouse/scripts/sync-sofr-daily.js"

RAW_CONNECTIONS="${RAW_CONNECTIONS:-12}"
RAW_BACKFILL_MAX_WORKERS="${RAW_BACKFILL_MAX_WORKERS:-16}"
TRADE_WORKERS="${TRADE_WORKERS:-10}"
QUOTE_WORKERS="${QUOTE_WORKERS:-12}"
STOCK_WORKERS="${STOCK_WORKERS:-10}"
OI_WORKERS="${OI_WORKERS:-12}"
RAW_CPU_TARGET_PCT="${RAW_CPU_TARGET_PCT:-95}"
RAW_MEMORY_RESERVE_MB="${RAW_MEMORY_RESERVE_MB:-2048}"
RAW_MEMORY_PER_WORKER_MB="${RAW_MEMORY_PER_WORKER_MB:-1536}"
RAW_RAM_BUDGET_MB="${RAW_RAM_BUDGET_MB:-20480}"
RAW_NODE_MAX_OLD_SPACE_MB="${RAW_NODE_MAX_OLD_SPACE_MB:-1024}"
RAW_WORKER_OVERHEAD_MB="${RAW_WORKER_OVERHEAD_MB:-512}"
RAW_STREAM_INSERT_MAX_INFLIGHT="${RAW_STREAM_INSERT_MAX_INFLIGHT:-4}"
RAW_SUPPLEMENTAL_CONCURRENCY="${RAW_SUPPLEMENTAL_CONCURRENCY:-8}"
OI_MEMORY_PER_WORKER_MB="${OI_MEMORY_PER_WORKER_MB:-2048}"
OI_RAM_BUDGET_MB="${OI_RAM_BUDGET_MB:-20480}"
OI_NODE_MAX_OLD_SPACE_MB="${OI_NODE_MAX_OLD_SPACE_MB:-1536}"
OI_WORKER_OVERHEAD_MB="${OI_WORKER_OVERHEAD_MB:-512}"
OI_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES="${OI_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES:-15}"
OI_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES="${OI_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES:-1}"

PRIORITY_CALC_WORKERS="${PRIORITY_CALC_WORKERS:-16}"
PRIORITY_CALC_QUERY_MAX_THREADS="${PRIORITY_CALC_QUERY_MAX_THREADS:-2}"
PRIORITY_CALC_QUERY_MAX_MEMORY_BYTES="${PRIORITY_CALC_QUERY_MAX_MEMORY_BYTES:-2684354560}"
BACKLOG_CALC_WORKERS="${BACKLOG_CALC_WORKERS:-10}"
BACKLOG_CALC_QUERY_MAX_THREADS="${BACKLOG_CALC_QUERY_MAX_THREADS:-2}"
BACKLOG_CALC_QUERY_MAX_MEMORY_BYTES="${BACKLOG_CALC_QUERY_MAX_MEMORY_BYTES:-2684354560}"
CALC_GREEKS_NODE_MAX_OLD_SPACE_MB="${CALC_GREEKS_NODE_MAX_OLD_SPACE_MB:-1024}"
INDEX_GREEKS_SYMBOLS="${INDEX_GREEKS_SYMBOLS:-SPX,SPXW,SPY,QQQ,VIX,VIXW,RUT,RUTW,XSP}"
INDEX_RAW_GREEKS_PRIORITY_WORKERS="${INDEX_RAW_GREEKS_PRIORITY_WORKERS:-12}"
INDEX_RAW_GREEKS_BACKLOG_WORKERS="${INDEX_RAW_GREEKS_BACKLOG_WORKERS:-10}"
INDEX_RAW_GREEKS_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES="${INDEX_RAW_GREEKS_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES:-15}"
INDEX_RAW_GREEKS_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES="${INDEX_RAW_GREEKS_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES:-1}"

ENRICH_WORKERS="${ENRICH_WORKERS:-18}"
ENRICH_MAX_WORKERS="${ENRICH_MAX_WORKERS:-20}"
ENRICH_CPU_TARGET_PCT="${ENRICH_CPU_TARGET_PCT:-95}"
ENRICH_MEMORY_RESERVE_MB="${ENRICH_MEMORY_RESERVE_MB:-2048}"
ENRICH_MEMORY_PER_WORKER_MB="${ENRICH_MEMORY_PER_WORKER_MB:-1024}"
ENRICH_RAM_BUDGET_MB="${ENRICH_RAM_BUDGET_MB:-22528}"
ENRICH_NODE_MAX_OLD_SPACE_MB="${ENRICH_NODE_MAX_OLD_SPACE_MB:-768}"
ENRICH_WORKER_OVERHEAD_MB="${ENRICH_WORKER_OVERHEAD_MB:-256}"
ENRICH_LOOP_SLEEP_MS="${ENRICH_LOOP_SLEEP_MS:-2000}"
ENRICH_LOOP_MAX_PASSES="${ENRICH_LOOP_MAX_PASSES:-4000}"
ENRICH_STREAM_CHUNK_SIZE="${ENRICH_STREAM_CHUNK_SIZE:-10000}"

CLICKHOUSE_MUTATION_TIMEOUT_SEC="${CLICKHOUSE_MUTATION_TIMEOUT_SEC:-7200}"
CLICKHOUSE_MUTATION_POLL_MS="${CLICKHOUSE_MUTATION_POLL_MS:-2000}"
RUN_BACKLOG_CALC="${RUN_BACKLOG_CALC:-1}"

mkdir -p "$RUN_ROOT" "$REPORT_ROOT"

if [[ ! -d "$REPO_ROOT/node_modules" ]]; then
  echo "Installing npm dependencies..."
  (
    cd "$REPO_ROOT"
    npm ci
  )
fi

if [[ ! -x "$DELETE_SYMBOL_DAYS_SCRIPT" ]]; then
  chmod +x "$DELETE_SYMBOL_DAYS_SCRIPT"
fi
if [[ ! -x "$WAIT_FOR_MUTATIONS_SCRIPT" ]]; then
  chmod +x "$WAIT_FOR_MUTATIONS_SCRIPT"
fi

echo "Fast resume run root: $RUN_ROOT"
echo "Runtime report root: $REPORT_ROOT"
echo "Theta base URL: $THETADATA_BASE_URL"
echo "Raw workers trade/quote/stock/oi: $TRADE_WORKERS/$QUOTE_WORKERS/$STOCK_WORKERS/$OI_WORKERS"
echo "Raw theta concurrency: $RAW_CONNECTIONS"
echo "Raw insert inflight / supplemental concurrency: $RAW_STREAM_INSERT_MAX_INFLIGHT/$RAW_SUPPLEMENTAL_CONCURRENCY"
echo "Priority calc workers: $PRIORITY_CALC_WORKERS"
echo "Backlog calc workers: $BACKLOG_CALC_WORKERS"
echo "Enrich workers: $ENRICH_WORKERS"

write_query() {
  local query="$1"
  local output_path="$2"
  clickhouse client --host 127.0.0.1 --port 9000 --query "$query" > "$output_path"
}

count_lines() {
  awk 'NF > 0 { count += 1 } END { print count + 0 }' "$1"
}

csv_to_sql_in_list() {
  local raw="$1"
  local out=""
  local part=""
  local normalized=""
  IFS=',' read -r -a parts <<< "$raw"
  for part in "${parts[@]}"; do
    normalized="$(printf '%s' "$part" | xargs | tr '[:lower:]' '[:upper:]')"
    [[ -z "$normalized" ]] && continue
    if [[ -n "$out" ]]; then
      out+=","
    fi
    out+="'$normalized'"
  done
  printf '%s' "$out"
}

run_raw_backfill() {
  local components="$1"
  local workers="$2"
  local input_path="$3"
  local log_path="$4"
  local memory_per_worker_mb="$RAW_MEMORY_PER_WORKER_MB"
  local ram_budget_mb="$RAW_RAM_BUDGET_MB"
  local node_max_old_space_mb="$RAW_NODE_MAX_OLD_SPACE_MB"
  local worker_overhead_mb="$RAW_WORKER_OVERHEAD_MB"
  local trade_read_window_minutes=""
  local trade_read_min_window_minutes=""

  if [[ "$components" == "oi" ]]; then
    memory_per_worker_mb="$OI_MEMORY_PER_WORKER_MB"
    ram_budget_mb="$OI_RAM_BUDGET_MB"
    node_max_old_space_mb="$OI_NODE_MAX_OLD_SPACE_MB"
    worker_overhead_mb="$OI_WORKER_OVERHEAD_MB"
    trade_read_window_minutes="$OI_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES"
    trade_read_min_window_minutes="$OI_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES"
  elif [[ "$components" == "greeks" ]]; then
    trade_read_window_minutes="$INDEX_RAW_GREEKS_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES"
    trade_read_min_window_minutes="$INDEX_RAW_GREEKS_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES"
  fi

  env \
    BACKFILL_MODE=download \
    BACKFILL_FORCE=1 \
    BACKFILL_RAW_COMPONENTS="$components" \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$input_path" \
    BACKFILL_REPORT_DIR="$REPORT_ROOT" \
    BACKFILL_WORKERS="$workers" \
    BACKFILL_MAX_WORKERS="$RAW_BACKFILL_MAX_WORKERS" \
    BACKFILL_CPU_TARGET_PCT="$RAW_CPU_TARGET_PCT" \
    BACKFILL_MEMORY_RESERVE_MB="$RAW_MEMORY_RESERVE_MB" \
    BACKFILL_MEMORY_PER_WORKER_MB="$memory_per_worker_mb" \
    BACKFILL_RAM_BUDGET_MB="$ram_budget_mb" \
    BACKFILL_NODE_MAX_OLD_SPACE_MB="$node_max_old_space_mb" \
    BACKFILL_WORKER_OVERHEAD_MB="$worker_overhead_mb" \
    BACKFILL_DOWNLOAD_WORKER_GUARD=0 \
    BACKFILL_SHARD_STRATEGY=balanced \
    THETADATA_BASE_URL="$THETADATA_BASE_URL" \
    THETADATA_MAX_CONCURRENT_CONNECTIONS="$RAW_CONNECTIONS" \
    THETADATA_DOWNLOAD_CONCURRENCY="$RAW_CONNECTIONS" \
    THETADATA_HISTORICAL_OPTION_FORMAT=ndjson \
    THETADATA_OPTION_QUOTE_FORMAT=ndjson \
    THETADATA_STREAM_IDLE_TIMEOUT_MS=1800000 \
    THETADATA_STREAM_HEARTBEAT_EVERY_ROWS=250000 \
    THETADATA_SUPPLEMENTAL_CONCURRENCY="$RAW_SUPPLEMENTAL_CONCURRENCY" \
    CLICKHOUSE_STREAM_INSERT_MAX_INFLIGHT="$RAW_STREAM_INSERT_MAX_INFLIGHT" \
    CLICKHOUSE_TRADE_READ_WINDOW_MINUTES="$trade_read_window_minutes" \
    CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES="$trade_read_min_window_minutes" \
    BACKFILL_FORCE_TRADE_FULL=0 \
    BACKFILL_FORCE_OI_FULL=1 \
    CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
    bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh \
    > "$log_path" 2>&1
}

INDEX_GREEKS_SYMBOLS_SQL="$(csv_to_sql_in_list "$INDEX_GREEKS_SYMBOLS")"
if [[ -z "$INDEX_GREEKS_SYMBOLS_SQL" ]]; then
  echo "INDEX_GREEKS_SYMBOLS resolved to an empty list"
  exit 1
fi

RAW_DAYS_SQL="toDate('2025-01-09'),toDate('2025-04-07'),toDate('2025-07-30'),toDate('2025-10-22'),toDate('2026-02-06'),toDate('2026-02-09'),toDate('2026-02-10'),toDate('2026-02-11'),toDate('2026-02-12'),toDate('2026-02-13'),toDate('2026-02-17'),toDate('2026-02-18'),toDate('2026-03-10'),toDate('2026-03-11'),toDate('2026-03-12'),toDate('2026-03-13'),toDate('2026-03-16'),toDate('2026-03-17'),toDate('2026-03-18'),toDate('2026-03-19'),toDate('2026-03-20')"
OI_DAYS_SQL="toDate('2025-01-09'),toDate('2025-12-29'),toDate('2026-01-02'),toDate('2026-01-05'),toDate('2026-01-06'),toDate('2026-01-07'),toDate('2026-01-08'),toDate('2026-01-09'),toDate('2026-01-12'),toDate('2026-01-13'),toDate('2026-01-14'),toDate('2026-01-15'),toDate('2026-01-16'),toDate('2026-01-20'),toDate('2026-01-21'),toDate('2026-01-22'),toDate('2026-01-23'),toDate('2026-01-26'),toDate('2026-01-27'),toDate('2026-01-28'),toDate('2026-01-29'),toDate('2026-01-30'),toDate('2026-02-02'),toDate('2026-03-13'),toDate('2026-03-16'),toDate('2026-03-17'),toDate('2026-03-18'),toDate('2026-03-19'),toDate('2026-03-20')"
ENRICH_DAYS_SQL="toDate('2025-03-13'),toDate('2025-03-14'),toDate('2025-03-17'),toDate('2025-03-18'),toDate('2025-03-19'),toDate('2025-03-20'),toDate('2025-03-21'),toDate('2025-03-24'),toDate('2025-03-25'),toDate('2025-03-26'),toDate('2025-03-27'),toDate('2025-03-28'),toDate('2025-03-31'),toDate('2025-04-01'),toDate('2025-04-02'),toDate('2025-04-03'),toDate('2025-04-04'),toDate('2025-04-07'),toDate('2025-04-08'),toDate('2025-04-09'),toDate('2025-04-10'),toDate('2025-04-11'),toDate('2025-04-14'),toDate('2025-04-15'),toDate('2025-04-16'),toDate('2025-04-17'),toDate('2025-04-21'),toDate('2025-04-22'),toDate('2025-04-23'),toDate('2025-04-24'),toDate('2025-04-25'),toDate('2025-04-28'),toDate('2025-04-29'),toDate('2025-04-30'),toDate('2025-05-01'),toDate('2025-05-02'),toDate('2025-05-05'),toDate('2025-05-06'),toDate('2025-05-07'),toDate('2025-05-08'),toDate('2025-05-09'),toDate('2025-05-12'),toDate('2025-05-13'),toDate('2025-05-14'),toDate('2025-05-15'),toDate('2025-05-16'),toDate('2025-05-19'),toDate('2025-05-20'),toDate('2025-05-21'),toDate('2025-05-22'),toDate('2025-05-23'),toDate('2025-05-27'),toDate('2025-05-28'),toDate('2025-05-29'),toDate('2025-05-30'),toDate('2025-06-02'),toDate('2025-06-03'),toDate('2025-06-04'),toDate('2025-06-05'),toDate('2025-06-06'),toDate('2025-06-09'),toDate('2025-06-10'),toDate('2025-06-11'),toDate('2025-06-12'),toDate('2025-06-13'),toDate('2025-06-16'),toDate('2025-06-17'),toDate('2025-06-18'),toDate('2025-06-20'),toDate('2025-06-23'),toDate('2025-06-24'),toDate('2025-06-25'),toDate('2025-06-26'),toDate('2025-06-27'),toDate('2025-06-30'),toDate('2025-07-01'),toDate('2025-07-02'),toDate('2025-07-03'),toDate('2025-07-07'),toDate('2025-07-08'),toDate('2025-07-09'),toDate('2025-07-10'),toDate('2025-07-11'),toDate('2025-07-14'),toDate('2025-07-15'),toDate('2025-07-16'),toDate('2025-07-17'),toDate('2025-07-18'),toDate('2025-07-21'),toDate('2025-07-22'),toDate('2025-07-23'),toDate('2025-07-24'),toDate('2025-07-25'),toDate('2025-07-28'),toDate('2025-07-29'),toDate('2025-07-30'),toDate('2025-07-31'),toDate('2025-10-22'),toDate('2026-01-09'),toDate('2026-01-12'),toDate('2026-01-13'),toDate('2026-01-14'),toDate('2026-01-15'),toDate('2026-01-16'),toDate('2026-01-20'),toDate('2026-01-21'),toDate('2026-01-22'),toDate('2026-01-23'),toDate('2026-01-26'),toDate('2026-01-27'),toDate('2026-01-28'),toDate('2026-01-29'),toDate('2026-01-30'),toDate('2026-02-02'),toDate('2026-02-03'),toDate('2026-02-04'),toDate('2026-02-05'),toDate('2026-02-06'),toDate('2026-02-09'),toDate('2026-02-10'),toDate('2026-02-11'),toDate('2026-02-12'),toDate('2026-02-13'),toDate('2026-02-17'),toDate('2026-02-18'),toDate('2026-03-16'),toDate('2026-03-17'),toDate('2026-03-18'),toDate('2026-03-19'),toDate('2026-03-20')"

RAW_TRADE_TSV="$RUN_ROOT/raw-trade-gaps.tsv"
RAW_QUOTE_TSV="$RUN_ROOT/raw-quote-gaps.tsv"
RAW_STOCK_TSV="$RUN_ROOT/raw-stock-gaps.tsv"
RAW_OI_TSV="$RUN_ROOT/raw-oi-gaps.tsv"
RAW_OI_PARTIAL_TSV="$RUN_ROOT/raw-oi-partial-preclear.tsv"
PRIORITY_CALC_TSV="$RUN_ROOT/calc-greeks-priority.tsv"
BACKLOG_CALC_TSV="$RUN_ROOT/calc-greeks-backlog.tsv"
INDEX_RAW_GREEKS_PRIORITY_TSV="$RUN_ROOT/index-raw-greeks-priority.tsv"
INDEX_RAW_GREEKS_BACKLOG_TSV="$RUN_ROOT/index-raw-greeks-backlog.tsv"
ENRICH_TSV="$RUN_ROOT/enrich-gaps.tsv"

if [[ -f "$SOFR_SYNC_SCRIPT" ]]; then
  echo "Refreshing SOFR reference data..."
  node "$SOFR_SYNC_SCRIPT" --refresh 1 --years 3 > "$RUN_ROOT/sofr-sync.log" 2>&1
fi

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
      SELECT symbol, trade_date_utc AS day, sum(minute_count) AS minute_count
      FROM options.option_download_chunk_status FINAL
      WHERE stream_name = 'option_quote_1m'
        AND trade_date_utc IN (SELECT day FROM days)
      GROUP BY symbol, trade_date_utc
    )
  SELECT toString(day), symbol
  FROM days
  CROSS JOIN syms
  LEFT JOIN quote_actual q USING (symbol, day)
  WHERE q.symbol IS NULL
     OR q.minute_count < 390
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
    ),
    oi_actual AS (
      SELECT symbol, trade_date_utc AS day, count() AS row_count
      FROM options.option_open_interest_raw
      WHERE trade_date_utc IN (SELECT day FROM days)
      GROUP BY symbol, trade_date_utc
    )
  SELECT toString(day), symbol
  FROM days
  CROSS JOIN syms
  LEFT JOIN oi_actual o USING (symbol, day)
  WHERE o.symbol IS NULL
     OR o.row_count = 0
     OR day = toDate('2026-03-13')
  ORDER BY day, symbol
  FORMAT TabSeparated
" "$RAW_OI_TSV"

echo "Raw gap counts trade/quote/stock/oi: $(count_lines "$RAW_TRADE_TSV")/$(count_lines "$RAW_QUOTE_TSV")/$(count_lines "$RAW_STOCK_TSV")/$(count_lines "$RAW_OI_TSV")"

if [[ -s "$RAW_TRADE_TSV" ]]; then
  echo "Running trade remediation..."
  run_raw_backfill tradequote "$TRADE_WORKERS" "$RAW_TRADE_TSV" "$RUN_ROOT/raw-trade-remediation.log"
fi

if [[ -s "$RAW_QUOTE_TSV" ]]; then
  echo "Running quote remediation..."
  run_raw_backfill quote "$QUOTE_WORKERS" "$RAW_QUOTE_TSV" "$RUN_ROOT/raw-quote-remediation.log"
fi

if [[ -s "$RAW_STOCK_TSV" ]]; then
  echo "Running stock remediation..."
  run_raw_backfill stock "$STOCK_WORKERS" "$RAW_STOCK_TSV" "$RUN_ROOT/raw-stock-remediation.log"
fi

echo "Generating calculated-greeks gap lists..."
write_query "
  WITH days AS (SELECT arrayJoin([$ENRICH_DAYS_SQL]) AS day)
  SELECT
    symbol,
    toString(trade_date_utc)
  FROM options.option_trade_day_cache
  WHERE trade_date_utc IN (SELECT day FROM days)
    AND row_count > 0
    AND cache_status = 'full'
    AND symbol NOT IN ($INDEX_GREEKS_SYMBOLS_SQL)
    AND (symbol, trade_date_utc) NOT IN (
      SELECT symbol, trade_date_utc
      FROM options.option_calculated_greeks_day_status FINAL
      WHERE status = 'complete'
    )
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$PRIORITY_CALC_TSV"

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
    AND symbol NOT IN ($INDEX_GREEKS_SYMBOLS_SQL)
    AND (symbol, trade_date_utc) NOT IN (
      SELECT symbol, trade_date_utc
      FROM options.option_calculated_greeks_day_status FINAL
      WHERE status = 'complete'
    )
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$BACKLOG_CALC_TSV"

echo "Generating index raw-greeks gap lists..."
write_query "
  WITH days AS (SELECT arrayJoin([$ENRICH_DAYS_SQL]) AS day)
  SELECT
    toString(trade_date_utc),
    symbol
  FROM options.option_trade_day_cache
  WHERE trade_date_utc IN (SELECT day FROM days)
    AND row_count > 0
    AND cache_status = 'full'
    AND symbol IN ($INDEX_GREEKS_SYMBOLS_SQL)
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$INDEX_RAW_GREEKS_PRIORITY_TSV"

write_query "
  WITH days AS (SELECT arrayJoin([$ENRICH_DAYS_SQL]) AS day)
  SELECT
    toString(trade_date_utc),
    symbol
  FROM options.option_trade_day_cache
  WHERE trade_date_utc >= toDate('2025-01-02')
    AND trade_date_utc <= toDate('2026-03-20')
    AND trade_date_utc NOT IN (SELECT day FROM days)
    AND row_count > 0
    AND cache_status = 'full'
    AND symbol IN ($INDEX_GREEKS_SYMBOLS_SQL)
  ORDER BY trade_date_utc, symbol
  FORMAT TabSeparated
" "$INDEX_RAW_GREEKS_BACKLOG_TSV"

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
echo "Index raw-greeks priority/backlog jobs: $(count_lines "$INDEX_RAW_GREEKS_PRIORITY_TSV")/$(count_lines "$INDEX_RAW_GREEKS_BACKLOG_TSV")"
echo "Enrichment jobs: $(count_lines "$ENRICH_TSV")"

priority_calc_pid=""
priority_index_raw_greeks_pid=""
if [[ -s "$PRIORITY_CALC_TSV" ]]; then
  echo "Starting priority calculated-greeks warmup in background..."
  (
    cd "$APP_ROOT"
    env \
      NODE_OPTIONS="--max-old-space-size=$CALC_GREEKS_NODE_MAX_OLD_SPACE_MB" \
      CALC_GREEKS_WORKERS="$PRIORITY_CALC_WORKERS" \
      CALC_GREEKS_REPORT_DIR="$REPORT_ROOT" \
      CALC_GREEKS_SYMBOL_DAY_LIST_PATH="$PRIORITY_CALC_TSV" \
      CALC_GREEKS_SKIP_COMPLETED=0 \
      CALC_GREEKS_START_DATE=2025-01-02 \
      CALC_GREEKS_END_DATE=2026-03-20 \
      CALC_GREEKS_SOURCE=quote_minute \
      CALC_GREEKS_QUERY_MAX_THREADS="$PRIORITY_CALC_QUERY_MAX_THREADS" \
      CALC_GREEKS_QUERY_MAX_MEMORY_BYTES="$PRIORITY_CALC_QUERY_MAX_MEMORY_BYTES" \
      bash "$CALC_GREEKS_SCRIPT"
  ) > "$RUN_ROOT/calc-priority-warmup.log" 2>&1 &
  priority_calc_pid="$!"
  echo "Priority calc warmup pid: $priority_calc_pid"
fi

if [[ -s "$INDEX_RAW_GREEKS_PRIORITY_TSV" ]]; then
  echo "Starting index raw-greeks warmup in background..."
  (
    run_raw_backfill greeks "$INDEX_RAW_GREEKS_PRIORITY_WORKERS" "$INDEX_RAW_GREEKS_PRIORITY_TSV" "$RUN_ROOT/index-raw-greeks-priority.log"
  ) > "$RUN_ROOT/index-raw-greeks-priority-wrapper.log" 2>&1 &
  priority_index_raw_greeks_pid="$!"
  echo "Index raw-greeks warmup pid: $priority_index_raw_greeks_pid"
fi

if [[ -s "$RAW_OI_TSV" ]]; then
  awk -F'\t' '$1 == "2026-03-13" { print $0 }' "$RAW_OI_TSV" > "$RAW_OI_PARTIAL_TSV"
  if [[ -s "$RAW_OI_PARTIAL_TSV" ]]; then
    echo "Pre-clearing partial OI symbol-days for 2026-03-13..."
    CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
      bash "$DELETE_SYMBOL_DAYS_SCRIPT" option_open_interest_raw trade_date_utc "$RAW_OI_PARTIAL_TSV" \
      > "$RUN_ROOT/raw-oi-preclear.log" 2>&1
    CLICKHOUSE_MUTATION_TIMEOUT_SEC="$CLICKHOUSE_MUTATION_TIMEOUT_SEC" \
    CLICKHOUSE_MUTATION_POLL_MS="$CLICKHOUSE_MUTATION_POLL_MS" \
      bash "$WAIT_FOR_MUTATIONS_SCRIPT" option_open_interest_raw \
      > "$RUN_ROOT/raw-oi-preclear-wait.log" 2>&1
  fi

  echo "Running OI remediation..."
  run_raw_backfill oi "$OI_WORKERS" "$RAW_OI_TSV" "$RUN_ROOT/raw-oi-remediation.log"
fi

if [[ -s "$ENRICH_TSV" ]]; then
  echo "Pre-clearing option_trade_enriched for targeted symbol-days..."
  CLICKHOUSE_DELETE_MUTATION_SYNC=0 \
    bash "$DELETE_SYMBOL_DAYS_SCRIPT" option_trade_enriched trade_date "$ENRICH_TSV" \
    > "$RUN_ROOT/enrich-preclear.log" 2>&1
  CLICKHOUSE_MUTATION_TIMEOUT_SEC="$CLICKHOUSE_MUTATION_TIMEOUT_SEC" \
  CLICKHOUSE_MUTATION_POLL_MS="$CLICKHOUSE_MUTATION_POLL_MS" \
    bash "$WAIT_FOR_MUTATIONS_SCRIPT" option_trade_enriched \
    > "$RUN_ROOT/enrich-preclear-wait.log" 2>&1

  echo "Starting enrichment with loop-until-ready..."
  env \
    NODE_OPTIONS="--max-old-space-size=$ENRICH_NODE_MAX_OLD_SPACE_MB" \
    BACKFILL_MODE=enrich \
    BACKFILL_FORCE=1 \
    BACKFILL_SYMBOL_DAY_LIST_PATH="$ENRICH_TSV" \
    BACKFILL_REPORT_DIR="$REPORT_ROOT" \
    BACKFILL_WORKERS="$ENRICH_WORKERS" \
    BACKFILL_MAX_WORKERS="$ENRICH_MAX_WORKERS" \
    BACKFILL_CPU_TARGET_PCT="$ENRICH_CPU_TARGET_PCT" \
    BACKFILL_MEMORY_RESERVE_MB="$ENRICH_MEMORY_RESERVE_MB" \
    BACKFILL_MEMORY_PER_WORKER_MB="$ENRICH_MEMORY_PER_WORKER_MB" \
    BACKFILL_RAM_BUDGET_MB="$ENRICH_RAM_BUDGET_MB" \
    BACKFILL_NODE_MAX_OLD_SPACE_MB="$ENRICH_NODE_MAX_OLD_SPACE_MB" \
    BACKFILL_WORKER_OVERHEAD_MB="$ENRICH_WORKER_OVERHEAD_MB" \
    BACKFILL_DOWNLOAD_WORKER_GUARD=0 \
    BACKFILL_SHARD_STRATEGY=balanced \
    BACKFILL_LOOP_UNTIL_READY=1 \
    BACKFILL_LOOP_MAX_PASSES="$ENRICH_LOOP_MAX_PASSES" \
    BACKFILL_LOOP_SLEEP_MS="$ENRICH_LOOP_SLEEP_MS" \
    CLICKHOUSE_ENRICH_STREAM_READ=1 \
    CLICKHOUSE_ENRICH_STREAM_WRITE=1 \
    CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE="$ENRICH_STREAM_CHUNK_SIZE" \
    CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES=10 \
    CLICKHOUSE_ENRICH_GREEKS_SOURCE=index_raw \
    CLICKHOUSE_ENRICH_GREEKS_INDEX_SYMBOLS="$INDEX_GREEKS_SYMBOLS" \
    CLICKHOUSE_ENRICH_REQUIRE_GREEKS_READY=1 \
    CLICKHOUSE_ENRICH_SKIP_DELETE=1 \
    bash scripts/backfill/backfill-clickhouse-historical-days-parallel.sh \
    > "$RUN_ROOT/enrich-priority.log" 2>&1
fi

if [[ -n "$priority_calc_pid" ]]; then
  echo "Waiting for priority calculated-greeks warmup..."
  wait "$priority_calc_pid"
fi

if [[ -n "$priority_index_raw_greeks_pid" ]]; then
  echo "Waiting for priority index raw-greeks warmup..."
  wait "$priority_index_raw_greeks_pid"
fi

if [[ "$RUN_BACKLOG_CALC" != "0" ]] && [[ "$RUN_BACKLOG_CALC" != "false" ]] && [[ -s "$BACKLOG_CALC_TSV" ]]; then
  echo "Starting deferred calculated-greeks backlog..."
  (
    cd "$APP_ROOT"
    env \
      NODE_OPTIONS="--max-old-space-size=$CALC_GREEKS_NODE_MAX_OLD_SPACE_MB" \
      CALC_GREEKS_WORKERS="$BACKLOG_CALC_WORKERS" \
      CALC_GREEKS_REPORT_DIR="$REPORT_ROOT" \
      CALC_GREEKS_SYMBOL_DAY_LIST_PATH="$BACKLOG_CALC_TSV" \
      CALC_GREEKS_SKIP_COMPLETED=0 \
      CALC_GREEKS_START_DATE=2025-01-02 \
      CALC_GREEKS_END_DATE=2026-03-20 \
      CALC_GREEKS_SOURCE=quote_minute \
      CALC_GREEKS_QUERY_MAX_THREADS="$BACKLOG_CALC_QUERY_MAX_THREADS" \
      CALC_GREEKS_QUERY_MAX_MEMORY_BYTES="$BACKLOG_CALC_QUERY_MAX_MEMORY_BYTES" \
      bash "$CALC_GREEKS_SCRIPT"
  ) > "$RUN_ROOT/calc-backlog.log" 2>&1
fi

if [[ -s "$INDEX_RAW_GREEKS_BACKLOG_TSV" ]]; then
  echo "Starting deferred index raw-greeks backlog..."
  (
    run_raw_backfill greeks "$INDEX_RAW_GREEKS_BACKLOG_WORKERS" "$INDEX_RAW_GREEKS_BACKLOG_TSV" "$RUN_ROOT/index-raw-greeks-backlog.log"
  ) > "$RUN_ROOT/index-raw-greeks-backlog-wrapper.log" 2>&1 &
fi

echo "Fast audited-gap resume finished."
echo "Artifacts: $RUN_ROOT"
