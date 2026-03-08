#!/usr/bin/env bash
set -euo pipefail

SQLITE_DB="${SQLITE_DB:-/Users/pawanagarwal/github/phenixflow/data/options_storage/curated/curated/sqlite/options_trade_quote.sqlite}"
SQLITE_DB_URI="file:${SQLITE_DB}?mode=ro"
SQLITE_BUSY_TIMEOUT_MS="${SQLITE_BUSY_TIMEOUT_MS:-120000}"
CH_HOST="${CH_HOST:-127.0.0.1}"
CH_PORT="${CH_PORT:-9000}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
DAY="${DAY:-}"
PARALLEL_DAYS="${PARALLEL_DAYS:-1}"
RESET_TABLES="${RESET_TABLES:-0}"
CH_CONNECT_TIMEOUT_SEC="${CH_CONNECT_TIMEOUT_SEC:-30}"
CH_SEND_TIMEOUT_SEC="${CH_SEND_TIMEOUT_SEC:-1800}"
CH_RECEIVE_TIMEOUT_SEC="${CH_RECEIVE_TIMEOUT_SEC:-1800}"
CH_QUERY_TIMEOUT_SEC="${CH_QUERY_TIMEOUT_SEC:-0}"
CH_INSERT_RETRIES="${CH_INSERT_RETRIES:-4}"
CH_RETRY_DELAY_SEC="${CH_RETRY_DELAY_SEC:-5}"
CH_RETRY_MAX_DELAY_SEC="${CH_RETRY_MAX_DELAY_SEC:-60}"

CPU_COUNT="$(( \
  $(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 8) \
))"
MAX_INSERT_THREADS="${MAX_INSERT_THREADS:-$CPU_COUNT}"
MAX_THREADS="${MAX_THREADS:-$CPU_COUNT}"
SELF_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"

if [[ ! -f "$SQLITE_DB" ]]; then
  echo "SQLite DB not found: $SQLITE_DB"
  exit 1
fi

if ! command -v clickhouse >/dev/null 2>&1; then
  echo "clickhouse binary not found. Install first with scripts/clickhouse/install-clickhouse.sh"
  exit 1
fi

if ! [[ "$PARALLEL_DAYS" =~ ^[0-9]+$ ]] || (( PARALLEL_DAYS < 1 )); then
  echo "PARALLEL_DAYS must be a positive integer (got: $PARALLEL_DAYS)"
  exit 1
fi

if ! [[ "$CH_INSERT_RETRIES" =~ ^[0-9]+$ ]] || (( CH_INSERT_RETRIES < 1 )); then
  echo "CH_INSERT_RETRIES must be a positive integer (got: $CH_INSERT_RETRIES)"
  exit 1
fi

if ! [[ "$CH_RETRY_DELAY_SEC" =~ ^[0-9]+$ ]] || (( CH_RETRY_DELAY_SEC < 1 )); then
  echo "CH_RETRY_DELAY_SEC must be a positive integer (got: $CH_RETRY_DELAY_SEC)"
  exit 1
fi

if ! [[ "$CH_RETRY_MAX_DELAY_SEC" =~ ^[0-9]+$ ]] || (( CH_RETRY_MAX_DELAY_SEC < CH_RETRY_DELAY_SEC )); then
  echo "CH_RETRY_MAX_DELAY_SEC must be a positive integer >= CH_RETRY_DELAY_SEC (got: $CH_RETRY_MAX_DELAY_SEC)"
  exit 1
fi

run_insert() {
  local query="$1"
  clickhouse client \
    --host "$CH_HOST" \
    --port "$CH_PORT" \
    --user "$CH_USER" \
    --password "$CH_PASSWORD" \
    --connect_timeout "$CH_CONNECT_TIMEOUT_SEC" \
    --send_timeout "$CH_SEND_TIMEOUT_SEC" \
    --receive_timeout "$CH_RECEIVE_TIMEOUT_SEC" \
    --max_execution_time "$CH_QUERY_TIMEOUT_SEC" \
    --query "$query"
}

sqlite_noheader() {
  local sql="$1"
  sqlite3 -cmd ".timeout $SQLITE_BUSY_TIMEOUT_MS" -noheader "$SQLITE_DB_URI" "$sql"
}

sqlite_csv() {
  local sql="$1"
  sqlite3 -cmd ".timeout $SQLITE_BUSY_TIMEOUT_MS" -header -csv "$SQLITE_DB_URI" "$sql"
}

with_insert_settings() {
  local insert_sql="$1"
  cat <<SQL
$insert_sql
SETTINGS
  input_format_parallel_parsing = 1,
  date_time_input_format = 'best_effort',
  max_insert_threads = $MAX_INSERT_THREADS,
  max_threads = $MAX_THREADS
FORMAT CSVWithNames
SQL
}

delete_scope_for_retry() {
  local label="$1"
  local import_day="$2"

  if [[ -z "$import_day" ]]; then
    case "$label" in
      option_symbol_status)
        run_insert "TRUNCATE TABLE options.option_symbol_status"
        ;;
    esac
    return 0
  fi

  case "$label" in
    option_trades)
      run_insert "ALTER TABLE options.option_trades DELETE WHERE trade_date = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    stock_ohlc_minute_raw)
      run_insert "ALTER TABLE options.stock_ohlc_minute_raw DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_open_interest_raw)
      run_insert "ALTER TABLE options.option_open_interest_raw DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_quote_minute_raw)
      run_insert "ALTER TABLE options.option_quote_minute_raw DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_greeks_minute_raw)
      run_insert "ALTER TABLE options.option_greeks_minute_raw DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_trade_enriched)
      run_insert "ALTER TABLE options.option_trade_enriched DELETE WHERE trade_date = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_symbol_minute_derived)
      run_insert "ALTER TABLE options.option_symbol_minute_derived DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_contract_minute_derived)
      run_insert "ALTER TABLE options.option_contract_minute_derived DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_trade_day_cache)
      run_insert "ALTER TABLE options.option_trade_day_cache DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
    option_trade_metric_day_cache)
      run_insert "ALTER TABLE options.option_trade_metric_day_cache DELETE WHERE trade_date_utc = toDate('${import_day}') SETTINGS mutations_sync = 1"
      ;;
  esac
}

import_csv_query() {
  local label="$1"
  local select_sql="$2"
  local insert_sql="$3"
  local prefix="$4"
  local import_day="$5"
  local attempt=1
  local fifo=""
  local sqlite_pid=""
  local client_status=0
  local sqlite_status=0

  while (( attempt <= CH_INSERT_RETRIES )); do
    if (( attempt == 1 )); then
      echo "${prefix}Importing ${label}..."
    else
      local delay_sec=$(( CH_RETRY_DELAY_SEC * (2 ** (attempt - 2)) ))
      if (( delay_sec > CH_RETRY_MAX_DELAY_SEC )); then
        delay_sec="$CH_RETRY_MAX_DELAY_SEC"
      fi
      echo "${prefix}Retrying ${label} (${attempt}/${CH_INSERT_RETRIES}) after ${delay_sec}s..."
      sleep "$delay_sec"
      delete_scope_for_retry "$label" "$import_day"
    fi

    fifo="$(mktemp "/tmp/${label}.XXXXXX")"
    rm -f "$fifo"
    mkfifo "$fifo"

    sqlite_csv "$select_sql" > "$fifo" &
    sqlite_pid=$!

    if run_insert "$(with_insert_settings "$insert_sql")" < "$fifo"; then
      client_status=0
    else
      client_status=$?
    fi

    if [[ -n "$sqlite_pid" ]] && kill -0 "$sqlite_pid" 2>/dev/null; then
      if (( client_status != 0 )); then
        kill "$sqlite_pid" 2>/dev/null || true
      fi
    fi

    if [[ -n "$sqlite_pid" ]]; then
      if wait "$sqlite_pid"; then
        sqlite_status=0
      else
        sqlite_status=$?
      fi
    else
      sqlite_status=0
    fi

    rm -f "$fifo"
    fifo=""
    sqlite_pid=""

    if (( client_status == 0 && sqlite_status == 0 )); then
      return 0
    fi

    attempt=$(( attempt + 1 ))
  done

  echo "${prefix}FAIL ${label} after ${CH_INSERT_RETRIES} attempts" >&2
  return 1
}

import_for_day() {
  local import_day="$1"
  local trade_ts_where=""
  local date_where=""
  local prefix=""

  if [[ -n "$import_day" ]]; then
    trade_ts_where="WHERE trade_ts_utc >= '${import_day}T00:00:00.000Z' AND trade_ts_utc < date('${import_day}','+1 day') || 'T00:00:00.000Z'"
    date_where="WHERE trade_date_utc = '${import_day}'"
    prefix="[$import_day] "
  fi

  import_csv_query \
    "option_trades" \
    "
SELECT
  trade_id,
  trade_ts_utc,
  trade_ts_et,
  symbol,
  expiration,
  strike,
  option_right,
  price,
  size,
  bid,
  ask,
  condition_code,
  exchange,
  raw_payload_json,
  watermark,
  ingested_at_utc
FROM option_trades
$trade_ts_where
;" \
    "INSERT INTO options.option_trades
(trade_id, trade_ts_utc, trade_ts_et, symbol, expiration, strike, option_right, price, size, bid, ask, condition_code, exchange, raw_payload_json, watermark, ingested_at_utc)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "stock_ohlc_minute_raw" \
    "
SELECT
  symbol,
  trade_date_utc,
  minute_bucket_utc,
  open,
  high,
  low,
  close,
  volume,
  source_endpoint,
  raw_payload_json,
  ingested_at_utc
FROM stock_ohlc_minute_raw
$date_where
;" \
    "INSERT INTO options.stock_ohlc_minute_raw
(symbol, trade_date_utc, minute_bucket_utc, open, high, low, close, volume, source_endpoint, raw_payload_json, ingested_at_utc)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "option_open_interest_raw" \
    "
SELECT
  symbol,
  trade_date_utc,
  expiration,
  strike,
  option_right,
  oi,
  source_endpoint,
  raw_payload_json,
  ingested_at_utc
FROM option_open_interest_raw
$date_where
;" \
    "INSERT INTO options.option_open_interest_raw
(symbol, trade_date_utc, expiration, strike, option_right, oi, source_endpoint, raw_payload_json, ingested_at_utc)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "option_quote_minute_raw" \
    "
SELECT
  symbol,
  trade_date_utc,
  expiration,
  strike,
  option_right,
  minute_bucket_utc,
  bid,
  ask,
  last,
  bid_size,
  ask_size,
  source_endpoint,
  raw_payload_json,
  ingested_at_utc
FROM option_quote_minute_raw
$date_where
;" \
    "INSERT INTO options.option_quote_minute_raw
(symbol, trade_date_utc, expiration, strike, option_right, minute_bucket_utc, bid, ask, last, bid_size, ask_size, source_endpoint, raw_payload_json, ingested_at_utc)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "option_greeks_minute_raw" \
    "
SELECT
  symbol,
  trade_date_utc,
  expiration,
  strike,
  option_right,
  minute_bucket_utc,
  delta,
  implied_vol,
  gamma,
  theta,
  vega,
  rho,
  underlying_price,
  source_endpoint,
  raw_payload_json,
  ingested_at_utc
FROM option_greeks_minute_raw
$date_where
;" \
    "INSERT INTO options.option_greeks_minute_raw
(symbol, trade_date_utc, expiration, strike, option_right, minute_bucket_utc, delta, implied_vol, gamma, theta, vega, rho, underlying_price, source_endpoint, raw_payload_json, ingested_at_utc)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "option_trade_enriched" \
    "
SELECT
  trade_id,
  trade_ts_utc,
  symbol,
  expiration,
  strike,
  option_right,
  price,
  size,
  bid,
  ask,
  condition_code,
  exchange,
  value,
  dte,
  spot,
  otm_pct,
  day_volume,
  oi,
  vol_oi_ratio,
  repeat3m,
  sig_score,
  sentiment,
  execution_side,
  symbol_vol_1m,
  symbol_vol_baseline_15m,
  open_window_baseline,
  bullish_ratio_15m,
  chips_json,
  rule_version,
  score_quality,
  missing_metrics_json,
  enriched_at_utc,
  is_sweep,
  is_multileg,
  minute_of_day_et,
  delta,
  implied_vol,
  time_norm,
  delta_norm,
  iv_skew_norm,
  value_shock_norm,
  dte_swing_norm,
  flow_imbalance_norm,
  delta_pressure_norm,
  cp_oi_pressure_norm,
  iv_skew_surface_norm,
  iv_term_slope_norm,
  underlying_trend_confirm_norm,
  liquidity_quality_norm,
  multileg_penalty_norm,
  sig_score_components_json
FROM option_trade_enriched
$trade_ts_where
;" \
    "INSERT INTO options.option_trade_enriched
(trade_id, trade_ts_utc, symbol, expiration, strike, option_right, price, size, bid, ask, condition_code, exchange, value, dte, spot, otm_pct, day_volume, oi, vol_oi_ratio, repeat3m, sig_score, sentiment, execution_side, symbol_vol_1m, symbol_vol_baseline_15m, open_window_baseline, bullish_ratio_15m, chips_json, rule_version, score_quality, missing_metrics_json, enriched_at_utc, is_sweep, is_multileg, minute_of_day_et, delta, implied_vol, time_norm, delta_norm, iv_skew_norm, value_shock_norm, dte_swing_norm, flow_imbalance_norm, delta_pressure_norm, cp_oi_pressure_norm, iv_skew_surface_norm, iv_term_slope_norm, underlying_trend_confirm_norm, liquidity_quality_norm, multileg_penalty_norm, sig_score_components_json)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "option_symbol_minute_derived" \
    "
SELECT
  symbol,
  trade_date_utc,
  minute_bucket_utc,
  trade_count,
  contract_count,
  total_size,
  total_value,
  call_size,
  put_size,
  bullish_count,
  bearish_count,
  neutral_count,
  avg_sig_score,
  max_sig_score,
  avg_vol_oi_ratio,
  max_vol_oi_ratio,
  max_repeat3m,
  oi_sum,
  day_volume_sum,
  chip_hits_json,
  updated_at_utc,
  spot,
  avg_sig_score_bullish,
  avg_sig_score_bearish,
  net_sig_score,
  value_weighted_sig_score,
  sweep_count,
  sweep_value_ratio,
  multileg_count,
  multileg_pct,
  avg_minute_of_day_et,
  avg_iv,
  call_iv_avg,
  put_iv_avg,
  iv_spread,
  net_delta_dollars,
  avg_value_pctile,
  avg_vol_oi_norm,
  avg_repeat_norm,
  avg_otm_norm,
  avg_side_confidence,
  avg_dte_norm,
  avg_spread_norm,
  avg_sweep_norm,
  avg_multileg_norm,
  avg_time_norm,
  avg_delta_norm,
  avg_iv_skew_norm,
  avg_value_shock_norm,
  avg_dte_swing_norm,
  avg_flow_imbalance_norm,
  avg_delta_pressure_norm,
  avg_cp_oi_pressure_norm,
  avg_iv_skew_surface_norm,
  avg_iv_term_slope_norm,
  avg_underlying_trend_confirm_norm,
  avg_liquidity_quality_norm,
  avg_multileg_penalty_norm
FROM option_symbol_minute_derived
$date_where
;" \
    "INSERT INTO options.option_symbol_minute_derived
(symbol, trade_date_utc, minute_bucket_utc, trade_count, contract_count, total_size, total_value, call_size, put_size, bullish_count, bearish_count, neutral_count, avg_sig_score, max_sig_score, avg_vol_oi_ratio, max_vol_oi_ratio, max_repeat3m, oi_sum, day_volume_sum, chip_hits_json, updated_at_utc, spot, avg_sig_score_bullish, avg_sig_score_bearish, net_sig_score, value_weighted_sig_score, sweep_count, sweep_value_ratio, multileg_count, multileg_pct, avg_minute_of_day_et, avg_iv, call_iv_avg, put_iv_avg, iv_spread, net_delta_dollars, avg_value_pctile, avg_vol_oi_norm, avg_repeat_norm, avg_otm_norm, avg_side_confidence, avg_dte_norm, avg_spread_norm, avg_sweep_norm, avg_multileg_norm, avg_time_norm, avg_delta_norm, avg_iv_skew_norm, avg_value_shock_norm, avg_dte_swing_norm, avg_flow_imbalance_norm, avg_delta_pressure_norm, avg_cp_oi_pressure_norm, avg_iv_skew_surface_norm, avg_iv_term_slope_norm, avg_underlying_trend_confirm_norm, avg_liquidity_quality_norm, avg_multileg_penalty_norm)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "option_contract_minute_derived" \
    "
SELECT
  symbol,
  expiration,
  strike,
  option_right,
  trade_date_utc,
  minute_bucket_utc,
  trade_count,
  size_sum,
  value_sum,
  avg_price,
  last_price,
  day_volume,
  oi,
  vol_oi_ratio,
  avg_sig_score,
  max_sig_score,
  max_repeat3m,
  bullish_count,
  bearish_count,
  neutral_count,
  chip_hits_json,
  updated_at_utc
FROM option_contract_minute_derived
$date_where
;" \
    "INSERT INTO options.option_contract_minute_derived
(symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc, trade_count, size_sum, value_sum, avg_price, last_price, day_volume, oi, vol_oi_ratio, avg_sig_score, max_sig_score, max_repeat3m, bullish_count, bearish_count, neutral_count, chip_hits_json, updated_at_utc)" \
    "$prefix" \
    "$import_day"

  import_csv_query \
    "option_trade_metric_day_cache" \
    "
SELECT
  symbol,
  trade_date_utc,
  metric_name,
  cache_status,
  row_count,
  last_sync_at_utc,
  last_error
FROM option_trade_metric_day_cache
$date_where
;" \
    "INSERT INTO options.option_trade_metric_day_cache
(symbol, trade_date_utc, metric_name, cache_status, row_count, last_sync_at_utc, last_error)" \
    "$prefix" \
    "$import_day"
}

import_day_cache_all() {
  import_csv_query \
    "option_trade_day_cache" \
    "
SELECT
  symbol,
  trade_date_utc,
  cache_status,
  row_count,
  last_sync_at_utc,
  last_error,
  source_endpoint,
  raw_file_path
FROM option_trade_day_cache
;" \
    "INSERT INTO options.option_trade_day_cache
(symbol, trade_date_utc, cache_status, row_count, last_sync_at_utc, last_error, source_endpoint, raw_file_path)" \
    "" \
    ""
}

import_symbol_status() {
  import_csv_query \
    "option_symbol_status" \
    "
SELECT
  symbol,
  status,
  last_reason,
  first_seen_at_utc,
  last_updated_at_utc
FROM option_symbol_status
;" \
    "INSERT INTO options.option_symbol_status
(symbol, status, last_reason, first_seen_at_utc, last_updated_at_utc)" \
    "" \
    ""
}

import_support_tables_all() {
  import_csv_query \
    "contract_stats_intraday" \
    "
SELECT
  symbol,
  expiration,
  strike,
  option_right,
  session_date,
  day_volume,
  oi,
  last_trade_ts_utc,
  updated_at_utc
FROM contract_stats_intraday
;" \
    "INSERT INTO options.contract_stats_intraday
(symbol, expiration, strike, option_right, session_date, day_volume, oi, last_trade_ts_utc, updated_at_utc)" \
    "" \
    ""

  import_csv_query \
    "symbol_stats_intraday" \
    "
SELECT
  symbol,
  minute_bucket_et,
  vol_1m,
  vol_baseline_15m,
  open_window_baseline,
  bullish_ratio_15m,
  updated_at_utc
FROM symbol_stats_intraday
;" \
    "INSERT INTO options.symbol_stats_intraday
(symbol, minute_bucket_et, vol_1m, vol_baseline_15m, open_window_baseline, bullish_ratio_15m, updated_at_utc)" \
    "" \
    ""

  import_csv_query \
    "filter_rule_versions" \
    "
SELECT
  version_id,
  config_json,
  checksum,
  is_active,
  created_at_utc,
  activated_at_utc
FROM filter_rule_versions
;" \
    "INSERT INTO options.filter_rule_versions
(version_id, config_json, checksum, is_active, created_at_utc, activated_at_utc)" \
    "" \
    ""

  import_csv_query \
    "supplemental_metric_cache" \
    "
SELECT
  metric_kind,
  cache_key,
  value_json,
  expires_at_utc,
  updated_at_utc
FROM supplemental_metric_cache
;" \
    "INSERT INTO options.supplemental_metric_cache
(metric_kind, cache_key, value_json, expires_at_utc, updated_at_utc)" \
    "" \
    ""

  import_csv_query \
    "feature_baseline_intraday" \
    "
SELECT
  symbol,
  minute_of_day_et,
  feature_name,
  sample_count,
  mean,
  m2,
  updated_at_utc
FROM feature_baseline_intraday
;" \
    "INSERT INTO options.feature_baseline_intraday
(symbol, minute_of_day_et, feature_name, sample_count, mean, m2, updated_at_utc)" \
    "" \
    ""

  import_csv_query \
    "option_open_interest_reference" \
    "
SELECT
  source,
  source_url,
  as_of_date,
  symbol,
  expiration,
  strike,
  option_right,
  oi,
  raw_payload_json,
  ingested_at_utc
FROM option_open_interest_reference
;" \
    "INSERT INTO options.option_open_interest_reference
(source, source_url, as_of_date, symbol, expiration, strike, option_right, oi, raw_payload_json, ingested_at_utc)" \
    "" \
    ""
}

if [[ "$RESET_TABLES" == "1" ]]; then
  echo "Resetting ClickHouse tables in options.* before import..."
  run_insert "TRUNCATE TABLE options.option_trades"
  run_insert "TRUNCATE TABLE options.stock_ohlc_minute_raw"
  run_insert "TRUNCATE TABLE options.option_open_interest_raw"
  run_insert "TRUNCATE TABLE options.option_quote_minute_raw"
  run_insert "TRUNCATE TABLE options.option_greeks_minute_raw"
  run_insert "TRUNCATE TABLE options.option_trade_enriched"
  run_insert "TRUNCATE TABLE options.option_symbol_minute_derived"
  run_insert "TRUNCATE TABLE options.option_contract_minute_derived"
  run_insert "TRUNCATE TABLE options.option_trade_day_cache"
  run_insert "TRUNCATE TABLE options.option_trade_metric_day_cache"
  run_insert "TRUNCATE TABLE options.option_symbol_status"
  run_insert "TRUNCATE TABLE options.contract_stats_intraday"
  run_insert "TRUNCATE TABLE options.symbol_stats_intraday"
  run_insert "TRUNCATE TABLE options.filter_rule_versions"
  run_insert "TRUNCATE TABLE options.supplemental_metric_cache"
  run_insert "TRUNCATE TABLE options.feature_baseline_intraday"
  run_insert "TRUNCATE TABLE options.option_open_interest_reference"
fi

if [[ -z "$DAY" && "$PARALLEL_DAYS" -gt 1 && "${CHILD_IMPORT:-0}" != "1" ]]; then
  echo "Parallel import enabled: PARALLEL_DAYS=$PARALLEL_DAYS, threads=$MAX_INSERT_THREADS"
  day_query="SELECT DISTINCT trade_date_utc AS day FROM option_trade_day_cache WHERE cache_status = 'full' ORDER BY day;"
  day_count="$(sqlite_noheader "$day_query" | wc -l | tr -d ' ')"
  if [[ "${day_count:-0}" == "0" ]]; then
    echo "No full cached days found in option_trade_day_cache; skipping day-scoped import."
  else
    export SQLITE_DB CH_HOST CH_PORT CH_USER CH_PASSWORD MAX_INSERT_THREADS MAX_THREADS SQLITE_BUSY_TIMEOUT_MS
    export CH_CONNECT_TIMEOUT_SEC CH_SEND_TIMEOUT_SEC CH_RECEIVE_TIMEOUT_SEC CH_QUERY_TIMEOUT_SEC
    export CH_INSERT_RETRIES CH_RETRY_DELAY_SEC CH_RETRY_MAX_DELAY_SEC
    sqlite_noheader "$day_query" \
      | xargs -I{} -P "$PARALLEL_DAYS" env CHILD_IMPORT=1 DAY={} PARALLEL_DAYS=1 RESET_TABLES=0 bash "$SELF_PATH"
  fi
  run_insert "TRUNCATE TABLE options.option_trade_day_cache"
  import_day_cache_all
  import_symbol_status
  run_insert "TRUNCATE TABLE options.contract_stats_intraday"
  run_insert "TRUNCATE TABLE options.symbol_stats_intraday"
  run_insert "TRUNCATE TABLE options.filter_rule_versions"
  run_insert "TRUNCATE TABLE options.supplemental_metric_cache"
  run_insert "TRUNCATE TABLE options.feature_baseline_intraday"
  run_insert "TRUNCATE TABLE options.option_open_interest_reference"
  import_support_tables_all
  echo "Parallel import complete."
  exit 0
fi

import_for_day "$DAY"
if [[ -z "$DAY" ]]; then
  run_insert "TRUNCATE TABLE options.option_trade_day_cache"
  import_day_cache_all
  import_symbol_status
  run_insert "TRUNCATE TABLE options.contract_stats_intraday"
  run_insert "TRUNCATE TABLE options.symbol_stats_intraday"
  run_insert "TRUNCATE TABLE options.filter_rule_versions"
  run_insert "TRUNCATE TABLE options.supplemental_metric_cache"
  run_insert "TRUNCATE TABLE options.feature_baseline_intraday"
  run_insert "TRUNCATE TABLE options.option_open_interest_reference"
  import_support_tables_all
fi

echo "Import complete."
