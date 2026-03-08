CREATE DATABASE IF NOT EXISTS options;

CREATE TABLE IF NOT EXISTS options.option_trades
(
  trade_id String,
  trade_ts_utc DateTime64(3, 'UTC'),
  trade_date Date MATERIALIZED toDate(trade_ts_utc),
  trade_ts_et DateTime64(3, 'America/New_York'),
  symbol LowCardinality(String),
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  price Float64,
  size UInt32,
  bid Nullable(Float64),
  ask Nullable(Float64),
  condition_code Nullable(String),
  exchange Nullable(String),
  raw_payload_json String,
  watermark Nullable(String),
  ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_ts_utc)
ORDER BY (symbol, trade_ts_utc, trade_id);

CREATE TABLE IF NOT EXISTS options.stock_ohlc_minute_raw
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  minute_bucket_utc DateTime64(3, 'UTC'),
  open Nullable(Float64),
  high Nullable(Float64),
  low Nullable(Float64),
  close Float64,
  volume Nullable(Float64),
  source_endpoint Nullable(String),
  raw_payload_json String,
  ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, trade_date_utc, minute_bucket_utc);

CREATE TABLE IF NOT EXISTS options.option_open_interest_raw
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  oi UInt32,
  source_endpoint Nullable(String),
  raw_payload_json String,
  ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, trade_date_utc, expiration, strike, option_right);

CREATE TABLE IF NOT EXISTS options.option_quote_minute_raw
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  minute_bucket_utc DateTime64(3, 'UTC'),
  bid Nullable(Float64),
  ask Nullable(Float64),
  last Nullable(Float64),
  bid_size Nullable(UInt32),
  ask_size Nullable(UInt32),
  source_endpoint Nullable(String),
  raw_payload_json String,
  ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc);

CREATE TABLE IF NOT EXISTS options.option_greeks_minute_raw
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  minute_bucket_utc DateTime64(3, 'UTC'),
  delta Nullable(Float64),
  implied_vol Nullable(Float64),
  gamma Nullable(Float64),
  theta Nullable(Float64),
  vega Nullable(Float64),
  rho Nullable(Float64),
  underlying_price Nullable(Float64),
  source_endpoint Nullable(String),
  raw_payload_json String,
  ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc);

CREATE TABLE IF NOT EXISTS options.option_trade_enriched
(
  trade_id String,
  trade_ts_utc DateTime64(3, 'UTC'),
  trade_date Date MATERIALIZED toDate(trade_ts_utc),
  symbol LowCardinality(String),
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  price Float64,
  size UInt32,
  bid Nullable(Float64),
  ask Nullable(Float64),
  condition_code Nullable(String),
  exchange Nullable(String),
  value Nullable(Float64),
  dte Nullable(Int32),
  spot Nullable(Float64),
  otm_pct Nullable(Float64),
  day_volume Nullable(UInt64),
  oi Nullable(UInt32),
  vol_oi_ratio Nullable(Float64),
  repeat3m Nullable(Int32),
  sig_score Nullable(Float64),
  sentiment Nullable(String),
  execution_side Nullable(String),
  symbol_vol_1m Nullable(Float64),
  symbol_vol_baseline_15m Nullable(Float64),
  open_window_baseline Nullable(Float64),
  bullish_ratio_15m Nullable(Float64),
  chips_json String,
  rule_version Nullable(String),
  score_quality LowCardinality(String),
  missing_metrics_json String,
  enriched_at_utc DateTime64(3, 'UTC'),
  is_sweep Nullable(UInt8),
  is_multileg Nullable(UInt8),
  minute_of_day_et Nullable(Int32),
  delta Nullable(Float64),
  implied_vol Nullable(Float64),
  time_norm Nullable(Float64),
  delta_norm Nullable(Float64),
  iv_skew_norm Nullable(Float64),
  value_shock_norm Nullable(Float64),
  dte_swing_norm Nullable(Float64),
  flow_imbalance_norm Nullable(Float64),
  delta_pressure_norm Nullable(Float64),
  cp_oi_pressure_norm Nullable(Float64),
  iv_skew_surface_norm Nullable(Float64),
  iv_term_slope_norm Nullable(Float64),
  underlying_trend_confirm_norm Nullable(Float64),
  liquidity_quality_norm Nullable(Float64),
  multileg_penalty_norm Nullable(Float64),
  sig_score_components_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trade_ts_utc)
ORDER BY (symbol, trade_ts_utc, trade_id);

CREATE TABLE IF NOT EXISTS options.option_symbol_minute_derived
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  minute_bucket_utc DateTime64(3, 'UTC'),
  trade_count UInt32,
  contract_count UInt32,
  total_size UInt64,
  total_value Float64,
  call_size UInt64,
  put_size UInt64,
  bullish_count UInt32,
  bearish_count UInt32,
  neutral_count UInt32,
  avg_sig_score Nullable(Float64),
  max_sig_score Nullable(Float64),
  avg_vol_oi_ratio Nullable(Float64),
  max_vol_oi_ratio Nullable(Float64),
  max_repeat3m Nullable(Int32),
  oi_sum UInt64,
  day_volume_sum UInt64,
  chip_hits_json String,
  updated_at_utc DateTime64(3, 'UTC'),
  spot Nullable(Float64),
  avg_sig_score_bullish Nullable(Float64),
  avg_sig_score_bearish Nullable(Float64),
  net_sig_score Nullable(Float64),
  value_weighted_sig_score Nullable(Float64),
  sweep_count UInt32,
  sweep_value_ratio Nullable(Float64),
  multileg_count UInt32,
  multileg_pct Nullable(Float64),
  avg_minute_of_day_et Nullable(Float64),
  avg_iv Nullable(Float64),
  call_iv_avg Nullable(Float64),
  put_iv_avg Nullable(Float64),
  iv_spread Nullable(Float64),
  net_delta_dollars Nullable(Float64),
  avg_value_pctile Nullable(Float64),
  avg_vol_oi_norm Nullable(Float64),
  avg_repeat_norm Nullable(Float64),
  avg_otm_norm Nullable(Float64),
  avg_side_confidence Nullable(Float64),
  avg_dte_norm Nullable(Float64),
  avg_spread_norm Nullable(Float64),
  avg_sweep_norm Nullable(Float64),
  avg_multileg_norm Nullable(Float64),
  avg_time_norm Nullable(Float64),
  avg_delta_norm Nullable(Float64),
  avg_iv_skew_norm Nullable(Float64),
  avg_value_shock_norm Nullable(Float64),
  avg_dte_swing_norm Nullable(Float64),
  avg_flow_imbalance_norm Nullable(Float64),
  avg_delta_pressure_norm Nullable(Float64),
  avg_cp_oi_pressure_norm Nullable(Float64),
  avg_iv_skew_surface_norm Nullable(Float64),
  avg_iv_term_slope_norm Nullable(Float64),
  avg_underlying_trend_confirm_norm Nullable(Float64),
  avg_liquidity_quality_norm Nullable(Float64),
  avg_multileg_penalty_norm Nullable(Float64)
)
ENGINE = ReplacingMergeTree(updated_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, trade_date_utc, minute_bucket_utc);

CREATE TABLE IF NOT EXISTS options.option_contract_minute_derived
(
  symbol LowCardinality(String),
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  trade_date_utc Date,
  minute_bucket_utc DateTime64(3, 'UTC'),
  trade_count UInt32,
  size_sum UInt64,
  value_sum Float64,
  avg_price Nullable(Float64),
  last_price Nullable(Float64),
  day_volume Nullable(UInt64),
  oi Nullable(UInt32),
  vol_oi_ratio Nullable(Float64),
  avg_sig_score Nullable(Float64),
  max_sig_score Nullable(Float64),
  max_repeat3m Nullable(Int32),
  bullish_count UInt32,
  bearish_count UInt32,
  neutral_count UInt32,
  chip_hits_json String,
  updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc);

CREATE TABLE IF NOT EXISTS options.option_trade_day_cache
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  cache_status LowCardinality(String),
  row_count UInt64,
  last_sync_at_utc DateTime64(3, 'UTC'),
  last_error Nullable(String),
  source_endpoint Nullable(String),
  raw_file_path Nullable(String)
)
ENGINE = ReplacingMergeTree(last_sync_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, trade_date_utc);

CREATE TABLE IF NOT EXISTS options.option_trade_metric_day_cache
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  metric_name LowCardinality(String),
  cache_status LowCardinality(String),
  row_count UInt64,
  last_sync_at_utc DateTime64(3, 'UTC'),
  last_error Nullable(String)
)
ENGINE = ReplacingMergeTree(last_sync_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, trade_date_utc, metric_name);

CREATE TABLE IF NOT EXISTS options.option_download_chunk_status
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  stream_name LowCardinality(String),
  chunk_start_utc DateTime64(3, 'UTC'),
  chunk_end_utc DateTime64(3, 'UTC'),
  chunk_minutes UInt16,
  row_count UInt64,
  minute_count UInt16,
  status LowCardinality(String),
  source_endpoint Nullable(String),
  last_error Nullable(String),
  updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, trade_date_utc, stream_name, chunk_start_utc);

CREATE TABLE IF NOT EXISTS options.option_enrich_chunk_status
(
  symbol LowCardinality(String),
  trade_date_utc Date,
  stream_name LowCardinality(String),
  chunk_start_utc DateTime64(3, 'UTC'),
  chunk_end_utc DateTime64(3, 'UTC'),
  chunk_minutes UInt16,
  input_row_count UInt64,
  output_row_count UInt64,
  input_minute_count UInt16,
  output_minute_count UInt16,
  status LowCardinality(String),
  rule_version Nullable(String),
  last_error Nullable(String),
  updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
PARTITION BY toYYYYMM(trade_date_utc)
ORDER BY (symbol, trade_date_utc, stream_name, chunk_start_utc);

CREATE TABLE IF NOT EXISTS options.option_symbol_status
(
  symbol LowCardinality(String),
  status LowCardinality(String),
  last_reason Nullable(String),
  first_seen_at_utc DateTime64(3, 'UTC'),
  last_updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(last_updated_at_utc)
ORDER BY symbol;

CREATE TABLE IF NOT EXISTS options.contract_stats_intraday
(
  symbol LowCardinality(String),
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  session_date Date,
  day_volume UInt64,
  oi UInt64,
  last_trade_ts_utc Nullable(DateTime64(3, 'UTC')),
  updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
PARTITION BY toYYYYMM(session_date)
ORDER BY (symbol, expiration, strike, option_right, session_date);

CREATE TABLE IF NOT EXISTS options.symbol_stats_intraday
(
  symbol LowCardinality(String),
  minute_bucket_et DateTime64(3, 'UTC'),
  vol_1m Float64,
  vol_baseline_15m Float64,
  open_window_baseline Float64,
  bullish_ratio_15m Float64,
  updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
PARTITION BY toYYYYMM(minute_bucket_et)
ORDER BY (symbol, minute_bucket_et);

CREATE TABLE IF NOT EXISTS options.filter_rule_versions
(
  version_id String,
  config_json String,
  checksum String,
  is_active UInt8,
  created_at_utc DateTime64(3, 'UTC'),
  activated_at_utc Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree(created_at_utc)
ORDER BY version_id;

CREATE TABLE IF NOT EXISTS options.supplemental_metric_cache
(
  metric_kind LowCardinality(String),
  cache_key String,
  value_json String,
  expires_at_utc DateTime64(3, 'UTC'),
  updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
PARTITION BY toYYYYMM(updated_at_utc)
ORDER BY (metric_kind, cache_key);

CREATE TABLE IF NOT EXISTS options.feature_baseline_intraday
(
  symbol LowCardinality(String),
  minute_of_day_et Int32,
  feature_name LowCardinality(String),
  sample_count UInt64,
  mean Float64,
  m2 Float64,
  updated_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at_utc)
ORDER BY (symbol, minute_of_day_et, feature_name);

CREATE TABLE IF NOT EXISTS options.option_open_interest_reference
(
  source LowCardinality(String),
  source_url Nullable(String),
  as_of_date Date,
  symbol LowCardinality(String),
  expiration Date,
  strike Float64,
  option_right Enum8('CALL' = 1, 'PUT' = -1),
  oi UInt32,
  raw_payload_json String,
  ingested_at_utc DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(ingested_at_utc)
PARTITION BY toYYYYMM(as_of_date)
ORDER BY (source, as_of_date, symbol, expiration, strike, option_right);
