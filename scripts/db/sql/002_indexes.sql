PRAGMA foreign_keys = ON;

CREATE INDEX IF NOT EXISTS idx_option_trades_trade_ts
  ON option_trades(trade_ts_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_trades_symbol_trade_ts
  ON option_trades(symbol, trade_ts_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_trades_contract_trade_ts
  ON option_trades(symbol, expiration, strike, option_right, trade_ts_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_trade_enriched_enriched_at
  ON option_trade_enriched(enriched_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_trade_enriched_symbol_enriched_at
  ON option_trade_enriched(symbol, enriched_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_trade_enriched_sigscore_trade
  ON option_trade_enriched(sig_score DESC, trade_id);

CREATE INDEX IF NOT EXISTS idx_option_trade_enriched_vol_oi
  ON option_trade_enriched(vol_oi_ratio DESC, trade_id);

CREATE INDEX IF NOT EXISTS idx_option_trade_enriched_repeat3m
  ON option_trade_enriched(repeat3m DESC, trade_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_filter_rule_versions_single_active
  ON filter_rule_versions(is_active)
  WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_saved_queries_kind_updated
  ON saved_queries(kind, updated_at_utc DESC);

CREATE INDEX IF NOT EXISTS idx_contract_stats_symbol_date
  ON contract_stats_intraday(symbol, session_date DESC);

CREATE INDEX IF NOT EXISTS idx_symbol_stats_symbol_bucket
  ON symbol_stats_intraday(symbol, minute_bucket_et DESC);

CREATE INDEX IF NOT EXISTS idx_option_symbol_minute_derived_symbol_date
  ON option_symbol_minute_derived(symbol, trade_date_utc DESC, minute_bucket_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_symbol_minute_derived_sig
  ON option_symbol_minute_derived(symbol, trade_date_utc DESC, max_sig_score DESC);

CREATE INDEX IF NOT EXISTS idx_option_contract_minute_derived_symbol_date
  ON option_contract_minute_derived(symbol, trade_date_utc DESC, minute_bucket_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_contract_minute_derived_contract_date
  ON option_contract_minute_derived(symbol, expiration, strike, option_right, trade_date_utc DESC, minute_bucket_utc DESC);

CREATE INDEX IF NOT EXISTS idx_option_oi_reference_symbol_asof
  ON option_open_interest_reference(symbol, as_of_date DESC);

CREATE INDEX IF NOT EXISTS idx_option_oi_reference_source_asof
  ON option_open_interest_reference(source, as_of_date DESC);
