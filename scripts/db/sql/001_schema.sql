PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS option_trades (
  trade_id TEXT PRIMARY KEY,
  trade_ts_utc TEXT NOT NULL,
  trade_ts_et TEXT NOT NULL,
  symbol TEXT NOT NULL,
  expiration TEXT NOT NULL,
  strike REAL NOT NULL,
  option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
  price REAL NOT NULL,
  size INTEGER NOT NULL,
  bid REAL,
  ask REAL,
  condition_code TEXT,
  exchange TEXT,
  raw_payload_json TEXT,
  watermark TEXT,
  ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS option_trade_enriched (
  trade_id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  expiration TEXT NOT NULL,
  strike REAL NOT NULL,
  option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
  value REAL,
  dte INTEGER,
  spot REAL,
  otm_pct REAL,
  day_volume INTEGER,
  oi INTEGER,
  vol_oi_ratio REAL,
  repeat3m INTEGER,
  sig_score REAL CHECK (sig_score >= 0.0 AND sig_score <= 1.0),
  sentiment TEXT CHECK (sentiment IN ('bullish', 'bearish', 'neutral')),
  chips_json TEXT NOT NULL DEFAULT '[]',
  rule_version TEXT,
  enriched_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  FOREIGN KEY (trade_id) REFERENCES option_trades(trade_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS contract_stats_intraday (
  symbol TEXT NOT NULL,
  expiration TEXT NOT NULL,
  strike REAL NOT NULL,
  option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
  session_date TEXT NOT NULL,
  day_volume INTEGER NOT NULL DEFAULT 0,
  oi INTEGER NOT NULL DEFAULT 0,
  last_trade_ts_utc TEXT,
  updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (symbol, expiration, strike, option_right, session_date)
);

CREATE TABLE IF NOT EXISTS symbol_stats_intraday (
  symbol TEXT NOT NULL,
  minute_bucket_et TEXT NOT NULL,
  vol_1m REAL NOT NULL DEFAULT 0,
  vol_baseline_15m REAL NOT NULL DEFAULT 0,
  open_window_baseline REAL NOT NULL DEFAULT 0,
  bullish_ratio_15m REAL NOT NULL DEFAULT 0,
  updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  PRIMARY KEY (symbol, minute_bucket_et)
);

CREATE TABLE IF NOT EXISTS filter_rule_versions (
  version_id TEXT PRIMARY KEY,
  config_json TEXT NOT NULL,
  checksum TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0, 1)),
  created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  activated_at_utc TEXT
);

CREATE TABLE IF NOT EXISTS ingest_checkpoints (
  stream_name TEXT PRIMARY KEY,
  watermark TEXT,
  updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS saved_queries (
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL CHECK (kind IN ('preset', 'alert')),
  name TEXT NOT NULL,
  payload_version TEXT NOT NULL CHECK (payload_version IN ('legacy', 'v2')),
  query_dsl_v2_json TEXT NOT NULL,
  created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
