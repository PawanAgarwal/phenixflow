const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');
let UndiciAgent = null;
try {
  ({ Agent: UndiciAgent } = require('undici'));
} catch {
  UndiciAgent = null;
}
const {
  resolveFlowReadBackend,
  resolveFlowWriteBackend,
  buildArtifactPath: buildClickHouseArtifactPath,
  queryRowsSync: queryClickHouseRowsSync,
  execQuerySync: execClickHouseQuerySync,
  insertJsonRowsSync: insertClickHouseJsonRowsSync,
} = require('./storage/clickhouse');

const {
  CHIP_DEFINITIONS,
  getThresholds,
} = require('./historical-filter-definitions');
const { resolveActiveRuleConfig } = require('./scoring/rule-config');
const {
  parseHistoricalFilters,
  getRequiredMetricsForQuery,
  applyHistoricalFilters,
} = require('./historical-query');
const {
  toFiniteNumber,
  normalizeRight,
  computeValue,
  computeDte,
  computeExecutionFlags,
  computeSentiment,
  isStandardMonthly,
  computeSpot,
  computeOtmPct,
  computeSigScore,
  toMinuteBucketUtc,
  isAmSpikeWindow,
  isSweep,
  isMultilegByCode,
  computeOtmNormBellCurve,
  computeMinuteOfDayEt,
  computeTimeNorm,
  computeIvSkewNorm,
  computeDteSwingNorm,
  computeFlowImbalanceNorm,
  computeDeltaPressureNorm,
  computeCpOiPressureNorm,
  computeIvTermSlopeNorm,
  computeUnderlyingTrendConfirmNorm,
  computeDeltaProxy,
  computeLiquidityQualityNorm,
  computeValueShockNorm,
} = require('./historical-formulas');
const { loadReferenceOiMap } = require('./oi-gov');

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const DAY_CACHE_STATUS_FULL = 'full';
const DAY_CACHE_STATUS_PARTIAL = 'partial';
const DEFAULT_HISTORICAL_OPTION_PATH = '/v3/option/history/trade_quote';
const DEFAULT_OPTION_QUOTE_PATH = '/v3/option/history/quote';
const DEFAULT_THETADATA_CALENDAR_PATH = '/v3/calendar/on_date';
const DEFAULT_SPOT_PATH = '/v3/stock/history/ohlc';
const DEFAULT_OI_PATH = '/v3/option/history/open_interest';
const DEFAULT_GREEKS_PATH = '/v3/option/history/greeks/first_order';
const DEFAULT_HISTORICAL_OPTION_FORMAT = 'ndjson';
const DEFAULT_OPTION_QUOTE_FORMAT = 'ndjson';
const DEFAULT_THETADATA_TIMEOUT_MS = 120000;
const DEFAULT_THETADATA_CALENDAR_TIMEOUT_MS = 30000;
const DEFAULT_THETADATA_STREAM_IDLE_TIMEOUT_MS = 1800000;
const DEFAULT_THETADATA_STREAM_HEARTBEAT_EVERY_ROWS = 250000;
const DEFAULT_THETADATA_MAX_CONNECTIONS_PER_PROCESS = 1;
const DEFAULT_THETADATA_MAX_PIPELINING_PER_CONNECTION = 1;
const DEFAULT_THETADATA_LARGE_SYMBOLS = ['SPY', 'QQQ'];
const DEFAULT_THETADATA_LARGE_SYMBOL_WINDOW_MINUTES = 60;
const DEFAULT_THETADATA_CALENDAR_CLOSE_PAD_MINUTES = 15;
const DEFAULT_BACKFILL_QUOTE_GAP_MAX_WINDOWS = 48;
const DEFAULT_SUPPLEMENTAL_CACHE_TTL_HOURS = 24;
const DEFAULT_SUPPLEMENTAL_CONCURRENCY = 4;
const DEFAULT_TREND_FALLBACK_MAX_LAG_MINUTES = 480;
const DEFAULT_GREEKS_CONTRACT_FALLBACK_LIMIT = 200;
const DEFAULT_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES = 60;
const DEFAULT_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES = 5;
const DEFAULT_CLICKHOUSE_QUOTE_STREAM_CHUNK_SIZE = 50000;
const DEFAULT_CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE = 5000;
const DEFAULT_CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES = 10;
const DEFAULT_CLICKHOUSE_CHUNK_STATUS_MINUTES = 10;

const DOWNLOAD_CHUNK_STREAMS = Object.freeze({
  TRADE_QUOTE_1M: 'option_trade_quote_1m',
  OPTION_QUOTE_1M: 'option_quote_1m',
  STOCK_PRICE_1M: 'stock_price_1m',
});
const ENRICH_CHUNK_STREAM = 'option_trade_enriched_1m';

const CHUNK_STATUS_STATE = Object.freeze({
  AVAILABLE: 'available',
  COMPLETE: 'complete',
  PARTIAL: 'partial',
  MISSING: 'missing',
  EXTRA: 'extra',
});

const METRIC_NAMES = Object.freeze([
  'enrichedRows',
  'execution',
  'value',
  'size',
  'dte',
  'expiration',
  'repeat3m',
  'sentiment',
  'symbolVolStats',
  'bullishRatio15m',
  'spot',
  'otmPct',
  'oi',
  'volOiRatio',
  'sigScore',
]);

let thetaFetchDispatcher = null;
let thetaFetchDispatcherKey = null;
const thetaCalendarSessionCache = new Map();

function resolveDbPath(env = process.env) {
  const configuredPath = env.PHENIX_DB_PATH || path.resolve(__dirname, '..', 'data', 'phenixflow.sqlite');
  return path.resolve(configuredPath);
}

function ensureDbDirectory(dbPath) {
  const dir = path.dirname(dbPath);
  fs.mkdirSync(dir, { recursive: true });
}

function ensureSchema(db) {
  db.exec(`
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

    CREATE INDEX IF NOT EXISTS idx_option_trades_symbol_ts
      ON option_trades(symbol, trade_ts_utc);

    CREATE TABLE IF NOT EXISTS option_trade_day_cache (
      symbol TEXT NOT NULL,
      trade_date_utc TEXT NOT NULL,
      cache_status TEXT NOT NULL CHECK (cache_status IN ('full', 'partial')),
      row_count INTEGER NOT NULL DEFAULT 0,
      last_sync_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      last_error TEXT,
      source_endpoint TEXT,
      PRIMARY KEY (symbol, trade_date_utc)
    );

    CREATE TABLE IF NOT EXISTS option_trade_metric_day_cache (
      symbol TEXT NOT NULL,
      trade_date_utc TEXT NOT NULL,
      metric_name TEXT NOT NULL,
      cache_status TEXT NOT NULL CHECK (cache_status IN ('full', 'partial')),
      row_count INTEGER NOT NULL DEFAULT 0,
      last_sync_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      last_error TEXT,
      PRIMARY KEY (symbol, trade_date_utc, metric_name)
    );

    CREATE TABLE IF NOT EXISTS stock_ohlc_minute_raw (
      symbol TEXT NOT NULL,
      trade_date_utc TEXT NOT NULL,
      minute_bucket_utc TEXT NOT NULL,
      open REAL,
      high REAL,
      low REAL,
      close REAL NOT NULL,
      volume REAL,
      source_endpoint TEXT,
      raw_payload_json TEXT NOT NULL,
      ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (symbol, trade_date_utc, minute_bucket_utc)
    );

    CREATE TABLE IF NOT EXISTS option_open_interest_raw (
      symbol TEXT NOT NULL,
      trade_date_utc TEXT NOT NULL,
      expiration TEXT NOT NULL,
      strike REAL NOT NULL,
      option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
      oi INTEGER NOT NULL,
      source_endpoint TEXT,
      raw_payload_json TEXT NOT NULL,
      ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (symbol, trade_date_utc, expiration, strike, option_right)
    );

    CREATE TABLE IF NOT EXISTS option_quote_minute_raw (
      symbol TEXT NOT NULL,
      trade_date_utc TEXT NOT NULL,
      expiration TEXT NOT NULL,
      strike REAL NOT NULL,
      option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
      minute_bucket_utc TEXT NOT NULL,
      bid REAL,
      ask REAL,
      last REAL,
      bid_size INTEGER,
      ask_size INTEGER,
      source_endpoint TEXT,
      raw_payload_json TEXT NOT NULL,
      ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc)
    );

    CREATE TABLE IF NOT EXISTS option_greeks_minute_raw (
      symbol TEXT NOT NULL,
      trade_date_utc TEXT NOT NULL,
      expiration TEXT NOT NULL,
      strike REAL NOT NULL,
      option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
      minute_bucket_utc TEXT NOT NULL,
      delta REAL,
      implied_vol REAL,
      gamma REAL,
      theta REAL,
      vega REAL,
      rho REAL,
      underlying_price REAL,
      source_endpoint TEXT,
      raw_payload_json TEXT NOT NULL,
      ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc)
    );

    CREATE TABLE IF NOT EXISTS option_trade_enriched (
      trade_id TEXT PRIMARY KEY,
      trade_ts_utc TEXT NOT NULL,
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
      execution_side TEXT,
      symbol_vol_1m REAL,
      symbol_vol_baseline_15m REAL,
      open_window_baseline REAL,
      bullish_ratio_15m REAL,
      chips_json TEXT NOT NULL DEFAULT '[]',
      rule_version TEXT,
      score_quality TEXT NOT NULL DEFAULT 'partial' CHECK (score_quality IN ('complete', 'partial')),
      missing_metrics_json TEXT NOT NULL DEFAULT '[]',
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

    CREATE TABLE IF NOT EXISTS option_symbol_minute_derived (
      symbol TEXT NOT NULL,
      trade_date_utc TEXT NOT NULL,
      minute_bucket_utc TEXT NOT NULL,
      trade_count INTEGER NOT NULL DEFAULT 0,
      contract_count INTEGER NOT NULL DEFAULT 0,
      total_size INTEGER NOT NULL DEFAULT 0,
      total_value REAL NOT NULL DEFAULT 0,
      call_size INTEGER NOT NULL DEFAULT 0,
      put_size INTEGER NOT NULL DEFAULT 0,
      bullish_count INTEGER NOT NULL DEFAULT 0,
      bearish_count INTEGER NOT NULL DEFAULT 0,
      neutral_count INTEGER NOT NULL DEFAULT 0,
      avg_sig_score REAL,
      max_sig_score REAL,
      avg_vol_oi_ratio REAL,
      max_vol_oi_ratio REAL,
      max_repeat3m INTEGER,
      oi_sum INTEGER NOT NULL DEFAULT 0,
      day_volume_sum INTEGER NOT NULL DEFAULT 0,
      chip_hits_json TEXT NOT NULL DEFAULT '{}',
      updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (symbol, trade_date_utc, minute_bucket_utc)
    );

    CREATE TABLE IF NOT EXISTS option_contract_minute_derived (
      symbol TEXT NOT NULL,
      expiration TEXT NOT NULL,
      strike REAL NOT NULL,
      option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
      trade_date_utc TEXT NOT NULL,
      minute_bucket_utc TEXT NOT NULL,
      trade_count INTEGER NOT NULL DEFAULT 0,
      size_sum INTEGER NOT NULL DEFAULT 0,
      value_sum REAL NOT NULL DEFAULT 0,
      avg_price REAL,
      last_price REAL,
      day_volume INTEGER,
      oi INTEGER,
      vol_oi_ratio REAL,
      avg_sig_score REAL,
      max_sig_score REAL,
      max_repeat3m INTEGER,
      bullish_count INTEGER NOT NULL DEFAULT 0,
      bearish_count INTEGER NOT NULL DEFAULT 0,
      neutral_count INTEGER NOT NULL DEFAULT 0,
      chip_hits_json TEXT NOT NULL DEFAULT '{}',
      updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc)
    );

    CREATE TABLE IF NOT EXISTS filter_rule_versions (
      version_id TEXT PRIMARY KEY,
      config_json TEXT NOT NULL,
      checksum TEXT NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 0 CHECK (is_active IN (0, 1)),
      created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      activated_at_utc TEXT
    );

    CREATE TABLE IF NOT EXISTS supplemental_metric_cache (
      metric_kind TEXT NOT NULL,
      cache_key TEXT NOT NULL,
      value_json TEXT NOT NULL,
      expires_at_utc TEXT NOT NULL,
      updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (metric_kind, cache_key)
    );

    CREATE TABLE IF NOT EXISTS feature_baseline_intraday (
      symbol TEXT NOT NULL,
      minute_of_day_et INTEGER NOT NULL,
      feature_name TEXT NOT NULL,
      sample_count INTEGER NOT NULL DEFAULT 0,
      mean REAL NOT NULL DEFAULT 0,
      m2 REAL NOT NULL DEFAULT 0,
      updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (symbol, minute_of_day_et, feature_name)
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

    CREATE INDEX IF NOT EXISTS idx_stock_ohlc_minute_raw_symbol_date
      ON stock_ohlc_minute_raw(symbol, trade_date_utc, minute_bucket_utc);

    CREATE INDEX IF NOT EXISTS idx_option_open_interest_raw_symbol_date
      ON option_open_interest_raw(symbol, trade_date_utc, expiration, strike, option_right);

    CREATE INDEX IF NOT EXISTS idx_option_quote_minute_raw_symbol_date
      ON option_quote_minute_raw(symbol, trade_date_utc, minute_bucket_utc);

    CREATE INDEX IF NOT EXISTS idx_option_quote_minute_raw_contract_date
      ON option_quote_minute_raw(symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc);

    CREATE INDEX IF NOT EXISTS idx_option_greeks_minute_raw_symbol_date
      ON option_greeks_minute_raw(symbol, trade_date_utc, minute_bucket_utc);

    CREATE INDEX IF NOT EXISTS idx_option_greeks_minute_raw_contract_date
      ON option_greeks_minute_raw(symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc);

    CREATE INDEX IF NOT EXISTS idx_option_symbol_minute_derived_symbol_date
      ON option_symbol_minute_derived(symbol, trade_date_utc DESC, minute_bucket_utc DESC);

    CREATE INDEX IF NOT EXISTS idx_option_symbol_minute_derived_sig
      ON option_symbol_minute_derived(symbol, trade_date_utc DESC, max_sig_score DESC);

    CREATE INDEX IF NOT EXISTS idx_option_contract_minute_derived_symbol_date
      ON option_contract_minute_derived(symbol, trade_date_utc DESC, minute_bucket_utc DESC);

    CREATE INDEX IF NOT EXISTS idx_option_contract_minute_derived_contract_date
      ON option_contract_minute_derived(symbol, expiration, strike, option_right, trade_date_utc DESC, minute_bucket_utc DESC);
  `);

  ensureSchemaMigrations(db);
}

function getTableColumns(db, tableName) {
  return db.prepare(`PRAGMA table_info(${tableName})`).all().map((row) => row.name);
}

function ensureColumn(db, tableName, columnName, columnDefinition) {
  const columns = new Set(getTableColumns(db, tableName));
  if (columns.has(columnName)) return;
  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnDefinition}`);
}

function ensureSchemaMigrations(db) {
  const enrichedColumns = [
    ['trade_ts_utc', 'TEXT'],
    ['price', 'REAL'],
    ['size', 'INTEGER'],
    ['bid', 'REAL'],
    ['ask', 'REAL'],
    ['condition_code', 'TEXT'],
    ['exchange', 'TEXT'],
    ['execution_side', 'TEXT'],
    ['symbol_vol_1m', 'REAL'],
    ['symbol_vol_baseline_15m', 'REAL'],
    ['open_window_baseline', 'REAL'],
    ['bullish_ratio_15m', 'REAL'],
  ];

  enrichedColumns.forEach(([columnName, columnDefinition]) => {
    ensureColumn(db, 'option_trade_enriched', columnName, columnDefinition);
  });

  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_option_trade_enriched_symbol_ts
      ON option_trade_enriched(symbol, trade_ts_utc);
  `);

  const enrichedV3Columns = [
    ['is_sweep', 'INTEGER DEFAULT 0'],
    ['is_multileg', 'INTEGER DEFAULT 0'],
    ['minute_of_day_et', 'INTEGER'],
    ['delta', 'REAL'],
    ['implied_vol', 'REAL'],
    ['time_norm', 'REAL'],
    ['delta_norm', 'REAL'],
    ['iv_skew_norm', 'REAL'],
    ['score_quality', "TEXT NOT NULL DEFAULT 'partial'"],
    ['missing_metrics_json', "TEXT NOT NULL DEFAULT '[]'"],
  ];

  enrichedV3Columns.forEach(([columnName, columnDefinition]) => {
    ensureColumn(db, 'option_trade_enriched', columnName, columnDefinition);
  });

  const enrichedV5Columns = [
    ['value_shock_norm', 'REAL'],
    ['dte_swing_norm', 'REAL'],
    ['flow_imbalance_norm', 'REAL'],
    ['delta_pressure_norm', 'REAL'],
    ['cp_oi_pressure_norm', 'REAL'],
    ['iv_skew_surface_norm', 'REAL'],
    ['iv_term_slope_norm', 'REAL'],
    ['underlying_trend_confirm_norm', 'REAL'],
    ['liquidity_quality_norm', 'REAL'],
    ['multileg_penalty_norm', 'REAL'],
    ['sig_score_components_json', "TEXT NOT NULL DEFAULT '{}'"],
  ];

  enrichedV5Columns.forEach(([columnName, columnDefinition]) => {
    ensureColumn(db, 'option_trade_enriched', columnName, columnDefinition);
  });

  const symbolMinuteDerivedColumns = [
    ['spot', 'REAL'],
    ['avg_sig_score_bullish', 'REAL'],
    ['avg_sig_score_bearish', 'REAL'],
    ['net_sig_score', 'REAL'],
    ['value_weighted_sig_score', 'REAL'],
    ['sweep_count', 'INTEGER DEFAULT 0'],
    ['sweep_value_ratio', 'REAL'],
    ['multileg_count', 'INTEGER DEFAULT 0'],
    ['multileg_pct', 'REAL'],
    ['avg_minute_of_day_et', 'REAL'],
    ['avg_iv', 'REAL'],
    ['call_iv_avg', 'REAL'],
    ['put_iv_avg', 'REAL'],
    ['iv_spread', 'REAL'],
    ['net_delta_dollars', 'REAL'],
    ['avg_value_pctile', 'REAL'],
    ['avg_vol_oi_norm', 'REAL'],
    ['avg_repeat_norm', 'REAL'],
    ['avg_otm_norm', 'REAL'],
    ['avg_side_confidence', 'REAL'],
    ['avg_dte_norm', 'REAL'],
    ['avg_spread_norm', 'REAL'],
    ['avg_sweep_norm', 'REAL'],
    ['avg_multileg_norm', 'REAL'],
    ['avg_time_norm', 'REAL'],
    ['avg_delta_norm', 'REAL'],
    ['avg_iv_skew_norm', 'REAL'],
    ['avg_value_shock_norm', 'REAL'],
    ['avg_dte_swing_norm', 'REAL'],
    ['avg_flow_imbalance_norm', 'REAL'],
    ['avg_delta_pressure_norm', 'REAL'],
    ['avg_cp_oi_pressure_norm', 'REAL'],
    ['avg_iv_skew_surface_norm', 'REAL'],
    ['avg_iv_term_slope_norm', 'REAL'],
    ['avg_underlying_trend_confirm_norm', 'REAL'],
    ['avg_liquidity_quality_norm', 'REAL'],
    ['avg_multileg_penalty_norm', 'REAL'],
  ];

  symbolMinuteDerivedColumns.forEach(([columnName, columnDefinition]) => {
    ensureColumn(db, 'option_symbol_minute_derived', columnName, columnDefinition);
  });
}

function normalizeIsoTimestamp(rawValue) {
  if (typeof rawValue !== 'string' || !rawValue.trim()) return null;
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function normalizeIsoDate(rawValue) {
  if (typeof rawValue === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(rawValue.trim())) {
    return rawValue.trim();
  }

  if (typeof rawValue === 'string' && /^\d{8}$/.test(rawValue.trim())) {
    const value = rawValue.trim();
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
  }

  if (rawValue === null || rawValue === undefined) return null;
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function normalizeSymbol(rawValue) {
  if (typeof rawValue !== 'string' || !rawValue.trim()) return null;
  return rawValue.trim().toUpperCase();
}

function parseLimit(rawValue) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed < 1) return DEFAULT_LIMIT;
  return Math.min(MAX_LIMIT, Math.trunc(parsed));
}

function toYyyymmdd(isoTs) {
  const d = new Date(isoTs);
  return `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`;
}

function badRequest(message) {
  return { status: 400, error: { code: 'invalid_query', message } };
}

function parseJsonRows(rawJson) {
  const parsed = JSON.parse(rawJson);

  if (Array.isArray(parsed)) {
    return parsed.filter((row) => row && typeof row === 'object');
  }

  if (parsed && Array.isArray(parsed.rows)) {
    return parsed.rows.filter((row) => row && typeof row === 'object');
  }

  if (parsed && Array.isArray(parsed.data)) {
    return parsed.data.filter((row) => row && typeof row === 'object');
  }

  if (parsed && Array.isArray(parsed.response)) {
    const header = Array.isArray(parsed.header) ? parsed.header : [];
    if (!header.length) return [];
    return parsed.response
      .filter((row) => Array.isArray(row))
      .map((values) => {
        const out = {};
        header.forEach((k, index) => {
          out[k] = values[index];
        });
        return out;
      });
  }

  if (parsed && typeof parsed === 'object') {
    const entries = Object.entries(parsed).filter(([, value]) => Array.isArray(value));
    if (entries.length) {
      const rowCount = entries[0][1].length;
      if (rowCount > 0 && entries.every(([, value]) => value.length === rowCount)) {
        return Array.from({ length: rowCount }, (_unused, index) => {
          const out = {};
          entries.forEach(([key, values]) => {
            out[key] = values[index];
          });
          return out;
        });
      }
    }
  }

  return [];
}

function pickField(row, names) {
  for (const name of names) {
    if (row[name] !== undefined && row[name] !== null) return row[name];
  }
  return null;
}

function toNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toInteger(value) {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function toIsoFromAnyTs(value, fallbackIso) {
  if (value === null || value === undefined || value === '') return fallbackIso;

  if (typeof value === 'number') {
    const ms = value > 1e12 ? value : value * 1000;
    const dt = new Date(ms);
    return Number.isNaN(dt.getTime()) ? fallbackIso : dt.toISOString();
  }

  const raw = String(value).trim();

  const numeric = Number(raw);
  if (Number.isFinite(numeric)) {
    const ms = numeric > 1e12 ? numeric : numeric * 1000;
    const dt = new Date(ms);
    if (!Number.isNaN(dt.getTime())) return dt.toISOString();
  }

  const hasOffset = /[zZ]|[+-]\d\d:\d\d$/.test(raw);
  const dt = new Date(hasOffset ? raw : `${raw}Z`);
  if (!Number.isNaN(dt.getTime())) return dt.toISOString();

  return fallbackIso;
}

function floorIsoToMinute(isoValue) {
  if (typeof isoValue !== 'string' || !isoValue.trim()) return null;
  const parsed = new Date(isoValue);
  if (Number.isNaN(parsed.getTime())) return null;
  parsed.setUTCSeconds(0, 0);
  return parsed.toISOString();
}

function floorIsoToChunk(isoValue, chunkMinutes) {
  const parsed = new Date(isoValue);
  const normalizedChunkMinutes = Number.isFinite(Number(chunkMinutes))
    ? Math.max(1, Math.trunc(Number(chunkMinutes)))
    : 1;
  if (Number.isNaN(parsed.getTime())) return null;
  const chunkMs = normalizedChunkMinutes * 60000;
  const flooredMs = Math.floor(parsed.getTime() / chunkMs) * chunkMs;
  return new Date(flooredMs).toISOString();
}

function isoToTimeHms(isoValue) {
  const parsed = new Date(isoValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(11, 19);
}

function addMinutesToIso(isoValue, minutes) {
  const parsed = new Date(isoValue);
  const minuteDelta = Number(minutes);
  if (Number.isNaN(parsed.getTime()) || !Number.isFinite(minuteDelta)) return null;
  parsed.setUTCMinutes(parsed.getUTCMinutes() + Math.trunc(minuteDelta), 0, 0);
  return parsed.toISOString();
}

function buildChunkMapFromMinuteRows(minuteRows = [], chunkMinutes = 10) {
  const map = new Map();
  const normalizedChunkMinutes = Number.isFinite(Number(chunkMinutes))
    ? Math.max(1, Math.trunc(Number(chunkMinutes)))
    : 10;

  minuteRows.forEach((row) => {
    const minuteBucketUtc = String(row?.minuteBucketUtc || '').trim();
    if (!minuteBucketUtc) return;
    const rowCount = Math.max(0, Math.trunc(Number(row?.rowCount || 0)));
    if (rowCount === 0) return;

    const chunkStartUtc = floorIsoToChunk(minuteBucketUtc, normalizedChunkMinutes);
    if (!chunkStartUtc) return;
    const chunkEndUtc = addMinutesToIso(chunkStartUtc, normalizedChunkMinutes);
    if (!chunkEndUtc) return;

    const existing = map.get(chunkStartUtc) || {
      chunkStartUtc,
      chunkEndUtc,
      rowCount: 0,
      minuteCount: 0,
    };
    existing.rowCount += rowCount;
    existing.minuteCount += 1;
    map.set(chunkStartUtc, existing);
  });

  return map;
}

function normalizeThetaRow(row, symbol, dayIso) {
  if (!row || typeof row !== 'object') return null;
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  const expiration = pickField(row, ['expiration', 'exp', 'expiry', 'expiration_date']);
  const strike = toNumber(pickField(row, ['strike', 'strike_price']));
  const right = normalizeRight(String(pickField(row, ['right', 'option_right', 'side']) || ''));
  const price = toNumber(pickField(row, ['price', 'trade_price', 'last']));
  const size = toInteger(pickField(row, ['size', 'trade_size', 'quantity', 'qty']));

  if (!expiration || strike === null || !right || price === null || size === null) return null;

  const tradeTsUtc = toIsoFromAnyTs(pickField(row, ['trade_timestamp', 'trade_ts', 'timestamp', 'time']), fallbackTs);
  const bid = toNumber(pickField(row, ['bid', 'bid_price']));
  const ask = toNumber(pickField(row, ['ask', 'ask_price']));
  const conditionCode = pickField(row, ['condition_code', 'condition', 'sale_condition']);
  const exchange = pickField(row, ['exchange', 'exch']);

  const tradeId = crypto
    .createHash('sha1')
    .update([symbol, expiration, strike, right, tradeTsUtc, price, size, conditionCode || '', exchange || ''].join('|'))
    .digest('hex');

  return {
    tradeId,
    tradeTsUtc,
    tradeTsEt: tradeTsUtc,
    symbol,
    expiration: String(expiration),
    strike,
    optionRight: right,
    price,
    size,
    bid,
    ask,
    conditionCode: conditionCode === null ? null : String(conditionCode),
    exchange: exchange === null ? null : String(exchange),
    rawPayloadJson: JSON.stringify(row),
    watermark: `theta-sync-${dayIso}`,
  };
}

function normalizeThetaRows(rows, symbol, dayIso) {
  return rows
    .map((row) => normalizeThetaRow(row, symbol, dayIso))
    .filter(Boolean);
}

function resolveThetaEndpoint(symbol, yyyymmdd, env = process.env, options = {}) {
  const { startTime = null, endTime = null } = options || {};
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  if (!baseUrl) return null;

  const configuredPath = (env.THETADATA_HISTORICAL_OPTION_PATH || DEFAULT_HISTORICAL_OPTION_PATH).trim();
  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;

  const url = new URL(`${normalizedBase}${normalizedPath}`);
  url.searchParams.set('symbol', symbol);
  url.searchParams.set('expiration', '*');
  url.searchParams.set('date', yyyymmdd);
  if (startTime) {
    url.searchParams.set('start_time', startTime);
  }
  if (endTime) {
    url.searchParams.set('end_time', endTime);
  }
  url.searchParams.set('format', parseHistoricalOptionFormat(env));
  return url.toString();
}

function resolveThetaSpotEndpoint(symbol, dayIso, env = process.env, options = {}) {
  const { startTime = null, endTime = null } = options || {};
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  const configuredPath = (env.THETADATA_SPOT_PATH || DEFAULT_SPOT_PATH).trim();
  if (!baseUrl || !configuredPath) return null;

  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;
  const url = new URL(`${normalizedBase}${normalizedPath}`);

  url.searchParams.set('symbol', symbol);
  url.searchParams.set('date', toYyyymmdd(`${dayIso}T00:00:00.000Z`));
  if (normalizedPath.includes('/history/ohlc')) {
    url.searchParams.set('interval', '1m');
    if (startTime) {
      url.searchParams.set('start_time', startTime);
    }
    if (endTime) {
      url.searchParams.set('end_time', endTime);
    }
  }
  url.searchParams.set('format', 'json');
  return url.toString();
}

function resolveThetaOptionQuoteEndpoint(symbol, dayIso, env = process.env, options = {}) {
  const { startTime = null, endTime = null } = options || {};
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  const configuredPath = (env.THETADATA_OPTION_QUOTE_PATH || DEFAULT_OPTION_QUOTE_PATH).trim();
  if (!baseUrl || !configuredPath) return null;

  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;
  const url = new URL(`${normalizedBase}${normalizedPath}`);

  url.searchParams.set('symbol', symbol);
  url.searchParams.set('expiration', '*');
  url.searchParams.set('date', toYyyymmdd(`${dayIso}T00:00:00.000Z`));
  url.searchParams.set('interval', '1m');
  if (startTime) {
    url.searchParams.set('start_time', startTime);
  }
  if (endTime) {
    url.searchParams.set('end_time', endTime);
  }
  url.searchParams.set('format', parseOptionQuoteFormat(env));
  return url.toString();
}

function resolveThetaCalendarEndpoint(dayIso, env = process.env) {
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  const configuredPath = (env.THETADATA_CALENDAR_PATH || DEFAULT_THETADATA_CALENDAR_PATH).trim();
  if (!baseUrl || !configuredPath) return null;

  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;
  const url = new URL(`${normalizedBase}${normalizedPath}`);

  url.searchParams.set('date', toYyyymmdd(`${dayIso}T00:00:00.000Z`));
  url.searchParams.set('format', 'json');
  return url.toString();
}

function resolveThetaOiEndpoint(row, dayIso, env = process.env) {
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  const configuredPath = (env.THETADATA_OI_PATH || DEFAULT_OI_PATH).trim();
  if (!baseUrl || !configuredPath) return null;

  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;
  const url = new URL(`${normalizedBase}${normalizedPath}`);

  url.searchParams.set('symbol', row.symbol);
  url.searchParams.set('expiration', toYyyymmdd(`${row.expiration}T00:00:00.000Z`));
  url.searchParams.set('strike', String(row.strike));
  url.searchParams.set('right', row.right);
  url.searchParams.set('date', toYyyymmdd(`${dayIso}T00:00:00.000Z`));
  url.searchParams.set('format', 'json');
  return url.toString();
}

function resolveThetaOiBulkEndpoint(symbol, dayIso, env = process.env) {
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  const configuredPath = (env.THETADATA_OI_PATH || DEFAULT_OI_PATH).trim();
  if (!baseUrl || !configuredPath) return null;

  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;
  const url = new URL(`${normalizedBase}${normalizedPath}`);

  url.searchParams.set('symbol', symbol);
  url.searchParams.set('expiration', '*');
  url.searchParams.set('date', toYyyymmdd(`${dayIso}T00:00:00.000Z`));
  url.searchParams.set('format', 'json');
  return url.toString();
}

function normalizeThetaRightParam(right, { allowBoth = false } = {}) {
  const normalized = normalizeRight(right);
  if (normalized === 'CALL') return 'C';
  if (normalized === 'PUT') return 'P';
  if (allowBoth) return 'both';
  return null;
}

function resolveThetaGreeksEndpoint(symbol, expiration, dayIso, env = process.env, options = {}) {
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  if (!baseUrl) return null;

  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const url = new URL(`${normalizedBase}${DEFAULT_GREEKS_PATH}`);
  const strike = options.strike === undefined || options.strike === null ? '*' : String(options.strike);
  const right = normalizeThetaRightParam(options.right, { allowBoth: true });

  url.searchParams.set('symbol', symbol);
  url.searchParams.set('expiration', toYyyymmdd(`${expiration}T00:00:00.000Z`));
  url.searchParams.set('strike', strike);
  url.searchParams.set('right', right || 'both');
  url.searchParams.set('date', toYyyymmdd(`${dayIso}T00:00:00.000Z`));
  url.searchParams.set('interval', '1m');
  url.searchParams.set('format', 'json');
  return url.toString();
}

function extractFirstNumericValue(rawValue, candidateKeys) {
  if (!rawValue || typeof rawValue !== 'object') return null;
  for (const key of candidateKeys) {
    const value = toFiniteNumber(rawValue[key]);
    if (value !== null) return value;
  }
  return null;
}

function extractMetricFromResponse(rawBody, candidateKeys) {
  const parsedRows = parseJsonRows(rawBody);
  for (const row of parsedRows) {
    const value = extractFirstNumericValue(row, candidateKeys);
    if (value !== null) return value;
  }

  try {
    const parsed = JSON.parse(rawBody);
    if (Array.isArray(parsed)) {
      for (const row of parsed) {
        const value = extractFirstNumericValue(row, candidateKeys);
        if (value !== null) return value;
      }
      return null;
    }

    if (parsed && typeof parsed === 'object') {
      const direct = extractFirstNumericValue(parsed, candidateKeys);
      if (direct !== null) return direct;

      const nestedCandidates = ['quote', 'data', 'response', 'result'];
      for (const key of nestedCandidates) {
        const nested = parsed[key];
        if (Array.isArray(nested)) {
          for (const row of nested) {
            const nestedValue = extractFirstNumericValue(row, candidateKeys);
            if (nestedValue !== null) return nestedValue;
          }
        } else {
          const nestedValue = extractFirstNumericValue(nested, candidateKeys);
          if (nestedValue !== null) return nestedValue;
        }
      }
    }
  } catch {
    return null;
  }

  return null;
}

function parseTimeoutMs(env = process.env) {
  const parsed = Number(env.THETADATA_TIMEOUT_MS);
  if (Number.isFinite(parsed) && parsed >= 1000) {
    return Math.trunc(parsed);
  }
  return DEFAULT_THETADATA_TIMEOUT_MS;
}

function parseOptionQuoteTimeoutMs(env = process.env) {
  const parsed = Number(env.THETADATA_OPTION_QUOTE_TIMEOUT_MS);
  if (Number.isFinite(parsed) && parsed === 0) {
    return 0;
  }
  if (Number.isFinite(parsed) && parsed >= 1000) {
    return Math.trunc(parsed);
  }
  return parseStreamIdleTimeoutMs(env);
}

function parseStreamIdleTimeoutMs(env = process.env) {
  const parsed = Number(env.THETADATA_STREAM_IDLE_TIMEOUT_MS);
  if (Number.isFinite(parsed) && parsed === 0) {
    return 0;
  }
  if (Number.isFinite(parsed) && parsed >= 1000) {
    return Math.trunc(parsed);
  }
  return DEFAULT_THETADATA_STREAM_IDLE_TIMEOUT_MS;
}

function parseStreamHeartbeatEveryRows(env = process.env) {
  const parsed = Number(env.THETADATA_STREAM_HEARTBEAT_EVERY_ROWS);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.trunc(parsed));
  }
  return DEFAULT_THETADATA_STREAM_HEARTBEAT_EVERY_ROWS;
}

function parseThetaMaxConnectionsPerProcess(env = process.env) {
  const parsed = Number(env.THETADATA_MAX_CONNECTIONS_PER_PROCESS);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(32, Math.trunc(parsed)));
  }
  return DEFAULT_THETADATA_MAX_CONNECTIONS_PER_PROCESS;
}

function parseThetaMaxPipeliningPerConnection(env = process.env) {
  const parsed = Number(env.THETADATA_MAX_PIPELINING_PER_CONNECTION);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(8, Math.trunc(parsed)));
  }
  return DEFAULT_THETADATA_MAX_PIPELINING_PER_CONNECTION;
}

function shouldForceThetaConnectionClose(env = process.env) {
  return String(env.THETADATA_FORCE_CONNECTION_CLOSE || '1') !== '0';
}

function getThetaFetchDispatcher(env = process.env) {
  if (!UndiciAgent) return null;
  const connections = parseThetaMaxConnectionsPerProcess(env);
  if (!Number.isFinite(connections) || connections <= 0) return null;
  const pipelining = parseThetaMaxPipeliningPerConnection(env);
  const key = `${connections}|${pipelining}`;
  if (thetaFetchDispatcher && thetaFetchDispatcherKey === key) {
    return thetaFetchDispatcher;
  }
  if (thetaFetchDispatcher && typeof thetaFetchDispatcher.close === 'function') {
    try {
      thetaFetchDispatcher.close();
    } catch {
      // Best-effort close during reconfiguration.
    }
  }
  thetaFetchDispatcher = new UndiciAgent({
    connections,
    pipelining: Math.max(1, pipelining),
    keepAliveTimeout: 10_000,
    keepAliveMaxTimeout: 60_000,
  });
  thetaFetchDispatcherKey = key;
  return thetaFetchDispatcher;
}

function parseLargeSymbolWindowMinutes(env = process.env) {
  const parsed = Number(env.THETADATA_LARGE_SYMBOL_WINDOW_MINUTES);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(24 * 60, Math.trunc(parsed)));
  }
  return DEFAULT_THETADATA_LARGE_SYMBOL_WINDOW_MINUTES;
}

function parseClickHouseTradeReadWindowMinutes(env = process.env) {
  const parsed = Number(env.CLICKHOUSE_TRADE_READ_WINDOW_MINUTES);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(24 * 60, Math.trunc(parsed)));
  }
  return DEFAULT_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES;
}

function parseClickHouseTradeReadMinWindowMinutes(env = process.env) {
  const parsed = Number(env.CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(60, Math.trunc(parsed)));
  }
  return DEFAULT_CLICKHOUSE_TRADE_READ_MIN_WINDOW_MINUTES;
}

function shouldStreamClickHouseEnrichedWrites(env = process.env) {
  return String(env.CLICKHOUSE_ENRICH_STREAM_WRITE || '1') !== '0';
}

function shouldStreamClickHouseEnrichedReads(env = process.env) {
  return String(env.CLICKHOUSE_ENRICH_STREAM_READ || '0') === '1';
}

function shouldIncludeClickHouseGreeksInEnrichment(env = process.env) {
  return String(env.CLICKHOUSE_ENRICH_INCLUDE_GREEKS || '1') !== '0';
}

function parseClickHouseEnrichStreamChunkSize(env = process.env) {
  const parsed = Number(env.CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(250, Math.min(50000, Math.trunc(parsed)));
  }
  return DEFAULT_CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE;
}

function parseClickHouseQuoteStreamChunkSize(env = process.env) {
  const parsed = Number(env.CLICKHOUSE_QUOTE_STREAM_CHUNK_SIZE);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(500, Math.min(50000, Math.trunc(parsed)));
  }
  return DEFAULT_CLICKHOUSE_QUOTE_STREAM_CHUNK_SIZE;
}

function shouldIncludeClickHouseQuoteRawPayload(env = process.env) {
  const raw = String(env.CLICKHOUSE_QUOTE_INCLUDE_RAW_PAYLOAD || '0').trim().toLowerCase();
  return !(raw === '0' || raw === 'false' || raw === 'no');
}

function shouldLogClickHouseEnrichBatchProgress(env = process.env) {
  return String(env.CLICKHOUSE_ENRICH_BATCH_PROGRESS_LOG || '1') !== '0';
}

function parseClickHouseEnrichProgressBatchMinutes(env = process.env) {
  const parsed = Number(env.CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(120, Math.trunc(parsed)));
  }
  return DEFAULT_CLICKHOUSE_ENRICH_PROGRESS_BATCH_MINUTES;
}

function parseClickHouseChunkStatusMinutes(env = process.env) {
  const parsed = Number(env.CLICKHOUSE_CHUNK_STATUS_MINUTES);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(120, Math.trunc(parsed)));
  }
  return DEFAULT_CLICKHOUSE_CHUNK_STATUS_MINUTES;
}

function parseBackfillQuoteGapMaxWindows(env = process.env) {
  const parsed = Number(env.BACKFILL_QUOTE_GAP_MAX_WINDOWS);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(2000, Math.trunc(parsed)));
  }
  return DEFAULT_BACKFILL_QUOTE_GAP_MAX_WINDOWS;
}

function shouldUseInsertOnlyStockQuoteUpserts(env = process.env) {
  const raw = String(env.CLICKHOUSE_INSERT_ONLY_STOCK_QUOTE || '1').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes';
}

function createClickHouseEnrichBatchProgressLogger({
  symbol,
  dayIso,
  env = process.env,
}) {
  if (!shouldLogClickHouseEnrichBatchProgress(env)) return null;
  const batchMinutes = parseClickHouseEnrichProgressBatchMinutes(env);
  if (!Number.isFinite(batchMinutes) || batchMinutes <= 0) return null;

  const batchMs = batchMinutes * 60000;
  const startedAtMs = Date.now();
  let emittedBatches = 0;
  let totalRows = 0;
  let currentBatchStartMs = null;
  let currentBatchRows = 0;
  let currentBatchMinuteBuckets = new Set();

  const emitCurrentBatch = ({ final = false } = {}) => {
    if (!Number.isFinite(currentBatchStartMs)) return;
    const elapsedSec = (Date.now() - startedAtMs) / 1000;
    emittedBatches += 1;
    totalRows += currentBatchRows;
    const rowsPerMinute = currentBatchRows / batchMinutes;

    console.log('[ENRICH_BATCH_PROGRESS]', JSON.stringify({
      symbol,
      dayIso,
      batchIndex: emittedBatches,
      batchMinutes,
      batchStartUtc: new Date(currentBatchStartMs).toISOString(),
      batchEndUtc: new Date(currentBatchStartMs + batchMs).toISOString(),
      rows: currentBatchRows,
      rowsPerMinute: Number(rowsPerMinute.toFixed(2)),
      activeMinutes: currentBatchMinuteBuckets.size,
      totalRows,
      totalRowsPerSec: Number((totalRows / Math.max(1, elapsedSec)).toFixed(2)),
      elapsedSec: Number(elapsedSec.toFixed(1)),
      final,
    }));

    currentBatchStartMs = null;
    currentBatchRows = 0;
    currentBatchMinuteBuckets = new Set();
  };

  return {
    recordRows(rows = []) {
      if (!Array.isArray(rows) || rows.length === 0) return;
      rows.forEach((row) => {
        const minuteBucket = toMinuteBucketUtc(row?.tradeTsUtc);
        if (!minuteBucket) return;
        const minuteMs = Date.parse(minuteBucket);
        if (!Number.isFinite(minuteMs)) return;
        const batchStartMs = Math.floor(minuteMs / batchMs) * batchMs;

        if (currentBatchStartMs === null) {
          currentBatchStartMs = batchStartMs;
        } else if (batchStartMs !== currentBatchStartMs) {
          emitCurrentBatch();
          currentBatchStartMs = batchStartMs;
        }

        currentBatchRows += 1;
        currentBatchMinuteBuckets.add(minuteBucket);
      });
    },
    flush() {
      emitCurrentBatch({ final: true });
    },
  };
}

function buildClickHouseTradeReadWindows(dayIso, windowMinutes) {
  const dayStartMs = Date.parse(`${dayIso}T00:00:00.000Z`);
  if (!Number.isFinite(dayStartMs)) return [];

  const normalizedWindowMinutes = Number.isFinite(windowMinutes)
    ? Math.max(1, Math.min(24 * 60, Math.trunc(windowMinutes)))
    : DEFAULT_CLICKHOUSE_TRADE_READ_WINDOW_MINUTES;
  const windowMs = normalizedWindowMinutes * 60000;
  const dayEndMs = dayStartMs + (24 * 60 * 60000);
  const windows = [];

  for (let fromMs = dayStartMs; fromMs < dayEndMs; fromMs += windowMs) {
    const toMs = Math.min(dayEndMs, fromMs + windowMs);
    windows.push({
      fromIso: new Date(fromMs).toISOString(),
      toIso: new Date(toMs).toISOString(),
    });
  }

  return windows;
}

function parseThetaCalendarTimeoutMs(env = process.env) {
  const parsed = Number(env.THETADATA_CALENDAR_TIMEOUT_MS);
  if (Number.isFinite(parsed) && parsed >= 1000) {
    return Math.trunc(parsed);
  }
  return DEFAULT_THETADATA_CALENDAR_TIMEOUT_MS;
}

function parseThetaCalendarClosePadMinutes(env = process.env) {
  const parsed = Number(env.THETADATA_CALENDAR_CLOSE_PAD_MINUTES);
  if (Number.isFinite(parsed) && parsed >= 0) {
    return Math.max(0, Math.min(240, Math.trunc(parsed)));
  }
  return DEFAULT_THETADATA_CALENDAR_CLOSE_PAD_MINUTES;
}

function shouldEmitBackfillGapTelemetry(env = process.env) {
  const raw = String(env.BACKFILL_GAP_TELEMETRY || '0').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes';
}

function shouldForceTradeSyncOnBackfill(env = process.env) {
  const raw = String(env.BACKFILL_FORCE_TRADE_SYNC || '0').trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes';
}

function shouldEnableBackfillQuoteGapFill(env = process.env) {
  const raw = String(env.BACKFILL_QUOTE_GAP_FILL || '1').trim().toLowerCase();
  return !(raw === '0' || raw === 'false' || raw === 'no');
}

function parseThetaLargeSymbols(env = process.env) {
  const raw = String(env.THETADATA_LARGE_SYMBOLS || DEFAULT_THETADATA_LARGE_SYMBOLS.join(',')).trim();
  if (!raw) return { symbols: new Set(), includeAll: false };

  const lowered = raw.toLowerCase();
  if (lowered === 'all' || raw === '*') {
    return { symbols: new Set(), includeAll: true };
  }

  const symbols = new Set(
    raw
      .split(',')
      .map((token) => normalizeSymbol(token))
      .filter(Boolean),
  );
  return { symbols, includeAll: false };
}

function parseTimeHmsToSecondOfDay(rawValue) {
  if (typeof rawValue !== 'string') return null;
  const value = rawValue.trim();
  const match = value.match(/^(\d{2}):(\d{2}):(\d{2})$/);
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3]);
  if (
    !Number.isInteger(hours) || !Number.isInteger(minutes) || !Number.isInteger(seconds)
    || hours < 0 || hours > 23
    || minutes < 0 || minutes > 59
    || seconds < 0 || seconds > 59
  ) {
    return null;
  }
  return (hours * 3600) + (minutes * 60) + seconds;
}

function formatSecondOfDayAsHms(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  const bounded = Math.max(0, Math.min(86399, Math.trunc(parsed)));
  const hours = Math.trunc(bounded / 3600);
  const minutes = Math.trunc((bounded % 3600) / 60);
  const seconds = bounded % 60;
  return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
}

function parseThetaCalendarSessionWindow(rawBody, env = process.env) {
  const rows = parseJsonRows(rawBody);
  const row = rows.find((entry) => entry && typeof entry === 'object') || null;
  if (!row) return null;

  const sessionType = String(row.type || '').trim().toLowerCase();
  const isTradableSession = sessionType === 'open' || sessionType === 'early_close';
  if (sessionType && !isTradableSession) {
    return {
      isOpen: false,
      type: sessionType,
      openTime: null,
      closeTime: null,
    };
  }
  if (!isTradableSession) return null;

  const openSecond = parseTimeHmsToSecondOfDay(typeof row.open === 'string' ? row.open : null);
  const closeSecond = parseTimeHmsToSecondOfDay(typeof row.close === 'string' ? row.close : null);
  if (openSecond === null || closeSecond === null) {
    return {
      isOpen: true,
      type: sessionType,
      openTime: null,
      closeTime: null,
    };
  }

  const closePadMinutes = parseThetaCalendarClosePadMinutes(env);
  const paddedCloseSecond = Math.min(86399, closeSecond + (closePadMinutes * 60));
  const boundedCloseSecond = Math.max(openSecond, paddedCloseSecond);
  const boundedRegularCloseSecond = Math.max(openSecond, closeSecond);

  return {
    isOpen: true,
    type: sessionType,
    openTime: formatSecondOfDayAsHms(openSecond),
    closeTime: formatSecondOfDayAsHms(boundedCloseSecond),
    regularCloseTime: formatSecondOfDayAsHms(boundedRegularCloseSecond),
    closePadMinutes,
  };
}

async function resolveThetaCalendarSessionWindowForDay(dayIso, { env = process.env } = {}) {
  const normalizedDayIso = normalizeIsoDate(dayIso);
  if (!normalizedDayIso) return null;

  const endpoint = resolveThetaCalendarEndpoint(normalizedDayIso, env);
  if (!endpoint) return null;

  const cacheKey = `${endpoint}|pad:${parseThetaCalendarClosePadMinutes(env)}`;
  if (thetaCalendarSessionCache.has(cacheKey)) {
    return thetaCalendarSessionCache.get(cacheKey);
  }

  let sessionWindow = null;
  try {
    const { response, body, durationMs } = await fetchTextWithTimeout(endpoint, {
      env,
      timeoutMs: parseThetaCalendarTimeoutMs(env),
    });
    if (response.ok) {
      sessionWindow = parseThetaCalendarSessionWindow(body, env);
    }
    logThetaDownload({
      env,
      url: endpoint,
      durationMs,
      status: response.status,
      ok: response.ok,
      rows: sessionWindow ? 1 : 0,
      error: response.ok ? null : `http_${response.status}`,
    });
  } catch {
    sessionWindow = null;
  }

  thetaCalendarSessionCache.set(cacheKey, sessionWindow);
  return sessionWindow;
}

function resolveThetaTimeWindowsForSymbol(symbol, {
  startTime = null,
  sessionStartTime = null,
  sessionEndTime = null,
  env = process.env,
} = {}) {
  const sessionStartSecond = parseTimeHmsToSecondOfDay(sessionStartTime);
  const sessionEndSecondRaw = parseTimeHmsToSecondOfDay(sessionEndTime);
  const hasSessionBounds = sessionStartSecond !== null || sessionEndSecondRaw !== null;
  const lowerBound = sessionStartSecond === null ? 0 : sessionStartSecond;
  const upperBound = sessionEndSecondRaw === null
    ? 86399
    : Math.max(lowerBound, sessionEndSecondRaw);

  const parsedStartSecond = startTime ? parseTimeHmsToSecondOfDay(startTime) : null;
  if (startTime && parsedStartSecond === null) {
    return [{ startTime: startTime || null, endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null }];
  }

  let startSecond = parsedStartSecond;
  if (startSecond === null && hasSessionBounds) {
    startSecond = lowerBound;
  }
  if (startSecond !== null) {
    startSecond = Math.max(lowerBound, Math.min(startSecond, upperBound));
  }

  const windowMinutes = parseLargeSymbolWindowMinutes(env);
  if (windowMinutes <= 0) {
    if (startSecond === null) {
      return [{ startTime: startTime || null, endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null }];
    }
    return [{
      startTime: formatSecondOfDayAsHms(startSecond),
      endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null,
    }];
  }

  const normalizedSymbol = normalizeSymbol(symbol);
  const { symbols, includeAll } = parseThetaLargeSymbols(env);
  if (!includeAll && (!normalizedSymbol || !symbols.has(normalizedSymbol))) {
    if (startSecond === null) {
      return [{ startTime: startTime || null, endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null }];
    }
    return [{
      startTime: formatSecondOfDayAsHms(startSecond),
      endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null,
    }];
  }

  const effectiveStartSecond = startSecond === null ? lowerBound : startSecond;

  const windowSeconds = windowMinutes * 60;
  const spanSeconds = (upperBound - effectiveStartSecond) + 1;
  if (windowSeconds >= spanSeconds) {
    return [{
      startTime: formatSecondOfDayAsHms(effectiveStartSecond),
      endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null,
    }];
  }

  const windows = [];
  for (let cursor = effectiveStartSecond; cursor <= upperBound; cursor += windowSeconds) {
    const windowStart = cursor;
    const windowEnd = Math.min(upperBound, cursor + windowSeconds - 1);
    windows.push({
      startTime: formatSecondOfDayAsHms(windowStart),
      endTime: formatSecondOfDayAsHms(windowEnd),
    });
  }

  return windows;
}

function parseHistoricalOptionFormat(env = process.env) {
  const raw = String(env.THETADATA_HISTORICAL_OPTION_FORMAT || DEFAULT_HISTORICAL_OPTION_FORMAT)
    .trim()
    .toLowerCase();
  if (raw === 'json' || raw === 'ndjson') return raw;
  return DEFAULT_HISTORICAL_OPTION_FORMAT;
}

function parseOptionQuoteFormat(env = process.env) {
  const raw = String(env.THETADATA_OPTION_QUOTE_FORMAT || DEFAULT_OPTION_QUOTE_FORMAT)
    .trim()
    .toLowerCase();
  if (raw === 'json' || raw === 'ndjson') return raw;
  return DEFAULT_OPTION_QUOTE_FORMAT;
}

function shouldTraceThetaDownloads(env = process.env) {
  return String(env.THETADATA_DOWNLOAD_TRACE || '1') !== '0';
}

function inferThetaApiName(url) {
  try {
    const parsed = new URL(url);
    const path = parsed.pathname || '';
    if (path.includes('/option/history/trade_quote')) return 'option_history_trade_quote';
    if (path.includes('/option/history/quote')) return 'option_history_quote';
    if (path.includes('/option/history/trade')) return 'option_history_trade';
    if (path.includes('/option/history/open_interest')) return 'option_history_open_interest';
    if (path.includes('/stock/history/ohlc')) return 'stock_history_ohlc';
    return path || 'unknown';
  } catch {
    return 'unknown';
  }
}

function thetaLogContext(url) {
  try {
    const parsed = new URL(url);
    return {
      symbol: parsed.searchParams.get('symbol') || null,
      date: parsed.searchParams.get('date') || null,
      expiration: parsed.searchParams.get('expiration') || null,
      strike: parsed.searchParams.get('strike') || null,
      right: parsed.searchParams.get('right') || null,
      format: parsed.searchParams.get('format') || null,
    };
  } catch {
    return {
      symbol: null,
      date: null,
      expiration: null,
      strike: null,
      right: null,
      format: null,
    };
  }
}

function logThetaDownload({
  env = process.env,
  url,
  durationMs,
  status,
  ok,
  rows = null,
  error = null,
}) {
  if (!shouldTraceThetaDownloads(env)) return;
  const api = inferThetaApiName(url);
  const context = thetaLogContext(url);
  console.log('[THETA_DOWNLOAD]', JSON.stringify({
    api,
    url,
    symbol: context.symbol,
    date: context.date,
    expiration: context.expiration,
    strike: context.strike,
    right: context.right,
    format: context.format,
    durationMs,
    status,
    ok,
    rows,
    error,
  }));
}

function createThetaStreamHeartbeatLogger({
  env = process.env,
  endpoint,
  stage,
  symbol = null,
  dayIso = null,
}) {
  const everyRows = parseStreamHeartbeatEveryRows(env);
  if (!Number.isFinite(everyRows) || everyRows <= 0) {
    return null;
  }

  let nextRows = everyRows;
  const startedAtMs = Date.now();
  const api = inferThetaApiName(endpoint);
  const context = thetaLogContext(endpoint);

  return ({
    parsedRows = 0,
    fetchedRows = 0,
    insertedRows = 0,
    bufferedRows = 0,
  } = {}) => {
    if (!Number.isFinite(parsedRows) || parsedRows < nextRows) return;

    while (parsedRows >= nextRows) {
      nextRows += everyRows;
    }

    const elapsedSec = (Date.now() - startedAtMs) / 1000;
    console.log('[THETA_STREAM_HEARTBEAT]', JSON.stringify({
      api,
      stage,
      symbol: symbol || context.symbol,
      dayIso: dayIso || context.date,
      format: context.format,
      parsedRows: Math.max(0, Math.trunc(parsedRows)),
      fetchedRows: Math.max(0, Math.trunc(fetchedRows)),
      insertedRows: Math.max(0, Math.trunc(insertedRows)),
      bufferedRows: Math.max(0, Math.trunc(bufferedRows)),
      elapsedSec: Number(elapsedSec.toFixed(1)),
      everyRows,
    }));
  };
}

async function fetchTextWithTimeout(url, {
  env = process.env,
  timeoutMs = parseTimeoutMs(env),
} = {}) {
  if (!url) {
    throw new Error('thetadata_endpoint_missing');
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const startedAt = Date.now();
  const dispatcher = getThetaFetchDispatcher(env);
  const requestOptions = { signal: controller.signal };
  if (dispatcher) requestOptions.dispatcher = dispatcher;
  if (shouldForceThetaConnectionClose(env)) {
    requestOptions.headers = { connection: 'close' };
  }

  try {
    const response = await fetch(url, requestOptions);
    const body = await response.text();
    const durationMs = Date.now() - startedAt;
    return { response, body, durationMs };
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    if (error && error.name === 'AbortError') {
      logThetaDownload({
        env,
        url,
        durationMs,
        status: 0,
        ok: false,
        rows: 0,
        error: `thetadata_request_timeout:${timeoutMs}`,
      });
      throw new Error(`thetadata_request_timeout:${timeoutMs}`);
    }
    logThetaDownload({
      env,
      url,
      durationMs,
      status: 0,
      ok: false,
      rows: 0,
      error: error.message || 'request_failed',
    });
    throw error;
  } finally {
    clearTimeout(timer);
  }
}

function parseThetaUrlFormat(url) {
  try {
    const parsed = new URL(url);
    return String(parsed.searchParams.get('format') || '').trim().toLowerCase();
  } catch {
    return '';
  }
}

async function streamNdjsonRowsFromResponse(response, onRow, { onChunk = null } = {}) {
  if (!response?.body || typeof response.body.getReader !== 'function') {
    const body = await response.text();
    if (typeof onChunk === 'function' && body.length > 0) {
      onChunk(body.length);
    }
    let rowCount = 0;
    parseJsonRows(body).forEach((row) => {
      if (!row || typeof row !== 'object') return;
      onRow(row);
      rowCount += 1;
    });
    return rowCount;
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  let rowCount = 0;

  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (typeof onChunk === 'function' && value && value.length > 0) {
      onChunk(value.length);
    }
    buffer += decoder.decode(value, { stream: true });

    let newlineIndex = buffer.indexOf('\n');
    while (newlineIndex >= 0) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line) {
        try {
          const parsed = JSON.parse(line);
          if (parsed && typeof parsed === 'object') {
            onRow(parsed);
            rowCount += 1;
          }
        } catch {
          // Skip malformed lines and continue parsing the stream.
        }
      }
      newlineIndex = buffer.indexOf('\n');
    }
  }

  buffer += decoder.decode();
  const tail = buffer.trim();
  if (tail) {
    try {
      const parsed = JSON.parse(tail);
      if (parsed && typeof parsed === 'object') {
        onRow(parsed);
        rowCount += 1;
      }
    } catch {
      // Ignore malformed trailing line.
    }
  }

  return rowCount;
}

async function fetchThetaNdjsonRows(url, {
  env = process.env,
  timeoutMs = parseStreamIdleTimeoutMs(env),
  onRow = null,
} = {}) {
  if (!url) {
    throw new Error('thetadata_endpoint_missing');
  }

  const useIdleTimeout = Number.isFinite(timeoutMs) && timeoutMs > 0;
  const controller = new AbortController();
  let timer = null;
  let timeoutKind = null;
  const startedAt = Date.now();
  const resetIdleTimer = () => {
    if (!useIdleTimeout) return;
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timeoutKind = 'idle';
      controller.abort();
    }, timeoutMs);
  };

  try {
    resetIdleTimer();
    const dispatcher = getThetaFetchDispatcher(env);
    const requestOptions = { signal: controller.signal };
    if (dispatcher) requestOptions.dispatcher = dispatcher;
    if (shouldForceThetaConnectionClose(env)) {
      requestOptions.headers = { connection: 'close' };
    }
    const response = await fetch(url, requestOptions);
    let rowCount = 0;
    if (response.ok && typeof onRow === 'function') {
      rowCount = await streamNdjsonRowsFromResponse(response, onRow, { onChunk: resetIdleTimer });
    } else if (response.ok) {
      rowCount = await streamNdjsonRowsFromResponse(response, () => {}, { onChunk: resetIdleTimer });
    } else {
      resetIdleTimer();
      await response.arrayBuffer();
    }
    const durationMs = Date.now() - startedAt;
    return { response, rowCount, durationMs };
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    if (error && error.name === 'AbortError') {
      const timeoutLabel = timeoutKind === 'idle'
        ? 'thetadata_request_idle_timeout'
        : 'thetadata_request_timeout';
      logThetaDownload({
        env,
        url,
        durationMs,
        status: 0,
        ok: false,
        rows: 0,
        error: `${timeoutLabel}:${timeoutMs}`,
      });
      throw new Error(`${timeoutLabel}:${timeoutMs}`);
    }
    logThetaDownload({
      env,
      url,
      durationMs,
      status: 0,
      ok: false,
      rows: 0,
      error: error.message || 'request_failed',
    });
    throw error;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function fetchThetaMetricNumber(url, candidateKeys) {
  if (!url) return null;

  try {
    const { response, body, durationMs } = await fetchTextWithTimeout(url);
    const rows = response.ok ? parseJsonRows(body).length : 0;
    const value = response.ok ? extractMetricFromResponse(body, candidateKeys) : null;
    logThetaDownload({
      url,
      durationMs,
      status: response.status,
      ok: response.ok,
      rows,
      error: response.ok ? null : `http_${response.status}`,
    });
    return value;
  } catch (error) {
    logThetaDownload({
      url,
      durationMs: null,
      status: 0,
      ok: false,
      rows: 0,
      error: error.message || 'request_failed',
    });
    return null;
  }
}

async function fetchThetaRows(url, { env = process.env, timeoutMs = null } = {}) {
  if (!url) return [];

  try {
    const { response, body, durationMs } = await fetchTextWithTimeout(url, {
      env,
      timeoutMs: timeoutMs ?? parseTimeoutMs(env),
    });
    const rows = response.ok ? parseJsonRows(body) : [];
    logThetaDownload({
      env,
      url,
      durationMs,
      status: response.status,
      ok: response.ok,
      rows: rows.length,
      error: response.ok ? null : `http_${response.status}`,
    });
    return rows;
  } catch (error) {
    logThetaDownload({
      env,
      url,
      durationMs: null,
      status: 0,
      ok: false,
      rows: 0,
      error: error.message || 'request_failed',
    });
    return [];
  }
}

function parseIsoMs(value) {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseJsonValue(rawValue, fallback = null) {
  if (typeof rawValue !== 'string') return fallback;
  try {
    return JSON.parse(rawValue);
  } catch {
    return fallback;
  }
}

function parseSupplementalCacheTtlHours(env = process.env) {
  const parsed = Number(env.SUPPLEMENTAL_CACHE_TTL_HOURS);
  if (!Number.isFinite(parsed) || parsed <= 0) return DEFAULT_SUPPLEMENTAL_CACHE_TTL_HOURS;
  return Math.trunc(parsed);
}

function parseSupplementalConcurrency(env = process.env) {
  const parsed = Number(env.THETADATA_SUPPLEMENTAL_CONCURRENCY);
  if (!Number.isFinite(parsed) || parsed < 1) return DEFAULT_SUPPLEMENTAL_CONCURRENCY;
  return Math.max(1, Math.min(64, Math.trunc(parsed)));
}

function parseTrendFallbackMaxLagMinutes(env = process.env) {
  const parsed = Number(env.THETADATA_STOCK_TREND_FALLBACK_MAX_LAG_MINUTES);
  if (!Number.isFinite(parsed) || parsed < 1) return DEFAULT_TREND_FALLBACK_MAX_LAG_MINUTES;
  return Math.max(1, Math.min(24 * 60, Math.trunc(parsed)));
}

function parseGreeksContractFallbackLimit(env = process.env) {
  const parsed = Number(env.THETADATA_GREEKS_CONTRACT_FALLBACK_LIMIT);
  if (!Number.isFinite(parsed) || parsed < 0) return DEFAULT_GREEKS_CONTRACT_FALLBACK_LIMIT;
  return Math.max(0, Math.min(2000, Math.trunc(parsed)));
}

function makeCacheExpiryIso(env = process.env, dayIso = null) {
  if (typeof dayIso === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(dayIso)) {
    const currentDay = new Date().toISOString().slice(0, 10);
    if (dayIso < currentDay) {
      return '9999-12-31T23:59:59.999Z';
    }
  }
  const ttlHours = parseSupplementalCacheTtlHours(env);
  return new Date(Date.now() + (ttlHours * 3600 * 1000)).toISOString();
}

function getSupplementalCache(db, metricKind, cacheKey) {
  const row = db.prepare(`
    SELECT value_json AS valueJson, expires_at_utc AS expiresAtUtc
    FROM supplemental_metric_cache
    WHERE metric_kind = @metricKind
      AND cache_key = @cacheKey
    LIMIT 1
  `).get({ metricKind, cacheKey });

  if (!row) return null;
  const expiresAtMs = parseIsoMs(row.expiresAtUtc);
  if (expiresAtMs === null || expiresAtMs <= Date.now()) return null;
  return parseJsonValue(row.valueJson, null);
}

function upsertSupplementalCache(db, metricKind, cacheKey, value, env = process.env, dayIso = null) {
  db.prepare(`
    INSERT INTO supplemental_metric_cache (
      metric_kind,
      cache_key,
      value_json,
      expires_at_utc,
      updated_at_utc
    ) VALUES (
      @metricKind,
      @cacheKey,
      @valueJson,
      @expiresAtUtc,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(metric_kind, cache_key) DO UPDATE SET
      value_json = excluded.value_json,
      expires_at_utc = excluded.expires_at_utc,
      updated_at_utc = excluded.updated_at_utc
  `).run({
    metricKind,
    cacheKey,
    valueJson: JSON.stringify(value),
    expiresAtUtc: makeCacheExpiryIso(env, dayIso),
  });
}

function loadFeatureBaselines(db, symbol) {
  const rows = db.prepare(`
    SELECT
      minute_of_day_et AS minuteOfDayEt,
      feature_name AS featureName,
      sample_count AS sampleCount,
      mean,
      m2
    FROM feature_baseline_intraday
    WHERE symbol = @symbol
  `).all({ symbol });

  const map = new Map();
  rows.forEach((row) => {
    const key = `${row.minuteOfDayEt}|${row.featureName}`;
    map.set(key, {
      sampleCount: Math.max(0, Math.trunc(toFiniteNumber(row.sampleCount) || 0)),
      mean: toFiniteNumber(row.mean) || 0,
      m2: toFiniteNumber(row.m2) || 0,
    });
  });
  return map;
}

function appendFeatureBaselineSample(updateMap, minuteOfDayEt, featureName, rawValue) {
  const minute = toFiniteNumber(minuteOfDayEt);
  const value = toFiniteNumber(rawValue);
  if (minute === null || value === null) return;
  const minuteInt = Math.max(0, Math.trunc(minute));
  const key = `${minuteInt}|${featureName}`;
  const current = updateMap.get(key) || {
    minuteOfDayEt: minuteInt,
    featureName,
    values: [],
  };
  current.values.push(value);
  updateMap.set(key, current);
}

function getFeatureBaselineStats(baselineMap, minuteOfDayEt, featureName) {
  const minute = toFiniteNumber(minuteOfDayEt);
  if (minute === null) return null;
  const key = `${Math.max(0, Math.trunc(minute))}|${featureName}`;
  const state = baselineMap.get(key);
  if (!state || state.sampleCount < 2) return null;
  const variance = state.m2 / Math.max(1, state.sampleCount - 1);
  return {
    sampleCount: state.sampleCount,
    mean: state.mean,
    std: Math.sqrt(Math.max(variance, 0)),
  };
}

function mergeFeatureBaselineState(current, value) {
  const nextCount = current.sampleCount + 1;
  const delta = value - current.mean;
  const nextMean = current.mean + (delta / nextCount);
  const delta2 = value - nextMean;
  return {
    sampleCount: nextCount,
    mean: nextMean,
    m2: current.m2 + (delta * delta2),
  };
}

function upsertFeatureBaselines(db, symbol, baselineMap, updates) {
  if (!(updates instanceof Map) || updates.size === 0) return;

  const upsert = db.prepare(`
    INSERT INTO feature_baseline_intraday (
      symbol,
      minute_of_day_et,
      feature_name,
      sample_count,
      mean,
      m2,
      updated_at_utc
    ) VALUES (
      @symbol,
      @minuteOfDayEt,
      @featureName,
      @sampleCount,
      @mean,
      @m2,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, minute_of_day_et, feature_name) DO UPDATE SET
      sample_count = excluded.sample_count,
      mean = excluded.mean,
      m2 = excluded.m2,
      updated_at_utc = excluded.updated_at_utc
  `);

  const rows = [];
  updates.forEach((update) => {
    const key = `${update.minuteOfDayEt}|${update.featureName}`;
    let state = baselineMap.get(key) || { sampleCount: 0, mean: 0, m2: 0 };
    update.values.forEach((sample) => {
      state = mergeFeatureBaselineState(state, sample);
    });
    baselineMap.set(key, state);
    rows.push({
      symbol,
      minuteOfDayEt: update.minuteOfDayEt,
      featureName: update.featureName,
      sampleCount: state.sampleCount,
      mean: state.mean,
      m2: state.m2,
    });
  });

  const txn = db.transaction((items) => {
    items.forEach((row) => upsert.run(row));
  });
  txn(rows);
}

function replaceRowsInSqlite(db, tableName, columns, rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;

  const placeholders = columns.map((column) => `@${column}`).join(', ');
  const statement = db.prepare(`
    INSERT OR REPLACE INTO ${tableName} (${columns.join(', ')})
    VALUES (${placeholders})
  `);

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((row) => {
      writes += statement.run(row).changes;
    });
    return writes;
  });

  return txn(rows);
}

function seedClickHouseStateIntoSqlite(
  db,
  {
    symbol,
    dayIso,
    env = process.env,
    queryRows = queryClickHouseRowsSync,
    includeDayState = true,
    includeQuoteRows = true,
  },
) {
  ensureSchema(db);

  const ruleRows = loadClickHouseRuleVersionRows(env, queryRows).map((row) => ({
    version_id: row.versionId,
    config_json: row.configJson,
    checksum: row.checksum,
    is_active: Number(row.isActive || 0),
    created_at_utc: row.createdAtUtc || new Date().toISOString(),
    activated_at_utc: row.activatedAtUtc || null,
  }));
  replaceRowsInSqlite(
    db,
    'filter_rule_versions',
    ['version_id', 'config_json', 'checksum', 'is_active', 'created_at_utc', 'activated_at_utc'],
    ruleRows,
  );

  const baselineRows = loadClickHouseFeatureBaselineRows({ symbol, env, queryRows }).map((row) => ({
    symbol: row.symbol,
    minute_of_day_et: Number(row.minuteOfDayEt || 0),
    feature_name: row.featureName,
    sample_count: Number(row.sampleCount || 0),
    mean: toFiniteNumber(row.mean) || 0,
    m2: toFiniteNumber(row.m2) || 0,
    updated_at_utc: new Date().toISOString(),
  }));
  replaceRowsInSqlite(
    db,
    'feature_baseline_intraday',
    ['symbol', 'minute_of_day_et', 'feature_name', 'sample_count', 'mean', 'm2', 'updated_at_utc'],
    baselineRows,
  );

  let tradeRows = [];
  let stockRows = [];
  let oiRows = [];
  let quoteRows = [];
  let greeksRows = [];

  if (includeDayState) {
    tradeRows = loadClickHouseTradeRowsForDay({
      symbol,
      dayIso,
      env,
      queryRows,
      includeRawPayloadJson: true,
    });
    upsertOptionTrades(db, tradeRows);

    stockRows = loadClickHouseStockRawRowsForDay({ symbol, dayIso, env, queryRows });
    if (stockRows.length > 0) {
      upsertStockOhlcMinuteRaw(
        db,
        symbol,
        dayIso,
        stockRows.map((row) => ({
          symbol: row.symbol,
          minuteBucketUtc: row.minuteBucketUtc,
          open: toFiniteNumber(row.open),
          high: toFiniteNumber(row.high),
          low: toFiniteNumber(row.low),
          close: toFiniteNumber(row.close),
          volume: toFiniteNumber(row.volume),
          rawPayloadJson: row.rawPayloadJson,
        })),
        stockRows.find((row) => row.sourceEndpoint)?.sourceEndpoint || null,
      );
    }

    oiRows = loadClickHouseOptionOiRawRowsForDay({ symbol, dayIso, env, queryRows });
    if (oiRows.length > 0) {
      upsertOptionOpenInterestRaw(
        db,
        dayIso,
        oiRows.map((row) => ({
          symbol: row.symbol,
          expiration: row.expiration,
          strike: toFiniteNumber(row.strike),
          right: row.right,
          oi: Math.max(0, Math.trunc(toFiniteNumber(row.oi) || 0)),
          rawPayloadJson: row.rawPayloadJson,
        })),
        oiRows.find((row) => row.sourceEndpoint)?.sourceEndpoint || null,
      );
    }

    if (includeQuoteRows) {
      quoteRows = loadClickHouseOptionQuoteRawRowsForDay({ symbol, dayIso, env, queryRows });
      if (quoteRows.length > 0) {
        upsertOptionQuoteMinuteRaw(
          db,
          dayIso,
          quoteRows.map((row) => ({
            symbol: row.symbol,
            expiration: row.expiration,
            strike: toFiniteNumber(row.strike),
            right: row.right,
            minuteBucketUtc: row.minuteBucketUtc,
            bid: toFiniteNumber(row.bid),
            ask: toFiniteNumber(row.ask),
            last: toFiniteNumber(row.last),
            bidSize: Number.isFinite(Number(row.bidSize)) ? Math.trunc(Number(row.bidSize)) : null,
            askSize: Number.isFinite(Number(row.askSize)) ? Math.trunc(Number(row.askSize)) : null,
            rawPayloadJson: row.rawPayloadJson,
          })),
          quoteRows.find((row) => row.sourceEndpoint)?.sourceEndpoint || null,
        );
      }
    }

    greeksRows = loadClickHouseOptionGreeksRawRowsForDay({ symbol, dayIso, env, queryRows });
    if (greeksRows.length > 0) {
      upsertOptionGreeksMinuteRaw(
        db,
        dayIso,
        greeksRows.map((row) => ({
          symbol: row.symbol,
          expiration: row.expiration,
          strike: toFiniteNumber(row.strike),
          right: row.right,
          minuteBucketUtc: row.minuteBucketUtc,
          delta: toFiniteNumber(row.delta),
          impliedVol: toFiniteNumber(row.impliedVol),
          gamma: toFiniteNumber(row.gamma),
          theta: toFiniteNumber(row.theta),
          vega: toFiniteNumber(row.vega),
          rho: toFiniteNumber(row.rho),
          underlyingPrice: toFiniteNumber(row.underlyingPrice),
          rawPayloadJson: row.rawPayloadJson,
        })),
        greeksRows.find((row) => row.sourceEndpoint)?.sourceEndpoint || null,
      );
    }
  }

  return {
    tradeRows: tradeRows.length,
    stockRows: stockRows.length,
    oiRows: oiRows.length,
    quoteRows: quoteRows.length,
    greeksRows: greeksRows.length,
    baselineRows: baselineRows.length,
    ruleRows: ruleRows.length,
  };
}

function parseClickHouseDeleteMutationSync(env = process.env) {
  const parsed = Number(env.CLICKHOUSE_DELETE_MUTATION_SYNC);
  if (Number.isFinite(parsed) && parsed >= 0 && parsed <= 2) {
    return Math.trunc(parsed);
  }
  return 1;
}

function deleteClickHouseScope(tableName, whereSql, params, env = process.env) {
  const mutationSync = parseClickHouseDeleteMutationSync(env);
  execClickHouseQuerySync(`
    ALTER TABLE options.${tableName}
    DELETE WHERE ${whereSql}
    SETTINGS mutations_sync = ${mutationSync}
  `, params, env);
}

const CLICKHOUSE_INSERT_ONLY_REPLACING_TABLES = new Set([
  'option_trade_day_cache',
  'option_trade_metric_day_cache',
  'supplemental_metric_cache',
  'feature_baseline_intraday',
]);

function requiresClickHouseDeleteBeforeInsert(tableName) {
  return !CLICKHOUSE_INSERT_ONLY_REPLACING_TABLES.has(tableName);
}

function insertClickHouseRows(tableName, columns, rows, env = process.env, options = {}) {
  if (!rows) return 0;
  return insertClickHouseJsonRowsSync(
    `INSERT INTO options.${tableName} (${columns.join(', ')})`,
    rows,
    env,
    options,
  );
}

function persistSqliteCacheStateToClickHouse(db, { symbol, dayIso, env = process.env }) {
  ensureClickHouseSupportSchema(env);

  if (requiresClickHouseDeleteBeforeInsert('option_trade_day_cache')) {
    deleteClickHouseScope('option_trade_day_cache', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
  }
  const dayCacheRows = db.prepare(`
    SELECT
      symbol,
      trade_date_utc,
      cache_status,
      row_count,
      last_sync_at_utc,
      last_error,
      source_endpoint
    FROM option_trade_day_cache
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).all({ symbol, dayIso });
  insertClickHouseRows(
    'option_trade_day_cache',
    ['symbol', 'trade_date_utc', 'cache_status', 'row_count', 'last_sync_at_utc', 'last_error', 'source_endpoint'],
    dayCacheRows,
    env,
  );

  if (requiresClickHouseDeleteBeforeInsert('option_trade_metric_day_cache')) {
    deleteClickHouseScope('option_trade_metric_day_cache', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
  }
  const metricCacheRows = db.prepare(`
    SELECT
      symbol,
      trade_date_utc,
      metric_name,
      cache_status,
      row_count,
      last_sync_at_utc,
      last_error
    FROM option_trade_metric_day_cache
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).all({ symbol, dayIso });
  insertClickHouseRows(
    'option_trade_metric_day_cache',
    ['symbol', 'trade_date_utc', 'metric_name', 'cache_status', 'row_count', 'last_sync_at_utc', 'last_error'],
    metricCacheRows,
    env,
  );
}

function persistSqliteDayStateToClickHouse(
  db,
  {
    symbol,
    dayIso,
    env = process.env,
    skipRawState = false,
    skipEnrichedState = false,
  },
) {
  ensureClickHouseSupportSchema(env);
  persistSqliteCacheStateToClickHouse(db, { symbol, dayIso, env });

  if (!skipRawState) {
    deleteClickHouseScope('option_trades', 'symbol = {symbol:String} AND trade_date = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'option_trades',
      ['trade_id', 'trade_ts_utc', 'trade_ts_et', 'symbol', 'expiration', 'strike', 'option_right', 'price', 'size', 'bid', 'ask', 'condition_code', 'exchange', 'raw_payload_json', 'watermark', 'ingested_at_utc'],
      db.prepare(`
        SELECT *
        FROM option_trades
        WHERE symbol = @symbol
          AND trade_ts_utc >= @from
          AND trade_ts_utc <= @to
        ORDER BY trade_ts_utc ASC, trade_id ASC
      `).iterate({
        symbol,
        from: `${dayIso}T00:00:00.000Z`,
        to: `${dayIso}T23:59:59.999Z`,
      }),
      env,
    );

    deleteClickHouseScope('stock_ohlc_minute_raw', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'stock_ohlc_minute_raw',
      ['symbol', 'trade_date_utc', 'minute_bucket_utc', 'open', 'high', 'low', 'close', 'volume', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
      db.prepare('SELECT * FROM stock_ohlc_minute_raw WHERE symbol = @symbol AND trade_date_utc = @dayIso ORDER BY minute_bucket_utc ASC').iterate({ symbol, dayIso }),
      env,
    );

    deleteClickHouseScope('option_open_interest_raw', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'option_open_interest_raw',
      ['symbol', 'trade_date_utc', 'expiration', 'strike', 'option_right', 'oi', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
      db.prepare('SELECT * FROM option_open_interest_raw WHERE symbol = @symbol AND trade_date_utc = @dayIso ORDER BY expiration ASC, strike ASC, option_right ASC').iterate({ symbol, dayIso }),
      env,
    );

    deleteClickHouseScope('option_quote_minute_raw', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'option_quote_minute_raw',
      ['symbol', 'trade_date_utc', 'expiration', 'strike', 'option_right', 'minute_bucket_utc', 'bid', 'ask', 'last', 'bid_size', 'ask_size', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
      db.prepare('SELECT * FROM option_quote_minute_raw WHERE symbol = @symbol AND trade_date_utc = @dayIso ORDER BY minute_bucket_utc ASC, expiration ASC, strike ASC, option_right ASC').iterate({ symbol, dayIso }),
      env,
      { chunkSize: 2500 },
    );

    deleteClickHouseScope('option_greeks_minute_raw', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'option_greeks_minute_raw',
      ['symbol', 'trade_date_utc', 'expiration', 'strike', 'option_right', 'minute_bucket_utc', 'delta', 'implied_vol', 'gamma', 'theta', 'vega', 'rho', 'underlying_price', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
      db.prepare('SELECT * FROM option_greeks_minute_raw WHERE symbol = @symbol AND trade_date_utc = @dayIso ORDER BY minute_bucket_utc ASC, expiration ASC, strike ASC, option_right ASC').iterate({ symbol, dayIso }),
      env,
      { chunkSize: 2500 },
    );
  }

  if (!skipEnrichedState) {
    deleteClickHouseScope('option_trade_enriched', 'symbol = {symbol:String} AND trade_date = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'option_trade_enriched',
      ['trade_id', 'trade_ts_utc', 'symbol', 'expiration', 'strike', 'option_right', 'price', 'size', 'bid', 'ask', 'condition_code', 'exchange', 'value', 'dte', 'spot', 'otm_pct', 'day_volume', 'oi', 'vol_oi_ratio', 'repeat3m', 'sig_score', 'sentiment', 'execution_side', 'symbol_vol_1m', 'symbol_vol_baseline_15m', 'open_window_baseline', 'bullish_ratio_15m', 'chips_json', 'rule_version', 'score_quality', 'missing_metrics_json', 'enriched_at_utc', 'is_sweep', 'is_multileg', 'minute_of_day_et', 'delta', 'implied_vol', 'time_norm', 'delta_norm', 'iv_skew_norm', 'value_shock_norm', 'dte_swing_norm', 'flow_imbalance_norm', 'delta_pressure_norm', 'cp_oi_pressure_norm', 'iv_skew_surface_norm', 'iv_term_slope_norm', 'underlying_trend_confirm_norm', 'liquidity_quality_norm', 'multileg_penalty_norm', 'sig_score_components_json'],
      db.prepare(`
        SELECT *
        FROM option_trade_enriched
        WHERE symbol = @symbol
          AND trade_ts_utc >= @from
          AND trade_ts_utc <= @to
        ORDER BY trade_ts_utc ASC, trade_id ASC
      `).iterate({
        symbol,
        from: `${dayIso}T00:00:00.000Z`,
        to: `${dayIso}T23:59:59.999Z`,
      }),
      env,
    );

    deleteClickHouseScope('option_symbol_minute_derived', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'option_symbol_minute_derived',
      ['symbol', 'trade_date_utc', 'minute_bucket_utc', 'trade_count', 'contract_count', 'total_size', 'total_value', 'call_size', 'put_size', 'bullish_count', 'bearish_count', 'neutral_count', 'avg_sig_score', 'max_sig_score', 'avg_vol_oi_ratio', 'max_vol_oi_ratio', 'max_repeat3m', 'oi_sum', 'day_volume_sum', 'chip_hits_json', 'updated_at_utc', 'spot', 'avg_sig_score_bullish', 'avg_sig_score_bearish', 'net_sig_score', 'value_weighted_sig_score', 'sweep_count', 'sweep_value_ratio', 'multileg_count', 'multileg_pct', 'avg_minute_of_day_et', 'avg_iv', 'call_iv_avg', 'put_iv_avg', 'iv_spread', 'net_delta_dollars', 'avg_value_pctile', 'avg_vol_oi_norm', 'avg_repeat_norm', 'avg_otm_norm', 'avg_side_confidence', 'avg_dte_norm', 'avg_spread_norm', 'avg_sweep_norm', 'avg_multileg_norm', 'avg_time_norm', 'avg_delta_norm', 'avg_iv_skew_norm', 'avg_value_shock_norm', 'avg_dte_swing_norm', 'avg_flow_imbalance_norm', 'avg_delta_pressure_norm', 'avg_cp_oi_pressure_norm', 'avg_iv_skew_surface_norm', 'avg_iv_term_slope_norm', 'avg_underlying_trend_confirm_norm', 'avg_liquidity_quality_norm', 'avg_multileg_penalty_norm'],
      db.prepare('SELECT * FROM option_symbol_minute_derived WHERE symbol = @symbol AND trade_date_utc = @dayIso ORDER BY minute_bucket_utc ASC').iterate({ symbol, dayIso }),
      env,
    );

    deleteClickHouseScope('option_contract_minute_derived', 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'option_contract_minute_derived',
      ['symbol', 'expiration', 'strike', 'option_right', 'trade_date_utc', 'minute_bucket_utc', 'trade_count', 'size_sum', 'value_sum', 'avg_price', 'last_price', 'day_volume', 'oi', 'vol_oi_ratio', 'avg_sig_score', 'max_sig_score', 'max_repeat3m', 'bullish_count', 'bearish_count', 'neutral_count', 'chip_hits_json', 'updated_at_utc'],
      db.prepare('SELECT * FROM option_contract_minute_derived WHERE symbol = @symbol AND trade_date_utc = @dayIso ORDER BY minute_bucket_utc ASC, expiration ASC, strike ASC, option_right ASC').iterate({ symbol, dayIso }),
      env,
    );

    deleteClickHouseScope('contract_stats_intraday', 'symbol = {symbol:String} AND session_date = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'contract_stats_intraday',
      ['symbol', 'expiration', 'strike', 'option_right', 'session_date', 'day_volume', 'oi', 'last_trade_ts_utc', 'updated_at_utc'],
      db.prepare('SELECT * FROM contract_stats_intraday WHERE symbol = @symbol AND session_date = @dayIso ORDER BY expiration ASC, strike ASC, option_right ASC').iterate({ symbol, dayIso }),
      env,
    );

    deleteClickHouseScope('symbol_stats_intraday', 'symbol = {symbol:String} AND toDate(minute_bucket_et) = toDate({dayIso:String})', { symbol, dayIso }, env);
    insertClickHouseRows(
      'symbol_stats_intraday',
      ['symbol', 'minute_bucket_et', 'vol_1m', 'vol_baseline_15m', 'open_window_baseline', 'bullish_ratio_15m', 'updated_at_utc'],
      db.prepare(`
        SELECT *
        FROM symbol_stats_intraday
        WHERE symbol = @symbol
          AND minute_bucket_et >= @from
          AND minute_bucket_et <= @to
        ORDER BY minute_bucket_et ASC
      `).iterate({
        symbol,
        from: `${dayIso}T00:00:00.000Z`,
        to: `${dayIso}T23:59:59.999Z`,
      }),
      env,
    );

    if (requiresClickHouseDeleteBeforeInsert('feature_baseline_intraday')) {
      deleteClickHouseScope('feature_baseline_intraday', 'symbol = {symbol:String}', { symbol }, env);
    }
    insertClickHouseRows(
      'feature_baseline_intraday',
      ['symbol', 'minute_of_day_et', 'feature_name', 'sample_count', 'mean', 'm2', 'updated_at_utc'],
      db.prepare('SELECT * FROM feature_baseline_intraday WHERE symbol = @symbol ORDER BY minute_of_day_et ASC, feature_name ASC').iterate({ symbol }),
      env,
    );

    const supplementalDayKey = `${symbol}|${dayIso}`;
    if (requiresClickHouseDeleteBeforeInsert('supplemental_metric_cache')) {
      deleteClickHouseScope(
        'supplemental_metric_cache',
        '(cache_key = {dayKey:String} OR startsWith(cache_key, {dayPrefix:String}))',
        { dayKey: supplementalDayKey, dayPrefix: `${supplementalDayKey}|` },
        env,
      );
    }
    insertClickHouseRows(
      'supplemental_metric_cache',
      ['metric_kind', 'cache_key', 'value_json', 'expires_at_utc', 'updated_at_utc'],
      db.prepare(`
        SELECT *
        FROM supplemental_metric_cache
        WHERE cache_key = @dayKey
           OR cache_key LIKE @dayPrefix
        ORDER BY metric_kind ASC, cache_key ASC
      `).iterate({
        dayKey: supplementalDayKey,
        dayPrefix: `${supplementalDayKey}|%`,
      }),
      env,
    );
  }
}

async function parallelMapLimit(items, limit, mapper) {
  if (!Array.isArray(items) || !items.length) return [];
  const concurrency = Math.max(1, Math.trunc(limit || 1));
  const out = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      out[index] = await mapper(items[index], index);
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return out;
}

function normalizeStockOhlcRows(rows, dayIso) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  return rows.map((row) => {
    const ts = toIsoFromAnyTs(
      pickField(row, ['timestamp', 'time', 'datetime', 'trade_timestamp', 'trade_ts', 'ms_of_day']),
      fallbackTs,
    );
    const close = toFiniteNumber(pickField(row, ['close', 'c', 'last', 'price']));
    const open = toFiniteNumber(pickField(row, ['open', 'o']));
    const high = toFiniteNumber(pickField(row, ['high', 'h']));
    const low = toFiniteNumber(pickField(row, ['low', 'l']));
    const volume = toFiniteNumber(pickField(row, ['volume', 'v']));
    if (!ts || close === null) return null;
    return {
      ts,
      minuteBucketUtc: toMinuteBucketUtc(ts),
      open,
      high,
      low,
      close,
      volume,
    };
  }).filter((row) => row && row.minuteBucketUtc);
}

function buildStockFeaturesByMinute(normalizedBars = []) {
  if (!normalizedBars.length) return new Map();

  const sorted = normalizedBars
    .slice()
    .sort((left, right) => Date.parse(left.minuteBucketUtc) - Date.parse(right.minuteBucketUtc));

  const byMinute = new Map();
  const trailing = [];

  sorted.forEach((bar, index) => {
    const prev = index > 0 ? sorted[index - 1] : null;
    let ret1m = 0;
    if (prev && prev.close > 0) {
      ret1m = Math.log(Math.max(bar.close, 0.0001) / Math.max(prev.close, 0.0001));
    }
    trailing.push(ret1m);
    while (trailing.length > 30) trailing.shift();

    const lagIndex = Math.max(0, index - 30);
    const lagBar = sorted[lagIndex];
    let ret30m = 0;
    if (lagBar && lagBar.close > 0) {
      ret30m = Math.log(Math.max(bar.close, 0.0001) / Math.max(lagBar.close, 0.0001));
    }

    const mean = trailing.length ? trailing.reduce((acc, value) => acc + value, 0) / trailing.length : 0;
    const variance = trailing.length
      ? trailing.reduce((acc, value) => acc + ((value - mean) ** 2), 0) / trailing.length
      : 0;
    const vol30m = Math.sqrt(Math.max(variance, 0));

    byMinute.set(bar.minuteBucketUtc, {
      close: bar.close,
      ret1m,
      ret30m,
      vol30m,
      trendSignal: vol30m > 0 ? (ret30m / vol30m) : 0,
    });
  });

  return byMinute;
}

function upsertStockOhlcMinuteRaw(db, symbol, dayIso, rows = [], sourceEndpoint = null) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const upsert = db.prepare(`
    INSERT INTO stock_ohlc_minute_raw (
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
    ) VALUES (
      @symbol,
      @dayIso,
      @minuteBucketUtc,
      @open,
      @high,
      @low,
      @close,
      @volume,
      @sourceEndpoint,
      @rawPayloadJson,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, trade_date_utc, minute_bucket_utc) DO UPDATE SET
      open = excluded.open,
      high = excluded.high,
      low = excluded.low,
      close = excluded.close,
      volume = excluded.volume,
      source_endpoint = excluded.source_endpoint,
      raw_payload_json = excluded.raw_payload_json,
      ingested_at_utc = excluded.ingested_at_utc
  `);

  const payload = rows.map((row) => ({
    symbol,
    dayIso,
    minuteBucketUtc: row.minuteBucketUtc,
    open: row.open,
    high: row.high,
    low: row.low,
    close: row.close,
    volume: row.volume,
    sourceEndpoint,
    rawPayloadJson: JSON.stringify(row.rawPayload || row),
  }));

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((item) => {
      writes += upsert.run(item).changes;
    });
    return writes;
  });
  return txn(payload);
}

function loadStockOhlcMinuteRaw(db, { symbol, dayIso }) {
  return db.prepare(`
    SELECT
      minute_bucket_utc AS minuteBucketUtc,
      open,
      high,
      low,
      close,
      volume
    FROM stock_ohlc_minute_raw
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
    ORDER BY minute_bucket_utc ASC
  `).all({ symbol, dayIso });
}

function countStockOhlcMinuteRaw(db, { symbol, dayIso }) {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM stock_ohlc_minute_raw
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).get({ symbol, dayIso });
  return Number(row?.count || 0);
}

function normalizeOptionOpenInterestRows(rows = []) {
  return rows.map((row) => {
    const symbol = normalizeSymbol(pickField(row, ['symbol', 'root', 'underlying']));
    const expiration = normalizeIsoDate(pickField(row, ['expiration', 'exp', 'expiration_date']));
    const strike = toFiniteNumber(pickField(row, ['strike', 'strike_price']));
    const right = normalizeRight(pickField(row, ['right', 'option_right', 'side']));
    const oi = toFiniteNumber(pickField(row, ['open_interest', 'openInterest', 'oi']));
    if (!symbol || !expiration || strike === null || !right || oi === null) return null;
    return {
      symbol,
      expiration,
      strike,
      right,
      oi: Math.trunc(oi),
      rawPayloadJson: JSON.stringify(row),
    };
  }).filter(Boolean);
}

function upsertOptionOpenInterestRaw(db, dayIso, rows = [], sourceEndpoint = null) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const upsert = db.prepare(`
    INSERT INTO option_open_interest_raw (
      symbol,
      trade_date_utc,
      expiration,
      strike,
      option_right,
      oi,
      source_endpoint,
      raw_payload_json,
      ingested_at_utc
    ) VALUES (
      @symbol,
      @dayIso,
      @expiration,
      @strike,
      @right,
      @oi,
      @sourceEndpoint,
      @rawPayloadJson,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, trade_date_utc, expiration, strike, option_right) DO UPDATE SET
      oi = excluded.oi,
      source_endpoint = excluded.source_endpoint,
      raw_payload_json = excluded.raw_payload_json,
      ingested_at_utc = excluded.ingested_at_utc
  `);

  const payload = rows.map((row) => ({
    ...row,
    dayIso,
    sourceEndpoint,
  }));

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((item) => {
      writes += upsert.run(item).changes;
    });
    return writes;
  });
  return txn(payload);
}

function loadContractOiFromRaw(db, { symbol, dayIso }) {
  const rows = db.prepare(`
    SELECT
      symbol,
      expiration,
      strike,
      option_right AS right,
      oi
    FROM option_open_interest_raw
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).all({ symbol, dayIso });

  const byContract = new Map();
  rows.forEach((row) => {
    const oi = toFiniteNumber(row.oi);
    if (oi === null) return;
    byContract.set(buildContractKey(row), Math.trunc(oi));
  });
  return byContract;
}

function countOptionOpenInterestRaw(db, { symbol, dayIso }) {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM option_open_interest_raw
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).get({ symbol, dayIso });
  return Number(row?.count || 0);
}

function normalizeOptionQuoteRows(rows = [], dayIso, options = {}) {
  return rows.map((row) => normalizeOptionQuoteRow(row, dayIso, options)).filter(Boolean);
}

function normalizeOptionQuoteRow(row, dayIso, { includeRawPayload = true } = {}) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  const symbol = normalizeSymbol(pickField(row, ['symbol', 'root', 'underlying']));
  const expiration = normalizeIsoDate(pickField(row, ['expiration', 'exp', 'expiration_date']));
  const strike = toFiniteNumber(pickField(row, ['strike', 'strike_price']));
  const right = normalizeRight(pickField(row, ['right', 'option_right', 'side']));
  const ts = toIsoFromAnyTs(
    pickField(row, ['timestamp', 'time', 'datetime', 'quote_timestamp', 'trade_timestamp', 'trade_ts']),
    fallbackTs,
  );
  const minuteBucketUtc = toMinuteBucketUtc(ts);
  if (!symbol || !expiration || strike === null || !right || !minuteBucketUtc) return null;
  return {
    symbol,
    expiration,
    strike,
    right,
    minuteBucketUtc,
    bid: toFiniteNumber(pickField(row, ['bid', 'bid_price'])),
    ask: toFiniteNumber(pickField(row, ['ask', 'ask_price'])),
    last: toFiniteNumber(pickField(row, ['last', 'price', 'mark', 'mid'])),
    bidSize: toInteger(pickField(row, ['bid_size', 'bidSize', 'bidsize'])),
    askSize: toInteger(pickField(row, ['ask_size', 'askSize', 'asksize'])),
    rawPayloadJson: includeRawPayload ? JSON.stringify(row) : '{}',
  };
}

function countOptionQuoteMinuteRaw(db, { symbol, dayIso }) {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM option_quote_minute_raw
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).get({ symbol, dayIso });
  return Number(row?.count || 0);
}

function upsertOptionQuoteMinuteRaw(db, dayIso, rows = [], sourceEndpoint = null) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const upsert = db.prepare(`
    INSERT INTO option_quote_minute_raw (
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
    ) VALUES (
      @symbol,
      @dayIso,
      @expiration,
      @strike,
      @right,
      @minuteBucketUtc,
      @bid,
      @ask,
      @last,
      @bidSize,
      @askSize,
      @sourceEndpoint,
      @rawPayloadJson,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc) DO UPDATE SET
      bid = excluded.bid,
      ask = excluded.ask,
      last = excluded.last,
      bid_size = excluded.bid_size,
      ask_size = excluded.ask_size,
      source_endpoint = excluded.source_endpoint,
      raw_payload_json = excluded.raw_payload_json,
      ingested_at_utc = excluded.ingested_at_utc
  `);

  const payload = rows.map((row) => ({
    ...row,
    dayIso,
    sourceEndpoint,
  }));

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((item) => {
      writes += upsert.run(item).changes;
    });
    return writes;
  });
  return txn(payload);
}

function normalizeOptionGreeksRows(rows = [], dayIso) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  return rows.map((row) => {
    const symbol = normalizeSymbol(pickField(row, ['symbol', 'root', 'underlying']));
    const expiration = normalizeIsoDate(pickField(row, ['expiration', 'exp', 'expiration_date']));
    const strike = toFiniteNumber(pickField(row, ['strike', 'strike_price']));
    const right = normalizeRight(pickField(row, ['right', 'option_right', 'side']));
    const rawTs = pickField(row, ['timestamp', 'trade_timestamp', 'datetime', 'time']);
    const ts = normalizeThetaTimestamp(rawTs) || toIsoFromAnyTs(rawTs, fallbackTs);
    const minuteBucketUtc = toMinuteBucketUtc(ts);
    if (!symbol || !expiration || strike === null || !right || !minuteBucketUtc) return null;
    return {
      symbol,
      expiration,
      strike,
      right,
      minuteBucketUtc,
      delta: toFiniteNumber(pickField(row, ['delta'])),
      impliedVol: toFiniteNumber(pickField(row, ['implied_vol', 'impliedVol', 'iv'])),
      gamma: toFiniteNumber(pickField(row, ['gamma'])),
      theta: toFiniteNumber(pickField(row, ['theta'])),
      vega: toFiniteNumber(pickField(row, ['vega'])),
      rho: toFiniteNumber(pickField(row, ['rho'])),
      underlyingPrice: toFiniteNumber(pickField(row, ['underlying_price', 'underlyingPrice', 'spot'])),
      rawPayloadJson: JSON.stringify(row),
    };
  }).filter(Boolean);
}

function upsertOptionGreeksMinuteRaw(db, dayIso, rows = [], sourceEndpoint = null) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  const upsert = db.prepare(`
    INSERT INTO option_greeks_minute_raw (
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
    ) VALUES (
      @symbol,
      @dayIso,
      @expiration,
      @strike,
      @right,
      @minuteBucketUtc,
      @delta,
      @impliedVol,
      @gamma,
      @theta,
      @vega,
      @rho,
      @underlyingPrice,
      @sourceEndpoint,
      @rawPayloadJson,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc) DO UPDATE SET
      delta = excluded.delta,
      implied_vol = excluded.implied_vol,
      gamma = excluded.gamma,
      theta = excluded.theta,
      vega = excluded.vega,
      rho = excluded.rho,
      underlying_price = excluded.underlying_price,
      source_endpoint = excluded.source_endpoint,
      raw_payload_json = excluded.raw_payload_json,
      ingested_at_utc = excluded.ingested_at_utc
  `);

  const payload = rows.map((row) => ({
    ...row,
    dayIso,
    sourceEndpoint,
  }));

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((item) => {
      writes += upsert.run(item).changes;
    });
    return writes;
  });
  return txn(payload);
}

function loadOptionGreeksMinuteRaw(db, { symbol, dayIso }) {
  return db.prepare(`
    SELECT
      symbol,
      expiration,
      strike,
      option_right AS right,
      minute_bucket_utc AS minuteBucketUtc,
      delta,
      implied_vol AS impliedVol,
      gamma,
      theta,
      vega,
      rho,
      underlying_price AS underlyingPrice
    FROM option_greeks_minute_raw
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
    ORDER BY minute_bucket_utc ASC, expiration ASC, strike ASC, option_right ASC
  `).all({ symbol, dayIso });
}

function countOptionGreeksMinuteRaw(db, { symbol, dayIso }) {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM option_greeks_minute_raw
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).get({ symbol, dayIso });
  return Number(row?.count || 0);
}

function upsertDayCache(db, {
  symbol,
  dayIso,
  cacheStatus,
  rowCount = 0,
  lastError = null,
  sourceEndpoint = null,
}) {
  db.prepare(`
    INSERT INTO option_trade_day_cache (
      symbol,
      trade_date_utc,
      cache_status,
      row_count,
      last_sync_at_utc,
      last_error,
      source_endpoint
    ) VALUES (
      @symbol,
      @dayIso,
      @cacheStatus,
      @rowCount,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      @lastError,
      @sourceEndpoint
    )
    ON CONFLICT(symbol, trade_date_utc) DO UPDATE SET
      cache_status = excluded.cache_status,
      row_count = excluded.row_count,
      last_sync_at_utc = excluded.last_sync_at_utc,
      last_error = excluded.last_error,
      source_endpoint = excluded.source_endpoint
  `).run({ symbol, dayIso, cacheStatus, rowCount, lastError, sourceEndpoint });
}

function getDayCache(db, { symbol, dayIso }) {
  const row = db.prepare(`
    SELECT
      symbol,
      trade_date_utc AS tradeDateUtc,
      cache_status AS cacheStatus,
      row_count AS rowCount,
      last_sync_at_utc AS lastSyncAtUtc,
      last_error AS lastError,
      source_endpoint AS sourceEndpoint
    FROM option_trade_day_cache
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).get({ symbol, dayIso });

  return row || null;
}

function upsertMetricCache(db, {
  symbol,
  dayIso,
  metricName,
  cacheStatus,
  rowCount = 0,
  lastError = null,
}) {
  db.prepare(`
    INSERT INTO option_trade_metric_day_cache (
      symbol,
      trade_date_utc,
      metric_name,
      cache_status,
      row_count,
      last_sync_at_utc,
      last_error
    ) VALUES (
      @symbol,
      @dayIso,
      @metricName,
      @cacheStatus,
      @rowCount,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      @lastError
    )
    ON CONFLICT(symbol, trade_date_utc, metric_name) DO UPDATE SET
      cache_status = excluded.cache_status,
      row_count = excluded.row_count,
      last_sync_at_utc = excluded.last_sync_at_utc,
      last_error = excluded.last_error
  `).run({ symbol, dayIso, metricName, cacheStatus, rowCount, lastError });
}

function getMetricCacheMap(db, { symbol, dayIso }) {
  const rows = db.prepare(`
    SELECT
      metric_name AS metricName,
      cache_status AS cacheStatus,
      row_count AS rowCount,
      last_error AS lastError,
      last_sync_at_utc AS lastSyncAtUtc
    FROM option_trade_metric_day_cache
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).all({ symbol, dayIso });

  return rows.reduce((acc, row) => {
    acc[row.metricName] = row;
    return acc;
  }, {});
}

function getClickHouseDayCache({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      symbol,
      toString(trade_date_utc) AS tradeDateUtc,
      argMax(cache_status, last_sync_at_utc) AS cacheStatus,
      argMax(row_count, last_sync_at_utc) AS rowCount,
      concat(replaceAll(toString(max(last_sync_at_utc), 'UTC'), ' ', 'T'), 'Z') AS lastSyncAtUtc,
      argMax(last_error, last_sync_at_utc) AS lastError,
      argMax(source_endpoint, last_sync_at_utc) AS sourceEndpoint
    FROM options.option_trade_day_cache
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY symbol, trade_date_utc
  `, { symbol, dayIso }, env);

  return rows[0] || null;
}

function getClickHouseMetricCacheMap({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      metric_name AS metricName,
      argMax(cache_status, last_sync_at_utc) AS cacheStatus,
      argMax(row_count, last_sync_at_utc) AS rowCount,
      argMax(last_error, last_sync_at_utc) AS lastError,
      concat(replaceAll(toString(max(last_sync_at_utc), 'UTC'), ' ', 'T'), 'Z') AS lastSyncAtUtc
    FROM options.option_trade_metric_day_cache
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY metric_name
    ORDER BY metric_name ASC
  `, { symbol, dayIso }, env);

  return rows.reduce((acc, row) => {
    acc[row.metricName] = row;
    return acc;
  }, {});
}

const CLICKHOUSE_SUPPORT_TABLE_DDLS = Object.freeze([
  `
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
    ORDER BY (symbol, expiration, strike, option_right, session_date)
  `,
  `
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
    ORDER BY (symbol, minute_bucket_et)
  `,
  `
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
    ORDER BY version_id
  `,
  `
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
    ORDER BY (metric_kind, cache_key)
  `,
  `
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
    ORDER BY (symbol, minute_of_day_et, feature_name)
  `,
  `
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
    ORDER BY (symbol, trade_date_utc, stream_name, chunk_start_utc)
  `,
  `
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
    ORDER BY (symbol, trade_date_utc, stream_name, chunk_start_utc)
  `,
  `
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
    ORDER BY (source, as_of_date, symbol, expiration, strike, option_right)
  `,
]);

function ensureClickHouseSupportSchema(env = process.env) {
  CLICKHOUSE_SUPPORT_TABLE_DDLS.forEach((ddl) => {
    execClickHouseQuerySync(ddl, {}, env);
  });
}

function listClickHouseCachedDays(env = process.env, queryRows = queryClickHouseRowsSync) {
  return queryRows(`
    SELECT
      symbol,
      toString(trade_date_utc) AS dayIso,
      argMax(row_count, last_sync_at_utc) AS rowCount
    FROM options.option_trade_day_cache
    GROUP BY symbol, trade_date_utc
    HAVING argMax(cache_status, last_sync_at_utc) = 'full'
    ORDER BY symbol, trade_date_utc
  `, {}, env);
}

function listClickHouseMetricCacheRows({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  return queryRows(`
    SELECT
      metric_name AS metricName,
      argMax(cache_status, last_sync_at_utc) AS cacheStatus,
      argMax(row_count, last_sync_at_utc) AS rowCount,
      argMax(last_error, last_sync_at_utc) AS lastError,
      concat(replaceAll(toString(max(last_sync_at_utc), 'UTC'), ' ', 'T'), 'Z') AS lastSyncAtUtc
    FROM options.option_trade_metric_day_cache
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY metric_name
    ORDER BY metric_name ASC
  `, { symbol, dayIso }, env);
}

function loadClickHouseTradeRowsForDay({
  symbol,
  dayIso,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
  includeRawPayloadJson = false,
  onChunk = null,
}) {
  const rawPayloadSelectSql = includeRawPayloadJson
    ? 'raw_payload_json AS rawPayloadJson,'
    : '\'{}\' AS rawPayloadJson,';
  const baseSelectSql = `
    SELECT
      trade_id AS tradeId,
      concat(replaceAll(toString(trade_ts_utc, 'UTC'), ' ', 'T'), 'Z') AS tradeTsUtc,
      concat(replaceAll(toString(trade_ts_utc, 'UTC'), ' ', 'T'), 'Z') AS tradeTsEt,
      symbol,
      toString(expiration) AS expiration,
      strike,
      option_right AS optionRight,
      price,
      size,
      bid,
      ask,
      condition_code AS conditionCode,
      exchange,
      ${rawPayloadSelectSql}
      coalesce(
        JSONExtract(raw_payload_json, 'spot', 'Nullable(Float64)'),
        JSONExtract(raw_payload_json, 'underlying_price', 'Nullable(Float64)'),
        JSONExtract(raw_payload_json, 'underlyingPrice', 'Nullable(Float64)'),
        JSONExtract(raw_payload_json, 'price', 'Nullable(Float64)'),
        JSONExtract(raw_payload_json, 'last', 'Nullable(Float64)'),
        JSONExtract(raw_payload_json, 'close', 'Nullable(Float64)')
      ) AS payloadSpot,
      coalesce(
        JSONExtract(raw_payload_json, 'oi', 'Nullable(Float64)'),
        JSONExtract(raw_payload_json, 'open_interest', 'Nullable(Float64)'),
        JSONExtract(raw_payload_json, 'openInterest', 'Nullable(Float64)')
      ) AS payloadOi,
      watermark
    FROM options.option_trades
  `;
  const orderBySql = 'ORDER BY trade_ts_utc ASC, trade_id ASC';
  const emitChunk = typeof onChunk === 'function'
    ? onChunk
    : null;
  const minWindowMinutes = parseClickHouseTradeReadMinWindowMinutes(env);
  const rows = emitChunk ? null : [];

  const emitRows = (chunkRows) => {
    if (!Array.isArray(chunkRows) || chunkRows.length === 0) return;
    if (emitChunk) {
      emitChunk(chunkRows);
      return;
    }
    chunkRows.forEach((row) => rows.push(row));
  };

  const isOversizedWindowError = (error) => {
    const message = String(error?.message || '').toLowerCase();
    return message.includes('cannot create a string longer than 0x1fffffe8 characters')
      || message.includes('invalid string length')
      || message.includes('string longer than')
      || message.includes('allocation failed - javascript heap out of memory');
  };

  const splitWindow = (fromIso, toIso) => {
    const fromMs = Date.parse(fromIso);
    const toMs = Date.parse(toIso);
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) {
      return null;
    }
    const midMs = fromMs + Math.floor((toMs - fromMs) / 2);
    if (!Number.isFinite(midMs) || midMs <= fromMs || midMs >= toMs) return null;
    return {
      left: { fromIso, toIso: new Date(midMs).toISOString() },
      right: { fromIso: new Date(midMs).toISOString(), toIso },
    };
  };

  const queryWindow = ({ fromIso, toIso, activeWindowMinutes }) => {
    try {
      const chunkRows = queryRows(`
        ${baseSelectSql}
        WHERE symbol = {symbol:String}
          AND trade_ts_utc >= parseDateTime64BestEffortOrNull({fromIso:String}, 3, 'UTC')
          AND trade_ts_utc < parseDateTime64BestEffortOrNull({toIso:String}, 3, 'UTC')
        ${orderBySql}
      `, { symbol, fromIso, toIso }, env);
      emitRows(chunkRows);
      return;
    } catch (error) {
      const canSplit = Number.isFinite(activeWindowMinutes)
        && activeWindowMinutes > minWindowMinutes;
      if (!canSplit || !isOversizedWindowError(error)) {
        throw error;
      }
      const split = splitWindow(fromIso, toIso);
      if (!split) {
        throw error;
      }
      const nextWindowMinutes = Math.max(minWindowMinutes, Math.trunc(activeWindowMinutes / 2));
      queryWindow({ ...split.left, activeWindowMinutes: nextWindowMinutes });
      queryWindow({ ...split.right, activeWindowMinutes: nextWindowMinutes });
    }
  };

  const windowMinutes = parseClickHouseTradeReadWindowMinutes(env);
  if (windowMinutes <= 0 || windowMinutes >= 24 * 60) {
    const rows = queryRows(`
      ${baseSelectSql}
      WHERE symbol = {symbol:String}
        AND trade_date = toDate({dayIso:String})
      ${orderBySql}
    `, { symbol, dayIso }, env);
    if (emitChunk) {
      if (rows.length > 0) emitChunk(rows);
      return [];
    }
    return rows;
  }

  const windows = buildClickHouseTradeReadWindows(dayIso, windowMinutes);
  windows.forEach(({ fromIso, toIso }) => {
    queryWindow({ fromIso, toIso, activeWindowMinutes: windowMinutes });
  });

  return rows || [];
}

function loadClickHouseStockRawRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  return queryRows(`
    SELECT
      symbol,
      concat(replaceAll(toString(minute_bucket_utc, 'UTC'), ' ', 'T'), 'Z') AS minuteBucketUtc,
      argMax(open, ingested_at_utc) AS open,
      argMax(high, ingested_at_utc) AS high,
      argMax(low, ingested_at_utc) AS low,
      argMax(close, ingested_at_utc) AS close,
      argMax(volume, ingested_at_utc) AS volume,
      argMax(source_endpoint, ingested_at_utc) AS sourceEndpoint,
      argMax(raw_payload_json, ingested_at_utc) AS rawPayloadJson
    FROM options.stock_ohlc_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY symbol, minute_bucket_utc
    ORDER BY minute_bucket_utc ASC
  `, { symbol, dayIso }, env);
}

function loadClickHouseOptionOiRawRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  return queryRows(`
    SELECT
      symbol,
      toString(expiration) AS expiration,
      strike,
      option_right AS right,
      argMax(oi, ingested_at_utc) AS oi,
      argMax(source_endpoint, ingested_at_utc) AS sourceEndpoint,
      argMax(raw_payload_json, ingested_at_utc) AS rawPayloadJson
    FROM options.option_open_interest_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY symbol, expiration, strike, option_right
    ORDER BY expiration ASC, strike ASC, option_right ASC
  `, { symbol, dayIso }, env);
}

function loadClickHouseOptionQuoteRawRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  return queryRows(`
    SELECT
      symbol,
      toString(expiration) AS expiration,
      strike,
      option_right AS right,
      concat(replaceAll(toString(minute_bucket_utc, 'UTC'), ' ', 'T'), 'Z') AS minuteBucketUtc,
      argMax(bid, ingested_at_utc) AS bid,
      argMax(ask, ingested_at_utc) AS ask,
      argMax(last, ingested_at_utc) AS last,
      argMax(bid_size, ingested_at_utc) AS bidSize,
      argMax(ask_size, ingested_at_utc) AS askSize,
      argMax(source_endpoint, ingested_at_utc) AS sourceEndpoint,
      argMax(raw_payload_json, ingested_at_utc) AS rawPayloadJson
    FROM options.option_quote_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY symbol, expiration, strike, option_right, minute_bucket_utc
    ORDER BY minute_bucket_utc ASC, expiration ASC, strike ASC, option_right ASC
  `, { symbol, dayIso }, env);
}

function countClickHouseOptionQuoteRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT count() AS count
    FROM options.option_quote_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
  `, { symbol, dayIso }, env);
  return Number(rows?.[0]?.count || 0);
}

function getClickHouseOptionQuoteResumeCursor({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      ifNull(concat(replaceAll(toString(max(minute_bucket_utc), 'UTC'), ' ', 'T'), 'Z'), null) AS maxMinuteBucketUtc
    FROM options.option_quote_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
  `, { symbol, dayIso }, env);

  const maxMinuteBucketUtc = rows?.[0]?.maxMinuteBucketUtc || null;
  if (!maxMinuteBucketUtc) return null;
  const resumeFromIso = floorIsoToMinute(maxMinuteBucketUtc);
  if (!resumeFromIso) return null;
  const startTime = isoToTimeHms(resumeFromIso);
  if (!startTime) return null;
  return { startTime, resumeFromIso };
}

function loadClickHouseOptionGreeksRawRowsForDay({
  symbol,
  dayIso,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
  onChunk = null,
  includeRawPayloadJson = true,
}) {
  const rawPayloadSelectSql = includeRawPayloadJson
    ? 'raw_payload_json AS rawPayloadJson'
    : '\'{}\' AS rawPayloadJson';
  const baseSelectSql = `
    SELECT
      symbol,
      toString(expiration) AS expiration,
      strike,
      option_right AS right,
      concat(replaceAll(toString(minute_bucket_utc, 'UTC'), ' ', 'T'), 'Z') AS minuteBucketUtc,
      delta,
      implied_vol AS impliedVol,
      gamma,
      theta,
      vega,
      rho,
      underlying_price AS underlyingPrice,
      source_endpoint AS sourceEndpoint,
      ${rawPayloadSelectSql}
    FROM options.option_greeks_minute_raw
  `;
  const orderBySql = 'ORDER BY minute_bucket_utc ASC, expiration ASC, strike ASC, option_right ASC';
  const emitChunk = typeof onChunk === 'function' ? onChunk : null;
  const minWindowMinutes = parseClickHouseTradeReadMinWindowMinutes(env);
  const rows = emitChunk ? null : [];

  const emitRows = (chunkRows) => {
    if (!Array.isArray(chunkRows) || chunkRows.length === 0) return;
    if (emitChunk) {
      emitChunk(chunkRows);
      return;
    }
    chunkRows.forEach((row) => rows.push(row));
  };

  const isOversizedWindowError = (error) => {
    const message = String(error?.message || '').toLowerCase();
    return message.includes('cannot create a string longer than 0x1fffffe8 characters')
      || message.includes('invalid string length')
      || message.includes('string longer than')
      || message.includes('allocation failed - javascript heap out of memory');
  };

  const splitWindow = (fromIso, toIso) => {
    const fromMs = Date.parse(fromIso);
    const toMs = Date.parse(toIso);
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) {
      return null;
    }
    const midMs = fromMs + Math.floor((toMs - fromMs) / 2);
    if (!Number.isFinite(midMs) || midMs <= fromMs || midMs >= toMs) return null;
    return {
      left: { fromIso, toIso: new Date(midMs).toISOString() },
      right: { fromIso: new Date(midMs).toISOString(), toIso },
    };
  };

  const queryWindow = ({ fromIso, toIso, activeWindowMinutes }) => {
    try {
      const chunkRows = queryRows(`
        ${baseSelectSql}
        WHERE symbol = {symbol:String}
          AND minute_bucket_utc >= parseDateTime64BestEffortOrNull({fromIso:String}, 3, 'UTC')
          AND minute_bucket_utc < parseDateTime64BestEffortOrNull({toIso:String}, 3, 'UTC')
        ${orderBySql}
      `, { symbol, fromIso, toIso }, env);
      emitRows(chunkRows);
      return;
    } catch (error) {
      const canSplit = Number.isFinite(activeWindowMinutes)
        && activeWindowMinutes > minWindowMinutes;
      if (!canSplit || !isOversizedWindowError(error)) {
        throw error;
      }
      const split = splitWindow(fromIso, toIso);
      if (!split) {
        throw error;
      }
      const nextWindowMinutes = Math.max(minWindowMinutes, Math.trunc(activeWindowMinutes / 2));
      queryWindow({ ...split.left, activeWindowMinutes: nextWindowMinutes });
      queryWindow({ ...split.right, activeWindowMinutes: nextWindowMinutes });
    }
  };

  const windowMinutes = parseClickHouseTradeReadWindowMinutes(env);
  if (windowMinutes <= 0 || windowMinutes >= 24 * 60) {
    const rows = queryRows(`
      ${baseSelectSql}
      WHERE symbol = {symbol:String}
        AND trade_date_utc = toDate({dayIso:String})
      ${orderBySql}
    `, { symbol, dayIso }, env);
    if (emitChunk) {
      if (rows.length > 0) emitChunk(rows);
      return [];
    }
    return rows;
  }

  const windows = buildClickHouseTradeReadWindows(dayIso, windowMinutes);
  windows.forEach(({ fromIso, toIso }) => {
    queryWindow({ fromIso, toIso, activeWindowMinutes: windowMinutes });
  });

  return rows || [];
}

function loadClickHouseFeatureBaselineRows({ symbol, env = process.env, queryRows = queryClickHouseRowsSync }) {
  ensureClickHouseSupportSchema(env);
  return queryRows(`
    SELECT
      symbol,
      minute_of_day_et AS minuteOfDayEt,
      feature_name AS featureName,
      sample_count AS sampleCount,
      mean,
      m2
    FROM options.feature_baseline_intraday
    WHERE symbol = {symbol:String}
    ORDER BY updated_at_utc DESC
    LIMIT 1 BY symbol, minute_of_day_et, feature_name
  `, { symbol }, env);
}

function loadClickHouseRuleVersionRows(env = process.env, queryRows = queryClickHouseRowsSync) {
  ensureClickHouseSupportSchema(env);
  return queryRows(`
    SELECT
      version_id AS versionId,
      config_json AS configJson,
      checksum,
      is_active AS isActive,
      concat(replaceAll(toString(created_at_utc, 'UTC'), ' ', 'T'), 'Z') AS createdAtUtc,
      ifNull(concat(replaceAll(toString(activated_at_utc, 'UTC'), ' ', 'T'), 'Z'), null) AS activatedAtUtc
    FROM options.filter_rule_versions
    ORDER BY created_at_utc DESC
  `, {}, env);
}

function hasMinuteDerivedRows(db, { symbol, dayIso }) {
  const symbolMinute = db.prepare(`
    SELECT COUNT(*) AS count
    FROM option_symbol_minute_derived
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).get({ symbol, dayIso });

  const contractMinute = db.prepare(`
    SELECT COUNT(*) AS count
    FROM option_contract_minute_derived
    WHERE symbol = @symbol
      AND trade_date_utc = @dayIso
  `).get({ symbol, dayIso });

  return Number(symbolMinute?.count || 0) > 0 && Number(contractMinute?.count || 0) > 0;
}

function upsertOptionTrades(db, rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return 0;

  const upsert = db.prepare(`
    INSERT INTO option_trades (
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
      watermark
    ) VALUES (
      @tradeId,
      @tradeTsUtc,
      @tradeTsEt,
      @symbol,
      @expiration,
      @strike,
      @optionRight,
      @price,
      @size,
      @bid,
      @ask,
      @conditionCode,
      @exchange,
      @rawPayloadJson,
      @watermark
    )
    ON CONFLICT(trade_id) DO UPDATE SET
      bid = excluded.bid,
      ask = excluded.ask,
      raw_payload_json = excluded.raw_payload_json,
      watermark = excluded.watermark
  `);

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((row) => {
      writes += upsert.run(row).changes;
    });
    return writes;
  });

  return txn(rows);
}

async function syncThetaTradesToSqlite({
  symbol,
  dayIso,
  env = process.env,
  db,
  markPartial = false,
}) {
  const dbPath = resolveDbPath(env);
  const writeDb = db || new Database(dbPath);
  const resumeCursor = getSqliteTradeResumeCursor(writeDb, { symbol, dayIso });
  let parsedRows = 0;
  let fetchedRows = 0;
  let upsertedRows = 0;
  let sawHttpOk = false;
  let sawNoData = false;

  try {
    ensureSchema(writeDb);
    const sessionWindow = await resolveThetaCalendarSessionWindowForDay(dayIso, { env });
    if (sessionWindow && sessionWindow.isOpen === false) {
      const cacheStatus = markPartial ? DAY_CACHE_STATUS_PARTIAL : DAY_CACHE_STATUS_FULL;
      upsertDayCache(writeDb, {
        symbol,
        dayIso,
        cacheStatus,
        rowCount: 0,
        lastError: null,
        sourceEndpoint: resolveThetaCalendarEndpoint(dayIso, env),
      });
      return {
        synced: true,
        reason: 'no_data',
        fetchedRows: 0,
        upsertedRows: 0,
        cachedRows: 0,
        cacheStatus,
      };
    }

    const yyyymmdd = toYyyymmdd(`${dayIso}T00:00:00.000Z`);
    const requestWindows = resolveThetaTimeWindowsForSymbol(symbol, {
      startTime: resumeCursor?.startTime || null,
      sessionStartTime: sessionWindow?.openTime || null,
      sessionEndTime: sessionWindow?.closeTime || null,
      env,
    });
    const endpoints = requestWindows
      .map((window) => resolveThetaEndpoint(
        symbol,
        yyyymmdd,
        env,
        {
          startTime: window.startTime || null,
          endTime: window.endTime || null,
        },
      ))
      .filter(Boolean);
    if (endpoints.length === 0) {
      return { synced: false, reason: 'thetadata_base_url_missing', fetchedRows: 0, upsertedRows: 0, cachedRows: 0 };
    }
    const endpointFormat = parseThetaUrlFormat(endpoints[0]);

    for (const endpoint of endpoints) {
      const fetchedBeforeEndpoint = fetchedRows;
      if (endpointFormat === 'ndjson') {
        const heartbeat = createThetaStreamHeartbeatLogger({
          env,
          endpoint,
          stage: 'trade_sync_sqlite',
          symbol,
          dayIso,
        });
        let chunk = [];
        const flushChunk = () => {
          if (chunk.length === 0) return;
          upsertedRows += upsertOptionTrades(writeDb, chunk);
          fetchedRows += chunk.length;
          chunk = [];
        };
        const streamResult = await fetchThetaNdjsonRows(endpoint, {
          env,
          onRow: (rawRow) => {
            parsedRows += 1;
            const normalized = normalizeThetaRow(rawRow, symbol, dayIso);
            if (normalized) {
              chunk.push(normalized);
              if (chunk.length >= 5000) flushChunk();
            }
            if (heartbeat) {
              heartbeat({
                parsedRows,
                fetchedRows: fetchedRows + chunk.length,
                insertedRows: upsertedRows,
                bufferedRows: chunk.length,
              });
            }
          },
        });
        const { response, durationMs } = streamResult;
        if (!response.ok) {
          const isThetaNoData = response.status === 472;
          logThetaDownload({
            env,
            url: endpoint,
            durationMs,
            status: response.status,
            ok: isThetaNoData,
            rows: 0,
            error: isThetaNoData ? 'no_data' : `http_${response.status}`,
          });
          if (isThetaNoData) {
            sawNoData = true;
            continue;
          }
          throw new Error(`thetadata_request_failed:${response.status}`);
        }
        sawHttpOk = true;
        flushChunk();
        logThetaDownload({
          env,
          url: endpoint,
          durationMs,
          status: response.status,
          ok: true,
          rows: fetchedRows - fetchedBeforeEndpoint,
          error: null,
        });
        continue;
      }

      const textResult = await fetchTextWithTimeout(endpoint, { env });
      const { response, durationMs } = textResult;
      if (!response.ok) {
        const isThetaNoData = response.status === 472;
        logThetaDownload({
          env,
          url: endpoint,
          durationMs,
          status: response.status,
          ok: isThetaNoData,
          rows: 0,
          error: isThetaNoData ? 'no_data' : `http_${response.status}`,
        });
        if (isThetaNoData) {
          sawNoData = true;
          continue;
        }
        throw new Error(`thetadata_request_failed:${response.status}`);
      }

      sawHttpOk = true;
      const parsed = parseJsonRows(textResult.body);
      const normalizedRows = normalizeThetaRows(parsed, symbol, dayIso);
      fetchedRows += normalizedRows.length;
      upsertedRows += upsertOptionTrades(writeDb, normalizedRows);
      logThetaDownload({
        env,
        url: endpoint,
        durationMs,
        status: response.status,
        ok: true,
        rows: normalizedRows.length,
        error: null,
      });
    }

    const dayStart = `${dayIso}T00:00:00.000Z`;
    const dayEnd = `${dayIso}T23:59:59.999Z`;
    const rowCount = countCachedRows(writeDb, { from: dayStart, to: dayEnd, symbol });

    if (!sawHttpOk && sawNoData && rowCount === 0) {
      const cacheStatus = markPartial ? DAY_CACHE_STATUS_PARTIAL : DAY_CACHE_STATUS_FULL;
      upsertDayCache(writeDb, {
        symbol,
        dayIso,
        cacheStatus,
        rowCount: 0,
        lastError: null,
        sourceEndpoint: endpoints[0] || null,
      });
      return {
        synced: true,
        reason: 'no_data',
        fetchedRows: 0,
        upsertedRows: 0,
        cachedRows: 0,
        cacheStatus,
      };
    }

    // Only mark as 'full' if we actually have rows. A successful HTTP response
    // with 0 parsed rows may indicate a malformed response — mark 'partial'
    // so it gets retried on the next run.
    const cacheStatus = markPartial || rowCount === 0
      ? DAY_CACHE_STATUS_PARTIAL
      : DAY_CACHE_STATUS_FULL;

    upsertDayCache(writeDb, {
      symbol,
      dayIso,
      cacheStatus,
      rowCount,
      lastError: rowCount === 0 ? 'empty_response' : null,
      sourceEndpoint: endpoints[0] || null,
    });

    return {
      synced: true,
      reason: null,
      fetchedRows,
      upsertedRows,
      cachedRows: 0,
      cacheStatus,
    };
  } finally {
    if (!db) {
      writeDb.close();
    }
  }
}

function countCachedRows(db, { from, to, symbol }) {
  const row = db.prepare(`
    SELECT COUNT(*) AS count
    FROM option_trades
    WHERE trade_ts_utc >= @from
      AND trade_ts_utc <= @to
      AND symbol = @symbol
  `).get({ from, to, symbol });

  return Number(row?.count || 0);
}

function getSqliteTradeResumeCursor(db, { symbol, dayIso }) {
  const from = `${dayIso}T00:00:00.000Z`;
  const to = `${dayIso}T23:59:59.999Z`;
  const row = db.prepare(`
    SELECT MAX(trade_ts_utc) AS maxTradeTsUtc
    FROM option_trades
    WHERE symbol = @symbol
      AND trade_ts_utc >= @from
      AND trade_ts_utc <= @to
  `).get({ symbol, from, to });

  const maxTradeTsUtc = typeof row?.maxTradeTsUtc === 'string' ? row.maxTradeTsUtc : null;
  if (!maxTradeTsUtc) return null;
  const resumeFromIso = floorIsoToMinute(maxTradeTsUtc);
  if (!resumeFromIso) return null;
  const startTime = isoToTimeHms(resumeFromIso);
  if (!startTime) return null;
  return { startTime, resumeFromIso };
}

function getRawTradesForDay(db, { symbol, dayIso }) {
  const from = `${dayIso}T00:00:00.000Z`;
  const to = `${dayIso}T23:59:59.999Z`;

  return db.prepare(`
    SELECT
      trade_id AS tradeId,
      trade_ts_utc AS tradeTsUtc,
      symbol,
      expiration,
      strike,
      option_right AS right,
      price,
      size,
      bid,
      ask,
      condition_code AS conditionCode,
      exchange,
      raw_payload_json AS rawPayloadJson
    FROM option_trades
    WHERE symbol = @symbol
      AND trade_ts_utc >= @from
      AND trade_ts_utc <= @to
    ORDER BY trade_ts_utc ASC, trade_id ASC
  `).all({ symbol, from, to });
}

function parsePayload(jsonValue) {
  if (typeof jsonValue !== 'string' || !jsonValue.trim()) return {};
  try {
    const parsed = JSON.parse(jsonValue);
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function finalizeMinuteStatsMap(minuteMap) {
  const orderedBuckets = Array.from(minuteMap.keys()).sort((a, b) => Date.parse(a) - Date.parse(b));

  const volumeWindow = [];
  let volumeWindowSum = 0;

  const sentimentWindow = [];
  let bullishWindowSum = 0;
  let bearishWindowSum = 0;

  const openWindow = [];
  let openWindowSum = 0;

  const statsByMinute = new Map();

  orderedBuckets.forEach((minuteBucket) => {
    const minuteTs = Date.parse(minuteBucket);
    const windowStart = minuteTs - (15 * 60000);

    while (volumeWindow.length && volumeWindow[0].ts < windowStart) {
      const removed = volumeWindow.shift();
      volumeWindowSum -= removed.volume;
    }

    while (sentimentWindow.length && sentimentWindow[0].ts < windowStart) {
      const removed = sentimentWindow.shift();
      bullishWindowSum -= removed.bullish;
      bearishWindowSum -= removed.bearish;
    }

    const current = minuteMap.get(minuteBucket);
    const symbolVolBaseline15m = volumeWindow.length ? (volumeWindowSum / volumeWindow.length) : 0;

    let openWindowBaseline = 0;
    if (isAmSpikeWindow(minuteBucket)) {
      openWindowBaseline = openWindow.length ? (openWindowSum / openWindow.length) : 0;
    }

    volumeWindow.push({ ts: minuteTs, volume: current.volume });
    volumeWindowSum += current.volume;

    sentimentWindow.push({ ts: minuteTs, bullish: current.bullish, bearish: current.bearish });
    bullishWindowSum += current.bullish;
    bearishWindowSum += current.bearish;

    if (isAmSpikeWindow(minuteBucket)) {
      openWindow.push({ ts: minuteTs, volume: current.volume });
      openWindowSum += current.volume;
    }

    const directionalTotal = bullishWindowSum + bearishWindowSum;
    const bullishRatio15m = directionalTotal > 0 ? bullishWindowSum / directionalTotal : 0;

    statsByMinute.set(minuteBucket, {
      symbolVol1m: current.volume,
      symbolVolBaseline15m,
      openWindowBaseline,
      bullishRatio15m,
    });
  });

  return statsByMinute;
}

function buildMinuteStats(rows) {
  const minuteMap = new Map();
  const lastContractPrint = new Map();

  rows.forEach((row) => {
    const minuteBucket = toMinuteBucketUtc(row.tradeTsUtc);
    if (!minuteBucket) return;

    const existing = minuteMap.get(minuteBucket) || {
      minuteBucket,
      volume: 0,
      bullish: 0,
      bearish: 0,
    };

    const size = toFiniteNumber(row.size) || 0;
    const contractKey = buildContractKey(row);
    const previousPrint = lastContractPrint.get(contractKey) || null;
    const execution = computeExecutionFlags({
      ...row,
      lastTradePrice: previousPrint ? previousPrint.tradePrice : null,
      lastExecutionSide: previousPrint ? previousPrint.executionSide : null,
    });
    const sentiment = computeSentiment({ right: row.right, executionSide: execution.executionSide });
    if (toFiniteNumber(row.price) !== null) {
      lastContractPrint.set(contractKey, {
        tradePrice: toFiniteNumber(row.price),
        executionSide: execution.executionSide,
      });
    }

    existing.volume += size;
    if (sentiment === 'bullish') existing.bullish += 1;
    if (sentiment === 'bearish') existing.bearish += 1;

    minuteMap.set(minuteBucket, existing);
  });
  return finalizeMinuteStatsMap(minuteMap);
}

function buildContractKey(row) {
  return [row.symbol, row.expiration, row.strike, row.right].join('|');
}

function detectHeuristicMultilegs(rawRows) {
  const multilegIndices = new Set();
  const bySecondBucket = new Map();

  rawRows.forEach((row, rowIndex) => {
    const ts = row.tradeTsUtc;
    if (!ts) return;
    const secondBucket = ts.slice(0, 19);
    const key = `${row.symbol}|${secondBucket}`;
    const group = bySecondBucket.get(key) || [];
    group.push({ rowIndex, contractKey: buildContractKey(row), size: toFiniteNumber(row.size) || 0 });
    bySecondBucket.set(key, group);
  });

  bySecondBucket.forEach((group) => {
    if (group.length < 2) return;
    const contracts = new Set(group.map((g) => g.contractKey));
    if (contracts.size < 2) return;

    const sizes = group.map((g) => g.size).filter((s) => s > 0);
    if (sizes.length < 2) return;

    const minSize = Math.min(...sizes);
    const maxSize = Math.max(...sizes);
    if (minSize > 0 && maxSize / minSize <= 3) {
      group.forEach((g) => multilegIndices.add(g.rowIndex));
    }
  });

  return multilegIndices;
}

function buildSideKey(row, executionSide) {
  return [row.symbol, row.expiration, row.strike, row.right, executionSide].join('|');
}

function clamp01(value) {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

function computeEmpiricalPercentile(sortedValues, value) {
  if (!Array.isArray(sortedValues) || !sortedValues.length) return 0;
  const target = toFiniteNumber(value);
  if (target === null) return 0;

  let left = 0;
  let right = sortedValues.length;
  while (left < right) {
    const mid = Math.floor((left + right) / 2);
    if (sortedValues[mid] <= target) left = mid + 1;
    else right = mid;
  }
  return clamp01((left - 1) / Math.max(1, sortedValues.length - 1));
}

function sideConfidence(executionSide) {
  if (executionSide === 'AA') return 1;
  if (executionSide === 'ASK') return 0.85;
  if (executionSide === 'BID') return 0.7;
  return 0.25;
}

function evaluateChips(row, thresholds, options = {}) {
  const strictScoreQuality = options.strictScoreQuality !== false;
  const scoreQualityEligible = !strictScoreQuality || row.scoreQuality === 'complete';
  const directionalSignalEligible = row.sentiment === 'bullish' || row.sentiment === 'bearish';
  const chips = [];

  if (row.execution.calls) chips.push('calls');
  if (row.execution.puts) chips.push('puts');
  if (row.execution.bid) chips.push('bid');
  if (row.execution.ask) chips.push('ask');
  if (row.execution.aa) chips.push('aa');

  if (row.value !== null && row.value >= thresholds.premium100kMin) chips.push('100k+');
  if (row.value !== null && row.value >= thresholds.premiumSizableMin) chips.push('sizable');
  if (row.value !== null && row.value >= thresholds.premiumWhalesMin) chips.push('whales');
  if (row.size !== null && row.size >= thresholds.sizeLargeMin) chips.push('large-size');

  if (row.dte !== null && row.dte >= 365) chips.push('leaps');
  if (!isStandardMonthly(row.expiration)) chips.push('weeklies');

  if (row.repeat3m !== null && row.repeat3m >= thresholds.repeatFlowMin) chips.push('repeat-flow');

  if (row.otmPct !== null && row.otmPct > 0) chips.push('otm');
  if (row.volOiRatio !== null && row.volOiRatio > thresholds.volOiMin) chips.push('vol>oi');

  if (row.symbolVolBaseline15m > 0 && row.symbolVol1m >= (2.5 * row.symbolVolBaseline15m)) {
    chips.push('rising-vol');
  }

  if (isAmSpikeWindow(row.tradeTsUtc)
    && row.openWindowBaseline > 0
    && row.symbolVol1m >= (3.0 * row.openWindowBaseline)) {
    chips.push('am-spike');
  }

  if (row.bullishRatio15m >= thresholds.bullflowRatioMin && row.sentiment === 'bullish') {
    chips.push('bullflow');
  }

  if (scoreQualityEligible && directionalSignalEligible && row.sigScore !== null && row.sigScore >= thresholds.highSigMin) chips.push('high-sig');

  if (scoreQualityEligible && directionalSignalEligible && row.value !== null && row.value >= thresholds.premium100kMin
    && row.volOiRatio !== null && row.volOiRatio >= thresholds.unusualVolOiMin) {
    chips.push('unusual');
  }

  if (scoreQualityEligible && directionalSignalEligible && ((row.repeat3m !== null && row.repeat3m >= thresholds.repeatFlowMin)
    || (row.value !== null && row.value >= thresholds.premiumSizableMin
      && row.dte !== null && row.dte <= 14
      && row.volOiRatio !== null && row.volOiRatio >= thresholds.urgentVolOiMin))) {
    chips.push('urgent');
  }

  if (row.dte !== null && row.dte >= 21 && row.dte <= 180
    && row.otmPct !== null && Math.abs(row.otmPct) <= 15
    && row.size !== null && row.size >= 250
    && (row.execution.executionSide === 'ASK' || row.execution.executionSide === 'AA')) {
    chips.push('position-builders');
  }

  if (row.dte !== null && row.dte <= 7
    && row.otmPct !== null && row.otmPct >= 5
    && row.value !== null && row.value >= thresholds.premium100kMin) {
    chips.push('grenade');
  }

  return chips;
}

const METRIC_STATUS_PREDICATES = Object.freeze({
  execution: (row) => row.execution && typeof row.execution.executionSide === 'string',
  value: (row) => row.value !== null,
  size: (row) => row.size !== null,
  dte: (row) => row.dte !== null,
  expiration: (row) => typeof row.expiration === 'string' && row.expiration.length >= 10,
  repeat3m: (row) => row.repeat3m !== null,
  sentiment: (row) => typeof row.sentiment === 'string',
  symbolVolStats: (row) => row.symbolVol1m !== null
    && row.symbolVolBaseline15m !== null
    && row.openWindowBaseline !== null,
  bullishRatio15m: (row) => row.bullishRatio15m !== null,
  spot: (row) => row.spot !== null,
  otmPct: (row) => row.otmPct !== null,
  oi: (row) => row.oi !== null,
  volOiRatio: (row) => row.volOiRatio !== null,
  sigScore: (row) => row.sigScore !== null,
});

function createMetricStatusAccumulator(markPartial = false) {
  const flags = {};
  Object.keys(METRIC_STATUS_PREDICATES).forEach((metricName) => {
    flags[metricName] = true;
  });
  return {
    markPartial: Boolean(markPartial),
    rowCount: 0,
    flags,
  };
}

function accumulateMetricStatuses(accumulator, row) {
  if (!accumulator || !row) return;
  accumulator.rowCount += 1;
  Object.entries(METRIC_STATUS_PREDICATES).forEach(([metricName, predicate]) => {
    if (!accumulator.flags[metricName]) return;
    if (!predicate(row)) {
      accumulator.flags[metricName] = false;
    }
  });
}

function finalizeMetricStatuses(accumulator) {
  const markPartial = Boolean(accumulator?.markPartial);
  const rowCount = Number(accumulator?.rowCount || 0);
  const emptyIsFull = rowCount === 0;
  const metricStatuses = {
    enrichedRows: markPartial ? DAY_CACHE_STATUS_PARTIAL : DAY_CACHE_STATUS_FULL,
  };
  Object.keys(METRIC_STATUS_PREDICATES).forEach((metricName) => {
    if (markPartial) {
      metricStatuses[metricName] = DAY_CACHE_STATUS_PARTIAL;
      return;
    }
    metricStatuses[metricName] = emptyIsFull || accumulator.flags[metricName]
      ? DAY_CACHE_STATUS_FULL
      : DAY_CACHE_STATUS_PARTIAL;
  });
  return metricStatuses;
}

function calculateMetricStatuses(rows, markPartial) {
  const accumulator = createMetricStatusAccumulator(markPartial);
  rows.forEach((row) => accumulateMetricStatuses(accumulator, row));
  return finalizeMetricStatuses(accumulator);
}

function upsertEnrichedRows(db, rows) {
  const upsert = db.prepare(`
    INSERT INTO option_trade_enriched (
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
      sig_score_components_json,
      enriched_at_utc
    ) VALUES (
      @tradeId,
      @tradeTsUtc,
      @symbol,
      @expiration,
      @strike,
      @right,
      @price,
      @size,
      @bid,
      @ask,
      @conditionCode,
      @exchange,
      @value,
      @dte,
      @spot,
      @otmPct,
      @dayVolume,
      @oi,
      @volOiRatio,
      @repeat3m,
      @sigScore,
      @sentiment,
      @executionSide,
      @symbolVol1m,
      @symbolVolBaseline15m,
      @openWindowBaseline,
      @bullishRatio15m,
      @chipsJson,
      @ruleVersion,
      @scoreQuality,
      @missingMetricsJson,
      @isSweep,
      @isMultileg,
      @minuteOfDayEt,
      @delta,
      @impliedVol,
      @timeNorm,
      @deltaNorm,
      @ivSkewNorm,
      @valueShockNorm,
      @dteSwingNorm,
      @flowImbalanceNorm,
      @deltaPressureNorm,
      @cpOiPressureNorm,
      @ivSkewSurfaceNorm,
      @ivTermSlopeNorm,
      @underlyingTrendConfirmNorm,
      @liquidityQualityNorm,
      @multilegPenaltyNorm,
      @sigScoreComponentsJson,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(trade_id) DO UPDATE SET
      value = excluded.value,
      dte = excluded.dte,
      spot = excluded.spot,
      otm_pct = excluded.otm_pct,
      day_volume = excluded.day_volume,
      oi = excluded.oi,
      vol_oi_ratio = excluded.vol_oi_ratio,
      repeat3m = excluded.repeat3m,
      sig_score = excluded.sig_score,
      sentiment = excluded.sentiment,
      execution_side = excluded.execution_side,
      symbol_vol_1m = excluded.symbol_vol_1m,
      symbol_vol_baseline_15m = excluded.symbol_vol_baseline_15m,
      open_window_baseline = excluded.open_window_baseline,
      bullish_ratio_15m = excluded.bullish_ratio_15m,
      chips_json = excluded.chips_json,
      rule_version = excluded.rule_version,
      score_quality = excluded.score_quality,
      missing_metrics_json = excluded.missing_metrics_json,
      is_sweep = excluded.is_sweep,
      is_multileg = excluded.is_multileg,
      minute_of_day_et = excluded.minute_of_day_et,
      delta = excluded.delta,
      implied_vol = excluded.implied_vol,
      time_norm = excluded.time_norm,
      delta_norm = excluded.delta_norm,
      iv_skew_norm = excluded.iv_skew_norm,
      value_shock_norm = excluded.value_shock_norm,
      dte_swing_norm = excluded.dte_swing_norm,
      flow_imbalance_norm = excluded.flow_imbalance_norm,
      delta_pressure_norm = excluded.delta_pressure_norm,
      cp_oi_pressure_norm = excluded.cp_oi_pressure_norm,
      iv_skew_surface_norm = excluded.iv_skew_surface_norm,
      iv_term_slope_norm = excluded.iv_term_slope_norm,
      underlying_trend_confirm_norm = excluded.underlying_trend_confirm_norm,
      liquidity_quality_norm = excluded.liquidity_quality_norm,
      multileg_penalty_norm = excluded.multileg_penalty_norm,
      sig_score_components_json = excluded.sig_score_components_json,
      enriched_at_utc = excluded.enriched_at_utc
  `);

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((row) => {
      writes += upsert.run(row).changes;
    });
    return writes;
  });

  return txn(rows);
}

function upsertContractStats(db, contractStatsMap, dayIso) {
  const upsert = db.prepare(`
    INSERT INTO contract_stats_intraday (
      symbol,
      expiration,
      strike,
      option_right,
      session_date,
      day_volume,
      oi,
      last_trade_ts_utc,
      updated_at_utc
    ) VALUES (
      @symbol,
      @expiration,
      @strike,
      @right,
      @sessionDate,
      @dayVolume,
      @oi,
      @lastTradeTsUtc,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, expiration, strike, option_right, session_date) DO UPDATE SET
      day_volume = excluded.day_volume,
      oi = excluded.oi,
      last_trade_ts_utc = excluded.last_trade_ts_utc,
      updated_at_utc = excluded.updated_at_utc
  `);

  const rows = Array.from(contractStatsMap.values()).map((row) => ({
    ...row,
    sessionDate: dayIso,
    oi: row.oi === null ? 0 : row.oi,
  }));

  const txn = db.transaction((items) => {
    items.forEach((item) => upsert.run(item));
  });

  txn(rows);
}

function upsertSymbolStats(db, symbol, statsByMinute) {
  const upsert = db.prepare(`
    INSERT INTO symbol_stats_intraday (
      symbol,
      minute_bucket_et,
      vol_1m,
      vol_baseline_15m,
      open_window_baseline,
      bullish_ratio_15m,
      updated_at_utc
    ) VALUES (
      @symbol,
      @minuteBucket,
      @vol1m,
      @volBaseline15m,
      @openWindowBaseline,
      @bullishRatio15m,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, minute_bucket_et) DO UPDATE SET
      vol_1m = excluded.vol_1m,
      vol_baseline_15m = excluded.vol_baseline_15m,
      open_window_baseline = excluded.open_window_baseline,
      bullish_ratio_15m = excluded.bullish_ratio_15m,
      updated_at_utc = excluded.updated_at_utc
  `);

  const rows = Array.from(statsByMinute.entries()).map(([minuteBucket, stats]) => ({
    symbol,
    minuteBucket,
    vol1m: stats.symbolVol1m,
    volBaseline15m: stats.symbolVolBaseline15m,
    openWindowBaseline: stats.openWindowBaseline,
    bullishRatio15m: stats.bullishRatio15m,
  }));

  const txn = db.transaction((items) => {
    items.forEach((item) => upsert.run(item));
  });

  txn(rows);
}

function createMinuteDerivedRollupState() {
  return {
    symbolMinute: new Map(),
    contractMinute: new Map(),
  };
}

function accumulateMinuteDerivedRollupRow(rollupState, row, dayIsoOverride = null) {
  if (!rollupState || !row) return;
  const minuteBucket = toMinuteBucketUtc(row.tradeTsUtc);
  if (!minuteBucket) return;

  const tradeDateUtc = dayIsoOverride || normalizeIsoDate(String(row.tradeTsUtc || '').slice(0, 10));
  if (!tradeDateUtc) return;

  const contractKey = buildContractKey(row);
  const symbolKey = `${row.symbol}|${minuteBucket}`;
  const contractMinuteKey = `${row.symbol}|${row.expiration}|${row.strike}|${row.right}|${minuteBucket}`;

  const symbolAgg = rollupState.symbolMinute.get(symbolKey) || {
    symbol: row.symbol,
    tradeDateUtc,
    minuteBucketUtc: minuteBucket,
    tradeCount: 0,
    contractSet: new Set(),
    totalSize: 0,
    totalValue: 0,
    callSize: 0,
    putSize: 0,
    bullishCount: 0,
    bearishCount: 0,
    neutralCount: 0,
    sigScoreSum: 0,
    sigScoreCount: 0,
    maxSigScore: null,
    sigScoreBullishSum: 0,
    sigScoreBullishCount: 0,
    sigScoreBearishSum: 0,
    sigScoreBearishCount: 0,
    valueSigScoreSum: 0,
    valueSigScoreWeight: 0,
    lastSpot: null,
    volOiSum: 0,
    volOiCount: 0,
    maxVolOiRatio: null,
    maxRepeat3m: null,
    oiSum: 0,
    dayVolumeSum: 0,
    chipHits: {},
    sweepCount: 0,
    sweepValueSum: 0,
    multilegCount: 0,
    ivSum: 0,
    ivCount: 0,
    callIvSum: 0,
    callIvCount: 0,
    putIvSum: 0,
    putIvCount: 0,
    netDeltaDollars: 0,
    minuteOfDayEtSum: 0,
    minuteOfDayEtCount: 0,
    valuePctileSum: 0,
    volOiNormSum: 0,
    repeatNormSum: 0,
    otmNormSum: 0,
    sideConfidenceSum: 0,
    dteNormSum: 0,
    spreadNormSum: 0,
    sweepNormSum: 0,
    multilegNormSum: 0,
    timeNormSum: 0,
    deltaNormSum: 0,
    ivSkewNormSum: 0,
    valueShockNormSum: 0,
    dteSwingNormSum: 0,
    flowImbalanceNormSum: 0,
    deltaPressureNormSum: 0,
    cpOiPressureNormSum: 0,
    ivSkewSurfaceNormSum: 0,
    ivTermSlopeNormSum: 0,
    underlyingTrendConfirmNormSum: 0,
    liquidityQualityNormSum: 0,
    multilegPenaltyNormSum: 0,
    componentCount: 0,
  };

  symbolAgg.tradeCount += 1;
  symbolAgg.contractSet.add(contractKey);
  symbolAgg.totalSize += Math.trunc(toFiniteNumber(row.size) || 0);
  symbolAgg.totalValue += toFiniteNumber(row.value) || 0;
  if (row.right === 'CALL') symbolAgg.callSize += Math.trunc(toFiniteNumber(row.size) || 0);
  if (row.right === 'PUT') symbolAgg.putSize += Math.trunc(toFiniteNumber(row.size) || 0);
  if (row.sentiment === 'bullish') symbolAgg.bullishCount += 1;
  else if (row.sentiment === 'bearish') symbolAgg.bearishCount += 1;
  else symbolAgg.neutralCount += 1;

  const sigScore = toFiniteNumber(row.sigScore);
  if (sigScore !== null) {
    symbolAgg.sigScoreSum += sigScore;
    symbolAgg.sigScoreCount += 1;
    symbolAgg.maxSigScore = symbolAgg.maxSigScore === null ? sigScore : Math.max(symbolAgg.maxSigScore, sigScore);

    if (row.sentiment === 'bullish') {
      symbolAgg.sigScoreBullishSum += sigScore;
      symbolAgg.sigScoreBullishCount += 1;
    } else if (row.sentiment === 'bearish') {
      symbolAgg.sigScoreBearishSum += sigScore;
      symbolAgg.sigScoreBearishCount += 1;
    }

    const tradeValue = toFiniteNumber(row.value);
    if (tradeValue !== null && tradeValue > 0) {
      symbolAgg.valueSigScoreSum += sigScore * tradeValue;
      symbolAgg.valueSigScoreWeight += tradeValue;
    }
  }

  const spotValue = toFiniteNumber(row.spot);
  if (spotValue !== null) {
    symbolAgg.lastSpot = spotValue;
  }

  const volOiRatio = toFiniteNumber(row.volOiRatio);
  if (volOiRatio !== null) {
    symbolAgg.volOiSum += volOiRatio;
    symbolAgg.volOiCount += 1;
    symbolAgg.maxVolOiRatio = symbolAgg.maxVolOiRatio === null ? volOiRatio : Math.max(symbolAgg.maxVolOiRatio, volOiRatio);
  }

  const repeat3m = toFiniteNumber(row.repeat3m);
  if (repeat3m !== null) {
    symbolAgg.maxRepeat3m = symbolAgg.maxRepeat3m === null ? Math.trunc(repeat3m) : Math.max(symbolAgg.maxRepeat3m, Math.trunc(repeat3m));
  }

  symbolAgg.oiSum += Math.trunc(toFiniteNumber(row.oi) || 0);
  symbolAgg.dayVolumeSum += Math.trunc(toFiniteNumber(row.dayVolume) || 0);

  if (Array.isArray(row.chips)) {
    row.chips.forEach((chipId) => {
      symbolAgg.chipHits[chipId] = (symbolAgg.chipHits[chipId] || 0) + 1;
    });
  }

  if (row.isSweep) {
    symbolAgg.sweepCount += 1;
    symbolAgg.sweepValueSum += toFiniteNumber(row.value) || 0;
  }

  if (row.isMultileg) {
    symbolAgg.multilegCount += 1;
  }

  const iv = toFiniteNumber(row.impliedVol);
  if (iv !== null) {
    symbolAgg.ivSum += iv;
    symbolAgg.ivCount += 1;
    if (row.right === 'CALL') {
      symbolAgg.callIvSum += iv;
      symbolAgg.callIvCount += 1;
    } else if (row.right === 'PUT') {
      symbolAgg.putIvSum += iv;
      symbolAgg.putIvCount += 1;
    }
  }

  const delta = toFiniteNumber(row.delta);
  const tradeValue = toFiniteNumber(row.value);
  if (delta !== null && tradeValue !== null) {
    const directionSign = row.sentiment === 'bullish' ? 1 : (row.sentiment === 'bearish' ? -1 : 0);
    symbolAgg.netDeltaDollars += delta * tradeValue * directionSign;
  }

  const minuteOfDayEt = toFiniteNumber(row.minuteOfDayEt);
  if (minuteOfDayEt !== null) {
    symbolAgg.minuteOfDayEtSum += minuteOfDayEt;
    symbolAgg.minuteOfDayEtCount += 1;
  }

  symbolAgg.valuePctileSum += toFiniteNumber(row.valuePctile) || 0;
  symbolAgg.volOiNormSum += toFiniteNumber(row.volOiNorm) || 0;
  symbolAgg.repeatNormSum += toFiniteNumber(row.repeatNorm) || 0;
  symbolAgg.otmNormSum += toFiniteNumber(row.otmNorm) || 0;
  symbolAgg.sideConfidenceSum += toFiniteNumber(row.sideConfidenceVal) || 0;
  symbolAgg.dteNormSum += toFiniteNumber(row.dteNorm) || 0;
  symbolAgg.spreadNormSum += toFiniteNumber(row.spreadNorm) || 0;
  symbolAgg.sweepNormSum += toFiniteNumber(row.sweepNorm) || 0;
  symbolAgg.multilegNormSum += toFiniteNumber(row.multilegNorm) || 0;
  symbolAgg.timeNormSum += toFiniteNumber(row.timeNorm) || 0;
  symbolAgg.deltaNormSum += toFiniteNumber(row.deltaNorm) || 0;
  symbolAgg.ivSkewNormSum += toFiniteNumber(row.ivSkewNorm) || 0;
  symbolAgg.valueShockNormSum += toFiniteNumber(row.valueShockNorm) || 0;
  symbolAgg.dteSwingNormSum += toFiniteNumber(row.dteSwingNorm) || 0;
  symbolAgg.flowImbalanceNormSum += toFiniteNumber(row.flowImbalanceNorm) || 0;
  symbolAgg.deltaPressureNormSum += toFiniteNumber(row.deltaPressureNorm) || 0;
  symbolAgg.cpOiPressureNormSum += toFiniteNumber(row.cpOiPressureNorm) || 0;
  symbolAgg.ivSkewSurfaceNormSum += toFiniteNumber(row.ivSkewSurfaceNorm) || 0;
  symbolAgg.ivTermSlopeNormSum += toFiniteNumber(row.ivTermSlopeNorm) || 0;
  symbolAgg.underlyingTrendConfirmNormSum += toFiniteNumber(row.underlyingTrendConfirmNorm) || 0;
  symbolAgg.liquidityQualityNormSum += toFiniteNumber(row.liquidityQualityNorm) || 0;
  symbolAgg.multilegPenaltyNormSum += toFiniteNumber(row.multilegPenaltyNorm) || 0;
  symbolAgg.componentCount += 1;

  rollupState.symbolMinute.set(symbolKey, symbolAgg);

  const contractAgg = rollupState.contractMinute.get(contractMinuteKey) || {
    symbol: row.symbol,
    expiration: row.expiration,
    strike: row.strike,
    right: row.right,
    tradeDateUtc,
    minuteBucketUtc: minuteBucket,
    tradeCount: 0,
    sizeSum: 0,
    valueSum: 0,
    priceSum: 0,
    priceCount: 0,
    lastPrice: null,
    dayVolume: null,
    oi: null,
    volOiRatio: null,
    sigScoreSum: 0,
    sigScoreCount: 0,
    maxSigScore: null,
    maxRepeat3m: null,
    bullishCount: 0,
    bearishCount: 0,
    neutralCount: 0,
    chipHits: {},
  };

  const size = Math.trunc(toFiniteNumber(row.size) || 0);
  const value = toFiniteNumber(row.value) || 0;
  const price = toFiniteNumber(row.price);
  const contractSigScore = toFiniteNumber(row.sigScore);
  const contractRepeat3m = toFiniteNumber(row.repeat3m);

  contractAgg.tradeCount += 1;
  contractAgg.sizeSum += size;
  contractAgg.valueSum += value;
  if (price !== null) {
    contractAgg.priceSum += price;
    contractAgg.priceCount += 1;
    contractAgg.lastPrice = price;
  }
  contractAgg.dayVolume = Math.trunc(toFiniteNumber(row.dayVolume) || 0);
  contractAgg.oi = Math.trunc(toFiniteNumber(row.oi) || 0);
  contractAgg.volOiRatio = toFiniteNumber(row.volOiRatio);

  if (contractSigScore !== null) {
    contractAgg.sigScoreSum += contractSigScore;
    contractAgg.sigScoreCount += 1;
    contractAgg.maxSigScore = contractAgg.maxSigScore === null
      ? contractSigScore
      : Math.max(contractAgg.maxSigScore, contractSigScore);
  }

  if (contractRepeat3m !== null) {
    const repeatValue = Math.trunc(contractRepeat3m);
    contractAgg.maxRepeat3m = contractAgg.maxRepeat3m === null
      ? repeatValue
      : Math.max(contractAgg.maxRepeat3m, repeatValue);
  }

  if (row.sentiment === 'bullish') contractAgg.bullishCount += 1;
  else if (row.sentiment === 'bearish') contractAgg.bearishCount += 1;
  else contractAgg.neutralCount += 1;

  if (Array.isArray(row.chips)) {
    row.chips.forEach((chipId) => {
      contractAgg.chipHits[chipId] = (contractAgg.chipHits[chipId] || 0) + 1;
    });
  }

  rollupState.contractMinute.set(contractMinuteKey, contractAgg);
}

function finalizeMinuteDerivedRollups(rollupState) {
  const symbolMinute = rollupState?.symbolMinute || new Map();
  const contractMinute = rollupState?.contractMinute || new Map();
  const symbolMinuteRows = Array.from(symbolMinute.values()).map((row) => ({
    symbol: row.symbol,
    tradeDateUtc: row.tradeDateUtc,
    minuteBucketUtc: row.minuteBucketUtc,
    tradeCount: row.tradeCount,
    contractCount: row.contractSet.size,
    totalSize: row.totalSize,
    totalValue: row.totalValue,
    callSize: row.callSize,
    putSize: row.putSize,
    bullishCount: row.bullishCount,
    bearishCount: row.bearishCount,
    neutralCount: row.neutralCount,
    avgSigScore: row.sigScoreCount ? (row.sigScoreSum / row.sigScoreCount) : null,
    maxSigScore: row.maxSigScore,
    avgSigScoreBullish: row.sigScoreBullishCount ? (row.sigScoreBullishSum / row.sigScoreBullishCount) : null,
    avgSigScoreBearish: row.sigScoreBearishCount ? (row.sigScoreBearishSum / row.sigScoreBearishCount) : null,
    netSigScore: (row.sigScoreBullishCount || row.sigScoreBearishCount)
      ? ((row.sigScoreBullishCount ? (row.sigScoreBullishSum / row.sigScoreBullishCount) : 0)
        - (row.sigScoreBearishCount ? (row.sigScoreBearishSum / row.sigScoreBearishCount) : 0))
      : null,
    valueWeightedSigScore: row.valueSigScoreWeight > 0 ? (row.valueSigScoreSum / row.valueSigScoreWeight) : null,
    spot: row.lastSpot,
    avgVolOiRatio: row.volOiCount ? (row.volOiSum / row.volOiCount) : null,
    maxVolOiRatio: row.maxVolOiRatio,
    maxRepeat3m: row.maxRepeat3m,
    oiSum: row.oiSum,
    dayVolumeSum: row.dayVolumeSum,
    chipHitsJson: JSON.stringify(row.chipHits),
    sweepCount: row.sweepCount,
    sweepValueRatio: row.totalValue > 0 ? (row.sweepValueSum / row.totalValue) : null,
    multilegCount: row.multilegCount,
    multilegPct: row.tradeCount > 0 ? (row.multilegCount / row.tradeCount) : null,
    avgMinuteOfDayEt: row.minuteOfDayEtCount > 0 ? (row.minuteOfDayEtSum / row.minuteOfDayEtCount) : null,
    avgIv: row.ivCount > 0 ? (row.ivSum / row.ivCount) : null,
    callIvAvg: row.callIvCount > 0 ? (row.callIvSum / row.callIvCount) : null,
    putIvAvg: row.putIvCount > 0 ? (row.putIvSum / row.putIvCount) : null,
    ivSpread: (row.callIvCount > 0 && row.putIvCount > 0)
      ? ((row.callIvSum / row.callIvCount) - (row.putIvSum / row.putIvCount))
      : null,
    netDeltaDollars: row.netDeltaDollars,
    avgValuePctile: row.componentCount > 0 ? (row.valuePctileSum / row.componentCount) : null,
    avgVolOiNorm: row.componentCount > 0 ? (row.volOiNormSum / row.componentCount) : null,
    avgRepeatNorm: row.componentCount > 0 ? (row.repeatNormSum / row.componentCount) : null,
    avgOtmNorm: row.componentCount > 0 ? (row.otmNormSum / row.componentCount) : null,
    avgSideConfidence: row.componentCount > 0 ? (row.sideConfidenceSum / row.componentCount) : null,
    avgDteNorm: row.componentCount > 0 ? (row.dteNormSum / row.componentCount) : null,
    avgSpreadNorm: row.componentCount > 0 ? (row.spreadNormSum / row.componentCount) : null,
    avgSweepNorm: row.componentCount > 0 ? (row.sweepNormSum / row.componentCount) : null,
    avgMultilegNorm: row.componentCount > 0 ? (row.multilegNormSum / row.componentCount) : null,
    avgTimeNorm: row.componentCount > 0 ? (row.timeNormSum / row.componentCount) : null,
    avgDeltaNorm: row.componentCount > 0 ? (row.deltaNormSum / row.componentCount) : null,
    avgIvSkewNorm: row.componentCount > 0 ? (row.ivSkewNormSum / row.componentCount) : null,
    avgValueShockNorm: row.componentCount > 0 ? (row.valueShockNormSum / row.componentCount) : null,
    avgDteSwingNorm: row.componentCount > 0 ? (row.dteSwingNormSum / row.componentCount) : null,
    avgFlowImbalanceNorm: row.componentCount > 0 ? (row.flowImbalanceNormSum / row.componentCount) : null,
    avgDeltaPressureNorm: row.componentCount > 0 ? (row.deltaPressureNormSum / row.componentCount) : null,
    avgCpOiPressureNorm: row.componentCount > 0 ? (row.cpOiPressureNormSum / row.componentCount) : null,
    avgIvSkewSurfaceNorm: row.componentCount > 0 ? (row.ivSkewSurfaceNormSum / row.componentCount) : null,
    avgIvTermSlopeNorm: row.componentCount > 0 ? (row.ivTermSlopeNormSum / row.componentCount) : null,
    avgUnderlyingTrendConfirmNorm: row.componentCount > 0 ? (row.underlyingTrendConfirmNormSum / row.componentCount) : null,
    avgLiquidityQualityNorm: row.componentCount > 0 ? (row.liquidityQualityNormSum / row.componentCount) : null,
    avgMultilegPenaltyNorm: row.componentCount > 0 ? (row.multilegPenaltyNormSum / row.componentCount) : null,
  }));

  const contractMinuteRows = Array.from(contractMinute.values()).map((row) => ({
    symbol: row.symbol,
    expiration: row.expiration,
    strike: row.strike,
    right: row.right,
    tradeDateUtc: row.tradeDateUtc,
    minuteBucketUtc: row.minuteBucketUtc,
    tradeCount: row.tradeCount,
    sizeSum: row.sizeSum,
    valueSum: row.valueSum,
    avgPrice: row.priceCount ? (row.priceSum / row.priceCount) : null,
    lastPrice: row.lastPrice,
    dayVolume: row.dayVolume,
    oi: row.oi,
    volOiRatio: row.volOiRatio,
    avgSigScore: row.sigScoreCount ? (row.sigScoreSum / row.sigScoreCount) : null,
    maxSigScore: row.maxSigScore,
    maxRepeat3m: row.maxRepeat3m,
    bullishCount: row.bullishCount,
    bearishCount: row.bearishCount,
    neutralCount: row.neutralCount,
    chipHitsJson: JSON.stringify(row.chipHits),
  }));

  return {
    symbolMinuteRows,
    contractMinuteRows,
  };
}

function buildMinuteDerivedRollups(rows, dayIso) {
  const rollupState = createMinuteDerivedRollupState();
  rows.forEach((row) => {
    accumulateMinuteDerivedRollupRow(rollupState, row, dayIso);
  });
  return finalizeMinuteDerivedRollups(rollupState);
}

function upsertSymbolMinuteDerived(db, rows) {
  const upsert = db.prepare(`
    INSERT INTO option_symbol_minute_derived (
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
      avg_sig_score_bullish,
      avg_sig_score_bearish,
      net_sig_score,
      value_weighted_sig_score,
      spot,
      avg_vol_oi_ratio,
      max_vol_oi_ratio,
      max_repeat3m,
      oi_sum,
      day_volume_sum,
      chip_hits_json,
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
      avg_multileg_penalty_norm,
      updated_at_utc
    ) VALUES (
      @symbol,
      @tradeDateUtc,
      @minuteBucketUtc,
      @tradeCount,
      @contractCount,
      @totalSize,
      @totalValue,
      @callSize,
      @putSize,
      @bullishCount,
      @bearishCount,
      @neutralCount,
      @avgSigScore,
      @maxSigScore,
      @avgSigScoreBullish,
      @avgSigScoreBearish,
      @netSigScore,
      @valueWeightedSigScore,
      @spot,
      @avgVolOiRatio,
      @maxVolOiRatio,
      @maxRepeat3m,
      @oiSum,
      @dayVolumeSum,
      @chipHitsJson,
      @sweepCount,
      @sweepValueRatio,
      @multilegCount,
      @multilegPct,
      @avgMinuteOfDayEt,
      @avgIv,
      @callIvAvg,
      @putIvAvg,
      @ivSpread,
      @netDeltaDollars,
      @avgValuePctile,
      @avgVolOiNorm,
      @avgRepeatNorm,
      @avgOtmNorm,
      @avgSideConfidence,
      @avgDteNorm,
      @avgSpreadNorm,
      @avgSweepNorm,
      @avgMultilegNorm,
      @avgTimeNorm,
      @avgDeltaNorm,
      @avgIvSkewNorm,
      @avgValueShockNorm,
      @avgDteSwingNorm,
      @avgFlowImbalanceNorm,
      @avgDeltaPressureNorm,
      @avgCpOiPressureNorm,
      @avgIvSkewSurfaceNorm,
      @avgIvTermSlopeNorm,
      @avgUnderlyingTrendConfirmNorm,
      @avgLiquidityQualityNorm,
      @avgMultilegPenaltyNorm,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, trade_date_utc, minute_bucket_utc) DO UPDATE SET
      trade_count = excluded.trade_count,
      contract_count = excluded.contract_count,
      total_size = excluded.total_size,
      total_value = excluded.total_value,
      call_size = excluded.call_size,
      put_size = excluded.put_size,
      bullish_count = excluded.bullish_count,
      bearish_count = excluded.bearish_count,
      neutral_count = excluded.neutral_count,
      avg_sig_score = excluded.avg_sig_score,
      max_sig_score = excluded.max_sig_score,
      avg_sig_score_bullish = excluded.avg_sig_score_bullish,
      avg_sig_score_bearish = excluded.avg_sig_score_bearish,
      net_sig_score = excluded.net_sig_score,
      value_weighted_sig_score = excluded.value_weighted_sig_score,
      spot = excluded.spot,
      avg_vol_oi_ratio = excluded.avg_vol_oi_ratio,
      max_vol_oi_ratio = excluded.max_vol_oi_ratio,
      max_repeat3m = excluded.max_repeat3m,
      oi_sum = excluded.oi_sum,
      day_volume_sum = excluded.day_volume_sum,
      chip_hits_json = excluded.chip_hits_json,
      sweep_count = excluded.sweep_count,
      sweep_value_ratio = excluded.sweep_value_ratio,
      multileg_count = excluded.multileg_count,
      multileg_pct = excluded.multileg_pct,
      avg_minute_of_day_et = excluded.avg_minute_of_day_et,
      avg_iv = excluded.avg_iv,
      call_iv_avg = excluded.call_iv_avg,
      put_iv_avg = excluded.put_iv_avg,
      iv_spread = excluded.iv_spread,
      net_delta_dollars = excluded.net_delta_dollars,
      avg_value_pctile = excluded.avg_value_pctile,
      avg_vol_oi_norm = excluded.avg_vol_oi_norm,
      avg_repeat_norm = excluded.avg_repeat_norm,
      avg_otm_norm = excluded.avg_otm_norm,
      avg_side_confidence = excluded.avg_side_confidence,
      avg_dte_norm = excluded.avg_dte_norm,
      avg_spread_norm = excluded.avg_spread_norm,
      avg_sweep_norm = excluded.avg_sweep_norm,
      avg_multileg_norm = excluded.avg_multileg_norm,
      avg_time_norm = excluded.avg_time_norm,
      avg_delta_norm = excluded.avg_delta_norm,
      avg_iv_skew_norm = excluded.avg_iv_skew_norm,
      avg_value_shock_norm = excluded.avg_value_shock_norm,
      avg_dte_swing_norm = excluded.avg_dte_swing_norm,
      avg_flow_imbalance_norm = excluded.avg_flow_imbalance_norm,
      avg_delta_pressure_norm = excluded.avg_delta_pressure_norm,
      avg_cp_oi_pressure_norm = excluded.avg_cp_oi_pressure_norm,
      avg_iv_skew_surface_norm = excluded.avg_iv_skew_surface_norm,
      avg_iv_term_slope_norm = excluded.avg_iv_term_slope_norm,
      avg_underlying_trend_confirm_norm = excluded.avg_underlying_trend_confirm_norm,
      avg_liquidity_quality_norm = excluded.avg_liquidity_quality_norm,
      avg_multileg_penalty_norm = excluded.avg_multileg_penalty_norm,
      updated_at_utc = excluded.updated_at_utc
  `);

  const txn = db.transaction((items) => {
    items.forEach((row) => upsert.run(row));
  });
  txn(rows);
}

function upsertContractMinuteDerived(db, rows) {
  const upsert = db.prepare(`
    INSERT INTO option_contract_minute_derived (
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
    ) VALUES (
      @symbol,
      @expiration,
      @strike,
      @right,
      @tradeDateUtc,
      @minuteBucketUtc,
      @tradeCount,
      @sizeSum,
      @valueSum,
      @avgPrice,
      @lastPrice,
      @dayVolume,
      @oi,
      @volOiRatio,
      @avgSigScore,
      @maxSigScore,
      @maxRepeat3m,
      @bullishCount,
      @bearishCount,
      @neutralCount,
      @chipHitsJson,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol, expiration, strike, option_right, trade_date_utc, minute_bucket_utc) DO UPDATE SET
      trade_count = excluded.trade_count,
      size_sum = excluded.size_sum,
      value_sum = excluded.value_sum,
      avg_price = excluded.avg_price,
      last_price = excluded.last_price,
      day_volume = excluded.day_volume,
      oi = excluded.oi,
      vol_oi_ratio = excluded.vol_oi_ratio,
      avg_sig_score = excluded.avg_sig_score,
      max_sig_score = excluded.max_sig_score,
      max_repeat3m = excluded.max_repeat3m,
      bullish_count = excluded.bullish_count,
      bearish_count = excluded.bearish_count,
      neutral_count = excluded.neutral_count,
      chip_hits_json = excluded.chip_hits_json,
      updated_at_utc = excluded.updated_at_utc
  `);

  const txn = db.transaction((items) => {
    items.forEach((row) => upsert.run(row));
  });
  txn(rows);
}

function normalizeThetaTimestamp(rawTs) {
  if (!rawTs || typeof rawTs !== 'string') return null;
  const hasOffset = /[zZ]|[+-]\d\d:\d\d$/.test(rawTs);
  if (hasOffset) return rawTs;
  // ThetaData returns bare timestamps without timezone suffix.
  // Trade timestamps go through toIsoFromAnyTs which appends Z for bare strings.
  // Greeks timestamps must be treated the same way so lookup keys match.
  return `${rawTs}Z`;
}

async function buildGreeksLookup({ db, symbol, dayIso, rawRows, env = process.env }) {
  const greeksByContractMinute = new Map();
  const greeksSurfaceBySymbolMinute = new Map();
  const cacheStats = { greeksHit: 0, greeksMiss: 0 };
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  if (!baseUrl) return { greeksByContractMinute, greeksSurfaceBySymbolMinute, cacheStats };

  const expirations = [...new Set(rawRows.map((r) => r.expiration).filter(Boolean))]
    .sort((left, right) => Date.parse(left) - Date.parse(right));
  const contractsByExpiration = new Map();
  rawRows.forEach((row) => {
    if (!row.expiration) return;
    const strike = toFiniteNumber(row.strike);
    const right = normalizeRight(row.right);
    if (strike === null || !right) return;
    const contractKey = buildContractKey({ symbol, expiration: row.expiration, strike, right });
    const existing = contractsByExpiration.get(row.expiration) || new Map();
    const current = existing.get(contractKey) || {
      contractKey,
      symbol,
      expiration: row.expiration,
      strike,
      right,
      tradeCount: 0,
    };
    current.tradeCount += 1;
    existing.set(contractKey, current);
    contractsByExpiration.set(row.expiration, existing);
  });

  const minuteExpirationAgg = new Map();
  const coveredContractsByExpiration = new Map();
  const concurrency = parseSupplementalConcurrency(env);
  const contractFallbackLimit = parseGreeksContractFallbackLimit(env);

  const ingestGreeksRows = (greeksRows = []) => {
    greeksRows.forEach((gr) => {
      const strike = toFiniteNumber(gr.strike);
      const right = normalizeRight(gr.right);
      const expiration = normalizeIsoDate(gr.expiration);
      const rawTs = gr.timestamp || gr.trade_timestamp || gr.datetime;
      const impliedVol = toFiniteNumber(gr.implied_vol ?? gr.impliedVol ?? gr.iv);
      const delta = toFiniteNumber(gr.delta);
      if (strike === null || !right || !rawTs || !expiration) return;

      const normalizedTs = normalizeThetaTimestamp(rawTs);
      const minuteBucket = toMinuteBucketUtc(normalizedTs);
      if (!minuteBucket) return;

      const contractKey = buildContractKey({ symbol, expiration, strike, right });
      const lookupKey = `${contractKey}|${minuteBucket}`;
      greeksByContractMinute.set(lookupKey, {
        delta,
        impliedVol,
      });

      const covered = coveredContractsByExpiration.get(expiration) || new Set();
      covered.add(contractKey);
      coveredContractsByExpiration.set(expiration, covered);

      const surfaceKey = `${minuteBucket}|${expiration}`;
      const current = minuteExpirationAgg.get(surfaceKey) || {
        expiration,
        minuteBucket,
        ivSum: 0,
        ivCount: 0,
        callIvSum: 0,
        callIvCount: 0,
        putIvSum: 0,
        putIvCount: 0,
      };

      if (impliedVol !== null) {
        current.ivSum += impliedVol;
        current.ivCount += 1;
        if (right === 'CALL') {
          current.callIvSum += impliedVol;
          current.callIvCount += 1;
        } else if (right === 'PUT') {
          current.putIvSum += impliedVol;
          current.putIvCount += 1;
        }
        minuteExpirationAgg.set(surfaceKey, current);
      }
    });
  };

  const rawGreeksRows = loadOptionGreeksMinuteRaw(db, { symbol, dayIso });
  if (rawGreeksRows.length > 0) {
    ingestGreeksRows(rawGreeksRows);
  }

  if (rawGreeksRows.length === 0) {
    await parallelMapLimit(expirations, concurrency, async (expiration) => {
      const cacheKey = `${symbol}|${dayIso}|${expiration}`;
      let greeksRows = getSupplementalCache(db, 'greeks_expiration_day', cacheKey);
      if (Array.isArray(greeksRows)) {
        cacheStats.greeksHit += 1;
      } else {
        cacheStats.greeksMiss += 1;
        const endpoint = resolveThetaGreeksEndpoint(symbol, expiration, dayIso, env);
        if (!endpoint) return;
        try {
          greeksRows = await fetchThetaRows(endpoint, { env });
          upsertSupplementalCache(db, 'greeks_expiration_day', cacheKey, greeksRows, env, dayIso);
          upsertOptionGreeksMinuteRaw(
            db,
            dayIso,
            normalizeOptionGreeksRows(greeksRows, dayIso),
            endpoint,
          );
        } catch {
          greeksRows = [];
        }
      }

      if (Array.isArray(greeksRows) && greeksRows.length > 0) {
        upsertOptionGreeksMinuteRaw(
          db,
          dayIso,
          normalizeOptionGreeksRows(greeksRows, dayIso),
          null,
        );
      }
      ingestGreeksRows(greeksRows);
    });
  }

  const contractFallbackTargets = [];
  expirations.forEach((expiration) => {
    const contracts = Array.from((contractsByExpiration.get(expiration) || new Map()).values())
      .sort((left, right) => right.tradeCount - left.tradeCount);
    const covered = coveredContractsByExpiration.get(expiration) || new Set();
    let remaining = contractFallbackLimit;
    contracts.forEach((entry) => {
      if (remaining <= 0) return;
      if (covered.has(entry.contractKey)) return;
      contractFallbackTargets.push(entry);
      remaining -= 1;
    });
  });

  if (rawGreeksRows.length === 0) {
    await parallelMapLimit(contractFallbackTargets, concurrency, async (entry) => {
      const cacheKey = `${symbol}|${dayIso}|${entry.expiration}|${entry.strike}|${entry.right}`;
      let greeksRows = getSupplementalCache(db, 'greeks_contract_day', cacheKey);
      if (Array.isArray(greeksRows)) {
        cacheStats.greeksHit += 1;
      } else {
        cacheStats.greeksMiss += 1;
        const endpoint = resolveThetaGreeksEndpoint(symbol, entry.expiration, dayIso, env, {
          strike: entry.strike,
          right: entry.right,
        });
        if (!endpoint) return;
        try {
          greeksRows = await fetchThetaRows(endpoint, { env });
          upsertSupplementalCache(db, 'greeks_contract_day', cacheKey, greeksRows, env, dayIso);
          upsertOptionGreeksMinuteRaw(
            db,
            dayIso,
            normalizeOptionGreeksRows(greeksRows, dayIso),
            endpoint,
          );
        } catch {
          greeksRows = [];
        }
      }
      if (Array.isArray(greeksRows) && greeksRows.length > 0) {
        upsertOptionGreeksMinuteRaw(
          db,
          dayIso,
          normalizeOptionGreeksRows(greeksRows, dayIso),
          null,
        );
      }
      ingestGreeksRows(greeksRows);
    });
  }

  const byMinute = new Map();
  minuteExpirationAgg.forEach((state, key) => {
    const [minuteBucket] = key.split('|');
    const list = byMinute.get(minuteBucket) || [];
    list.push({
      expiration: state.expiration,
      ivAvg: state.ivCount > 0 ? (state.ivSum / state.ivCount) : null,
      callIvAvg: state.callIvCount > 0 ? (state.callIvSum / state.callIvCount) : null,
      putIvAvg: state.putIvCount > 0 ? (state.putIvSum / state.putIvCount) : null,
    });
    byMinute.set(minuteBucket, list);
  });

  byMinute.forEach((entries, minuteBucket) => {
    const valid = entries
      .slice()
      .sort((left, right) => Date.parse(left.expiration) - Date.parse(right.expiration));
    const callSeries = valid.map((entry) => entry.callIvAvg).filter((value) => value !== null);
    const putSeries = valid.map((entry) => entry.putIvAvg).filter((value) => value !== null);
    const callIvAvg = callSeries.length
      ? callSeries.reduce((acc, value) => acc + value, 0) / callSeries.length
      : null;
    const putIvAvg = putSeries.length
      ? putSeries.reduce((acc, value) => acc + value, 0) / putSeries.length
      : null;

    const ivSeries = valid.map((entry) => entry.ivAvg).filter((value) => value !== null);
    const frontIv = ivSeries.length ? ivSeries[0] : null;
    const backIv = ivSeries.length ? ivSeries[ivSeries.length - 1] : null;

    greeksSurfaceBySymbolMinute.set(`${symbol}|${minuteBucket}`, {
      callIvAvg,
      putIvAvg,
      ivSkewSurfaceNorm: computeIvSkewNorm(callIvAvg, putIvAvg),
      ivTermSlopeNorm: computeIvTermSlopeNorm(frontIv, backIv),
      frontIv,
      backIv,
    });
  });

  return { greeksByContractMinute, greeksSurfaceBySymbolMinute, cacheStats };
}

function loadStockFeaturesFromRaw(db, symbol, dayIso) {
  const rawRows = loadStockOhlcMinuteRaw(db, { symbol, dayIso });
  if (rawRows.length === 0) {
    return {
      latestSpot: null,
      stockByMinute: new Map(),
    };
  }
  const stockByMinute = buildStockFeaturesByMinute(rawRows);
  const latestSpot = rawRows.length > 0 ? (toFiniteNumber(rawRows[rawRows.length - 1].close) ?? null) : null;
  return { latestSpot, stockByMinute };
}

async function ensureStockRawForDay(db, symbol, dayIso, env = process.env, cacheStats = null) {
  const fromRaw = loadStockFeaturesFromRaw(db, symbol, dayIso);
  if (fromRaw.stockByMinute.size > 0 || fromRaw.latestSpot !== null) {
    if (cacheStats) {
      cacheStats.stockHit += 1;
      if (fromRaw.latestSpot !== null) cacheStats.spotHit += 1;
    }
    return fromRaw;
  }

  const spotCacheKey = `${symbol}|${dayIso}`;
  let stockRows = getSupplementalCache(db, 'stock_ohlc_symbol_day', spotCacheKey);
  let sourceEndpoint = null;
  let fallbackSpot = null;
  if (Array.isArray(stockRows)) {
    if (cacheStats) cacheStats.stockHit += 1;
  } else {
    if (cacheStats) cacheStats.stockMiss += 1;
    sourceEndpoint = resolveThetaSpotEndpoint(symbol, dayIso, env);
    stockRows = await fetchThetaRows(sourceEndpoint, { env });
    if (Array.isArray(stockRows) && stockRows.length > 0) {
      upsertSupplementalCache(db, 'stock_ohlc_symbol_day', spotCacheKey, stockRows, env, dayIso);
    } else if (sourceEndpoint) {
      fallbackSpot = await fetchThetaMetricNumber(sourceEndpoint, [
        'spot',
        'underlying_price',
        'underlyingPrice',
        'price',
        'last',
        'close',
        'open',
        'high',
        'low',
        'mark',
        'mid',
      ]);
      if (fallbackSpot !== null) {
        upsertSupplementalCache(db, 'spot_symbol_day', spotCacheKey, fallbackSpot, env, dayIso);
      }
    }
  }

  const normalizedBars = normalizeStockOhlcRows(Array.isArray(stockRows) ? stockRows : [], dayIso)
    .map((row, index) => ({
      ...row,
      rawPayload: Array.isArray(stockRows) ? stockRows[index] : row,
    }));
  if (normalizedBars.length > 0) {
    upsertStockOhlcMinuteRaw(db, symbol, dayIso, normalizedBars, sourceEndpoint);
  }

  const reloaded = loadStockFeaturesFromRaw(db, symbol, dayIso);
  if (reloaded.latestSpot === null && fallbackSpot !== null) {
    reloaded.latestSpot = fallbackSpot;
  }
  if (reloaded.latestSpot === null && Array.isArray(stockRows) && stockRows.length > 0) {
    const fallbackSpot = extractMetricFromResponse(JSON.stringify(stockRows), [
      'spot',
      'underlying_price',
      'underlyingPrice',
      'price',
      'last',
      'close',
      'open',
      'high',
      'low',
      'mark',
      'mid',
    ]);
    if (fallbackSpot !== null) {
      reloaded.latestSpot = fallbackSpot;
    }
  }
  if (cacheStats) {
    if (reloaded.latestSpot !== null) cacheStats.spotHit += 1;
    else cacheStats.spotMiss += 1;
  }
  return reloaded;
}

async function ensureOptionQuoteRawForDay(db, symbol, dayIso, env = process.env) {
  if (countOptionQuoteMinuteRaw(db, { symbol, dayIso }) > 0) return true;
  if (resolveFlowWriteBackend(env) === 'clickhouse') {
    const clickhouseCount = countClickHouseOptionQuoteRowsForDay({ symbol, dayIso, env });
    if (clickhouseCount > 0) return true;
  }
  const endpoint = resolveThetaOptionQuoteEndpoint(symbol, dayIso, env);
  if (!endpoint) return false;
  const rows = await fetchThetaRows(endpoint, {
    env,
    timeoutMs: parseOptionQuoteTimeoutMs(env),
  });
  const normalized = normalizeOptionQuoteRows(rows, dayIso);
  if (normalized.length === 0) return false;
  upsertOptionQuoteMinuteRaw(db, dayIso, normalized, endpoint);
  return true;
}

async function ensureOiRawForDay(db, symbol, dayIso, rawRows, env = process.env, cacheStats = null) {
  const oiByContract = loadContractOiFromRaw(db, { symbol, dayIso });
  let oiDefaultsToZero = false;
  let bulkOiSucceeded = false;
  if (oiByContract.size > 0) {
    if (cacheStats) cacheStats.oiHit += oiByContract.size;
    return { oiByContract, oiDefaultsToZero };
  }

  const shouldFetchOi = Boolean((env.THETADATA_OI_PATH || DEFAULT_OI_PATH || '').trim());
  if (!shouldFetchOi) return { oiByContract, oiDefaultsToZero };

  const bulkOiEndpoint = resolveThetaOiBulkEndpoint(symbol, dayIso, env);
  if (bulkOiEndpoint) {
    try {
      const { response, body, durationMs } = await fetchTextWithTimeout(bulkOiEndpoint, { env });
      if (response.ok) {
        bulkOiSucceeded = true;
        oiDefaultsToZero = true;
        const oiRows = parseJsonRows(body);
        logThetaDownload({
          env,
          url: bulkOiEndpoint,
          durationMs,
          status: response.status,
          ok: true,
          rows: oiRows.length,
          error: null,
        });
        const normalizedBulkRows = normalizeOptionOpenInterestRows(oiRows);
        if (normalizedBulkRows.length > 0) {
          upsertOptionOpenInterestRaw(db, dayIso, normalizedBulkRows, bulkOiEndpoint);
          normalizedBulkRows.forEach((row) => {
            oiByContract.set(buildContractKey(row), row.oi);
          });
        }
      } else {
        logThetaDownload({
          env,
          url: bulkOiEndpoint,
          durationMs,
          status: response.status,
          ok: false,
          rows: 0,
          error: `http_${response.status}`,
        });
      }
    } catch (error) {
      logThetaDownload({
        env,
        url: bulkOiEndpoint,
        durationMs: null,
        status: 0,
        ok: false,
        rows: 0,
        error: error.message || 'request_failed',
      });
    }
  }

  if (bulkOiSucceeded) {
    if (cacheStats) {
      if (oiByContract.size > 0) cacheStats.oiHit += oiByContract.size;
      else cacheStats.oiMiss += 1;
    }
    return { oiByContract, oiDefaultsToZero };
  }

  const seenContracts = new Set();
  const missingContracts = [];
  rawRows.forEach((row) => {
    const contractKey = buildContractKey(row);
    if (seenContracts.has(contractKey)) return;
    seenContracts.add(contractKey);
    if (!oiByContract.has(contractKey)) missingContracts.push(row);
  });

  const concurrency = parseSupplementalConcurrency(env);
  await parallelMapLimit(missingContracts, concurrency, async (row) => {
    const oiEndpoint = resolveThetaOiEndpoint(row, dayIso, env);
    if (!oiEndpoint) return;
    const oiRows = await fetchThetaRows(oiEndpoint, { env });
    const normalizedRows = normalizeOptionOpenInterestRows(oiRows);
    if (normalizedRows.length > 0) {
      upsertOptionOpenInterestRaw(db, dayIso, normalizedRows, oiEndpoint);
      normalizedRows.forEach((entry) => {
        oiByContract.set(buildContractKey(entry), entry.oi);
      });
      return;
    }

    const fallbackOi = await fetchThetaMetricNumber(oiEndpoint, [
      'oi',
      'open_interest',
      'openInterest',
    ]);
    if (fallbackOi === null) return;
    const fallbackRow = {
      symbol: row.symbol,
      expiration: row.expiration,
      strike: row.strike,
      right: row.right,
      oi: Math.trunc(fallbackOi),
      rawPayloadJson: JSON.stringify({
        source: 'scalar_oi_response',
        oi: Math.trunc(fallbackOi),
      }),
    };
    upsertOptionOpenInterestRaw(db, dayIso, [fallbackRow], oiEndpoint);
    oiByContract.set(buildContractKey(fallbackRow), fallbackRow.oi);
  });

  if (cacheStats) {
    if (oiByContract.size > 0) cacheStats.oiHit += oiByContract.size;
    else cacheStats.oiMiss += missingContracts.length;
  }
  return { oiByContract, oiDefaultsToZero };
}

function extractOi(rawPayload) {
  const oiKeys = ['oi', 'open_interest', 'openInterest'];
  for (const key of oiKeys) {
    const value = toFiniteNumber(rawPayload[key]);
    if (value !== null) return Math.trunc(value);
  }
  return null;
}

function loadContractOiFromStats(db, { symbol, dayIso }) {
  const rows = db.prepare(`
    SELECT
      symbol,
      expiration,
      strike,
      option_right AS right,
      oi
    FROM contract_stats_intraday
    WHERE symbol = @symbol
      AND session_date = @dayIso
  `).all({ symbol, dayIso });

  const byContract = new Map();
  rows.forEach((row) => {
    const oi = toFiniteNumber(row.oi);
    if (oi === null || oi <= 0) return;
    byContract.set(buildContractKey(row), Math.trunc(oi));
  });
  return byContract;
}

async function buildSupplementalMetricLookup({
  db,
  symbol,
  dayIso,
  rawRows,
  env = process.env,
  requiredMetrics: _requiredMetrics = [],
}) {
  const cacheStats = {
    spotHit: 0,
    spotMiss: 0,
    stockHit: 0,
    stockMiss: 0,
    oiHit: 0,
    oiMiss: 0,
    greeksHit: 0,
    greeksMiss: 0,
  };
  const spotBySymbol = new Map();
  const stockBySymbolMinute = new Map();
  const oiByContract = loadContractOiFromRaw(db, { symbol, dayIso });
  const featureBaselines = loadFeatureBaselines(db, symbol);
  let oiDefaultsToZero = false;
  const referenceOiByContract = loadReferenceOiMap(db, { symbol, asOfDate: dayIso });
  const supplementalConcurrency = parseSupplementalConcurrency(env);
  const symbolsInRows = Array.from(new Set(rawRows.map((row) => row.symbol).filter(Boolean)));
  const payloadOiSeedRows = [];
  const payloadOiSeeded = new Set();
  const shouldFetchSpot = Boolean((env.THETADATA_SPOT_PATH || DEFAULT_SPOT_PATH || '').trim());
  const shouldFetchOi = Boolean((env.THETADATA_OI_PATH || DEFAULT_OI_PATH || '').trim());
  const shouldFetchOptionQuote = Boolean((env.THETADATA_OPTION_QUOTE_PATH || DEFAULT_OPTION_QUOTE_PATH || '').trim());

  if (oiByContract.size > 0) {
    cacheStats.oiHit += oiByContract.size;
  }

  rawRows.forEach((row) => {
    const payload = parsePayload(row.rawPayloadJson);
    const spot = computeSpot(payload);
    if (spot !== null && !spotBySymbol.has(row.symbol)) {
      spotBySymbol.set(row.symbol, spot);
    }

    const oi = extractOi(payload);
    if (oi !== null) {
      const contractKey = buildContractKey(row);
      oiByContract.set(contractKey, oi);
      if (!payloadOiSeeded.has(contractKey)) {
        payloadOiSeeded.add(contractKey);
        payloadOiSeedRows.push({
          symbol: row.symbol,
          expiration: row.expiration,
          strike: row.strike,
          right: row.right,
          oi,
          rawPayloadJson: JSON.stringify({
            source: 'option_trade_payload',
            trade_id: row.id || row.tradeId || null,
            oi,
          }),
        });
      }
    }
  });

  if (payloadOiSeedRows.length > 0) {
    upsertOptionOpenInterestRaw(db, dayIso, payloadOiSeedRows, 'option_trade_payload');
  }

  const requiresSpot = true;
  const requiresOi = true;

  if ((env.THETADATA_BASE_URL || '').trim()) {
    const tasks = [];

    if (shouldFetchSpot && requiresSpot) {
      tasks.push(parallelMapLimit(symbolsInRows, supplementalConcurrency, async (rowSymbol) => {
        const stockResult = await ensureStockRawForDay(db, rowSymbol, dayIso, env, cacheStats);
        stockResult.stockByMinute.forEach((features, minuteBucketUtc) => {
          stockBySymbolMinute.set(`${rowSymbol}|${minuteBucketUtc}`, features);
        });
        if (stockResult.latestSpot !== null) {
          spotBySymbol.set(rowSymbol, stockResult.latestSpot);
        }
      }));
    } else {
      symbolsInRows.forEach((rowSymbol) => {
        const stockResult = loadStockFeaturesFromRaw(db, rowSymbol, dayIso);
        stockResult.stockByMinute.forEach((features, minuteBucketUtc) => {
          stockBySymbolMinute.set(`${rowSymbol}|${minuteBucketUtc}`, features);
        });
        if (stockResult.latestSpot !== null) {
          spotBySymbol.set(rowSymbol, stockResult.latestSpot);
        }
      });
    }

    let oiResult = null;
    if (shouldFetchOi && requiresOi) {
      tasks.push((async () => {
        oiResult = await ensureOiRawForDay(db, symbol, dayIso, rawRows, env, cacheStats);
      })());
    }

    if (shouldFetchOptionQuote && symbol) {
      tasks.push((async () => {
        try {
          await ensureOptionQuoteRawForDay(db, symbol, dayIso, env);
        } catch {
          // Quote-chain raw persistence is best-effort for enrichment.
        }
      })());
    }

    await Promise.all(tasks);

    if (oiResult && oiResult.oiByContract instanceof Map) {
      oiResult.oiByContract.forEach((oiValue, contractKey) => {
        oiByContract.set(contractKey, oiValue);
      });
      oiDefaultsToZero = oiResult.oiDefaultsToZero;
    }
  } else {
    symbolsInRows.forEach((rowSymbol) => {
      const stockResult = loadStockFeaturesFromRaw(db, rowSymbol, dayIso);
      stockResult.stockByMinute.forEach((features, minuteBucketUtc) => {
        stockBySymbolMinute.set(`${rowSymbol}|${minuteBucketUtc}`, features);
      });
      if (stockResult.latestSpot !== null) {
        spotBySymbol.set(rowSymbol, stockResult.latestSpot);
      }
    });
  }

  const statsOiByContract = loadContractOiFromStats(db, { symbol, dayIso });
  statsOiByContract.forEach((oiValue, contractKey) => {
    if (oiValue !== null && oiValue !== undefined && !oiByContract.has(contractKey)) {
      oiByContract.set(contractKey, oiValue);
    }
  });

  referenceOiByContract.forEach((oiValue, contractKey) => {
    if (oiValue !== null && oiValue !== undefined && !oiByContract.has(contractKey)) {
      oiByContract.set(contractKey, oiValue);
    }
  });

  const greeksResult = await buildGreeksLookup({ db, symbol, dayIso, rawRows, env });
  cacheStats.greeksHit += greeksResult.cacheStats.greeksHit;
  cacheStats.greeksMiss += greeksResult.cacheStats.greeksMiss;

  return {
    spotBySymbol,
    stockBySymbolMinute,
    trendFallbackMaxLagMinutes: parseTrendFallbackMaxLagMinutes(env),
    oiByContract,
    oiDefaultsToZero,
    greeksByContractMinute: greeksResult.greeksByContractMinute,
    greeksSurfaceBySymbolMinute: greeksResult.greeksSurfaceBySymbolMinute,
    featureBaselines,
    cacheStats,
  };
}

function buildEnrichedRows(rawRows, thresholds, supplementalMetrics = {}, scoringConfig = {}) {
  const spotBySymbol = supplementalMetrics.spotBySymbol || new Map();
  const stockBySymbolMinute = supplementalMetrics.stockBySymbolMinute || new Map();
  const trendFallbackMaxLagMinutes = Number.isFinite(supplementalMetrics.trendFallbackMaxLagMinutes)
    ? Math.max(1, Math.trunc(supplementalMetrics.trendFallbackMaxLagMinutes))
    : DEFAULT_TREND_FALLBACK_MAX_LAG_MINUTES;
  const oiByContract = supplementalMetrics.oiByContract || new Map();
  const oiDefaultsToZero = Boolean(supplementalMetrics.oiDefaultsToZero);
  const greeksByContractMinute = supplementalMetrics.greeksByContractMinute || new Map();
  const greeksSurfaceBySymbolMinute = supplementalMetrics.greeksSurfaceBySymbolMinute || new Map();
  const featureBaselines = supplementalMetrics.featureBaselines || new Map();
  const strictScoreQuality = String(process.env.FLOW_SCORE_QUALITY_STRICT || '1') !== '0';
  const rowConsumer = typeof scoringConfig.rowConsumer === 'function'
    ? scoringConfig.rowConsumer
    : null;
  const rowConsumerChunkSize = Number.isFinite(Number(scoringConfig.rowConsumerChunkSize))
    ? Math.max(250, Math.min(50000, Math.trunc(Number(scoringConfig.rowConsumerChunkSize))))
    : DEFAULT_CLICKHOUSE_ENRICH_STREAM_CHUNK_SIZE;
  const minuteRollupState = scoringConfig.minuteRollupState || null;
  const metricStatusAccumulator = scoringConfig.metricStatusAccumulator || null;
  const rollingState = scoringConfig.rollingState || null;
  const statsByMinute = scoringConfig.precomputedStatsByMinute instanceof Map
    ? scoringConfig.precomputedStatsByMinute
    : buildMinuteStats(rawRows);

  const disableHeuristicMultileg = Boolean(scoringConfig.disableHeuristicMultileg);
  const multilegIndices = disableHeuristicMultileg
    ? new Set()
    : detectHeuristicMultilegs(rawRows);

  const contractDayVolume = rollingState?.contractDayVolume || new Map();
  const contractStatsMap = rollingState?.contractStatsMap || new Map();
  const sideWindows = rollingState?.sideWindows || new Map();
  const symbolPressureWindows = rollingState?.symbolPressureWindows || new Map();
  const cpOiPressureWindows = rollingState?.cpOiPressureWindows || new Map();
  const runningCallIv = rollingState?.runningCallIv || new Map();
  const runningPutIv = rollingState?.runningPutIv || new Map();
  const spotLastSeenBySymbol = rollingState?.spotLastSeenBySymbol || new Map();
  const trendLastSeenBySymbol = rollingState?.trendLastSeenBySymbol || new Map();
  const lastContractPrint = rollingState?.lastContractPrint || new Map();
  const featureBaselineUpdates = rollingState?.featureBaselineUpdates || new Map();

  const valueSamples = Array.isArray(scoringConfig.precomputedValueSamples)
    ? scoringConfig.precomputedValueSamples
    : rawRows
      .map((row) => computeValue(row.price, row.size))
      .filter((value) => value !== null)
      .sort((a, b) => a - b);

  const minValue = Number.isFinite(Number(scoringConfig.precomputedMinValue))
    ? Number(scoringConfig.precomputedMinValue)
    : (valueSamples.length ? valueSamples[0] : 0);
  const maxValue = Number.isFinite(Number(scoringConfig.precomputedMaxValue))
    ? Number(scoringConfig.precomputedMaxValue)
    : (valueSamples.length ? valueSamples[valueSamples.length - 1] : 0);
  const scoreModel = scoringConfig.scoringModel || 'v4_expanded';
  const scoreRuleVersion = scoringConfig.versionId
    || (scoreModel === 'v1_baseline' ? 'v1_baseline_default' : (scoreModel === 'v5_swing' ? 'v5_swing_default' : 'v4_expanded_default'));

  const enrichedRows = rowConsumer ? null : [];
  let rowCount = 0;
  let rowConsumerBuffer = [];
  const flushRowConsumerBuffer = () => {
    if (!rowConsumer || rowConsumerBuffer.length === 0) return;
    rowConsumer(rowConsumerBuffer);
    rowConsumerBuffer = [];
  };

  rawRows.forEach((row, rowIndex) => {
    const contractKey = buildContractKey(row);
    const previousPrint = lastContractPrint.get(contractKey) || null;
    const execution = computeExecutionFlags({
      ...row,
      lastTradePrice: previousPrint ? previousPrint.tradePrice : null,
      lastExecutionSide: previousPrint ? previousPrint.executionSide : null,
    });
    if (toFiniteNumber(row.price) !== null) {
      lastContractPrint.set(contractKey, {
        tradePrice: toFiniteNumber(row.price),
        executionSide: execution.executionSide,
      });
    }
    const sentiment = computeSentiment({ right: row.right, executionSide: execution.executionSide });

    const previousVolume = contractDayVolume.get(contractKey) || 0;
    const size = toFiniteNumber(row.size) || 0;
    const dayVolume = previousVolume + size;
    contractDayVolume.set(contractKey, dayVolume);

    const minuteBucket = toMinuteBucketUtc(row.tradeTsUtc);
    const minuteStats = statsByMinute.get(minuteBucket) || {
      symbolVol1m: 0,
      symbolVolBaseline15m: 0,
      openWindowBaseline: 0,
      bullishRatio15m: 0,
    };

    const rowMs = Date.parse(row.tradeTsUtc);
    const sideKey = buildSideKey(row, execution.executionSide);
    let sideWindowState = sideWindows.get(sideKey);
    if (!sideWindowState) {
      sideWindowState = { values: [], head: 0 };
      sideWindows.set(sideKey, sideWindowState);
    }
    const sideValues = sideWindowState.values;
    while (sideWindowState.head < sideValues.length && (rowMs - sideValues[sideWindowState.head]) > 180000) {
      sideWindowState.head += 1;
    }
    sideValues.push(rowMs);
    if (sideWindowState.head > 256 && sideWindowState.head * 2 > sideValues.length) {
      sideWindowState.values = sideValues.slice(sideWindowState.head);
      sideWindowState.head = 0;
    }
    const repeat3m = sideWindowState.values.length - sideWindowState.head;

    let payload = null;
    let payloadSpot = toFiniteNumber(row.payloadSpot);
    let payloadOi = toFiniteNumber(row.payloadOi);
    if (
      (payloadSpot === null || payloadOi === null)
      && typeof row.rawPayloadJson === 'string'
      && row.rawPayloadJson !== '{}'
      && row.rawPayloadJson.trim()
    ) {
      payload = parsePayload(row.rawPayloadJson);
    }
    if (payloadSpot === null && payload) {
      payloadSpot = computeSpot(payload);
    }
    if (payloadOi === null && payload) {
      payloadOi = extractOi(payload);
    }
    let stockFeatures = minuteBucket ? (stockBySymbolMinute.get(`${row.symbol}|${minuteBucket}`) || null) : null;
    if (stockFeatures && minuteBucket) {
      trendLastSeenBySymbol.set(row.symbol, { minuteBucket, features: stockFeatures });
    } else if (minuteBucket) {
      const previousTrend = trendLastSeenBySymbol.get(row.symbol);
      if (previousTrend && previousTrend.features && previousTrend.minuteBucket) {
        const previousMinuteMs = Date.parse(previousTrend.minuteBucket);
        const currentMinuteMs = Date.parse(minuteBucket);
        if (Number.isFinite(previousMinuteMs) && Number.isFinite(currentMinuteMs) && currentMinuteMs >= previousMinuteMs) {
          const lagMinutes = Math.trunc((currentMinuteMs - previousMinuteMs) / 60000);
          if (lagMinutes <= trendFallbackMaxLagMinutes) {
            stockFeatures = previousTrend.features;
          }
        }
      }
    }
    const stockSpot = stockFeatures && toFiniteNumber(stockFeatures.close) !== null
      ? toFiniteNumber(stockFeatures.close)
      : null;
    const fallbackSpot = spotLastSeenBySymbol.get(row.symbol) ?? spotBySymbol.get(row.symbol) ?? null;
    const spot = payloadSpot ?? stockSpot ?? fallbackSpot ?? null;
    if (spot !== null) {
      spotLastSeenBySymbol.set(row.symbol, spot);
      if (!spotBySymbol.has(row.symbol)) {
        spotBySymbol.set(row.symbol, spot);
      }
    }

    const otmPct = computeOtmPct({ right: row.right, strike: row.strike, spot });
    const value = computeValue(row.price, row.size);
    const dte = computeDte(row.tradeTsUtc, row.expiration);
    const oiCandidate = payloadOi ?? oiByContract.get(contractKey) ?? null;
    const oi = (oiCandidate === null && oiDefaultsToZero) ? 0 : oiCandidate;
    const volOiRatio = oi === null ? null : (dayVolume / Math.max(oi, 1));

    // Sweep detection
    const sweepFlag = isSweep(row.conditionCode);
    const sweepNorm = sweepFlag ? 1 : 0;

    // Multi-leg detection (code-based OR heuristic)
    const multilegFlag = isMultilegByCode(row.conditionCode) || multilegIndices.has(rowIndex);
    const multilegNorm = multilegFlag ? 1 : 0;

    // Time-of-day feature
    const minuteOfDayEt = computeMinuteOfDayEt(row.tradeTsUtc);

    // OTM bell curve (replaces linear otmNorm)
    const otmNorm = computeOtmNormBellCurve(otmPct);

    // Greeks lookup
    const greeksKey = `${contractKey}|${minuteBucket}`;
    const greeks = greeksByContractMinute.get(greeksKey) || { delta: null, impliedVol: null };
    const surfaceGreeks = minuteBucket
      ? (greeksSurfaceBySymbolMinute.get(`${row.symbol}|${minuteBucket}`) || null)
      : null;
    const effectiveDelta = greeks.delta !== null
      ? greeks.delta
      : computeDeltaProxy({
        right: row.right,
        strike: row.strike,
        spot,
        dte,
      });

    // Delta norm — |delta| is already 0-1
    const deltaNorm = effectiveDelta !== null ? Math.abs(effectiveDelta) : 0;

    // Time norm
    const timeNorm = computeTimeNorm(minuteOfDayEt);

    // IV skew norm — running call/put IV averages
    const iv = greeks.impliedVol;
    if (iv !== null) {
      const ivMap = row.right === 'CALL' ? runningCallIv : runningPutIv;
      const acc = ivMap.get(row.symbol) || { sum: 0, count: 0 };
      acc.sum += iv; acc.count += 1;
      ivMap.set(row.symbol, acc);
    }
    const callAcc = runningCallIv.get(row.symbol);
    const putAcc = runningPutIv.get(row.symbol);
    const runningCallAvg = callAcc && callAcc.count > 0 ? callAcc.sum / callAcc.count : null;
    const runningPutAvg = putAcc && putAcc.count > 0 ? putAcc.sum / putAcc.count : null;
    const fallbackIvSkewNorm = computeIvSkewNorm(runningCallAvg, runningPutAvg);
    const ivSkewNorm = surfaceGreeks && toFiniteNumber(surfaceGreeks.ivSkewSurfaceNorm) !== null
      ? toFiniteNumber(surfaceGreeks.ivSkewSurfaceNorm)
      : fallbackIvSkewNorm;
    const ivSkewSurfaceNorm = surfaceGreeks && toFiniteNumber(surfaceGreeks.ivSkewSurfaceNorm) !== null
      ? toFiniteNumber(surfaceGreeks.ivSkewSurfaceNorm)
      : 0;
    const ivTermSlopeNorm = surfaceGreeks && toFiniteNumber(surfaceGreeks.ivTermSlopeNorm) !== null
      ? toFiniteNumber(surfaceGreeks.ivTermSlopeNorm)
      : 0;

    const valuePctile = value === null
      ? 0
      : computeEmpiricalPercentile(valueSamples, value);
    const valueBaseline = getFeatureBaselineStats(featureBaselines, minuteOfDayEt, 'log_value');
    const valueShockNorm = value === null
      ? 0
      : computeValueShockNorm(
        value,
        valueBaseline
          ? { mean: valueBaseline.mean, std: valueBaseline.std }
          : { min: minValue, max: maxValue },
      );
    if (value !== null) {
      appendFeatureBaselineSample(featureBaselineUpdates, minuteOfDayEt, 'log_value', Math.log1p(value));
    }

    const volOiNorm = volOiRatio === null ? 0 : clamp01(volOiRatio / 5);
    const repeatNorm = clamp01(repeat3m / Math.max(1, thresholds.repeatFlowMin));
    const dteNorm = dte === null ? 0 : clamp01(1 - dte / 60);
    const dteSwingNorm = dte === null ? 0 : computeDteSwingNorm(dte);
    const spreadNorm = (row.bid !== null && row.ask !== null && row.ask > row.bid)
      ? clamp01(1 - ((((row.ask - row.bid) / ((row.ask + row.bid) / 2)) * 100) / 10))
      : 0;
    const sideConfidenceVal = sideConfidence(execution.executionSide);
    const liquidityQualityNorm = computeLiquidityQualityNorm({
      price: row.price,
      bid: row.bid,
      ask: row.ask,
      executionSide: execution.executionSide,
    });

    const direction = sentiment === 'bullish' ? 1 : (sentiment === 'bearish' ? -1 : 0);
    const hasDirection = direction !== 0;
    let pressureState = symbolPressureWindows.get(row.symbol);
    if (!pressureState) {
      pressureState = {
        entries: [],
        head: 0,
        signedPremium: 0,
        totalPremium: 0,
        signedDeltaNotional: 0,
        totalDeltaNotional: 0,
      };
      symbolPressureWindows.set(row.symbol, pressureState);
    }
    const pressureEntries = pressureState.entries;
    while (pressureState.head < pressureEntries.length && (rowMs - pressureEntries[pressureState.head].ts) > 1800000) {
      const removed = pressureEntries[pressureState.head];
      pressureState.head += 1;
      pressureState.signedPremium -= removed.signedPremium;
      pressureState.totalPremium -= removed.totalPremium;
      pressureState.signedDeltaNotional -= removed.signedDeltaNotional;
      pressureState.totalDeltaNotional -= removed.totalDeltaNotional;
    }

    const premiumValue = value || 0;
    const deltaNotional = (effectiveDelta !== null && value !== null) ? (Math.abs(effectiveDelta) * value) : 0;
    if (hasDirection && value !== null) {
      const pressureEntry = {
        ts: rowMs,
        signedPremium: direction * premiumValue,
        totalPremium: premiumValue,
        signedDeltaNotional: direction * deltaNotional,
        totalDeltaNotional: deltaNotional,
      };
      pressureEntries.push(pressureEntry);
      pressureState.signedPremium += pressureEntry.signedPremium;
      pressureState.totalPremium += pressureEntry.totalPremium;
      pressureState.signedDeltaNotional += pressureEntry.signedDeltaNotional;
      pressureState.totalDeltaNotional += pressureEntry.totalDeltaNotional;
    }
    if (pressureState.head > 256 && pressureState.head * 2 > pressureEntries.length) {
      pressureState.entries = pressureEntries.slice(pressureState.head);
      pressureState.head = 0;
    }

    const flowImbalanceNorm = hasDirection
      ? computeFlowImbalanceNorm(pressureState.signedPremium, pressureState.totalPremium)
      : 0;
    const deltaPressureNorm = hasDirection
      ? computeDeltaPressureNorm(pressureState.signedDeltaNotional, pressureState.totalDeltaNotional)
      : 0;

    let cpState = cpOiPressureWindows.get(row.symbol);
    if (!cpState) {
      cpState = {
        entries: [],
        head: 0,
        callPressure: 0,
        putPressure: 0,
      };
      cpOiPressureWindows.set(row.symbol, cpState);
    }
    const cpEntries = cpState.entries;
    while (cpState.head < cpEntries.length && (rowMs - cpEntries[cpState.head].ts) > 1800000) {
      const removed = cpEntries[cpState.head];
      cpState.head += 1;
      cpState.callPressure -= removed.callPressure;
      cpState.putPressure -= removed.putPressure;
    }

    if (value !== null && oi !== null && oi >= 0 && dte !== null && dte <= 60 && otmPct !== null && Math.abs(otmPct) <= 20) {
      const cpEntry = {
        ts: rowMs,
        callPressure: row.right === 'CALL' ? (value / Math.max(oi, 1)) : 0,
        putPressure: row.right === 'PUT' ? (value / Math.max(oi, 1)) : 0,
      };
      cpEntries.push(cpEntry);
      cpState.callPressure += cpEntry.callPressure;
      cpState.putPressure += cpEntry.putPressure;
    }
    if (cpState.head > 256 && cpState.head * 2 > cpEntries.length) {
      cpState.entries = cpEntries.slice(cpState.head);
      cpState.head = 0;
    }
    const cpOiPressureNorm = computeCpOiPressureNorm(cpState.callPressure, cpState.putPressure);

    const trendSignal = stockFeatures && toFiniteNumber(stockFeatures.trendSignal) !== null
      ? toFiniteNumber(stockFeatures.trendSignal)
      : null;
    const alignedTrendSignal = (trendSignal === null || !hasDirection) ? null : (direction * trendSignal);
    const underlyingTrendConfirmNorm = alignedTrendSignal === null
      ? 0
      : computeUnderlyingTrendConfirmNorm(sentiment, alignedTrendSignal);
    const multilegPenaltyNorm = multilegFlag ? 1 : 0;

    const scoreAvailability = {
      valuePctile: value !== null,
      valueShockNorm: value !== null && minuteOfDayEt !== null,
      volOiNorm: volOiRatio !== null,
      repeatNorm: repeat3m !== null,
      otmNorm: otmPct !== null,
      sideConfidence: typeof execution.executionSide === 'string',
      dteNorm: dte !== null,
      dteSwingNorm: dte !== null,
      spreadNorm: row.bid !== null && row.ask !== null && row.ask > row.bid,
      liquidityQualityNorm: row.price !== null,
      sweepNorm: row.conditionCode !== null && row.conditionCode !== undefined,
      multilegNorm: true,
      multilegPenaltyNorm: true,
      timeNorm: minuteOfDayEt !== null,
      deltaNorm: effectiveDelta !== null,
      ivSkewNorm: (runningCallAvg !== null && runningPutAvg !== null) || (surfaceGreeks && surfaceGreeks.callIvAvg !== null && surfaceGreeks.putIvAvg !== null),
      flowImbalanceNorm: hasDirection ? (value !== null) : true,
      deltaPressureNorm: hasDirection ? (effectiveDelta !== null && value !== null) : true,
      cpOiPressureNorm: oi !== null && dte !== null && otmPct !== null,
      ivSkewSurfaceNorm: surfaceGreeks && surfaceGreeks.callIvAvg !== null && surfaceGreeks.putIvAvg !== null,
      ivTermSlopeNorm: surfaceGreeks && surfaceGreeks.frontIv !== null && surfaceGreeks.backIv !== null,
      underlyingTrendConfirmNorm: hasDirection ? (alignedTrendSignal !== null) : true,
    };

    const sigScoreResult = computeSigScore({
      valuePctile,
      valueShockNorm,
      volOiNorm,
      repeatNorm,
      otmNorm,
      sideConfidence: sideConfidenceVal,
      dteNorm,
      dteSwingNorm,
      spreadNorm,
      liquidityQualityNorm,
      sweepNorm,
      multilegNorm,
      multilegPenaltyNorm,
      timeNorm,
      deltaNorm,
      ivSkewNorm,
      flowImbalanceNorm,
      deltaPressureNorm,
      cpOiPressureNorm,
      ivSkewSurfaceNorm,
      ivTermSlopeNorm,
      underlyingTrendConfirmNorm,
      model: scoreModel,
      weights: scoringConfig.weights || null,
      availability: scoreAvailability,
      returnDetails: true,
    });
    const sigScore = toFiniteNumber(sigScoreResult && sigScoreResult.score) !== null
      ? toFiniteNumber(sigScoreResult.score)
      : 0;
    const unavailableComponents = Array.isArray(sigScoreResult?.unavailableComponents)
      ? sigScoreResult.unavailableComponents
      : [];

    const missingMetricSet = new Set();
    if (value === null) missingMetricSet.add('value');
    if (dte === null) missingMetricSet.add('dte');
    if (otmPct === null) missingMetricSet.add('otmPct');
    if (repeat3m === null) missingMetricSet.add('repeat3m');
    if (oi === null) missingMetricSet.add('oi');
    if (volOiRatio === null) missingMetricSet.add('volOiRatio');
    if (minuteOfDayEt === null) missingMetricSet.add('timeNorm');
    if (row.bid === null || row.ask === null || row.ask <= row.bid) missingMetricSet.add('spreadNorm');
    if (effectiveDelta === null) missingMetricSet.add('deltaNorm');
    if ((runningCallAvg === null || runningPutAvg === null) && (!surfaceGreeks || surfaceGreeks.callIvAvg === null || surfaceGreeks.putIvAvg === null)) {
      missingMetricSet.add('ivSkewNorm');
    }
    unavailableComponents.forEach((component) => missingMetricSet.add(`scoreComponent:${component}`));
    const missingMetrics = Array.from(missingMetricSet);
    const scoreQuality = missingMetrics.length ? 'partial' : 'complete';

    const enriched = {
      tradeId: row.tradeId,
      tradeTsUtc: row.tradeTsUtc,
      symbol: row.symbol,
      expiration: row.expiration,
      strike: row.strike,
      right: row.right,
      price: row.price,
      size,
      bid: row.bid,
      ask: row.ask,
      conditionCode: row.conditionCode,
      exchange: row.exchange,
      value,
      dte,
      spot,
      otmPct,
      dayVolume,
      oi,
      volOiRatio,
      repeat3m,
      sigScore,
      sentiment,
      execution,
      executionSide: execution.executionSide,
      symbolVol1m: minuteStats.symbolVol1m,
      symbolVolBaseline15m: minuteStats.symbolVolBaseline15m,
      openWindowBaseline: minuteStats.openWindowBaseline,
      bullishRatio15m: minuteStats.bullishRatio15m,
      isSweep: sweepFlag ? 1 : 0,
      isMultileg: multilegFlag ? 1 : 0,
      minuteOfDayEt,
      delta: effectiveDelta,
      impliedVol: greeks.impliedVol,
      timeNorm,
      deltaNorm,
      ivSkewNorm,
      valueShockNorm,
      valuePctile,
      volOiNorm,
      repeatNorm,
      otmNorm,
      sideConfidenceVal,
      dteNorm,
      dteSwingNorm,
      spreadNorm,
      liquidityQualityNorm,
      sweepNorm,
      multilegNorm,
      multilegPenaltyNorm,
      flowImbalanceNorm,
      deltaPressureNorm,
      cpOiPressureNorm,
      ivSkewSurfaceNorm,
      ivTermSlopeNorm,
      underlyingTrendConfirmNorm,
      sigScoreComponents: sigScoreResult,
      scoreQuality,
      missingMetrics,
      chips: [],
      ruleVersion: scoreRuleVersion,
    };

    enriched.chips = evaluateChips(enriched, thresholds, { strictScoreQuality });
    rowCount += 1;
    if (metricStatusAccumulator) {
      accumulateMetricStatuses(metricStatusAccumulator, enriched);
    }
    if (minuteRollupState) {
      accumulateMinuteDerivedRollupRow(minuteRollupState, enriched, scoringConfig.dayIso || null);
    }
    if (rowConsumer) {
      rowConsumerBuffer.push(enriched);
      if (rowConsumerBuffer.length >= rowConsumerChunkSize) {
        flushRowConsumerBuffer();
      }
    } else {
      enrichedRows.push(enriched);
    }

    const contractStats = contractStatsMap.get(contractKey) || {
      symbol: row.symbol,
      expiration: row.expiration,
      strike: row.strike,
      right: row.right,
      dayVolume: 0,
      oi: null,
      lastTradeTsUtc: row.tradeTsUtc,
    };

    contractStats.dayVolume = dayVolume;
    if (oi !== null) contractStats.oi = oi;
    contractStats.lastTradeTsUtc = row.tradeTsUtc;
    contractStatsMap.set(contractKey, contractStats);
  });

  flushRowConsumerBuffer();

  if (rollingState) {
    rollingState.contractDayVolume = contractDayVolume;
    rollingState.contractStatsMap = contractStatsMap;
    rollingState.sideWindows = sideWindows;
    rollingState.symbolPressureWindows = symbolPressureWindows;
    rollingState.cpOiPressureWindows = cpOiPressureWindows;
    rollingState.runningCallIv = runningCallIv;
    rollingState.runningPutIv = runningPutIv;
    rollingState.spotLastSeenBySymbol = spotLastSeenBySymbol;
    rollingState.trendLastSeenBySymbol = trendLastSeenBySymbol;
    rollingState.lastContractPrint = lastContractPrint;
    rollingState.featureBaselineUpdates = featureBaselineUpdates;
  }

  return {
    rows: enrichedRows || [],
    rowCount,
    contractStatsMap,
    statsByMinute,
    featureBaselineUpdates,
  };
}

function ensureMetricCacheRows(db, { symbol, dayIso, rows, metricStatuses, markPartial }) {
  METRIC_NAMES.forEach((metricName) => {
    const cacheStatus = markPartial ? DAY_CACHE_STATUS_PARTIAL : metricStatuses[metricName] || DAY_CACHE_STATUS_PARTIAL;
    upsertMetricCache(db, {
      symbol,
      dayIso,
      metricName,
      cacheStatus,
      rowCount: rows.length,
      lastError: null,
    });
  });
}

async function ensureRawHydratedForDay({
  db,
  symbol,
  dayIso,
  env = process.env,
}) {
  const rawRows = getRawTradesForDay(db, { symbol, dayIso });
  if (rawRows.length === 0) {
    return {
      tradeRows: 0,
      stockRows: 0,
      oiRows: 0,
      quoteRows: 0,
      greeksRows: 0,
      supplementalCache: null,
    };
  }

  const supplementalMetrics = await buildSupplementalMetricLookup({
    db,
    symbol,
    dayIso,
    rawRows,
    env,
    requiredMetrics: [],
  });

  return {
    tradeRows: rawRows.length,
    stockRows: countStockOhlcMinuteRaw(db, { symbol, dayIso }),
    oiRows: countOptionOpenInterestRaw(db, { symbol, dayIso }),
    quoteRows: countOptionQuoteMinuteRaw(db, { symbol, dayIso }),
    greeksRows: countOptionGreeksMinuteRaw(db, { symbol, dayIso }),
    supplementalCache: supplementalMetrics.cacheStats || null,
  };
}

async function ensureEnrichedForDay({
  db,
  symbol,
  dayIso,
  forceRecompute = false,
  markPartial = false,
  thresholds,
  env = process.env,
  requiredMetrics = [],
}) {
  const metricCacheMap = getMetricCacheMap(db, { symbol, dayIso });
  const activeRuleConfig = resolveActiveRuleConfig(db, thresholds, env);
  const enrichedRowsCache = metricCacheMap.enrichedRows;

  if (!forceRecompute && enrichedRowsCache) {
    if (!hasMinuteDerivedRows(db, { symbol, dayIso })) {
      const enrichedRows = readEnrichedRows(db, { symbol, dayIso });
      const minuteRollups = buildMinuteDerivedRollups(enrichedRows, dayIso);
      upsertSymbolMinuteDerived(db, minuteRollups.symbolMinuteRows);
      upsertContractMinuteDerived(db, minuteRollups.contractMinuteRows);
    }

    return {
      synced: false,
      reason: enrichedRowsCache.cacheStatus === DAY_CACHE_STATUS_FULL
        ? 'metric_cache_full'
        : 'metric_cache_partial',
      rowCount: enrichedRowsCache.rowCount || 0,
      ruleVersion: activeRuleConfig.versionId,
      scoringModel: activeRuleConfig.scoringModel,
      targetHorizon: activeRuleConfig.targetSpec?.horizon || null,
      supplementalCache: null,
      metricCacheMap,
    };
  }

  const rawRows = getRawTradesForDay(db, { symbol, dayIso });

  const supplementalMetrics = await buildSupplementalMetricLookup({
    db,
    symbol,
    dayIso,
    rawRows,
    env,
    requiredMetrics,
  });
  const built = buildEnrichedRows(rawRows, activeRuleConfig.thresholds, supplementalMetrics, activeRuleConfig);

  const upsertPayload = built.rows.map((row) => ({
    ...row,
    chipsJson: JSON.stringify(row.chips),
    missingMetricsJson: JSON.stringify(row.missingMetrics || []),
    sigScoreComponentsJson: JSON.stringify(row.sigScoreComponents || {}),
  }));

  upsertEnrichedRows(db, upsertPayload);
  upsertContractStats(db, built.contractStatsMap, dayIso);
  upsertSymbolStats(db, symbol, built.statsByMinute);
  const minuteRollups = buildMinuteDerivedRollups(built.rows, dayIso);
  upsertSymbolMinuteDerived(db, minuteRollups.symbolMinuteRows);
  upsertContractMinuteDerived(db, minuteRollups.contractMinuteRows);
  upsertFeatureBaselines(
    db,
    symbol,
    supplementalMetrics.featureBaselines || new Map(),
    built.featureBaselineUpdates || new Map(),
  );

  const metricStatuses = calculateMetricStatuses(built.rows, markPartial);
  ensureMetricCacheRows(db, {
    symbol,
    dayIso,
    rows: built.rows,
    metricStatuses,
    markPartial,
  });

  return {
    synced: true,
    reason: null,
    rowCount: built.rows.length,
    ruleVersion: activeRuleConfig.versionId,
    scoringModel: activeRuleConfig.scoringModel,
    targetHorizon: activeRuleConfig.targetSpec?.horizon || null,
    supplementalCache: supplementalMetrics.cacheStats || null,
    metricCacheMap: getMetricCacheMap(db, { symbol, dayIso }),
  };
}

function buildMetricUnavailableError(requiredMetrics, metricCacheMap) {
  const unavailableMetrics = requiredMetrics.filter((metricName) => {
    const cache = metricCacheMap[metricName];
    return !cache || cache.cacheStatus !== DAY_CACHE_STATUS_FULL;
  });

  if (!unavailableMetrics.length) {
    return null;
  }

  return {
    status: 422,
    error: {
      code: 'metric_unavailable',
      message: `Required metric cache is not full for: ${unavailableMetrics.join(', ')}`,
      details: unavailableMetrics.map((metricName) => ({
        metric: metricName,
        cacheStatus: metricCacheMap[metricName]?.cacheStatus || null,
        lastError: metricCacheMap[metricName]?.lastError || null,
      })),
    },
  };
}

function parseJsonArrayField(jsonValue) {
  try {
    const parsed = JSON.parse(jsonValue || '[]');
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function parseJsonObjectField(jsonValue) {
  try {
    const parsed = JSON.parse(jsonValue || '{}');
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

function hydrateEnrichedRows(rows = []) {
  return rows.map((row) => ({
    ...row,
    chips: parseJsonArrayField(row.chipsJson),
    missingMetrics: parseJsonArrayField(row.missingMetricsJson),
    sigScoreComponents: parseJsonObjectField(row.sigScoreComponentsJson),
    chipsJson: undefined,
    missingMetricsJson: undefined,
    sigScoreComponentsJson: undefined,
  }));
}

function readEnrichedRows(db, { symbol, dayIso }) {
  const from = `${dayIso}T00:00:00.000Z`;
  const to = `${dayIso}T23:59:59.999Z`;

  const rows = db.prepare(`
    SELECT
      trade_id AS id,
      trade_ts_utc AS tradeTsUtc,
      symbol,
      expiration,
      strike,
      option_right AS right,
      price,
      size,
      bid,
      ask,
      condition_code AS conditionCode,
      exchange,
      value,
      dte,
      spot,
      otm_pct AS otmPct,
      day_volume AS dayVolume,
      oi,
      vol_oi_ratio AS volOiRatio,
      repeat3m,
      sig_score AS sigScore,
      sentiment,
      execution_side AS executionSide,
      symbol_vol_1m AS symbolVol1m,
      symbol_vol_baseline_15m AS symbolVolBaseline15m,
      open_window_baseline AS openWindowBaseline,
      bullish_ratio_15m AS bullishRatio15m,
      is_sweep AS isSweep,
      is_multileg AS isMultileg,
      minute_of_day_et AS minuteOfDayEt,
      delta,
      implied_vol AS impliedVol,
      time_norm AS timeNorm,
      delta_norm AS deltaNorm,
      iv_skew_norm AS ivSkewNorm,
      value_shock_norm AS valueShockNorm,
      dte_swing_norm AS dteSwingNorm,
      flow_imbalance_norm AS flowImbalanceNorm,
      delta_pressure_norm AS deltaPressureNorm,
      cp_oi_pressure_norm AS cpOiPressureNorm,
      iv_skew_surface_norm AS ivSkewSurfaceNorm,
      iv_term_slope_norm AS ivTermSlopeNorm,
      underlying_trend_confirm_norm AS underlyingTrendConfirmNorm,
      liquidity_quality_norm AS liquidityQualityNorm,
      multileg_penalty_norm AS multilegPenaltyNorm,
      rule_version AS ruleVersion,
      score_quality AS scoreQuality,
      missing_metrics_json AS missingMetricsJson,
      sig_score_components_json AS sigScoreComponentsJson,
      chips_json AS chipsJson
    FROM option_trade_enriched
    WHERE symbol = @symbol
      AND trade_ts_utc >= @from
      AND trade_ts_utc <= @to
    ORDER BY trade_ts_utc ASC, trade_id ASC
  `).all({ symbol, from, to });

  return hydrateEnrichedRows(rows);
}

function readClickHouseEnrichedRows({
  symbol,
  from,
  to,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
}) {
  const rows = queryRows(`
    SELECT
      trade_id AS id,
      concat(replaceAll(toString(trade_ts_utc, 'UTC'), ' ', 'T'), 'Z') AS tradeTsUtc,
      symbol,
      toString(expiration) AS expiration,
      strike,
      option_right AS right,
      price,
      size,
      bid,
      ask,
      condition_code AS conditionCode,
      exchange,
      value,
      dte,
      spot,
      otm_pct AS otmPct,
      day_volume AS dayVolume,
      oi,
      vol_oi_ratio AS volOiRatio,
      repeat3m,
      sig_score AS sigScore,
      sentiment,
      execution_side AS executionSide,
      symbol_vol_1m AS symbolVol1m,
      symbol_vol_baseline_15m AS symbolVolBaseline15m,
      open_window_baseline AS openWindowBaseline,
      bullish_ratio_15m AS bullishRatio15m,
      is_sweep AS isSweep,
      is_multileg AS isMultileg,
      minute_of_day_et AS minuteOfDayEt,
      delta,
      implied_vol AS impliedVol,
      time_norm AS timeNorm,
      delta_norm AS deltaNorm,
      iv_skew_norm AS ivSkewNorm,
      value_shock_norm AS valueShockNorm,
      dte_swing_norm AS dteSwingNorm,
      flow_imbalance_norm AS flowImbalanceNorm,
      delta_pressure_norm AS deltaPressureNorm,
      cp_oi_pressure_norm AS cpOiPressureNorm,
      iv_skew_surface_norm AS ivSkewSurfaceNorm,
      iv_term_slope_norm AS ivTermSlopeNorm,
      underlying_trend_confirm_norm AS underlyingTrendConfirmNorm,
      liquidity_quality_norm AS liquidityQualityNorm,
      multileg_penalty_norm AS multilegPenaltyNorm,
      rule_version AS ruleVersion,
      score_quality AS scoreQuality,
      missing_metrics_json AS missingMetricsJson,
      sig_score_components_json AS sigScoreComponentsJson,
      chips_json AS chipsJson
    FROM options.option_trade_enriched
    WHERE symbol = {symbol:String}
      AND trade_ts_utc >= parseDateTime64BestEffort({from:String})
      AND trade_ts_utc <= parseDateTime64BestEffort({to:String})
    ORDER BY trade_ts_utc ASC, trade_id ASC
  `, { symbol, from, to }, env);

  return hydrateEnrichedRows(rows);
}

function summarizeMetricCacheReason(metricCacheMap = {}) {
  const entries = Object.values(metricCacheMap);
  if (!entries.length) return 'clickhouse_cached';
  return entries.every((row) => row.cacheStatus === DAY_CACHE_STATUS_FULL)
    ? 'metric_cache_full'
    : 'metric_cache_partial';
}

function loadClickHouseHistoricalDay({
  symbol,
  dayIso,
  from,
  to,
  requiredMetrics,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
}) {
  const dayCache = getClickHouseDayCache({ symbol, dayIso, env, queryRows });
  if (!dayCache || dayCache.cacheStatus !== DAY_CACHE_STATUS_FULL) {
    return null;
  }

  const metricCacheMap = getClickHouseMetricCacheMap({ symbol, dayIso, env, queryRows });
  const metricUnavailable = buildMetricUnavailableError(requiredMetrics, metricCacheMap);
  if (metricUnavailable) {
    return null;
  }

  const rows = readClickHouseEnrichedRows({ symbol, from, to, env, queryRows });
  if (!rows.length && Number(dayCache.rowCount || 0) > 0) {
    return null;
  }

  return {
    rows,
    dayCache,
    metricCacheMap,
    observability: {
      source: 'clickhouse',
      artifactPath: buildClickHouseArtifactPath(env),
    },
    enrichment: {
      synced: false,
      reason: summarizeMetricCacheReason(metricCacheMap),
      rowCount: rows.length,
      ruleVersion: rows[0]?.ruleVersion || null,
      scoringModel: null,
      targetHorizon: null,
      supplementalCache: null,
    },
  };
}

function countClickHouseTradesForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT count() AS count
    FROM options.option_trades
    WHERE symbol = {symbol:String}
      AND trade_date = toDate({dayIso:String})
  `, { symbol, dayIso }, env);
  return Number(rows?.[0]?.count || 0);
}

function getClickHouseTradeResumeCursor({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      ifNull(concat(replaceAll(toString(max(trade_ts_utc), 'UTC'), ' ', 'T'), 'Z'), null) AS maxTradeTsUtc
    FROM options.option_trades
    WHERE symbol = {symbol:String}
      AND trade_date = toDate({dayIso:String})
  `, { symbol, dayIso }, env);

  const maxTradeTsUtc = rows?.[0]?.maxTradeTsUtc || null;
  if (!maxTradeTsUtc) return null;
  const resumeFromIso = floorIsoToMinute(maxTradeTsUtc);
  if (!resumeFromIso) return null;
  const startTime = isoToTimeHms(resumeFromIso);
  if (!startTime) return null;
  return { startTime, resumeFromIso };
}

function countClickHouseStockRawRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT count() AS count
    FROM options.stock_ohlc_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
  `, { symbol, dayIso }, env);
  return Number(rows?.[0]?.count || 0);
}

function countClickHouseOptionOiRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT count() AS count
    FROM options.option_open_interest_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
  `, { symbol, dayIso }, env);
  return Number(rows?.[0]?.count || 0);
}

function countClickHouseOptionGreeksRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT count() AS count
    FROM options.option_greeks_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
  `, { symbol, dayIso }, env);
  return Number(rows?.[0]?.count || 0);
}

function normalizeClickHouseMinuteCountRows(rows = []) {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  return rows
    .map((row) => {
      const minuteBucketUtc = floorIsoToMinute(row?.minuteBucketUtc);
      const rowCount = Math.max(0, Math.trunc(Number(row?.rowCount || 0)));
      if (!minuteBucketUtc || rowCount <= 0) return null;
      return {
        minuteBucketUtc,
        rowCount,
      };
    })
    .filter(Boolean);
}

function loadClickHouseTradeMinuteCountRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      concat(replaceAll(toString(toStartOfMinute(trade_ts_utc), 'UTC'), ' ', 'T'), 'Z') AS minuteBucketUtc,
      count() AS rowCount
    FROM options.option_trades
    WHERE symbol = {symbol:String}
      AND trade_date = toDate({dayIso:String})
    GROUP BY toStartOfMinute(trade_ts_utc)
    ORDER BY toStartOfMinute(trade_ts_utc) ASC
  `, { symbol, dayIso }, env);
  return normalizeClickHouseMinuteCountRows(rows);
}

function loadClickHouseOptionQuoteMinuteCountRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      concat(replaceAll(toString(minute_bucket_utc, 'UTC'), ' ', 'T'), 'Z') AS minuteBucketUtc,
      count() AS rowCount
    FROM options.option_quote_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY minuteBucketUtc
    ORDER BY minuteBucketUtc ASC
  `, { symbol, dayIso }, env);
  return normalizeClickHouseMinuteCountRows(rows);
}

function loadClickHouseStockMinuteCountRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      concat(replaceAll(toString(minute_bucket_utc, 'UTC'), ' ', 'T'), 'Z') AS minuteBucketUtc,
      count() AS rowCount
    FROM options.stock_ohlc_minute_raw
    WHERE symbol = {symbol:String}
      AND trade_date_utc = toDate({dayIso:String})
    GROUP BY minuteBucketUtc
    ORDER BY minuteBucketUtc ASC
  `, { symbol, dayIso }, env);
  return normalizeClickHouseMinuteCountRows(rows);
}

function loadClickHouseEnrichedMinuteCountRowsForDay({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      concat(replaceAll(toString(toStartOfMinute(trade_ts_utc), 'UTC'), ' ', 'T'), 'Z') AS minuteBucketUtc,
      count() AS rowCount
    FROM options.option_trade_enriched
    WHERE symbol = {symbol:String}
      AND trade_date = toDate({dayIso:String})
    GROUP BY toStartOfMinute(trade_ts_utc)
    ORDER BY toStartOfMinute(trade_ts_utc) ASC
  `, { symbol, dayIso }, env);
  return normalizeClickHouseMinuteCountRows(rows);
}

function computeExpectedSessionMinuteSlots(sessionWindow, { useRegularClose = false, includeCloseMinute = true } = {}) {
  if (!sessionWindow || sessionWindow.isOpen === false) return 0;
  const openSecond = parseTimeHmsToSecondOfDay(sessionWindow.openTime);
  const closeSecond = parseTimeHmsToSecondOfDay(
    useRegularClose
      ? (sessionWindow.regularCloseTime || sessionWindow.closeTime)
      : sessionWindow.closeTime,
  );
  if (openSecond === null || closeSecond === null) return null;
  const boundedCloseSecond = Math.max(openSecond, closeSecond);
  const baseSlots = Math.floor((boundedCloseSecond - openSecond) / 60);
  if (baseSlots <= 0) return includeCloseMinute ? 1 : 0;
  return includeCloseMinute ? (baseSlots + 1) : baseSlots;
}

function summarizeMinuteRowsRange(minuteRows = []) {
  if (!Array.isArray(minuteRows) || minuteRows.length === 0) {
    return {
      slots: 0,
      firstMinuteUtc: null,
      lastMinuteUtc: null,
    };
  }
  return {
    slots: minuteRows.length,
    firstMinuteUtc: minuteRows[0]?.minuteBucketUtc || null,
    lastMinuteUtc: minuteRows[minuteRows.length - 1]?.minuteBucketUtc || null,
  };
}

function isoMinuteToSecondOfDay(isoValue) {
  if (typeof isoValue !== 'string' || !isoValue.trim()) return null;
  const parsed = new Date(isoValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return (parsed.getUTCHours() * 3600) + (parsed.getUTCMinutes() * 60);
}

function buildQuoteGapFillWindowsForDay({ dayIso, sessionWindow, existingMinuteRows = [] }) {
  if (!sessionWindow || sessionWindow.isOpen === false) {
    return {
      dayIso,
      expectedMinutes: 0,
      missingMinutes: 0,
      missingFraction: 0,
      windows: [],
    };
  }

  const openSecond = parseTimeHmsToSecondOfDay(sessionWindow.openTime);
  const closeSecond = parseTimeHmsToSecondOfDay(sessionWindow.regularCloseTime || sessionWindow.closeTime);
  if (
    openSecond === null
    || closeSecond === null
    || closeSecond <= openSecond
  ) {
    return {
      dayIso,
      expectedMinutes: 0,
      missingMinutes: 0,
      missingFraction: 0,
      windows: [],
    };
  }

  const expectedMinutes = Math.max(0, Math.floor((closeSecond - openSecond) / 60));
  const existingSeconds = new Set();
  existingMinuteRows.forEach((row) => {
    const second = isoMinuteToSecondOfDay(row?.minuteBucketUtc);
    if (second !== null) existingSeconds.add(second);
  });

  const missingSeconds = [];
  for (let second = openSecond; second < closeSecond; second += 60) {
    if (!existingSeconds.has(second)) {
      missingSeconds.push(second);
    }
  }

  const windows = [];
  let rangeStart = null;
  let previousSecond = null;
  missingSeconds.forEach((second) => {
    if (rangeStart === null) {
      rangeStart = second;
      previousSecond = second;
      return;
    }
    if (second === (previousSecond + 60)) {
      previousSecond = second;
      return;
    }
    windows.push({
      startTime: formatSecondOfDayAsHms(rangeStart),
      endTime: formatSecondOfDayAsHms(previousSecond),
    });
    rangeStart = second;
    previousSecond = second;
  });
  if (rangeStart !== null) {
    windows.push({
      startTime: formatSecondOfDayAsHms(rangeStart),
      endTime: formatSecondOfDayAsHms(previousSecond),
    });
  }

  return {
    dayIso,
    expectedMinutes,
    missingMinutes: missingSeconds.length,
    missingFraction: expectedMinutes > 0 ? (missingSeconds.length / expectedMinutes) : 0,
    windows,
  };
}

async function buildClickHouseGapTelemetryForDay({
  symbol,
  dayIso,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
}) {
  const [tradeMinuteRows, quoteMinuteRows, stockMinuteRows, enrichMinuteRows, sessionWindow] = await Promise.all([
    loadClickHouseTradeMinuteCountRowsForDay({ symbol, dayIso, env, queryRows }),
    loadClickHouseOptionQuoteMinuteCountRowsForDay({ symbol, dayIso, env, queryRows }),
    loadClickHouseStockMinuteCountRowsForDay({ symbol, dayIso, env, queryRows }),
    loadClickHouseEnrichedMinuteCountRowsForDay({ symbol, dayIso, env, queryRows }),
    resolveThetaCalendarSessionWindowForDay(dayIso, { env }),
  ]);

  const tradeSummary = summarizeMinuteRowsRange(tradeMinuteRows);
  const quoteSummary = summarizeMinuteRowsRange(quoteMinuteRows);
  const stockSummary = summarizeMinuteRowsRange(stockMinuteRows);
  const enrichSummary = summarizeMinuteRowsRange(enrichMinuteRows);
  const expectedPaddedSlots = computeExpectedSessionMinuteSlots(sessionWindow, { useRegularClose: false, includeCloseMinute: true });
  const expectedCoreSlots = computeExpectedSessionMinuteSlots(sessionWindow, { useRegularClose: true, includeCloseMinute: false });
  const missingStockSlots = expectedPaddedSlots === null ? null : Math.max(0, expectedPaddedSlots - stockSummary.slots);
  const missingQuoteSlots = expectedPaddedSlots === null ? null : Math.max(0, expectedPaddedSlots - quoteSummary.slots);
  const missingTradeSlots = expectedPaddedSlots === null ? null : Math.max(0, expectedPaddedSlots - tradeSummary.slots);
  const missingQuoteCoreSlots = expectedCoreSlots === null ? null : Math.max(0, expectedCoreSlots - quoteSummary.slots);
  const missingTradeCoreSlots = expectedCoreSlots === null ? null : Math.max(0, expectedCoreSlots - tradeSummary.slots);
  const stockCoveragePct = expectedPaddedSlots > 0
    ? Number(((stockSummary.slots / expectedPaddedSlots) * 100).toFixed(2))
    : null;
  const quoteCoveragePct = expectedCoreSlots > 0
    ? Number(((quoteSummary.slots / expectedCoreSlots) * 100).toFixed(2))
    : null;
  const tradeCoveragePct = expectedCoreSlots > 0
    ? Number(((tradeSummary.slots / expectedCoreSlots) * 100).toFixed(2))
    : null;

  return {
    dayType: sessionWindow?.type || null,
    sessionOpenTime: sessionWindow?.openTime || null,
    sessionRegularCloseTime: sessionWindow?.regularCloseTime || null,
    sessionCloseTime: sessionWindow?.closeTime || null,
    expectedSlots: expectedPaddedSlots,
    expectedPaddedSlots,
    expectedCoreSlots,
    stockSlots: stockSummary.slots,
    quoteSlots: quoteSummary.slots,
    tradeSlots: tradeSummary.slots,
    enrichSlots: enrichSummary.slots,
    missingStockSlots,
    missingQuoteSlots,
    missingTradeSlots,
    missingQuoteCoreSlots,
    missingTradeCoreSlots,
    stockCoveragePct,
    quoteCoveragePct,
    tradeCoveragePct,
    missingEnrichVsTradeSlots: Math.max(0, tradeSummary.slots - enrichSummary.slots),
    stockFirstMinuteUtc: stockSummary.firstMinuteUtc,
    stockLastMinuteUtc: stockSummary.lastMinuteUtc,
    quoteFirstMinuteUtc: quoteSummary.firstMinuteUtc,
    quoteLastMinuteUtc: quoteSummary.lastMinuteUtc,
    tradeFirstMinuteUtc: tradeSummary.firstMinuteUtc,
    tradeLastMinuteUtc: tradeSummary.lastMinuteUtc,
    enrichFirstMinuteUtc: enrichSummary.firstMinuteUtc,
    enrichLastMinuteUtc: enrichSummary.lastMinuteUtc,
  };
}

function loadClickHouseDownloadMinuteCountRowsForStream({
  symbol,
  dayIso,
  streamName,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
}) {
  switch (streamName) {
    case DOWNLOAD_CHUNK_STREAMS.TRADE_QUOTE_1M:
      return loadClickHouseTradeMinuteCountRowsForDay({ symbol, dayIso, env, queryRows });
    case DOWNLOAD_CHUNK_STREAMS.OPTION_QUOTE_1M:
      return loadClickHouseOptionQuoteMinuteCountRowsForDay({ symbol, dayIso, env, queryRows });
    case DOWNLOAD_CHUNK_STREAMS.STOCK_PRICE_1M:
      return loadClickHouseStockMinuteCountRowsForDay({ symbol, dayIso, env, queryRows });
    default:
      throw new Error(`unsupported_download_chunk_stream:${streamName}`);
  }
}

function upsertClickHouseDownloadChunkStatusForStream({
  symbol,
  dayIso,
  streamName,
  sourceEndpoint = null,
  lastError = null,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
}) {
  ensureClickHouseSupportSchema(env);
  const chunkMinutes = parseClickHouseChunkStatusMinutes(env);
  const minuteRows = loadClickHouseDownloadMinuteCountRowsForStream({
    symbol,
    dayIso,
    streamName,
    env,
    queryRows,
  });
  const chunkRows = Array.from(buildChunkMapFromMinuteRows(minuteRows, chunkMinutes).values())
    .sort((left, right) => Date.parse(left.chunkStartUtc) - Date.parse(right.chunkStartUtc));
  const nowIso = new Date().toISOString();

  replaceClickHouseDayRows({
    tableName: 'option_download_chunk_status',
    whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String}) AND stream_name = {streamName:String}',
    deleteParams: { symbol, dayIso, streamName },
    columns: [
      'symbol',
      'trade_date_utc',
      'stream_name',
      'chunk_start_utc',
      'chunk_end_utc',
      'chunk_minutes',
      'row_count',
      'minute_count',
      'status',
      'source_endpoint',
      'last_error',
      'updated_at_utc',
    ],
    rows: chunkRows.map((row) => ({
      symbol,
      trade_date_utc: dayIso,
      stream_name: streamName,
      chunk_start_utc: row.chunkStartUtc,
      chunk_end_utc: row.chunkEndUtc,
      chunk_minutes: chunkMinutes,
      row_count: row.rowCount,
      minute_count: row.minuteCount,
      status: CHUNK_STATUS_STATE.AVAILABLE,
      source_endpoint: sourceEndpoint,
      last_error: lastError,
      updated_at_utc: nowIso,
    })),
    env,
  });

  return {
    streamName,
    chunkMinutes,
    chunkCount: chunkRows.length,
    rowCount: chunkRows.reduce((acc, row) => acc + row.rowCount, 0),
    minuteCount: chunkRows.reduce((acc, row) => acc + row.minuteCount, 0),
  };
}

function resolveEnrichChunkStatus(inputRowCount, outputRowCount) {
  if (inputRowCount <= 0 && outputRowCount > 0) return CHUNK_STATUS_STATE.EXTRA;
  if (inputRowCount <= 0 && outputRowCount <= 0) return CHUNK_STATUS_STATE.AVAILABLE;
  if (outputRowCount === inputRowCount) return CHUNK_STATUS_STATE.COMPLETE;
  if (outputRowCount <= 0) return CHUNK_STATUS_STATE.MISSING;
  if (outputRowCount < inputRowCount) return CHUNK_STATUS_STATE.PARTIAL;
  return CHUNK_STATUS_STATE.EXTRA;
}

function upsertClickHouseEnrichChunkStatusForDay({
  symbol,
  dayIso,
  ruleVersion = null,
  lastError = null,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
}) {
  ensureClickHouseSupportSchema(env);
  const chunkMinutes = parseClickHouseChunkStatusMinutes(env);
  const inputChunkMap = buildChunkMapFromMinuteRows(
    loadClickHouseTradeMinuteCountRowsForDay({ symbol, dayIso, env, queryRows }),
    chunkMinutes,
  );
  const outputChunkMap = buildChunkMapFromMinuteRows(
    loadClickHouseEnrichedMinuteCountRowsForDay({ symbol, dayIso, env, queryRows }),
    chunkMinutes,
  );
  const chunkStarts = Array.from(new Set([...inputChunkMap.keys(), ...outputChunkMap.keys()]))
    .sort((left, right) => Date.parse(left) - Date.parse(right));
  const nowIso = new Date().toISOString();

  replaceClickHouseDayRows({
    tableName: 'option_enrich_chunk_status',
    whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String}) AND stream_name = {streamName:String}',
    deleteParams: { symbol, dayIso, streamName: ENRICH_CHUNK_STREAM },
    columns: [
      'symbol',
      'trade_date_utc',
      'stream_name',
      'chunk_start_utc',
      'chunk_end_utc',
      'chunk_minutes',
      'input_row_count',
      'output_row_count',
      'input_minute_count',
      'output_minute_count',
      'status',
      'rule_version',
      'last_error',
      'updated_at_utc',
    ],
    rows: chunkStarts.map((chunkStartUtc) => {
      const inputChunk = inputChunkMap.get(chunkStartUtc) || null;
      const outputChunk = outputChunkMap.get(chunkStartUtc) || null;
      const chunkEndUtc = inputChunk?.chunkEndUtc
        || outputChunk?.chunkEndUtc
        || addMinutesToIso(chunkStartUtc, chunkMinutes);
      const inputRowCount = Math.max(0, Math.trunc(Number(inputChunk?.rowCount || 0)));
      const outputRowCount = Math.max(0, Math.trunc(Number(outputChunk?.rowCount || 0)));
      const inputMinuteCount = Math.max(0, Math.trunc(Number(inputChunk?.minuteCount || 0)));
      const outputMinuteCount = Math.max(0, Math.trunc(Number(outputChunk?.minuteCount || 0)));
      return {
        symbol,
        trade_date_utc: dayIso,
        stream_name: ENRICH_CHUNK_STREAM,
        chunk_start_utc: chunkStartUtc,
        chunk_end_utc: chunkEndUtc,
        chunk_minutes: chunkMinutes,
        input_row_count: inputRowCount,
        output_row_count: outputRowCount,
        input_minute_count: inputMinuteCount,
        output_minute_count: outputMinuteCount,
        status: resolveEnrichChunkStatus(inputRowCount, outputRowCount),
        rule_version: ruleVersion,
        last_error: lastError,
        updated_at_utc: nowIso,
      };
    }),
    env,
  });

  return {
    chunkMinutes,
    chunkCount: chunkStarts.length,
    inputRowCount: chunkStarts.reduce((acc, chunkStartUtc) => acc + Math.max(0, Math.trunc(Number(inputChunkMap.get(chunkStartUtc)?.rowCount || 0))), 0),
    outputRowCount: chunkStarts.reduce((acc, chunkStartUtc) => acc + Math.max(0, Math.trunc(Number(outputChunkMap.get(chunkStartUtc)?.rowCount || 0))), 0),
  };
}

function normalizeClickHouseRawTradeRow(row) {
  const normalized = {
    tradeId: row.tradeId,
    tradeTsUtc: row.tradeTsUtc,
    symbol: row.symbol,
    expiration: row.expiration,
    strike: toFiniteNumber(row.strike),
    right: normalizeRight(row.optionRight),
    price: toFiniteNumber(row.price),
    size: Number.isFinite(Number(row.size)) ? Math.trunc(Number(row.size)) : 0,
    bid: toFiniteNumber(row.bid),
    ask: toFiniteNumber(row.ask),
    conditionCode: row.conditionCode === null ? null : String(row.conditionCode),
    exchange: row.exchange === null ? null : String(row.exchange),
    payloadSpot: toFiniteNumber(row.payloadSpot),
    payloadOi: toFiniteNumber(row.payloadOi),
    rawPayloadJson: row.rawPayloadJson || '{}',
  };
  if (!normalized.tradeId || !normalized.tradeTsUtc || !normalized.symbol || !normalized.expiration || !normalized.right) {
    return null;
  }
  return normalized;
}

function loadClickHouseRawTradesForDay({
  symbol,
  dayIso,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
  onChunk = null,
}) {
  const emitChunk = typeof onChunk === 'function' ? onChunk : null;
  const rows = loadClickHouseTradeRowsForDay({
    symbol,
    dayIso,
    env,
    queryRows,
    includeRawPayloadJson: false,
    onChunk: emitChunk
      ? (chunkRows) => {
        const normalizedChunk = chunkRows
          .map((row) => normalizeClickHouseRawTradeRow(row))
          .filter(Boolean);
        if (normalizedChunk.length > 0) {
          emitChunk(normalizedChunk);
        }
      }
      : null,
  });
  if (emitChunk) return [];
  return rows
    .map((row) => normalizeClickHouseRawTradeRow(row))
    .filter(Boolean);
}

function buildClickHouseStreamingPrecompute({
  symbol,
  dayIso,
  env = process.env,
  queryRows = queryClickHouseRowsSync,
}) {
  const minuteMap = new Map();
  const lastContractPrint = new Map();
  const valueSamples = [];
  const payloadSpotBySymbol = new Map();
  const payloadOiByContract = new Map();
  let rowCount = 0;

  loadClickHouseRawTradesForDay({
    symbol,
    dayIso,
    env,
    queryRows,
    onChunk: (rows) => {
      rows.forEach((row) => {
        rowCount += 1;
        const minuteBucket = toMinuteBucketUtc(row.tradeTsUtc);
        if (minuteBucket) {
          const existing = minuteMap.get(minuteBucket) || {
            minuteBucket,
            volume: 0,
            bullish: 0,
            bearish: 0,
          };
          const size = toFiniteNumber(row.size) || 0;
          const contractKey = buildContractKey(row);
          const previousPrint = lastContractPrint.get(contractKey) || null;
          const execution = computeExecutionFlags({
            ...row,
            lastTradePrice: previousPrint ? previousPrint.tradePrice : null,
            lastExecutionSide: previousPrint ? previousPrint.executionSide : null,
          });
          const sentiment = computeSentiment({ right: row.right, executionSide: execution.executionSide });
          if (toFiniteNumber(row.price) !== null) {
            lastContractPrint.set(contractKey, {
              tradePrice: toFiniteNumber(row.price),
              executionSide: execution.executionSide,
            });
          }
          existing.volume += size;
          if (sentiment === 'bullish') existing.bullish += 1;
          if (sentiment === 'bearish') existing.bearish += 1;
          minuteMap.set(minuteBucket, existing);
        }

        const value = computeValue(row.price, row.size);
        if (value !== null) {
          valueSamples.push(value);
        }

        const payloadSpot = toFiniteNumber(row.payloadSpot);
        if (payloadSpot !== null && !payloadSpotBySymbol.has(row.symbol)) {
          payloadSpotBySymbol.set(row.symbol, payloadSpot);
        }

        const payloadOi = toFiniteNumber(row.payloadOi);
        if (payloadOi !== null) {
          payloadOiByContract.set(buildContractKey(row), Math.trunc(payloadOi));
        }
      });
    },
  });

  valueSamples.sort((left, right) => left - right);
  return {
    statsByMinute: finalizeMinuteStatsMap(minuteMap),
    valueSamples,
    minValue: valueSamples.length ? valueSamples[0] : 0,
    maxValue: valueSamples.length ? valueSamples[valueSamples.length - 1] : 0,
    payloadSpotBySymbol,
    payloadOiByContract,
    rowCount,
  };
}

function loadClickHouseContractOiFromRaw({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = loadClickHouseOptionOiRawRowsForDay({ symbol, dayIso, env, queryRows });
  const map = new Map();
  rows.forEach((row) => {
    const oi = toFiniteNumber(row.oi);
    if (oi === null) return;
    map.set(buildContractKey({
      symbol: row.symbol,
      expiration: row.expiration,
      strike: toFiniteNumber(row.strike),
      right: row.right,
    }), Math.trunc(oi));
  });
  return map;
}

function loadClickHouseContractOiFromStats({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      symbol,
      toString(expiration) AS expiration,
      strike,
      option_right AS right,
      argMax(oi, updated_at_utc) AS oi
    FROM options.contract_stats_intraday
    WHERE symbol = {symbol:String}
      AND session_date = toDate({dayIso:String})
    GROUP BY symbol, expiration, strike, right
  `, { symbol, dayIso }, env);

  const map = new Map();
  rows.forEach((row) => {
    const oi = toFiniteNumber(row.oi);
    if (oi === null || oi <= 0) return;
    map.set(buildContractKey({
      symbol: row.symbol,
      expiration: row.expiration,
      strike: toFiniteNumber(row.strike),
      right: row.right,
    }), Math.trunc(oi));
  });
  return map;
}

function loadClickHouseReferenceOiMap({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  ensureClickHouseSupportSchema(env);
  const rows = queryRows(`
    SELECT
      symbol,
      toString(expiration) AS expiration,
      strike,
      option_right AS right,
      argMax(oi, ingested_at_utc) AS oi
    FROM options.option_open_interest_reference
    WHERE symbol = {symbol:String}
      AND as_of_date = toDate({dayIso:String})
    GROUP BY symbol, expiration, strike, right
  `, { symbol, dayIso }, env);

  const map = new Map();
  rows.forEach((row) => {
    const oi = toFiniteNumber(row.oi);
    if (oi === null) return;
    map.set(buildContractKey({
      symbol: row.symbol,
      expiration: row.expiration,
      strike: toFiniteNumber(row.strike),
      right: row.right,
    }), Math.trunc(oi));
  });
  return map;
}

function createGreeksLookupState() {
  return {
    greeksByContractMinute: new Map(),
    minuteExpirationAgg: new Map(),
  };
}

function accumulateGreeksLookupRows(symbol, greeksRows = [], state = createGreeksLookupState()) {
  if (!Array.isArray(greeksRows) || greeksRows.length === 0) return state;

  greeksRows.forEach((row) => {
    const strike = toFiniteNumber(row.strike);
    const right = normalizeRight(row.right);
    const expiration = normalizeIsoDate(row.expiration);
    const minuteBucket = row.minuteBucketUtc;
    if (strike === null || !right || !expiration || !minuteBucket) return;

    const contractKey = buildContractKey({ symbol, expiration, strike, right });
    state.greeksByContractMinute.set(`${contractKey}|${minuteBucket}`, {
      delta: toFiniteNumber(row.delta),
      impliedVol: toFiniteNumber(row.impliedVol),
    });

    const surfaceKey = `${minuteBucket}|${expiration}`;
    const current = state.minuteExpirationAgg.get(surfaceKey) || {
      expiration,
      ivSum: 0,
      ivCount: 0,
      callIvSum: 0,
      callIvCount: 0,
      putIvSum: 0,
      putIvCount: 0,
    };

    const impliedVol = toFiniteNumber(row.impliedVol);
    if (impliedVol !== null) {
      current.ivSum += impliedVol;
      current.ivCount += 1;
      if (right === 'CALL') {
        current.callIvSum += impliedVol;
        current.callIvCount += 1;
      } else if (right === 'PUT') {
        current.putIvSum += impliedVol;
        current.putIvCount += 1;
      }
      state.minuteExpirationAgg.set(surfaceKey, current);
    }
  });

  return state;
}

function finalizeGreeksLookupState(symbol, state = createGreeksLookupState()) {
  const minuteToExpirationSeries = new Map();
  state.minuteExpirationAgg.forEach((aggState, key) => {
    const [minuteBucket] = key.split('|');
    const list = minuteToExpirationSeries.get(minuteBucket) || [];
    list.push({
      expiration: aggState.expiration,
      ivAvg: aggState.ivCount > 0 ? (aggState.ivSum / aggState.ivCount) : null,
      callIvAvg: aggState.callIvCount > 0 ? (aggState.callIvSum / aggState.callIvCount) : null,
      putIvAvg: aggState.putIvCount > 0 ? (aggState.putIvSum / aggState.putIvCount) : null,
    });
    minuteToExpirationSeries.set(minuteBucket, list);
  });

  const greeksSurfaceBySymbolMinute = new Map();
  minuteToExpirationSeries.forEach((entries, minuteBucket) => {
    const sorted = entries
      .slice()
      .sort((left, right) => Date.parse(left.expiration) - Date.parse(right.expiration));
    const callSeries = sorted.map((entry) => entry.callIvAvg).filter((value) => value !== null);
    const putSeries = sorted.map((entry) => entry.putIvAvg).filter((value) => value !== null);
    const callIvAvg = callSeries.length ? (callSeries.reduce((acc, value) => acc + value, 0) / callSeries.length) : null;
    const putIvAvg = putSeries.length ? (putSeries.reduce((acc, value) => acc + value, 0) / putSeries.length) : null;
    const ivSeries = sorted.map((entry) => entry.ivAvg).filter((value) => value !== null);
    const frontIv = ivSeries.length ? ivSeries[0] : null;
    const backIv = ivSeries.length ? ivSeries[ivSeries.length - 1] : null;

    greeksSurfaceBySymbolMinute.set(`${symbol}|${minuteBucket}`, {
      callIvAvg,
      putIvAvg,
      ivSkewSurfaceNorm: computeIvSkewNorm(callIvAvg, putIvAvg),
      ivTermSlopeNorm: computeIvTermSlopeNorm(frontIv, backIv),
      frontIv,
      backIv,
    });
  });

  return {
    greeksByContractMinute: state.greeksByContractMinute,
    greeksSurfaceBySymbolMinute,
  };
}

function buildGreeksLookupFromRawRows(symbol, greeksRows = []) {
  const state = createGreeksLookupState();
  accumulateGreeksLookupRows(symbol, greeksRows, state);
  return finalizeGreeksLookupState(symbol, state);
}

function loadClickHouseStockFeatures(symbol, dayIso, env = process.env) {
  const rows = loadClickHouseStockRawRowsForDay({ symbol, dayIso, env })
    .map((row) => ({
      minuteBucketUtc: row.minuteBucketUtc,
      open: toFiniteNumber(row.open),
      high: toFiniteNumber(row.high),
      low: toFiniteNumber(row.low),
      close: toFiniteNumber(row.close),
      volume: toFiniteNumber(row.volume),
    }))
    .filter((row) => row.minuteBucketUtc && row.close !== null);

  if (rows.length === 0) {
    return {
      latestSpot: null,
      stockByMinute: new Map(),
    };
  }

  const stockByMinute = buildStockFeaturesByMinute(rows);
  const latestSpot = rows[rows.length - 1].close;
  return { latestSpot, stockByMinute };
}

function upsertClickHouseDayCache({
  symbol,
  dayIso,
  cacheStatus,
  rowCount = 0,
  lastError = null,
  sourceEndpoint = null,
  env = process.env,
}) {
  insertClickHouseRows(
    'option_trade_day_cache',
    ['symbol', 'trade_date_utc', 'cache_status', 'row_count', 'last_sync_at_utc', 'last_error', 'source_endpoint', 'raw_file_path'],
    [{
      symbol,
      trade_date_utc: dayIso,
      cache_status: cacheStatus,
      row_count: Math.max(0, Math.trunc(Number(rowCount) || 0)),
      last_sync_at_utc: new Date().toISOString(),
      last_error: lastError,
      source_endpoint: sourceEndpoint,
      raw_file_path: null,
    }],
    env,
  );
}

function upsertClickHouseMetricCacheRows({
  symbol,
  dayIso,
  rows,
  metricStatuses,
  markPartial,
  env = process.env,
}) {
  const payload = METRIC_NAMES.map((metricName) => ({
    symbol,
    trade_date_utc: dayIso,
    metric_name: metricName,
    cache_status: markPartial ? DAY_CACHE_STATUS_PARTIAL : (metricStatuses[metricName] || DAY_CACHE_STATUS_PARTIAL),
    row_count: Math.max(0, Math.trunc(rows.length || 0)),
    last_sync_at_utc: new Date().toISOString(),
    last_error: null,
  }));

  insertClickHouseRows(
    'option_trade_metric_day_cache',
    ['symbol', 'trade_date_utc', 'metric_name', 'cache_status', 'row_count', 'last_sync_at_utc', 'last_error'],
    payload,
    env,
  );
}

function resolveActiveRuleConfigFromClickHouse(thresholds, env = process.env, queryRows = queryClickHouseRowsSync) {
  const activeRule = loadClickHouseRuleVersionRows(env, queryRows).find((row) => Number(row.isActive || 0) === 1) || null;
  if (!activeRule) {
    return resolveActiveRuleConfig({
      prepare() {
        throw new Error('no_active_rule');
      },
    }, thresholds, env);
  }

  return resolveActiveRuleConfig({
    prepare() {
      return {
        get() {
          return {
            versionId: activeRule.versionId,
            configJson: activeRule.configJson,
            checksum: activeRule.checksum,
          };
        },
      };
    },
  }, thresholds, env);
}

function upsertClickHouseFeatureBaselines(symbol, baselineMap, updates, env = process.env) {
  if (!(updates instanceof Map) || updates.size === 0) return;

  const rows = [];
  updates.forEach((update) => {
    const key = `${update.minuteOfDayEt}|${update.featureName}`;
    let state = baselineMap.get(key) || { sampleCount: 0, mean: 0, m2: 0 };
    update.values.forEach((sample) => {
      state = mergeFeatureBaselineState(state, sample);
    });
    baselineMap.set(key, state);
    rows.push({
      symbol,
      minute_of_day_et: update.minuteOfDayEt,
      feature_name: update.featureName,
      sample_count: state.sampleCount,
      mean: state.mean,
      m2: state.m2,
      updated_at_utc: new Date().toISOString(),
    });
  });

  insertClickHouseRows(
    'feature_baseline_intraday',
    ['symbol', 'minute_of_day_et', 'feature_name', 'sample_count', 'mean', 'm2', 'updated_at_utc'],
    rows,
    env,
  );
}

function replaceClickHouseDayRows({
  tableName,
  whereSql,
  deleteParams,
  columns,
  rows,
  env = process.env,
  chunkSize = undefined,
  skipDelete = false,
}) {
  if (!skipDelete) {
    deleteClickHouseScope(tableName, whereSql, deleteParams, env);
  }
  if (!Array.isArray(rows) || rows.length === 0) return 0;
  return insertClickHouseRows(tableName, columns, rows, env, chunkSize ? { chunkSize } : {});
}

async function syncThetaTradesToClickHouse({
  symbol,
  dayIso,
  env = process.env,
  markPartial = false,
}) {
  const resumeCursor = getClickHouseTradeResumeCursor({ symbol, dayIso, env });
  const sessionWindow = await resolveThetaCalendarSessionWindowForDay(dayIso, { env });
  if (sessionWindow && sessionWindow.isOpen === false) {
    const cacheStatus = markPartial ? DAY_CACHE_STATUS_PARTIAL : DAY_CACHE_STATUS_FULL;
    upsertClickHouseDayCache({
      symbol,
      dayIso,
      cacheStatus,
      rowCount: 0,
      lastError: null,
      sourceEndpoint: resolveThetaCalendarEndpoint(dayIso, env),
      env,
    });
    upsertClickHouseDownloadChunkStatusForStream({
      symbol,
      dayIso,
      streamName: DOWNLOAD_CHUNK_STREAMS.TRADE_QUOTE_1M,
      sourceEndpoint: resolveThetaCalendarEndpoint(dayIso, env),
      env,
    });
    return {
      synced: true,
      reason: 'no_data',
      fetchedRows: 0,
      upsertedRows: 0,
      cachedRows: 0,
      cacheStatus,
    };
  }

  const yyyymmdd = toYyyymmdd(`${dayIso}T00:00:00.000Z`);
  const requestWindows = resolveThetaTimeWindowsForSymbol(symbol, {
    startTime: resumeCursor?.startTime || null,
    sessionStartTime: sessionWindow?.openTime || null,
    sessionEndTime: sessionWindow?.closeTime || null,
    env,
  });
  const endpoints = requestWindows
    .map((window) => resolveThetaEndpoint(
      symbol,
      yyyymmdd,
      env,
      {
        startTime: window.startTime || null,
        endTime: window.endTime || null,
      },
    ))
    .filter(Boolean);
  if (endpoints.length === 0) {
    return {
      synced: false,
      reason: 'thetadata_base_url_missing',
      fetchedRows: 0,
      upsertedRows: 0,
      cachedRows: 0,
      cacheStatus: null,
    };
  }
  const endpointFormat = parseThetaUrlFormat(endpoints[0]);
  let parsedRows = 0;
  let fetchedRows = 0;
  let upsertedRows = 0;
  let sawHttpOk = false;
  let sawNoData = false;
  const columns = ['trade_id', 'trade_ts_utc', 'trade_ts_et', 'symbol', 'expiration', 'strike', 'option_right', 'price', 'size', 'bid', 'ask', 'condition_code', 'exchange', 'raw_payload_json', 'watermark', 'ingested_at_utc'];
  const nowIso = new Date().toISOString();
  let dayScopeCleared = false;
  const needsInitialDayDelete = resumeCursor?.resumeFromIso
    ? true
    : countClickHouseTradesForDay({ symbol, dayIso, env }) > 0;
  const clearDayScope = () => {
    if (dayScopeCleared) return;
    if (resumeCursor?.resumeFromIso) {
      const resumeUntilIso = addMinutesToIso(resumeCursor.resumeFromIso, 1);
      if (resumeUntilIso) {
        deleteClickHouseScope(
          'option_trades',
          'symbol = {symbol:String} AND trade_ts_utc >= parseDateTime64BestEffortOrNull({resumeFromIso:String}, 3, \'UTC\') AND trade_ts_utc < parseDateTime64BestEffortOrNull({resumeUntilIso:String}, 3, \'UTC\')',
          { symbol, resumeFromIso: resumeCursor.resumeFromIso, resumeUntilIso },
          env,
        );
        dayScopeCleared = true;
        return;
      }
      deleteClickHouseScope(
        'option_trades',
        'symbol = {symbol:String} AND trade_ts_utc >= parseDateTime64BestEffortOrNull({resumeFromIso:String}, 3, \'UTC\')',
        { symbol, resumeFromIso: resumeCursor.resumeFromIso },
        env,
      );
    } else {
      if (needsInitialDayDelete) {
        deleteClickHouseScope(
          'option_trades',
          'symbol = {symbol:String} AND trade_date = toDate({dayIso:String})',
          { symbol, dayIso },
          env,
        );
      }
    }
    dayScopeCleared = true;
  };

  if (endpointFormat === 'ndjson') {
    for (const endpoint of endpoints) {
      const fetchedBeforeEndpoint = fetchedRows;
      const heartbeat = createThetaStreamHeartbeatLogger({
        env,
        endpoint,
        stage: 'trade_sync_clickhouse',
        symbol,
        dayIso,
      });
      let chunk = [];
      const flushChunk = () => {
        if (chunk.length === 0) return;
        clearDayScope();
        upsertedRows += insertClickHouseRows('option_trades', columns, chunk, env);
        fetchedRows += chunk.length;
        chunk = [];
      };
      const streamResult = await fetchThetaNdjsonRows(endpoint, {
        env,
        onRow: (rawRow) => {
          parsedRows += 1;
          const normalized = normalizeThetaRow(rawRow, symbol, dayIso);
          if (normalized) {
            chunk.push({
              trade_id: normalized.tradeId,
              trade_ts_utc: normalized.tradeTsUtc,
              trade_ts_et: normalized.tradeTsEt,
              symbol: normalized.symbol,
              expiration: normalized.expiration,
              strike: normalized.strike,
              option_right: normalized.optionRight,
              price: normalized.price,
              size: normalized.size,
              bid: normalized.bid,
              ask: normalized.ask,
              condition_code: normalized.conditionCode,
              exchange: normalized.exchange,
              raw_payload_json: normalized.rawPayloadJson,
              watermark: normalized.watermark,
              ingested_at_utc: nowIso,
            });
            if (chunk.length >= 5000) flushChunk();
          }
          if (heartbeat) {
            heartbeat({
              parsedRows,
              fetchedRows: fetchedRows + chunk.length,
              insertedRows: upsertedRows,
              bufferedRows: chunk.length,
            });
          }
        },
      });
      const { response, durationMs } = streamResult;
      if (!response.ok) {
        const isThetaNoData = response.status === 472;
        logThetaDownload({
          env,
          url: endpoint,
          durationMs,
          status: response.status,
          ok: isThetaNoData,
          rows: 0,
          error: isThetaNoData ? 'no_data' : `http_${response.status}`,
        });
        if (isThetaNoData) {
          sawNoData = true;
          continue;
        }
        throw new Error(`thetadata_request_failed:${response.status}`);
      }
      sawHttpOk = true;
      flushChunk();
      logThetaDownload({
        env,
        url: endpoint,
        durationMs,
        status: response.status,
        ok: true,
        rows: fetchedRows - fetchedBeforeEndpoint,
        error: null,
      });
    }
  } else {
    for (const endpoint of endpoints) {
      const textResult = await fetchTextWithTimeout(endpoint, { env });
      const { response, durationMs } = textResult;
      if (!response.ok) {
        const isThetaNoData = response.status === 472;
        logThetaDownload({
          env,
          url: endpoint,
          durationMs,
          status: response.status,
          ok: isThetaNoData,
          rows: 0,
          error: isThetaNoData ? 'no_data' : `http_${response.status}`,
        });
        if (isThetaNoData) {
          sawNoData = true;
          continue;
        }
        throw new Error(`thetadata_request_failed:${response.status}`);
      }
      sawHttpOk = true;
      const parsed = parseJsonRows(textResult.body);
      const normalizedRows = normalizeThetaRows(parsed, symbol, dayIso);
      fetchedRows += normalizedRows.length;
      if (normalizedRows.length > 0) {
        const mappedRows = normalizedRows.map((row) => ({
          trade_id: row.tradeId,
          trade_ts_utc: row.tradeTsUtc,
          trade_ts_et: row.tradeTsEt,
          symbol: row.symbol,
          expiration: row.expiration,
          strike: row.strike,
          option_right: row.optionRight,
          price: row.price,
          size: row.size,
          bid: row.bid,
          ask: row.ask,
          condition_code: row.conditionCode,
          exchange: row.exchange,
          raw_payload_json: row.rawPayloadJson,
          watermark: row.watermark,
          ingested_at_utc: new Date().toISOString(),
        }));
        clearDayScope();
        upsertedRows += insertClickHouseRows(
          'option_trades',
          columns,
          mappedRows,
          env,
        );
      }
      logThetaDownload({
        env,
        url: endpoint,
        durationMs,
        status: response.status,
        ok: true,
        rows: normalizedRows.length,
        error: null,
      });
    }
  }

  const rowCount = countClickHouseTradesForDay({ symbol, dayIso, env });
  if (!sawHttpOk && sawNoData && rowCount === 0) {
    const cacheStatus = markPartial ? DAY_CACHE_STATUS_PARTIAL : DAY_CACHE_STATUS_FULL;
    upsertClickHouseDayCache({
      symbol,
      dayIso,
      cacheStatus,
      rowCount: 0,
      lastError: null,
      sourceEndpoint: endpoints[0] || null,
      env,
    });
    upsertClickHouseDownloadChunkStatusForStream({
      symbol,
      dayIso,
      streamName: DOWNLOAD_CHUNK_STREAMS.TRADE_QUOTE_1M,
      sourceEndpoint: endpoints[0] || null,
      env,
    });
    return {
      synced: true,
      reason: 'no_data',
      fetchedRows: 0,
      upsertedRows: 0,
      cachedRows: 0,
      cacheStatus,
    };
  }

  const cacheStatus = markPartial || rowCount === 0
    ? DAY_CACHE_STATUS_PARTIAL
    : DAY_CACHE_STATUS_FULL;

  upsertClickHouseDayCache({
    symbol,
    dayIso,
    cacheStatus,
    rowCount,
    lastError: rowCount === 0 ? 'empty_response' : null,
    sourceEndpoint: endpoints[0] || null,
    env,
  });
  upsertClickHouseDownloadChunkStatusForStream({
    symbol,
    dayIso,
    streamName: DOWNLOAD_CHUNK_STREAMS.TRADE_QUOTE_1M,
    sourceEndpoint: endpoints[0] || null,
    env,
  });

  return {
    synced: true,
    reason: null,
    fetchedRows,
    upsertedRows,
    cachedRows: rowCount,
    cacheStatus,
  };
}

async function ensureClickHouseStockRawForDay(symbol, dayIso, env = process.env, cacheStats = null) {
  const forceRawTargets = parseClickHouseRawHydrationTargets(env);
  const forceStockRefresh = (
    String(env.BACKFILL_FORCE || '').trim().toLowerCase() === '1'
    || String(env.BACKFILL_FORCE || '').trim().toLowerCase() === 'true'
  ) && forceRawTargets.includeStock;
  const fromRaw = forceStockRefresh
    ? { latestSpot: null, stockByMinute: new Map() }
    : loadClickHouseStockFeatures(symbol, dayIso, env);
  if (!forceStockRefresh && (fromRaw.stockByMinute.size > 0 || fromRaw.latestSpot !== null)) {
    if (cacheStats) {
      cacheStats.stockHit += 1;
      if (fromRaw.latestSpot !== null) cacheStats.spotHit += 1;
    }
    upsertClickHouseDownloadChunkStatusForStream({
      symbol,
      dayIso,
      streamName: DOWNLOAD_CHUNK_STREAMS.STOCK_PRICE_1M,
      sourceEndpoint: null,
      env,
    });
    return fromRaw;
  }

  const sessionWindow = await resolveThetaCalendarSessionWindowForDay(dayIso, { env });
  if (sessionWindow && sessionWindow.isOpen === false) {
    upsertClickHouseDownloadChunkStatusForStream({
      symbol,
      dayIso,
      streamName: DOWNLOAD_CHUNK_STREAMS.STOCK_PRICE_1M,
      sourceEndpoint: resolveThetaCalendarEndpoint(dayIso, env),
      env,
    });
    return fromRaw;
  }

  const endpoint = resolveThetaSpotEndpoint(
    symbol,
    dayIso,
    env,
    {
      startTime: sessionWindow?.openTime || null,
      endTime: sessionWindow?.closeTime || null,
    },
  );
  if (!endpoint) return fromRaw;

  if (cacheStats) cacheStats.stockMiss += 1;
  const fetchStartedAtMs = Date.now();
  const stockRows = await fetchThetaRows(endpoint, { env });
  const fetchDurationMs = Math.max(0, Date.now() - fetchStartedAtMs);
  const normalizedBars = normalizeStockOhlcRows(Array.isArray(stockRows) ? stockRows : [], dayIso)
    .map((row, index) => ({
      ...row,
      rawPayload: Array.isArray(stockRows) ? stockRows[index] : row,
    }));

  const insertOnlyUpsert = shouldUseInsertOnlyStockQuoteUpserts(env);
  let insertDurationMs = 0;
  if (normalizedBars.length > 0) {
    const insertStartedAtMs = Date.now();
    replaceClickHouseDayRows({
      tableName: 'stock_ohlc_minute_raw',
      whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
      deleteParams: { symbol, dayIso },
      columns: ['symbol', 'trade_date_utc', 'minute_bucket_utc', 'open', 'high', 'low', 'close', 'volume', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
      rows: normalizedBars.map((row) => ({
        symbol,
        trade_date_utc: dayIso,
        minute_bucket_utc: row.minuteBucketUtc,
        open: row.open,
        high: row.high,
        low: row.low,
        close: row.close,
        volume: row.volume,
        source_endpoint: endpoint,
        raw_payload_json: JSON.stringify(row.rawPayload || row),
        ingested_at_utc: new Date().toISOString(),
      })),
      env,
      skipDelete: insertOnlyUpsert,
    });
    insertDurationMs = Math.max(0, Date.now() - insertStartedAtMs);
  }

  const reloadStartedAtMs = Date.now();
  const reloaded = loadClickHouseStockFeatures(symbol, dayIso, env);
  const reloadDurationMs = Math.max(0, Date.now() - reloadStartedAtMs);
  if (reloaded.latestSpot === null && Array.isArray(stockRows) && stockRows.length > 0) {
    reloaded.latestSpot = extractMetricFromResponse(JSON.stringify(stockRows), [
      'spot',
      'underlying_price',
      'underlyingPrice',
      'price',
      'last',
      'close',
      'open',
      'high',
      'low',
      'mark',
      'mid',
    ]);
  }
  console.log('[STOCK_SYNC_STATS]', JSON.stringify({
    symbol,
    dayIso,
    fetchedRows: Array.isArray(stockRows) ? stockRows.length : 0,
    insertedRows: normalizedBars.length,
    fetchDurationMs,
    insertDurationMs,
    reloadDurationMs,
    insertOnlyUpsert,
  }));

  if (cacheStats) {
    if (reloaded.latestSpot !== null) cacheStats.spotHit += 1;
    else cacheStats.spotMiss += 1;
  }
  upsertClickHouseDownloadChunkStatusForStream({
    symbol,
    dayIso,
    streamName: DOWNLOAD_CHUNK_STREAMS.STOCK_PRICE_1M,
    sourceEndpoint: endpoint,
    env,
  });

  return reloaded;
}

async function ensureClickHouseOptionQuoteRawForDay(symbol, dayIso, env = process.env) {
  const forceRawTargets = parseClickHouseRawHydrationTargets(env);
  const forceRequested = (
    String(env.BACKFILL_FORCE || '').trim().toLowerCase() === '1'
    || String(env.BACKFILL_FORCE || '').trim().toLowerCase() === 'true'
  );
  // Default to minute-scope resume even in force mode; full-day quote rewrites are opt-in.
  const forceQuoteFullRefresh = (
    String(env.BACKFILL_FORCE_QUOTE_FULL || '0').trim().toLowerCase() !== '0'
    && String(env.BACKFILL_FORCE_QUOTE_FULL || '0').trim().toLowerCase() !== 'false'
  );
  const forceQuoteRefresh = forceRequested && forceRawTargets.includeQuote && forceQuoteFullRefresh;
  const resumeCursor = forceQuoteRefresh ? null : getClickHouseOptionQuoteResumeCursor({ symbol, dayIso, env });
  const sessionWindow = await resolveThetaCalendarSessionWindowForDay(dayIso, { env });
  if (sessionWindow && sessionWindow.isOpen === false) {
    upsertClickHouseDownloadChunkStatusForStream({
      symbol,
      dayIso,
      streamName: DOWNLOAD_CHUNK_STREAMS.OPTION_QUOTE_1M,
      sourceEndpoint: resolveThetaCalendarEndpoint(dayIso, env),
      env,
    });
    return true;
  }
  let requestWindows = [];
  let useGapWindows = false;
  if (!forceQuoteRefresh && shouldEnableBackfillQuoteGapFill(env)) {
    const existingMinuteRows = loadClickHouseOptionQuoteMinuteCountRowsForDay({ symbol, dayIso, env });
    const gapPlan = buildQuoteGapFillWindowsForDay({
      dayIso,
      sessionWindow,
      existingMinuteRows,
    });
    if (gapPlan.expectedMinutes > 0 && gapPlan.missingMinutes === 0) {
      upsertClickHouseDownloadChunkStatusForStream({
        symbol,
        dayIso,
        streamName: DOWNLOAD_CHUNK_STREAMS.OPTION_QUOTE_1M,
        sourceEndpoint: resolveThetaCalendarEndpoint(dayIso, env),
        env,
      });
      return true;
    }
    const maxGapWindows = parseBackfillQuoteGapMaxWindows(env);
    if (gapPlan.windows.length > 0 && gapPlan.windows.length <= maxGapWindows) {
      requestWindows = gapPlan.windows;
      useGapWindows = true;
      console.log('[QUOTE_GAP_FILL]', JSON.stringify({
        symbol,
        dayIso,
        expectedMinutes: gapPlan.expectedMinutes,
        missingMinutes: gapPlan.missingMinutes,
        missingFraction: Number(gapPlan.missingFraction.toFixed(4)),
        windows: gapPlan.windows.length,
      }));
    }
  }

  if (requestWindows.length === 0) {
    requestWindows = resolveThetaTimeWindowsForSymbol(symbol, {
      startTime: resumeCursor?.startTime || null,
      sessionStartTime: sessionWindow?.openTime || null,
      sessionEndTime: sessionWindow?.closeTime || null,
      env,
    });
  }
  const endpoints = requestWindows
    .map((window) => resolveThetaOptionQuoteEndpoint(
      symbol,
      dayIso,
      env,
      {
        startTime: window.startTime || null,
        endTime: window.endTime || null,
      },
    ))
    .filter(Boolean);
  if (endpoints.length === 0) {
    const existingRows = countClickHouseOptionQuoteRowsForDay({ symbol, dayIso, env });
    if (existingRows === 0) {
      console.log('[QUOTE_SYNC_SKIPPED_NO_ENDPOINT]', JSON.stringify({
        symbol,
        dayIso,
        requestWindows: requestWindows.length,
        hasBaseUrl: Boolean((env.THETADATA_BASE_URL || '').trim()),
        quotePath: (env.THETADATA_OPTION_QUOTE_PATH || DEFAULT_OPTION_QUOTE_PATH || '').trim() || null,
      }));
    }
    return existingRows > 0;
  }
  const defaultSourceEndpoint = endpoints[0] || null;
  const refreshDownloadChunkStatus = (sourceEndpoint = defaultSourceEndpoint, lastError = null) => {
    upsertClickHouseDownloadChunkStatusForStream({
      symbol,
      dayIso,
      streamName: DOWNLOAD_CHUNK_STREAMS.OPTION_QUOTE_1M,
      sourceEndpoint,
      lastError,
      env,
    });
  };
  const columns = [
    'symbol',
    'trade_date_utc',
    'expiration',
    'strike',
    'option_right',
    'minute_bucket_utc',
    'bid',
    'ask',
    'last',
    'bid_size',
    'ask_size',
    'source_endpoint',
    'raw_payload_json',
    'ingested_at_utc',
  ];
  const chunkSize = parseClickHouseQuoteStreamChunkSize(env);
  const insertOnlyUpsert = shouldUseInsertOnlyStockQuoteUpserts(env);
  const includeRawPayload = shouldIncludeClickHouseQuoteRawPayload(env);
  const nowIso = new Date().toISOString();
  const endpointFormat = parseThetaUrlFormat(endpoints[0]);
  const startedAtMs = Date.now();
  let insertedRows = 0;
  let parsedRows = 0;
  let insertDurationMs = 0;
  let streamDownloadDurationMs = 0;
  let chunkFlushes = 0;

  let chunk = [];
  let dayScopeCleared = false;
  const needsInitialDayDelete = insertOnlyUpsert
    ? false
    : useGapWindows
    ? false
    : resumeCursor?.resumeFromIso
    ? true
    : countClickHouseOptionQuoteRowsForDay({ symbol, dayIso, env }) > 0;
  const flushChunk = () => {
    if (chunk.length === 0) return;
    if (!dayScopeCleared) {
      if (!insertOnlyUpsert) {
        if (resumeCursor?.resumeFromIso) {
          const resumeUntilIso = addMinutesToIso(resumeCursor.resumeFromIso, 1);
          if (resumeUntilIso) {
            deleteClickHouseScope(
              'option_quote_minute_raw',
              'symbol = {symbol:String} AND minute_bucket_utc >= parseDateTime64BestEffortOrNull({resumeFromIso:String}, 3, \'UTC\') AND minute_bucket_utc < parseDateTime64BestEffortOrNull({resumeUntilIso:String}, 3, \'UTC\')',
              { symbol, resumeFromIso: resumeCursor.resumeFromIso, resumeUntilIso },
              env,
            );
          } else {
            deleteClickHouseScope(
              'option_quote_minute_raw',
              'symbol = {symbol:String} AND minute_bucket_utc >= parseDateTime64BestEffortOrNull({resumeFromIso:String}, 3, \'UTC\')',
              { symbol, resumeFromIso: resumeCursor.resumeFromIso },
              env,
            );
          }
        } else if (needsInitialDayDelete) {
          deleteClickHouseScope(
            'option_quote_minute_raw',
            'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
            { symbol, dayIso },
            env,
          );
        }
      }
      dayScopeCleared = true;
    }
    const insertStartedAtMs = Date.now();
    insertedRows += insertClickHouseRows('option_quote_minute_raw', columns, chunk, env, { chunkSize });
    insertDurationMs += Math.max(0, Date.now() - insertStartedAtMs);
    chunkFlushes += 1;
    chunk = [];
  };

  if (endpointFormat === 'ndjson') {
    for (const endpoint of endpoints) {
      const parsedBeforeEndpoint = parsedRows;
      const heartbeat = createThetaStreamHeartbeatLogger({
        env,
        endpoint,
        stage: 'quote_sync_clickhouse',
        symbol,
        dayIso,
      });
      const streamResult = await fetchThetaNdjsonRows(endpoint, {
        env,
        timeoutMs: parseOptionQuoteTimeoutMs(env),
        onRow: (rawRow) => {
          parsedRows += 1;
          const normalized = normalizeOptionQuoteRow(rawRow, dayIso, { includeRawPayload });
          if (normalized) {
            chunk.push({
              symbol: normalized.symbol,
              trade_date_utc: dayIso,
              expiration: normalized.expiration,
              strike: normalized.strike,
              option_right: normalized.right,
              minute_bucket_utc: normalized.minuteBucketUtc,
              bid: normalized.bid,
              ask: normalized.ask,
              last: normalized.last,
              bid_size: normalized.bidSize,
              ask_size: normalized.askSize,
              source_endpoint: endpoint,
              raw_payload_json: normalized.rawPayloadJson,
              ingested_at_utc: nowIso,
            });
            if (chunk.length >= chunkSize) flushChunk();
          }
          if (heartbeat) {
            heartbeat({
              parsedRows,
              fetchedRows: parsedRows,
              insertedRows,
              bufferedRows: chunk.length,
            });
          }
        },
      });
      const { response, durationMs } = streamResult;
      if (!response.ok) {
        const isThetaNoData = response.status === 472;
        logThetaDownload({
          env,
          url: endpoint,
          durationMs,
          status: response.status,
          ok: isThetaNoData,
          rows: 0,
          error: isThetaNoData ? 'no_data' : `http_${response.status}`,
        });
        if (isThetaNoData) continue;
        refreshDownloadChunkStatus(endpoint, `http_${response.status}`);
        return false;
      }
      if (Number.isFinite(durationMs)) {
        streamDownloadDurationMs += Math.max(0, Math.trunc(durationMs));
      }
      flushChunk();
      logThetaDownload({
        env,
        url: endpoint,
        durationMs,
        status: response.status,
        ok: true,
        rows: parsedRows - parsedBeforeEndpoint,
        error: null,
      });
    }
  } else {
    for (const endpoint of endpoints) {
      const { response, body, durationMs } = await fetchTextWithTimeout(endpoint, {
        env,
        timeoutMs: parseOptionQuoteTimeoutMs(env),
      });
      if (!response.ok) {
        const isThetaNoData = response.status === 472;
        logThetaDownload({
          env,
          url: endpoint,
          durationMs,
          status: response.status,
          ok: isThetaNoData,
          rows: 0,
          error: isThetaNoData ? 'no_data' : `http_${response.status}`,
        });
        if (isThetaNoData) continue;
        refreshDownloadChunkStatus(endpoint, `http_${response.status}`);
        return false;
      }
      if (Number.isFinite(durationMs)) {
        streamDownloadDurationMs += Math.max(0, Math.trunc(durationMs));
      }

      const rows = parseJsonRows(body);
      parsedRows += rows.length;
      for (const rawRow of rows) {
        const normalized = normalizeOptionQuoteRow(rawRow, dayIso, { includeRawPayload });
        if (!normalized) continue;
        chunk.push({
          symbol: normalized.symbol,
          trade_date_utc: dayIso,
          expiration: normalized.expiration,
          strike: normalized.strike,
          option_right: normalized.right,
          minute_bucket_utc: normalized.minuteBucketUtc,
          bid: normalized.bid,
          ask: normalized.ask,
          last: normalized.last,
          bid_size: normalized.bidSize,
          ask_size: normalized.askSize,
          source_endpoint: endpoint,
          raw_payload_json: normalized.rawPayloadJson,
          ingested_at_utc: nowIso,
        });
        if (chunk.length >= chunkSize) {
          flushChunk();
        }
      }
      logThetaDownload({
        env,
        url: endpoint,
        durationMs,
        status: response.status,
        ok: true,
        rows: rows.length,
        error: null,
      });
    }
    flushChunk();
  }
  refreshDownloadChunkStatus(defaultSourceEndpoint, null);
  const wallDurationMs = Math.max(0, Date.now() - startedAtMs);
  console.log('[QUOTE_SYNC_STATS]', JSON.stringify({
    symbol,
    dayIso,
    endpoints: endpoints.length,
    endpointFormat,
    parsedRows,
    insertedRows,
    chunkFlushes,
    streamDownloadDurationMs,
    insertDurationMs,
    wallDurationMs,
    insertOnlyUpsert,
    includeRawPayload,
    chunkSize,
  }));
  if (insertedRows > 0) return true;
  return countClickHouseOptionQuoteRowsForDay({ symbol, dayIso, env }) > 0;
}

async function ensureClickHouseOiRawForDay(symbol, dayIso, rawRows, env = process.env, cacheStats = null) {
  const oiByContract = loadClickHouseContractOiFromRaw({ symbol, dayIso, env });
  let oiDefaultsToZero = false;
  if (oiByContract.size > 0) {
    if (cacheStats) cacheStats.oiHit += oiByContract.size;
    return { oiByContract, oiDefaultsToZero };
  }

  const shouldFetchOi = Boolean((env.THETADATA_OI_PATH || DEFAULT_OI_PATH || '').trim());
  if (!shouldFetchOi) return { oiByContract, oiDefaultsToZero };

  const bulkEndpoint = resolveThetaOiBulkEndpoint(symbol, dayIso, env);
  let bulkRows = [];
  if (bulkEndpoint) {
    try {
      const { response, body, durationMs } = await fetchTextWithTimeout(bulkEndpoint, { env });
      if (response.ok) {
        oiDefaultsToZero = true;
        bulkRows = normalizeOptionOpenInterestRows(parseJsonRows(body));
        logThetaDownload({
          env,
          url: bulkEndpoint,
          durationMs,
          status: response.status,
          ok: true,
          rows: bulkRows.length,
          error: null,
        });
      } else {
        logThetaDownload({
          env,
          url: bulkEndpoint,
          durationMs,
          status: response.status,
          ok: false,
          rows: 0,
          error: `http_${response.status}`,
        });
      }
    } catch (error) {
      logThetaDownload({
        env,
        url: bulkEndpoint,
        durationMs: null,
        status: 0,
        ok: false,
        rows: 0,
        error: error.message || 'request_failed',
      });
    }
  }

  if (bulkRows.length > 0) {
    replaceClickHouseDayRows({
      tableName: 'option_open_interest_raw',
      whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
      deleteParams: { symbol, dayIso },
      columns: ['symbol', 'trade_date_utc', 'expiration', 'strike', 'option_right', 'oi', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
      rows: bulkRows.map((row) => ({
        symbol: row.symbol,
        trade_date_utc: dayIso,
        expiration: row.expiration,
        strike: row.strike,
        option_right: row.right,
        oi: Math.max(0, Math.trunc(row.oi || 0)),
        source_endpoint: bulkEndpoint,
        raw_payload_json: row.rawPayloadJson,
        ingested_at_utc: new Date().toISOString(),
      })),
      env,
    });

    bulkRows.forEach((row) => {
      oiByContract.set(buildContractKey(row), Math.max(0, Math.trunc(row.oi || 0)));
    });
  }

  if (oiByContract.size === 0 && Array.isArray(rawRows) && rawRows.length > 0) {
    const missingContracts = [];
    const seenContracts = new Set();
    rawRows.forEach((row) => {
      const contractKey = buildContractKey(row);
      if (seenContracts.has(contractKey)) return;
      seenContracts.add(contractKey);
      if (!oiByContract.has(contractKey)) {
        missingContracts.push(row);
      }
    });

    const concurrency = parseSupplementalConcurrency(env);
    const fallbackRows = [];
    await parallelMapLimit(missingContracts, concurrency, async (row) => {
      const oiEndpoint = resolveThetaOiEndpoint(row, dayIso, env);
      if (!oiEndpoint) return;
      const oiRows = await fetchThetaRows(oiEndpoint, { env });
      const normalizedRows = normalizeOptionOpenInterestRows(oiRows);
      if (normalizedRows.length > 0) {
        normalizedRows.forEach((entry) => {
          oiByContract.set(buildContractKey(entry), entry.oi);
          fallbackRows.push({
            symbol: entry.symbol,
            trade_date_utc: dayIso,
            expiration: entry.expiration,
            strike: entry.strike,
            option_right: entry.right,
            oi: Math.max(0, Math.trunc(entry.oi || 0)),
            source_endpoint: oiEndpoint,
            raw_payload_json: entry.rawPayloadJson,
            ingested_at_utc: new Date().toISOString(),
          });
        });
        return;
      }

      const fallbackOi = await fetchThetaMetricNumber(oiEndpoint, ['oi', 'open_interest', 'openInterest']);
      if (fallbackOi === null) return;
      const oiValue = Math.max(0, Math.trunc(fallbackOi));
      oiByContract.set(buildContractKey(row), oiValue);
      fallbackRows.push({
        symbol: row.symbol,
        trade_date_utc: dayIso,
        expiration: row.expiration,
        strike: row.strike,
        option_right: row.right,
        oi: oiValue,
        source_endpoint: oiEndpoint,
        raw_payload_json: JSON.stringify({
          source: 'scalar_oi_response',
          oi: oiValue,
        }),
        ingested_at_utc: new Date().toISOString(),
      });
    });

    if (fallbackRows.length > 0) {
      insertClickHouseRows(
        'option_open_interest_raw',
        ['symbol', 'trade_date_utc', 'expiration', 'strike', 'option_right', 'oi', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
        fallbackRows,
        env,
      );
    }
  }

  if (cacheStats) {
    if (oiByContract.size > 0) cacheStats.oiHit += oiByContract.size;
    else cacheStats.oiMiss += 1;
  }

  return { oiByContract, oiDefaultsToZero };
}

async function ensureClickHouseGreeksRawForDay(symbol, dayIso, rawRows, env = process.env, cacheStats = null) {
  if (countClickHouseOptionGreeksRowsForDay({ symbol, dayIso, env }) > 0) {
    if (cacheStats) cacheStats.greeksHit += 1;
    return true;
  }

  const expirations = Array.from(new Set((rawRows || []).map((row) => row.expiration).filter(Boolean)))
    .sort((left, right) => Date.parse(left) - Date.parse(right));
  if (expirations.length === 0) return false;

  if (cacheStats) cacheStats.greeksMiss += expirations.length;
  const concurrency = parseSupplementalConcurrency(env);
  const collectedRows = [];
  await parallelMapLimit(expirations, concurrency, async (expiration) => {
    const endpoint = resolveThetaGreeksEndpoint(symbol, expiration, dayIso, env);
    if (!endpoint) return;
    const rows = await fetchThetaRows(endpoint, { env });
    const normalized = normalizeOptionGreeksRows(rows, dayIso);
    normalized.forEach((row) => {
      collectedRows.push(row);
    });
  });

  if (collectedRows.length === 0) return false;

  const deduped = new Map();
  collectedRows.forEach((row) => {
    const key = `${row.symbol}|${row.expiration}|${row.strike}|${row.right}|${row.minuteBucketUtc}`;
    deduped.set(key, row);
  });

  replaceClickHouseDayRows({
    tableName: 'option_greeks_minute_raw',
    whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
    deleteParams: { symbol, dayIso },
    columns: ['symbol', 'trade_date_utc', 'expiration', 'strike', 'option_right', 'minute_bucket_utc', 'delta', 'implied_vol', 'gamma', 'theta', 'vega', 'rho', 'underlying_price', 'source_endpoint', 'raw_payload_json', 'ingested_at_utc'],
    rows: Array.from(deduped.values()).map((row) => ({
      symbol: row.symbol,
      trade_date_utc: dayIso,
      expiration: row.expiration,
      strike: row.strike,
      option_right: row.right,
      minute_bucket_utc: row.minuteBucketUtc,
      delta: row.delta,
      implied_vol: row.impliedVol,
      gamma: row.gamma,
      theta: row.theta,
      vega: row.vega,
      rho: row.rho,
      underlying_price: row.underlyingPrice,
      source_endpoint: null,
      raw_payload_json: row.rawPayloadJson,
      ingested_at_utc: new Date().toISOString(),
    })),
    env,
    chunkSize: 2500,
  });

  return true;
}

function parseClickHouseRawHydrationTargets(env = process.env) {
  const raw = String(env.BACKFILL_RAW_COMPONENTS || env.BACKFILL_RAW_TARGETS || '')
    .trim()
    .toLowerCase();
  if (!raw || raw === 'all') {
    return {
      includeStock: true,
      includeOi: true,
      includeQuote: true,
      includeGreeks: true,
    };
  }

  const tokens = new Set(
    raw
      .split(',')
      .map((token) => token.trim().toLowerCase())
      .filter(Boolean),
  );

  return {
    includeStock: tokens.has('stock'),
    includeOi: tokens.has('oi'),
    includeQuote: tokens.has('quote'),
    includeGreeks: tokens.has('greeks'),
  };
}

async function ensureRawHydratedForDayInClickHouse({
  symbol,
  dayIso,
  env = process.env,
  includeStock = true,
  includeOi = true,
  includeQuote = true,
  includeGreeks = true,
  tradeRowsHint = null,
}) {
  const hintedTradeRows = Number(tradeRowsHint);
  const tradeRows = Number.isFinite(hintedTradeRows) && hintedTradeRows >= 0
    ? Math.max(0, Math.trunc(hintedTradeRows))
    : countClickHouseTradesForDay({ symbol, dayIso, env });
  if (tradeRows === 0) {
    return {
      tradeRows: 0,
      stockRows: 0,
      oiRows: 0,
      quoteRows: 0,
      greeksRows: 0,
      supplementalCache: null,
    };
  }

  const needsContractLevelRawRows = includeOi || includeGreeks;
  const rawRows = needsContractLevelRawRows
    ? loadClickHouseRawTradesForDay({ symbol, dayIso, env })
    : [];

  const cacheStats = {
    spotHit: 0,
    spotMiss: 0,
    stockHit: 0,
    stockMiss: 0,
    oiHit: 0,
    oiMiss: 0,
    greeksHit: 0,
    greeksMiss: 0,
  };

  const hydrationTasks = [];
  if (includeStock) {
    hydrationTasks.push(ensureClickHouseStockRawForDay(symbol, dayIso, env, cacheStats));
  }
  if (includeOi && rawRows.length > 0) {
    hydrationTasks.push(ensureClickHouseOiRawForDay(symbol, dayIso, rawRows, env, cacheStats));
  }
  let quoteHydrationPromise = null;
  if (includeQuote) {
    quoteHydrationPromise = ensureClickHouseOptionQuoteRawForDay(symbol, dayIso, env);
    hydrationTasks.push(quoteHydrationPromise);
  }
  if (includeGreeks && rawRows.length > 0) {
    hydrationTasks.push(ensureClickHouseGreeksRawForDay(symbol, dayIso, rawRows, env, cacheStats));
  }

  await Promise.all(hydrationTasks);
  if (includeQuote && quoteHydrationPromise) {
    const quoteHydrated = await quoteHydrationPromise;
    if (!quoteHydrated) {
      throw new Error('quote_raw_hydration_failed');
    }
  }

  return {
    tradeRows,
    stockRows: includeStock ? countClickHouseStockRawRowsForDay({ symbol, dayIso, env }) : 0,
    oiRows: includeOi ? countClickHouseOptionOiRowsForDay({ symbol, dayIso, env }) : 0,
    quoteRows: includeQuote ? countClickHouseOptionQuoteRowsForDay({ symbol, dayIso, env }) : 0,
    greeksRows: includeGreeks ? countClickHouseOptionGreeksRowsForDay({ symbol, dayIso, env }) : 0,
    supplementalCache: cacheStats,
  };
}

function buildSupplementalMetricLookupFromClickHouse({
  symbol,
  dayIso,
  rawRows,
  env = process.env,
}) {
  const spotBySymbol = new Map();
  const stockBySymbolMinute = new Map();
  const oiByContract = new Map();
  const featureBaselineRows = loadClickHouseFeatureBaselineRows({ symbol, env });
  const featureBaselines = new Map();

  featureBaselineRows.forEach((row) => {
    const key = `${row.minuteOfDayEt}|${row.featureName}`;
    featureBaselines.set(key, {
      sampleCount: Math.max(0, Math.trunc(toFiniteNumber(row.sampleCount) || 0)),
      mean: toFiniteNumber(row.mean) || 0,
      m2: toFiniteNumber(row.m2) || 0,
    });
  });

  const stock = loadClickHouseStockFeatures(symbol, dayIso, env);
  stock.stockByMinute.forEach((features, minuteBucketUtc) => {
    stockBySymbolMinute.set(`${symbol}|${minuteBucketUtc}`, features);
  });
  if (stock.latestSpot !== null) {
    spotBySymbol.set(symbol, stock.latestSpot);
  }

  rawRows.forEach((row) => {
    const payloadSpot = toFiniteNumber(row.payloadSpot);
    if (payloadSpot !== null && !spotBySymbol.has(row.symbol)) {
      spotBySymbol.set(row.symbol, payloadSpot);
    }

    const payloadOi = toFiniteNumber(row.payloadOi);
    if (payloadOi !== null) {
      oiByContract.set(buildContractKey(row), Math.trunc(payloadOi));
      return;
    }

    if (typeof row.rawPayloadJson !== 'string' || row.rawPayloadJson === '{}' || !row.rawPayloadJson.trim()) {
      return;
    }

    const payload = parsePayload(row.rawPayloadJson);
    const fallbackOi = extractOi(payload);
    if (fallbackOi !== null) {
      oiByContract.set(buildContractKey(row), fallbackOi);
    }
  });

  loadClickHouseContractOiFromRaw({ symbol, dayIso, env }).forEach((value, key) => {
    oiByContract.set(key, value);
  });
  loadClickHouseContractOiFromStats({ symbol, dayIso, env }).forEach((value, key) => {
    if (!oiByContract.has(key)) {
      oiByContract.set(key, value);
    }
  });
  loadClickHouseReferenceOiMap({ symbol, dayIso, env }).forEach((value, key) => {
    if (!oiByContract.has(key)) {
      oiByContract.set(key, value);
    }
  });

  let greeksLookup = {
    greeksByContractMinute: new Map(),
    greeksSurfaceBySymbolMinute: new Map(),
  };
  if (shouldIncludeClickHouseGreeksInEnrichment(env)) {
    const greeksState = createGreeksLookupState();
    loadClickHouseOptionGreeksRawRowsForDay({
      symbol,
      dayIso,
      env,
      includeRawPayloadJson: false,
      onChunk: (chunkRows) => {
        accumulateGreeksLookupRows(symbol, chunkRows, greeksState);
      },
    });
    greeksLookup = finalizeGreeksLookupState(symbol, greeksState);
  }

  return {
    spotBySymbol,
    stockBySymbolMinute,
    trendFallbackMaxLagMinutes: parseTrendFallbackMaxLagMinutes(env),
    oiByContract,
    oiDefaultsToZero: false,
    greeksByContractMinute: greeksLookup.greeksByContractMinute,
    greeksSurfaceBySymbolMinute: greeksLookup.greeksSurfaceBySymbolMinute,
    featureBaselines,
    cacheStats: null,
  };
}

function hasClickHouseMinuteDerivedRows({ symbol, dayIso, env = process.env, queryRows = queryClickHouseRowsSync }) {
  const rows = queryRows(`
    SELECT
      (
        SELECT count()
        FROM options.option_symbol_minute_derived
        WHERE symbol = {symbol:String}
          AND trade_date_utc = toDate({dayIso:String})
      ) AS symbolCount,
      (
        SELECT count()
        FROM options.option_contract_minute_derived
        WHERE symbol = {symbol:String}
          AND trade_date_utc = toDate({dayIso:String})
      ) AS contractCount
  `, { symbol, dayIso }, env);

  const symbolCount = Number(rows?.[0]?.symbolCount || 0);
  const contractCount = Number(rows?.[0]?.contractCount || 0);
  return symbolCount > 0 && contractCount > 0;
}

function toClickHouseEnrichedInsertRow(row, nowIso) {
  return {
    trade_id: row.tradeId,
    trade_ts_utc: row.tradeTsUtc,
    symbol: row.symbol,
    expiration: row.expiration,
    strike: row.strike,
    option_right: row.right,
    price: row.price,
    size: Math.max(0, Math.trunc(toFiniteNumber(row.size) || 0)),
    bid: row.bid,
    ask: row.ask,
    condition_code: row.conditionCode,
    exchange: row.exchange,
    value: row.value,
    dte: row.dte,
    spot: row.spot,
    otm_pct: row.otmPct,
    day_volume: row.dayVolume,
    oi: row.oi,
    vol_oi_ratio: row.volOiRatio,
    repeat3m: row.repeat3m,
    sig_score: row.sigScore,
    sentiment: row.sentiment,
    execution_side: row.executionSide,
    symbol_vol_1m: row.symbolVol1m,
    symbol_vol_baseline_15m: row.symbolVolBaseline15m,
    open_window_baseline: row.openWindowBaseline,
    bullish_ratio_15m: row.bullishRatio15m,
    chips_json: JSON.stringify(row.chips || []),
    rule_version: row.ruleVersion || null,
    score_quality: row.scoreQuality || 'partial',
    missing_metrics_json: JSON.stringify(row.missingMetrics || []),
    enriched_at_utc: nowIso,
    is_sweep: Number.isFinite(Number(row.isSweep)) ? Math.trunc(Number(row.isSweep)) : 0,
    is_multileg: Number.isFinite(Number(row.isMultileg)) ? Math.trunc(Number(row.isMultileg)) : 0,
    minute_of_day_et: Number.isFinite(Number(row.minuteOfDayEt)) ? Math.trunc(Number(row.minuteOfDayEt)) : null,
    delta: row.delta,
    implied_vol: row.impliedVol,
    time_norm: row.timeNorm,
    delta_norm: row.deltaNorm,
    iv_skew_norm: row.ivSkewNorm,
    value_shock_norm: row.valueShockNorm,
    dte_swing_norm: row.dteSwingNorm,
    flow_imbalance_norm: row.flowImbalanceNorm,
    delta_pressure_norm: row.deltaPressureNorm,
    cp_oi_pressure_norm: row.cpOiPressureNorm,
    iv_skew_surface_norm: row.ivSkewSurfaceNorm,
    iv_term_slope_norm: row.ivTermSlopeNorm,
    underlying_trend_confirm_norm: row.underlyingTrendConfirmNorm,
    liquidity_quality_norm: row.liquidityQualityNorm,
    multileg_penalty_norm: row.multilegPenaltyNorm,
    sig_score_components_json: JSON.stringify(row.sigScoreComponents || {}),
  };
}

function persistClickHouseEnrichedDayState({
  symbol,
  dayIso,
  built,
  featureBaselines,
  env = process.env,
  skipEnrichedRowsReplace = false,
  minuteRollupsOverride = null,
}) {
  const nowIso = new Date().toISOString();
  const enrichedRows = Array.isArray(built?.rows) ? built.rows : [];

  if (!skipEnrichedRowsReplace) {
    replaceClickHouseDayRows({
      tableName: 'option_trade_enriched',
      whereSql: 'symbol = {symbol:String} AND trade_date = toDate({dayIso:String})',
      deleteParams: { symbol, dayIso },
      columns: ['trade_id', 'trade_ts_utc', 'symbol', 'expiration', 'strike', 'option_right', 'price', 'size', 'bid', 'ask', 'condition_code', 'exchange', 'value', 'dte', 'spot', 'otm_pct', 'day_volume', 'oi', 'vol_oi_ratio', 'repeat3m', 'sig_score', 'sentiment', 'execution_side', 'symbol_vol_1m', 'symbol_vol_baseline_15m', 'open_window_baseline', 'bullish_ratio_15m', 'chips_json', 'rule_version', 'score_quality', 'missing_metrics_json', 'enriched_at_utc', 'is_sweep', 'is_multileg', 'minute_of_day_et', 'delta', 'implied_vol', 'time_norm', 'delta_norm', 'iv_skew_norm', 'value_shock_norm', 'dte_swing_norm', 'flow_imbalance_norm', 'delta_pressure_norm', 'cp_oi_pressure_norm', 'iv_skew_surface_norm', 'iv_term_slope_norm', 'underlying_trend_confirm_norm', 'liquidity_quality_norm', 'multileg_penalty_norm', 'sig_score_components_json'],
      rows: enrichedRows.map((row) => toClickHouseEnrichedInsertRow(row, nowIso)),
      env,
    });
  }

  const minuteRollups = minuteRollupsOverride || buildMinuteDerivedRollups(enrichedRows, dayIso);

  replaceClickHouseDayRows({
    tableName: 'option_symbol_minute_derived',
    whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
    deleteParams: { symbol, dayIso },
    columns: ['symbol', 'trade_date_utc', 'minute_bucket_utc', 'trade_count', 'contract_count', 'total_size', 'total_value', 'call_size', 'put_size', 'bullish_count', 'bearish_count', 'neutral_count', 'avg_sig_score', 'max_sig_score', 'avg_vol_oi_ratio', 'max_vol_oi_ratio', 'max_repeat3m', 'oi_sum', 'day_volume_sum', 'chip_hits_json', 'updated_at_utc', 'spot', 'avg_sig_score_bullish', 'avg_sig_score_bearish', 'net_sig_score', 'value_weighted_sig_score', 'sweep_count', 'sweep_value_ratio', 'multileg_count', 'multileg_pct', 'avg_minute_of_day_et', 'avg_iv', 'call_iv_avg', 'put_iv_avg', 'iv_spread', 'net_delta_dollars', 'avg_value_pctile', 'avg_vol_oi_norm', 'avg_repeat_norm', 'avg_otm_norm', 'avg_side_confidence', 'avg_dte_norm', 'avg_spread_norm', 'avg_sweep_norm', 'avg_multileg_norm', 'avg_time_norm', 'avg_delta_norm', 'avg_iv_skew_norm', 'avg_value_shock_norm', 'avg_dte_swing_norm', 'avg_flow_imbalance_norm', 'avg_delta_pressure_norm', 'avg_cp_oi_pressure_norm', 'avg_iv_skew_surface_norm', 'avg_iv_term_slope_norm', 'avg_underlying_trend_confirm_norm', 'avg_liquidity_quality_norm', 'avg_multileg_penalty_norm'],
    rows: minuteRollups.symbolMinuteRows.map((row) => ({
      symbol: row.symbol,
      trade_date_utc: row.tradeDateUtc,
      minute_bucket_utc: row.minuteBucketUtc,
      trade_count: Math.max(0, Math.trunc(toFiniteNumber(row.tradeCount) || 0)),
      contract_count: Math.max(0, Math.trunc(toFiniteNumber(row.contractCount) || 0)),
      total_size: Math.max(0, Math.trunc(toFiniteNumber(row.totalSize) || 0)),
      total_value: toFiniteNumber(row.totalValue) || 0,
      call_size: Math.max(0, Math.trunc(toFiniteNumber(row.callSize) || 0)),
      put_size: Math.max(0, Math.trunc(toFiniteNumber(row.putSize) || 0)),
      bullish_count: Math.max(0, Math.trunc(toFiniteNumber(row.bullishCount) || 0)),
      bearish_count: Math.max(0, Math.trunc(toFiniteNumber(row.bearishCount) || 0)),
      neutral_count: Math.max(0, Math.trunc(toFiniteNumber(row.neutralCount) || 0)),
      avg_sig_score: row.avgSigScore,
      max_sig_score: row.maxSigScore,
      avg_vol_oi_ratio: row.avgVolOiRatio,
      max_vol_oi_ratio: row.maxVolOiRatio,
      max_repeat3m: row.maxRepeat3m,
      oi_sum: Math.max(0, Math.trunc(toFiniteNumber(row.oiSum) || 0)),
      day_volume_sum: Math.max(0, Math.trunc(toFiniteNumber(row.dayVolumeSum) || 0)),
      chip_hits_json: row.chipHitsJson || '{}',
      updated_at_utc: nowIso,
      spot: row.spot,
      avg_sig_score_bullish: row.avgSigScoreBullish,
      avg_sig_score_bearish: row.avgSigScoreBearish,
      net_sig_score: row.netSigScore,
      value_weighted_sig_score: row.valueWeightedSigScore,
      sweep_count: Math.max(0, Math.trunc(toFiniteNumber(row.sweepCount) || 0)),
      sweep_value_ratio: row.sweepValueRatio,
      multileg_count: Math.max(0, Math.trunc(toFiniteNumber(row.multilegCount) || 0)),
      multileg_pct: row.multilegPct,
      avg_minute_of_day_et: row.avgMinuteOfDayEt,
      avg_iv: row.avgIv,
      call_iv_avg: row.callIvAvg,
      put_iv_avg: row.putIvAvg,
      iv_spread: row.ivSpread,
      net_delta_dollars: row.netDeltaDollars,
      avg_value_pctile: row.avgValuePctile,
      avg_vol_oi_norm: row.avgVolOiNorm,
      avg_repeat_norm: row.avgRepeatNorm,
      avg_otm_norm: row.avgOtmNorm,
      avg_side_confidence: row.avgSideConfidence,
      avg_dte_norm: row.avgDteNorm,
      avg_spread_norm: row.avgSpreadNorm,
      avg_sweep_norm: row.avgSweepNorm,
      avg_multileg_norm: row.avgMultilegNorm,
      avg_time_norm: row.avgTimeNorm,
      avg_delta_norm: row.avgDeltaNorm,
      avg_iv_skew_norm: row.avgIvSkewNorm,
      avg_value_shock_norm: row.avgValueShockNorm,
      avg_dte_swing_norm: row.avgDteSwingNorm,
      avg_flow_imbalance_norm: row.avgFlowImbalanceNorm,
      avg_delta_pressure_norm: row.avgDeltaPressureNorm,
      avg_cp_oi_pressure_norm: row.avgCpOiPressureNorm,
      avg_iv_skew_surface_norm: row.avgIvSkewSurfaceNorm,
      avg_iv_term_slope_norm: row.avgIvTermSlopeNorm,
      avg_underlying_trend_confirm_norm: row.avgUnderlyingTrendConfirmNorm,
      avg_liquidity_quality_norm: row.avgLiquidityQualityNorm,
      avg_multileg_penalty_norm: row.avgMultilegPenaltyNorm,
    })),
    env,
  });

  replaceClickHouseDayRows({
    tableName: 'option_contract_minute_derived',
    whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
    deleteParams: { symbol, dayIso },
    columns: ['symbol', 'expiration', 'strike', 'option_right', 'trade_date_utc', 'minute_bucket_utc', 'trade_count', 'size_sum', 'value_sum', 'avg_price', 'last_price', 'day_volume', 'oi', 'vol_oi_ratio', 'avg_sig_score', 'max_sig_score', 'max_repeat3m', 'bullish_count', 'bearish_count', 'neutral_count', 'chip_hits_json', 'updated_at_utc'],
    rows: minuteRollups.contractMinuteRows.map((row) => ({
      symbol: row.symbol,
      expiration: row.expiration,
      strike: row.strike,
      option_right: row.right,
      trade_date_utc: row.tradeDateUtc,
      minute_bucket_utc: row.minuteBucketUtc,
      trade_count: Math.max(0, Math.trunc(toFiniteNumber(row.tradeCount) || 0)),
      size_sum: Math.max(0, Math.trunc(toFiniteNumber(row.sizeSum) || 0)),
      value_sum: toFiniteNumber(row.valueSum) || 0,
      avg_price: row.avgPrice,
      last_price: row.lastPrice,
      day_volume: row.dayVolume,
      oi: row.oi,
      vol_oi_ratio: row.volOiRatio,
      avg_sig_score: row.avgSigScore,
      max_sig_score: row.maxSigScore,
      max_repeat3m: row.maxRepeat3m,
      bullish_count: Math.max(0, Math.trunc(toFiniteNumber(row.bullishCount) || 0)),
      bearish_count: Math.max(0, Math.trunc(toFiniteNumber(row.bearishCount) || 0)),
      neutral_count: Math.max(0, Math.trunc(toFiniteNumber(row.neutralCount) || 0)),
      chip_hits_json: row.chipHitsJson || '{}',
      updated_at_utc: nowIso,
    })),
    env,
  });

  replaceClickHouseDayRows({
    tableName: 'contract_stats_intraday',
    whereSql: 'symbol = {symbol:String} AND session_date = toDate({dayIso:String})',
    deleteParams: { symbol, dayIso },
    columns: ['symbol', 'expiration', 'strike', 'option_right', 'session_date', 'day_volume', 'oi', 'last_trade_ts_utc', 'updated_at_utc'],
    rows: Array.from((built.contractStatsMap || new Map()).values()).map((row) => ({
      symbol: row.symbol,
      expiration: row.expiration,
      strike: row.strike,
      option_right: row.right,
      session_date: dayIso,
      day_volume: Math.max(0, Math.trunc(toFiniteNumber(row.dayVolume) || 0)),
      oi: Math.max(0, Math.trunc(toFiniteNumber(row.oi) || 0)),
      last_trade_ts_utc: row.lastTradeTsUtc || null,
      updated_at_utc: nowIso,
    })),
    env,
  });

  replaceClickHouseDayRows({
    tableName: 'symbol_stats_intraday',
    whereSql: 'symbol = {symbol:String} AND toDate(minute_bucket_et) = toDate({dayIso:String})',
    deleteParams: { symbol, dayIso },
    columns: ['symbol', 'minute_bucket_et', 'vol_1m', 'vol_baseline_15m', 'open_window_baseline', 'bullish_ratio_15m', 'updated_at_utc'],
    rows: Array.from((built.statsByMinute || new Map()).entries()).map(([minuteBucket, stats]) => ({
      symbol,
      minute_bucket_et: minuteBucket,
      vol_1m: toFiniteNumber(stats.symbolVol1m) || 0,
      vol_baseline_15m: toFiniteNumber(stats.symbolVolBaseline15m) || 0,
      open_window_baseline: toFiniteNumber(stats.openWindowBaseline) || 0,
      bullish_ratio_15m: toFiniteNumber(stats.bullishRatio15m) || 0,
      updated_at_utc: nowIso,
    })),
    env,
  });

  upsertClickHouseFeatureBaselines(
    symbol,
    featureBaselines || new Map(),
    built.featureBaselineUpdates || new Map(),
    env,
  );
}

async function ensureEnrichedForDayInClickHouse({
  symbol,
  dayIso,
  forceRecompute = false,
  markPartial = false,
  thresholds,
  env = process.env,
}) {
  const metricCacheMap = getClickHouseMetricCacheMap({ symbol, dayIso, env });
  const activeRuleConfig = resolveActiveRuleConfigFromClickHouse(thresholds, env);
  const enrichedRowsCache = metricCacheMap.enrichedRows;
  const dayFrom = `${dayIso}T00:00:00.000Z`;
  const dayTo = `${dayIso}T23:59:59.999Z`;

  if (!forceRecompute && enrichedRowsCache) {
    if (!hasClickHouseMinuteDerivedRows({ symbol, dayIso, env })) {
      const enrichedRows = readClickHouseEnrichedRows({
        symbol,
        from: dayFrom,
        to: dayTo,
        env,
      });
      const minuteRollups = buildMinuteDerivedRollups(enrichedRows, dayIso);

      replaceClickHouseDayRows({
        tableName: 'option_symbol_minute_derived',
        whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
        deleteParams: { symbol, dayIso },
        columns: ['symbol', 'trade_date_utc', 'minute_bucket_utc', 'trade_count', 'contract_count', 'total_size', 'total_value', 'call_size', 'put_size', 'bullish_count', 'bearish_count', 'neutral_count', 'avg_sig_score', 'max_sig_score', 'avg_vol_oi_ratio', 'max_vol_oi_ratio', 'max_repeat3m', 'oi_sum', 'day_volume_sum', 'chip_hits_json', 'updated_at_utc', 'spot', 'avg_sig_score_bullish', 'avg_sig_score_bearish', 'net_sig_score', 'value_weighted_sig_score', 'sweep_count', 'sweep_value_ratio', 'multileg_count', 'multileg_pct', 'avg_minute_of_day_et', 'avg_iv', 'call_iv_avg', 'put_iv_avg', 'iv_spread', 'net_delta_dollars', 'avg_value_pctile', 'avg_vol_oi_norm', 'avg_repeat_norm', 'avg_otm_norm', 'avg_side_confidence', 'avg_dte_norm', 'avg_spread_norm', 'avg_sweep_norm', 'avg_multileg_norm', 'avg_time_norm', 'avg_delta_norm', 'avg_iv_skew_norm', 'avg_value_shock_norm', 'avg_dte_swing_norm', 'avg_flow_imbalance_norm', 'avg_delta_pressure_norm', 'avg_cp_oi_pressure_norm', 'avg_iv_skew_surface_norm', 'avg_iv_term_slope_norm', 'avg_underlying_trend_confirm_norm', 'avg_liquidity_quality_norm', 'avg_multileg_penalty_norm'],
        rows: minuteRollups.symbolMinuteRows.map((row) => ({
          symbol: row.symbol,
          trade_date_utc: row.tradeDateUtc,
          minute_bucket_utc: row.minuteBucketUtc,
          trade_count: row.tradeCount,
          contract_count: row.contractCount,
          total_size: row.totalSize,
          total_value: row.totalValue,
          call_size: row.callSize,
          put_size: row.putSize,
          bullish_count: row.bullishCount,
          bearish_count: row.bearishCount,
          neutral_count: row.neutralCount,
          avg_sig_score: row.avgSigScore,
          max_sig_score: row.maxSigScore,
          avg_vol_oi_ratio: row.avgVolOiRatio,
          max_vol_oi_ratio: row.maxVolOiRatio,
          max_repeat3m: row.maxRepeat3m,
          oi_sum: row.oiSum,
          day_volume_sum: row.dayVolumeSum,
          chip_hits_json: row.chipHitsJson,
          updated_at_utc: new Date().toISOString(),
          spot: row.spot,
          avg_sig_score_bullish: row.avgSigScoreBullish,
          avg_sig_score_bearish: row.avgSigScoreBearish,
          net_sig_score: row.netSigScore,
          value_weighted_sig_score: row.valueWeightedSigScore,
          sweep_count: row.sweepCount,
          sweep_value_ratio: row.sweepValueRatio,
          multileg_count: row.multilegCount,
          multileg_pct: row.multilegPct,
          avg_minute_of_day_et: row.avgMinuteOfDayEt,
          avg_iv: row.avgIv,
          call_iv_avg: row.callIvAvg,
          put_iv_avg: row.putIvAvg,
          iv_spread: row.ivSpread,
          net_delta_dollars: row.netDeltaDollars,
          avg_value_pctile: row.avgValuePctile,
          avg_vol_oi_norm: row.avgVolOiNorm,
          avg_repeat_norm: row.avgRepeatNorm,
          avg_otm_norm: row.avgOtmNorm,
          avg_side_confidence: row.avgSideConfidence,
          avg_dte_norm: row.avgDteNorm,
          avg_spread_norm: row.avgSpreadNorm,
          avg_sweep_norm: row.avgSweepNorm,
          avg_multileg_norm: row.avgMultilegNorm,
          avg_time_norm: row.avgTimeNorm,
          avg_delta_norm: row.avgDeltaNorm,
          avg_iv_skew_norm: row.avgIvSkewNorm,
          avg_value_shock_norm: row.avgValueShockNorm,
          avg_dte_swing_norm: row.avgDteSwingNorm,
          avg_flow_imbalance_norm: row.avgFlowImbalanceNorm,
          avg_delta_pressure_norm: row.avgDeltaPressureNorm,
          avg_cp_oi_pressure_norm: row.avgCpOiPressureNorm,
          avg_iv_skew_surface_norm: row.avgIvSkewSurfaceNorm,
          avg_iv_term_slope_norm: row.avgIvTermSlopeNorm,
          avg_underlying_trend_confirm_norm: row.avgUnderlyingTrendConfirmNorm,
          avg_liquidity_quality_norm: row.avgLiquidityQualityNorm,
          avg_multileg_penalty_norm: row.avgMultilegPenaltyNorm,
        })),
        env,
      });

      replaceClickHouseDayRows({
        tableName: 'option_contract_minute_derived',
        whereSql: 'symbol = {symbol:String} AND trade_date_utc = toDate({dayIso:String})',
        deleteParams: { symbol, dayIso },
        columns: ['symbol', 'expiration', 'strike', 'option_right', 'trade_date_utc', 'minute_bucket_utc', 'trade_count', 'size_sum', 'value_sum', 'avg_price', 'last_price', 'day_volume', 'oi', 'vol_oi_ratio', 'avg_sig_score', 'max_sig_score', 'max_repeat3m', 'bullish_count', 'bearish_count', 'neutral_count', 'chip_hits_json', 'updated_at_utc'],
        rows: minuteRollups.contractMinuteRows.map((row) => ({
          symbol: row.symbol,
          expiration: row.expiration,
          strike: row.strike,
          option_right: row.right,
          trade_date_utc: row.tradeDateUtc,
          minute_bucket_utc: row.minuteBucketUtc,
          trade_count: row.tradeCount,
          size_sum: row.sizeSum,
          value_sum: row.valueSum,
          avg_price: row.avgPrice,
          last_price: row.lastPrice,
          day_volume: row.dayVolume,
          oi: row.oi,
          vol_oi_ratio: row.volOiRatio,
          avg_sig_score: row.avgSigScore,
          max_sig_score: row.maxSigScore,
          max_repeat3m: row.maxRepeat3m,
          bullish_count: row.bullishCount,
          bearish_count: row.bearishCount,
          neutral_count: row.neutralCount,
          chip_hits_json: row.chipHitsJson,
          updated_at_utc: new Date().toISOString(),
        })),
        env,
      });
    }
    upsertClickHouseEnrichChunkStatusForDay({
      symbol,
      dayIso,
      ruleVersion: activeRuleConfig.versionId,
      env,
    });

    return {
      synced: false,
      reason: enrichedRowsCache.cacheStatus === DAY_CACHE_STATUS_FULL ? 'metric_cache_full' : 'metric_cache_partial',
      rowCount: enrichedRowsCache.rowCount || 0,
      ruleVersion: activeRuleConfig.versionId,
      scoringModel: activeRuleConfig.scoringModel,
      targetHorizon: activeRuleConfig.targetSpec?.horizon || null,
      supplementalCache: null,
      metricCacheMap,
    };
  }

  const streamEnrichedRows = shouldStreamClickHouseEnrichedWrites(env);
  const streamEnrichedRead = shouldStreamClickHouseEnrichedReads(env);
  if (streamEnrichedRead && !streamEnrichedRows) {
    throw new Error('clickhouse_stream_read_requires_stream_write');
  }
  const metricStatusAccumulator = createMetricStatusAccumulator(markPartial);
  const enrichChunkSize = parseClickHouseEnrichStreamChunkSize(env);
  const minuteRollupState = createMinuteDerivedRollupState();
  const enrichProgressLogger = createClickHouseEnrichBatchProgressLogger({ symbol, dayIso, env });
  const enrichedInsertColumns = ['trade_id', 'trade_ts_utc', 'symbol', 'expiration', 'strike', 'option_right', 'price', 'size', 'bid', 'ask', 'condition_code', 'exchange', 'value', 'dte', 'spot', 'otm_pct', 'day_volume', 'oi', 'vol_oi_ratio', 'repeat3m', 'sig_score', 'sentiment', 'execution_side', 'symbol_vol_1m', 'symbol_vol_baseline_15m', 'open_window_baseline', 'bullish_ratio_15m', 'chips_json', 'rule_version', 'score_quality', 'missing_metrics_json', 'enriched_at_utc', 'is_sweep', 'is_multileg', 'minute_of_day_et', 'delta', 'implied_vol', 'time_norm', 'delta_norm', 'iv_skew_norm', 'value_shock_norm', 'dte_swing_norm', 'flow_imbalance_norm', 'delta_pressure_norm', 'cp_oi_pressure_norm', 'iv_skew_surface_norm', 'iv_term_slope_norm', 'underlying_trend_confirm_norm', 'liquidity_quality_norm', 'multileg_penalty_norm', 'sig_score_components_json'];
  let rawRows = [];
  let supplementalMetrics = null;
  let built = null;
  let minuteRollupsOverride = null;

  if (streamEnrichedRows) {
    deleteClickHouseScope(
      'option_trade_enriched',
      'symbol = {symbol:String} AND trade_date = toDate({dayIso:String})',
      { symbol, dayIso },
      env,
    );
  }

  const baseBuildConfig = {
    ...activeRuleConfig,
    dayIso,
    minuteRollupState: streamEnrichedRows ? minuteRollupState : null,
    metricStatusAccumulator,
    rowConsumerChunkSize: enrichChunkSize,
    rowConsumer: streamEnrichedRows
      ? (chunkRows) => {
        if (!Array.isArray(chunkRows) || chunkRows.length === 0) return;
        const nowIso = new Date().toISOString();
        insertClickHouseRows(
          'option_trade_enriched',
          enrichedInsertColumns,
          chunkRows.map((row) => toClickHouseEnrichedInsertRow(row, nowIso)),
          env,
          { chunkSize: enrichChunkSize },
        );
      }
      : null,
  };

  if (streamEnrichedRead) {
    const precomputed = buildClickHouseStreamingPrecompute({ symbol, dayIso, env });
    supplementalMetrics = buildSupplementalMetricLookupFromClickHouse({
      symbol,
      dayIso,
      rawRows: [],
      env,
    });
    precomputed.payloadSpotBySymbol.forEach((value, rowSymbol) => {
      if (!supplementalMetrics.spotBySymbol.has(rowSymbol)) {
        supplementalMetrics.spotBySymbol.set(rowSymbol, value);
      }
    });
    precomputed.payloadOiByContract.forEach((value, key) => {
      if (!supplementalMetrics.oiByContract.has(key)) {
        supplementalMetrics.oiByContract.set(key, value);
      }
    });

    const rollingState = {};
    let streamedRowCount = 0;
    loadClickHouseRawTradesForDay({
      symbol,
      dayIso,
      env,
      onChunk: (chunkRows) => {
        if (enrichProgressLogger) {
          enrichProgressLogger.recordRows(chunkRows);
        }
        const chunkBuilt = buildEnrichedRows(
          chunkRows,
          activeRuleConfig.thresholds,
          supplementalMetrics,
          {
            ...baseBuildConfig,
            rollingState,
            precomputedStatsByMinute: precomputed.statsByMinute,
            precomputedValueSamples: precomputed.valueSamples,
            precomputedMinValue: precomputed.minValue,
            precomputedMaxValue: precomputed.maxValue,
            disableHeuristicMultileg: true,
          },
        );
        streamedRowCount += Number(chunkBuilt.rowCount || 0);
      },
    });
    if (enrichProgressLogger) {
      enrichProgressLogger.flush();
    }

    built = {
      rows: [],
      rowCount: streamedRowCount,
      contractStatsMap: rollingState.contractStatsMap || new Map(),
      statsByMinute: precomputed.statsByMinute || new Map(),
      featureBaselineUpdates: rollingState.featureBaselineUpdates || new Map(),
    };
    minuteRollupsOverride = streamEnrichedRows
      ? finalizeMinuteDerivedRollups(minuteRollupState)
      : null;
  } else {
    rawRows = loadClickHouseRawTradesForDay({ symbol, dayIso, env });
    if (enrichProgressLogger) {
      enrichProgressLogger.recordRows(rawRows);
      enrichProgressLogger.flush();
    }
    supplementalMetrics = buildSupplementalMetricLookupFromClickHouse({
      symbol,
      dayIso,
      rawRows,
      env,
    });
    built = buildEnrichedRows(rawRows, activeRuleConfig.thresholds, supplementalMetrics, baseBuildConfig);
    minuteRollupsOverride = streamEnrichedRows
      ? finalizeMinuteDerivedRollups(minuteRollupState)
      : null;
    rawRows = [];
  }

  persistClickHouseEnrichedDayState({
    symbol,
    dayIso,
    built,
    featureBaselines: supplementalMetrics.featureBaselines,
    env,
    skipEnrichedRowsReplace: streamEnrichedRows,
    minuteRollupsOverride,
  });

  const metricStatuses = finalizeMetricStatuses(metricStatusAccumulator);
  const rowCount = Number(streamEnrichedRows ? built.rowCount : built.rows.length) || 0;
  upsertClickHouseMetricCacheRows({
    symbol,
    dayIso,
    rows: { length: rowCount },
    metricStatuses,
    markPartial,
    env,
  });
  upsertClickHouseEnrichChunkStatusForDay({
    symbol,
    dayIso,
    ruleVersion: activeRuleConfig.versionId,
    env,
  });

  return {
    synced: true,
    reason: null,
    rowCount,
    ruleVersion: activeRuleConfig.versionId,
    scoringModel: activeRuleConfig.scoringModel,
    targetHorizon: activeRuleConfig.targetSpec?.horizon || null,
    supplementalCache: supplementalMetrics.cacheStats || null,
    metricCacheMap: getClickHouseMetricCacheMap({ symbol, dayIso, env }),
  };
}

async function materializeHistoricalDayInClickHouse({
  symbol,
  dayIso,
  thresholds,
  env = process.env,
  requiredMetrics = [],
  markPartial = false,
  mode = 'full',
  forceRecompute = false,
}) {
  const normalizedMode = String(mode || 'full').trim().toLowerCase();
  const modeDownloadOnly = normalizedMode === 'download';
  const modeEnrichOnly = normalizedMode === 'enrich';
  if (!['full', 'download', 'enrich'].includes(normalizedMode)) {
    throw new Error(`invalid_materialize_mode:${mode}`);
  }

  const rawHydrationTargets = parseClickHouseRawHydrationTargets(env);

  ensureClickHouseSupportSchema(env);
  let existingDayCache = getClickHouseDayCache({ symbol, dayIso, env });
  const dayCacheRowCount = Number(existingDayCache?.rowCount);
  const cachedRows = (
    existingDayCache?.cacheStatus === DAY_CACHE_STATUS_FULL
    && Number.isFinite(dayCacheRowCount)
    && dayCacheRowCount >= 0
  )
    ? Math.max(0, Math.trunc(dayCacheRowCount))
    : countClickHouseTradesForDay({ symbol, dayIso, env });

  let sync = {
    synced: false,
    reason: existingDayCache?.cacheStatus === DAY_CACHE_STATUS_FULL ? 'day_cache_full' : null,
    fetchedRows: 0,
    upsertedRows: 0,
    cachedRows,
    cacheStatus: existingDayCache?.cacheStatus || null,
  };

  const shouldSyncTrades = !modeEnrichOnly
    && (
      (forceRecompute && shouldForceTradeSyncOnBackfill(env))
      || !existingDayCache
      || existingDayCache.cacheStatus !== DAY_CACHE_STATUS_FULL
      || cachedRows === 0
    );

  if (shouldSyncTrades) {
    try {
      sync = await syncThetaTradesToClickHouse({
        symbol,
        dayIso,
        env,
        markPartial,
      });
    } catch (error) {
      const rowCount = countClickHouseTradesForDay({ symbol, dayIso, env });
      upsertClickHouseDayCache({
        symbol,
        dayIso,
        cacheStatus: DAY_CACHE_STATUS_PARTIAL,
        rowCount,
        lastError: error.message,
        sourceEndpoint: null,
        env,
      });
      throw error;
    }

    if (!sync.synced) {
      const notConfiguredError = new Error(sync.reason || 'thetadata_not_configured');
      notConfiguredError.code = 'thetadata_not_configured';
      throw notConfiguredError;
    }

    existingDayCache = getClickHouseDayCache({ symbol, dayIso, env });
  }

  if (modeEnrichOnly && (!existingDayCache || existingDayCache.cacheStatus !== DAY_CACHE_STATUS_FULL)) {
    return {
      db: null,
      sync,
      enrichment: {
        synced: false,
        reason: 'raw_not_ready',
        rowCount: 0,
        ruleVersion: null,
        scoringModel: null,
        targetHorizon: null,
        supplementalCache: null,
        metricCacheMap: getClickHouseMetricCacheMap({ symbol, dayIso, env }),
      },
      rawHydration: null,
    };
  }

  let rawHydration = modeEnrichOnly
    ? {
      tradeRows: cachedRows,
      stockRows: countClickHouseStockRawRowsForDay({ symbol, dayIso, env }),
      oiRows: countClickHouseOptionOiRowsForDay({ symbol, dayIso, env }),
      quoteRows: countClickHouseOptionQuoteRowsForDay({ symbol, dayIso, env }),
      greeksRows: countClickHouseOptionGreeksRowsForDay({ symbol, dayIso, env }),
      supplementalCache: null,
    }
    : await ensureRawHydratedForDayInClickHouse({
      symbol,
      dayIso,
      env,
      ...rawHydrationTargets,
      tradeRowsHint: Number.isFinite(Number(sync.cachedRows))
        ? Math.max(0, Math.trunc(Number(sync.cachedRows)))
        : cachedRows,
    });

  const enrichment = modeDownloadOnly
    ? {
      synced: false,
      reason: 'download_only',
      rowCount: 0,
      ruleVersion: null,
      scoringModel: null,
      targetHorizon: null,
      supplementalCache: rawHydration?.supplementalCache || null,
      metricCacheMap: getClickHouseMetricCacheMap({ symbol, dayIso, env }),
    }
    : await ensureEnrichedForDayInClickHouse({
      symbol,
      dayIso,
      forceRecompute: true,
      markPartial,
      thresholds,
      env,
      requiredMetrics,
    });

  if (rawHydration && shouldEmitBackfillGapTelemetry(env)) {
    try {
      rawHydration.coverage = await buildClickHouseGapTelemetryForDay({ symbol, dayIso, env });
    } catch (error) {
      rawHydration.coverage = {
        error: String(error?.message || 'coverage_telemetry_failed'),
      };
    }
  }

  return {
    db: null,
    sync,
    enrichment,
    rawHydration,
  };
}

async function queryHistoricalFlow(rawQuery = {}, env = process.env) {
  const from = normalizeIsoTimestamp(rawQuery.from);
  const to = normalizeIsoTimestamp(rawQuery.to);

  if (!from || !to) {
    return badRequest('Query params "from" and "to" are required ISO-8601 timestamps.');
  }

  if (from > to) {
    return badRequest('Query param "from" must be less than or equal to "to".');
  }

  const symbol = normalizeSymbol(rawQuery.symbol);
  if (!symbol) {
    return badRequest('Query param "symbol" is required.');
  }

  const fromDay = from.slice(0, 10);
  const toDay = to.slice(0, 10);
  if (fromDay !== toDay) {
    return badRequest('Current historical API supports only a single UTC day range (from/to same date).');
  }

  const limit = parseLimit(rawQuery.limit);
  const hasExplicitLimit = rawQuery.limit !== undefined;
  const readBackend = resolveFlowReadBackend(env);
  const writeBackend = resolveFlowWriteBackend(env);
  const thresholds = getThresholds(env);
  const filters = parseHistoricalFilters(rawQuery);
  const requiredMetrics = getRequiredMetricsForQuery(filters);

  if (writeBackend !== 'clickhouse') {
    return {
      status: 503,
      error: {
        code: 'sqlite_deprecated',
        message: 'SQLite runtime path is disabled. Set PHENIX_FLOW_WRITE_BACKEND=clickhouse.',
      },
    };
  }

  if (readBackend !== 'sqlite') {
    try {
      const clickhouseData = loadClickHouseHistoricalDay({
        symbol,
        dayIso: fromDay,
        from,
        to,
        requiredMetrics,
        env,
      });

      if (clickhouseData) {
        const filteredRows = applyHistoricalFilters(clickhouseData.rows, filters)
          .filter((row) => row.tradeTsUtc >= from && row.tradeTsUtc <= to);

        const data = filteredRows.slice(0, limit).map((row) => ({
          id: row.id,
          tradeTsUtc: row.tradeTsUtc,
          symbol: row.symbol,
          expiration: row.expiration,
          strike: row.strike,
          right: row.right,
          price: row.price,
          size: row.size,
          bid: row.bid,
          ask: row.ask,
          conditionCode: row.conditionCode,
          exchange: row.exchange,
          value: row.value,
          dte: row.dte,
          spot: row.spot,
          otmPct: row.otmPct,
          dayVolume: row.dayVolume,
          oi: row.oi,
          volOiRatio: row.volOiRatio,
          repeat3m: row.repeat3m,
          sigScore: row.sigScore,
          sigScoreComponents: row.sigScoreComponents || {},
          sentiment: row.sentiment,
          scoreQuality: row.scoreQuality || 'partial',
          missingMetrics: Array.isArray(row.missingMetrics) ? row.missingMetrics : [],
          ruleVersion: row.ruleVersion || clickhouseData.enrichment.ruleVersion || null,
          targetHorizon: clickhouseData.enrichment.targetHorizon || null,
          chips: row.chips,
        }));

        return {
          data,
          meta: {
            source: 'clickhouse',
            dbPath: clickhouseData.observability.artifactPath,
            artifactPath: clickhouseData.observability.artifactPath,
            dateRange: { from, to },
            filter: {
              symbol,
              chips: filters.chips,
              right: filters.right,
              expiration: filters.expiration,
              side: filters.side,
              sentiment: filters.sentiment,
            },
            total: filteredRows.length,
            sync: {
              synced: false,
              reason: 'day_cache_full',
              fetchedRows: 0,
              upsertedRows: 0,
              cachedRows: Number(clickhouseData.dayCache.rowCount || filteredRows.length || 0),
              cacheStatus: clickhouseData.dayCache.cacheStatus,
            },
            enrichment: clickhouseData.enrichment,
          },
        };
      }
    } catch {
      // Materialize via ClickHouse path below.
    }
  }

  try {
    const materialized = await materializeHistoricalDayInClickHouse({
      symbol,
      dayIso: fromDay,
      thresholds,
      env,
      requiredMetrics,
      markPartial: hasExplicitLimit,
    });

    const { sync, enrichment } = materialized;
    const metricCacheMap = enrichment.metricCacheMap || getClickHouseMetricCacheMap({ symbol, dayIso: fromDay, env });
    const metricUnavailable = buildMetricUnavailableError(requiredMetrics, metricCacheMap);
    if (metricUnavailable) {
      return metricUnavailable;
    }

    const allRows = readClickHouseEnrichedRows({
      symbol,
      from: `${fromDay}T00:00:00.000Z`,
      to: `${fromDay}T23:59:59.999Z`,
      env,
    });
    const filteredRows = applyHistoricalFilters(allRows, filters)
      .filter((row) => row.tradeTsUtc >= from && row.tradeTsUtc <= to);

    const data = filteredRows.slice(0, limit).map((row) => ({
      id: row.id,
      tradeTsUtc: row.tradeTsUtc,
      symbol: row.symbol,
      expiration: row.expiration,
      strike: row.strike,
      right: row.right,
      price: row.price,
      size: row.size,
      bid: row.bid,
      ask: row.ask,
      conditionCode: row.conditionCode,
      exchange: row.exchange,
      value: row.value,
      dte: row.dte,
      spot: row.spot,
      otmPct: row.otmPct,
      dayVolume: row.dayVolume,
      oi: row.oi,
      volOiRatio: row.volOiRatio,
      repeat3m: row.repeat3m,
      sigScore: row.sigScore,
      sigScoreComponents: row.sigScoreComponents || {},
      sentiment: row.sentiment,
      scoreQuality: row.scoreQuality || 'partial',
      missingMetrics: Array.isArray(row.missingMetrics) ? row.missingMetrics : [],
      ruleVersion: row.ruleVersion || enrichment.ruleVersion || null,
      targetHorizon: enrichment.targetHorizon || null,
      chips: row.chips,
    }));

    return {
      data,
      meta: {
        source: 'clickhouse',
        dbPath: buildClickHouseArtifactPath(env),
        artifactPath: buildClickHouseArtifactPath(env),
        dateRange: { from, to },
        filter: {
          symbol,
          chips: filters.chips,
          right: filters.right,
          expiration: filters.expiration,
          side: filters.side,
          sentiment: filters.sentiment,
        },
        total: filteredRows.length,
        sync,
        enrichment: {
          ...enrichment,
          metricCacheMap,
        },
      },
    };
  } catch (error) {
    if (error?.code === 'thetadata_not_configured') {
      return {
        status: 503,
        error: {
          code: 'thetadata_not_configured',
          message: 'THETADATA_BASE_URL is required to fetch real historical trades.',
        },
      };
    }

    if (String(error?.message || '').startsWith('thetadata_request_failed:')) {
      return {
        status: 502,
        error: {
          code: 'thetadata_sync_failed',
          message: error.message,
        },
      };
    }

    return {
      status: 500,
      error: {
        code: 'enrichment_failed',
        message: error.message,
      },
    };
  }
}

module.exports = {
  queryHistoricalFlow,
  __private: {
    ensureSchema,
    resolveDbPath,
    normalizeIsoTimestamp,
    normalizeSymbol,
    parseLimit,
    parseJsonRows,
    normalizeThetaRows,
    resolveThetaEndpoint,
    resolveThetaTimeWindowsForSymbol,
    resolveThetaSpotEndpoint,
    resolveThetaOiEndpoint,
    resolveThetaOiBulkEndpoint,
    resolveThetaOptionQuoteEndpoint,
    extractMetricFromResponse,
    fetchThetaMetricNumber,
    fetchThetaRows,
    buildSupplementalMetricLookup,
    ensureStockRawForDay,
    ensureOiRawForDay,
    ensureOptionQuoteRawForDay,
    countCachedRows,
    upsertDayCache,
    getDayCache,
    upsertMetricCache,
    getMetricCacheMap,
    getClickHouseDayCache,
    getClickHouseMetricCacheMap,
    listClickHouseCachedDays,
    ensureEnrichedForDay,
    materializeHistoricalDayInClickHouse,
    buildEnrichedRows,
    buildMetricUnavailableError,
    loadClickHouseHistoricalDay,
    hydrateEnrichedRows,
    buildMinuteStats,
    buildMinuteDerivedRollups,
    buildClickHouseGapTelemetryForDay,
    upsertSymbolMinuteDerived,
    upsertContractMinuteDerived,
    upsertOptionTrades,
    evaluateChips,
    requiresClickHouseDeleteBeforeInsert,
    DAY_CACHE_STATUS_FULL,
    DAY_CACHE_STATUS_PARTIAL,
    METRIC_NAMES,
    CHIP_DEFINITIONS,
  },
};
