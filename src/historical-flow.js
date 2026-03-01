const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');

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
const DEFAULT_SPOT_PATH = '/v3/stock/history/ohlc';
const DEFAULT_OI_PATH = '/v3/option/history/open_interest';
const DEFAULT_GREEKS_PATH = '/v3/option/history/greeks/first_order';
const DEFAULT_THETADATA_TIMEOUT_MS = 15000;
const DEFAULT_SUPPLEMENTAL_CACHE_TTL_HOURS = 24;
const DEFAULT_SUPPLEMENTAL_CONCURRENCY = 18;
const DEFAULT_TREND_FALLBACK_MAX_LAG_MINUTES = 480;
const DEFAULT_GREEKS_CONTRACT_FALLBACK_LIMIT = 200;

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

function normalizeThetaRows(rows, symbol, dayIso) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;

  return rows.map((row) => {
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
  }).filter(Boolean);
}

function resolveThetaEndpoint(symbol, yyyymmdd, env = process.env) {
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  if (!baseUrl) return null;

  const configuredPath = (env.THETADATA_HISTORICAL_OPTION_PATH || DEFAULT_HISTORICAL_OPTION_PATH).trim();
  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;

  const url = new URL(`${normalizedBase}${normalizedPath}`);
  url.searchParams.set('symbol', symbol);
  url.searchParams.set('expiration', '*');
  url.searchParams.set('date', yyyymmdd);
  url.searchParams.set('format', 'json');
  return url.toString();
}

function resolveThetaSpotEndpoint(symbol, dayIso, env = process.env) {
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
  }
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

function shouldTraceThetaDownloads(env = process.env) {
  return String(env.THETADATA_DOWNLOAD_TRACE || '1') !== '0';
}

function inferThetaApiName(url) {
  try {
    const parsed = new URL(url);
    const path = parsed.pathname || '';
    if (path.includes('/option/history/trade_quote')) return 'option_history_trade_quote';
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

  try {
    const response = await fetch(url, { signal: controller.signal });
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

async function fetchThetaRows(url, { env = process.env } = {}) {
  if (!url) return [];

  try {
    const { response, body, durationMs } = await fetchTextWithTimeout(url, { env });
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
      pickField(row, ['timestamp', 'time', 'datetime', 'ms_of_day']),
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

async function syncThetaTradesToSqlite({
  symbol,
  dayIso,
  env = process.env,
  db,
  markPartial = false,
}) {
  const endpoint = resolveThetaEndpoint(symbol, toYyyymmdd(`${dayIso}T00:00:00.000Z`), env);
  if (!endpoint) {
    return { synced: false, reason: 'thetadata_base_url_missing', fetchedRows: 0, upsertedRows: 0, cachedRows: 0 };
  }

  const { response, body, durationMs } = await fetchTextWithTimeout(endpoint, { env });

  if (!response.ok) {
    logThetaDownload({
      env,
      url: endpoint,
      durationMs,
      status: response.status,
      ok: false,
      rows: 0,
      error: `http_${response.status}`,
    });
    throw new Error(`thetadata_request_failed:${response.status}`);
  }

  const parsedRows = parseJsonRows(body);
  const normalizedRows = normalizeThetaRows(parsedRows, symbol, dayIso);
  logThetaDownload({
    env,
    url: endpoint,
    durationMs,
    status: response.status,
    ok: true,
    rows: normalizedRows.length,
    error: null,
  });
  const dbPath = resolveDbPath(env);
  const writeDb = db || new Database(dbPath);

  try {
    ensureSchema(writeDb);

    const upsert = writeDb.prepare(`
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

    const txn = writeDb.transaction((rows) => {
      let writes = 0;
      rows.forEach((row) => {
        writes += upsert.run(row).changes;
      });
      return writes;
    });

    const upsertedRows = txn(normalizedRows);
    const dayStart = `${dayIso}T00:00:00.000Z`;
    const dayEnd = `${dayIso}T23:59:59.999Z`;
    const rowCount = countCachedRows(writeDb, { from: dayStart, to: dayEnd, symbol });

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
      sourceEndpoint: endpoint,
    });

    return {
      synced: true,
      reason: null,
      fetchedRows: normalizedRows.length,
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

function calculateMetricStatuses(rows, markPartial) {
  const emptyIsFull = rows.length === 0;

  const statusFromPredicate = (predicate) => {
    if (markPartial) return DAY_CACHE_STATUS_PARTIAL;
    if (emptyIsFull) return DAY_CACHE_STATUS_FULL;
    return rows.every(predicate) ? DAY_CACHE_STATUS_FULL : DAY_CACHE_STATUS_PARTIAL;
  };

  return {
    enrichedRows: markPartial ? DAY_CACHE_STATUS_PARTIAL : DAY_CACHE_STATUS_FULL,
    execution: statusFromPredicate((row) => row.execution && typeof row.execution.executionSide === 'string'),
    value: statusFromPredicate((row) => row.value !== null),
    size: statusFromPredicate((row) => row.size !== null),
    dte: statusFromPredicate((row) => row.dte !== null),
    expiration: statusFromPredicate((row) => typeof row.expiration === 'string' && row.expiration.length >= 10),
    repeat3m: statusFromPredicate((row) => row.repeat3m !== null),
    sentiment: statusFromPredicate((row) => typeof row.sentiment === 'string'),
    symbolVolStats: statusFromPredicate((row) => row.symbolVol1m !== null
      && row.symbolVolBaseline15m !== null
      && row.openWindowBaseline !== null),
    bullishRatio15m: statusFromPredicate((row) => row.bullishRatio15m !== null),
    spot: statusFromPredicate((row) => row.spot !== null),
    otmPct: statusFromPredicate((row) => row.otmPct !== null),
    oi: statusFromPredicate((row) => row.oi !== null),
    volOiRatio: statusFromPredicate((row) => row.volOiRatio !== null),
    sigScore: statusFromPredicate((row) => row.sigScore !== null),
  };
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

function buildMinuteDerivedRollups(rows, dayIso) {
  const symbolMinute = new Map();
  const contractMinute = new Map();

  rows.forEach((row) => {
    const minuteBucket = toMinuteBucketUtc(row.tradeTsUtc);
    if (!minuteBucket) return;

    const contractKey = buildContractKey(row);
    const symbolKey = `${row.symbol}|${minuteBucket}`;
    const contractMinuteKey = `${row.symbol}|${row.expiration}|${row.strike}|${row.right}|${minuteBucket}`;

    const symbolAgg = symbolMinute.get(symbolKey) || {
      symbol: row.symbol,
      tradeDateUtc: dayIso,
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

    // Sweep accumulation
    if (row.isSweep) {
      symbolAgg.sweepCount += 1;
      symbolAgg.sweepValueSum += toFiniteNumber(row.value) || 0;
    }

    // Multileg accumulation
    if (row.isMultileg) {
      symbolAgg.multilegCount += 1;
    }

    // IV accumulation
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

    // Delta accumulation
    const delta = toFiniteNumber(row.delta);
    const tradeValue = toFiniteNumber(row.value);
    if (delta !== null && tradeValue !== null) {
      const directionSign = row.sentiment === 'bullish' ? 1 : (row.sentiment === 'bearish' ? -1 : 0);
      symbolAgg.netDeltaDollars += delta * tradeValue * directionSign;
    }

    // Time accumulation
    const minuteOfDayEt = toFiniteNumber(row.minuteOfDayEt);
    if (minuteOfDayEt !== null) {
      symbolAgg.minuteOfDayEtSum += minuteOfDayEt;
      symbolAgg.minuteOfDayEtCount += 1;
    }

    // Per-component accumulation for sigScore decomposition
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

    symbolMinute.set(symbolKey, symbolAgg);

    const contractAgg = contractMinute.get(contractMinuteKey) || {
      symbol: row.symbol,
      expiration: row.expiration,
      strike: row.strike,
      right: row.right,
      tradeDateUtc: dayIso,
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

    contractMinute.set(contractMinuteKey, contractAgg);
  });

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
      } catch {
        greeksRows = [];
      }
    }

    ingestGreeksRows(greeksRows);
  });

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
      } catch {
        greeksRows = [];
      }
    }
    ingestGreeksRows(greeksRows);
  });

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
  const oiByContract = loadContractOiFromStats(db, { symbol, dayIso });
  const featureBaselines = loadFeatureBaselines(db, symbol);
  let oiDefaultsToZero = false;
  const referenceOiByContract = loadReferenceOiMap(db, { symbol, asOfDate: dayIso });
  const supplementalConcurrency = parseSupplementalConcurrency(env);

  referenceOiByContract.forEach((oiValue, contractKey) => {
    if (oiValue !== null && oiValue !== undefined && !oiByContract.has(contractKey)) {
      oiByContract.set(contractKey, oiValue);
    }
  });

  rawRows.forEach((row) => {
    const payload = parsePayload(row.rawPayloadJson);
    const spot = computeSpot(payload);
    if (spot !== null && !spotBySymbol.has(row.symbol)) {
      spotBySymbol.set(row.symbol, spot);
    }

    const oi = extractOi(payload);
    if (oi !== null) {
      oiByContract.set(buildContractKey(row), oi);
    }
  });

  const requiresSpot = true;
  const requiresOi = true;

  if ((env.THETADATA_BASE_URL || '').trim()) {
    const shouldFetchSpot = Boolean((env.THETADATA_SPOT_PATH || DEFAULT_SPOT_PATH || '').trim());
    const shouldFetchOi = Boolean((env.THETADATA_OI_PATH || DEFAULT_OI_PATH || '').trim());

    if (shouldFetchSpot && requiresSpot) {
      const symbolsMissingSpot = Array.from(new Set(rawRows.map((row) => row.symbol)))
        .filter((rowSymbol) => !spotBySymbol.has(rowSymbol));

      await parallelMapLimit(symbolsMissingSpot, supplementalConcurrency, async (rowSymbol) => {
        const spotCacheKey = `${rowSymbol}|${dayIso}`;
        let stockRows = getSupplementalCache(db, 'stock_ohlc_symbol_day', spotCacheKey);
        if (Array.isArray(stockRows)) {
          cacheStats.stockHit += 1;
        } else {
          cacheStats.stockMiss += 1;
          const spotEndpoint = resolveThetaSpotEndpoint(rowSymbol, dayIso, env);
          stockRows = await fetchThetaRows(spotEndpoint, { env });
          if (Array.isArray(stockRows) && stockRows.length > 0) {
            upsertSupplementalCache(db, 'stock_ohlc_symbol_day', spotCacheKey, stockRows, env, dayIso);
          }
        }

        const normalizedBars = normalizeStockOhlcRows(Array.isArray(stockRows) ? stockRows : [], dayIso);
        if (normalizedBars.length > 0) {
          const byMinute = buildStockFeaturesByMinute(normalizedBars);
          byMinute.forEach((features, minuteBucketUtc) => {
            stockBySymbolMinute.set(`${rowSymbol}|${minuteBucketUtc}`, features);
          });
          const latest = normalizedBars[normalizedBars.length - 1];
          if (latest && toFiniteNumber(latest.close) !== null) {
            spotBySymbol.set(rowSymbol, toFiniteNumber(latest.close));
            cacheStats.spotHit += 1;
            return;
          }
        }

        const numericSpot = await fetchThetaMetricNumber(resolveThetaSpotEndpoint(rowSymbol, dayIso, env), [
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
        if (numericSpot !== null) {
          spotBySymbol.set(rowSymbol, numericSpot);
          cacheStats.spotHit += 1;
          upsertSupplementalCache(db, 'spot_symbol_day', spotCacheKey, numericSpot, env, dayIso);
        } else {
          cacheStats.spotMiss += 1;
        }
      });
    }

    if (shouldFetchOi && requiresOi) {
      const seenContracts = new Set();
      const contractsMissingOi = [];
      rawRows.forEach((row) => {
        const contractKey = buildContractKey(row);
        if (seenContracts.has(contractKey)) return;
        seenContracts.add(contractKey);
        if (!oiByContract.has(contractKey)) contractsMissingOi.push(row);
      });

      if (contractsMissingOi.length > 0) {
        const bulkOiEndpoint = resolveThetaOiBulkEndpoint(symbol, dayIso, env);
        try {
          const { response, body, durationMs } = await fetchTextWithTimeout(bulkOiEndpoint, { env });
          if (response.ok) {
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
            oiRows.forEach((oiRow) => {
              const rowSymbol = normalizeSymbol(oiRow.symbol);
              const expiration = normalizeIsoDate(oiRow.expiration);
              const strike = toFiniteNumber(oiRow.strike);
              const right = normalizeRight(oiRow.right);
              const oi = toFiniteNumber(oiRow.open_interest ?? oiRow.oi ?? oiRow.openInterest);

              if (!rowSymbol || !expiration || strike === null || !right || oi === null) return;
              const oiCacheKey = `${rowSymbol}|${expiration}|${strike}|${right}|${dayIso}`;
              upsertSupplementalCache(db, 'oi_contract_day', oiCacheKey, Math.trunc(oi), env, dayIso);
              oiByContract.set(buildContractKey({
                symbol: rowSymbol,
                expiration,
                strike,
                right,
              }), Math.trunc(oi));
            });
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
          oiDefaultsToZero = false;
        }
      }

      await parallelMapLimit(contractsMissingOi, supplementalConcurrency, async (row) => {
        const contractKey = buildContractKey(row);
        if (oiByContract.has(contractKey)) return;
        const oiCacheKey = `${row.symbol}|${row.expiration}|${row.strike}|${row.right}|${dayIso}`;
        const cachedOi = getSupplementalCache(db, 'oi_contract_day', oiCacheKey);
        let oi = toFiniteNumber(cachedOi);
        if (oi !== null) {
          cacheStats.oiHit += 1;
        } else {
          cacheStats.oiMiss += 1;
          const oiEndpoint = resolveThetaOiEndpoint(row, dayIso, env);
          oi = await fetchThetaMetricNumber(oiEndpoint, [
            'oi',
            'open_interest',
            'openInterest',
          ]);
          if (oi !== null) {
            upsertSupplementalCache(db, 'oi_contract_day', oiCacheKey, oi, env, dayIso);
          }
        }
        if (oi !== null) {
          oiByContract.set(contractKey, Math.trunc(oi));
        }
      });
    }
  }

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
  const statsByMinute = buildMinuteStats(rawRows);

  const multilegIndices = detectHeuristicMultilegs(rawRows);

  const contractDayVolume = new Map();
  const contractStatsMap = new Map();
  const sideWindows = new Map();
  const symbolPressureWindows = new Map();
  const cpOiPressureWindows = new Map();
  const runningCallIv = new Map();
  const runningPutIv = new Map();
  const spotLastSeenBySymbol = new Map();
  const trendLastSeenBySymbol = new Map();
  const lastContractPrint = new Map();
  const featureBaselineUpdates = new Map();

  const valueSamples = rawRows
    .map((row) => computeValue(row.price, row.size))
    .filter((value) => value !== null)
    .sort((a, b) => a - b);

  const minValue = valueSamples.length ? valueSamples[0] : 0;
  const maxValue = valueSamples.length ? valueSamples[valueSamples.length - 1] : 0;
  const scoreModel = scoringConfig.scoringModel || 'v4_expanded';
  const scoreRuleVersion = scoringConfig.versionId
    || (scoreModel === 'v1_baseline' ? 'v1_baseline_default' : (scoreModel === 'v5_swing' ? 'v5_swing_default' : 'v4_expanded_default'));

  const enrichedRows = [];

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
    const sideWindow = sideWindows.get(sideKey) || [];

    while (sideWindow.length && (rowMs - sideWindow[0]) > 180000) {
      sideWindow.shift();
    }

    sideWindow.push(rowMs);
    sideWindows.set(sideKey, sideWindow);

    const repeat3m = sideWindow.length;

    const payload = parsePayload(row.rawPayloadJson);
    const payloadSpot = computeSpot(payload);
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
    const oiCandidate = extractOi(payload) ?? oiByContract.get(contractKey) ?? null;
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
    const pressureWindow = symbolPressureWindows.get(row.symbol) || [];
    while (pressureWindow.length && (rowMs - pressureWindow[0].ts) > 1800000) {
      pressureWindow.shift();
    }

    const premiumValue = value || 0;
    const deltaNotional = (effectiveDelta !== null && value !== null) ? (Math.abs(effectiveDelta) * value) : 0;
    if (hasDirection && value !== null) {
      pressureWindow.push({
        ts: rowMs,
        signedPremium: direction * premiumValue,
        totalPremium: premiumValue,
        signedDeltaNotional: direction * deltaNotional,
        totalDeltaNotional: deltaNotional,
      });
    }
    symbolPressureWindows.set(row.symbol, pressureWindow);

    const pressureTotals = pressureWindow.reduce((acc, item) => {
      acc.signedPremium += item.signedPremium;
      acc.totalPremium += item.totalPremium;
      acc.signedDeltaNotional += item.signedDeltaNotional;
      acc.totalDeltaNotional += item.totalDeltaNotional;
      return acc;
    }, {
      signedPremium: 0,
      totalPremium: 0,
      signedDeltaNotional: 0,
      totalDeltaNotional: 0,
    });

    const flowImbalanceNorm = hasDirection
      ? computeFlowImbalanceNorm(pressureTotals.signedPremium, pressureTotals.totalPremium)
      : 0;
    const deltaPressureNorm = hasDirection
      ? computeDeltaPressureNorm(pressureTotals.signedDeltaNotional, pressureTotals.totalDeltaNotional)
      : 0;

    const cpWindow = cpOiPressureWindows.get(row.symbol) || [];
    while (cpWindow.length && (rowMs - cpWindow[0].ts) > 1800000) {
      cpWindow.shift();
    }

    if (value !== null && oi !== null && oi >= 0 && dte !== null && dte <= 60 && otmPct !== null && Math.abs(otmPct) <= 20) {
      cpWindow.push({
        ts: rowMs,
        callPressure: row.right === 'CALL' ? (value / Math.max(oi, 1)) : 0,
        putPressure: row.right === 'PUT' ? (value / Math.max(oi, 1)) : 0,
      });
    }
    cpOiPressureWindows.set(row.symbol, cpWindow);
    const cpTotals = cpWindow.reduce((acc, item) => {
      acc.call += item.callPressure;
      acc.put += item.putPressure;
      return acc;
    }, { call: 0, put: 0 });
    const cpOiPressureNorm = computeCpOiPressureNorm(cpTotals.call, cpTotals.put);

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

    enrichedRows.push(enriched);

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

  return {
    rows: enrichedRows,
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

  return rows.map((row) => {
    let chips = [];
    try {
      const parsed = JSON.parse(row.chipsJson || '[]');
      chips = Array.isArray(parsed) ? parsed : [];
    } catch {
      chips = [];
    }

    let missingMetrics = [];
    try {
      const parsedMissing = JSON.parse(row.missingMetricsJson || '[]');
      missingMetrics = Array.isArray(parsedMissing) ? parsedMissing : [];
    } catch {
      missingMetrics = [];
    }

    let sigScoreComponents = {};
    try {
      const parsedComponents = JSON.parse(row.sigScoreComponentsJson || '{}');
      sigScoreComponents = parsedComponents && typeof parsedComponents === 'object' ? parsedComponents : {};
    } catch {
      sigScoreComponents = {};
    }

    return {
      ...row,
      chips,
      missingMetrics,
      sigScoreComponents,
      chipsJson: undefined,
      missingMetricsJson: undefined,
      sigScoreComponentsJson: undefined,
    };
  });
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
  const dbPath = resolveDbPath(env);
  ensureDbDirectory(dbPath);

  const thresholds = getThresholds(env);
  const filters = parseHistoricalFilters(rawQuery);
  const requiredMetrics = getRequiredMetricsForQuery(filters);

  let readDb;
  try {
    readDb = new Database(dbPath);
    ensureSchema(readDb);
  } catch (error) {
    return {
      status: 503,
      error: {
        code: 'db_unavailable',
        message: `Historical DB is not available at ${dbPath}: ${error.message}`,
      },
    };
  }

  let sync = {
    synced: false,
    reason: null,
    fetchedRows: 0,
    upsertedRows: 0,
    cachedRows: 0,
  };

  const dayCache = getDayCache(readDb, { symbol, dayIso: fromDay });
  const cachedRows = dayCache ? dayCache.rowCount : countCachedRows(readDb, { from, to, symbol });
  sync.cachedRows = cachedRows;

  if (!dayCache || dayCache.cacheStatus !== DAY_CACHE_STATUS_FULL) {
    try {
      sync = await syncThetaTradesToSqlite({
        symbol,
        dayIso: fromDay,
        env,
        db: readDb,
        markPartial: hasExplicitLimit,
      });
    } catch (error) {
      upsertDayCache(readDb, {
        symbol,
        dayIso: fromDay,
        cacheStatus: DAY_CACHE_STATUS_PARTIAL,
        rowCount: cachedRows,
        lastError: error.message,
      });
      return {
        status: 502,
        error: {
          code: 'thetadata_sync_failed',
          message: error.message,
        },
      };
    }

    if (!sync.synced) {
      return {
        status: 503,
        error: {
          code: 'thetadata_not_configured',
          message: 'THETADATA_BASE_URL is required to fetch real historical trades.',
        },
      };
    }
  } else {
    sync.reason = 'day_cache_full';
    sync.cacheStatus = DAY_CACHE_STATUS_FULL;
    sync.cachedRows = dayCache.rowCount;
  }

  let enrichment;
  try {
    enrichment = await ensureEnrichedForDay({
      db: readDb,
      symbol,
      dayIso: fromDay,
      forceRecompute: sync.synced,
      markPartial: hasExplicitLimit,
      thresholds,
      env,
      requiredMetrics,
    });
  } catch (error) {
    METRIC_NAMES.forEach((metricName) => {
      upsertMetricCache(readDb, {
        symbol,
        dayIso: fromDay,
        metricName,
        cacheStatus: DAY_CACHE_STATUS_PARTIAL,
        rowCount: 0,
        lastError: error.message,
      });
    });

    return {
      status: 500,
      error: {
        code: 'enrichment_failed',
        message: error.message,
      },
    };
  }

  let metricCacheMap = enrichment.metricCacheMap || getMetricCacheMap(readDb, { symbol, dayIso: fromDay });
  let metricUnavailable = buildMetricUnavailableError(requiredMetrics, metricCacheMap);
  if (
    metricUnavailable
    && requiredMetrics.length > 0
    && !enrichment.synced
    && enrichment.reason !== 'metric_cache_partial'
  ) {
    enrichment = await ensureEnrichedForDay({
      db: readDb,
      symbol,
      dayIso: fromDay,
      forceRecompute: true,
      markPartial: hasExplicitLimit,
      thresholds,
      env,
      requiredMetrics,
    });
    metricCacheMap = enrichment.metricCacheMap || getMetricCacheMap(readDb, { symbol, dayIso: fromDay });
    metricUnavailable = buildMetricUnavailableError(requiredMetrics, metricCacheMap);
  }

  if (metricUnavailable) {
    readDb.close();
    return metricUnavailable;
  }

  try {
    const allRows = readEnrichedRows(readDb, { symbol, dayIso: fromDay });

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
        source: 'sqlite',
        dbPath,
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
          synced: enrichment.synced,
          reason: enrichment.reason,
          rowCount: enrichment.rowCount,
          ruleVersion: enrichment.ruleVersion || null,
          scoringModel: enrichment.scoringModel || null,
          targetHorizon: enrichment.targetHorizon || null,
          supplementalCache: enrichment.supplementalCache || null,
        },
      },
    };
  } catch (error) {
    return {
      status: 500,
      error: {
        code: 'query_failed',
        message: error.message,
      },
    };
  } finally {
    readDb.close();
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
    resolveThetaSpotEndpoint,
    resolveThetaOiEndpoint,
    resolveThetaOiBulkEndpoint,
    extractMetricFromResponse,
    fetchThetaMetricNumber,
    fetchThetaRows,
    buildSupplementalMetricLookup,
    countCachedRows,
    upsertDayCache,
    getDayCache,
    upsertMetricCache,
    getMetricCacheMap,
    ensureEnrichedForDay,
    buildEnrichedRows,
    buildMinuteStats,
    buildMinuteDerivedRollups,
    upsertSymbolMinuteDerived,
    upsertContractMinuteDerived,
    evaluateChips,
    DAY_CACHE_STATUS_FULL,
    DAY_CACHE_STATUS_PARTIAL,
    METRIC_NAMES,
    CHIP_DEFINITIONS,
  },
};
