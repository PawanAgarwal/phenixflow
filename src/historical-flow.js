const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');

const {
  CHIP_DEFINITIONS,
  getThresholds,
} = require('./historical-filter-definitions');
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
} = require('./historical-formulas');
const { loadReferenceOiMap } = require('./oi-gov');

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const DAY_CACHE_STATUS_FULL = 'full';
const DAY_CACHE_STATUS_PARTIAL = 'partial';
const DEFAULT_HISTORICAL_OPTION_PATH = '/v3/option/history/trade_quote';
const DEFAULT_SPOT_PATH = '/v3/stock/history/ohlc';
const DEFAULT_OI_PATH = '/v3/option/history/open_interest';
const DEFAULT_THETADATA_TIMEOUT_MS = 15000;

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

    const cacheStatus = markPartial ? DAY_CACHE_STATUS_PARTIAL : DAY_CACHE_STATUS_FULL;

    upsertDayCache(writeDb, {
      symbol,
      dayIso,
      cacheStatus,
      rowCount,
      lastError: null,
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
    const execution = computeExecutionFlags(row);
    const sentiment = computeSentiment({ right: row.right, executionSide: execution.executionSide });

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

function buildSideKey(row, executionSide) {
  return [row.symbol, row.expiration, row.strike, row.right, executionSide].join('|');
}

function clamp01(value) {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

function sideConfidence(executionSide) {
  if (executionSide === 'AA') return 1;
  if (executionSide === 'ASK') return 0.85;
  if (executionSide === 'BID') return 0.7;
  return 0.25;
}

function evaluateChips(row, thresholds) {
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

  if (row.sigScore !== null && row.sigScore >= thresholds.highSigMin) chips.push('high-sig');

  if (row.value !== null && row.value >= thresholds.premium100kMin
    && row.volOiRatio !== null && row.volOiRatio >= thresholds.unusualVolOiMin) {
    chips.push('unusual');
  }

  if ((row.repeat3m !== null && row.repeat3m >= thresholds.repeatFlowMin)
    || (row.value !== null && row.value >= thresholds.premiumSizableMin
      && row.dte !== null && row.dte <= 14
      && row.volOiRatio !== null && row.volOiRatio >= thresholds.urgentVolOiMin)) {
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
      volOiSum: 0,
      volOiCount: 0,
      maxVolOiRatio: null,
      maxRepeat3m: null,
      oiSum: 0,
      dayVolumeSum: 0,
      chipHits: {},
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
    avgVolOiRatio: row.volOiCount ? (row.volOiSum / row.volOiCount) : null,
    maxVolOiRatio: row.maxVolOiRatio,
    maxRepeat3m: row.maxRepeat3m,
    oiSum: row.oiSum,
    dayVolumeSum: row.dayVolumeSum,
    chipHitsJson: JSON.stringify(row.chipHits),
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
      avg_vol_oi_ratio,
      max_vol_oi_ratio,
      max_repeat3m,
      oi_sum,
      day_volume_sum,
      chip_hits_json,
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
      @avgVolOiRatio,
      @maxVolOiRatio,
      @maxRepeat3m,
      @oiSum,
      @dayVolumeSum,
      @chipHitsJson,
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
      avg_vol_oi_ratio = excluded.avg_vol_oi_ratio,
      max_vol_oi_ratio = excluded.max_vol_oi_ratio,
      max_repeat3m = excluded.max_repeat3m,
      oi_sum = excluded.oi_sum,
      day_volume_sum = excluded.day_volume_sum,
      chip_hits_json = excluded.chip_hits_json,
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
  requiredMetrics = [],
}) {
  const spotBySymbol = new Map();
  const oiByContract = loadContractOiFromStats(db, { symbol, dayIso });
  let oiDefaultsToZero = false;
  const referenceOiByContract = loadReferenceOiMap(db, { symbol, asOfDate: dayIso });

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

  const requiredMetricSet = new Set(Array.isArray(requiredMetrics) ? requiredMetrics : []);
  const requiresSpot = requiredMetricSet.has('spot') || requiredMetricSet.has('otmPct');
  const requiresOi = requiredMetricSet.has('oi') || requiredMetricSet.has('volOiRatio');

  if ((env.THETADATA_BASE_URL || '').trim()) {
    const shouldFetchSpot = Boolean((env.THETADATA_SPOT_PATH || DEFAULT_SPOT_PATH || '').trim());
    const shouldFetchOi = Boolean((env.THETADATA_OI_PATH || DEFAULT_OI_PATH || '').trim());

    if (shouldFetchSpot && requiresSpot) {
      const symbolsMissingSpot = Array.from(new Set(rawRows.map((row) => row.symbol)))
        .filter((rowSymbol) => !spotBySymbol.has(rowSymbol));

      for (const rowSymbol of symbolsMissingSpot) {
        const spotEndpoint = resolveThetaSpotEndpoint(rowSymbol, dayIso, env);
        const spot = await fetchThetaMetricNumber(spotEndpoint, [
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
        if (spot !== null) {
          spotBySymbol.set(rowSymbol, spot);
        }
      }
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

      for (const row of contractsMissingOi) {
        const contractKey = buildContractKey(row);
        if (oiByContract.has(contractKey)) continue;
        const oiEndpoint = resolveThetaOiEndpoint(row, dayIso, env);
        const oi = await fetchThetaMetricNumber(oiEndpoint, [
          'oi',
          'open_interest',
          'openInterest',
        ]);
        if (oi !== null) {
          oiByContract.set(contractKey, Math.trunc(oi));
        }
      }
    }
  }

  return { spotBySymbol, oiByContract, oiDefaultsToZero };
}

function buildEnrichedRows(rawRows, thresholds, supplementalMetrics = {}) {
  const spotBySymbol = supplementalMetrics.spotBySymbol || new Map();
  const oiByContract = supplementalMetrics.oiByContract || new Map();
  const oiDefaultsToZero = Boolean(supplementalMetrics.oiDefaultsToZero);
  const statsByMinute = buildMinuteStats(rawRows);

  const contractDayVolume = new Map();
  const contractStatsMap = new Map();
  const sideWindows = new Map();

  const valueSamples = rawRows
    .map((row) => computeValue(row.price, row.size))
    .filter((value) => value !== null)
    .sort((a, b) => a - b);

  const minValue = valueSamples.length ? valueSamples[0] : 0;
  const maxValue = valueSamples.length ? valueSamples[valueSamples.length - 1] : 0;

  const enrichedRows = [];

  rawRows.forEach((row) => {
    const execution = computeExecutionFlags(row);
    const sentiment = computeSentiment({ right: row.right, executionSide: execution.executionSide });

    const contractKey = buildContractKey(row);
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
    const spot = computeSpot(payload) ?? spotBySymbol.get(row.symbol) ?? null;
    const otmPct = computeOtmPct({ right: row.right, strike: row.strike, spot });
    const value = computeValue(row.price, row.size);
    const dte = computeDte(row.tradeTsUtc, row.expiration);
    const oiCandidate = extractOi(payload) ?? oiByContract.get(contractKey) ?? null;
    const oi = (oiCandidate === null && oiDefaultsToZero) ? 0 : oiCandidate;
    const volOiRatio = oi === null ? null : (dayVolume / Math.max(oi, 1));

    const valuePctile = value === null || maxValue === minValue
      ? (value === null ? 0 : 1)
      : ((value - minValue) / (maxValue - minValue));

    const volOiNorm = volOiRatio === null ? 0 : clamp01(volOiRatio / 5);
    const repeatNorm = clamp01(repeat3m / Math.max(1, thresholds.repeatFlowMin));
    const otmNorm = otmPct === null ? 0 : clamp01(Math.abs(otmPct) / 25);

    const sigScore = computeSigScore({
      valuePctile,
      volOiNorm,
      repeatNorm,
      otmNorm,
      sideConfidence: sideConfidence(execution.executionSide),
    });

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
      chips: [],
      ruleVersion: 'historical-v1',
    };

    enriched.chips = evaluateChips(enriched, thresholds);

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
  const built = buildEnrichedRows(rawRows, thresholds, supplementalMetrics);

  const upsertPayload = built.rows.map((row) => ({
    ...row,
    chipsJson: JSON.stringify(row.chips),
  }));

  upsertEnrichedRows(db, upsertPayload);
  upsertContractStats(db, built.contractStatsMap, dayIso);
  upsertSymbolStats(db, symbol, built.statsByMinute);
  const minuteRollups = buildMinuteDerivedRollups(built.rows, dayIso);
  upsertSymbolMinuteDerived(db, minuteRollups.symbolMinuteRows);
  upsertContractMinuteDerived(db, minuteRollups.contractMinuteRows);

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

    return {
      ...row,
      chips,
      chipsJson: undefined,
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
      sentiment: row.sentiment,
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
