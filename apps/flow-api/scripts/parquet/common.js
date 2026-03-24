const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const parquet = require('parquetjs-lite');

const {
  readUniverseEntries,
} = require('../../config/instrument-universe');
const {
  normalizeRight,
  toMinuteBucketUtc,
} = require('../../historical-formulas');
const {
  __private: historicalPrivate,
} = require('../../historical-flow');

const DEFAULT_PARQUET_ROOT = path.join(os.homedir(), 'Library', 'Caches', 'phenixflow', 'parquet');
const DEFAULT_SYMBOL_FILE = path.resolve(__dirname, '..', '..', 'config', 'top100-universe.json');
const DEFAULT_INDEX_GREEKS_SYMBOLS = Object.freeze(['SPX', 'SPXW', 'SPY', 'QQQ', 'VIX', 'VIXW', 'RUT', 'RUTW', 'XSP']);
const DEFAULT_THETADATA_BASE_URL = 'http://127.0.0.1:25503';
const DEFAULT_TIMEOUT_MS = 120000;
const DEFAULT_STREAM_IDLE_TIMEOUT_MS = 1800000;
const DEFAULT_CALENDAR_TIMEOUT_MS = 30000;
const DEFAULT_LARGE_SYMBOLS = new Set(DEFAULT_INDEX_GREEKS_SYMBOLS);
const DEFAULT_LARGE_SYMBOL_WINDOW_MINUTES = 60;
const DEFAULT_GREEKS_WINDOW_MINUTES_MIN = 15;
const DEFAULT_GREEKS_WINDOW_MINUTES_MAX = 391;
const DEFAULT_GREEKS_ADAPTIVE_LOW_EXPIRATIONS = 120;
const DEFAULT_GREEKS_ADAPTIVE_HIGH_EXPIRATIONS = 400;
const DEFAULT_GREEKS_ADAPTIVE_VERY_HIGH_EXPIRATIONS = 800;
const DEFAULT_GREEKS_IV_LOW = 0.0005;
const DEFAULT_GREEKS_IV_HIGH = 5.0;
const DEFAULT_GREEKS_IV_ITERATIONS = 30;
const DEFAULT_GREEKS_DIVIDEND_YIELD = 0.0;
const DEFAULT_GREEKS_FALLBACK_RATE = 0.0;
const DEFAULT_CALENDAR_PATH = '/v3/calendar/on_date';
const DEFAULT_CHUNK_LOG_EVERY = 250000;

const HEAVY_SYMBOL_WEIGHTS = new Map([
  ['SPXW', 64],
  ['SPX', 60],
  ['SPY', 40],
  ['QQQ', 36],
  ['RUTW', 34],
  ['RUT', 30],
  ['TSLA', 28],
  ['VIXW', 26],
  ['VIX', 24],
  ['NVDA', 24],
  ['AAPL', 22],
  ['XSP', 20],
  ['META', 20],
  ['AMD', 18],
  ['AMZN', 16],
  ['MSFT', 16],
  ['COIN', 14],
  ['PLTR', 14],
  ['MU', 12],
  ['NFLX', 12],
]);

function withCompression(fields) {
  return Object.fromEntries(
    Object.entries(fields).map(([key, definition]) => [key, { compression: 'SNAPPY', ...definition }]),
  );
}

const RAW_STOCK_SCHEMA = new parquet.ParquetSchema(withCompression({
  symbol: { type: 'UTF8' },
  trade_date_utc: { type: 'UTF8' },
  minute_bucket_utc: { type: 'UTF8' },
  open: { type: 'DOUBLE', optional: true },
  high: { type: 'DOUBLE', optional: true },
  low: { type: 'DOUBLE', optional: true },
  close: { type: 'DOUBLE', optional: true },
  volume: { type: 'DOUBLE', optional: true },
  source_endpoint: { type: 'UTF8', optional: true },
  raw_payload_json: { type: 'UTF8', optional: true },
}));

const RAW_QUOTE_SCHEMA = new parquet.ParquetSchema(withCompression({
  symbol: { type: 'UTF8' },
  trade_date_utc: { type: 'UTF8' },
  expiration: { type: 'UTF8' },
  strike: { type: 'DOUBLE' },
  option_right: { type: 'UTF8' },
  minute_bucket_utc: { type: 'UTF8' },
  bid: { type: 'DOUBLE', optional: true },
  ask: { type: 'DOUBLE', optional: true },
  last: { type: 'DOUBLE', optional: true },
  bid_size: { type: 'INT32', optional: true },
  ask_size: { type: 'INT32', optional: true },
  source_endpoint: { type: 'UTF8', optional: true },
  raw_payload_json: { type: 'UTF8', optional: true },
}));

const RAW_GREEKS_SCHEMA = new parquet.ParquetSchema(withCompression({
  symbol: { type: 'UTF8' },
  trade_date_utc: { type: 'UTF8' },
  expiration: { type: 'UTF8' },
  strike: { type: 'DOUBLE' },
  option_right: { type: 'UTF8' },
  minute_bucket_utc: { type: 'UTF8' },
  delta: { type: 'DOUBLE', optional: true },
  implied_vol: { type: 'DOUBLE', optional: true },
  gamma: { type: 'DOUBLE', optional: true },
  theta: { type: 'DOUBLE', optional: true },
  vega: { type: 'DOUBLE', optional: true },
  rho: { type: 'DOUBLE', optional: true },
  underlying_price: { type: 'DOUBLE', optional: true },
  source_endpoint: { type: 'UTF8', optional: true },
  raw_payload_json: { type: 'UTF8', optional: true },
}));

const FINAL_GREEKS_SCHEMA = new parquet.ParquetSchema(withCompression({
  symbol: { type: 'UTF8' },
  trade_date_utc: { type: 'UTF8' },
  expiration: { type: 'UTF8' },
  strike: { type: 'DOUBLE' },
  option_right: { type: 'UTF8' },
  minute_bucket_utc: { type: 'UTF8' },
  bid: { type: 'DOUBLE', optional: true },
  ask: { type: 'DOUBLE', optional: true },
  last: { type: 'DOUBLE', optional: true },
  mid_price: { type: 'DOUBLE', optional: true },
  underlying_price: { type: 'DOUBLE', optional: true },
  risk_free_rate: { type: 'DOUBLE', optional: true },
  dividend_yield: { type: 'DOUBLE', optional: true },
  time_to_expiry_years: { type: 'DOUBLE', optional: true },
  implied_vol: { type: 'DOUBLE', optional: true },
  delta: { type: 'DOUBLE', optional: true },
  gamma: { type: 'DOUBLE', optional: true },
  theta_annual: { type: 'DOUBLE', optional: true },
  theta_per_day: { type: 'DOUBLE', optional: true },
  vega_annual: { type: 'DOUBLE', optional: true },
  vega_per_1pct: { type: 'DOUBLE', optional: true },
  rho_annual: { type: 'DOUBLE', optional: true },
  rho_per_1pct: { type: 'DOUBLE', optional: true },
  model_price: { type: 'DOUBLE', optional: true },
  price_error_abs: { type: 'DOUBLE', optional: true },
  iv_low: { type: 'DOUBLE', optional: true },
  iv_high: { type: 'DOUBLE', optional: true },
  calc_status: { type: 'UTF8' },
  calc_run_id: { type: 'UTF8' },
  calc_version: { type: 'UTF8' },
  source_mode: { type: 'UTF8' },
  source_endpoint: { type: 'UTF8', optional: true },
  ingested_at_utc: { type: 'UTF8' },
}));

function ensureDir(target) {
  fs.mkdirSync(target, { recursive: true });
}

function nowIso() {
  return new Date().toISOString();
}

function buildRunId(prefix = 'parquet-greeks') {
  return `${prefix}-${new Date().toISOString().replace(/[-:.]/g, '').slice(0, 15)}Z`;
}

function resolveThetaBaseUrl(env = process.env) {
  return String(env.THETADATA_BASE_URL || DEFAULT_THETADATA_BASE_URL).trim().replace(/\/$/, '');
}

function resolveParquetRoot(env = process.env) {
  const configured = String(env.PHENIXFLOW_PARQUET_ROOT || '').trim();
  return configured ? path.resolve(configured) : DEFAULT_PARQUET_ROOT;
}

function resolveRunRoot(runId, env = process.env) {
  return path.join(resolveParquetRoot(env), 'runs', runId);
}

function resolveLayout(runRoot) {
  return {
    runRoot,
    datasetsRoot: path.join(runRoot, 'datasets'),
    manifestsRoot: path.join(runRoot, 'manifests'),
    reportsRoot: path.join(runRoot, 'reports'),
    logsRoot: path.join(runRoot, 'logs'),
    rawStockRoot: path.join(runRoot, 'datasets', 'raw', 'stock_ohlc_minute'),
    rawQuoteRoot: path.join(runRoot, 'datasets', 'raw', 'option_quote_minute'),
    rawGreeksRoot: path.join(runRoot, 'datasets', 'raw', 'option_greeks_minute'),
    finalGreeksRoot: path.join(runRoot, 'datasets', 'derived', 'option_greeks_minute'),
  };
}

function ensureRunLayout(runRoot) {
  const layout = resolveLayout(runRoot);
  Object.values(layout).forEach((target) => ensureDir(target));
  return layout;
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

function toYyyymmdd(isoTs) {
  const d = new Date(isoTs);
  return `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, '0')}${String(d.getUTCDate()).padStart(2, '0')}`;
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
        header.forEach((key, index) => {
          out[key] = values[index];
        });
        return out;
      });
  }
  if (parsed && typeof parsed === 'object') {
    const entries = Object.entries(parsed).filter(([, value]) => Array.isArray(value));
    if (entries.length > 0) {
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
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toInteger(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
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

function normalizeThetaTimestamp(rawTs) {
  if (!rawTs || typeof rawTs !== 'string') return null;
  return /[zZ]|[+-]\d\d:\d\d$/.test(rawTs) ? rawTs : `${rawTs}Z`;
}

function parseBooleanLike(value, fallback = false) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (raw === '1' || raw === 'true' || raw === 'yes') return true;
  if (raw === '0' || raw === 'false' || raw === 'no') return false;
  return fallback;
}

function parseNumberEnv(envKey, fallback, env = process.env) {
  const parsed = Number(env[envKey]);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseIndexGreeksSymbols(env = process.env) {
  const raw = String(env.PARQUET_INDEX_GREEKS_SYMBOLS || env.INDEX_GREEKS_SYMBOLS || DEFAULT_INDEX_GREEKS_SYMBOLS.join(',')).trim();
  return new Set(raw.split(',').map((token) => normalizeSymbol(token)).filter(Boolean));
}

function parseLargeSymbols(env = process.env) {
  const raw = String(env.THETADATA_LARGE_SYMBOLS || Array.from(DEFAULT_LARGE_SYMBOLS).join(',')).trim();
  const includeAll = raw === '*' || raw.toLowerCase() === 'all';
  return {
    includeAll,
    symbols: includeAll ? new Set() : new Set(raw.split(',').map((token) => normalizeSymbol(token)).filter(Boolean)),
  };
}

function parseLargeSymbolWindowMinutes(env = process.env) {
  const parsed = Number(env.THETADATA_LARGE_SYMBOL_WINDOW_MINUTES);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(24 * 60, Math.trunc(parsed)));
  }
  return DEFAULT_LARGE_SYMBOL_WINDOW_MINUTES;
}

function parseThetaGreeksWindowMinutes(env = process.env) {
  const parsed = Number(env.THETADATA_GREEKS_WINDOW_MINUTES);
  if (Number.isFinite(parsed) && parsed === 0) return 0;
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(24 * 60, Math.trunc(parsed)));
  }
  return parseLargeSymbolWindowMinutes(env);
}

function parseThetaGreeksWindowMinMinutes(env = process.env) {
  const parsed = Number(env.THETADATA_GREEKS_WINDOW_MIN_MINUTES);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(24 * 60, Math.trunc(parsed)));
  }
  return DEFAULT_GREEKS_WINDOW_MINUTES_MIN;
}

function parseThetaGreeksWindowMaxMinutes(env = process.env) {
  const parsed = Number(env.THETADATA_GREEKS_WINDOW_MAX_MINUTES);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(24 * 60, Math.trunc(parsed)));
  }
  return DEFAULT_GREEKS_WINDOW_MINUTES_MAX;
}

function parseThetaGreeksAdaptiveExpirationThresholds(env = process.env) {
  const low = Math.max(0, Math.trunc(parseNumberEnv('THETADATA_GREEKS_ADAPTIVE_LOW_EXPIRATIONS', DEFAULT_GREEKS_ADAPTIVE_LOW_EXPIRATIONS, env)));
  const high = Math.max(1, Math.trunc(parseNumberEnv('THETADATA_GREEKS_ADAPTIVE_HIGH_EXPIRATIONS', DEFAULT_GREEKS_ADAPTIVE_HIGH_EXPIRATIONS, env)));
  const veryHigh = Math.max(high, Math.trunc(parseNumberEnv('THETADATA_GREEKS_ADAPTIVE_VERY_HIGH_EXPIRATIONS', DEFAULT_GREEKS_ADAPTIVE_VERY_HIGH_EXPIRATIONS, env)));
  return { low, high, veryHigh };
}

function resolveThetaGreeksAdaptiveWindowMinutes({ symbol, expirationCount = 0, env = process.env } = {}) {
  const minWindowMinutes = parseThetaGreeksWindowMinMinutes(env);
  const maxWindowMinutes = parseThetaGreeksWindowMaxMinutes(env);
  const boundedMin = Math.max(1, Math.min(minWindowMinutes, maxWindowMinutes));
  const boundedMax = Math.max(boundedMin, maxWindowMinutes);
  const configuredBaseWindow = parseThetaGreeksWindowMinutes(env);
  const boundedBase = configuredBaseWindow <= 0
    ? 0
    : Math.max(boundedMin, Math.min(boundedMax, configuredBaseWindow));
  const baseWindow = boundedBase === 0
    ? Math.max(boundedMin, Math.min(boundedMax, parseLargeSymbolWindowMinutes(env)))
    : boundedBase;
  if (!parseBooleanLike(env.THETADATA_GREEKS_ADAPTIVE_WINDOWS, true)) {
    return { symbol: normalizeSymbol(symbol), expirationCount, windowMinutes: baseWindow, mode: 'fixed' };
  }
  const normalizedExpirationCount = Math.max(0, Math.trunc(Number(expirationCount) || 0));
  const thresholds = parseThetaGreeksAdaptiveExpirationThresholds(env);
  let mode = 'adaptive_base';
  let windowMinutes = baseWindow;
  if (normalizedExpirationCount >= thresholds.veryHigh) {
    mode = 'adaptive_very_high';
    windowMinutes = boundedMin;
  } else if (normalizedExpirationCount >= thresholds.high) {
    mode = 'adaptive_high';
    windowMinutes = Math.max(boundedMin, Math.min(boundedMax, Math.trunc(baseWindow / 2) || boundedMin));
  } else if (normalizedExpirationCount <= thresholds.low) {
    mode = 'adaptive_low';
    windowMinutes = boundedMax;
  }
  return { symbol: normalizeSymbol(symbol), expirationCount: normalizedExpirationCount, windowMinutes, mode };
}

function parseTimeHmsToSecondOfDay(rawValue) {
  if (typeof rawValue !== 'string') return null;
  const match = rawValue.trim().match(/^(\d{2}):(\d{2}):(\d{2})$/);
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  const seconds = Number(match[3]);
  if (
    !Number.isInteger(hours) || !Number.isInteger(minutes) || !Number.isInteger(seconds)
    || hours < 0 || hours > 23
    || minutes < 0 || minutes > 59
    || seconds < 0 || seconds > 59
  ) return null;
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

function resolveThetaTimeWindowsForSymbol(symbol, {
  startTime = null,
  sessionStartTime = null,
  sessionEndTime = null,
  windowMinutes = null,
  forceWindowing = false,
  env = process.env,
} = {}) {
  const sessionStartSecond = parseTimeHmsToSecondOfDay(sessionStartTime);
  const sessionEndSecondRaw = parseTimeHmsToSecondOfDay(sessionEndTime);
  const hasSessionBounds = sessionStartSecond !== null || sessionEndSecondRaw !== null;
  const lowerBound = sessionStartSecond === null ? 0 : sessionStartSecond;
  const upperBound = sessionEndSecondRaw === null ? 86399 : Math.max(lowerBound, sessionEndSecondRaw);
  const parsedStartSecond = startTime ? parseTimeHmsToSecondOfDay(startTime) : null;
  if (startTime && parsedStartSecond === null) {
    return [{ startTime, endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null }];
  }
  let startSecond = parsedStartSecond;
  if (startSecond === null && hasSessionBounds) startSecond = lowerBound;
  if (startSecond !== null) startSecond = Math.max(lowerBound, Math.min(startSecond, upperBound));
  const hasExplicitWindowMinutes = !(windowMinutes === null || windowMinutes === undefined || String(windowMinutes).trim() === '');
  const parsedWindowMinutes = hasExplicitWindowMinutes ? Number(windowMinutes) : Number.NaN;
  const normalizedWindowMinutes = Number.isFinite(parsedWindowMinutes)
    ? Math.max(0, Math.min(24 * 60, Math.trunc(parsedWindowMinutes)))
    : parseLargeSymbolWindowMinutes(env);
  if (normalizedWindowMinutes <= 0) {
    return [{
      startTime: startSecond === null ? startTime || null : formatSecondOfDayAsHms(startSecond),
      endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null,
    }];
  }
  const normalizedSymbol = normalizeSymbol(symbol);
  const largeSymbols = parseLargeSymbols(env);
  if (!forceWindowing && !largeSymbols.includeAll && (!normalizedSymbol || !largeSymbols.symbols.has(normalizedSymbol))) {
    return [{
      startTime: startSecond === null ? startTime || null : formatSecondOfDayAsHms(startSecond),
      endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null,
    }];
  }
  const effectiveStartSecond = startSecond === null ? lowerBound : startSecond;
  const windowSeconds = normalizedWindowMinutes * 60;
  const spanSeconds = (upperBound - effectiveStartSecond) + 1;
  if (windowSeconds >= spanSeconds) {
    return [{
      startTime: formatSecondOfDayAsHms(effectiveStartSecond),
      endTime: hasSessionBounds ? formatSecondOfDayAsHms(upperBound) : null,
    }];
  }
  const windows = [];
  for (let cursor = effectiveStartSecond; cursor <= upperBound; cursor += windowSeconds) {
    const windowEnd = Math.min(upperBound, cursor + windowSeconds - 1);
    windows.push({
      startTime: formatSecondOfDayAsHms(cursor),
      endTime: formatSecondOfDayAsHms(windowEnd),
    });
  }
  return windows;
}

function parseCalendarSessionWindow(rawBody, env = process.env) {
  const rows = parseJsonRows(rawBody);
  const row = rows.find((entry) => entry && typeof entry === 'object') || null;
  if (!row) return null;
  const sessionType = String(row.type || '').trim().toLowerCase();
  const isTradable = sessionType === 'open' || sessionType === 'early_close';
  if (sessionType && !isTradable) {
    return { isOpen: false, type: sessionType, openTime: null, closeTime: null, regularCloseTime: null };
  }
  if (!isTradable) return null;
  const openSecond = parseTimeHmsToSecondOfDay(typeof row.open === 'string' ? row.open : null);
  const closeSecond = parseTimeHmsToSecondOfDay(typeof row.close === 'string' ? row.close : null);
  if (openSecond === null || closeSecond === null) {
    return { isOpen: true, type: sessionType, openTime: null, closeTime: null, regularCloseTime: null };
  }
  const closePadMinutes = Math.max(0, Math.min(240, Math.trunc(parseNumberEnv('THETADATA_CALENDAR_CLOSE_PAD_MINUTES', 15, env))));
  const paddedCloseSecond = Math.min(86399, closeSecond + (closePadMinutes * 60));
  return {
    isOpen: true,
    type: sessionType,
    openTime: formatSecondOfDayAsHms(openSecond),
    closeTime: formatSecondOfDayAsHms(Math.max(openSecond, paddedCloseSecond)),
    regularCloseTime: formatSecondOfDayAsHms(Math.max(openSecond, closeSecond)),
  };
}

async function fetchTextWithTimeout(url, {
  env = process.env,
  timeoutMs = parseNumberEnv('THETADATA_TIMEOUT_MS', DEFAULT_TIMEOUT_MS, env),
} = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const startedAtMs = Date.now();
  try {
    const response = await fetch(url, { signal: controller.signal });
    const body = await response.text();
    return {
      response,
      body,
      durationMs: Date.now() - startedAtMs,
      bytesDownloaded: Buffer.byteLength(body),
    };
  } finally {
    clearTimeout(timer);
  }
}

async function fetchNdjsonRows(url, {
  env = process.env,
  timeoutMs = parseNumberEnv('THETADATA_STREAM_IDLE_TIMEOUT_MS', DEFAULT_STREAM_IDLE_TIMEOUT_MS, env),
  onRow,
} = {}) {
  const controller = new AbortController();
  let timer = null;
  let timeoutKind = null;
  const startedAtMs = Date.now();
  const resetIdleTimer = () => {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => {
      timeoutKind = 'idle';
      controller.abort();
    }, timeoutMs);
  };
  try {
    resetIdleTimer();
    const response = await fetch(url, { signal: controller.signal });
    if (!response.ok) {
      const body = await response.text();
      return {
        response,
        rowCount: 0,
        durationMs: Date.now() - startedAtMs,
        bytesDownloaded: Buffer.byteLength(body),
      };
    }
    if (!response.body || typeof response.body.getReader !== 'function') {
      const body = await response.text();
      const rows = parseJsonRows(body);
      for (const row of rows) {
        const maybePromise = onRow(row);
        if (maybePromise && typeof maybePromise.then === 'function') {
          await maybePromise;
        }
      }
      return {
        response,
        rowCount: rows.length,
        durationMs: Date.now() - startedAtMs,
        bytesDownloaded: Buffer.byteLength(body),
      };
    }
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let rowCount = 0;
    let bytesDownloaded = 0;
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value && value.length > 0) {
        bytesDownloaded += value.length;
        resetIdleTimer();
      }
      buffer += decoder.decode(value, { stream: true });
      let newlineIndex = buffer.indexOf('\n');
      while (newlineIndex >= 0) {
        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (line) {
          const parsed = JSON.parse(line);
          const maybePromise = onRow(parsed);
          if (maybePromise && typeof maybePromise.then === 'function') {
            await maybePromise;
          }
          rowCount += 1;
        }
        newlineIndex = buffer.indexOf('\n');
      }
    }
    buffer += decoder.decode();
    const tail = buffer.trim();
    if (tail) {
      const parsed = JSON.parse(tail);
      const maybePromise = onRow(parsed);
      if (maybePromise && typeof maybePromise.then === 'function') {
        await maybePromise;
      }
      rowCount += 1;
    }
    return {
      response,
      rowCount,
      durationMs: Date.now() - startedAtMs,
      bytesDownloaded,
    };
  } catch (error) {
    if (error?.name === 'AbortError') {
      throw new Error(timeoutKind === 'idle'
        ? `thetadata_request_idle_timeout:${timeoutMs}`
        : `thetadata_request_timeout:${timeoutMs}`);
    }
    throw error;
  } finally {
    if (timer) clearTimeout(timer);
  }
}

async function fetchCalendarSessionWindow(dayIso, env = process.env) {
  const baseUrl = resolveThetaBaseUrl(env);
  const calendarPath = String(env.THETADATA_CALENDAR_PATH || DEFAULT_CALENDAR_PATH).trim();
  const normalizedPath = calendarPath.startsWith('/') ? calendarPath : `/${calendarPath}`;
  const url = `${baseUrl}${normalizedPath}?date=${toYyyymmdd(`${dayIso}T00:00:00.000Z`)}&format=json`;
  const { response, body } = await fetchTextWithTimeout(url, {
    env,
    timeoutMs: parseNumberEnv('THETADATA_CALENDAR_TIMEOUT_MS', DEFAULT_CALENDAR_TIMEOUT_MS, env),
  });
  if (!response.ok) {
    throw new Error(`calendar_http_${response.status}:${dayIso}`);
  }
  return parseCalendarSessionWindow(body, env);
}

async function listOpenDaysInRange(startIso, endIso, env = process.env) {
  const out = [];
  let cursor = new Date(`${startIso}T00:00:00.000Z`);
  const end = new Date(`${endIso}T00:00:00.000Z`);
  while (cursor <= end) {
    const dayIso = cursor.toISOString().slice(0, 10);
    const dayOfWeek = cursor.getUTCDay();
    if (dayOfWeek >= 1 && dayOfWeek <= 5) {
      try {
        const session = await fetchCalendarSessionWindow(dayIso, env);
        if (session?.isOpen) out.push(dayIso);
      } catch {
        out.push(dayIso);
      }
    }
    cursor = new Date(cursor.getTime() + 86400000);
  }
  return out;
}

function loadUniverse({ symbolFile = DEFAULT_SYMBOL_FILE, symbolLimit = 100, extraSymbols = [] } = {}) {
  const entries = readUniverseEntries(path.resolve(symbolFile));
  const primary = symbolLimit > 0 ? entries.slice(0, symbolLimit) : entries.slice();
  const combined = [];
  const seen = new Set();
  primary.forEach((entry) => {
    if (!entry?.symbol || seen.has(entry.symbol)) return;
    seen.add(entry.symbol);
    combined.push(entry.symbol);
  });
  extraSymbols.forEach((rawSymbol) => {
    const symbol = normalizeSymbol(rawSymbol);
    if (!symbol || seen.has(symbol)) return;
    seen.add(symbol);
    combined.push(symbol);
  });
  return combined;
}

async function buildJobs({
  startDate,
  endDate,
  symbolFile = DEFAULT_SYMBOL_FILE,
  symbolLimit = 100,
  extraSymbols = [],
  env = process.env,
}) {
  const openDays = await listOpenDaysInRange(startDate, endDate, env);
  const symbols = loadUniverse({ symbolFile, symbolLimit, extraSymbols });
  const jobs = [];
  openDays.forEach((dayIso) => {
    symbols.forEach((symbol) => {
      jobs.push({ symbol, dayIso });
    });
  });
  return { openDays, symbols, jobs };
}

function estimateJobWeight(job, originalIndex, totalRows) {
  const heavyWeight = HEAVY_SYMBOL_WEIGHTS.get(job.symbol) || 0;
  const decileSize = Math.max(1, Math.ceil(totalRows / 10));
  const rankWeight = Math.max(1, Math.ceil((totalRows - originalIndex) / decileSize));
  return heavyWeight + rankWeight;
}

function shardJobsBalanced(jobs, workerTotal, workerIndex) {
  if (workerTotal <= 1) return jobs;
  const weighted = jobs.map((job, index) => ({
    job,
    index,
    weight: estimateJobWeight(job, index, jobs.length),
  }));
  weighted.sort((left, right) => {
    if (right.weight !== left.weight) return right.weight - left.weight;
    return left.index - right.index;
  });
  const buckets = Array.from({ length: workerTotal }, () => ({ load: 0, jobs: [] }));
  weighted.forEach((entry) => {
    let target = 0;
    for (let idx = 1; idx < buckets.length; idx += 1) {
      if (buckets[idx].load < buckets[target].load) {
        target = idx;
      }
    }
    buckets[target].jobs.push(entry);
    buckets[target].load += entry.weight;
  });
  return (buckets[workerIndex]?.jobs || [])
    .sort((left, right) => left.index - right.index)
    .map((entry) => entry.job);
}

function getStockPath(runRoot, symbol, dayIso) {
  return path.join(resolveLayout(runRoot).rawStockRoot, `symbol=${symbol}`, `trade_date_utc=${dayIso}`, 'part-000.parquet');
}

function getQuotePath(runRoot, symbol, dayIso) {
  return path.join(resolveLayout(runRoot).rawQuoteRoot, `symbol=${symbol}`, `trade_date_utc=${dayIso}`, 'part-000.parquet');
}

function getRawGreeksPath(runRoot, symbol, dayIso) {
  return path.join(resolveLayout(runRoot).rawGreeksRoot, `symbol=${symbol}`, `trade_date_utc=${dayIso}`, 'part-000.parquet');
}

function getFinalGreeksPath(runRoot, symbol, dayIso) {
  return path.join(resolveLayout(runRoot).finalGreeksRoot, `symbol=${symbol}`, `trade_date_utc=${dayIso}`, 'part-000.parquet');
}

async function openParquetWriter(schema, filePath) {
  ensureDir(path.dirname(filePath));
  const tempPath = `${filePath}.tmp`;
  if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
  if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
  const writer = await parquet.ParquetWriter.openFile(schema, tempPath);
  return {
    writer,
    async close(success = true) {
      await writer.close();
      if (!success) {
        if (fs.existsSync(tempPath)) fs.unlinkSync(tempPath);
        return;
      }
      fs.renameSync(tempPath, filePath);
    },
  };
}

async function appendRows(writer, rows) {
  for (const row of rows) {
    await writer.appendRow(row);
  }
}

function writeJsonFile(filePath, payload) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
}

function normalizeStockOhlcRows(rows, dayIso, { includeRawPayload = false } = {}) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  return rows.map((row) => {
    const ts = toIsoFromAnyTs(
      pickField(row, ['timestamp', 'time', 'datetime', 'trade_timestamp', 'trade_ts', 'ms_of_day']),
      fallbackTs,
    );
    const close = toNumber(pickField(row, ['close', 'c', 'last', 'price']));
    const open = toNumber(pickField(row, ['open', 'o']));
    const high = toNumber(pickField(row, ['high', 'h']));
    const low = toNumber(pickField(row, ['low', 'l']));
    const volume = toNumber(pickField(row, ['volume', 'v']));
    const minuteBucketUtc = toMinuteBucketUtc(ts);
    if (!ts || close === null || !minuteBucketUtc) return null;
    return {
      symbol: null,
      trade_date_utc: dayIso,
      minute_bucket_utc: minuteBucketUtc,
      open,
      high,
      low,
      close,
      volume,
      source_endpoint: null,
      raw_payload_json: includeRawPayload ? JSON.stringify(row) : null,
    };
  }).filter(Boolean);
}

function normalizeOptionQuoteRow(row, dayIso, { includeRawPayload = false } = {}) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  const symbol = normalizeSymbol(pickField(row, ['symbol', 'root', 'underlying']));
  const expiration = normalizeIsoDate(pickField(row, ['expiration', 'exp', 'expiration_date']));
  const strike = toNumber(pickField(row, ['strike', 'strike_price']));
  const optionRight = normalizeRight(pickField(row, ['right', 'option_right', 'side']));
  const ts = toIsoFromAnyTs(
    pickField(row, ['timestamp', 'time', 'datetime', 'quote_timestamp', 'trade_timestamp', 'trade_ts']),
    fallbackTs,
  );
  const minuteBucketUtc = toMinuteBucketUtc(ts);
  if (!symbol || !expiration || strike === null || !optionRight || !minuteBucketUtc) return null;
  return {
    symbol,
    trade_date_utc: dayIso,
    expiration,
    strike,
    option_right: optionRight,
    minute_bucket_utc: minuteBucketUtc,
    bid: toNumber(pickField(row, ['bid', 'bid_price'])),
    ask: toNumber(pickField(row, ['ask', 'ask_price'])),
    last: toNumber(pickField(row, ['last', 'price', 'mark', 'mid'])),
    bid_size: toInteger(pickField(row, ['bid_size', 'bidSize', 'bidsize'])),
    ask_size: toInteger(pickField(row, ['ask_size', 'askSize', 'asksize'])),
    source_endpoint: null,
    raw_payload_json: includeRawPayload ? JSON.stringify(row) : null,
  };
}

function normalizeOptionGreeksRow(row, dayIso, { includeRawPayload = false } = {}) {
  const fallbackTs = `${dayIso}T00:00:00.000Z`;
  const symbol = normalizeSymbol(pickField(row, ['symbol', 'root', 'underlying']));
  const expiration = normalizeIsoDate(pickField(row, ['expiration', 'exp', 'expiration_date']));
  const strike = toNumber(pickField(row, ['strike', 'strike_price']));
  const optionRight = normalizeRight(pickField(row, ['right', 'option_right', 'side']));
  const rawTs = pickField(row, ['timestamp', 'trade_timestamp', 'datetime', 'time']);
  const ts = normalizeThetaTimestamp(rawTs) || toIsoFromAnyTs(rawTs, fallbackTs);
  const minuteBucketUtc = toMinuteBucketUtc(ts);
  if (!symbol || !expiration || strike === null || !optionRight || !minuteBucketUtc) return null;
  return {
    symbol,
    trade_date_utc: dayIso,
    expiration,
    strike,
    option_right: optionRight,
    minute_bucket_utc: minuteBucketUtc,
    delta: toNumber(pickField(row, ['delta'])),
    implied_vol: toNumber(pickField(row, ['implied_vol', 'impliedVol', 'iv'])),
    gamma: toNumber(pickField(row, ['gamma'])),
    theta: toNumber(pickField(row, ['theta'])),
    vega: toNumber(pickField(row, ['vega'])),
    rho: toNumber(pickField(row, ['rho'])),
    underlying_price: toNumber(pickField(row, ['underlying_price', 'underlyingPrice', 'spot'])),
    source_endpoint: null,
    raw_payload_json: includeRawPayload ? JSON.stringify(row) : null,
  };
}

function normalizeFinalGreekFromRaw(rawRow, {
  runId,
  calcVersion = 'theta_raw_v1',
  sourceEndpoint = null,
}) {
  const thetaAnnual = rawRow.theta === null || rawRow.theta === undefined ? null : Number(rawRow.theta);
  const vegaAnnual = rawRow.vega === null || rawRow.vega === undefined ? null : Number(rawRow.vega);
  const rhoAnnual = rawRow.rho === null || rawRow.rho === undefined ? null : Number(rawRow.rho);
  return {
    symbol: rawRow.symbol,
    trade_date_utc: rawRow.trade_date_utc,
    expiration: rawRow.expiration,
    strike: Number(rawRow.strike),
    option_right: rawRow.option_right,
    minute_bucket_utc: rawRow.minute_bucket_utc,
    bid: null,
    ask: null,
    last: null,
    mid_price: null,
    underlying_price: rawRow.underlying_price ?? null,
    risk_free_rate: null,
    dividend_yield: null,
    time_to_expiry_years: null,
    implied_vol: rawRow.implied_vol ?? null,
    delta: rawRow.delta ?? null,
    gamma: rawRow.gamma ?? null,
    theta_annual: thetaAnnual,
    theta_per_day: thetaAnnual === null || !Number.isFinite(thetaAnnual) ? null : (thetaAnnual / 365.0),
    vega_annual: vegaAnnual,
    vega_per_1pct: vegaAnnual === null || !Number.isFinite(vegaAnnual) ? null : (vegaAnnual / 100.0),
    rho_annual: rhoAnnual,
    rho_per_1pct: rhoAnnual === null || !Number.isFinite(rhoAnnual) ? null : (rhoAnnual / 100.0),
    model_price: null,
    price_error_abs: null,
    iv_low: null,
    iv_high: null,
    calc_status: 'raw_source',
    calc_run_id: runId,
    calc_version: calcVersion,
    source_mode: 'raw',
    source_endpoint: sourceEndpoint,
    ingested_at_utc: nowIso(),
  };
}

function getTimeZoneOffsetMinutes(date, timeZone) {
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  });
  const byType = {};
  formatter.formatToParts(date).forEach((part) => {
    if (part.type !== 'literal') byType[part.type] = part.value;
  });
  const asUtcMs = Date.UTC(
    Number(byType.year),
    Number(byType.month) - 1,
    Number(byType.day),
    Number(byType.hour),
    Number(byType.minute),
    Number(byType.second),
  );
  return (asUtcMs - date.getTime()) / 60000;
}

function zonedTimeToUtcMs(dayIso, hour, minute, second, timeZone) {
  const [year, month, day] = dayIso.split('-').map(Number);
  const utcGuess = Date.UTC(year, month - 1, day, hour, minute, second, 0);
  const offsetMinutes = getTimeZoneOffsetMinutes(new Date(utcGuess), timeZone);
  return utcGuess - (offsetMinutes * 60000);
}

function buildExpirationCloseMs(expiration) {
  return zonedTimeToUtcMs(expiration, 16, 0, 0, 'America/New_York');
}

function erfApprox(x) {
  const sign = x < 0 ? -1 : 1;
  const absX = Math.abs(x);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;
  const t = 1 / (1 + (p * absX));
  const y = 1 - (((((a5 * t) + a4) * t + a3) * t + a2) * t + a1) * t * Math.exp(-absX * absX);
  return sign * y;
}

function normCdf(x) {
  return 0.5 * (1 + erfApprox(x / Math.sqrt(2)));
}

function normPdf(x) {
  return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI);
}

function bsD1(sigma, s, k, r, q, t) {
  return (Math.log(s / k) + ((r - q + (0.5 * sigma * sigma)) * t)) / (sigma * Math.sqrt(t));
}

function bsD2(sigma, s, k, r, q, t) {
  return bsD1(sigma, s, k, r, q, t) - (sigma * Math.sqrt(t));
}

function bsPrice(sigma, s, k, r, q, t, isCall) {
  const d1 = bsD1(sigma, s, k, r, q, t);
  const d2 = bsD2(sigma, s, k, r, q, t);
  if (isCall) {
    return (s * Math.exp(-q * t) * normCdf(d1)) - (k * Math.exp(-r * t) * normCdf(d2));
  }
  return (k * Math.exp(-r * t) * normCdf(-d2)) - (s * Math.exp(-q * t) * normCdf(-d1));
}

function bsDelta(sigma, s, k, r, q, t, isCall) {
  const d1 = bsD1(sigma, s, k, r, q, t);
  return isCall
    ? Math.exp(-q * t) * normCdf(d1)
    : Math.exp(-q * t) * (normCdf(d1) - 1);
}

function bsGamma(sigma, s, k, r, q, t) {
  return Math.exp(-q * t) * normPdf(bsD1(sigma, s, k, r, q, t)) / (s * sigma * Math.sqrt(t));
}

function bsVega(sigma, s, k, r, q, t) {
  return s * Math.exp(-q * t) * normPdf(bsD1(sigma, s, k, r, q, t)) * Math.sqrt(t);
}

function bsThetaAnnual(sigma, s, k, r, q, t, isCall) {
  const d1 = bsD1(sigma, s, k, r, q, t);
  const d2 = bsD2(sigma, s, k, r, q, t);
  const firstTerm = -((s * Math.exp(-q * t) * normPdf(d1) * sigma) / (2 * Math.sqrt(t)));
  if (isCall) {
    return firstTerm - (r * k * Math.exp(-r * t) * normCdf(d2)) + (q * s * Math.exp(-q * t) * normCdf(d1));
  }
  return firstTerm + (r * k * Math.exp(-r * t) * normCdf(-d2)) - (q * s * Math.exp(-q * t) * normCdf(-d1));
}

function bsRhoAnnual(sigma, s, k, r, q, t, isCall) {
  const d2 = bsD2(sigma, s, k, r, q, t);
  return isCall
    ? (k * t * Math.exp(-r * t) * normCdf(d2))
    : (-k * t * Math.exp(-r * t) * normCdf(-d2));
}

function computeGreeksFromQuoteRow(quoteRow, stockByMinute, {
  runId,
  calcVersion = 'bs_v1',
  riskFreeRate = DEFAULT_GREEKS_FALLBACK_RATE,
  dividendYield = DEFAULT_GREEKS_DIVIDEND_YIELD,
  ivLow = DEFAULT_GREEKS_IV_LOW,
  ivHigh = DEFAULT_GREEKS_IV_HIGH,
  ivIterations = DEFAULT_GREEKS_IV_ITERATIONS,
}) {
  const bid = toNumber(quoteRow.bid);
  const ask = toNumber(quoteRow.ask);
  const last = toNumber(quoteRow.last);
  const strike = toNumber(quoteRow.strike);
  const underlyingPrice = stockByMinute.get(quoteRow.minute_bucket_utc) ?? null;
  const midPrice = bid !== null && ask !== null && bid > 0 && ask > 0
    ? (bid + ask) / 2
    : (last !== null && last > 0 ? last : null);
  const minuteMs = Date.parse(quoteRow.minute_bucket_utc);
  const expirationCloseMs = buildExpirationCloseMs(quoteRow.expiration);
  const timeToExpiryYears = !Number.isFinite(minuteMs) || !Number.isFinite(expirationCloseMs)
    ? null
    : Math.max(0, (expirationCloseMs - minuteMs) / 31557600000);
  const isCall = quoteRow.option_right === 'CALL';

  let calcStatus = 'ok';
  if (midPrice === null) {
    calcStatus = 'missing_price';
  } else if (underlyingPrice === null || underlyingPrice <= 0 || strike === null || strike <= 0) {
    calcStatus = 'missing_underlying';
  } else if (timeToExpiryYears === null || timeToExpiryYears <= 0) {
    calcStatus = 'expired';
  }

  let intrinsicPrice = null;
  let upperBoundPrice = null;
  if (calcStatus === 'ok') {
    intrinsicPrice = isCall
      ? Math.max((underlyingPrice * Math.exp(-dividendYield * timeToExpiryYears)) - (strike * Math.exp(-riskFreeRate * timeToExpiryYears)), 0)
      : Math.max((strike * Math.exp(-riskFreeRate * timeToExpiryYears)) - (underlyingPrice * Math.exp(-dividendYield * timeToExpiryYears)), 0);
    upperBoundPrice = isCall
      ? (underlyingPrice * Math.exp(-dividendYield * timeToExpiryYears))
      : (strike * Math.exp(-riskFreeRate * timeToExpiryYears));
    if (!(midPrice > intrinsicPrice + 1e-9 && midPrice < upperBoundPrice - 1e-9)) {
      calcStatus = 'invalid_input';
    }
  }

  let impliedVol = null;
  let modelPrice = null;
  let delta = null;
  let gamma = null;
  let thetaAnnual = null;
  let vegaAnnual = null;
  let rhoAnnual = null;
  let finalIvLow = null;
  let finalIvHigh = null;
  if (calcStatus === 'ok') {
    let low = ivLow;
    let high = ivHigh;
    for (let step = 0; step < ivIterations; step += 1) {
      const midIv = (low + high) / 2;
      const trialPrice = bsPrice(midIv, underlyingPrice, strike, riskFreeRate, dividendYield, timeToExpiryYears, isCall);
      if (trialPrice > midPrice) high = midIv;
      else low = midIv;
    }
    impliedVol = (low + high) / 2;
    finalIvLow = low;
    finalIvHigh = high;
    modelPrice = bsPrice(impliedVol, underlyingPrice, strike, riskFreeRate, dividendYield, timeToExpiryYears, isCall);
    delta = bsDelta(impliedVol, underlyingPrice, strike, riskFreeRate, dividendYield, timeToExpiryYears, isCall);
    gamma = bsGamma(impliedVol, underlyingPrice, strike, riskFreeRate, dividendYield, timeToExpiryYears);
    thetaAnnual = bsThetaAnnual(impliedVol, underlyingPrice, strike, riskFreeRate, dividendYield, timeToExpiryYears, isCall);
    vegaAnnual = bsVega(impliedVol, underlyingPrice, strike, riskFreeRate, dividendYield, timeToExpiryYears);
    rhoAnnual = bsRhoAnnual(impliedVol, underlyingPrice, strike, riskFreeRate, dividendYield, timeToExpiryYears, isCall);
  }

  return {
    symbol: quoteRow.symbol,
    trade_date_utc: quoteRow.trade_date_utc,
    expiration: quoteRow.expiration,
    strike,
    option_right: quoteRow.option_right,
    minute_bucket_utc: quoteRow.minute_bucket_utc,
    bid,
    ask,
    last,
    mid_price: midPrice,
    underlying_price: underlyingPrice,
    risk_free_rate: riskFreeRate,
    dividend_yield: dividendYield,
    time_to_expiry_years: timeToExpiryYears,
    implied_vol: impliedVol,
    delta,
    gamma,
    theta_annual: thetaAnnual,
    theta_per_day: thetaAnnual === null ? null : (thetaAnnual / 365.0),
    vega_annual: vegaAnnual,
    vega_per_1pct: vegaAnnual === null ? null : (vegaAnnual / 100.0),
    rho_annual: rhoAnnual,
    rho_per_1pct: rhoAnnual === null ? null : (rhoAnnual / 100.0),
    model_price: modelPrice,
    price_error_abs: modelPrice === null || midPrice === null ? null : Math.abs(modelPrice - midPrice),
    iv_low: finalIvLow,
    iv_high: finalIvHigh,
    calc_status: calcStatus,
    calc_run_id: runId,
    calc_version: calcVersion,
    source_mode: 'calculated',
    source_endpoint: quoteRow.source_endpoint || null,
    ingested_at_utc: nowIso(),
  };
}

async function downloadStockToParquet({ runRoot, symbol, dayIso, env = process.env }) {
  const includeRawPayload = parseBooleanLike(env.PARQUET_INCLUDE_RAW_PAYLOAD, false);
  const filePath = getStockPath(runRoot, symbol, dayIso);
  const endpoint = historicalPrivate.resolveThetaSpotEndpoint(symbol, dayIso, env);
  const rows = endpoint ? await historicalPrivate.fetchThetaRows(endpoint, { env }) : [];
  const normalizedRows = normalizeStockOhlcRows(rows, dayIso, { includeRawPayload })
    .map((row) => ({
      ...row,
      symbol,
      source_endpoint: endpoint,
    }));
  const handle = await openParquetWriter(RAW_STOCK_SCHEMA, filePath);
  try {
    await appendRows(handle.writer, normalizedRows);
    await handle.close(true);
  } catch (error) {
    await handle.close(false);
    throw error;
  }
  const stockByMinute = new Map(normalizedRows.map((row) => [row.minute_bucket_utc, toNumber(row.close)]).filter(([, close]) => close !== null));
  return {
    rowCount: normalizedRows.length,
    filePath,
    stockByMinute,
  };
}

async function downloadQuotesToParquet({ runRoot, symbol, dayIso, env = process.env }) {
  const includeRawPayload = parseBooleanLike(env.PARQUET_INCLUDE_RAW_PAYLOAD, false);
  const filePath = getQuotePath(runRoot, symbol, dayIso);
  const sessionWindow = await fetchCalendarSessionWindow(dayIso, env).catch(() => null);
  const quoteWindows = resolveThetaTimeWindowsForSymbol(symbol, {
    sessionStartTime: sessionWindow?.openTime || null,
    sessionEndTime: sessionWindow?.regularCloseTime || sessionWindow?.closeTime || null,
    env,
  });
  const handle = await openParquetWriter(RAW_QUOTE_SCHEMA, filePath);
  const expirations = new Set();
  let writtenRows = 0;
  const logEvery = Math.max(1, Math.trunc(parseNumberEnv('PARQUET_PROGRESS_EVERY_ROWS', DEFAULT_CHUNK_LOG_EVERY, env)));
  try {
    for (const window of quoteWindows) {
      const endpoint = historicalPrivate.resolveThetaOptionQuoteEndpoint(symbol, dayIso, env, window);
      if (!endpoint) continue;
      const format = new URL(endpoint).searchParams.get('format');
      if (format === 'ndjson') {
        let buffer = [];
        const flush = async () => {
          if (buffer.length === 0) return;
          await appendRows(handle.writer, buffer);
          writtenRows += buffer.length;
          buffer = [];
          if (writtenRows > 0 && writtenRows % logEvery === 0) {
            console.log('[PARQUET_QUOTE_PROGRESS]', JSON.stringify({ symbol, dayIso, writtenRows }));
          }
        };
        const result = await fetchNdjsonRows(endpoint, {
          env,
          onRow: async (rawRow) => {
            const normalized = normalizeOptionQuoteRow(rawRow, dayIso, { includeRawPayload });
            if (!normalized) return;
            normalized.source_endpoint = endpoint;
            expirations.add(normalized.expiration);
            buffer.push(normalized);
            if (buffer.length >= 2000) {
              await flush();
            }
          },
        });
        if (!result.response.ok && result.response.status !== 472) {
          throw new Error(`thetadata_request_failed:${result.response.status}`);
        }
        await flush();
      } else {
        const rows = await historicalPrivate.fetchThetaRows(endpoint, { env });
        const normalizedRows = rows
          .map((rawRow) => normalizeOptionQuoteRow(rawRow, dayIso, { includeRawPayload }))
          .filter(Boolean)
          .map((row) => {
            expirations.add(row.expiration);
            return { ...row, source_endpoint: endpoint };
          });
        await appendRows(handle.writer, normalizedRows);
        writtenRows += normalizedRows.length;
      }
    }
    await handle.close(true);
  } catch (error) {
    await handle.close(false);
    throw error;
  }
  return {
    rowCount: writtenRows,
    filePath,
    expirations: Array.from(expirations).sort(),
  };
}

async function downloadIndexGreeksToParquet({
  runRoot,
  symbol,
  dayIso,
  expirations,
  runId,
  env = process.env,
}) {
  const includeRawPayload = parseBooleanLike(env.PARQUET_INCLUDE_RAW_PAYLOAD, false);
  const rawPath = getRawGreeksPath(runRoot, symbol, dayIso);
  const finalPath = getFinalGreeksPath(runRoot, symbol, dayIso);
  const rawHandle = await openParquetWriter(RAW_GREEKS_SCHEMA, rawPath);
  const finalHandle = await openParquetWriter(FINAL_GREEKS_SCHEMA, finalPath);
  const sessionWindow = await fetchCalendarSessionWindow(dayIso, env).catch(() => null);
  const openSecond = parseTimeHmsToSecondOfDay(sessionWindow?.openTime || null);
  const closeSecondRaw = parseTimeHmsToSecondOfDay(sessionWindow?.regularCloseTime || sessionWindow?.closeTime || null);
  const coreCloseSecond = closeSecondRaw === null ? null : Math.max(openSecond ?? 0, closeSecondRaw - 60);
  const adaptivePlan = resolveThetaGreeksAdaptiveWindowMinutes({
    symbol,
    expirationCount: expirations.length,
    env,
  });
  const windows = resolveThetaTimeWindowsForSymbol(symbol, {
    sessionStartTime: sessionWindow?.openTime || null,
    sessionEndTime: coreCloseSecond === null ? null : formatSecondOfDayAsHms(coreCloseSecond),
    windowMinutes: adaptivePlan.windowMinutes,
    forceWindowing: true,
    env,
  });
  const syncFormat = String(env.THETADATA_GREEKS_SYNC_FORMAT || 'ndjson').trim().toLowerCase() === 'json' ? 'json' : 'ndjson';
  let rawRowsWritten = 0;
  try {
    for (const expiration of expirations) {
      for (const window of windows) {
        const endpoint = historicalPrivate.resolveThetaGreeksEndpoint(symbol, expiration, dayIso, env, {
          format: syncFormat,
          startTime: window.startTime || null,
          endTime: window.endTime || null,
        });
        if (!endpoint) continue;
        if (syncFormat === 'ndjson') {
          let rawBuffer = [];
          let finalBuffer = [];
          const flush = async () => {
            if (rawBuffer.length === 0) return;
            await appendRows(rawHandle.writer, rawBuffer);
            await appendRows(finalHandle.writer, finalBuffer);
            rawRowsWritten += rawBuffer.length;
            rawBuffer = [];
            finalBuffer = [];
          };
          const result = await fetchNdjsonRows(endpoint, {
            env,
            onRow: async (rawRow) => {
              const normalized = normalizeOptionGreeksRow(rawRow, dayIso, { includeRawPayload });
              if (!normalized) return;
              normalized.source_endpoint = endpoint;
              rawBuffer.push(normalized);
              finalBuffer.push(normalizeFinalGreekFromRaw(normalized, {
                runId,
                calcVersion: 'theta_raw_v1',
                sourceEndpoint: endpoint,
              }));
              if (rawBuffer.length >= 2000) {
                await flush();
              }
            },
          });
          if (!result.response.ok && result.response.status !== 472) {
            throw new Error(`thetadata_request_failed:${result.response.status}`);
          }
          await flush();
        } else {
          const rows = await historicalPrivate.fetchThetaRows(endpoint, { env });
          const normalizedRows = rows
            .map((rawRow) => normalizeOptionGreeksRow(rawRow, dayIso, { includeRawPayload }))
            .filter(Boolean)
            .map((row) => ({ ...row, source_endpoint: endpoint }));
          const finalRows = normalizedRows.map((row) => normalizeFinalGreekFromRaw(row, {
            runId,
            calcVersion: 'theta_raw_v1',
            sourceEndpoint: endpoint,
          }));
          await appendRows(rawHandle.writer, normalizedRows);
          await appendRows(finalHandle.writer, finalRows);
          rawRowsWritten += normalizedRows.length;
        }
      }
    }
    await rawHandle.close(true);
    await finalHandle.close(true);
  } catch (error) {
    await rawHandle.close(false);
    await finalHandle.close(false);
    throw error;
  }
  return {
    rawRowsWritten,
    rawPath,
    finalPath,
  };
}

async function calculateGreeksToParquet({
  runRoot,
  symbol,
  dayIso,
  stockByMinute,
  runId,
  env = process.env,
}) {
  const quotePath = getQuotePath(runRoot, symbol, dayIso);
  const finalPath = getFinalGreeksPath(runRoot, symbol, dayIso);
  const reader = await parquet.ParquetReader.openFile(quotePath);
  const cursor = reader.getCursor();
  const handle = await openParquetWriter(FINAL_GREEKS_SCHEMA, finalPath);
  const riskFreeRate = parseNumberEnv('CALC_GREEKS_FALLBACK_RATE', DEFAULT_GREEKS_FALLBACK_RATE, env);
  const dividendYield = parseNumberEnv('CALC_GREEKS_DIVIDEND_YIELD', DEFAULT_GREEKS_DIVIDEND_YIELD, env);
  const ivLow = parseNumberEnv('CALC_GREEKS_IV_LOW', DEFAULT_GREEKS_IV_LOW, env);
  const ivHigh = parseNumberEnv('CALC_GREEKS_IV_HIGH', DEFAULT_GREEKS_IV_HIGH, env);
  const ivIterations = Math.max(1, Math.trunc(parseNumberEnv('CALC_GREEKS_IV_ITERATIONS', DEFAULT_GREEKS_IV_ITERATIONS, env)));
  const calcVersion = String(env.CALC_GREEKS_VERSION || 'bs_v1').trim() || 'bs_v1';
  let writtenRows = 0;
  try {
    while (true) {
      const row = await cursor.next();
      if (!row) break;
      const finalRow = computeGreeksFromQuoteRow(row, stockByMinute, {
        runId,
        calcVersion,
        riskFreeRate,
        dividendYield,
        ivLow,
        ivHigh,
        ivIterations,
      });
      await handle.writer.appendRow(finalRow);
      writtenRows += 1;
    }
    await reader.close();
    await handle.close(true);
  } catch (error) {
    await reader.close();
    await handle.close(false);
    throw error;
  }
  return {
    writtenRows,
    finalPath,
  };
}

module.exports = {
  DEFAULT_INDEX_GREEKS_SYMBOLS,
  DEFAULT_SYMBOL_FILE,
  buildJobs,
  buildRunId,
  calculateGreeksToParquet,
  downloadIndexGreeksToParquet,
  downloadQuotesToParquet,
  downloadStockToParquet,
  ensureRunLayout,
  parseIndexGreeksSymbols,
  resolveRunRoot,
  shardJobsBalanced,
  writeJsonFile,
};
