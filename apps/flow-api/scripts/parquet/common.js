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

const DEFAULT_LOCAL_PARQUET_ROOT = path.join(os.homedir(), 'Library', 'Caches', 'phenixflow', 'parquet');
const DEFAULT_EXTERNAL_PARQUET_ROOT = path.join('/Volumes', 'Phenix4TB', 'phenixflow', 'parquet');
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
const DEFAULT_HEAVY_RAW_INDEX_MIN_EXPIRATIONS = 120;
const DEFAULT_HEAVY_RAW_INDEX_QUOTE_WINDOW_MINUTES = 15;
const DEFAULT_HEAVY_RAW_INDEX_WINDOW_MINUTES = 5;
const DEFAULT_HEAVY_RAW_INDEX_QUOTE_CONCURRENCY = 4;
const DEFAULT_HEAVY_RAW_INDEX_GREEKS_CONCURRENCY = 4;
const DEFAULT_HEAVY_RAW_INDEX_EXPIRATION_CONCURRENCY = 4;
const DEFAULT_HEAVY_RAW_INDEX_EXPIRATION_GROUP_SIZE = 6;
const DEFAULT_HEAVY_RAW_INDEX_EXPIRATION_GROUP_MAX_EXPIRATIONS = 64;
const DEFAULT_PARQUET_RESUME_EXISTING = true;
const DEFAULT_THETA_RETRY_ATTEMPTS = 8;
const DEFAULT_THETA_RETRY_BASE_DELAY_MS = 2000;
const DEFAULT_THETA_RETRY_MAX_DELAY_MS = 60000;
const DEFAULT_THETA_GLOBAL_COOLDOWN_MS = 30000;
const DEFAULT_THETA_MAX_CONCURRENT_CONNECTIONS = 4;
const DEFAULT_MARKET_OPEN_TIME = '09:30:00';
const DEFAULT_MARKET_REGULAR_CLOSE_TIME = '16:00:00';
const DEFAULT_PARQUET_WRITE_MAX_PENDING_BATCHES = 8;
const DEFAULT_PARQUET_WRITE_MAX_PENDING_ROWS = 40000;

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

const DEFAULT_HEAVY_RAW_INDEX_SYMBOLS = new Set(DEFAULT_INDEX_GREEKS_SYMBOLS);

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

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, Math.max(0, Math.trunc(ms || 0)));
  });
}

function buildRunId(prefix = 'parquet-greeks') {
  return `${prefix}-${new Date().toISOString().replace(/[-:.]/g, '').slice(0, 15)}Z`;
}

function resolveThetaBaseUrl(env = process.env) {
  return String(env.THETADATA_BASE_URL || DEFAULT_THETADATA_BASE_URL).trim().replace(/\/$/, '');
}

function resolveParquetRoot(env = process.env) {
  const configured = String(env.PHENIXFLOW_PARQUET_ROOT || '').trim();
  if (configured) return path.resolve(configured);
  if (fs.existsSync(path.dirname(DEFAULT_EXTERNAL_PARQUET_ROOT))) {
    return DEFAULT_EXTERNAL_PARQUET_ROOT;
  }
  return DEFAULT_LOCAL_PARQUET_ROOT;
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
    stateRoot: path.join(runRoot, 'state'),
    jobStateRoot: path.join(runRoot, 'state', 'jobs'),
    lockRoot: path.join(runRoot, 'state', 'locks'),
    controlRoot: path.join(runRoot, 'state', 'control'),
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

function parseResumeExisting(env = process.env) {
  return parseBooleanLike(env.PARQUET_RESUME_EXISTING, DEFAULT_PARQUET_RESUME_EXISTING);
}

function parseNumberEnv(envKey, fallback, env = process.env) {
  const parsed = Number(env[envKey]);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseIndexGreeksSymbols(env = process.env) {
  const raw = String(env.PARQUET_INDEX_GREEKS_SYMBOLS || env.INDEX_GREEKS_SYMBOLS || DEFAULT_INDEX_GREEKS_SYMBOLS.join(',')).trim();
  return new Set(raw.split(',').map((token) => normalizeSymbol(token)).filter(Boolean));
}

function parseThetaRetryAttempts(env = process.env) {
  return Math.max(1, Math.min(32, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_RETRY_ATTEMPTS',
    DEFAULT_THETA_RETRY_ATTEMPTS,
    env,
  ))));
}

function parseThetaRetryBaseDelayMs(env = process.env) {
  return Math.max(250, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_RETRY_BASE_DELAY_MS',
    DEFAULT_THETA_RETRY_BASE_DELAY_MS,
    env,
  )));
}

function parseThetaRetryMaxDelayMs(env = process.env) {
  return Math.max(parseThetaRetryBaseDelayMs(env), Math.trunc(parseNumberEnv(
    'PARQUET_THETA_RETRY_MAX_DELAY_MS',
    DEFAULT_THETA_RETRY_MAX_DELAY_MS,
    env,
  )));
}

function parseThetaGlobalCooldownMs(env = process.env) {
  return Math.max(1000, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_GLOBAL_COOLDOWN_MS',
    DEFAULT_THETA_GLOBAL_COOLDOWN_MS,
    env,
  )));
}

function parseThetaMaxConcurrentConnections(env = process.env) {
  return Math.max(1, Math.min(16, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS',
    DEFAULT_THETA_MAX_CONCURRENT_CONNECTIONS,
    env,
  ))));
}

function resolveThetaRateLimitStatePath(env = process.env) {
  const configured = String(env.PARQUET_THETA_RATE_LIMIT_STATE_PATH || '').trim();
  if (configured) return path.resolve(configured);
  return path.join(resolveParquetRoot(env), 'theta-rate-limit-state.json');
}

function resolveThetaConnectionSlotsRoot(env = process.env) {
  const configured = String(env.PARQUET_THETA_CONNECTION_SLOTS_ROOT || '').trim();
  if (configured) return path.resolve(configured);
  return path.join(resolveParquetRoot(env), 'theta-connection-slots');
}

function resolveThetaConnectionSlotPath(slotsRoot, slotIndex) {
  return path.join(slotsRoot, `slot-${slotIndex}.lock`);
}

function isPidAlive(pid) {
  const normalizedPid = Math.trunc(Number(pid) || 0);
  if (normalizedPid <= 0) return false;
  try {
    process.kill(normalizedPid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

function readThetaConnectionSlotMetadata(slotPath) {
  if (!fs.existsSync(slotPath)) return null;
  try {
    const stat = fs.statSync(slotPath);
    if (stat.isDirectory()) {
      const metadataPath = path.join(slotPath, 'metadata.json');
      if (!fs.existsSync(metadataPath)) {
        return { slotType: 'dir', pid: null, token: null };
      }
      const parsed = JSON.parse(fs.readFileSync(metadataPath, 'utf8'));
      return {
        slotType: 'dir',
        pid: Number(parsed?.pid || 0) || null,
        token: parsed?.token || null,
      };
    }
    const parsed = JSON.parse(fs.readFileSync(slotPath, 'utf8'));
    return {
      slotType: 'file',
      pid: Number(parsed?.pid || 0) || null,
      token: parsed?.token || null,
    };
  } catch {
    return { slotType: 'unknown', pid: null, token: null };
  }
}

function cleanupStaleThetaConnectionSlot(slotPath) {
  if (!fs.existsSync(slotPath)) return false;
  const metadata = readThetaConnectionSlotMetadata(slotPath);
  if (metadata?.pid && isPidAlive(metadata.pid)) return false;
  fs.rmSync(slotPath, { recursive: true, force: true });
  return true;
}

async function acquireThetaConnectionSlot({
  env = process.env,
  label = 'theta_request',
} = {}) {
  const slotsRoot = resolveThetaConnectionSlotsRoot(env);
  ensureDir(slotsRoot);
  const slotCount = parseThetaMaxConcurrentConnections(env);
  const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const startedAtMs = Date.now();
  let waitLogged = false;

  while (true) {
    for (let slotIndex = 0; slotIndex < slotCount; slotIndex += 1) {
      const slotPath = resolveThetaConnectionSlotPath(slotsRoot, slotIndex);
      try {
        fs.writeFileSync(slotPath, `${JSON.stringify({
          token,
          label,
          pid: process.pid,
          acquiredAt: nowIso(),
        }, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' });
        return { slotIndex, slotPath, token };
      } catch (error) {
        if (error?.code === 'ENOENT') {
          ensureDir(slotsRoot);
          continue;
        }
        if (error?.code !== 'EEXIST') throw error;
        cleanupStaleThetaConnectionSlot(slotPath);
      }
    }

    if (!waitLogged && (Date.now() - startedAtMs) >= 1000) {
      waitLogged = true;
      console.log('[PARQUET_THETA_SLOT_WAIT]', JSON.stringify({
        label,
        waitedMs: Date.now() - startedAtMs,
        slotCount,
      }));
    }
    await sleep(200 + Math.trunc(Math.random() * 150));
  }
}

function releaseThetaConnectionSlot(slot) {
  if (!slot?.slotPath) return;
  try {
    if (fs.existsSync(slot.slotPath)) {
      const metadata = readThetaConnectionSlotMetadata(slot.slotPath);
      if (slot.token && metadata?.token && metadata.token !== slot.token) return;
    }
    fs.rmSync(slot.slotPath, { recursive: true, force: true });
  } catch {
    // Best effort cleanup.
  }
}

async function withThetaConnectionSlot(taskFn, {
  env = process.env,
  label = 'theta_request',
} = {}) {
  const slot = await acquireThetaConnectionSlot({ env, label });
  try {
    return await taskFn(slot);
  } finally {
    releaseThetaConnectionSlot(slot);
  }
}

function readThetaRateLimitState(env = process.env) {
  const filePath = resolveThetaRateLimitStatePath(env);
  if (!fs.existsSync(filePath)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const untilMs = Number(parsed?.untilMs || 0);
    if (!Number.isFinite(untilMs) || untilMs <= 0) return null;
    return { filePath, untilMs, reason: parsed?.reason || null, updatedAt: parsed?.updatedAt || null };
  } catch {
    return null;
  }
}

function writeThetaRateLimitState(untilMs, {
  env = process.env,
  reason = null,
} = {}) {
  const filePath = resolveThetaRateLimitStatePath(env);
  ensureDir(path.dirname(filePath));
  const tempPath = `${filePath}.tmp`;
  const payload = {
    untilMs: Math.max(0, Math.trunc(untilMs || 0)),
    reason: reason || null,
    updatedAt: nowIso(),
  };
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  fs.renameSync(tempPath, filePath);
}

async function waitForThetaCooldown(env = process.env) {
  while (true) {
    const state = readThetaRateLimitState(env);
    if (!state) return;
    const remainingMs = state.untilMs - Date.now();
    if (remainingMs <= 0) return;
    console.log('[PARQUET_THETA_COOLDOWN_WAIT]', JSON.stringify({
      untilIso: new Date(state.untilMs).toISOString(),
      waitMs: remainingMs,
      reason: state.reason || null,
    }));
    await sleep(remainingMs);
  }
}

function markThetaCooldown(delayMs, {
  env = process.env,
  reason = null,
} = {}) {
  const minimumDelayMs = parseThetaGlobalCooldownMs(env);
  const desiredUntilMs = Date.now() + Math.max(minimumDelayMs, Math.trunc(delayMs || 0));
  const existing = readThetaRateLimitState(env);
  if (existing && existing.untilMs >= desiredUntilMs) return;
  writeThetaRateLimitState(desiredUntilMs, { env, reason });
}

function isRetryableThetaError(error) {
  const message = String(error?.message || error || '');
  if (!message) return false;
  if (/thetadata_request_failed:429/.test(message)) return true;
  if (/thetadata_request_timeout:/.test(message)) return true;
  if (/thetadata_request_idle_timeout:/.test(message)) return true;
  if (/fetch failed/i.test(message)) return true;
  if (/ECONNRESET|EPIPE|socket hang up|UND_ERR|ETIMEDOUT|ECONNREFUSED/i.test(message)) return true;
  return false;
}

function computeRetryDelayMs(attempt, env = process.env) {
  const baseDelayMs = parseThetaRetryBaseDelayMs(env);
  const maxDelayMs = parseThetaRetryMaxDelayMs(env);
  const backoffMs = Math.min(maxDelayMs, baseDelayMs * (2 ** Math.max(0, attempt - 1)));
  const jitterMs = Math.trunc(Math.random() * Math.max(250, Math.trunc(baseDelayMs / 3)));
  return Math.min(maxDelayMs, backoffMs + jitterMs);
}

async function withThetaRetry(taskFn, {
  env = process.env,
  label = 'theta_request',
  attempts = parseThetaRetryAttempts(env),
} = {}) {
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    await waitForThetaCooldown(env);
    try {
      return await withThetaConnectionSlot(
        () => taskFn({ attempt, attempts }),
        { env, label },
      );
    } catch (error) {
      lastError = error;
      if (!isRetryableThetaError(error) || attempt >= attempts) {
        throw error;
      }
      const message = String(error?.message || error || '');
      const retryDelayMs = computeRetryDelayMs(attempt, env);
      if (/thetadata_request_failed:429/.test(message)) {
        markThetaCooldown(retryDelayMs, { env, reason: `${label}:429` });
      }
      console.warn('[PARQUET_THETA_RETRY]', JSON.stringify({
        label,
        attempt,
        attempts,
        retryDelayMs,
        error: message.split('\n')[0],
      }));
      await sleep(retryDelayMs);
    }
  }
  throw lastError || new Error(`theta_retry_failed:${label}`);
}

function parseHeavyRawIndexSymbols(env = process.env) {
  const raw = String(env.PARQUET_HEAVY_RAW_INDEX_SYMBOLS || Array.from(DEFAULT_HEAVY_RAW_INDEX_SYMBOLS).join(',')).trim();
  return new Set(raw.split(',').map((token) => normalizeSymbol(token)).filter(Boolean));
}

function parseHeavyRawIndexMinExpirations(env = process.env) {
  return Math.max(1, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_MIN_EXPIRATIONS',
    DEFAULT_HEAVY_RAW_INDEX_MIN_EXPIRATIONS,
    env,
  )));
}

function parseHeavyRawIndexWindowMinutes(env = process.env) {
  return Math.max(1, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_WINDOW_MINUTES',
    DEFAULT_HEAVY_RAW_INDEX_WINDOW_MINUTES,
    env,
  )));
}

function parseHeavyRawIndexQuoteWindowMinutes(env = process.env) {
  return Math.max(1, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_QUOTE_WINDOW_MINUTES',
    DEFAULT_HEAVY_RAW_INDEX_QUOTE_WINDOW_MINUTES,
    env,
  )));
}

function parseHeavyRawIndexQuoteConcurrency(env = process.env) {
  return Math.max(1, Math.min(32, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_QUOTE_CONCURRENCY',
    DEFAULT_HEAVY_RAW_INDEX_QUOTE_CONCURRENCY,
    env,
  ))));
}

function parseHeavyRawIndexGreeksConcurrency(env = process.env) {
  return Math.max(1, Math.min(32, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_GREEKS_CONCURRENCY',
    DEFAULT_HEAVY_RAW_INDEX_GREEKS_CONCURRENCY,
    env,
  ))));
}

function parseHeavyRawIndexExpirationConcurrency(env = process.env) {
  return Math.max(1, Math.min(32, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_EXPIRATION_CONCURRENCY',
    DEFAULT_HEAVY_RAW_INDEX_EXPIRATION_CONCURRENCY,
    env,
  ))));
}

function parseHeavyRawIndexExpirationGroupSize(env = process.env) {
  return Math.max(1, Math.min(64, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_EXPIRATION_GROUP_SIZE',
    DEFAULT_HEAVY_RAW_INDEX_EXPIRATION_GROUP_SIZE,
    env,
  ))));
}

function parseHeavyRawIndexExpirationGroupMaxExpirations(env = process.env) {
  return Math.max(1, Math.trunc(parseNumberEnv(
    'PARQUET_HEAVY_RAW_INDEX_EXPIRATION_GROUP_MAX_EXPIRATIONS',
    DEFAULT_HEAVY_RAW_INDEX_EXPIRATION_GROUP_MAX_EXPIRATIONS,
    env,
  )));
}

function shouldSplitHeavyRawIndexJob(symbol, {
  expirationCount = 0,
  env = process.env,
} = {}) {
  if (!parseBooleanLike(env.PARQUET_HEAVY_RAW_INDEX_SPLIT_ENABLED, true)) return false;
  const normalizedSymbol = normalizeSymbol(symbol);
  if (!normalizedSymbol) return false;
  if (parseHeavyRawIndexSymbols(env).has(normalizedSymbol)) return true;
  return expirationCount >= parseHeavyRawIndexMinExpirations(env);
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

function buildFallbackSessionWindow(env = process.env) {
  const openSecond = parseTimeHmsToSecondOfDay(DEFAULT_MARKET_OPEN_TIME);
  const regularCloseSecond = parseTimeHmsToSecondOfDay(DEFAULT_MARKET_REGULAR_CLOSE_TIME);
  const closePadMinutes = Math.max(0, Math.min(240, Math.trunc(parseNumberEnv('THETADATA_CALENDAR_CLOSE_PAD_MINUTES', 15, env))));
  const paddedCloseSecond = Math.min(86399, regularCloseSecond + (closePadMinutes * 60));
  return {
    isOpen: true,
    type: 'fallback_default',
    openTime: formatSecondOfDayAsHms(openSecond),
    regularCloseTime: formatSecondOfDayAsHms(regularCloseSecond),
    closeTime: formatSecondOfDayAsHms(paddedCloseSecond),
    closePadMinutes,
  };
}

function hasUsableSessionBounds(sessionWindow) {
  if (!sessionWindow || sessionWindow.isOpen === false) return false;
  const openSecond = parseTimeHmsToSecondOfDay(sessionWindow.openTime);
  const closeSecond = parseTimeHmsToSecondOfDay(sessionWindow.regularCloseTime || sessionWindow.closeTime);
  return openSecond !== null && closeSecond !== null && closeSecond >= openSecond;
}

function resolveProcessingSessionWindow(sessionWindow, {
  symbol = null,
  dayIso = null,
  stage = 'processing',
  env = process.env,
} = {}) {
  if (hasUsableSessionBounds(sessionWindow)) return sessionWindow;
  const fallback = buildFallbackSessionWindow(env);
  console.warn('[PARQUET_SESSION_WINDOW_FALLBACK]', JSON.stringify({
    symbol: normalizeSymbol(symbol),
    dayIso: normalizeIsoDate(dayIso),
    stage,
    reason: sessionWindow?.isOpen === false ? 'calendar_closed_or_missing_bounds' : 'missing_or_invalid_bounds',
    originalType: sessionWindow?.type || null,
    originalOpenTime: sessionWindow?.openTime || null,
    originalRegularCloseTime: sessionWindow?.regularCloseTime || null,
    originalCloseTime: sessionWindow?.closeTime || null,
    fallbackOpenTime: fallback.openTime,
    fallbackRegularCloseTime: fallback.regularCloseTime,
    fallbackCloseTime: fallback.closeTime,
  }));
  return fallback;
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
  const { response, body } = await withThetaConnectionSlot(() => fetchTextWithTimeout(url, {
    env,
    timeoutMs: parseNumberEnv('THETADATA_CALENDAR_TIMEOUT_MS', DEFAULT_CALENDAR_TIMEOUT_MS, env),
  }), {
    env,
    label: `calendar:${dayIso}`,
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

function getStockPartitionDir(runRoot, symbol, dayIso) {
  return path.dirname(getStockPath(runRoot, symbol, dayIso));
}

function getQuotePath(runRoot, symbol, dayIso) {
  return path.join(resolveLayout(runRoot).rawQuoteRoot, `symbol=${symbol}`, `trade_date_utc=${dayIso}`, 'part-000.parquet');
}

function getQuotePartitionDir(runRoot, symbol, dayIso) {
  return path.dirname(getQuotePath(runRoot, symbol, dayIso));
}

function getRawGreeksPath(runRoot, symbol, dayIso) {
  return path.join(resolveLayout(runRoot).rawGreeksRoot, `symbol=${symbol}`, `trade_date_utc=${dayIso}`, 'part-000.parquet');
}

function getRawGreeksPartitionDir(runRoot, symbol, dayIso) {
  return path.dirname(getRawGreeksPath(runRoot, symbol, dayIso));
}

function getFinalGreeksPath(runRoot, symbol, dayIso) {
  return path.join(resolveLayout(runRoot).finalGreeksRoot, `symbol=${symbol}`, `trade_date_utc=${dayIso}`, 'part-000.parquet');
}

function getFinalGreeksPartitionDir(runRoot, symbol, dayIso) {
  return path.dirname(getFinalGreeksPath(runRoot, symbol, dayIso));
}

function getPartitionPartPath(partitionDir, partIndex = 0) {
  return path.join(partitionDir, `part-${String(Math.max(0, partIndex)).padStart(4, '0')}.parquet`);
}

function getPartitionSuccessMarkerPath(partitionDir) {
  return path.join(partitionDir, '_SUCCESS.json');
}

function resetPartitionDir(partitionDir) {
  fs.rmSync(partitionDir, { recursive: true, force: true });
  ensureDir(partitionDir);
}

function listParquetPartFiles(partitionDir) {
  if (!fs.existsSync(partitionDir)) return [];
  return fs.readdirSync(partitionDir)
    .filter((name) => name.endsWith('.parquet'))
    .sort()
    .map((name) => path.join(partitionDir, name));
}

function chunkArray(values, chunkSize) {
  const size = Math.max(1, Math.trunc(chunkSize || 1));
  const out = [];
  for (let index = 0; index < values.length; index += size) {
    out.push(values.slice(index, index + size));
  }
  return out;
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

function createAsyncBatchAppender(writeBatch, {
  env = process.env,
  maxPendingBatches = Math.max(1, Math.trunc(parseNumberEnv(
    'PARQUET_WRITE_MAX_PENDING_BATCHES',
    DEFAULT_PARQUET_WRITE_MAX_PENDING_BATCHES,
    env,
  ))),
  maxPendingRows = Math.max(1000, Math.trunc(parseNumberEnv(
    'PARQUET_WRITE_MAX_PENDING_ROWS',
    DEFAULT_PARQUET_WRITE_MAX_PENDING_ROWS,
    env,
  ))),
} = {}) {
  let pendingBatches = 0;
  let pendingRows = 0;
  let queue = Promise.resolve();
  let firstError = null;

  async function drain() {
    await queue;
    if (firstError) {
      const error = firstError;
      firstError = null;
      throw error;
    }
  }

  async function schedule(rows, { forceDrain = false, rowCount = null } = {}) {
    const normalizedRowCount = Number.isFinite(Number(rowCount))
      ? Math.max(0, Math.trunc(Number(rowCount)))
      : (Array.isArray(rows) ? rows.length : 0);
    if (normalizedRowCount === 0) {
      if (forceDrain) await drain();
      return;
    }
    if (firstError) throw firstError;
    pendingBatches += 1;
    pendingRows += normalizedRowCount;
    queue = queue.then(async () => {
      try {
        await writeBatch(rows);
      } catch (error) {
        firstError = firstError || error;
      } finally {
        pendingBatches = Math.max(0, pendingBatches - 1);
        pendingRows = Math.max(0, pendingRows - normalizedRowCount);
      }
    });
    if (forceDrain || pendingBatches >= maxPendingBatches || pendingRows >= maxPendingRows) {
      await drain();
    }
  }

  return {
    schedule,
    drain,
  };
}

async function runTasksWithConcurrency(tasks, concurrency, workerFn) {
  if (!Array.isArray(tasks) || tasks.length === 0) return [];
  const normalizedConcurrency = Math.max(1, Math.min(tasks.length, Math.trunc(concurrency || 1)));
  const results = new Array(tasks.length);
  let nextIndex = 0;
  async function runWorker() {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      if (currentIndex >= tasks.length) return;
      results[currentIndex] = await workerFn(tasks[currentIndex], currentIndex);
    }
  }
  await Promise.all(Array.from({ length: normalizedConcurrency }, () => runWorker()));
  return results;
}

async function scanParquetFiles(partitionDir, onRow) {
  const partFiles = listParquetPartFiles(partitionDir);
  for (const filePath of partFiles) {
    const reader = await parquet.ParquetReader.openFile(filePath);
    try {
      const cursor = reader.getCursor();
      while (true) {
        const row = await cursor.next();
        if (!row) break;
        const maybePromise = onRow(row, filePath);
        if (maybePromise && typeof maybePromise.then === 'function') {
          await maybePromise;
        }
      }
    } finally {
      await reader.close();
    }
  }
}

function readPartitionSuccessMarker(partitionDir) {
  const markerPath = getPartitionSuccessMarkerPath(partitionDir);
  if (!fs.existsSync(markerPath)) return null;
  try {
    return JSON.parse(fs.readFileSync(markerPath, 'utf8'));
  } catch {
    return null;
  }
}

function writePartitionSuccessMarker(partitionDir, payload) {
  const markerPath = getPartitionSuccessMarkerPath(partitionDir);
  writeJsonFile(markerPath, {
    completedAt: nowIso(),
    ...payload,
  });
}

async function loadStockPartition(partitionDir) {
  const partFiles = listParquetPartFiles(partitionDir);
  if (partFiles.length === 0) return null;
  let rowCount = 0;
  const stockByMinute = new Map();
  await scanParquetFiles(partitionDir, (row) => {
    rowCount += 1;
    const close = toNumber(row.close);
    if (row?.minute_bucket_utc && close !== null) {
      stockByMinute.set(row.minute_bucket_utc, close);
    }
  });
  return { rowCount, stockByMinute };
}

async function probeStockPartition(partitionDir) {
  const marker = readPartitionSuccessMarker(partitionDir);
  if (marker?.rowCount !== undefined) {
    const loaded = await loadStockPartition(partitionDir);
    if (!loaded) return null;
    return {
      rowCount: loaded.rowCount,
      stockByMinute: loaded.stockByMinute,
      marker,
      legacy: false,
    };
  }
  const legacy = await loadStockPartition(partitionDir);
  if (!legacy) return null;
  writePartitionSuccessMarker(partitionDir, {
    stage: 'stock',
    rowCount: legacy.rowCount,
    partCount: listParquetPartFiles(partitionDir).length,
    legacyInferred: true,
  });
  return {
    rowCount: legacy.rowCount,
    stockByMinute: legacy.stockByMinute,
    marker: readPartitionSuccessMarker(partitionDir),
    legacy: true,
  };
}

async function summarizeQuotePartition(partitionDir) {
  const partFiles = listParquetPartFiles(partitionDir);
  if (partFiles.length === 0) return null;
  let rowCount = 0;
  const expirations = new Set();
  await scanParquetFiles(partitionDir, (row) => {
    rowCount += 1;
    if (row?.expiration) expirations.add(normalizeIsoDate(row.expiration));
  });
  return {
    rowCount,
    expirations: Array.from(expirations).filter(Boolean).sort(),
  };
}

async function probeQuotePartition(partitionDir) {
  const marker = readPartitionSuccessMarker(partitionDir);
  if (marker?.rowCount !== undefined) {
    const summary = await summarizeQuotePartition(partitionDir);
    if (!summary) return null;
    return {
      ...summary,
      marker,
      legacy: false,
    };
  }
  const legacy = await summarizeQuotePartition(partitionDir);
  if (!legacy) return null;
  writePartitionSuccessMarker(partitionDir, {
    stage: 'quotes',
    rowCount: legacy.rowCount,
    expirationCount: legacy.expirations.length,
    expirations: legacy.expirations,
    partCount: listParquetPartFiles(partitionDir).length,
    legacyInferred: true,
  });
  return {
    ...legacy,
    marker: readPartitionSuccessMarker(partitionDir),
    legacy: true,
  };
}

async function summarizeRowPartition(partitionDir) {
  const partFiles = listParquetPartFiles(partitionDir);
  if (partFiles.length === 0) return null;
  let rowCount = 0;
  await scanParquetFiles(partitionDir, () => {
    rowCount += 1;
  });
  return { rowCount };
}

async function probeRowPartition(partitionDir, stage = 'rows') {
  const marker = readPartitionSuccessMarker(partitionDir);
  if (marker?.rowCount !== undefined) {
    const summary = await summarizeRowPartition(partitionDir);
    if (!summary) return null;
    return {
      ...summary,
      marker,
      legacy: false,
    };
  }
  const legacy = await summarizeRowPartition(partitionDir);
  if (!legacy) return null;
  writePartitionSuccessMarker(partitionDir, {
    stage,
    rowCount: legacy.rowCount,
    partCount: listParquetPartFiles(partitionDir).length,
    legacyInferred: true,
  });
  return {
    ...legacy,
    marker: readPartitionSuccessMarker(partitionDir),
    legacy: true,
  };
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

async function writeQuoteWindowPart({
  filePath,
  symbol,
  dayIso,
  window,
  includeRawPayload = false,
  env = process.env,
}) {
  const handle = await openParquetWriter(RAW_QUOTE_SCHEMA, filePath);
  const expirations = new Set();
  let writtenRows = 0;
  const logEvery = Math.max(1, Math.trunc(parseNumberEnv('PARQUET_PROGRESS_EVERY_ROWS', DEFAULT_CHUNK_LOG_EVERY, env)));
  const appender = createAsyncBatchAppender(async (rows) => {
    await appendRows(handle.writer, rows);
    writtenRows += rows.length;
    if (writtenRows > 0 && writtenRows % logEvery === 0) {
      console.log('[PARQUET_QUOTE_PROGRESS]', JSON.stringify({ symbol, dayIso, writtenRows }));
    }
  }, { env });
  const endpoint = historicalPrivate.resolveThetaOptionQuoteEndpoint(symbol, dayIso, env, window);
  try {
    if (!endpoint) {
      await handle.close(false);
      return { rowCount: 0, expirations: [], endpoint: null };
    }
    const format = new URL(endpoint).searchParams.get('format');
    if (format === 'ndjson') {
      let buffer = [];
      const flush = async (forceDrain = false) => {
        if (buffer.length === 0) return;
        const rows = buffer;
        buffer = [];
        await appender.schedule(rows, { forceDrain });
      };
      const result = await withThetaRetry(() => fetchNdjsonRows(endpoint, {
        env,
        onRow: async (rawRow) => {
          const normalized = normalizeOptionQuoteRow(rawRow, dayIso, { includeRawPayload });
          if (!normalized) return;
          normalized.source_endpoint = endpoint;
          expirations.add(normalized.expiration);
          buffer.push(normalized);
          if (buffer.length >= 2000) {
            await flush(false);
          }
        },
      }), {
        env,
        label: `quote:${symbol}:${dayIso}:${window.startTime || 'full'}-${window.endTime || 'end'}`,
      });
      if (!result.response.ok && result.response.status !== 472) {
        throw new Error(`thetadata_request_failed:${result.response.status}`);
      }
      await flush(true);
      await appender.drain();
    } else {
      const rows = await withThetaRetry(() => historicalPrivate.fetchThetaRows(endpoint, { env }), {
        env,
        label: `quote:${symbol}:${dayIso}:${window.startTime || 'full'}-${window.endTime || 'end'}`,
      });
      const normalizedRows = rows
        .map((rawRow) => normalizeOptionQuoteRow(rawRow, dayIso, { includeRawPayload }))
        .filter(Boolean)
        .map((row) => {
          expirations.add(row.expiration);
          return { ...row, source_endpoint: endpoint };
        });
      await appender.schedule(normalizedRows, { forceDrain: true });
    }
    await handle.close(writtenRows > 0);
    return {
      rowCount: writtenRows,
      expirations: Array.from(expirations),
      endpoint,
    };
  } catch (error) {
    await handle.close(false);
    throw error;
  }
}

async function writeRawIndexWindowPart({
  rawPath,
  finalPath,
  symbol,
  dayIso,
  expirations,
  window,
  runId,
  includeRawPayload = false,
  env = process.env,
}) {
  const rawHandle = await openParquetWriter(RAW_GREEKS_SCHEMA, rawPath);
  const finalHandle = await openParquetWriter(FINAL_GREEKS_SCHEMA, finalPath);
  const syncFormat = String(env.THETADATA_GREEKS_SYNC_FORMAT || 'ndjson').trim().toLowerCase() === 'json' ? 'json' : 'ndjson';
  const expirationConcurrency = Math.max(1, Math.min(expirations.length || 1, parseHeavyRawIndexExpirationConcurrency(env)));
  let rawRowsWritten = 0;
  try {
    const appender = createAsyncBatchAppender(async ({ rawRows, finalRows }) => {
      if (!rawRows.length) return;
      await appendRows(rawHandle.writer, rawRows);
      await appendRows(finalHandle.writer, finalRows);
      rawRowsWritten += rawRows.length;
    }, { env });
    await runTasksWithConcurrency(expirations, expirationConcurrency, async (expiration) => {
      const endpoint = historicalPrivate.resolveThetaGreeksEndpoint(symbol, expiration, dayIso, env, {
        format: syncFormat,
        startTime: window.startTime || null,
        endTime: window.endTime || null,
      });
      if (!endpoint) return;
      if (syncFormat === 'ndjson') {
        let rawBuffer = [];
        let finalBuffer = [];
        const flush = async (force = false) => {
          if (!force && rawBuffer.length < 2000) return;
          if (rawBuffer.length === 0) return;
          const rows = rawBuffer;
          const finalRows = finalBuffer;
          rawBuffer = [];
          finalBuffer = [];
          await appender.schedule({ rawRows: rows, finalRows }, { forceDrain: force, rowCount: rows.length });
        };
        const result = await withThetaRetry(() => fetchNdjsonRows(endpoint, {
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
            await flush(false);
          },
        }), {
          env,
          label: `raw_greeks:${symbol}:${expiration}:${dayIso}:${window.startTime || 'full'}-${window.endTime || 'end'}`,
        });
        if (!result.response.ok && result.response.status !== 472) {
          throw new Error(`thetadata_request_failed:${result.response.status}`);
        }
        await flush(true);
      } else {
        const rows = await withThetaRetry(() => historicalPrivate.fetchThetaRows(endpoint, { env }), {
          env,
          label: `raw_greeks:${symbol}:${expiration}:${dayIso}:${window.startTime || 'full'}-${window.endTime || 'end'}`,
        });
        const normalizedRows = rows
          .map((rawRow) => normalizeOptionGreeksRow(rawRow, dayIso, { includeRawPayload }))
          .filter(Boolean)
          .map((row) => ({ ...row, source_endpoint: endpoint }));
        const finalRows = normalizedRows.map((row) => normalizeFinalGreekFromRaw(row, {
          runId,
          calcVersion: 'theta_raw_v1',
          sourceEndpoint: endpoint,
        }));
        await appender.schedule({ rawRows: normalizedRows, finalRows }, { forceDrain: true, rowCount: normalizedRows.length });
      }
    });
    await appender.drain();
    await rawHandle.close(rawRowsWritten > 0);
    await finalHandle.close(rawRowsWritten > 0);
    return { rawRowsWritten };
  } catch (error) {
    await rawHandle.close(false);
    await finalHandle.close(false);
    throw error;
  }
}

async function downloadStockToParquet({ runRoot, symbol, dayIso, env = process.env }) {
  const includeRawPayload = parseBooleanLike(env.PARQUET_INCLUDE_RAW_PAYLOAD, false);
  const partitionDir = getStockPartitionDir(runRoot, symbol, dayIso);
  const filePath = getStockPath(runRoot, symbol, dayIso);
  if (parseResumeExisting(env)) {
    const existing = await probeStockPartition(partitionDir);
    if (existing) {
      console.log('[PARQUET_STAGE_RESUME]', JSON.stringify({ stage: 'stock', symbol, dayIso, rowCount: existing.rowCount }));
      return {
        rowCount: existing.rowCount,
        filePath,
        stockByMinute: existing.stockByMinute,
      };
    }
  }
  resetPartitionDir(partitionDir);
  const endpoint = historicalPrivate.resolveThetaSpotEndpoint(symbol, dayIso, env);
  const rows = endpoint ? await withThetaRetry(() => historicalPrivate.fetchThetaRows(endpoint, { env }), {
    env,
    label: `stock:${symbol}:${dayIso}`,
  }) : [];
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
  const stockPartCount = listParquetPartFiles(partitionDir).length;
  if (normalizedRows.length > 0 && stockPartCount === 0) {
    throw new Error(`stock_partition_missing_after_write:${symbol}:${dayIso}`);
  }
  if (stockPartCount > 0) {
    writePartitionSuccessMarker(partitionDir, {
      stage: 'stock',
      rowCount: normalizedRows.length,
      partCount: stockPartCount,
    });
  }
  return {
    rowCount: normalizedRows.length,
    filePath,
    stockByMinute,
  };
}

async function downloadQuotesToParquet({ runRoot, symbol, dayIso, env = process.env }) {
  const includeRawPayload = parseBooleanLike(env.PARQUET_INCLUDE_RAW_PAYLOAD, false);
  const partitionDir = getQuotePartitionDir(runRoot, symbol, dayIso);
  const filePath = getPartitionPartPath(partitionDir, 0);
  if (parseResumeExisting(env)) {
    const existing = await probeQuotePartition(partitionDir);
    if (existing) {
      console.log('[PARQUET_STAGE_RESUME]', JSON.stringify({
        stage: 'quotes',
        symbol,
        dayIso,
        rowCount: existing.rowCount,
        expirationCount: existing.expirations.length,
      }));
      return {
        rowCount: existing.rowCount,
        filePath: partitionDir,
        expirations: existing.expirations,
      };
    }
  }
  const sessionWindow = resolveProcessingSessionWindow(
    await fetchCalendarSessionWindow(dayIso, env).catch(() => null),
    { symbol, dayIso, stage: 'quotes', env },
  );
  const splitHeavyRawIndex = shouldSplitHeavyRawIndexJob(symbol, { env });
  const quoteWindows = resolveThetaTimeWindowsForSymbol(symbol, {
    sessionStartTime: sessionWindow?.openTime || null,
    sessionEndTime: sessionWindow?.regularCloseTime || sessionWindow?.closeTime || null,
    windowMinutes: splitHeavyRawIndex ? parseHeavyRawIndexQuoteWindowMinutes(env) : null,
    forceWindowing: splitHeavyRawIndex,
    env,
  });
  resetPartitionDir(partitionDir);
  const expirations = new Set();
  let writtenRows = 0;
  const quoteConcurrency = splitHeavyRawIndex ? parseHeavyRawIndexQuoteConcurrency(env) : 1;
  if (splitHeavyRawIndex && quoteWindows.length > 1 && quoteConcurrency > 1) {
    console.log('[PARQUET_QUOTE_SPLIT_PLAN]', JSON.stringify({
      symbol,
      dayIso,
      windowCount: quoteWindows.length,
      windowMinutes: parseHeavyRawIndexQuoteWindowMinutes(env),
      concurrency: quoteConcurrency,
    }));
    const parts = await runTasksWithConcurrency(quoteWindows, quoteConcurrency, async (window, partIndex) => {
      const partResult = await writeQuoteWindowPart({
        filePath: getPartitionPartPath(partitionDir, partIndex),
        symbol,
        dayIso,
        window,
        includeRawPayload,
        env,
      });
      return {
        rowCount: partResult.rowCount,
        expirations: partResult.expirations,
      };
    });
    parts.forEach((part) => {
      writtenRows += Number(part?.rowCount || 0);
      (part?.expirations || []).forEach((expiration) => expirations.add(expiration));
    });
  } else {
    const partResult = await writeQuoteWindowPart({
      filePath,
      symbol,
      dayIso,
      window: quoteWindows[0] || {},
      includeRawPayload,
      env,
    });
    writtenRows = partResult.rowCount;
    partResult.expirations.forEach((expiration) => expirations.add(expiration));
    if (quoteWindows.length > 1) {
      for (let index = 1; index < quoteWindows.length; index += 1) {
        const extraResult = await writeQuoteWindowPart({
          filePath: getPartitionPartPath(partitionDir, index),
          symbol,
          dayIso,
          window: quoteWindows[index],
          includeRawPayload,
          env,
        });
        writtenRows += extraResult.rowCount;
        extraResult.expirations.forEach((expiration) => expirations.add(expiration));
      }
    }
  }
  const quotePartCount = listParquetPartFiles(partitionDir).length;
  if ((writtenRows > 0 || expirations.size > 0) && quotePartCount === 0) {
    throw new Error(`quote_partition_missing_after_write:${symbol}:${dayIso}`);
  }
  if (quotePartCount > 0) {
    writePartitionSuccessMarker(partitionDir, {
      stage: 'quotes',
      rowCount: writtenRows,
      expirationCount: expirations.size,
      expirations: Array.from(expirations).sort(),
      partCount: quotePartCount,
    });
  }
  return {
    rowCount: writtenRows,
    filePath: partitionDir,
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
  const rawPartitionDir = getRawGreeksPartitionDir(runRoot, symbol, dayIso);
  const finalPartitionDir = getFinalGreeksPartitionDir(runRoot, symbol, dayIso);
  if (parseResumeExisting(env)) {
    const existingRaw = await probeRowPartition(rawPartitionDir, 'raw_greeks');
    const existingFinal = await probeRowPartition(finalPartitionDir, 'final_greeks_raw');
    if (existingRaw && existingFinal) {
      console.log('[PARQUET_STAGE_RESUME]', JSON.stringify({
        stage: 'raw_greeks',
        symbol,
        dayIso,
        rowCount: existingRaw.rowCount,
      }));
      return {
        rawRowsWritten: existingRaw.rowCount,
        rawPath: rawPartitionDir,
        finalPath: finalPartitionDir,
      };
    }
  }
  const sessionWindow = resolveProcessingSessionWindow(
    await fetchCalendarSessionWindow(dayIso, env).catch(() => null),
    { symbol, dayIso, stage: 'raw_greeks', env },
  );
  const openSecond = parseTimeHmsToSecondOfDay(sessionWindow?.openTime || null);
  const closeSecondRaw = parseTimeHmsToSecondOfDay(sessionWindow?.regularCloseTime || sessionWindow?.closeTime || null);
  const coreCloseSecond = closeSecondRaw === null ? null : Math.max(openSecond ?? 0, closeSecondRaw - 60);
  const splitHeavyRawIndex = shouldSplitHeavyRawIndexJob(symbol, {
    expirationCount: expirations.length,
    env,
  });
  const adaptivePlan = resolveThetaGreeksAdaptiveWindowMinutes({
    symbol,
    expirationCount: expirations.length,
    env,
  });
  const sessionStartTime = sessionWindow?.openTime || null;
  const sessionEndTime = coreCloseSecond === null ? null : formatSecondOfDayAsHms(coreCloseSecond);
  const effectiveWindowMinutes = splitHeavyRawIndex
    ? Math.min(adaptivePlan.windowMinutes, parseHeavyRawIndexWindowMinutes(env))
    : adaptivePlan.windowMinutes;
  const windows = resolveThetaTimeWindowsForSymbol(symbol, {
    sessionStartTime,
    sessionEndTime,
    windowMinutes: effectiveWindowMinutes,
    forceWindowing: true,
    env,
  });
  resetPartitionDir(rawPartitionDir);
  resetPartitionDir(finalPartitionDir);
  let rawRowsWritten = 0;
  const greekConcurrency = splitHeavyRawIndex ? parseHeavyRawIndexGreeksConcurrency(env) : 1;
  const groupModeEnabled = splitHeavyRawIndex
    && expirations.length <= parseHeavyRawIndexExpirationGroupMaxExpirations(env);
  const tasks = groupModeEnabled
    ? chunkArray(expirations, parseHeavyRawIndexExpirationGroupSize(env)).map((group, partIndex) => ({
      partIndex,
      expirations: group,
      window: { startTime: sessionStartTime, endTime: sessionEndTime },
    }))
    : windows.map((window, partIndex) => ({
      partIndex,
      expirations,
      window,
    }));
  if (splitHeavyRawIndex && tasks.length > 1 && greekConcurrency > 1) {
    console.log('[PARQUET_RAW_GREEKS_SPLIT_PLAN]', JSON.stringify({
      symbol,
      dayIso,
      expirationCount: expirations.length,
      taskMode: groupModeEnabled ? 'expiration_groups' : 'time_windows',
      taskCount: tasks.length,
      windowCount: windows.length,
      windowMinutes: effectiveWindowMinutes,
      expirationGroupSize: groupModeEnabled ? parseHeavyRawIndexExpirationGroupSize(env) : null,
      concurrency: greekConcurrency,
      mode: adaptivePlan.mode,
    }));
    const parts = await runTasksWithConcurrency(tasks, greekConcurrency, async (task) => writeRawIndexWindowPart({
      rawPath: getPartitionPartPath(rawPartitionDir, task.partIndex),
      finalPath: getPartitionPartPath(finalPartitionDir, task.partIndex),
      symbol,
      dayIso,
      expirations: task.expirations,
      window: task.window,
      runId,
      includeRawPayload,
      env,
    }));
    parts.forEach((part) => {
      rawRowsWritten += Number(part?.rawRowsWritten || 0);
    });
  } else {
    for (const task of tasks) {
      const part = await writeRawIndexWindowPart({
        rawPath: getPartitionPartPath(rawPartitionDir, task.partIndex),
        finalPath: getPartitionPartPath(finalPartitionDir, task.partIndex),
        symbol,
        dayIso,
        expirations: task.expirations,
        window: task.window,
        runId,
        includeRawPayload,
        env,
      });
      rawRowsWritten += Number(part?.rawRowsWritten || 0);
    }
  }
  const rawPartCount = listParquetPartFiles(rawPartitionDir).length;
  const finalRawPartCount = listParquetPartFiles(finalPartitionDir).length;
  if (rawRowsWritten > 0 && (rawPartCount === 0 || finalRawPartCount === 0)) {
    throw new Error(`raw_greeks_partition_missing_after_write:${symbol}:${dayIso}`);
  }
  if (rawPartCount > 0) {
    writePartitionSuccessMarker(rawPartitionDir, {
      stage: 'raw_greeks',
      rowCount: rawRowsWritten,
      partCount: rawPartCount,
    });
  }
  if (finalRawPartCount > 0) {
    writePartitionSuccessMarker(finalPartitionDir, {
      stage: 'final_greeks_raw',
      rowCount: rawRowsWritten,
      partCount: finalRawPartCount,
    });
  }
  return {
    rawRowsWritten,
    rawPath: rawPartitionDir,
    finalPath: finalPartitionDir,
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
  const quotePartFiles = listParquetPartFiles(getQuotePartitionDir(runRoot, symbol, dayIso));
  const finalPath = getFinalGreeksPath(runRoot, symbol, dayIso);
  if (quotePartFiles.length === 0) {
    throw new Error(`missing_quote_parquet:${symbol}:${dayIso}`);
  }
  if (parseResumeExisting(env)) {
    const existing = await probeRowPartition(getFinalGreeksPartitionDir(runRoot, symbol, dayIso), 'final_greeks_calculated');
    if (existing) {
      console.log('[PARQUET_STAGE_RESUME]', JSON.stringify({
        stage: 'calc_greeks',
        symbol,
        dayIso,
        rowCount: existing.rowCount,
      }));
      return {
        writtenRows: existing.rowCount,
        finalPath,
      };
    }
  }
  resetPartitionDir(getFinalGreeksPartitionDir(runRoot, symbol, dayIso));
  const handle = await openParquetWriter(FINAL_GREEKS_SCHEMA, finalPath);
  const riskFreeRate = parseNumberEnv('CALC_GREEKS_FALLBACK_RATE', DEFAULT_GREEKS_FALLBACK_RATE, env);
  const dividendYield = parseNumberEnv('CALC_GREEKS_DIVIDEND_YIELD', DEFAULT_GREEKS_DIVIDEND_YIELD, env);
  const ivLow = parseNumberEnv('CALC_GREEKS_IV_LOW', DEFAULT_GREEKS_IV_LOW, env);
  const ivHigh = parseNumberEnv('CALC_GREEKS_IV_HIGH', DEFAULT_GREEKS_IV_HIGH, env);
  const ivIterations = Math.max(1, Math.trunc(parseNumberEnv('CALC_GREEKS_IV_ITERATIONS', DEFAULT_GREEKS_IV_ITERATIONS, env)));
  const calcVersion = String(env.CALC_GREEKS_VERSION || 'bs_v1').trim() || 'bs_v1';
  let writtenRows = 0;
  try {
    for (const quotePath of quotePartFiles) {
      const reader = await parquet.ParquetReader.openFile(quotePath);
      const cursor = reader.getCursor();
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
    }
    await handle.close(true);
  } catch (error) {
    await handle.close(false);
    throw error;
  }
  const finalCalcPartitionDir = getFinalGreeksPartitionDir(runRoot, symbol, dayIso);
  const finalCalcPartCount = listParquetPartFiles(finalCalcPartitionDir).length;
  if (writtenRows > 0 && finalCalcPartCount === 0) {
    throw new Error(`calc_greeks_partition_missing_after_write:${symbol}:${dayIso}`);
  }
  if (finalCalcPartCount > 0) {
    writePartitionSuccessMarker(finalCalcPartitionDir, {
      stage: 'final_greeks_calculated',
      rowCount: writtenRows,
      partCount: finalCalcPartCount,
    });
  }
  return {
    writtenRows,
    finalPath,
  };
}

module.exports = {
  __private: {
    buildFallbackSessionWindow,
    parseThetaMaxConcurrentConnections,
    resolveProcessingSessionWindow,
    resolveThetaConnectionSlotsRoot,
    resolveThetaTimeWindowsForSymbol,
  },
  DEFAULT_INDEX_GREEKS_SYMBOLS,
  DEFAULT_SYMBOL_FILE,
  buildJobs,
  buildRunId,
  calculateGreeksToParquet,
  downloadIndexGreeksToParquet,
  downloadQuotesToParquet,
  downloadStockToParquet,
  ensureRunLayout,
  getFinalGreeksPartitionDir,
  getPartitionSuccessMarkerPath,
  getQuotePartitionDir,
  getRawGreeksPartitionDir,
  getStockPartitionDir,
  loadStockPartition,
  parseIndexGreeksSymbols,
  probeQuotePartition,
  probeRowPartition,
  probeStockPartition,
  resolveRunRoot,
  shardJobsBalanced,
  summarizeQuotePartition,
  writeJsonFile,
  writePartitionSuccessMarker,
};
