#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');

const THETA_BASE = (process.env.THETADATA_BASE_URL || 'http://127.0.0.1:25503').replace(/\/$/, '');
const HISTORICAL_OPTION_PATH = (process.env.THETADATA_HISTORICAL_OPTION_PATH || '/v3/option/history/trade_quote').trim();
const CALENDAR_PATH = (process.env.THETADATA_CALENDAR_PATH || '/v3/calendar/on_date').trim();
const CONCURRENCY = Math.max(1, Number(process.env.OPTIONS_CONCURRENCY || 2));
const MAX_RETRIES = Math.max(1, Number(process.env.OPTIONS_MAX_RETRIES || 3));
const REQUEST_TIMEOUT_MS = Math.max(1000, Number(process.env.OPTIONS_TIMEOUT_MS || 120000));
const RETRY_BASE_MS = Math.max(250, Number(process.env.OPTIONS_RETRY_BASE_MS || 2000));
const RETRY_MAX_MS = Math.max(RETRY_BASE_MS, Number(process.env.OPTIONS_RETRY_MAX_MS || 30000));
const DAYS_BACK = Math.max(1, Number(process.env.OPTIONS_DAYS_BACK || 30));
const SYMBOL_LIMIT = Math.max(1, Number(process.env.OPTIONS_SYMBOL_LIMIT || 100));
const SKIP_CALENDAR_CHECK = String(process.env.OPTIONS_SKIP_CALENDAR_CHECK || '0') === '1';
const INCLUDE_TODAY = String(process.env.OPTIONS_INCLUDE_TODAY || '0') === '1';
const OVERWRITE_RAW = String(process.env.OPTIONS_OVERWRITE_RAW || '0') === '1';
const END_DATE_OVERRIDE = (process.env.OPTIONS_END_DATE || '').trim();
const DB_BUSY_TIMEOUT_MS = Math.max(1000, Number(process.env.OPTIONS_DB_BUSY_TIMEOUT_MS || 60000));
const RAW_ROOT = path.resolve(process.env.OPTIONS_RAW_ROOT || path.join(process.cwd(), 'data', 'options_storage', 'raw'));
const CURATED_ROOT = path.resolve(process.env.OPTIONS_CURATED_ROOT || path.join(process.cwd(), 'data', 'options_storage', 'curated'));
const SYMBOL_FILE = path.resolve(process.env.OPTIONS_SYMBOL_FILE || path.join(process.cwd(), 'config', 'top200-universe.json'));
const DB_PATH = path.resolve(process.env.OPTIONS_DB_PATH || path.join(CURATED_ROOT, 'curated', 'sqlite', 'options_trade_quote.sqlite'));

function parseStatusCodes(rawValue, fallbackCodes) {
  const parsed = String(rawValue || '')
    .split(',')
    .map((value) => Number.parseInt(value.trim(), 10))
    .filter((value) => Number.isInteger(value) && value >= 100 && value <= 599);
  return new Set(parsed.length ? parsed : fallbackCodes);
}

const STALE_HTTP_CODES = parseStatusCodes(process.env.OPTIONS_STALE_HTTP_CODES || '472', [472]);

function toIsoDay(d) {
  return d.toISOString().slice(0, 10);
}

function toYyyymmdd(dayIso) {
  return dayIso.replace(/-/g, '');
}

function toDateOnly(value) {
  const d = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) throw new Error(`invalid_date:${value}`);
  return d;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isDbLockError(error) {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('database is locked') || message.includes('database is busy');
}

async function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function dayRangeBack(daysBack, { includeToday, endDateOverride }) {
  let end = endDateOverride ? toDateOnly(endDateOverride) : new Date();
  if (!includeToday) {
    end = new Date(end.getTime() - 86400000);
  }
  const start = new Date(end.getTime() - ((daysBack - 1) * 86400000));
  const out = [];
  let cursor = new Date(start);
  while (cursor <= end) {
    const dow = cursor.getUTCDay();
    if (dow >= 1 && dow <= 5) {
      out.push(toIsoDay(cursor));
    }
    cursor = new Date(cursor.getTime() + 86400000);
  }
  return out;
}

async function isMarketOpenDay(dayIso) {
  const calendarPath = CALENDAR_PATH.startsWith('/') ? CALENDAR_PATH : `/${CALENDAR_PATH}`;
  const url = `${THETA_BASE}${calendarPath}?date=${toYyyymmdd(dayIso)}&format=json`;
  try {
    const response = await fetchWithTimeout(url, REQUEST_TIMEOUT_MS);
    if (!response.ok) return false;
    const text = await response.text();
    const parsed = JSON.parse(text);
    const type = Array.isArray(parsed?.type) ? String(parsed.type[0] || '').toLowerCase() : '';
    return type === 'open';
  } catch {
    return false;
  }
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

function normalizeRight(value) {
  if (typeof value !== 'string') return null;
  const normalized = value.trim().toUpperCase();
  if (normalized === 'CALL' || normalized === 'C') return 'CALL';
  if (normalized === 'PUT' || normalized === 'P') return 'PUT';
  return null;
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
      watermark: `top100-30d-${dayIso}`,
    };
  }).filter(Boolean);
}

function ensureStorageLayout(rawRoot, curatedRoot) {
  const dirs = [
    path.join(rawRoot, 'raw', 'thetadata', 'trade_quote', 'by_day'),
    path.join(rawRoot, 'raw', 'thetadata', 'trade_quote', 'by_symbol'),
    path.join(rawRoot, 'manifests', 'download_runs'),
    path.join(rawRoot, 'logs'),
    path.join(rawRoot, 'tmp'),
    path.join(curatedRoot, 'curated', 'sqlite'),
    path.join(curatedRoot, 'curated', 'parquet'),
    path.join(curatedRoot, 'curated', 'catalog'),
    path.join(curatedRoot, 'curated', 'reports'),
    path.join(curatedRoot, 'derived', 'features'),
    path.join(curatedRoot, 'derived', 'signals'),
    path.join(curatedRoot, 'logs'),
    path.join(curatedRoot, 'tmp'),
  ];
  dirs.forEach((target) => fs.mkdirSync(target, { recursive: true }));
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
      raw_file_path TEXT,
      PRIMARY KEY (symbol, trade_date_utc)
    );

    CREATE TABLE IF NOT EXISTS option_symbol_status (
      symbol TEXT PRIMARY KEY,
      status TEXT NOT NULL CHECK (status IN ('active', 'stale')),
      last_reason TEXT,
      first_seen_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      last_updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
  `);
}

function isDayCached(db, symbol, dayIso) {
  const row = db.prepare(`
    SELECT cache_status AS cacheStatus
    FROM option_trade_day_cache
    WHERE symbol = ? AND trade_date_utc = ?
  `).get(symbol, dayIso);
  return row && row.cacheStatus === 'full';
}

function upsertDayCache(db, {
  symbol,
  dayIso,
  cacheStatus,
  rowCount,
  lastError,
  sourceEndpoint,
  rawFilePath,
}) {
  db.prepare(`
    INSERT INTO option_trade_day_cache (
      symbol, trade_date_utc, cache_status, row_count,
      last_sync_at_utc, last_error, source_endpoint, raw_file_path
    ) VALUES (
      @symbol, @dayIso, @cacheStatus, @rowCount,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), @lastError, @sourceEndpoint, @rawFilePath
    )
    ON CONFLICT(symbol, trade_date_utc) DO UPDATE SET
      cache_status = excluded.cache_status,
      row_count = excluded.row_count,
      last_sync_at_utc = excluded.last_sync_at_utc,
      last_error = excluded.last_error,
      source_endpoint = excluded.source_endpoint,
      raw_file_path = excluded.raw_file_path
  `).run({
    symbol,
    dayIso,
    cacheStatus,
    rowCount,
    lastError,
    sourceEndpoint,
    rawFilePath,
  });
}

function countCachedRows(db, { symbol, dayIso }) {
  const from = `${dayIso}T00:00:00.000Z`;
  const to = `${dayIso}T23:59:59.999Z`;
  const row = db.prepare(`
    SELECT COUNT(*) AS count FROM option_trades
    WHERE trade_ts_utc >= @from AND trade_ts_utc <= @to AND symbol = @symbol
  `).get({ from, to, symbol });
  return Number(row?.count || 0);
}

function loadPersistedStaleSymbols(db) {
  const rows = db.prepare(`
    SELECT symbol
    FROM option_symbol_status
    WHERE status = 'stale'
  `).all();
  return new Set(rows.map((row) => String(row.symbol || '').trim().toUpperCase()).filter(Boolean));
}

function markSymbolStale(db, { symbol, reason }) {
  db.prepare(`
    INSERT INTO option_symbol_status (
      symbol,
      status,
      last_reason,
      first_seen_at_utc,
      last_updated_at_utc
    ) VALUES (
      @symbol,
      'stale',
      @reason,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(symbol) DO UPDATE SET
      status = 'stale',
      last_reason = excluded.last_reason,
      last_updated_at_utc = excluded.last_updated_at_utc
  `).run({ symbol, reason });
}

function loadSymbols(filePath, limit) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((symbol) => String(symbol || '').trim().toUpperCase())
      .filter(Boolean)
      .slice(0, limit);
  } catch {
    return [];
  }
}

function resolveDayFile(rawRoot, dayIso, symbol) {
  return path.join(
    rawRoot,
    'raw',
    'thetadata',
    'trade_quote',
    'by_day',
    `date=${dayIso}`,
    `symbol=${symbol}.json`,
  );
}

function resolveSymbolFile(rawRoot, dayIso, symbol) {
  return path.join(
    rawRoot,
    'raw',
    'thetadata',
    'trade_quote',
    'by_symbol',
    `symbol=${symbol}`,
    `date=${dayIso}.json`,
  );
}

function writeRawFiles(rawRoot, dayIso, symbol, rawBody) {
  const byDayFile = resolveDayFile(rawRoot, dayIso, symbol);
  const bySymbolFile = resolveSymbolFile(rawRoot, dayIso, symbol);

  if (!OVERWRITE_RAW && fs.existsSync(byDayFile) && fs.existsSync(bySymbolFile)) {
    return { byDayFile, bySymbolFile, skipped: true };
  }

  fs.mkdirSync(path.dirname(byDayFile), { recursive: true });
  fs.mkdirSync(path.dirname(bySymbolFile), { recursive: true });
  fs.writeFileSync(byDayFile, rawBody, 'utf8');
  fs.writeFileSync(bySymbolFile, rawBody, 'utf8');
  return { byDayFile, bySymbolFile, skipped: false };
}

async function syncOneDay(db, upsertStmt, { symbol, dayIso }) {
  const yyyymmdd = toYyyymmdd(dayIso);
  const normalizedPath = HISTORICAL_OPTION_PATH.startsWith('/') ? HISTORICAL_OPTION_PATH : `/${HISTORICAL_OPTION_PATH}`;
  const url = new URL(`${THETA_BASE}${normalizedPath}`);
  url.searchParams.set('symbol', symbol);
  url.searchParams.set('expiration', '*');
  url.searchParams.set('date', yyyymmdd);
  url.searchParams.set('format', 'json');
  const endpoint = url.toString();

  let lastError = null;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt += 1) {
    try {
      const started = Date.now();
      const response = await fetchWithTimeout(endpoint, REQUEST_TIMEOUT_MS);
      const body = await response.text();
      const durationMs = Date.now() - started;

      if (response.ok) {
        const fileWrites = writeRawFiles(RAW_ROOT, dayIso, symbol, body);
        const parsedRows = parseJsonRows(body);
        const normalizedRows = normalizeThetaRows(parsedRows, symbol, dayIso);

        const txn = db.transaction((rows) => {
          let writes = 0;
          rows.forEach((row) => {
            writes += upsertStmt.run(row).changes;
          });
          return writes;
        });
        const upsertedRows = txn(normalizedRows);
        const rowCount = countCachedRows(db, { symbol, dayIso });
        const cacheStatus = rowCount > 0 ? 'full' : 'partial';

        upsertDayCache(db, {
          symbol,
          dayIso,
          cacheStatus,
          rowCount,
          lastError: rowCount === 0 ? 'empty_response' : null,
          sourceEndpoint: endpoint,
          rawFilePath: fileWrites.byDayFile,
        });

        return {
          ok: true,
          symbol,
          dayIso,
          fetchedRows: normalizedRows.length,
          upsertedRows,
          totalRows: rowCount,
          durationMs,
          rawWriteSkipped: fileWrites.skipped,
        };
      }

      lastError = `http_${response.status}`;
      if (STALE_HTTP_CODES.has(response.status)) {
        upsertDayCache(db, {
          symbol,
          dayIso,
          cacheStatus: 'partial',
          rowCount: 0,
          lastError,
          sourceEndpoint: endpoint,
          rawFilePath: null,
        });
        return { ok: false, symbol, dayIso, error: lastError, stale: true };
      }

      const retryable = response.status >= 500 || response.status === 429;
      if (!retryable) break;
    } catch (error) {
      lastError = error.name === 'AbortError' ? 'timeout' : error.message;
    }

    const exponential = RETRY_BASE_MS * (2 ** (attempt - 1));
    const jitter = Math.floor(Math.random() * RETRY_BASE_MS);
    await sleep(Math.min(RETRY_MAX_MS, exponential + jitter));
  }

  try {
    upsertDayCache(db, {
      symbol,
      dayIso,
      cacheStatus: 'partial',
      rowCount: 0,
      lastError,
      sourceEndpoint: endpoint,
      rawFilePath: null,
    });
  } catch (error) {
    if (!isDbLockError(error)) throw error;
  }

  return { ok: false, symbol, dayIso, error: lastError };
}

function summarizeRoots() {
  return {
    rawRoot: RAW_ROOT,
    curatedRoot: CURATED_ROOT,
    dbPath: DB_PATH,
    dayRawPath: path.join(RAW_ROOT, 'raw', 'thetadata', 'trade_quote', 'by_day'),
    symbolRawPath: path.join(RAW_ROOT, 'raw', 'thetadata', 'trade_quote', 'by_symbol'),
    reportsPath: path.join(CURATED_ROOT, 'curated', 'reports'),
  };
}

async function runDay(db, upsertStmt, symbols, dayIso, report, staleSymbols, newStaleSymbols) {
  console.log(`\n=== DAY ${dayIso} (${symbols.length} symbols) ===`);
  const dayStart = Date.now();
  let nextSymbolIndex = 0;

  async function workerLoop() {
    while (true) {
      if (nextSymbolIndex >= symbols.length) return;
      const index = nextSymbolIndex;
      nextSymbolIndex += 1;
      const symbol = symbols[index];
      const prefix = `${dayIso} [${index + 1}/${symbols.length}] ${symbol}`;

      if (staleSymbols.has(symbol)) {
        report.staleSkippedJobs += 1;
        console.log(`${prefix} SKIP stale`);
        continue;
      }

      if (isDayCached(db, symbol, dayIso)) {
        report.skippedJobs += 1;
        console.log(`${prefix} SKIP cached`);
        continue;
      }

      let result = null;
      try {
        result = await syncOneDay(db, upsertStmt, { symbol, dayIso });
      } catch (error) {
        report.failedJobs += 1;
        report.failures.push({ symbol, dayIso, error: String(error?.message || error) });
        console.log(`${prefix} FAIL ${String(error?.message || error)}`);
        continue;
      }
      if (result.ok) {
        report.successJobs += 1;
        report.totalFetchedRows += result.fetchedRows;
        report.totalUpsertedRows += result.upsertedRows;
        if (result.rawWriteSkipped) {
          report.rawFileReusedJobs += 1;
        } else {
          report.rawFileWrittenJobs += 1;
        }
        console.log(`${prefix} OK fetched:${result.fetchedRows} upserted:${result.upsertedRows} total:${result.totalRows} ${result.durationMs}ms`);
      } else {
        if (result.stale) {
          staleSymbols.add(result.symbol);
          newStaleSymbols.add(result.symbol);
          markSymbolStale(db, { symbol: result.symbol, reason: result.error });
          report.staleMarkedJobs += 1;
          report.staleErrorCounts[result.error] = (report.staleErrorCounts[result.error] || 0) + 1;
          console.log(`${prefix} STALE ${result.error} (skip remaining days)`);
          continue;
        }

        report.failedJobs += 1;
        report.failures.push({ symbol: result.symbol, dayIso: result.dayIso, error: result.error });
        console.log(`${prefix} FAIL ${result.error}`);
      }
    }
  }

  const workerCount = Math.min(CONCURRENCY, symbols.length || 1);
  await Promise.all(Array.from({ length: workerCount }, () => workerLoop()));
  const elapsedMs = Date.now() - dayStart;
  console.log(`=== DAY ${dayIso} DONE in ${elapsedMs}ms ===`);
}

async function run() {
  if (!THETA_BASE) {
    throw new Error('THETADATA_BASE_URL missing');
  }

  ensureStorageLayout(RAW_ROOT, CURATED_ROOT);

  const symbols = loadSymbols(SYMBOL_FILE, SYMBOL_LIMIT);
  if (!symbols.length) {
    throw new Error(`No symbols found in ${SYMBOL_FILE}`);
  }

  const weekdays = dayRangeBack(DAYS_BACK, {
    includeToday: INCLUDE_TODAY,
    endDateOverride: END_DATE_OVERRIDE || null,
  });
  const days = [];
  if (SKIP_CALENDAR_CHECK) {
    days.push(...weekdays);
  } else {
    console.log(`Checking market calendar for ${weekdays.length} weekdays...`);
    for (const dayIso of weekdays) {
      if (await isMarketOpenDay(dayIso)) {
        days.push(dayIso);
      }
    }
  }

  if (!days.length) {
    throw new Error('No open-market days found in selected range');
  }

  const orderedDays = [...days].sort((a, b) => toDateOnly(b) - toDateOnly(a));
  const totalJobs = orderedDays.length * symbols.length;
  const roots = summarizeRoots();

  console.log('Starting day-first ingest (latest day first)');
  console.log(JSON.stringify({
    thetaBase: THETA_BASE,
    symbols: symbols.length,
    days: orderedDays.length,
    totalJobs,
    concurrency: CONCURRENCY,
    dbBusyTimeoutMs: DB_BUSY_TIMEOUT_MS,
    roots,
  }, null, 2));

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma(`busy_timeout = ${DB_BUSY_TIMEOUT_MS}`);
  ensureSchema(db);
  const staleSymbols = loadPersistedStaleSymbols(db);
  const staleSymbolsAtStart = Array.from(staleSymbols).sort();
  const newStaleSymbols = new Set();

  const upsertStmt = db.prepare(`
    INSERT INTO option_trades (
      trade_id, trade_ts_utc, trade_ts_et, symbol, expiration, strike,
      option_right, price, size, bid, ask, condition_code, exchange,
      raw_payload_json, watermark
    ) VALUES (
      @tradeId, @tradeTsUtc, @tradeTsEt, @symbol, @expiration, @strike,
      @optionRight, @price, @size, @bid, @ask, @conditionCode, @exchange,
      @rawPayloadJson, @watermark
    )
    ON CONFLICT(trade_id) DO UPDATE SET
      bid = excluded.bid,
      ask = excluded.ask,
      raw_payload_json = excluded.raw_payload_json,
      watermark = excluded.watermark
  `);

  const report = {
    startedAt: new Date().toISOString(),
    mode: 'top100_30d_day_first',
    thetaBase: THETA_BASE,
    historicalPath: HISTORICAL_OPTION_PATH,
    calendarPath: CALENDAR_PATH,
    symbols,
    symbolLimit: SYMBOL_LIMIT,
    daysBack: DAYS_BACK,
    includeToday: INCLUDE_TODAY,
    endDateOverride: END_DATE_OVERRIDE || null,
    openDays: orderedDays,
    totalJobs,
    concurrency: CONCURRENCY,
    skippedJobs: 0,
    staleSkippedJobs: 0,
    staleMarkedJobs: 0,
    staleErrorCounts: {},
    staleSymbolsAtStart,
    successJobs: 0,
    failedJobs: 0,
    rawFileWrittenJobs: 0,
    rawFileReusedJobs: 0,
    totalFetchedRows: 0,
    totalUpsertedRows: 0,
    roots,
    failures: [],
  };

  for (const dayIso of orderedDays) {
    await runDay(db, upsertStmt, symbols, dayIso, report, staleSymbols, newStaleSymbols);
  }

  report.staleSymbolsNew = Array.from(newStaleSymbols).sort();
  report.staleSymbolsActive = Array.from(staleSymbols).sort();
  db.close();

  report.completedAt = new Date().toISOString();
  report.durationSec = Math.round((new Date(report.completedAt).getTime() - new Date(report.startedAt).getTime()) / 1000);

  const reportFile = path.join(
    CURATED_ROOT,
    'curated',
    'reports',
    `top100-30d-run-${new Date().toISOString().replace(/[-:.]/g, '').slice(0, 15)}.json`,
  );
  fs.mkdirSync(path.dirname(reportFile), { recursive: true });
  fs.writeFileSync(reportFile, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  const latestFile = path.join(CURATED_ROOT, 'curated', 'reports', 'top100-30d-run-latest.json');
  fs.writeFileSync(latestFile, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  const manifestFile = path.join(RAW_ROOT, 'manifests', 'download_runs', `run-${Date.now()}.json`);
  fs.mkdirSync(path.dirname(manifestFile), { recursive: true });
  fs.writeFileSync(manifestFile, `${JSON.stringify({
    startedAt: report.startedAt,
    completedAt: report.completedAt,
    reportFile,
    latestFile,
    dbPath: DB_PATH,
    dayRawPath: roots.dayRawPath,
    symbolRawPath: roots.symbolRawPath,
  }, null, 2)}\n`, 'utf8');

  console.log('\n--- SUMMARY ---');
  console.log(JSON.stringify({
    totalJobs: report.totalJobs,
    skippedJobs: report.skippedJobs,
    staleSkippedJobs: report.staleSkippedJobs,
    staleMarkedJobs: report.staleMarkedJobs,
    successJobs: report.successJobs,
    failedJobs: report.failedJobs,
    rawFileWrittenJobs: report.rawFileWrittenJobs,
    rawFileReusedJobs: report.rawFileReusedJobs,
    totalFetchedRows: report.totalFetchedRows,
    totalUpsertedRows: report.totalUpsertedRows,
    dbPath: DB_PATH,
    reportFile,
    latestFile,
  }, null, 2));

  if (report.failedJobs > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
