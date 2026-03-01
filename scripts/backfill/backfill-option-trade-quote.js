#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');

// --- Config from env ---
const THETA_BASE = (process.env.THETADATA_BASE_URL || 'http://127.0.0.1:25503').replace(/\/$/, '');
const HISTORICAL_OPTION_PATH = (process.env.THETADATA_HISTORICAL_OPTION_PATH || '/v3/option/history/trade_quote').trim();
const CONCURRENCY = Math.max(1, Number(process.env.BACKFILL_CONCURRENCY || 2));
const MAX_RETRIES = Number(process.env.BACKFILL_MAX_RETRIES || 3);
const REQUEST_TIMEOUT_MS = Number(process.env.BACKFILL_TIMEOUT_MS || 120000);
const RETRY_BASE_MS = Math.max(250, Number(process.env.BACKFILL_RETRY_BASE_MS || 2000));
const RETRY_MAX_MS = Math.max(RETRY_BASE_MS, Number(process.env.BACKFILL_RETRY_MAX_MS || 30000));
const DB_PATH = process.env.PHENIX_DB_PATH
  || path.resolve(process.cwd(), 'data', 'phenixflow.sqlite');
const OUTPUT_PATH = process.env.BACKFILL_REPORT_PATH
  || path.resolve(process.cwd(), 'artifacts', 'reports', 'option-trade-quote-backfill.json');

const DEFAULT_SYMBOLS = [
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL',
  'META', 'TSLA', 'AVGO', 'NFLX', 'AMD',
];

// --- Date helpers ---
function toDateOnly(value) {
  const d = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(d.getTime())) throw new Error(`invalid_date:${value}`);
  return d;
}

function toIsoDay(d) {
  return d.toISOString().slice(0, 10);
}

function toYyyymmdd(dayIso) {
  return dayIso.replace(/-/g, '');
}

function* iterWeekdays(startIso, endIso) {
  let d = toDateOnly(startIso);
  const end = toDateOnly(endIso);
  while (d <= end) {
    const dow = d.getUTCDay();
    if (dow >= 1 && dow <= 5) {
      yield toIsoDay(d);
    }
    d = new Date(d.getTime() + 86400000);
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// --- Date range: last 30 calendar days ---
function defaultDateRange() {
  const end = new Date();
  const start = new Date(end.getTime() - 30 * 86400000);
  return {
    startDate: toIsoDay(start),
    endDate: toIsoDay(end),
  };
}

// --- HTTP ---
async function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// --- ThetaData calendar check ---
async function isMarketOpenDay(dayIso) {
  const url = `${THETA_BASE}/v3/calendar/on_date?date=${toYyyymmdd(dayIso)}&format=json`;
  try {
    const response = await fetchWithTimeout(url, REQUEST_TIMEOUT_MS);
    if (!response.ok) return false;
    const body = await response.text();
    const parsed = JSON.parse(body);
    const type = Array.isArray(parsed?.type) ? String(parsed.type[0] || '').toLowerCase() : '';
    return type === 'open';
  } catch {
    return false;
  }
}

// --- ThetaData response parsing (from historical-flow.js) ---
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
        header.forEach((k, index) => { out[k] = values[index]; });
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
          entries.forEach(([key, values]) => { out[key] = values[index]; });
          return out;
        });
      }
    }
  }
  return [];
}

// --- Row normalization (from historical-flow.js) ---
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
      watermark: `theta-sync-${dayIso}`,
    };
  }).filter(Boolean);
}

// --- Schema (from historical-flow.js) ---
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
  `);
}

// --- Day cache helpers ---
function isDayCached(db, symbol, dayIso) {
  const row = db.prepare(`
    SELECT cache_status AS cacheStatus
    FROM option_trade_day_cache
    WHERE symbol = ? AND trade_date_utc = ?
  `).get(symbol, dayIso);
  return row && row.cacheStatus === 'full';
}

function upsertDayCache(db, { symbol, dayIso, cacheStatus, rowCount, lastError, sourceEndpoint }) {
  db.prepare(`
    INSERT INTO option_trade_day_cache (
      symbol, trade_date_utc, cache_status, row_count,
      last_sync_at_utc, last_error, source_endpoint
    ) VALUES (
      @symbol, @dayIso, @cacheStatus, @rowCount,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), @lastError, @sourceEndpoint
    )
    ON CONFLICT(symbol, trade_date_utc) DO UPDATE SET
      cache_status = excluded.cache_status,
      row_count = excluded.row_count,
      last_sync_at_utc = excluded.last_sync_at_utc,
      last_error = excluded.last_error,
      source_endpoint = excluded.source_endpoint
  `).run({ symbol, dayIso, cacheStatus, rowCount, lastError, sourceEndpoint });
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

// --- Core: fetch + upsert one symbol+day ---
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
        const parsedRows = parseJsonRows(body);
        const normalizedRows = normalizeThetaRows(parsedRows, symbol, dayIso);

        const txn = db.transaction((rows) => {
          let writes = 0;
          rows.forEach((row) => { writes += upsertStmt.run(row).changes; });
          return writes;
        });
        const upsertedRows = txn(normalizedRows);

        const rowCount = countCachedRows(db, { symbol, dayIso });
        // Only mark as 'full' if we actually received rows. A 200 with 0 rows
        // could indicate a malformed response or API issue — mark as 'partial'
        // so the day gets retried on the next run.
        const cacheStatus = rowCount > 0 ? 'full' : 'partial';
        upsertDayCache(db, {
          symbol,
          dayIso,
          cacheStatus,
          rowCount,
          lastError: rowCount === 0 ? 'empty_response' : null,
          sourceEndpoint: endpoint,
        });

        return {
          ok: true,
          symbol,
          dayIso,
          fetchedRows: normalizedRows.length,
          upsertedRows,
          totalRows: rowCount,
          durationMs,
        };
      }

      lastError = `http_${response.status}`;
      const retryable = response.status >= 500 || response.status === 429;
      if (!retryable) break;
    } catch (error) {
      lastError = error.name === 'AbortError' ? 'timeout' : error.message;
    }

    const exponential = RETRY_BASE_MS * (2 ** (attempt - 1));
    const jitter = Math.floor(Math.random() * RETRY_BASE_MS);
    await sleep(Math.min(RETRY_MAX_MS, exponential + jitter));
  }

  upsertDayCache(db, {
    symbol,
    dayIso,
    cacheStatus: 'partial',
    rowCount: 0,
    lastError,
    sourceEndpoint: endpoint,
  });

  return { ok: false, symbol, dayIso, error: lastError };
}

// --- Main ---
async function run() {
  const symbols = process.env.BACKFILL_SYMBOLS
    ? process.env.BACKFILL_SYMBOLS.split(',').map((s) => s.trim().toUpperCase()).filter(Boolean)
    : DEFAULT_SYMBOLS;

  const { startDate: defaultStart, endDate: defaultEnd } = defaultDateRange();
  const startDate = process.env.BACKFILL_START_DATE || defaultStart;
  const endDate = process.env.BACKFILL_END_DATE || defaultEnd;

  const weekdays = Array.from(iterWeekdays(startDate, endDate));

  console.log(`Checking ThetaData calendar for ${weekdays.length} weekdays...`);
  const days = [];
  for (const dayIso of weekdays) {
    if (await isMarketOpenDay(dayIso)) {
      days.push(dayIso);
    } else {
      console.log(`  ${dayIso} — market closed (holiday), skipping`);
    }
  }

  console.log(`Backfill option trade_quote: ${symbols.length} symbols × ${days.length} trading days = ${symbols.length * days.length} jobs`);
  console.log(`Date range: ${startDate} → ${endDate} | Concurrency: ${CONCURRENCY}`);
  console.log(`DB: ${DB_PATH}`);

  // Open single shared DB connection
  fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  ensureSchema(db);

  // Prepare upsert statement once
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

  // Build job list
  const jobs = [];
  for (const symbol of symbols) {
    for (const dayIso of days) {
      jobs.push({ symbol, dayIso });
    }
  }

  const report = {
    startedAt: new Date().toISOString(),
    startDate,
    endDate,
    symbols,
    concurrency: CONCURRENCY,
    weekdayCount: days.length,
    totalJobs: jobs.length,
    skippedJobs: 0,
    successJobs: 0,
    failedJobs: 0,
    totalFetchedRows: 0,
    totalUpsertedRows: 0,
    failures: [],
  };

  let nextJobIndex = 0;
  async function workerLoop() {
    while (true) {
      if (nextJobIndex >= jobs.length) return;
      const currentIndex = nextJobIndex;
      nextJobIndex += 1;
      const job = jobs[currentIndex];
      const prefix = `[${currentIndex + 1}/${jobs.length}] ${job.symbol} ${job.dayIso}`;

      if (isDayCached(db, job.symbol, job.dayIso)) {
        report.skippedJobs += 1;
        console.log(`${prefix} SKIP cached`);
        continue;
      }

      const result = await syncOneDay(db, upsertStmt, job);

      if (result.ok) {
        report.successJobs += 1;
        report.totalFetchedRows += result.fetchedRows;
        report.totalUpsertedRows += result.upsertedRows;
        console.log(`${prefix} OK fetched:${result.fetchedRows} upserted:${result.upsertedRows} total:${result.totalRows} ${result.durationMs}ms`);
      } else {
        report.failedJobs += 1;
        report.failures.push({ symbol: result.symbol, dayIso: result.dayIso, error: result.error });
        console.log(`${prefix} FAIL ${result.error}`);
      }
    }
  }

  const workerCount = Math.min(CONCURRENCY, jobs.length || 1);
  await Promise.all(Array.from({ length: workerCount }, () => workerLoop()));

  db.close();

  report.completedAt = new Date().toISOString();

  // Write report
  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log('\n--- SUMMARY ---');
  console.log(JSON.stringify({
    totalJobs: report.totalJobs,
    skippedJobs: report.skippedJobs,
    successJobs: report.successJobs,
    failedJobs: report.failedJobs,
    totalFetchedRows: report.totalFetchedRows,
    totalUpsertedRows: report.totalUpsertedRows,
    reportPath: OUTPUT_PATH,
  }, null, 2));

  if (report.failedJobs > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
