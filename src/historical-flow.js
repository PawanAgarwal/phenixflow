const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;
const DAY_CACHE_STATUS_FULL = 'full';
const DAY_CACHE_STATUS_PARTIAL = 'partial';

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

function normalizeIsoTimestamp(rawValue) {
  if (typeof rawValue !== 'string' || !rawValue.trim()) return null;
  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
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

function normalizeRight(raw) {
  if (typeof raw !== 'string') return null;
  const value = raw.trim().toUpperCase();
  if (value === 'CALL' || value === 'C') return 'CALL';
  if (value === 'PUT' || value === 'P') return 'PUT';
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

  // Theta often returns timestamps without timezone. Treat as UTC for deterministic storage.
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

  const configuredPath = (env.THETADATA_HISTORICAL_OPTION_PATH || '/v3/option/history/trade_quote').trim();
  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = configuredPath.startsWith('/') ? configuredPath : `/${configuredPath}`;

  const url = new URL(`${normalizedBase}${normalizedPath}`);
  url.searchParams.set('symbol', symbol);
  url.searchParams.set('expiration', '*');
  url.searchParams.set('date', yyyymmdd);
  url.searchParams.set('format', 'json');
  return url.toString();
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

  const response = await fetch(endpoint);
  const body = await response.text();

  if (!response.ok) {
    throw new Error(`thetadata_request_failed:${response.status}`);
  }

  const parsedRows = parseJsonRows(body);
  const normalizedRows = normalizeThetaRows(parsedRows, symbol, dayIso);
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

  let readDb;
  try {
    readDb = new Database(dbPath);
    ensureSchema(readDb);
  } catch {
    return {
      status: 503,
      error: {
        code: 'db_unavailable',
        message: `Historical DB is not available at ${dbPath}.`,
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
  const cachedRows = countCachedRows(readDb, { from, to, symbol });
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

  try {
    const data = readDb.prepare(`
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
        exchange
      FROM option_trades
      WHERE trade_ts_utc >= @from
        AND trade_ts_utc <= @to
        AND symbol = @symbol
      ORDER BY trade_ts_utc ASC, trade_id ASC
      LIMIT @limit
    `).all({ from, to, symbol, limit });

    return {
      data,
      meta: {
        source: 'sqlite',
        dbPath,
        dateRange: { from, to },
        filter: { symbol },
        total: data.length,
        sync,
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
    countCachedRows,
    upsertDayCache,
    getDayCache,
    DAY_CACHE_STATUS_FULL,
    DAY_CACHE_STATUS_PARTIAL,
  },
};
