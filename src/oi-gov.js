const path = require('node:path');
const Database = require('better-sqlite3');

function resolveDbPath(env = process.env) {
  const configuredPath = env.PHENIX_DB_PATH || path.resolve(__dirname, '..', 'data', 'phenixflow.sqlite');
  return path.resolve(configuredPath);
}

function ensureOiGovSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS option_open_interest_reference (
      source TEXT NOT NULL,
      source_url TEXT,
      as_of_date TEXT NOT NULL,
      symbol TEXT NOT NULL,
      expiration TEXT NOT NULL,
      strike REAL NOT NULL,
      option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
      oi INTEGER NOT NULL CHECK (oi >= 0),
      raw_payload_json TEXT,
      ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      PRIMARY KEY (source, as_of_date, symbol, expiration, strike, option_right)
    );

    CREATE INDEX IF NOT EXISTS idx_option_oi_reference_symbol_asof
      ON option_open_interest_reference(symbol, as_of_date DESC);

    CREATE INDEX IF NOT EXISTS idx_option_oi_reference_source_asof
      ON option_open_interest_reference(source, as_of_date DESC);
  `);
}

function normalizeDate(rawValue) {
  if (typeof rawValue === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(rawValue.trim())) {
    return rawValue.trim();
  }

  if (typeof rawValue === 'string' && /^\d{8}$/.test(rawValue.trim())) {
    const value = rawValue.trim();
    return `${value.slice(0, 4)}-${value.slice(4, 6)}-${value.slice(6, 8)}`;
  }

  const parsed = new Date(rawValue);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function normalizeRight(rawValue) {
  if (typeof rawValue !== 'string') return null;
  const normalized = rawValue.trim().toUpperCase();
  if (normalized === 'CALL' || normalized === 'C') return 'CALL';
  if (normalized === 'PUT' || normalized === 'P') return 'PUT';
  return null;
}

function normalizeSymbol(rawValue) {
  if (typeof rawValue !== 'string') return null;
  const symbol = rawValue.trim().toUpperCase();
  return symbol || null;
}

function toNumber(rawValue) {
  const value = Number(rawValue);
  return Number.isFinite(value) ? value : null;
}

function toInteger(rawValue) {
  const value = Number(rawValue);
  return Number.isFinite(value) ? Math.trunc(value) : null;
}

function normalizeHeaderName(name) {
  return String(name || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function normalizeSource(source) {
  const normalized = String(source || '').trim().toUpperCase();
  if (!normalized) return '';

  if (normalized === 'CMEGROUP') return 'CME';
  return normalized;
}

function splitCsvLines(csvText) {
  const rows = [];
  let current = '';
  let inQuotes = false;

  for (let index = 0; index < csvText.length; index += 1) {
    const char = csvText[index];
    const next = csvText[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && next === '\n') index += 1;
      rows.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  rows.push(current);
  return rows.filter((line) => line.trim().length > 0);
}

function splitCsvFields(line) {
  const fields = [];
  let current = '';
  let inQuotes = false;

  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    const next = line[index + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      fields.push(current.trim());
      current = '';
      continue;
    }

    current += char;
  }

  fields.push(current.trim());
  return fields;
}

function parseCsvToObjects(csvText) {
  const lines = splitCsvLines(csvText || '');
  if (!lines.length) return [];

  const rawHeaders = splitCsvFields(lines[0]);
  const headers = rawHeaders.map((header) => normalizeHeaderName(header));

  const rows = [];
  lines.slice(1).forEach((line) => {
    const values = splitCsvFields(line);
    if (!values.length) return;

    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] || '';
    });
    rows.push(row);
  });

  return rows;
}

function parseJsonToObjects(rawText) {
  let parsed;
  try {
    parsed = JSON.parse(rawText);
  } catch {
    return [];
  }

  if (Array.isArray(parsed)) {
    return parsed.filter((row) => row && typeof row === 'object');
  }

  if (parsed && Array.isArray(parsed.rows)) {
    return parsed.rows.filter((row) => row && typeof row === 'object');
  }

  if (parsed && Array.isArray(parsed.data)) {
    return parsed.data.filter((row) => row && typeof row === 'object');
  }

  return [];
}

function pickValue(row, aliases) {
  const keys = Object.keys(row);
  for (const key of keys) {
    const normalized = normalizeHeaderName(key);
    if (!aliases.includes(normalized)) continue;
    const value = row[key];
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      return value;
    }
  }
  return null;
}

function normalizeOiRows(inputRows = [], {
  source,
  sourceUrl,
  asOfDate,
} = {}) {
  const normalizedSource = normalizeSource(source || 'GOV') || 'GOV';
  const defaultAsOfDate = normalizeDate(asOfDate);
  const accepted = [];
  const rejected = [];

  inputRows.forEach((rawRow) => {
    const symbol = normalizeSymbol(pickValue(rawRow, ['symbol', 'ticker', 'underlying', 'root', 'underlyingsymbol']));
    const expiration = normalizeDate(pickValue(rawRow, ['expiration', 'exp', 'expiry', 'expirationdate', 'expirationdt']));
    const strike = toNumber(pickValue(rawRow, ['strike', 'strikeprice', 'strikepx']));
    const right = normalizeRight(String(pickValue(rawRow, ['right', 'optionright', 'type', 'putcall', 'cp', 'optiontype']) || ''));
    const oi = toInteger(pickValue(rawRow, ['oi', 'openinterest', 'open_interest', 'openint']));
    const rowAsOfDate = normalizeDate(pickValue(rawRow, ['asofdate', 'as_of_date', 'date', 'tradedate', 'businessdate', 'tradingdate'])) || defaultAsOfDate;

    if (!symbol || !expiration || strike === null || !right || oi === null || oi < 0 || !rowAsOfDate) {
      rejected.push(rawRow);
      return;
    }

    accepted.push({
      source: normalizedSource,
      sourceUrl: sourceUrl || null,
      asOfDate: rowAsOfDate,
      symbol,
      expiration,
      strike,
      optionRight: right,
      oi,
      rawPayloadJson: JSON.stringify(rawRow),
    });
  });

  return {
    source: normalizedSource,
    accepted,
    rejected,
  };
}

function parseGovOiPayload(rawText, options = {}) {
  const trimmed = String(rawText || '').trim();
  if (!trimmed) return { source: normalizeSource(options.source || 'GOV') || 'GOV', accepted: [], rejected: [] };

  const jsonRows = parseJsonToObjects(trimmed);
  if (jsonRows.length) {
    return normalizeOiRows(jsonRows, options);
  }

  const csvRows = parseCsvToObjects(trimmed);
  return normalizeOiRows(csvRows, options);
}

function upsertOptionOiRows(db, rows = []) {
  ensureOiGovSchema(db);
  if (!rows.length) return 0;

  const upsert = db.prepare(`
    INSERT INTO option_open_interest_reference (
      source,
      source_url,
      as_of_date,
      symbol,
      expiration,
      strike,
      option_right,
      oi,
      raw_payload_json,
      ingested_at_utc
    ) VALUES (
      @source,
      @sourceUrl,
      @asOfDate,
      @symbol,
      @expiration,
      @strike,
      @optionRight,
      @oi,
      @rawPayloadJson,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(source, as_of_date, symbol, expiration, strike, option_right) DO UPDATE SET
      source_url = excluded.source_url,
      oi = excluded.oi,
      raw_payload_json = excluded.raw_payload_json,
      ingested_at_utc = excluded.ingested_at_utc
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

function resolveGovOiUrl(source, env = process.env) {
  const normalized = normalizeSource(source);
  if (!normalized) return null;

  if (normalized === 'FINRA') {
    return (env.GOV_OI_FINRA_URL || '').trim() || null;
  }

  if (normalized === 'CFTC') {
    return (env.GOV_OI_CFTC_URL || '').trim() || null;
  }

  if (normalized === 'CME') {
    return (env.GOV_OI_CME_URL || '').trim() || null;
  }

  return null;
}

function isBlockedResponse(bodyText = '') {
  const normalized = String(bodyText || '').toLowerCase();
  return normalized.includes('blocked')
    || normalized.includes('scraping')
    || normalized.includes('captcha')
    || normalized.includes('access denied')
    || normalized.includes('forbidden');
}

function buildFetchOptionsForSource(source, sourceUrl, env = process.env) {
  const normalized = normalizeSource(source);
  if (normalized !== 'CME') return {};

  const referer = (env.GOV_OI_CME_REFERER || 'https://www.cmegroup.com/market-data/browse-data/exchange-volume.html').trim();
  const userAgent = (env.GOV_OI_CME_USER_AGENT || 'Mozilla/5.0 (compatible; PhenixFlowOI/1.0)').trim();

  const headers = {
    Accept: 'text/csv,application/json,text/plain,*/*',
    Referer: referer,
    'User-Agent': userAgent,
  };

  if (sourceUrl && sourceUrl.startsWith('https://www.cmegroup.com')) {
    headers.Origin = 'https://www.cmegroup.com';
  }

  return { headers };
}

async function syncOptionOiFromGov({
  source,
  sourceUrl,
  asOfDate,
  env = process.env,
  fetchImpl = global.fetch,
}) {
  const normalizedSource = normalizeSource(source);
  if (!normalizedSource) {
    throw new Error('source_required');
  }

  const resolvedSourceUrl = (sourceUrl || '').trim() || resolveGovOiUrl(normalizedSource, env);
  if (!resolvedSourceUrl) {
    throw new Error('source_url_required');
  }

  if (typeof fetchImpl !== 'function') {
    throw new Error('fetch_unavailable');
  }

  const fetchOptions = buildFetchOptionsForSource(normalizedSource, resolvedSourceUrl, env);
  const response = await fetchImpl(resolvedSourceUrl, fetchOptions);
  const body = await response.text();

  if (!response.ok) {
    if (isBlockedResponse(body)) {
      throw new Error(`source_fetch_failed:${response.status}:blocked`);
    }
    throw new Error(`source_fetch_failed:${response.status}`);
  }

  if (isBlockedResponse(body)) {
    throw new Error('source_fetch_failed:200:blocked');
  }

  const parsed = parseGovOiPayload(body, {
    source: normalizedSource,
    sourceUrl: resolvedSourceUrl,
    asOfDate,
  });

  if (!parsed.accepted.length && !parsed.rejected.length) {
    throw new Error('source_payload_empty');
  }

  if (!parsed.accepted.length) {
    throw new Error('source_payload_unusable');
  }

  const dbPath = resolveDbPath(env);
  const db = new Database(dbPath);

  try {
    ensureOiGovSchema(db);
    const upsertedRows = upsertOptionOiRows(db, parsed.accepted);

    return {
      source: normalizedSource,
      sourceUrl: resolvedSourceUrl,
      asOfDate: normalizeDate(asOfDate) || null,
      fetchedRows: parsed.accepted.length + parsed.rejected.length,
      acceptedRows: parsed.accepted.length,
      rejectedRows: parsed.rejected.length,
      upsertedRows,
    };
  } finally {
    db.close();
  }
}

function parseLimit(rawValue, fallback = 100, max = 1000) {
  const parsed = Number(rawValue);
  if (!Number.isFinite(parsed) || parsed < 1) return fallback;
  return Math.min(max, Math.trunc(parsed));
}

function queryOptionOi(rawQuery = {}, env = process.env) {
  const dbPath = resolveDbPath(env);
  const db = new Database(dbPath, { readonly: true });

  try {
    ensureOiGovSchema(db);

    const where = [];
    const params = {};

    if (typeof rawQuery.symbol === 'string' && rawQuery.symbol.trim()) {
      where.push('symbol = @symbol');
      params.symbol = rawQuery.symbol.trim().toUpperCase();
    }

    const asOfDate = normalizeDate(rawQuery.asOfDate || rawQuery.date);
    if (asOfDate) {
      where.push('as_of_date = @asOfDate');
      params.asOfDate = asOfDate;
    }

    const expiration = normalizeDate(rawQuery.expiration);
    if (expiration) {
      where.push('expiration = @expiration');
      params.expiration = expiration;
    }

    const right = normalizeRight(String(rawQuery.right || ''));
    if (right) {
      where.push('option_right = @right');
      params.right = right;
    }

    const source = typeof rawQuery.source === 'string' && rawQuery.source.trim()
      ? rawQuery.source.trim().toUpperCase()
      : null;
    if (source) {
      where.push('source = @source');
      params.source = source;
    }

    const strike = toNumber(rawQuery.strike);
    if (strike !== null) {
      where.push('strike = @strike');
      params.strike = strike;
    }

    const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
    const limit = parseLimit(rawQuery.limit, 100, 2000);

    const rows = db.prepare(`
      SELECT
        source,
        source_url AS sourceUrl,
        as_of_date AS asOfDate,
        symbol,
        expiration,
        strike,
        option_right AS right,
        oi,
        ingested_at_utc AS ingestedAtUtc
      FROM option_open_interest_reference
      ${whereSql}
      ORDER BY as_of_date DESC, symbol ASC, expiration ASC, strike ASC, option_right ASC
      LIMIT @limit
    `).all({ ...params, limit });

    return {
      data: rows,
      meta: {
        source: 'sqlite',
        dbPath,
        filters: {
          symbol: params.symbol || null,
          asOfDate: params.asOfDate || null,
          expiration: params.expiration || null,
          right: params.right || null,
          source: params.source || null,
          strike: params.strike || null,
        },
        total: rows.length,
      },
    };
  } finally {
    db.close();
  }
}

function listOptionOiSources(env = process.env) {
  const dbPath = resolveDbPath(env);
  const db = new Database(dbPath, { readonly: true });

  try {
    ensureOiGovSchema(db);
    const rows = db.prepare(`
      SELECT
        source,
        as_of_date AS asOfDate,
        COUNT(*) AS rows,
        MAX(ingested_at_utc) AS lastIngestedAtUtc
      FROM option_open_interest_reference
      GROUP BY source, as_of_date
      ORDER BY as_of_date DESC, source ASC
      LIMIT 200
    `).all();

    return {
      data: rows,
      meta: {
        source: 'sqlite',
        dbPath,
        total: rows.length,
      },
    };
  } finally {
    db.close();
  }
}

function buildContractKey({ symbol, expiration, strike, right }) {
  return [symbol, expiration, strike, right].join('|');
}

function loadReferenceOiMap(db, { symbol, asOfDate }) {
  ensureOiGovSchema(db);

  const rows = db.prepare(`
    SELECT
      source,
      symbol,
      expiration,
      strike,
      option_right AS right,
      oi,
      ingested_at_utc AS ingestedAtUtc
    FROM option_open_interest_reference
    WHERE symbol = @symbol
      AND as_of_date = @asOfDate
    ORDER BY ingested_at_utc DESC
  `).all({ symbol, asOfDate });

  const map = new Map();
  rows.forEach((row) => {
    const key = buildContractKey(row);
    if (map.has(key)) return;
    map.set(key, toInteger(row.oi));
  });

  return map;
}

module.exports = {
  resolveDbPath,
  ensureOiGovSchema,
  parseGovOiPayload,
  normalizeOiRows,
  upsertOptionOiRows,
  queryOptionOi,
  listOptionOiSources,
  syncOptionOiFromGov,
  loadReferenceOiMap,
  __private: {
    normalizeDate,
    normalizeRight,
    normalizeSymbol,
    parseCsvToObjects,
    parseJsonToObjects,
    buildContractKey,
    resolveGovOiUrl,
  },
};
