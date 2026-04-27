const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const zlib = require('node:zlib');

const DEFAULT_MASSIVE_DATA_ROOT = path.join('/Volumes', 'SEC4TB', 'massive-data');
const DEFAULT_STOCK_DATASET_ID = 'stock_quotes_1m';
const DEFAULT_INDEX_DATASET_ID = 'indices_1m';
const ET_TIME_ZONE = 'America/New_York';
const REGULAR_SESSION_START_MINUTES = (9 * 60) + 30;
const REGULAR_SESSION_END_MINUTES = 16 * 60;

const INDEX_SYMBOL_TO_MASSIVE_TICKER = new Map([
  ['SPX', 'I:SPX'],
  ['VIX', 'I:VIX'],
  ['VIX1D', 'I:VIX1D'],
  ['VIX3M', 'I:VIX3M'],
  ['VIX9D', 'I:VIX9D'],
]);

const ET_OFFSET_FORMATTER = new Intl.DateTimeFormat('en-US', {
  timeZone: ET_TIME_ZONE,
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
  timeZoneName: 'shortOffset',
});

function normalizeIsoDate(rawValue) {
  if (typeof rawValue !== 'string') return null;
  const trimmed = rawValue.trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? trimmed : null;
}

function parseNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeSymbols(symbols = []) {
  return Array.from(new Set(
    symbols
      .map((symbol) => String(symbol || '').trim().toUpperCase())
      .filter(Boolean),
  ));
}

function listDatesInRange(startDate, endDate) {
  const out = [];
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  while (cursor <= end) {
    out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

function resolveMassiveDataRoot(env = process.env) {
  const configured = String(
    env.MASSIVE_DATA_ROOT
    || env.PHENIXFLOW_MASSIVE_DATA_ROOT
    || '',
  ).trim();
  if (configured) return path.resolve(configured);
  return DEFAULT_MASSIVE_DATA_ROOT;
}

function resolveMassiveDatasetRoots(env = process.env) {
  const dataRoot = resolveMassiveDataRoot(env);
  return {
    dataRoot,
    stockDatasetRoot: path.resolve(
      String(env.MASSIVE_STOCK_DATASET_ROOT || path.join(dataRoot, 'massive', DEFAULT_STOCK_DATASET_ID)).trim(),
    ),
    indexDatasetRoot: path.resolve(
      String(env.MASSIVE_INDEX_DATASET_ROOT || path.join(dataRoot, 'massive', DEFAULT_INDEX_DATASET_ID)).trim(),
    ),
  };
}

function resolveDayFilePath(datasetRoot, dateIso) {
  const dayDir = path.join(datasetRoot, `date=${dateIso}`);
  const gzipPath = path.join(dayDir, `${dateIso}.csv.gz`);
  if (fs.existsSync(gzipPath)) return gzipPath;
  const plainPath = path.join(dayDir, `${dateIso}.csv`);
  if (fs.existsSync(plainPath)) return plainPath;
  return null;
}

function parseOffsetMinutes(partValue) {
  const match = String(partValue || '').trim().match(/^(?:GMT|UTC)([+-])(\d{1,2})(?::?(\d{2}))?$/i);
  if (!match) return 0;
  const sign = match[1] === '-' ? -1 : 1;
  return sign * ((Number(match[2]) * 60) + Number(match[3] || 0));
}

function resolveRegularSessionBoundsUtcMs(dateIso) {
  const noonUtc = new Date(`${dateIso}T12:00:00.000Z`);
  const offsetPart = ET_OFFSET_FORMATTER
    .formatToParts(noonUtc)
    .find((part) => part.type === 'timeZoneName');
  const offsetMinutes = parseOffsetMinutes(offsetPart?.value);
  const startMinutesUtc = REGULAR_SESSION_START_MINUTES - offsetMinutes;
  const endMinutesUtc = REGULAR_SESSION_END_MINUTES - offsetMinutes;
  const dayStartMs = Date.parse(`${dateIso}T00:00:00.000Z`);
  return {
    startMs: dayStartMs + (startMinutesUtc * 60 * 1000),
    endMs: dayStartMs + (endMinutesUtc * 60 * 1000),
  };
}

function timestampNsToUtcMs(rawValue) {
  if (typeof rawValue !== 'string' && typeof rawValue !== 'number' && typeof rawValue !== 'bigint') return null;
  try {
    const ns = BigInt(String(rawValue).trim());
    return Number(ns / 1000000n);
  } catch {
    return null;
  }
}

function minuteUtcFromMs(utcMs) {
  if (!Number.isFinite(utcMs)) return null;
  return new Date(utcMs).toISOString();
}

function parseStockAggregateLine(line, dateIso, sessionBounds, targets) {
  if (!line || line.startsWith('ticker,')) return null;
  const fields = line.split(',');
  if (fields.length < 8) return null;

  const ticker = String(fields[0] || '').trim().toUpperCase();
  if (!targets.has(ticker)) return null;

  const utcMs = timestampNsToUtcMs(fields[6]);
  if (utcMs === null || utcMs < sessionBounds.startMs || utcMs >= sessionBounds.endMs) return null;

  return {
    symbol: ticker,
    tradeDateUtc: dateIso,
    minuteUtc: minuteUtcFromMs(utcMs),
    open: parseNumber(fields[2]),
    high: parseNumber(fields[4]),
    low: parseNumber(fields[5]),
    close: parseNumber(fields[3]),
    volume: parseNumber(fields[1]),
  };
}

function parseIndexAggregateLine(line, dateIso, sessionBounds, targets) {
  if (!line) return null;
  const fields = line.split(',');
  if (fields.length < 6) return null;

  const rawTicker = String(fields[0] || '').trim().toUpperCase();
  if (!targets.has(rawTicker)) return null;

  const utcMs = timestampNsToUtcMs(fields[5]);
  if (utcMs === null || utcMs < sessionBounds.startMs || utcMs >= sessionBounds.endMs) return null;

  const normalizedSymbol = rawTicker.startsWith('I:') ? rawTicker.slice(2) : rawTicker;
  return {
    symbol: normalizedSymbol,
    tradeDateUtc: dateIso,
    minuteUtc: minuteUtcFromMs(utcMs),
    open: parseNumber(fields[1]),
    high: parseNumber(fields[3]),
    low: parseNumber(fields[4]),
    close: parseNumber(fields[2]),
    volume: null,
  };
}

async function loadDatasetRows({ datasetRoot, dates, parseLine }) {
  const rows = [];
  for (const dateIso of dates) {
    const dayFilePath = resolveDayFilePath(datasetRoot, dateIso);
    if (!dayFilePath) continue;

    const baseStream = fs.createReadStream(dayFilePath);
    const input = dayFilePath.endsWith('.gz')
      ? baseStream.pipe(zlib.createGunzip())
      : baseStream;
    const reader = readline.createInterface({ input, crlfDelay: Infinity });
    const sessionBounds = resolveRegularSessionBoundsUtcMs(dateIso);

    try {
      for await (const line of reader) {
        const row = parseLine(line, dateIso, sessionBounds);
        if (row) rows.push(row);
      }
    } finally {
      reader.close();
    }
  }
  return rows;
}

async function loadMassiveMinuteRows(options = {}) {
  const startDate = normalizeIsoDate(options.startDate);
  const endDate = normalizeIsoDate(options.endDate);
  if (!startDate || !endDate) {
    throw new Error(`invalid_date_range:${options.startDate || ''}:${options.endDate || ''}`);
  }
  if (startDate > endDate) {
    throw new Error(`invalid_date_range:${startDate}:${endDate}`);
  }

  const requiredSymbols = normalizeSymbols(options.requiredSymbols || []);
  if (!requiredSymbols.length) return [];

  const env = options.env || process.env;
  const {
    stockDatasetRoot,
    indexDatasetRoot,
  } = resolveMassiveDatasetRoots(env);
  const dates = listDatesInRange(startDate, endDate);

  const stockTargets = new Set(requiredSymbols.filter((symbol) => !INDEX_SYMBOL_TO_MASSIVE_TICKER.has(symbol)));
  const indexTargets = new Set(
    requiredSymbols
      .filter((symbol) => INDEX_SYMBOL_TO_MASSIVE_TICKER.has(symbol))
      .map((symbol) => INDEX_SYMBOL_TO_MASSIVE_TICKER.get(symbol)),
  );

  const [stockRows, indexRows] = await Promise.all([
    stockTargets.size
      ? loadDatasetRows({
        datasetRoot: stockDatasetRoot,
        dates,
        parseLine: (line, dateIso, sessionBounds) => parseStockAggregateLine(line, dateIso, sessionBounds, stockTargets),
      })
      : Promise.resolve([]),
    indexTargets.size
      ? loadDatasetRows({
        datasetRoot: indexDatasetRoot,
        dates,
        parseLine: (line, dateIso, sessionBounds) => parseIndexAggregateLine(line, dateIso, sessionBounds, indexTargets),
      })
      : Promise.resolve([]),
  ]);

  const rows = stockRows.concat(indexRows);
  rows.sort((left, right) => {
    if (left.symbol !== right.symbol) return left.symbol.localeCompare(right.symbol);
    return left.minuteUtc.localeCompare(right.minuteUtc);
  });
  return rows;
}

module.exports = {
  DEFAULT_MASSIVE_DATA_ROOT,
  DEFAULT_STOCK_DATASET_ID,
  DEFAULT_INDEX_DATASET_ID,
  resolveMassiveDataRoot,
  resolveMassiveDatasetRoots,
  loadMassiveMinuteRows,
  __private: {
    normalizeIsoDate,
    normalizeSymbols,
    listDatesInRange,
    resolveDayFilePath,
    resolveRegularSessionBoundsUtcMs,
    timestampNsToUtcMs,
    parseStockAggregateLine,
    parseIndexAggregateLine,
  },
};
