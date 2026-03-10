#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const {
  queryRowsSync,
} = require('../../src/storage/clickhouse');

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith('--')) {
      out[key] = '1';
      continue;
    }
    out[key] = value;
    i += 1;
  }
  return out;
}

function parseTsv(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.split('\t'));
}

function normalizeDate(value) {
  if (typeof value !== 'string') return null;
  const raw = value.trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
}

function dayInRange(dayIso, fromIso, toIso) {
  return dayIso >= fromIso && dayIso <= toIso;
}

function monthKey(dayIso) {
  return String(dayIso).slice(0, 7);
}

function tsvEscape(value) {
  if (value === null || value === undefined) return '';
  return String(value);
}

function parseHmsToSecond(rawValue) {
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

function writeTsv(filePath, headers, rows) {
  const body = [headers.join('\t')]
    .concat(rows.map((row) => headers.map((header) => tsvEscape(row[header])).join('\t')))
    .join('\n');
  fs.writeFileSync(filePath, `${body}\n`, 'utf8');
}

function readCalendarMap(calendarPath) {
  const rows = parseTsv(calendarPath);
  if (rows.length === 0) return new Map();
  const [header, ...data] = rows;
  const idxDay = header.indexOf('day_iso');
  const idxType = header.indexOf('type');
  const idxExpected = header.indexOf('expected_slots');
  const idxOpenSec = header.indexOf('open_sec');
  const idxCloseSec = header.indexOf('close_sec');
  const idxOpen = header.indexOf('open');
  const idxClose = header.indexOf('close');
  if (idxDay < 0 || idxExpected < 0) {
    throw new Error(`calendar file missing required columns: ${calendarPath}`);
  }

  const map = new Map();
  data.forEach((cols) => {
    const dayIso = normalizeDate(cols[idxDay]);
    if (!dayIso) return;
    const expectedPaddedSlots = Number(cols[idxExpected] || 0);
    const openSecFromColumn = idxOpenSec >= 0 ? Number(cols[idxOpenSec] || NaN) : NaN;
    const closeSecFromColumn = idxCloseSec >= 0 ? Number(cols[idxCloseSec] || NaN) : NaN;
    const openSec = Number.isFinite(openSecFromColumn)
      ? openSecFromColumn
      : (idxOpen >= 0 ? parseHmsToSecond(cols[idxOpen]) : null);
    const closeSec = Number.isFinite(closeSecFromColumn)
      ? closeSecFromColumn
      : (idxClose >= 0 ? parseHmsToSecond(cols[idxClose]) : null);
    const expectedCoreSlots = Number.isFinite(openSec) && Number.isFinite(closeSec)
      ? Math.max(0, Math.floor((Number(closeSec) - Number(openSec)) / 60))
      : 0;
    map.set(dayIso, {
      dayIso,
      type: idxType >= 0 ? String(cols[idxType] || '').trim().toLowerCase() : '',
      expectedPaddedSlots: Number.isFinite(expectedPaddedSlots) ? Math.max(0, Math.trunc(expectedPaddedSlots)) : 0,
      expectedCoreSlots: Number.isFinite(expectedCoreSlots) ? Math.max(0, Math.trunc(expectedCoreSlots)) : 0,
    });
  });
  return map;
}

function readExpectedSymbolDays(symbolDaysPath, calendarMap, fromIso, toIso) {
  const rows = parseTsv(symbolDaysPath);
  const expected = [];
  rows.forEach((cols) => {
    if (cols.length < 2) return;
    const dayIso = normalizeDate(cols[0]);
    const symbol = String(cols[1] || '').trim().toUpperCase();
    if (!dayIso || !symbol) return;
    if (!dayInRange(dayIso, fromIso, toIso)) return;
    const calendar = calendarMap.get(dayIso);
    if (!calendar) return;
    expected.push({
      dayIso,
      symbol,
      month: monthKey(dayIso),
      type: calendar.type || '',
      expectedPaddedSlots: calendar.expectedPaddedSlots || 0,
      expectedCoreSlots: calendar.expectedCoreSlots || 0,
    });
  });
  return expected;
}

function buildSymbolInClause(symbols) {
  return symbols
    .map((symbol) => `'${symbol.replace(/'/g, "''")}'`)
    .join(',');
}

function queryMapFromRows(rows) {
  const out = new Map();
  rows.forEach((row) => {
    const dayIso = normalizeDate(String(row.day_iso || '').slice(0, 10));
    const symbol = String(row.symbol || '').trim().toUpperCase();
    if (!dayIso || !symbol) return;
    const key = `${dayIso}\t${symbol}`;
    out.set(key, {
      slots: Number(row.minute_slots || 0),
      rows: Number(row.row_count || 0),
    });
  });
  return out;
}

function queryRawComponentMap({
  tableName,
  dayColumn,
  minuteExpr,
  fromIso,
  toIso,
  symbolInClause,
}) {
  const rows = queryRowsSync(
    `
    SELECT
      toString(${dayColumn}) AS day_iso,
      symbol,
      uniqExact(${minuteExpr}) AS minute_slots,
      count() AS row_count
    FROM options.${tableName}
    WHERE ${dayColumn} BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
      AND symbol IN (${symbolInClause})
    GROUP BY day_iso, symbol
    `,
    { fromIso, toIso },
  );
  return queryMapFromRows(rows);
}

function queryDownloadStreamMap({
  streamName,
  fromIso,
  toIso,
  symbolInClause,
}) {
  const rows = queryRowsSync(
    `
    SELECT
      toString(trade_date_utc) AS day_iso,
      symbol,
      sum(minute_count) AS minute_slots,
      sum(row_count) AS row_count
    FROM options.option_download_chunk_status FINAL
    WHERE trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
      AND symbol IN (${symbolInClause})
      AND stream_name = {streamName:String}
    GROUP BY day_iso, symbol
    `,
    { fromIso, toIso, streamName },
  );

  return queryMapFromRows(rows);
}

function queryEnrichStreamMap({
  streamName,
  fromIso,
  toIso,
  symbolInClause,
}) {
  const rows = queryRowsSync(
    `
    SELECT
      toString(trade_date_utc) AS day_iso,
      symbol,
      sum(output_minute_count) AS minute_slots,
      sum(output_row_count) AS row_count
    FROM options.option_enrich_chunk_status FINAL
    WHERE trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
      AND symbol IN (${symbolInClause})
      AND stream_name = {streamName:String}
    GROUP BY day_iso, symbol
    `,
    { fromIso, toIso, streamName },
  );

  return queryMapFromRows(rows);
}

function queryDownloadAttemptMap({
  fromIso,
  toIso,
  symbolInClause,
}) {
  const rows = queryRowsSync(
    `
    SELECT
      toString(trade_date_utc) AS day_iso,
      symbol,
      max(stream_name = 'stock_price_1m') AS stock_attempted,
      max(stream_name = 'option_quote_1m') AS quote_attempted,
      max(stream_name = 'option_trade_quote_1m') AS trade_attempted
    FROM options.option_download_chunk_status FINAL
    WHERE trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
      AND symbol IN (${symbolInClause})
    GROUP BY day_iso, symbol
    `,
    { fromIso, toIso },
  );

  const out = new Map();
  rows.forEach((row) => {
    const dayIso = normalizeDate(String(row.day_iso || '').slice(0, 10));
    const symbol = String(row.symbol || '').trim().toUpperCase();
    if (!dayIso || !symbol) return;
    const key = `${dayIso}\t${symbol}`;
    out.set(key, {
      stockAttempted: Number(row.stock_attempted || 0) > 0,
      quoteAttempted: Number(row.quote_attempted || 0) > 0,
      tradeAttempted: Number(row.trade_attempted || 0) > 0,
    });
  });
  return out;
}

function queryEnrichAttemptMap({
  fromIso,
  toIso,
  symbolInClause,
}) {
  const rows = queryRowsSync(
    `
    SELECT
      toString(trade_date_utc) AS day_iso,
      symbol,
      max(1) AS enrich_attempted
    FROM options.option_enrich_chunk_status FINAL
    WHERE trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
      AND symbol IN (${symbolInClause})
      AND stream_name = 'option_trade_enriched_1m'
    GROUP BY day_iso, symbol
    `,
    { fromIso, toIso },
  );

  const out = new Map();
  rows.forEach((row) => {
    const dayIso = normalizeDate(String(row.day_iso || '').slice(0, 10));
    const symbol = String(row.symbol || '').trim().toUpperCase();
    if (!dayIso || !symbol) return;
    out.set(`${dayIso}\t${symbol}`, Number(row.enrich_attempted || 0) > 0);
  });
  return out;
}

function initAggregate() {
  return {
    expectedSymbolDays: 0,
    expectedPaddedSlots: 0,
    expectedCoreSlots: 0,
    stockSlots: 0,
    quoteSlots: 0,
    tradeSlots: 0,
    enrichSlots: 0,
    stockMissingSlots: 0,
    quoteMissingSlots: 0,
    tradeMissingSlots: 0,
    enrichVsTradeMissingSlots: 0,
    stockDaysMissing: 0,
    quoteDaysMissing: 0,
    tradeDaysMissing: 0,
    enrichDaysMissing: 0,
    stockAttemptedDays: 0,
    quoteAttemptedDays: 0,
    tradeAttemptedDays: 0,
    enrichAttemptedDays: 0,
    openSymbolDays: 0,
    earlyCloseSymbolDays: 0,
  };
}

function accumulate(agg, row) {
  agg.expectedSymbolDays += 1;
  agg.expectedPaddedSlots += row.expectedPaddedSlots;
  agg.expectedCoreSlots += row.expectedCoreSlots;
  agg.stockSlots += row.stockSlots;
  agg.quoteSlots += row.quoteSlots;
  agg.tradeSlots += row.tradeSlots;
  agg.enrichSlots += row.enrichSlots;
  agg.stockMissingSlots += row.stockMissingSlots;
  agg.quoteMissingSlots += row.quoteMissingSlots;
  agg.tradeMissingSlots += row.tradeMissingSlots;
  agg.enrichVsTradeMissingSlots += row.enrichVsTradeMissingSlots;
  agg.stockDaysMissing += row.stockMissingSlots > 0 ? 1 : 0;
  agg.quoteDaysMissing += row.quoteMissingSlots > 0 ? 1 : 0;
  agg.tradeDaysMissing += row.tradeMissingSlots > 0 ? 1 : 0;
  agg.enrichDaysMissing += row.enrichVsTradeMissingSlots > 0 ? 1 : 0;
  agg.stockAttemptedDays += row.stockAttempted ? 1 : 0;
  agg.quoteAttemptedDays += row.quoteAttempted ? 1 : 0;
  agg.tradeAttemptedDays += row.tradeAttempted ? 1 : 0;
  agg.enrichAttemptedDays += row.enrichAttempted ? 1 : 0;
  agg.openSymbolDays += row.type === 'open' ? 1 : 0;
  agg.earlyCloseSymbolDays += row.type === 'early_close' ? 1 : 0;
}

function main() {
  const args = parseArgs(process.argv);
  const symbolDaysPath = path.resolve(args['symbol-days']);
  const calendarPath = path.resolve(args.calendar);
  const fromIso = normalizeDate(args.from);
  const toIso = normalizeDate(args.to);
  const outDir = path.resolve(args['out-dir'] || path.resolve(process.cwd(), 'artifacts', 'reports'));
  const tag = String(args.tag || new Date().toISOString().replace(/[-:]/g, '').replace(/\..*/, '').replace('T', 'T'));
  const source = String(args.source || 'raw').trim().toLowerCase();
  const attemptedOnlyRaw = String(args['attempted-only'] || '1').trim().toLowerCase();
  const attemptedOnly = !(attemptedOnlyRaw === '0' || attemptedOnlyRaw === 'false' || attemptedOnlyRaw === 'no');

  if (!symbolDaysPath || !calendarPath || !fromIso || !toIso) {
    throw new Error('usage: --symbol-days <tsv> --calendar <tsv> --from YYYY-MM-DD --to YYYY-MM-DD [--out-dir dir] [--tag tag]');
  }

  const calendarMap = readCalendarMap(calendarPath);
  const expectedRows = readExpectedSymbolDays(symbolDaysPath, calendarMap, fromIso, toIso);
  if (expectedRows.length === 0) {
    throw new Error('no expected symbol-days found for the requested range');
  }

  const symbols = Array.from(new Set(expectedRows.map((row) => row.symbol))).sort();
  const symbolInClause = buildSymbolInClause(symbols);
  const downloadAttemptMap = queryDownloadAttemptMap({
    fromIso,
    toIso,
    symbolInClause,
  });
  const enrichAttemptMap = queryEnrichAttemptMap({
    fromIso,
    toIso,
    symbolInClause,
  });

  const stockMap = source === 'chunk'
    ? queryDownloadStreamMap({
      streamName: 'stock_price_1m',
      fromIso,
      toIso,
      symbolInClause,
    })
    : queryRawComponentMap({
      tableName: 'stock_ohlc_minute_raw',
      dayColumn: 'trade_date_utc',
      minuteExpr: 'minute_bucket_utc',
      fromIso,
      toIso,
      symbolInClause,
    });

  const quoteMap = source === 'chunk'
    ? queryDownloadStreamMap({
      streamName: 'option_quote_1m',
      fromIso,
      toIso,
      symbolInClause,
    })
    : queryRawComponentMap({
      tableName: 'option_quote_minute_raw',
      dayColumn: 'trade_date_utc',
      minuteExpr: 'minute_bucket_utc',
      fromIso,
      toIso,
      symbolInClause,
    });

  const tradeMap = source === 'chunk'
    ? queryDownloadStreamMap({
      streamName: 'option_trade_quote_1m',
      fromIso,
      toIso,
      symbolInClause,
    })
    : queryRawComponentMap({
      tableName: 'option_trades',
      dayColumn: 'trade_date',
      minuteExpr: 'toStartOfMinute(trade_ts_utc)',
      fromIso,
      toIso,
      symbolInClause,
    });

  const enrichMap = source === 'chunk'
    ? queryEnrichStreamMap({
      streamName: 'option_trade_enriched_1m',
      fromIso,
      toIso,
      symbolInClause,
    })
    : queryRawComponentMap({
      tableName: 'option_trade_enriched',
      dayColumn: 'trade_date',
      minuteExpr: 'toStartOfMinute(trade_ts_utc)',
      fromIso,
      toIso,
      symbolInClause,
    });

  const detailedRows = expectedRows.map((row) => {
    const key = `${row.dayIso}\t${row.symbol}`;
    const attempts = downloadAttemptMap.get(key) || {};
    const stockAttempted = Boolean(attempts.stockAttempted);
    const quoteAttempted = Boolean(attempts.quoteAttempted);
    const tradeAttempted = Boolean(attempts.tradeAttempted);
    const enrichAttempted = Boolean(enrichAttemptMap.get(key));
    const stockSlots = Number(stockMap.get(key)?.slots || 0);
    const quoteSlots = Number(quoteMap.get(key)?.slots || 0);
    const tradeSlots = Number(tradeMap.get(key)?.slots || 0);
    const enrichSlots = Number(enrichMap.get(key)?.slots || 0);
    const stockMissingSlots = (!attemptedOnly || stockAttempted)
      ? Math.max(0, row.expectedPaddedSlots - stockSlots)
      : 0;
    const quoteMissingSlots = (!attemptedOnly || quoteAttempted)
      ? Math.max(0, row.expectedCoreSlots - quoteSlots)
      : 0;
    const tradeMissingSlots = (!attemptedOnly || tradeAttempted)
      ? Math.max(0, row.expectedCoreSlots - tradeSlots)
      : 0;
    const enrichVsTradeMissingSlots = (!attemptedOnly || enrichAttempted)
      ? Math.max(0, tradeSlots - enrichSlots)
      : 0;

    return {
      dayIso: row.dayIso,
      month: row.month,
      symbol: row.symbol,
      type: row.type,
      stockAttempted,
      quoteAttempted,
      tradeAttempted,
      enrichAttempted,
      expectedPaddedSlots: row.expectedPaddedSlots,
      expectedCoreSlots: row.expectedCoreSlots,
      stockSlots,
      quoteSlots,
      tradeSlots,
      enrichSlots,
      stockMissingSlots,
      quoteMissingSlots,
      tradeMissingSlots,
      enrichVsTradeMissingSlots,
    };
  });

  const monthAgg = new Map();
  const symbolAgg = new Map();
  detailedRows.forEach((row) => {
    if (!monthAgg.has(row.month)) monthAgg.set(row.month, initAggregate());
    accumulate(monthAgg.get(row.month), row);

    if (!symbolAgg.has(row.symbol)) symbolAgg.set(row.symbol, initAggregate());
    accumulate(symbolAgg.get(row.symbol), row);
  });

  fs.mkdirSync(outDir, { recursive: true });

  const monthRows = Array.from(monthAgg.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([month, agg]) => ({ month, ...agg }));
  const symbolRows = Array.from(symbolAgg.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([symbol, agg]) => ({ symbol, ...agg }));
  const anomalyRows = detailedRows
    .filter((row) => (
      row.stockMissingSlots > 0
      || row.quoteMissingSlots > 0
      || row.enrichVsTradeMissingSlots > 0
    ))
    .sort((a, b) => (
      a.dayIso.localeCompare(b.dayIso)
      || a.symbol.localeCompare(b.symbol)
    ));

  const base = `coverage-1m-${fromIso}-${toIso}-${tag}`;
  const monthPath = path.join(outDir, `${base}-month-summary.tsv`);
  const symbolPath = path.join(outDir, `${base}-symbol-summary.tsv`);
  const anomalyPath = path.join(outDir, `${base}-anomalies.tsv`);

  writeTsv(monthPath, [
    'month',
    'expectedSymbolDays',
    'expectedPaddedSlots',
    'expectedCoreSlots',
    'stockSlots',
    'quoteSlots',
    'tradeSlots',
    'enrichSlots',
    'stockMissingSlots',
    'quoteMissingSlots',
    'tradeMissingSlots',
    'enrichVsTradeMissingSlots',
    'stockDaysMissing',
    'quoteDaysMissing',
    'tradeDaysMissing',
    'enrichDaysMissing',
    'stockAttemptedDays',
    'quoteAttemptedDays',
    'tradeAttemptedDays',
    'enrichAttemptedDays',
    'openSymbolDays',
    'earlyCloseSymbolDays',
  ], monthRows);

  writeTsv(symbolPath, [
    'symbol',
    'expectedSymbolDays',
    'expectedPaddedSlots',
    'expectedCoreSlots',
    'stockSlots',
    'quoteSlots',
    'tradeSlots',
    'enrichSlots',
    'stockMissingSlots',
    'quoteMissingSlots',
    'tradeMissingSlots',
    'enrichVsTradeMissingSlots',
    'stockDaysMissing',
    'quoteDaysMissing',
    'tradeDaysMissing',
    'enrichDaysMissing',
    'stockAttemptedDays',
    'quoteAttemptedDays',
    'tradeAttemptedDays',
    'enrichAttemptedDays',
    'openSymbolDays',
    'earlyCloseSymbolDays',
  ], symbolRows);

  writeTsv(anomalyPath, [
    'dayIso',
    'month',
    'symbol',
    'type',
    'stockAttempted',
    'quoteAttempted',
    'tradeAttempted',
    'enrichAttempted',
    'expectedPaddedSlots',
    'expectedCoreSlots',
    'stockSlots',
    'quoteSlots',
    'tradeSlots',
    'enrichSlots',
    'stockMissingSlots',
    'quoteMissingSlots',
    'tradeMissingSlots',
    'enrichVsTradeMissingSlots',
  ], anomalyRows);

  console.log(`Month summary: ${monthPath}`);
  console.log(`Symbol summary: ${symbolPath}`);
  console.log(`Anomalies: ${anomalyPath}`);
  console.log(`Expected symbol-days: ${detailedRows.length}`);
  console.log(`Anomaly rows: ${anomalyRows.length}`);
  console.log(`Source: ${source}`);
  console.log(`Attempted only: ${attemptedOnly}`);
}

main();
