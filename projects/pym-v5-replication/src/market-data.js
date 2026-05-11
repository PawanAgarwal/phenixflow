const fs = require('node:fs');
const { spawn } = require('node:child_process');
const readline = require('node:readline');

const { closeMinuteEt } = require('./calendar');
const { readGzipCsv, toNumber } = require('./csv');
const { stockCsvPath, stockParquetPath, stockSuccessPath } = require('./config');

const etFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  hour: '2-digit',
  minute: '2-digit',
  hourCycle: 'h23',
});

function minuteEtFromNs(nsValue) {
  const ms = Math.floor(Number(nsValue) / 1e6);
  if (!Number.isFinite(ms)) return null;
  const parts = etFormatter.formatToParts(new Date(ms));
  const hour = Number(parts.find((part) => part.type === 'hour')?.value);
  const minute = Number(parts.find((part) => part.type === 'minute')?.value);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  return (hour * 60) + minute;
}

function emptyAgg(ticker, date) {
  return {
    date,
    ticker,
    open: null,
    high: null,
    low: null,
    close: null,
    volume: 0,
    transactions: 0,
    regularMinuteCount: 0,
  };
}

function updateAgg(agg, row) {
  const open = toNumber(row.open);
  const high = toNumber(row.high);
  const low = toNumber(row.low);
  const close = toNumber(row.close);
  if (agg.open === null) agg.open = open;
  if (Number.isFinite(high)) agg.high = agg.high === null ? high : Math.max(agg.high, high);
  if (Number.isFinite(low)) agg.low = agg.low === null ? low : Math.min(agg.low, low);
  agg.close = close;
  agg.volume += toNumber(row.volume) || 0;
  agg.transactions += toNumber(row.transactions) || 0;
  agg.regularMinuteCount += 1;
}

function resolveStockBarsSource(config, dayIso) {
  const parquetPath = stockParquetPath(config, dayIso);
  if (parquetPath && fs.existsSync(parquetPath)) {
    return {
      format: 'parquet',
      filePath: parquetPath,
      preferredFilePath: parquetPath,
    };
  }
  const csvPath = stockCsvPath(config, dayIso);
  if (fs.existsSync(csvPath)) {
    return {
      format: 'csv.gz',
      filePath: csvPath,
      preferredFilePath: parquetPath || csvPath,
    };
  }
  return {
    format: 'missing',
    filePath: parquetPath || csvPath,
    preferredFilePath: parquetPath || csvPath,
    fallbackFilePath: csvPath,
  };
}

function duckdbString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function stockParquetSql(filePath) {
  return `COPY (
    SELECT
      ticker,
      volume,
      open,
      close,
      high,
      low,
      window_start,
      COALESCE(CAST(transactions AS VARCHAR), '0') AS transactions
    FROM read_parquet(${duckdbString(filePath)})
  ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`;
}

async function streamParquetStockRows(filePath, onRow) {
  const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', stockParquetSql(filePath)], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const stderr = [];
  child.stderr.on('data', (chunk) => {
    stderr.push(String(chunk));
  });

  const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  let headers = null;
  for await (const line of reader) {
    if (!line) continue;
    if (!headers) {
      headers = String(line).split(',');
      continue;
    }
    const values = String(line).split(',');
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });
    await onRow(row);
  }

  const code = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', resolve);
  });
  if (code !== 0) {
    throw new Error(`duckdb_stock_parquet_read_failed:${filePath}:${stderr.join('').trim() || code}`);
  }
}

async function readDailyBarsForDay(config, day, tickers) {
  const source = resolveStockBarsSource(config, day.date);
  const selected = new Set([...tickers].map((ticker) => ticker.toUpperCase()));
  const closeMinute = closeMinuteEt(day);
  const bars = new Map();
  if (source.format === 'missing') return bars;

  function onRow(row) {
    const ticker = String(row.ticker || '').toUpperCase();
    if (!selected.has(ticker)) return;
    const minuteEt = minuteEtFromNs(row.window_start);
    if (minuteEt === null || minuteEt < 570 || minuteEt >= closeMinute) return;
    let agg = bars.get(ticker);
    if (!agg) {
      agg = emptyAgg(ticker, day.date);
      bars.set(ticker, agg);
    }
    updateAgg(agg, row);
  }

  if (source.format === 'parquet') await streamParquetStockRows(source.filePath, onRow);
  else await readGzipCsv(source.filePath, onRow);

  return bars;
}

function fileCoverageForDay(config, day) {
  const csvPath = stockCsvPath(config, day.date);
  const parquetPath = stockParquetPath(config, day.date);
  const successPath = stockSuccessPath(config, day.date);
  const csvExists = fs.existsSync(csvPath);
  const parquetExists = parquetPath ? fs.existsSync(parquetPath) : false;
  const successExists = fs.existsSync(successPath);
  let status = 'ready';
  if (!csvExists && !parquetExists && !successExists) status = 'unattempted';
  else if (!csvExists && !parquetExists) status = 'attempted_missing';
  return {
    date: day.date,
    status,
    csvExists,
    parquetExists,
    successExists,
    csvPath,
    parquetPath,
    successPath,
  };
}

module.exports = {
  readDailyBarsForDay,
  fileCoverageForDay,
  minuteEtFromNs,
  resolveStockBarsSource,
  stockParquetSql,
};
