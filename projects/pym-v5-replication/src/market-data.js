const fs = require('node:fs');

const { closeMinuteEt } = require('./calendar');
const { readGzipCsv, toNumber } = require('./csv');
const { stockCsvPath, stockSuccessPath } = require('./config');

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

async function readDailyBarsForDay(config, day, tickers) {
  const filePath = stockCsvPath(config, day.date);
  const selected = new Set([...tickers].map((ticker) => ticker.toUpperCase()));
  const closeMinute = closeMinuteEt(day);
  const bars = new Map();
  if (!fs.existsSync(filePath)) return bars;

  await readGzipCsv(filePath, (row) => {
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
  });

  return bars;
}

function fileCoverageForDay(config, day) {
  const csvPath = stockCsvPath(config, day.date);
  const successPath = stockSuccessPath(config, day.date);
  const csvExists = fs.existsSync(csvPath);
  const successExists = fs.existsSync(successPath);
  let status = 'ready';
  if (!csvExists && !successExists) status = 'unattempted';
  else if (!csvExists || !successExists) status = 'attempted_missing';
  return {
    date: day.date,
    status,
    csvExists,
    successExists,
    csvPath,
    successPath,
  };
}

module.exports = {
  readDailyBarsForDay,
  fileCoverageForDay,
  minuteEtFromNs,
};
