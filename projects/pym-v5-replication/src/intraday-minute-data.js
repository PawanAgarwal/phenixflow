const fs = require('node:fs');

const { closeMinuteEt } = require('./calendar');
const { stockCsvPath } = require('./config');
const { readGzipCsv, toNumber } = require('./csv');
const { minuteEtFromNs } = require('./market-data');

function emptyMinuteBar(ticker, minuteEt) {
  return {
    ticker,
    minuteEt,
    open: null,
    high: null,
    low: null,
    close: null,
    volume: 0,
    transactions: 0,
  };
}

function updateMinuteBar(bar, row) {
  const open = toNumber(row.open);
  const high = toNumber(row.high);
  const low = toNumber(row.low);
  const close = toNumber(row.close);
  if (bar.open === null && Number.isFinite(open)) bar.open = open;
  if (Number.isFinite(high)) bar.high = bar.high === null ? high : Math.max(bar.high, high);
  if (Number.isFinite(low)) bar.low = bar.low === null ? low : Math.min(bar.low, low);
  if (Number.isFinite(close)) bar.close = close;
  bar.volume += toNumber(row.volume) || 0;
  bar.transactions += toNumber(row.transactions) || 0;
}

async function readMinuteBarsForDay(config, day, tickers) {
  const filePath = stockCsvPath(config, day.date);
  const selected = new Set([...tickers].map((ticker) => ticker.toUpperCase()));
  const closeMinute = closeMinuteEt(day);
  const barsByTicker = new Map();
  const minuteSet = new Set();
  if (!fs.existsSync(filePath)) return { filePath, barsByTicker, minutes: [], rowsRead: 0 };

  const rowsRead = await readGzipCsv(filePath, (row) => {
    const ticker = String(row.ticker || '').toUpperCase();
    if (!selected.has(ticker)) return;
    const minuteEt = minuteEtFromNs(row.window_start);
    if (minuteEt === null || minuteEt < 570 || minuteEt >= closeMinute) return;
    if (!barsByTicker.has(ticker)) barsByTicker.set(ticker, new Map());
    const tickerBars = barsByTicker.get(ticker);
    let bar = tickerBars.get(minuteEt);
    if (!bar) {
      bar = emptyMinuteBar(ticker, minuteEt);
      tickerBars.set(minuteEt, bar);
      minuteSet.add(minuteEt);
    }
    updateMinuteBar(bar, row);
  });

  return {
    filePath,
    barsByTicker,
    minutes: [...minuteSet].sort((left, right) => left - right),
    rowsRead,
  };
}

function closeAt(dayBars, ticker, minuteEt) {
  return dayBars.barsByTicker.get(ticker)?.get(minuteEt)?.close ?? null;
}

function barAt(dayBars, ticker, minuteEt) {
  return dayBars.barsByTicker.get(ticker)?.get(minuteEt) ?? null;
}

module.exports = {
  readMinuteBarsForDay,
  closeAt,
  barAt,
};
