const fs = require('node:fs');
const readline = require('node:readline');
const zlib = require('node:zlib');

const { datasetCsvPath } = require('./config');
const { toNumber } = require('./csv');
const {
  getEtParts,
  isRegularSessionMinute,
  nsToMinuteMs,
} = require('./time');

async function readRegularSessionRowsForDay({ config, dayIso, symbol = config.target || 'SPY' }) {
  const filePath = datasetCsvPath(config, 'stockBars', dayIso);
  if (!fs.existsSync(filePath)) return { rows: [], missingFile: true, filePath };

  const rows = [];
  const stream = fs.createReadStream(filePath).pipe(zlib.createGunzip());
  const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let headers = null;
  const symbolPrefix = `${symbol},`;

  for await (const line of reader) {
    if (!headers) {
      headers = line.split(',');
      continue;
    }
    if (!line.startsWith(symbolPrefix)) continue;
    const values = line.split(',');
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });
    const minuteMs = nsToMinuteMs(row.window_start);
    if (!Number.isFinite(minuteMs) || !isRegularSessionMinute(minuteMs, config.session)) continue;

    const open = toNumber(row.open);
    const high = toNumber(row.high);
    const low = toNumber(row.low);
    const close = toNumber(row.close);
    if (![open, high, low, close].every((value) => Number.isFinite(value))) continue;

    const et = getEtParts(minuteMs);
    rows.push({
      date: dayIso,
      minuteMs,
      minuteUtc: new Date(minuteMs).toISOString(),
      minuteOfDayEt: et.minuteOfDayEt,
      minFromOpen: et.minuteOfDayEt - config.session.regularOpenMinuteEt + 1,
      open,
      high,
      low,
      close,
      volume: toNumber(row.volume) || 0,
      transactions: toNumber(row.transactions) || 0,
    });
  }

  rows.sort((left, right) => left.minuteMs - right.minuteMs);
  return { rows, missingFile: false, filePath };
}

module.exports = {
  readRegularSessionRowsForDay,
};
