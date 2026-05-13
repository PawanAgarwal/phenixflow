// VIX term-structure regime gate strategy.
//
// Signal: the VIX1D/VIX3M ratio (and VIX/VIX3M as an alternate).
//   - In a calm market, the term structure is in CONTANGO: VIX1D < VIX < VIX3M.
//     The ratio sits between ~0.5–0.85.
//   - During stress, the front end spikes and the curve INVERTS:
//     VIX1D > VIX > VIX3M, ratio rises above 1.0 (sometimes 1.5–2.0 at panic peaks).
//
// Contrarian hypothesis:
//   - Strong inversion (ratio z-score >> 0) = panic priced into front-end
//     → buy SPY the next day, V-shape recovery is common
//   - Extreme contango (ratio z-score << 0) = complacency
//     → fade SPY the next day (overdue correction)
//
// We trade on the signal known at prior EOD (the 15:55 ET VIX print), so
// entry at the next day 9:35 ET is causal — no look-ahead.

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { loadStockBars } = require('./build-features-1m');
const { PROJECT_ROOT, loadConfig, resolveDatasetSource } = require('./config');
const { getEtParts, nsToMinuteMs } = require('./time');
const { readGzipCsv, toNumber } = require('./csv');
const { spawn } = require('node:child_process');

let cachedUniverseConfig = null;
function getUniverseConfig() {
  if (!cachedUniverseConfig) cachedUniverseConfig = loadConfig();
  return cachedUniverseConfig;
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

function duckdbString(value) { return `'${String(value).replace(/'/g, "''")}'`; }

async function streamParquetRows(filePath, columns, onRow) {
  const sql = `COPY (SELECT ${columns.join(', ')} FROM read_parquet(${duckdbString(filePath)})) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`;
  const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', sql], { stdio: ['ignore', 'pipe', 'pipe'] });
  const stderr = [];
  child.stderr.on('data', (chunk) => stderr.push(String(chunk)));
  const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  let headers = null;
  for await (const line of reader) {
    if (!line) continue;
    if (!headers) { headers = String(line).split(','); continue; }
    const values = String(line).split(',');
    const row = {};
    headers.forEach((header, index) => { row[header] = values[index] ?? ''; });
    await onRow(row);
  }
  const code = await new Promise((resolve, reject) => { child.on('error', reject); child.on('close', resolve); });
  if (code !== 0) throw new Error(`duckdb_parquet_read_failed:${filePath}:${stderr.join('').trim() || code}`);
}

// Read the LATEST close for each VIX tenor on a given day. Handles both historical CSV and live parquet.
async function readVixTenorsForDay(day) {
  const config = getUniverseConfig();
  const source = resolveDatasetSource(config, config.datasets.indexBars, day);
  if (source.format === 'missing') return null;
  const wanted = new Set(['I:VIX', 'I:VIX1D', 'I:VIX9D', 'I:VIX3M', 'I:VIX1Y']);
  const out = {};
  async function onRow(row) {
    const ticker = String(row.ticker || '').trim();
    if (!wanted.has(ticker)) return;
    const close = toNumber(row.close);
    if (!Number.isFinite(close)) return;
    // Keep the LAST close seen per ticker (rows are not guaranteed sorted, so prefer the
    // greatest window_start to mean "most recent in day")
    const ws = toNumber(row.window_start);
    if (!out[ticker] || (ws && (!out[ticker].ws || ws > out[ticker].ws))) out[ticker] = { close, ws };
  }
  if (source.format === 'csv.gz') {
    await readGzipCsv(source.filePath, onRow);
  } else if (source.format === 'parquet') {
    await streamParquetRows(source.filePath, ['ticker', 'open', 'close', 'window_start'], onRow);
  }
  // Return just the close values
  const result = {};
  for (const [t, v] of Object.entries(out)) result[t] = v.close;
  if (!Number.isFinite(result['I:VIX']) && !Number.isFinite(result['I:VIX1D'])) return null;
  return result;
}

async function loadSpyMinuteBars(day) {
  const universe = getUniverseConfig();
  const byMinute = await loadStockBars(universe, day, 'SPY');
  if (byMinute.size === 0) return null;
  const rows = [];
  for (const [minuteMs, bar] of byMinute.entries()) {
    const et = getEtParts(minuteMs);
    if (et.dateEt !== day) continue;
    if (et.minuteOfDayEt < 570 || et.minuteOfDayEt >= 960) continue;
    rows.push({ minuteMs, ...bar, minute_of_day_et: et.minuteOfDayEt });
  }
  rows.sort((a, b) => a.minuteMs - b.minuteMs);
  return rows;
}

function rollingZ(series, lookback) {
  // For each index, compute z-score of values[i] using values[i-lookback..i-1].
  // Returns array same length as series; first lookback entries are null.
  const out = new Array(series.length).fill(null);
  for (let i = lookback; i < series.length; i += 1) {
    const window = series.slice(Math.max(0, i - lookback), i);
    if (window.length < Math.min(5, lookback)) continue;
    const m = window.reduce((a, b) => a + b, 0) / window.length;
    const sd = Math.sqrt(window.reduce((a, b) => a + ((b - m) ** 2), 0) / window.length);
    if (sd === 0) continue;
    out[i] = (series[i] - m) / sd;
  }
  return out;
}

async function runBacktest({
  startDate,
  endDate,
  params = {},
}) {
  const p = {
    metric: 'vix1d_over_vix3m', // or 'vix_over_vix3m'
    zEnter: 1.5,
    zCap: 8.0,
    lookbackDays: 20,
    leverage: 1.0,
    entryMinuteEt: 575,
    exitMinuteEt: 955,
    costBpsRoundTrip: 2,
    overnight: false,
    inversionLongOnly: false, // skip the steep-contango → short side if true
    contangoShortOnly: false, // skip the inversion → long side if true
    longTicker: 'SPY',
    shortTicker: 'SH',
    overnightLongTicker: null,
    overnightShortTicker: null,
    ...params,
  };

  // Build full list of weekdays in range
  const days = [];
  const cur = new Date(`${startDate}T00:00:00.000Z`);
  const stop = new Date(`${endDate}T00:00:00.000Z`);
  while (cur <= stop) {
    const d = cur.toISOString().slice(0, 10);
    if (!isWeekend(d)) days.push(d);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }

  // Load VIX series across all days (only keep days where we got data)
  const dayVix = new Map();
  for (const day of days) {
    // eslint-disable-next-line no-await-in-loop
    const v = await readVixTenorsForDay(day);
    if (v) dayVix.set(day, v);
  }
  const validDays = days.filter((d) => dayVix.has(d));
  const ratios = validDays.map((d) => {
    const v = dayVix.get(d);
    if (p.metric === 'vix_over_vix3m') {
      return v['I:VIX'] && v['I:VIX3M'] ? v['I:VIX'] / v['I:VIX3M'] : null;
    }
    return v['I:VIX1D'] && v['I:VIX3M'] ? v['I:VIX1D'] / v['I:VIX3M'] : null;
  });
  // Drop nulls but preserve index alignment
  for (let i = 0; i < ratios.length; i += 1) {
    if (!Number.isFinite(ratios[i])) ratios[i] = null;
  }
  // Compute rolling z-score (skips early entries with insufficient lookback)
  const valid = ratios.map((r, idx) => (r === null ? null : r));
  const zSeries = rollingZ(valid.filter((x) => x !== null), p.lookbackDays);
  // Map z-score back to days (skipping null ratios)
  const dayZ = new Map();
  let zIdx = 0;
  for (let i = 0; i < validDays.length; i += 1) {
    if (ratios[i] === null) continue;
    const z = zSeries[zIdx];
    if (Number.isFinite(z) && Math.abs(z) <= p.zCap) {
      dayZ.set(validDays[i], { ratio: ratios[i], z });
    }
    zIdx += 1;
  }

  const trades = [];
  const openPositions = [];
  for (let i = 0; i < validDays.length - 1; i += 1) {
    const signalDay = validDays[i];
    const tradeDay = validDays[i + 1];
    const sig = dayZ.get(signalDay);
    if (!sig) continue;
    const { ratio, z } = sig;
    if (Math.abs(z) < p.zEnter) continue;
    // Inversion (positive z) → LONG SPY (contrarian fear)
    // Contango (negative z) → SHORT SPY (contrarian complacency)
    let side = null;
    if (z >= p.zEnter && !p.contangoShortOnly) side = 'LONG';
    else if (z <= -p.zEnter && !p.inversionLongOnly) side = 'SHORT';
    if (!side) continue;

    // eslint-disable-next-line no-await-in-loop
    const tradeRows = await loadSpyMinuteBars(tradeDay);
    if (!tradeRows || tradeRows.length === 0) continue;
    const exactExit = tradeRows.find((r) => r.minute_of_day_et === p.exitMinuteEt);
    let entryPrice = null;
    let entryMode = 'intraday';
    let entryDate = tradeDay;
    let ticker = side === 'LONG'
      ? (p.leverage > 1 ? 'SPXL' : p.longTicker)
      : (p.leverage > 1 ? 'SPXU' : p.shortTicker);

    if (p.overnight) {
      // eslint-disable-next-line no-await-in-loop
      const priorRows = await loadSpyMinuteBars(signalDay);
      if (!priorRows || priorRows.length === 0) continue;
      const priorClose = priorRows.find((r) => r.minute_of_day_et === p.exitMinuteEt) || priorRows[priorRows.length - 1];
      if (!Number.isFinite(priorClose?.close)) continue;
      entryPrice = priorClose.close;
      entryMode = 'overnight';
      entryDate = signalDay;
      ticker = side === 'LONG'
        ? (p.overnightLongTicker || (p.leverage > 1 ? 'TQQQ' : p.longTicker))
        : (p.overnightShortTicker || (p.leverage > 1 ? 'SQQQ' : p.shortTicker));
    } else {
      const entryRow = tradeRows.find((r) => r.minute_of_day_et === p.entryMinuteEt);
      if (!entryRow || !Number.isFinite(entryRow.open)) continue;
      entryPrice = entryRow.open;
    }

    if (!exactExit || !Number.isFinite(exactExit.close)) {
      openPositions.push({
        signalDate: signalDay, signalZ: z, signalRatio: ratio,
        entryDate, entryMinuteEt: p.overnight ? p.exitMinuteEt : p.entryMinuteEt,
        expectedExitDate: tradeDay, expectedExitMinuteEt: p.exitMinuteEt,
        side, ticker, leverage: p.leverage, entryPrice, entryMode,
        carryOver: p.overnight,
      });
      continue;
    }

    const exitPrice = exactExit.close;
    const sign = side === 'LONG' ? +1 : -1;
    const gross = sign * p.leverage * (exitPrice / entryPrice - 1);
    const cost = p.costBpsRoundTrip / 10_000;
    const net = gross - cost;
    trades.push({
      date: tradeDay, entryDate, exitDate: tradeDay,
      signalDate: signalDay, signalZ: z, signalRatio: ratio,
      side, ticker, leverage: p.leverage,
      entryPrice, exitPrice,
      grossReturn: gross, cost, netReturn: net,
      isWin: net > 0, entryMode, carryOver: p.overnight,
    });
  }

  return { trades, openPositions, params: p, validDayCount: validDays.length };
}

function summarizeTrades(trades) {
  if (trades.length === 0) {
    return {
      trade_count: 0, hit_rate: 0, total_gross_pct: 0, total_net_pct: 0,
      avg_net_bps: 0, sharpe_per_trade: 0, max_drawdown_pct: 0,
    };
  }
  const n = trades.length;
  const wins = trades.filter((t) => t.netReturn > 0).length;
  const sumGross = trades.reduce((a, t) => a + t.grossReturn, 0);
  const sumNet = trades.reduce((a, t) => a + t.netReturn, 0);
  const meanNet = sumNet / n;
  const sd = Math.sqrt(trades.reduce((a, t) => a + ((t.netReturn - meanNet) ** 2), 0) / n);
  let cum = 0; let peak = 0; let dd = 0;
  for (const t of trades) { cum += t.netReturn; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum; }
  return {
    trade_count: n,
    hit_rate: wins / n,
    total_gross_pct: sumGross * 100,
    total_net_pct: sumNet * 100,
    avg_net_bps: meanNet * 10_000,
    sharpe_per_trade: sd > 0 ? (meanNet / sd) * Math.sqrt(252) : 0,
    max_drawdown_pct: dd * 100,
  };
}

module.exports = {
  runBacktest,
  summarizeTrades,
  readVixTenorsForDay,
  loadSpyMinuteBars,
};
