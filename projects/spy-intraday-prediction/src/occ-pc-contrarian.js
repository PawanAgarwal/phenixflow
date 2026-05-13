// OCC put/call ratio extreme-contrarian strategy.
//
// Hypothesis: when OCC equity P/C ratio is at a rolling-20-day z-score extreme,
// the underlying sentiment is mean-reverting. Signal is computed from features-1m's
// occ_equity_pc_ratio_z field (already present, no new data needed).
//
//   z >= +zEnter (excess put-buying = fear)     → next-day LONG SPY  (contrarian)
//   z <= -zEnter (excess call-buying = greed)   → next-day SHORT SPY (contrarian)
//
// The OCC daily total is finalized after EOD on day T, so the signal is known by
// roughly 17:00 ET on T and acts on day T+1 — no look-ahead.
//
// Variants explored: pure intraday (enter 9:35, exit 15:55), overnight hold
// (enter prior-day 15:55, exit next-day 15:55), 1× SPY vs 3× SPXL/SPXU, asymmetric
// long-only filter, and bias-magnitude position sizing.

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { defaultFeaturesPath, loadStockBars, buildOccOverlay } = require('./build-features-1m');
const { getEtParts } = require('./time');
const { PROJECT_ROOT, loadConfig } = require('./config');

const DEFAULT_OCC_ROOT = '/Volumes/SEC4TB/massive-data/occ/option_open_interest_eod';

let cachedUniverseConfig = null;
function getUniverseConfig() {
  if (!cachedUniverseConfig) cachedUniverseConfig = loadConfig();
  return cachedUniverseConfig;
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

// Read first row from features-1m JSONL to grab the day's OCC P/C ratio z-score.
async function readOccZForDay(day, overlay = null) {
  const p = defaultFeaturesPath(PROJECT_ROOT, 'SPY', day);
  if (fs.existsSync(p)) {
    const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    for await (const line of rl) {
      if (!line) continue;
      const r = JSON.parse(line);
      return {
        occ_equity_pc_ratio: r.occ_equity_pc_ratio,
        occ_equity_pc_ratio_z: r.occ_equity_pc_ratio_z,
      };
    }
  }
  const occ = overlay?.get(day);
  if (occ && Number.isFinite(occ.equity_pc_ratio_z)) {
    return {
      occ_equity_pc_ratio: occ.equity_pc_ratio,
      occ_equity_pc_ratio_z: occ.equity_pc_ratio_z,
    };
  }
  return null;
}

// Load minute-level SPY bars for a given day (supports historical CSV + live parquet fallback).
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

function listTradingDays(startDate, endDate, validDays) {
  const out = [];
  const cur = new Date(`${startDate}T00:00:00.000Z`);
  const stop = new Date(`${endDate}T00:00:00.000Z`);
  while (cur <= stop) {
    const d = cur.toISOString().slice(0, 10);
    if (!isWeekend(d) && (!validDays || validDays.has(d))) out.push(d);
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

function stockDatasetDates(config, startDate, endDate) {
  const roots = [config.roots.historical, config.roots.liveParquet].filter(Boolean);
  const dates = new Set();
  for (const root of roots) {
    const datasetRoot = path.join(root, config.datasets.stockBars);
    if (!fs.existsSync(datasetRoot)) continue;
    fs.readdirSync(datasetRoot)
      .filter((entry) => entry.startsWith('date='))
      .map((entry) => entry.slice('date='.length))
      .filter((day) => day >= startDate && day <= endDate && !isWeekend(day))
      .forEach((day) => dates.add(day));
  }
  return dates;
}

// Run the backtest for one parameter configuration.
async function runBacktest({
  startDate,
  endDate,
  params = {},
}) {
  const p = {
    zEnter: 2.0,           // |z| ≥ this triggers a contrarian trade
    zCap: 8.0,             // skip absurd outliers (e.g. early-history small-window z=-10)
    longOnly: false,       // if true, only act on positive z (fear)
    shortOnly: false,      // if true, only act on negative z (greed)
    leverage: 1.0,         // 1× SPY, 3× for SPXL/SPXU emulation
    entryMinuteEt: 575,    // 9:35 ET intraday entry
    exitMinuteEt: 955,     // 15:55 ET exit
    overnight: false,      // enter at prior 15:55 instead of next-day 9:35
    costBpsRoundTrip: 2,
    biasProportionalSize: false,
    biasSizeMultiplier: 0.4, // size = clamp(|z| * mult, 1.0)
    longTicker: 'SPY',
    shortTicker: 'SH',
    overnightLongTicker: null, // if null, same as longTicker
    overnightShortTicker: null,
    ...params,
  };

  // Build full date list from Massive stock bars so provisional live parquet
  // days can be used even when the derived features-1m cache is not current.
  const config = getUniverseConfig();
  const validDays = stockDatasetDates(config, startDate, endDate);
  const days = listTradingDays(startDate, endDate, validDays);

  // Pre-load OCC z-score per day.
  const occRoot = process.env.OCC_EOD_ROOT || DEFAULT_OCC_ROOT;
  const occOverlay = fs.existsSync(occRoot)
    ? await buildOccOverlay({ occRoot, dates: days })
    : new Map();
  const occByDay = new Map();
  for (const day of days) {
    // eslint-disable-next-line no-await-in-loop
    const r = await readOccZForDay(day, occOverlay);
    if (r && Number.isFinite(r.occ_equity_pc_ratio_z)) {
      if (Math.abs(r.occ_equity_pc_ratio_z) <= p.zCap) occByDay.set(day, r.occ_equity_pc_ratio_z);
    }
  }

  const trades = [];
  const openPositions = [];
  // For each day T with a valid signal, attempt to trade day T+1.
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    const tradeDay = days[i + 1];
    const z = occByDay.get(signalDay);
    if (!Number.isFinite(z)) continue;
    if (Math.abs(z) < p.zEnter) continue;
    // Contrarian: positive z (fear) → LONG; negative z (greed) → SHORT
    let side = null;
    if (z >= p.zEnter && !p.shortOnly) side = 'LONG';
    else if (z <= -p.zEnter && !p.longOnly) side = 'SHORT';
    if (!side) continue;

    // Load minute bars for the trade day (and prior day if overnight mode).
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

    // If exit data isn't available yet (mid-session refresh on tradeDay), record as open.
    if (!exactExit || !Number.isFinite(exactExit.close)) {
      openPositions.push({
        signalDate: signalDay,
        signalZ: z,
        entryDate,
        entryMinuteEt: p.overnight ? p.exitMinuteEt : p.entryMinuteEt,
        expectedExitDate: tradeDay,
        expectedExitMinuteEt: p.exitMinuteEt,
        side,
        ticker,
        leverage: p.leverage,
        entryPrice,
        entryMode,
        carryOver: p.overnight,
      });
      continue;
    }

    const exitPrice = exactExit.close;
    // Position sizing
    let size = 1.0;
    if (p.biasProportionalSize) {
      size = Math.min(Math.abs(z) * p.biasSizeMultiplier, 1.0);
    }
    const sign = side === 'LONG' ? +1 : -1;
    const gross = sign * p.leverage * size * (exitPrice / entryPrice - 1);
    const cost = (p.costBpsRoundTrip / 10_000) * size;
    const net = gross - cost;
    trades.push({
      date: tradeDay,
      entryDate,
      exitDate: tradeDay,
      signalDate: signalDay,
      signalZ: z,
      side,
      ticker,
      leverage: p.leverage,
      size,
      entryPrice,
      exitPrice,
      grossReturn: gross,
      cost,
      netReturn: net,
      isWin: net > 0,
      entryMode,
      carryOver: p.overnight,
    });
  }

  return { trades, openPositions, params: p };
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
  readOccZForDay,
  loadSpyMinuteBars,
};
