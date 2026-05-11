// Shared backtest engine for the BullFlow/CheddarFlow-style strategies.
// Each strategy is a function:
//   (rowsForDay, ctx) -> [{ minute_ms, side: 'LONG'|'SHORT', exit_minute_ms?, hold_minutes?, size? }, ...]
// The runner converts those trade intents into SPY-equity P&L using close-to-close minute returns,
// applies a cost model, and reports cohort stats.
//
// Notes:
//   - Position size is in "units of SPY notional" (default 1.0 = 100% of capital).
//   - Entries fill at the *next* minute's open; exits at the close of the exit minute
//     (or session close if hold runs past 16:00 ET).
//   - Slippage + commission expressed in bps of notional; default 2 bps round-trip.

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { defaultFeaturesPath } = require('./build-features-1m');
const { listCalendarDates } = require('./time');
const { buildEventIndex, tagDay } = require('./event-calendar');

const REGULAR_OPEN_ET = 570;
const REGULAR_CLOSE_ET = 960;

async function loadFeaturesForDay(projectRoot, root, dayIso) {
  const p = defaultFeaturesPath(projectRoot, root, dayIso);
  if (!fs.existsSync(p)) return [];
  const rows = [];
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    rows.push(JSON.parse(line));
  }
  return rows;
}

function isWeekend(dayIso) {
  const d = new Date(`${dayIso}T00:00:00.000Z`);
  const dow = d.getUTCDay();
  return dow === 0 || dow === 6;
}

function executeIntent({ rows, intent, costsBpsRoundTrip }) {
  // Entry on next minute's open after the signal minute; exit at the exit minute's close.
  const idxSignal = rows.findIndex((r) => r.minute_ms === intent.minute_ms);
  if (idxSignal === -1) return null;
  const entryIdx = idxSignal + 1;
  if (entryIdx >= rows.length) return null;
  const entryRow = rows[entryIdx];
  const entryPrice = entryRow.spy_open;
  if (!Number.isFinite(entryPrice)) return null;
  let exitIdx;
  if (intent.exit_minute_ms) {
    const idx = rows.findIndex((r) => r.minute_ms === intent.exit_minute_ms);
    exitIdx = idx === -1 ? rows.length - 1 : idx;
  } else if (Number.isFinite(intent.hold_minutes)) {
    exitIdx = Math.min(entryIdx + intent.hold_minutes - 1, rows.length - 1);
  } else {
    // default: end of day
    exitIdx = rows.length - 1;
  }
  if (exitIdx <= entryIdx) exitIdx = entryIdx;
  const exitRow = rows[exitIdx];
  const exitPrice = exitRow.spy_close;
  if (!Number.isFinite(exitPrice)) return null;
  const sign = intent.side === 'SHORT' ? -1 : +1;
  const size = Number.isFinite(intent.size) ? intent.size : 1.0;
  const gross = sign * size * (exitPrice / entryPrice - 1);
  const cost = (costsBpsRoundTrip / 10_000) * size;
  const net = gross - cost;
  return {
    date_et: rows[idxSignal].date_et,
    signal_minute_ms: intent.minute_ms,
    signal_minute_of_day_et: rows[idxSignal].minute_of_day_et,
    entry_minute_ms: entryRow.minute_ms,
    entry_price: entryPrice,
    exit_minute_ms: exitRow.minute_ms,
    exit_minute_of_day_et: exitRow.minute_of_day_et,
    exit_price: exitPrice,
    side: intent.side,
    size,
    hold_minutes: exitIdx - entryIdx + 1,
    gross_return: gross,
    cost,
    net_return: net,
    notes: intent.notes || null,
  };
}

function aggregateTrades(trades) {
  if (trades.length === 0) {
    return {
      trade_count: 0,
      hit_rate: 0,
      avg_gross_return: 0,
      avg_net_return: 0,
      total_gross_return: 0,
      total_net_return: 0,
      mean_hold_minutes: 0,
      std_net_return: 0,
      sharpe_per_trade: 0,
      max_drawdown_net: 0,
      best: null,
      worst: null,
    };
  }
  const n = trades.length;
  const winners = trades.filter((t) => t.net_return > 0).length;
  const mean = trades.reduce((a, t) => a + t.net_return, 0) / n;
  const variance = trades.reduce((a, t) => a + ((t.net_return - mean) ** 2), 0) / n;
  const sd = Math.sqrt(variance);
  let cumNet = 0;
  let peak = 0;
  let maxDd = 0;
  for (const t of trades) {
    cumNet += t.net_return;
    if (cumNet > peak) peak = cumNet;
    const dd = peak - cumNet;
    if (dd > maxDd) maxDd = dd;
  }
  const sorted = [...trades].sort((a, b) => a.net_return - b.net_return);
  return {
    trade_count: n,
    hit_rate: winners / n,
    avg_gross_return: trades.reduce((a, t) => a + t.gross_return, 0) / n,
    avg_net_return: mean,
    total_gross_return: trades.reduce((a, t) => a + t.gross_return, 0),
    total_net_return: cumNet,
    mean_hold_minutes: trades.reduce((a, t) => a + t.hold_minutes, 0) / n,
    std_net_return: sd,
    sharpe_per_trade: sd > 0 ? (mean / sd) * Math.sqrt(252) : 0, // approximate (per-trade not per-day)
    max_drawdown_net: maxDd,
    best: sorted[sorted.length - 1],
    worst: sorted[0],
  };
}

async function runStrategy({
  projectRoot,
  root = 'SPY',
  strategyName,
  strategyFn,
  startDate,
  endDate,
  costsBpsRoundTrip = 2,
  ctx = {},
  dayFilter = null,
}) {
  const eventIdx = buildEventIndex();
  const dates = listCalendarDates(startDate, endDate)
    .filter((d) => !isWeekend(d))
    .filter((d) => !dayFilter || dayFilter(d, tagDay(d, eventIdx)));
  const trades = [];
  const perDay = [];
  for (const dayIso of dates) {
    // eslint-disable-next-line no-await-in-loop
    const rows = await loadFeaturesForDay(projectRoot, root, dayIso);
    if (rows.length === 0) continue;
    const intents = strategyFn(rows, { ...ctx, dayIso }) || [];
    const dayTrades = [];
    for (const intent of intents) {
      const exec = executeIntent({ rows, intent, costsBpsRoundTrip });
      if (exec) {
        exec.strategy = strategyName;
        trades.push(exec);
        dayTrades.push(exec);
      }
    }
    perDay.push({
      date: dayIso,
      intents: intents.length,
      trades: dayTrades.length,
      net: dayTrades.reduce((a, t) => a + t.net_return, 0),
    });
  }
  const stats = aggregateTrades(trades);
  return {
    strategy: strategyName,
    window: { startDate, endDate },
    costsBpsRoundTrip,
    stats,
    trades,
    perDay,
  };
}

function writeJson(filePath, obj) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(obj, null, 2));
}

// Swing runner: each strategy emits a per-day end-of-day signal {side: LONG|SHORT|FLAT, ...}.
// Runner pairs the signal at day N with SPY open→close return on day N+1.
async function runSwingStrategy({
  projectRoot,
  root = 'SPY',
  strategyName,
  strategyFn,
  startDate,
  endDate,
  costsBpsRoundTrip = 2,
  ctx = {},
  dayFilter = null, // applied to the SIGNAL day (today); entry happens on next day regardless
}) {
  const eventIdx = buildEventIndex();
  const dates = listCalendarDates(startDate, endDate)
    .filter((d) => !isWeekend(d))
    .filter((d) => !dayFilter || dayFilter(d, tagDay(d, eventIdx)));
  // Load all daily summaries
  const dayClose = []; // [{date, rows, openNextDay?, closeNextDay?}]
  for (const dayIso of dates) {
    // eslint-disable-next-line no-await-in-loop
    const rows = await loadFeaturesForDay(projectRoot, root, dayIso);
    if (rows.length === 0) continue;
    const last = rows[rows.length - 1];
    const first = rows[0];
    dayClose.push({ date: dayIso, rows, last, first });
  }
  const trades = [];
  for (let i = 0; i < dayClose.length - 1; i += 1) {
    const today = dayClose[i];
    const next = dayClose[i + 1];
    const signal = strategyFn(today.rows, { ...ctx, dayIso: today.date, prior: dayClose.slice(0, i) }) || null;
    if (!signal || signal.side === 'FLAT') continue;
    const entryPrice = next.first.spy_open;
    const exitPrice = next.last.spy_close;
    if (!Number.isFinite(entryPrice) || !Number.isFinite(exitPrice)) continue;
    const sign = signal.side === 'SHORT' ? -1 : +1;
    const size = Number.isFinite(signal.size) ? signal.size : 1.0;
    const gross = sign * size * (exitPrice / entryPrice - 1);
    const cost = (costsBpsRoundTrip / 10_000) * size;
    const net = gross - cost;
    trades.push({
      strategy: strategyName,
      signal_date: today.date,
      entry_date: next.date,
      entry_price: entryPrice,
      exit_price: exitPrice,
      side: signal.side,
      size,
      gross_return: gross,
      cost,
      net_return: net,
      notes: signal.notes || null,
    });
  }
  const stats = aggregateTrades(trades);
  return {
    strategy: strategyName,
    window: { startDate, endDate },
    costsBpsRoundTrip,
    mode: 'swing',
    stats,
    trades,
  };
}

module.exports = {
  loadFeaturesForDay,
  runStrategy,
  runSwingStrategy,
  executeIntent,
  aggregateTrades,
  writeJson,
};
