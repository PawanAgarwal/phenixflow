// S8 — Vol selling on flow spikes.
// Trigger: net aggressive sweep premium z-score > Z_ENTER (absolute value)
//   = (call_sweep_buy + put_sweep_sell) - (call_sweep_sell + put_sweep_buy)
// On trigger, short an ATM 0DTE straddle (1 lot = 1 call + 1 put at the nearest strike to spot).
// Hold for `holdMinutes`, then close at the same-strike straddle mid price.
// P&L (in $) = (entry_call_mid + entry_put_mid - exit_call_mid - exit_put_mid) * 100  (per straddle)
// Costs: $0.10/contract spread + $0.65/contract commission per side → $3.00 round-trip per straddle.
// Express returns as % of underlying notional (100 × entry_spot) so they compose with our other strategies.

const fs = require('node:fs');
const zlib = require('node:zlib');
const readline = require('node:readline');
const path = require('node:path');

const { defaultStraddlePath } = require('./build-atm-straddle-1m');
const { defaultFeaturesPath } = require('./build-features-1m');
const { defaultOutputPath: defaultGreeksPath } = require('./build-greeks-1m');
const { listCalendarDates } = require('./time');
const { buildEventIndex, tagDay } = require('./event-calendar');

const PER_STRADDLE_DOLLAR_COST = 3.00; // slippage + commission round-trip

async function loadFeatures(projectRoot, root, day) {
  const p = defaultFeaturesPath(projectRoot, root, day);
  if (!fs.existsSync(p)) return [];
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = [];
  for await (const line of rl) {
    if (!line) continue;
    out.push(JSON.parse(line));
  }
  return out;
}

async function loadStraddle(projectRoot, root, day) {
  const p = defaultStraddlePath(projectRoot, root, day);
  if (!fs.existsSync(p)) return new Map();
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const byMinute = new Map();
  for await (const line of rl) {
    if (!line) continue;
    const r = JSON.parse(line);
    if (!byMinute.has(r.minute_ms)) byMinute.set(r.minute_ms, {});
    byMinute.get(r.minute_ms)[r.dte_bucket] = r;
  }
  return byMinute;
}

// Load all 0DTE per-contract prices for the day, indexed by (strike, right, minute_ms).
// We need this so that when we close a position 30m later, we look up the SAME strike
// (not the new ATM strike) to avoid strike-selection survivorship bias.
async function loadOptionGrid(projectRoot, root, day) {
  const p = defaultGreeksPath(projectRoot, root, day);
  if (!fs.existsSync(p)) return new Map();
  // key: `${strike}|${right}|${minute_ms}` → { price, dte, iv, delta, gamma }
  const grid = new Map();
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    const r = JSON.parse(line);
    if (r.dte !== 0) continue; // only 0DTE for our straddle universe
    if (!Number.isFinite(r.price) || r.price <= 0) continue;
    const key = `${r.strike}|${r.right}|${r.minute_ms}`;
    grid.set(key, { price: r.price, iv: r.iv, delta: r.delta, gamma: r.gamma });
  }
  return grid;
}

function priceAt(grid, strike, right, minuteMs, maxFallbackMs = 5 * 60_000) {
  const key = `${strike}|${right}|${minuteMs}`;
  if (grid.has(key)) return grid.get(key).price;
  // Walk back at most maxFallback in 1-minute steps for staleness
  for (let step = 60_000; step <= maxFallbackMs; step += 60_000) {
    const k2 = `${strike}|${right}|${minuteMs - step}`;
    if (grid.has(k2)) return grid.get(k2).price;
  }
  return null;
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

function safeStd(values) {
  if (values.length < 2) return 0;
  const m = values.reduce((a, b) => a + b, 0) / values.length;
  const v = values.reduce((a, b) => a + ((b - m) ** 2), 0) / values.length;
  return Math.sqrt(v);
}

function zScoreSeries(values, idx, lookback) {
  const start = Math.max(0, idx - lookback + 1);
  const window = values.slice(start, idx + 1);
  if (window.length < Math.min(5, lookback)) return 0;
  const m = window.reduce((a, b) => a + b, 0) / window.length;
  const s = safeStd(window);
  if (s === 0) return 0;
  return (values[idx] - m) / s;
}

async function runVolSellingStrategy({
  projectRoot,
  root = 'SPY',
  startDate,
  endDate,
  params = {},
  dayFilter = null,
}) {
  const p = {
    lookback: 30,
    enterAbsZ: 2.0,
    holdMinutes: 30,
    cooldownMinutes: 30,
    minSignalPremium: 50_000,
    earliestEntryEt: 600, // 10:00 ET, avoid first 30 minutes of session
    latestEntryEt: 900, // 15:00 ET, avoid last 60 minutes for 0DTE
    dollarCostPerStraddle: PER_STRADDLE_DOLLAR_COST,
    ...params,
  };
  const eventIdx = buildEventIndex();
  const dates = listCalendarDates(startDate, endDate)
    .filter((d) => !isWeekend(d))
    .filter((d) => !dayFilter || dayFilter(d, tagDay(d, eventIdx)));

  const trades = [];
  for (const day of dates) {
    // eslint-disable-next-line no-await-in-loop
    const rows = await loadFeatures(projectRoot, root, day);
    if (rows.length === 0) continue;
    // eslint-disable-next-line no-await-in-loop
    const straddleByMinute = await loadStraddle(projectRoot, root, day);
    if (straddleByMinute.size === 0) continue;
    // eslint-disable-next-line no-await-in-loop
    const grid = await loadOptionGrid(projectRoot, root, day);

    // Build per-minute aggressive sweep series for the z-score
    const aggregate = rows.map((r) => {
      const cb = r.flow_sweep_call_buy_premium || 0;
      const cs = r.flow_sweep_call_sell_premium || 0;
      const pb = r.flow_sweep_put_buy_premium || 0;
      const ps = r.flow_sweep_put_sell_premium || 0;
      return (cb + ps) - (cs + pb);
    });

    let cooldownUntilIdx = -1;
    for (let i = 0; i < rows.length; i += 1) {
      if (i < cooldownUntilIdx) continue;
      const r = rows[i];
      if (r.minute_of_day_et < p.earliestEntryEt || r.minute_of_day_et > p.latestEntryEt) continue;
      if (Math.abs(aggregate[i]) < p.minSignalPremium) continue;
      const z = zScoreSeries(aggregate, i, p.lookback);
      if (Math.abs(z) < p.enterAbsZ) continue;
      // Trigger — pick a straddle at entry minute
      const entryS = straddleByMinute.get(r.minute_ms)?.['0dte'];
      if (!entryS) continue;
      const exitIdx = Math.min(i + p.holdMinutes, rows.length - 1);
      const exitRow = rows[exitIdx];
      // Critical fix: at exit, look up the ENTRY strike's call+put price.
      // Using the new-ATM strike at exit introduces survivorship bias because it implicitly
      // requires that SPY didn't drift far from the entry strike.
      const exitCallPrice = priceAt(grid, entryS.strike, 'CALL', exitRow.minute_ms);
      const exitPutPrice = priceAt(grid, entryS.strike, 'PUT', exitRow.minute_ms);
      if (exitCallPrice === null || exitPutPrice === null) continue;
      const exitStraddlePrice = exitCallPrice + exitPutPrice;
      const exitS = { strike: entryS.strike, call_price: exitCallPrice, put_price: exitPutPrice, straddle_price: exitStraddlePrice };
      const grossDollars = (entryS.straddle_price - exitS.straddle_price) * 100; // we sold at entry, buy back at exit
      const netDollars = grossDollars - p.dollarCostPerStraddle;
      const spyNotional = r.spy_close * 100; // 100 shares of SPY notional ≈ 1 standard option contract size
      const grossReturn = grossDollars / spyNotional;
      const netReturn = netDollars / spyNotional;
      trades.push({
        date_et: r.date_et,
        signal_minute_ms: r.minute_ms,
        signal_minute_of_day_et: r.minute_of_day_et,
        exit_minute_ms: exitRow.minute_ms,
        exit_minute_of_day_et: exitRow.minute_of_day_et,
        z_score: z,
        entry_spot: r.spy_close,
        entry_strike: entryS.strike,
        entry_call: entryS.call_price,
        entry_put: entryS.put_price,
        entry_straddle: entryS.straddle_price,
        exit_call: exitS.call_price,
        exit_put: exitS.put_price,
        exit_straddle: exitS.straddle_price,
        gross_dollars: grossDollars,
        cost_dollars: p.dollarCostPerStraddle,
        net_dollars: netDollars,
        gross_return: grossReturn,
        net_return: netReturn,
        spy_intraday_at_entry: r.intraday_return,
        spy_intraday_at_exit: exitRow.intraday_return,
        cum_dealer_gamma_at_entry: r.cum_dealer_gamma,
      });
      cooldownUntilIdx = i + p.holdMinutes + p.cooldownMinutes;
    }
  }

  // Aggregate stats
  if (trades.length === 0) {
    return { strategy: 'S8_vol_selling', stats: emptyStats(), trades, params: p };
  }
  const n = trades.length;
  const winners = trades.filter((t) => t.net_return > 0).length;
  const sumGross = trades.reduce((a, t) => a + t.gross_return, 0);
  const sumNet = trades.reduce((a, t) => a + t.net_return, 0);
  const meanNet = sumNet / n;
  const sd = safeStd(trades.map((t) => t.net_return));
  let cum = 0; let peak = 0; let dd = 0;
  for (const t of trades) { cum += t.net_return; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum; }
  return {
    strategy: 'S8_vol_selling',
    params: p,
    stats: {
      trade_count: n,
      hit_rate: Number((winners / n).toFixed(4)),
      total_gross_return_pct: Number((sumGross * 100).toFixed(2)),
      total_net_return_pct: Number((sumNet * 100).toFixed(2)),
      avg_net_return_bps: Number((meanNet * 10_000).toFixed(2)),
      std_net_return_bps: Number((sd * 10_000).toFixed(2)),
      sharpe_per_trade: sd > 0 ? Number(((meanNet / sd) * Math.sqrt(252)).toFixed(2)) : 0,
      max_drawdown_net_pct: Number((dd * 100).toFixed(2)),
      total_gross_dollars: Number(trades.reduce((a, t) => a + t.gross_dollars, 0).toFixed(2)),
      total_net_dollars: Number(trades.reduce((a, t) => a + t.net_dollars, 0).toFixed(2)),
    },
    trades,
  };
}

function emptyStats() {
  return {
    trade_count: 0, hit_rate: 0, total_gross_return_pct: 0, total_net_return_pct: 0,
    avg_net_return_bps: 0, std_net_return_bps: 0, sharpe_per_trade: 0, max_drawdown_net_pct: 0,
    total_gross_dollars: 0, total_net_dollars: 0,
  };
}

module.exports = {
  runVolSellingStrategy,
};
