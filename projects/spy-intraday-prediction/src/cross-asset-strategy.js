// S9 — SPY/QQQ relative flow pair trade.
//
// Hypothesis: aggressive option flow is a contrarian signal for the underlying ETF.
// When SPY flow is bullish but QQQ flow isn't (asymmetric flow imbalance),
// the SPY/QQQ pair should mean-revert toward QQQ — i.e. SHORT the SPY/QQQ spread.
// Conversely, bearish SPY flow without matching QQQ flow → LONG the spread.
//
// Trade: hold a dollar-neutral SPY−QQQ spread. P&L (in % of one leg's notional) = SPY_return − QQQ_return × sign.

const fs = require('node:fs');
const zlib = require('node:zlib');
const readline = require('node:readline');
const path = require('node:path');

const { defaultFeaturesPath } = require('./build-features-1m');
const { listCalendarDates } = require('./time');
const { buildEventIndex, tagDay } = require('./event-calendar');

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

function netAggressivePremium(row) {
  return ((row.flow_sweep_call_buy_premium || 0)
    + (row.flow_sweep_put_sell_premium || 0))
    - ((row.flow_sweep_call_sell_premium || 0)
    + (row.flow_sweep_put_buy_premium || 0));
}

async function runCrossAssetStrategy({
  projectRoot,
  startDate,
  endDate,
  params = {},
  dayFilter = null,
}) {
  const p = {
    lookback: 30,
    enterAbsZ: 1.5,
    holdMinutes: 30,
    cooldownMinutes: 30,
    minSpyPremium: 50_000,
    earliestEntryEt: 600,
    latestEntryEt: 930,
    spyCostBps: 2,
    qqqCostBps: 2,
    // If true: when SPY flow > QQQ flow, LONG spread (assume flow predicts continuation, not reversion).
    // If false: SHORT spread (mean-reversion hypothesis, original).
    continuationMode: false,
    ...params,
  };
  const eventIdx = buildEventIndex();
  const dates = listCalendarDates(startDate, endDate)
    .filter((d) => !isWeekend(d))
    .filter((d) => !dayFilter || dayFilter(d, tagDay(d, eventIdx)));

  const trades = [];
  for (const day of dates) {
    // eslint-disable-next-line no-await-in-loop
    const spy = await loadFeatures(projectRoot, 'SPY', day);
    // eslint-disable-next-line no-await-in-loop
    const qqq = await loadFeatures(projectRoot, 'QQQ', day);
    if (spy.length === 0 || qqq.length === 0) continue;
    const qqqByMinute = new Map(qqq.map((r) => [r.minute_ms, r]));
    const spySeries = spy.map((r) => netAggressivePremium(r));
    const qqqSeries = spy.map((r) => {
      const qr = qqqByMinute.get(r.minute_ms);
      return qr ? netAggressivePremium(qr) : 0;
    });

    let cooldownUntilIdx = -1;
    for (let i = 0; i < spy.length; i += 1) {
      if (i < cooldownUntilIdx) continue;
      const sr = spy[i];
      const qr = qqqByMinute.get(sr.minute_ms);
      if (!qr) continue;
      if (sr.minute_of_day_et < p.earliestEntryEt || sr.minute_of_day_et > p.latestEntryEt) continue;
      if (Math.abs(spySeries[i]) < p.minSpyPremium) continue;
      const zSpy = zScoreSeries(spySeries, i, p.lookback);
      const zQqq = zScoreSeries(qqqSeries, i, p.lookback);
      const zDiff = zSpy - zQqq;
      if (Math.abs(zDiff) < p.enterAbsZ) continue;

      // Hypothesis (mean-revert): SPY flow z > QQQ flow z → SPY underperforms QQQ → SHORT spread.
      // Alternative (continuation): SPY flow z > QQQ flow z → SPY outperforms QQQ → LONG spread.
      const side = p.continuationMode
        ? (zDiff > 0 ? 'LONG_SPREAD' : 'SHORT_SPREAD')
        : (zDiff > 0 ? 'SHORT_SPREAD' : 'LONG_SPREAD');
      const exitIdx = Math.min(i + p.holdMinutes, spy.length - 1);
      const spyEntry = spy[i + 1]?.spy_open ?? sr.spy_close;
      const spyExit = spy[exitIdx].spy_close;
      const qqqEntryRow = qqqByMinute.get(spy[i + 1]?.minute_ms);
      const qqqExitRow = qqqByMinute.get(spy[exitIdx].minute_ms);
      const qqqEntry = qqqEntryRow?.spy_open ?? qr.spy_close; // spy_open in the QQQ file is QQQ's open (we used same builder)
      const qqqExit = qqqExitRow?.spy_close ?? qr.spy_close;
      if (![spyEntry, spyExit, qqqEntry, qqqExit].every((v) => Number.isFinite(v) && v > 0)) continue;

      const spyRet = spyExit / spyEntry - 1;
      const qqqRet = qqqExit / qqqEntry - 1;
      const spreadRet = spyRet - qqqRet;
      const sign = side === 'LONG_SPREAD' ? +1 : -1;
      const gross = sign * spreadRet;
      // Costs: both legs incur round-trip slippage
      const cost = (p.spyCostBps + p.qqqCostBps) / 10_000;
      const net = gross - cost;

      trades.push({
        date_et: sr.date_et,
        signal_minute_ms: sr.minute_ms,
        signal_minute_of_day_et: sr.minute_of_day_et,
        side,
        z_spy: zSpy,
        z_qqq: zQqq,
        z_diff: zDiff,
        spy_entry: spyEntry,
        spy_exit: spyExit,
        spy_return: spyRet,
        qqq_entry: qqqEntry,
        qqq_exit: qqqExit,
        qqq_return: qqqRet,
        spread_return: spreadRet,
        gross_return: gross,
        cost,
        net_return: net,
      });
      cooldownUntilIdx = i + p.holdMinutes + p.cooldownMinutes;
    }
  }

  if (trades.length === 0) {
    return { strategy: 'S9_spy_qqq_spread', stats: emptyStats(), trades, params: p };
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
    strategy: 'S9_spy_qqq_spread',
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
    },
    trades,
  };
}

function emptyStats() {
  return {
    trade_count: 0, hit_rate: 0, total_gross_return_pct: 0, total_net_return_pct: 0,
    avg_net_return_bps: 0, std_net_return_bps: 0, sharpe_per_trade: 0, max_drawdown_net_pct: 0,
  };
}

module.exports = { runCrossAssetStrategy };
