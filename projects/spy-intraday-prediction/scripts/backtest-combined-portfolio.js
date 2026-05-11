#!/usr/bin/env node
// Phase 21 — combine the production winner (PYM-gated V, entry 11:30) with
// the only other walk-forward survivors from earlier phases (SPXW S6 vanna,
// SPY S5 charm pin) into a single equal-risk-weighted portfolio.
//
// Goal: see if diversification improves risk-adjusted return vs PYM-gated V alone.

const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT } = require('../src/config');
const { runStrategy, runSwingStrategy, writeJson } = require('../src/strategy-runner');
const strategies = require('../src/option-flow-strategies');
const { loadPymHoldings, backtestPymGated } = require('../src/pym-bias-strategy');

const PYM_ARTIFACT = '/Users/pawanagarwal/github/phenixflow/projects/pym-v5-replication/artifacts/pym-v5-backtest-massive-eod-rsi-wilder-next_close-2025-01-02-2026-05-06.json';

const WINDOWS = [
  { name: 'train_2025', startDate: '2025-01-02', endDate: '2025-12-31' },
  { name: 'test_2026', startDate: '2026-01-02', endDate: '2026-04-27' },
  { name: 'full_16mo', startDate: '2025-01-02', endDate: '2026-04-27' },
];

function buildDailyReturns(trades, dateKey, returnKey) {
  // Returns Map<date, net_return> aggregating multiple trades per day.
  const m = new Map();
  for (const t of trades) {
    const d = t[dateKey];
    if (!d) continue;
    m.set(d, (m.get(d) || 0) + t[returnKey]);
  }
  return m;
}

function combineDailyReturns(returnMaps, weights) {
  // Compute per-day portfolio return = sum_i (weight_i * return_i_on_that_day).
  // Days where a strategy didn't trade contribute 0 (treated as cash).
  const allDates = new Set();
  for (const m of returnMaps) for (const d of m.keys()) allDates.add(d);
  const out = new Map();
  for (const d of Array.from(allDates).sort()) {
    let ret = 0;
    for (let i = 0; i < returnMaps.length; i += 1) {
      const r = returnMaps[i].get(d) || 0;
      ret += weights[i] * r;
    }
    out.set(d, ret);
  }
  return out;
}

function summarize(returnsByDate) {
  const dates = Array.from(returnsByDate.keys()).sort();
  if (dates.length === 0) return { trade_days: 0, total_net_pct: 0, sharpe_per_day: 0 };
  const returns = dates.map((d) => returnsByDate.get(d));
  const n = returns.length;
  const sumNet = returns.reduce((a, b) => a + b, 0);
  const meanNet = sumNet / n;
  const sd = Math.sqrt(returns.reduce((a, r) => a + ((r - meanNet) ** 2), 0) / n);
  let cum = 0; let peak = 0; let dd = 0;
  for (const r of returns) { cum += r; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum; }
  return {
    trade_days: n,
    total_net_pct: Number((sumNet * 100).toFixed(2)),
    avg_daily_net_bps: Number((meanNet * 10_000).toFixed(2)),
    sharpe_per_day: Number((sd > 0 ? (meanNet / sd) * Math.sqrt(252) : 0).toFixed(2)),
    max_drawdown_pct: Number((dd * 100).toFixed(2)),
    win_rate: Number(((returns.filter((r) => r > 0).length / n) * 100).toFixed(1)),
  };
}

async function main() {
  const pymByDate = loadPymHoldings(PYM_ARTIFACT);

  // Strategy 1: PYM-gated V (entry 11:30, exit 15:55, bias ±0.20)
  const pymResults = {};
  for (const w of WINDOWS) {
    // eslint-disable-next-line no-await-in-loop
    const r = await backtestPymGated({
      projectRoot: PROJECT_ROOT, root: 'SPY', pymByDate,
      startDate: w.startDate, endDate: w.endDate,
      params: { biasLong: 0.20, biasShort: -0.20, entryMinuteEt: 690, exitMinuteEt: 955 },
    });
    pymResults[w.name] = r;
  }

  // Strategy 2: SPXW S6 vanna trend (only walk-forward survivor from Phase 17)
  const spxwResults = {};
  for (const w of WINDOWS) {
    // eslint-disable-next-line no-await-in-loop
    const r = await runSwingStrategy({
      projectRoot: PROJECT_ROOT, root: 'SPXW',
      strategyName: 'S6_vanna_trend', strategyFn: strategies.strategyS6, mode: 'swing',
      startDate: w.startDate, endDate: w.endDate, costsBpsRoundTrip: 2,
    });
    spxwResults[w.name] = r;
  }

  // Strategy 3: SPY S5 charm pin
  const charmResults = {};
  for (const w of WINDOWS) {
    // eslint-disable-next-line no-await-in-loop
    const r = await runStrategy({
      projectRoot: PROJECT_ROOT, root: 'SPY',
      strategyName: 'S5_charm_pin', strategyFn: strategies.strategyS5, mode: 'intraday',
      startDate: w.startDate, endDate: w.endDate, costsBpsRoundTrip: 2,
    });
    charmResults[w.name] = r;
  }

  // Combine: equal-weight on the three (1/3 each)
  // Also try 70/15/15 weighting (favor the PYM-gated production strategy)
  const allResults = { weights_equal: {}, weights_concentrated: {}, individual: {} };
  for (const w of WINDOWS) {
    const pymDaily = buildDailyReturns(pymResults[w.name].trades, 'date', 'net_return');
    const spxwDaily = buildDailyReturns(spxwResults[w.name].trades, 'entry_date', 'net_return');
    const charmDaily = buildDailyReturns(charmResults[w.name].trades, 'date_et', 'net_return');

    const eq = combineDailyReturns([pymDaily, spxwDaily, charmDaily], [1/3, 1/3, 1/3]);
    const concentrated = combineDailyReturns([pymDaily, spxwDaily, charmDaily], [0.7, 0.15, 0.15]);

    allResults.individual[w.name] = {
      pym_gated_V: summarize(pymDaily),
      spxw_s6_vanna: summarize(spxwDaily),
      spy_s5_charm: summarize(charmDaily),
    };
    allResults.weights_equal[w.name] = summarize(eq);
    allResults.weights_concentrated[w.name] = summarize(concentrated);
  }

  // Print
  for (const w of WINDOWS) {
    process.stdout.write(`\n=== ${w.name} (${w.startDate} → ${w.endDate}) ===\n`);
    const ind = allResults.individual[w.name];
    for (const [k, s] of Object.entries(ind)) {
      process.stdout.write(`  ${k.padEnd(20)} days=${String(s.trade_days).padStart(4)} net=${s.total_net_pct.toFixed(2).padStart(7)}% sharpe=${s.sharpe_per_day.toFixed(2).padStart(6)} dd=${s.max_drawdown_pct.toFixed(2)}% win=${s.win_rate}%\n`);
    }
    const eq = allResults.weights_equal[w.name];
    const co = allResults.weights_concentrated[w.name];
    process.stdout.write(`  ${'COMBO_equal_1/3'.padEnd(20)} days=${String(eq.trade_days).padStart(4)} net=${eq.total_net_pct.toFixed(2).padStart(7)}% sharpe=${eq.sharpe_per_day.toFixed(2).padStart(6)} dd=${eq.max_drawdown_pct.toFixed(2)}% win=${eq.win_rate}%\n`);
    process.stdout.write(`  ${'COMBO_70_15_15'.padEnd(20)} days=${String(co.trade_days).padStart(4)} net=${co.total_net_pct.toFixed(2).padStart(7)}% sharpe=${co.sharpe_per_day.toFixed(2).padStart(6)} dd=${co.max_drawdown_pct.toFixed(2)}% win=${co.win_rate}%\n`);
  }

  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'combined-portfolio-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), windows: WINDOWS, results: allResults });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
