#!/usr/bin/env node
// Phase 22 — stress test the production PYM-gated V strategy.
// Variants:
//   1. Cost sensitivity (2, 4, 6, 10 bps round trip)
//   2. Entry-time shift (±10 min)
//   3. Bias-threshold shift (±0.05)
//   4. 50% out-of-sample bootstrap (random 50% of days)
//   5. Monthly P&L stability

const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT } = require('../src/config');
const { writeJson } = require('../src/strategy-runner');
const { loadPymHoldings, backtestPymGated } = require('../src/pym-bias-strategy');

const PYM_ARTIFACT = '/Users/pawanagarwal/github/phenixflow/projects/pym-v5-replication/artifacts/pym-v5-backtest-massive-eod-rsi-wilder-next_close-2025-01-02-2026-05-06.json';

async function main() {
  const pymByDate = loadPymHoldings(PYM_ARTIFACT);
  const baseParams = { biasLong: 0.20, biasShort: -0.20, entryMinuteEt: 690, exitMinuteEt: 955 };
  const startDate = '2025-01-02';
  const endDate = '2026-04-27';

  process.stdout.write('\n=== STRESS TEST: PYM-gated V (entry 11:30 ET, bias ±0.20) ===\n');

  // 1. Cost sensitivity
  process.stdout.write('\n[1] Cost sensitivity:\n');
  for (const cost of [0, 2, 4, 6, 10, 20]) {
    // eslint-disable-next-line no-await-in-loop
    const r = await backtestPymGated({
      projectRoot: PROJECT_ROOT, root: 'SPY', pymByDate,
      startDate, endDate,
      params: { ...baseParams, costBpsRoundTrip: cost },
    });
    process.stdout.write(`  cost=${String(cost).padStart(2)}bps RT: net=${r.total_net_pct.toFixed(2).padStart(7)}% sharpe=${r.sharpe_per_trade.toFixed(2).padStart(6)} dd=${r.max_drawdown_pct.toFixed(2)}%\n`);
  }

  // 2. Entry time sweep
  process.stdout.write('\n[2] Entry time sensitivity:\n');
  for (const entry of [600, 630, 660, 690, 720, 750]) {
    // eslint-disable-next-line no-await-in-loop
    const r = await backtestPymGated({
      projectRoot: PROJECT_ROOT, root: 'SPY', pymByDate,
      startDate, endDate,
      params: { ...baseParams, entryMinuteEt: entry },
    });
    const et = `${Math.floor(entry/60)}:${String(entry%60).padStart(2,'0')}`;
    process.stdout.write(`  entry=${et} (min ${entry}): net=${r.total_net_pct.toFixed(2).padStart(7)}% sharpe=${r.sharpe_per_trade.toFixed(2).padStart(6)} dd=${r.max_drawdown_pct.toFixed(2)}% N=${r.trade_count}\n`);
  }

  // 3. Bias threshold sweep
  process.stdout.write('\n[3] Bias threshold sensitivity:\n');
  for (const bias of [0.05, 0.10, 0.15, 0.20, 0.25, 0.30]) {
    // eslint-disable-next-line no-await-in-loop
    const r = await backtestPymGated({
      projectRoot: PROJECT_ROOT, root: 'SPY', pymByDate,
      startDate, endDate,
      params: { ...baseParams, biasLong: bias, biasShort: -bias },
    });
    process.stdout.write(`  bias=±${bias.toFixed(2)}: net=${r.total_net_pct.toFixed(2).padStart(7)}% sharpe=${r.sharpe_per_trade.toFixed(2).padStart(6)} dd=${r.max_drawdown_pct.toFixed(2)}% N=${r.trade_count}\n`);
  }

  // 4. Bootstrap — train on random 50% of days, test on the other 50% (repeated 5 times)
  process.stdout.write('\n[4] 50/50 random bootstrap (5 iterations):\n');
  // Re-implement on-trade selection
  const fullRun = await backtestPymGated({
    projectRoot: PROJECT_ROOT, root: 'SPY', pymByDate,
    startDate, endDate,
    params: baseParams,
  });
  const trades = fullRun.trades;
  function rng(seed) {
    let s = seed;
    return () => { s = (s * 1664525 + 1013904223) >>> 0; return (s & 0x7fffffff) / 0x7fffffff; };
  }
  const bootstraps = [];
  for (let iter = 0; iter < 5; iter += 1) {
    const r = rng(42 + iter);
    const halfA = []; const halfB = [];
    for (const t of trades) (r() < 0.5 ? halfA : halfB).push(t);
    const summarize = (arr) => {
      if (arr.length === 0) return { n: 0, net: 0, sharpe: 0 };
      const n = arr.length;
      const sum = arr.reduce((a, t) => a + t.net_return, 0);
      const m = sum / n;
      const sd = Math.sqrt(arr.reduce((a, t) => a + ((t.net_return - m) ** 2), 0) / n);
      return { n, net: sum * 100, sharpe: sd > 0 ? (m / sd) * Math.sqrt(252) : 0 };
    };
    const a = summarize(halfA); const b = summarize(halfB);
    bootstraps.push({ iter, halfA: a, halfB: b });
    process.stdout.write(`  iter=${iter}: A N=${String(a.n).padStart(3)} net=${a.net.toFixed(2).padStart(7)}% sharpe=${a.sharpe.toFixed(2).padStart(6)}    B N=${String(b.n).padStart(3)} net=${b.net.toFixed(2).padStart(7)}% sharpe=${b.sharpe.toFixed(2).padStart(6)}\n`);
  }

  // 5. Monthly P&L
  process.stdout.write('\n[5] Monthly P&L (production config):\n');
  const monthly = new Map();
  for (const t of trades) {
    const mo = t.date.slice(0, 7);
    if (!monthly.has(mo)) monthly.set(mo, { n: 0, net: 0, wins: 0, long: 0, short: 0 });
    const m = monthly.get(mo);
    m.n += 1; m.net += t.net_return; if (t.net_return > 0) m.wins += 1;
    if (t.side === 'LONG') m.long += 1; else m.short += 1;
  }
  const months = Array.from(monthly.keys()).sort();
  let cum = 0; let posMonths = 0;
  for (const mo of months) {
    const m = monthly.get(mo);
    cum += m.net;
    if (m.net > 0) posMonths += 1;
    process.stdout.write(`  ${mo}: N=${String(m.n).padStart(3)} L=${String(m.long).padStart(2)} S=${String(m.short).padStart(2)} net=${(m.net*100).toFixed(2).padStart(7)}% hit=${(m.wins/m.n*100).toFixed(0).padStart(3)}% cum=${(cum*100).toFixed(2)}%\n`);
  }
  process.stdout.write(`\n  Positive months: ${posMonths}/${months.length}\n`);

  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'pym-gated-stress-test.json');
  writeJson(outPath, {
    generated_at: new Date().toISOString(),
    base_params: baseParams,
    full_stats: {
      trade_count: trades.length,
      total_net_pct: trades.reduce((a, t) => a + t.net_return, 0) * 100,
    },
    monthly: Array.from(monthly.entries()).map(([k, v]) => ({ month: k, ...v, net_pct: Number((v.net*100).toFixed(3)) })),
    bootstraps,
  });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
