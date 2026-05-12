#!/usr/bin/env node
// Phase 24 stress test for the 4 new orthogonal PYM-gated variants.
// Loads each variant's pre-built artifact, computes train/test slice stats,
// cost sensitivity, monthly stability, and applies the 4-check pass/fail rule:
//   1. positive on train AND positive on test
//   2. positive in ≥ 2 of 4 test months (Jan, Feb, Mar, Apr 2026)
//   3. still profitable at +50% cost (3 bps for 1×, 4.5 bps for 3×)
//   4. max drawdown < 25%

const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT } = require('../src/config');

const ARTIFACTS_DIR = path.join(PROJECT_ROOT, 'artifacts');
const TRAIN_START = '2025-01-02';
const TRAIN_END = '2025-12-31';
const TEST_START = '2026-01-02';
const TEST_END = '2026-04-27';

const VARIANTS = [
  { id: 'pym-gated-intraday-long-only', baseCostBps: 2 },
  { id: 'pym-gated-intraday-tight-bias', baseCostBps: 2 },
  { id: 'pym-gated-intraday-flow-weighted', baseCostBps: 2 },
  { id: 'pym-gated-spxw-vanna-swing', baseCostBps: 2 },
];

function loadVariant(id) {
  const p = path.join(ARTIFACTS_DIR, `${id}-report.json`);
  if (!fs.existsSync(p)) throw new Error(`missing artifact: ${p}`);
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function sliceTrades(trades, startDate, endDate) {
  return trades.filter((t) => t.date >= startDate && t.date <= endDate);
}

function summarize(trades) {
  if (!trades.length) return { n: 0, net: 0, gross: 0, sharpe: 0, dd: 0, hit: 0 };
  const n = trades.length;
  const sumNet = trades.reduce((a, t) => a + t.netReturn, 0);
  const sumGross = trades.reduce((a, t) => a + t.grossReturn, 0);
  const wins = trades.filter((t) => t.netReturn > 0).length;
  const m = sumNet / n;
  const sd = Math.sqrt(trades.reduce((a, t) => a + ((t.netReturn - m) ** 2), 0) / n);
  // Trade-by-trade equity walk for DD
  let equity = 1; let peak = 1; let dd = 0;
  for (const t of trades) {
    equity *= (1 + t.netReturn);
    if (equity > peak) peak = equity;
    if (peak > 0) dd = Math.min(dd, equity / peak - 1);
  }
  return {
    n,
    net: sumNet * 100,
    gross: sumGross * 100,
    sharpe: sd > 0 ? (m / sd) * Math.sqrt(252) : 0,
    dd: dd * 100,
    hit: (wins / n) * 100,
  };
}

function applyCostBps(trades, costBps) {
  // Re-derive net at a different cost. The existing trade stores gross + cost + size.
  // If size is set (flow-weighted), cost scales with size.
  return trades.map((t) => {
    const size = t.size ?? 1;
    const cost = (costBps / 10_000) * size;
    return { ...t, cost, netReturn: t.grossReturn - cost };
  });
}

function monthlyBreakdown(trades) {
  const months = new Map();
  for (const t of trades) {
    const mo = t.date.slice(0, 7);
    if (!months.has(mo)) months.set(mo, { n: 0, net: 0, wins: 0 });
    const m = months.get(mo);
    m.n += 1; m.net += t.netReturn; if (t.netReturn > 0) m.wins += 1;
  }
  return Array.from(months.entries()).sort()
    .map(([month, m]) => ({ month, n: m.n, netPct: m.net * 100, hitPct: m.n ? (m.wins / m.n) * 100 : 0 }));
}

function fmtPct(x) { return (x >= 0 ? '+' : '') + x.toFixed(2) + '%'; }
function pad(s, w) { return String(s).padStart(w); }

function stressCheck(variant) {
  const report = loadVariant(variant.id);
  const trades = report.trades || [];
  const train = sliceTrades(trades, TRAIN_START, TRAIN_END);
  const test = sliceTrades(trades, TEST_START, TEST_END);
  const trainStats = summarize(train);
  const testStats = summarize(test);

  // Cost sensitivity sweeps
  const costSensitivity = [0, 2, 4, 6, 10].map((bps) => {
    const re = applyCostBps(trades, bps);
    const t = summarize(re);
    return { costBps: bps, ...t };
  });

  // Stressed cost (+50%) — 3 bps for the 1× variants we have here
  const stressedCost = Math.round(variant.baseCostBps * 1.5 * 10) / 10;
  const stressedTrades = applyCostBps(trades, stressedCost);
  const stressedTrain = summarize(sliceTrades(stressedTrades, TRAIN_START, TRAIN_END));
  const stressedTest = summarize(sliceTrades(stressedTrades, TEST_START, TEST_END));

  // Test-month stability (Jan/Feb/Mar/Apr 2026)
  const testMonths = monthlyBreakdown(test);
  const positiveTestMonths = testMonths.filter((m) => m.netPct > 0).length;
  const monthlyFull = monthlyBreakdown(trades);

  // Stress checks
  const checks = {
    trainAndTestPositive: trainStats.net > 0 && testStats.net > 0,
    testMonthStability: positiveTestMonths >= 2,
    profitableAtStressedCost: stressedTrain.net > 0 && stressedTest.net > 0,
    maxDdUnder25: Math.abs(trainStats.dd) < 25 && Math.abs(testStats.dd) < 25,
  };
  const passed = Object.values(checks).every(Boolean);

  return {
    variantId: variant.id,
    baseCostBps: variant.baseCostBps,
    stressedCostBps: stressedCost,
    fullStats: summarize(trades),
    trainStats,
    testStats,
    stressedTrain,
    stressedTest,
    costSensitivity,
    testMonths,
    monthlyFull,
    positiveTestMonths,
    checks,
    passed,
  };
}

function printResult(r) {
  process.stdout.write(`\n=== ${r.variantId}  (base cost ${r.baseCostBps} bps RT) ===\n`);
  process.stdout.write(`  full     : N=${pad(r.fullStats.n, 4)} net=${pad(fmtPct(r.fullStats.net), 8)} sharpe=${r.fullStats.sharpe.toFixed(2)} dd=${r.fullStats.dd.toFixed(2)}% hit=${r.fullStats.hit.toFixed(1)}%\n`);
  process.stdout.write(`  train    : N=${pad(r.trainStats.n, 4)} net=${pad(fmtPct(r.trainStats.net), 8)} sharpe=${r.trainStats.sharpe.toFixed(2)} dd=${r.trainStats.dd.toFixed(2)}% hit=${r.trainStats.hit.toFixed(1)}%\n`);
  process.stdout.write(`  test     : N=${pad(r.testStats.n, 4)} net=${pad(fmtPct(r.testStats.net), 8)} sharpe=${r.testStats.sharpe.toFixed(2)} dd=${r.testStats.dd.toFixed(2)}% hit=${r.testStats.hit.toFixed(1)}%\n`);
  process.stdout.write(`  +50% cost: train ${pad(fmtPct(r.stressedTrain.net), 8)}  test ${pad(fmtPct(r.stressedTest.net), 8)}  (${r.stressedCostBps} bps)\n`);
  process.stdout.write('  cost sensitivity:\n');
  for (const c of r.costSensitivity) {
    process.stdout.write(`    cost=${pad(c.costBps, 2)}bps: net=${pad(fmtPct(c.net), 8)} sharpe=${c.sharpe.toFixed(2)} dd=${c.dd.toFixed(2)}%\n`);
  }
  process.stdout.write('  test months:\n');
  for (const m of r.testMonths) {
    process.stdout.write(`    ${m.month}: N=${pad(m.n, 3)} net=${pad(fmtPct(m.netPct), 8)} hit=${m.hitPct.toFixed(0)}%\n`);
  }
  process.stdout.write(`  test months positive: ${r.positiveTestMonths}/${r.testMonths.length}\n`);
  process.stdout.write('  STRESS CHECKS:\n');
  process.stdout.write(`    [${r.checks.trainAndTestPositive ? 'PASS' : 'FAIL'}] train AND test both positive\n`);
  process.stdout.write(`    [${r.checks.testMonthStability ? 'PASS' : 'FAIL'}] ≥ 2/4 test months positive\n`);
  process.stdout.write(`    [${r.checks.profitableAtStressedCost ? 'PASS' : 'FAIL'}] profitable at +50% cost (${r.stressedCostBps} bps)\n`);
  process.stdout.write(`    [${r.checks.maxDdUnder25 ? 'PASS' : 'FAIL'}] max drawdown < 25% (train ${r.trainStats.dd.toFixed(1)}%, test ${r.testStats.dd.toFixed(1)}%)\n`);
  process.stdout.write(`  VERDICT: ${r.passed ? 'PASS (register)' : 'FAIL (drop)'}\n`);
}

async function main() {
  process.stdout.write('\n=== Phase 24 stress test — 4 orthogonal PYM-gated variants ===\n');
  process.stdout.write(`Train: ${TRAIN_START} → ${TRAIN_END}\n`);
  process.stdout.write(`Test:  ${TEST_START} → ${TEST_END}\n`);

  const results = [];
  for (const v of VARIANTS) {
    const r = stressCheck(v);
    printResult(r);
    results.push(r);
  }

  process.stdout.write('\n=== SUMMARY ===\n');
  for (const r of results) {
    process.stdout.write(`  ${r.variantId.padEnd(36)}  train ${pad(fmtPct(r.trainStats.net), 8)}  test ${pad(fmtPct(r.testStats.net), 8)}  ${r.passed ? 'PASS' : 'FAIL'}\n`);
  }

  const out = path.join(ARTIFACTS_DIR, 'pym-gated-phase24-stress-test.json');
  fs.writeFileSync(out, JSON.stringify({
    generatedAt: new Date().toISOString(),
    trainStart: TRAIN_START, trainEnd: TRAIN_END,
    testStart: TEST_START, testEnd: TEST_END,
    results,
  }, null, 2));
  process.stdout.write(`\nWritten ${out}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
