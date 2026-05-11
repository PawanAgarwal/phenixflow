#!/usr/bin/env node
// Walk-forward validation of the event-gated composite portfolio.
//
// Step 1: On the TRAIN window (Jan 2026) alone, rank every (strategy, slice) combination
//         by total_net_return_pct. Pick the top-K that are positive on train.
// Step 2: Apply that exact selection to the TEST windows (Feb, Mar, Apr 2026) — unseen.
// Step 3: Compare composite test returns to "buy-hold" benchmark on the same windows.
//
// This protects against the post-hoc bias of Phase 6 (which used the full window to pick winners).

const path = require('node:path');

const { PROJECT_ROOT } = require('../src/config');
const { runStrategy, runSwingStrategy, writeJson } = require('../src/strategy-runner');
const strategies = require('../src/option-flow-strategies');

const TRAIN_WINDOW = { name: 'train_2026_01', startDate: '2026-01-02', endDate: '2026-01-30' };
const TEST_WINDOWS = [
  { name: 'test_2026_02', startDate: '2026-02-02', endDate: '2026-02-27' },
  { name: 'test_2026_03', startDate: '2026-03-02', endDate: '2026-03-31' },
  { name: 'test_2026_04', startDate: '2026-04-01', endDate: '2026-04-27' },
];

// All candidate (strategy, slice) combinations we'll evaluate on train.
const STRATEGY_SPECS = [
  { name: 'S1_sweep_momentum', fn: strategies.strategyS1, mode: 'intraday' },
  { name: 'S2_block_follow', fn: strategies.strategyS2, mode: 'intraday' },
  { name: 'S3_vgex_regime', fn: strategies.strategyS3, mode: 'intraday' },
  { name: 'S4_0dte_squeeze', fn: strategies.strategyS4, mode: 'intraday' },
  { name: 'S5_charm_pin', fn: strategies.strategyS5, mode: 'intraday' },
  { name: 'S6_vanna_trend', fn: strategies.strategyS6, mode: 'swing' },
  { name: 'S7_premium_flow', fn: strategies.strategyS7, mode: 'swing' },
  { name: 'S1c_sweep_fade', fn: strategies.strategyS1Contrarian, mode: 'intraday' },
  { name: 'S2c_block_fade', fn: strategies.strategyS2Contrarian, mode: 'intraday' },
  { name: 'S4c_0dte_fade', fn: strategies.strategyS4Contrarian, mode: 'intraday' },
];

const SLICES = [
  { name: 'all_days', filter: null },
  { name: 'event_only', filter: (_d, tag) => tag.is_event },
  { name: 'non_event', filter: (_d, tag) => !tag.is_event },
];

async function runOne({ spec, slice, startDate, endDate }) {
  const runner = spec.mode === 'swing' ? runSwingStrategy : runStrategy;
  return runner({
    projectRoot: PROJECT_ROOT,
    root: 'SPY',
    strategyName: spec.name,
    strategyFn: spec.fn,
    startDate,
    endDate,
    costsBpsRoundTrip: 2,
    dayFilter: slice.filter,
  });
}

async function runAllCombos(startDate, endDate) {
  const out = [];
  for (const spec of STRATEGY_SPECS) {
    for (const slice of SLICES) {
      // eslint-disable-next-line no-await-in-loop
      const r = await runOne({ spec, slice, startDate, endDate });
      out.push({
        strategy: spec.name,
        slice: slice.name,
        mode: spec.mode,
        stats: r.stats,
      });
    }
  }
  return out;
}

function fmtPct(x) { return (x * 100).toFixed(2).padStart(7); }

async function main() {
  process.stdout.write(`\n=== Walk-forward validation of event-gated composite ===\n`);
  process.stdout.write(`Train window: ${TRAIN_WINDOW.startDate} → ${TRAIN_WINDOW.endDate}\n`);
  process.stdout.write(`Test windows: ${TEST_WINDOWS.map((w) => w.name).join(', ')}\n\n`);

  // Step 1: train
  process.stdout.write(`--- Step 1: Score all (strategy × slice) on TRAIN ---\n`);
  const trainResults = await runAllCombos(TRAIN_WINDOW.startDate, TRAIN_WINDOW.endDate);

  // Sort by total_net_return on train; positive only; require minimum trade count
  const trainSorted = trainResults
    .filter((r) => r.stats.trade_count >= 3) // need at least 3 trades for any signal at all
    .sort((a, b) => b.stats.total_net_return - a.stats.total_net_return);

  process.stdout.write(`\nTop train candidates (sorted by train net %):\n`);
  for (const r of trainSorted.slice(0, 12)) {
    process.stdout.write(`  ${r.strategy.padEnd(20)} ${r.slice.padEnd(12)} N=${String(r.stats.trade_count).padStart(3)} train_net=${fmtPct(r.stats.total_net_return)}% hit=${(r.stats.hit_rate*100).toFixed(1).padStart(5)}%\n`);
  }

  // Selection rule: pick (strategy, slice) combos that are positive on train AND have ≥3 trades.
  // To avoid one strategy dominating across slices (e.g. S6 on all/event/non), pick at most one slice per strategy
  // — the one with the best train net %.
  const bestSlicePerStrategy = new Map();
  for (const r of trainSorted) {
    if (r.stats.total_net_return <= 0) continue;
    const existing = bestSlicePerStrategy.get(r.strategy);
    if (!existing || r.stats.total_net_return > existing.stats.total_net_return) {
      bestSlicePerStrategy.set(r.strategy, r);
    }
  }
  const selected = Array.from(bestSlicePerStrategy.values())
    .sort((a, b) => b.stats.total_net_return - a.stats.total_net_return);

  process.stdout.write(`\nSelected for composite (best-slice-per-strategy on train, train_net > 0):\n`);
  for (const r of selected) {
    process.stdout.write(`  ${r.strategy.padEnd(20)} ${r.slice.padEnd(12)} train_net=${fmtPct(r.stats.total_net_return)}% trades=${r.stats.trade_count}\n`);
  }
  if (selected.length === 0) {
    process.stdout.write('\nNo positive train candidates. Cannot build composite.\n');
    return;
  }

  // Step 2: re-run those exact selections on TEST windows
  process.stdout.write(`\n--- Step 2: Apply selection to TEST windows ---\n`);
  const testResults = {};
  for (const testW of TEST_WINDOWS) {
    testResults[testW.name] = [];
    process.stdout.write(`\n${testW.name} (${testW.startDate} → ${testW.endDate}):\n`);
    let compositeNet = 0;
    let compositeTrades = 0;
    for (const sel of selected) {
      const spec = STRATEGY_SPECS.find((s) => s.name === sel.strategy);
      const slice = SLICES.find((s) => s.name === sel.slice);
      // eslint-disable-next-line no-await-in-loop
      const r = await runOne({ spec, slice, startDate: testW.startDate, endDate: testW.endDate });
      const net = r.stats.total_net_return;
      compositeNet += net;
      compositeTrades += r.stats.trade_count;
      process.stdout.write(`  ${sel.strategy.padEnd(20)} ${sel.slice.padEnd(12)} N=${String(r.stats.trade_count).padStart(3)} test_net=${fmtPct(net)}% hit=${(r.stats.hit_rate*100).toFixed(1).padStart(5)}%\n`);
      testResults[testW.name].push({
        strategy: sel.strategy,
        slice: sel.slice,
        train_net_pct: Number((sel.stats.total_net_return * 100).toFixed(2)),
        test_stats: r.stats,
      });
    }
    process.stdout.write(`  ${' '.repeat(33)} ----------\n`);
    process.stdout.write(`  ${'COMPOSITE'.padEnd(33)} N=${String(compositeTrades).padStart(3)} test_net=${fmtPct(compositeNet)}%\n`);
    testResults[testW.name].push({ strategy: '__composite__', composite_net_pct: Number((compositeNet * 100).toFixed(2)), composite_trades: compositeTrades });
  }

  // Step 3: also compute the all-test-windows totals
  let totalNet = 0; let totalTrades = 0;
  for (const w of TEST_WINDOWS) {
    const last = testResults[w.name][testResults[w.name].length - 1];
    if (last && last.composite_net_pct) {
      totalNet += last.composite_net_pct;
      totalTrades += last.composite_trades;
    }
  }
  process.stdout.write(`\n=== AGGREGATE TEST COMPOSITE ===\n`);
  process.stdout.write(`Total test trades:  ${totalTrades}\n`);
  process.stdout.write(`Total test net:     ${totalNet.toFixed(2)}% across Feb+Mar+Apr 2026\n`);

  // Compare to buy-hold benchmark on same test windows
  process.stdout.write(`\n--- Buy-hold benchmark on test windows ---\n`);
  let bhTotal = 0;
  for (const testW of TEST_WINDOWS) {
    // eslint-disable-next-line no-await-in-loop
    const r = await runOne({
      spec: { name: 'B0_buy_hold_day', fn: strategies.strategyBuyHoldDay, mode: 'intraday' },
      slice: SLICES[0],
      startDate: testW.startDate,
      endDate: testW.endDate,
    });
    process.stdout.write(`  ${testW.name.padEnd(20)} buyhold_net=${fmtPct(r.stats.total_net_return)}%\n`);
    bhTotal += r.stats.total_net_return * 100;
  }
  process.stdout.write(`  ${'TOTAL'.padEnd(20)} buyhold_net=${bhTotal.toFixed(2).padStart(7)}%\n`);

  process.stdout.write(`\n=== HEAD-TO-HEAD ===\n`);
  process.stdout.write(`Composite test return: ${totalNet.toFixed(2).padStart(7)}%\n`);
  process.stdout.write(`Buy-hold test return:  ${bhTotal.toFixed(2).padStart(7)}%\n`);
  process.stdout.write(`Edge (composite - bh): ${(totalNet - bhTotal).toFixed(2).padStart(7)}%\n`);

  // Save
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'walk-forward-composite-summary.json');
  writeJson(outPath, {
    generated_at: new Date().toISOString(),
    train_window: TRAIN_WINDOW,
    test_windows: TEST_WINDOWS,
    train_scores: trainResults,
    selected_composite: selected.map((s) => ({ strategy: s.strategy, slice: s.slice, train_net_pct: Number((s.stats.total_net_return * 100).toFixed(2)) })),
    test_results: testResults,
    aggregate: { total_test_net_pct: Number(totalNet.toFixed(2)), total_test_trades: totalTrades, buy_hold_test_net_pct: Number(bhTotal.toFixed(2)) },
  });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
