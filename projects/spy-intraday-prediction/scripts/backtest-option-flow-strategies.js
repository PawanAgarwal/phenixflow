#!/usr/bin/env node
// Run all 7 BullFlow/CheddarFlow-style strategies (S1-S7) against the
// per-minute feature dataset. Outputs a per-strategy JSON in artifacts/
// and a roll-up comparison report.

const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT } = require('../src/config');
const { runStrategy, runSwingStrategy, writeJson } = require('../src/strategy-runner');
const strategies = require('../src/option-flow-strategies');

const DEFAULT_TRAIN = { startDate: '2026-01-02', endDate: '2026-01-30' };
const DEFAULT_TESTS = [
  { name: 'test_2026_02', startDate: '2026-02-02', endDate: '2026-02-27' },
  { name: 'test_2026_03', startDate: '2026-03-02', endDate: '2026-03-31' },
  { name: 'test_2026_04', startDate: '2026-04-01', endDate: '2026-04-27' },
];

const ALL_STRATEGIES = [
  { name: 'S1_sweep_momentum', fn: strategies.strategyS1, mode: 'intraday' },
  { name: 'S2_block_follow', fn: strategies.strategyS2, mode: 'intraday' },
  { name: 'S3_vgex_regime', fn: strategies.strategyS3, mode: 'intraday' },
  { name: 'S4_0dte_squeeze', fn: strategies.strategyS4, mode: 'intraday' },
  { name: 'S5_charm_pin', fn: strategies.strategyS5, mode: 'intraday' },
  { name: 'S6_vanna_trend', fn: strategies.strategyS6, mode: 'swing' },
  { name: 'S7_premium_flow', fn: strategies.strategyS7, mode: 'swing' },
  // Contrarian variants — flow-followers lost money, so reverse them
  { name: 'S1c_sweep_fade', fn: strategies.strategyS1Contrarian, mode: 'intraday' },
  { name: 'S2c_block_fade', fn: strategies.strategyS2Contrarian, mode: 'intraday' },
  { name: 'S4c_0dte_fade', fn: strategies.strategyS4Contrarian, mode: 'intraday' },
  // Benchmark — buy-and-hold SPY each day open-to-close
  { name: 'B0_buy_hold_day', fn: strategies.strategyBuyHoldDay, mode: 'intraday' },
];

function parseArgs(argv) {
  const out = { root: 'SPY', costsBps: 2 };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--root') out.root = argv[++i].toUpperCase();
    else if (a === '--costs-bps') out.costsBps = Number(argv[++i]);
    else if (a === '--strategies') out.strategies = argv[++i].split(',');
    else if (a === '--start') out.start = argv[++i];
    else if (a === '--end') out.end = argv[++i];
    else if (a === '--window-name') out.windowName = argv[++i];
  }
  return out;
}

function summarizeStats(s) {
  if (!s) return null;
  return {
    trade_count: s.trade_count,
    hit_rate: Number((s.hit_rate || 0).toFixed(4)),
    avg_gross_return_bps: Number((s.avg_gross_return * 10_000).toFixed(2)),
    avg_net_return_bps: Number((s.avg_net_return * 10_000).toFixed(2)),
    total_gross_return_pct: Number((s.total_gross_return * 100).toFixed(2)),
    total_net_return_pct: Number((s.total_net_return * 100).toFixed(2)),
    mean_hold_minutes: Number((s.mean_hold_minutes || 0).toFixed(1)),
    std_net_return_bps: Number((s.std_net_return * 10_000).toFixed(2)),
    sharpe_per_trade: Number((s.sharpe_per_trade || 0).toFixed(2)),
    max_drawdown_net_pct: Number((s.max_drawdown_net * 100).toFixed(2)),
  };
}

async function runForWindow({ projectRoot, root, costsBps, startDate, endDate, strategiesToRun }) {
  const results = [];
  for (const spec of strategiesToRun) {
    const t0 = Date.now();
    const runner = spec.mode === 'swing' ? runSwingStrategy : runStrategy;
    // eslint-disable-next-line no-await-in-loop
    const result = await runner({
      projectRoot,
      root,
      strategyName: spec.name,
      strategyFn: spec.fn,
      startDate,
      endDate,
      costsBpsRoundTrip: costsBps,
    });
    const ms = Date.now() - t0;
    process.stdout.write(`  ${spec.name.padEnd(20)} ${spec.mode.padEnd(8)} trades=${result.stats.trade_count.toString().padStart(4)} net=${(result.stats.total_net_return * 100).toFixed(2).padStart(7)}% hit=${(result.stats.hit_rate * 100).toFixed(1)}% sharpe=${(result.stats.sharpe_per_trade || 0).toFixed(2)} dd=${(result.stats.max_drawdown_net * 100).toFixed(2)}% (${ms}ms)\n`);
    results.push(result);
  }
  return results;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const projectRoot = PROJECT_ROOT;
  const strategiesToRun = args.strategies
    ? ALL_STRATEGIES.filter((s) => args.strategies.includes(s.name) || args.strategies.includes(s.name.split('_')[0]))
    : ALL_STRATEGIES;

  const artifactsDir = path.join(projectRoot, 'artifacts');
  const allResults = {};

  // Train window
  const trainWindow = args.start && args.end
    ? { name: args.windowName || 'custom', startDate: args.start, endDate: args.end }
    : { name: 'train_2026_01', ...DEFAULT_TRAIN };
  process.stdout.write(`\n=== ${trainWindow.name} (${trainWindow.startDate} → ${trainWindow.endDate}) ===\n`);
  const trainResults = await runForWindow({
    projectRoot,
    root: args.root,
    costsBps: args.costsBps,
    startDate: trainWindow.startDate,
    endDate: trainWindow.endDate,
    strategiesToRun,
  });
  allResults[trainWindow.name] = trainResults;

  // Test windows (only if no custom range was provided)
  if (!args.start) {
    for (const tw of DEFAULT_TESTS) {
      process.stdout.write(`\n=== ${tw.name} (${tw.startDate} → ${tw.endDate}) ===\n`);
      // eslint-disable-next-line no-await-in-loop
      const r = await runForWindow({
        projectRoot,
        root: args.root,
        costsBps: args.costsBps,
        startDate: tw.startDate,
        endDate: tw.endDate,
        strategiesToRun,
      });
      allResults[tw.name] = r;
    }
  }

  // Save per-strategy + summary
  const summary = {
    generated_at: new Date().toISOString(),
    root: args.root,
    costs_bps: args.costsBps,
    strategies_run: strategiesToRun.map((s) => s.name),
    by_window: {},
  };
  for (const [windowName, results] of Object.entries(allResults)) {
    summary.by_window[windowName] = results.map((r) => ({
      strategy: r.strategy,
      mode: r.mode || 'intraday',
      stats: summarizeStats(r.stats),
    }));
  }
  const summaryPath = path.join(artifactsDir, `option-flow-strategies-summary-${args.root}.json`);
  writeJson(summaryPath, summary);
  process.stdout.write(`\nSummary written to ${summaryPath}\n`);

  // Save detailed per-strategy trade lists (one file per window)
  for (const [windowName, results] of Object.entries(allResults)) {
    for (const r of results) {
      const fp = path.join(artifactsDir, 'option-flow-trades', `${windowName}-${r.strategy}.json`);
      writeJson(fp, {
        strategy: r.strategy,
        window: r.window,
        costs_bps: r.costsBpsRoundTrip,
        stats: summarizeStats(r.stats),
        trades: r.trades,
      });
    }
  }
  process.stdout.write(`Detailed trades written under ${path.join(artifactsDir, 'option-flow-trades')}\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal: ${err.stack || err.message}\n`);
  process.exit(1);
});
