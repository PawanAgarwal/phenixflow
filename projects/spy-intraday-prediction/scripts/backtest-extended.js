#!/usr/bin/env node
// Rerun all strategies on the EXTENDED 16-month dataset.
// Train: 2025-01 to 2025-12 (12 months)
// Test:  2026-01, 2026-02, 2026-03, 2026-04 (4 test months)
// Plus full-history aggregate.

const path = require('node:path');

const { PROJECT_ROOT } = require('../src/config');
const { runStrategy, runSwingStrategy, writeJson } = require('../src/strategy-runner');
const strategies = require('../src/option-flow-strategies');

const WINDOWS = [
  { name: 'train_2025', startDate: '2025-01-02', endDate: '2025-12-31' },
  { name: 'test_2026_01', startDate: '2026-01-02', endDate: '2026-01-30' },
  { name: 'test_2026_02', startDate: '2026-02-02', endDate: '2026-02-27' },
  { name: 'test_2026_03', startDate: '2026-03-02', endDate: '2026-03-31' },
  { name: 'test_2026_04', startDate: '2026-04-01', endDate: '2026-04-27' },
  { name: 'full_history', startDate: '2025-01-02', endDate: '2026-04-27' },
];

const ALL_STRATEGIES = [
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
  { name: 'B0_buy_hold_day', fn: strategies.strategyBuyHoldDay, mode: 'intraday' },
];

function parseArgs(argv) {
  const out = { root: 'SPY' };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === '--root') out.root = argv[++i].toUpperCase();
  }
  return out;
}

function summarize(s) {
  return {
    trade_count: s.trade_count,
    hit_rate: Number((s.hit_rate || 0).toFixed(4)),
    total_gross_pct: Number((s.total_gross_return * 100).toFixed(2)),
    total_net_pct: Number((s.total_net_return * 100).toFixed(2)),
    avg_net_bps: Number((s.avg_net_return * 10_000).toFixed(2)),
    sharpe_per_trade: Number((s.sharpe_per_trade || 0).toFixed(2)),
    max_drawdown_net_pct: Number((s.max_drawdown_net * 100).toFixed(2)),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const allResults = {};
  for (const w of WINDOWS) {
    process.stdout.write(`\n=== ${w.name} (${w.startDate} → ${w.endDate}) ===\n`);
    allResults[w.name] = [];
    for (const spec of ALL_STRATEGIES) {
      const runner = spec.mode === 'swing' ? runSwingStrategy : runStrategy;
      // eslint-disable-next-line no-await-in-loop
      const r = await runner({
        projectRoot: PROJECT_ROOT,
        root: args.root,
        strategyName: spec.name,
        strategyFn: spec.fn,
        startDate: w.startDate,
        endDate: w.endDate,
        costsBpsRoundTrip: 2,
      });
      const s = r.stats;
      process.stdout.write(
        `  ${spec.name.padEnd(20)} ${spec.mode.padEnd(8)} N=${String(s.trade_count).padStart(5)} net=${(s.total_net_return*100).toFixed(2).padStart(7)}% hit=${(s.hit_rate*100).toFixed(1).padStart(5)}% sharpe=${(s.sharpe_per_trade||0).toFixed(2).padStart(6)} dd=${(s.max_drawdown_net*100).toFixed(2)}%\n`,
      );
      allResults[w.name].push({ strategy: spec.name, mode: spec.mode, stats: summarize(s) });
    }
  }
  const outPath = path.join(PROJECT_ROOT, 'artifacts', `extended-strategies-summary-${args.root}.json`);
  writeJson(outPath, { generated_at: new Date().toISOString(), root: args.root, windows: WINDOWS, by_window: allResults });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
