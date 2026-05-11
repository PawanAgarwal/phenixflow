#!/usr/bin/env node
// Run all 7 BullFlow/CheddarFlow strategies under three day-filter regimes:
//   - all_days
//   - event_only (FOMC, CPI, PPI, NFP, OPEX)
//   - non_event
// Compares P&L per slice to see if flow signals are regime-conditional.

const path = require('node:path');

const { PROJECT_ROOT } = require('../src/config');
const { runStrategy, runSwingStrategy, writeJson } = require('../src/strategy-runner');
const strategies = require('../src/option-flow-strategies');

const FULL_WINDOW = { startDate: '2026-01-02', endDate: '2026-04-27' };

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

const SLICES = [
  { name: 'all_days', filter: null },
  { name: 'event_only', filter: (_d, tag) => tag.is_event },
  { name: 'non_event', filter: (_d, tag) => !tag.is_event },
  { name: 'fomc_only', filter: (_d, tag) => tag.is_fomc },
  { name: 'cpi_only', filter: (_d, tag) => tag.is_cpi },
  { name: 'nfp_only', filter: (_d, tag) => tag.is_nfp },
  { name: 'opex_only', filter: (_d, tag) => tag.is_opex },
];

function fmt(n) { return (n * 100).toFixed(2).padStart(7); }
function fmtBps(n) { return (n * 10000).toFixed(2).padStart(7); }

async function run() {
  const projectRoot = PROJECT_ROOT;
  const costsBps = 2;
  const results = {};
  for (const slice of SLICES) {
    process.stdout.write(`\n=== slice: ${slice.name} ===\n`);
    results[slice.name] = [];
    for (const spec of ALL_STRATEGIES) {
      const runner = spec.mode === 'swing' ? runSwingStrategy : runStrategy;
      // eslint-disable-next-line no-await-in-loop
      const r = await runner({
        projectRoot,
        root: 'SPY',
        strategyName: spec.name,
        strategyFn: spec.fn,
        startDate: FULL_WINDOW.startDate,
        endDate: FULL_WINDOW.endDate,
        costsBpsRoundTrip: costsBps,
        dayFilter: slice.filter,
      });
      const s = r.stats;
      process.stdout.write(`  ${spec.name.padEnd(20)} ${spec.mode.padEnd(8)} N=${String(s.trade_count).padStart(4)} net=${fmt(s.total_net_return)}% hit=${(s.hit_rate*100).toFixed(1).padStart(5)}% sharpe=${(s.sharpe_per_trade||0).toFixed(2).padStart(6)}\n`);
      results[slice.name].push({ strategy: spec.name, mode: spec.mode, stats: {
        trade_count: s.trade_count,
        hit_rate: Number((s.hit_rate||0).toFixed(4)),
        total_net_return_pct: Number((s.total_net_return*100).toFixed(3)),
        total_gross_return_pct: Number((s.total_gross_return*100).toFixed(3)),
        avg_net_return_bps: Number((s.avg_net_return*10000).toFixed(2)),
        sharpe_per_trade: Number((s.sharpe_per_trade||0).toFixed(2)),
        max_drawdown_net_pct: Number((s.max_drawdown_net*100).toFixed(2)),
      }});
    }
  }
  const outPath = path.join(projectRoot, 'artifacts', 'option-flow-event-gated-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), window: FULL_WINDOW, costs_bps: costsBps, slices: results });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

run().catch((err) => { process.stderr.write(`${err.stack||err.message}\n`); process.exit(1); });
