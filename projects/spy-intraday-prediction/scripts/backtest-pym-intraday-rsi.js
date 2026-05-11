#!/usr/bin/env node
const path = require('node:path');
const { PROJECT_ROOT } = require('../src/config');
const { writeJson } = require('../src/strategy-runner');
const { loadPymHoldings } = require('../src/pym-bias-strategy');
const { backtestPymWithIntradayRsi } = require('../src/pym-intraday-rsi');

const PYM_ARTIFACT = '/Users/pawanagarwal/github/phenixflow/projects/pym-v5-replication/artifacts/pym-v5-backtest-massive-eod-rsi-wilder-next_close-2025-01-02-2026-05-06.json';

const WINDOWS = [
  { name: 'train_2025', startDate: '2025-01-02', endDate: '2025-12-31' },
  { name: 'test_2026', startDate: '2026-01-02', endDate: '2026-04-27' },
  { name: 'full_16mo', startDate: '2025-01-02', endDate: '2026-04-27' },
];

const VARIANTS = [
  { name: 'V1_rsi14_at_1130_50_50', params: { biasLong: 0.20, biasShort: -0.20, rsiPeriod: 14, rsiBullThreshold: 50, rsiBearThreshold: 50, rsiSampleBucket: 0 } },
  { name: 'V2_rsi14_at_1130_55_45', params: { biasLong: 0.20, biasShort: -0.20, rsiPeriod: 14, rsiBullThreshold: 55, rsiBearThreshold: 45, rsiSampleBucket: 0 } },
  { name: 'V3_rsi10_at_1330_55_45', params: { biasLong: 0.20, biasShort: -0.20, rsiPeriod: 10, rsiBullThreshold: 55, rsiBearThreshold: 45, rsiSampleBucket: 1 } },
  { name: 'V4_rsi14_loose_bias', params: { biasLong: 0.10, biasShort: -0.10, rsiPeriod: 14, rsiBullThreshold: 50, rsiBearThreshold: 50, rsiSampleBucket: 0 } },
  { name: 'V5_rsi14_no_rsi_filter', params: { biasLong: 0.20, biasShort: -0.20, rsiPeriod: 14, rsiBullThreshold: 0, rsiBearThreshold: 100, rsiSampleBucket: 0 } },
  { name: 'V6_rsi14_at_1530_60_40', params: { biasLong: 0.20, biasShort: -0.20, rsiPeriod: 14, rsiBullThreshold: 60, rsiBearThreshold: 40, rsiSampleBucket: 2 } },
];

async function main() {
  const pymByDate = loadPymHoldings(PYM_ARTIFACT);
  console.log(`Loaded ${pymByDate.size} PYM days`);
  const results = {};
  for (const v of VARIANTS) {
    process.stdout.write(`\n=== ${v.name} ===\n`);
    process.stdout.write(`  params: ${JSON.stringify(v.params)}\n`);
    results[v.name] = {};
    for (const w of WINDOWS) {
      // eslint-disable-next-line no-await-in-loop
      const r = await backtestPymWithIntradayRsi({
        projectRoot: PROJECT_ROOT,
        pymByDate,
        startDate: w.startDate,
        endDate: w.endDate,
        params: v.params,
      });
      process.stdout.write(`  ${w.name.padEnd(12)} N=${String(r.trade_count).padStart(4)} net=${r.total_net_pct.toFixed(2).padStart(7)}% hit=${(r.hit_rate*100).toFixed(1).padStart(5)}% sharpe=${r.sharpe_per_trade.toFixed(2).padStart(6)} dd=${r.max_drawdown_pct.toFixed(2)}%\n`);
      results[v.name][w.name] = {
        trade_count: r.trade_count, hit_rate: Number(r.hit_rate.toFixed(4)),
        total_gross_pct: Number(r.total_gross_pct.toFixed(2)),
        total_net_pct: Number(r.total_net_pct.toFixed(2)),
        avg_net_bps: Number((r.avg_net_bps || 0).toFixed(2)),
        sharpe_per_trade: Number(r.sharpe_per_trade.toFixed(2)),
        max_drawdown_pct: Number(r.max_drawdown_pct.toFixed(2)),
      };
    }
  }
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'pym-intraday-rsi-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), variants: VARIANTS, results });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
