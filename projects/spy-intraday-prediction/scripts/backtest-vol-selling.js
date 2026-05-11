#!/usr/bin/env node
const path = require('node:path');
const { PROJECT_ROOT } = require('../src/config');
const { runVolSellingStrategy } = require('../src/vol-selling-strategy');
const { writeJson } = require('../src/strategy-runner');

const WINDOWS = [
  { name: 'train_2026_01', startDate: '2026-01-02', endDate: '2026-01-30' },
  { name: 'test_2026_02', startDate: '2026-02-02', endDate: '2026-02-27' },
  { name: 'test_2026_03', startDate: '2026-03-02', endDate: '2026-03-31' },
  { name: 'test_2026_04', startDate: '2026-04-01', endDate: '2026-04-27' },
];

const SLICES = [
  { name: 'all_days', filter: null },
  { name: 'event_only', filter: (_d, tag) => tag.is_event },
  { name: 'non_event', filter: (_d, tag) => !tag.is_event },
];

async function main() {
  const results = {};
  for (const slice of SLICES) {
    process.stdout.write(`\n=== slice: ${slice.name} ===\n`);
    results[slice.name] = {};
    for (const w of WINDOWS) {
      // eslint-disable-next-line no-await-in-loop
      const r = await runVolSellingStrategy({
        projectRoot: PROJECT_ROOT,
        root: 'SPY',
        startDate: w.startDate,
        endDate: w.endDate,
        dayFilter: slice.filter,
      });
      const s = r.stats;
      process.stdout.write(
        `  ${w.name.padEnd(16)} N=${String(s.trade_count).padStart(4)} gross=${s.total_gross_return_pct.toFixed(2).padStart(6)}% net=${s.total_net_return_pct.toFixed(2).padStart(6)}% hit=${(s.hit_rate*100).toFixed(1).padStart(5)}% sharpe=${(s.sharpe_per_trade||0).toFixed(2).padStart(6)} dd=${s.max_drawdown_net_pct.toFixed(2)}% $net=${s.total_net_dollars.toFixed(0)}\n`,
      );
      results[slice.name][w.name] = { stats: s, trades_count: r.trades.length };
      // Save trade detail
      const fp = path.join(PROJECT_ROOT, 'artifacts', 'vol-selling-trades', `${slice.name}-${w.name}.json`);
      writeJson(fp, { window: w, slice: slice.name, params: r.params, stats: s, trades: r.trades });
    }
  }
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'vol-selling-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), windows: WINDOWS, slices: results });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
