#!/usr/bin/env node
// Sweep OCC P/C contrarian variants and report walk-forward stability.

const path = require('node:path');
const { PROJECT_ROOT } = require('../src/config');
const { runBacktest, summarizeTrades } = require('../src/occ-pc-contrarian');
const { writeJson } = require('../src/strategy-runner');

const WINDOWS = [
  { name: 'train_2025', startDate: '2025-01-02', endDate: '2025-12-31' },
  { name: 'test_2026', startDate: '2026-01-02', endDate: '2026-04-27' },
  { name: 'full_16mo', startDate: '2025-01-02', endDate: '2026-04-27' },
];

const VARIANTS = [
  // Pure intraday, varying thresholds
  { name: 'A_z1.5_1x_intraday', params: { zEnter: 1.5, leverage: 1.0 } },
  { name: 'B_z2.0_1x_intraday', params: { zEnter: 2.0, leverage: 1.0 } },
  { name: 'C_z2.5_1x_intraday', params: { zEnter: 2.5, leverage: 1.0 } },
  { name: 'D_z3.0_1x_intraday', params: { zEnter: 3.0, leverage: 1.0 } },
  // Long-only (fear extremes only)
  { name: 'E_z2.0_long_only_1x', params: { zEnter: 2.0, leverage: 1.0, longOnly: true } },
  // Short-only (greed extremes only)
  { name: 'F_z2.0_short_only_1x', params: { zEnter: 2.0, leverage: 1.0, shortOnly: true } },
  // 3x leverage variants
  { name: 'G_z2.0_3x_intraday', params: { zEnter: 2.0, leverage: 3.0, costBpsRoundTrip: 3 } },
  { name: 'H_z2.5_3x_intraday', params: { zEnter: 2.5, leverage: 3.0, costBpsRoundTrip: 3 } },
  // Overnight variants
  { name: 'I_z2.0_1x_overnight', params: { zEnter: 2.0, leverage: 1.0, overnight: true } },
  { name: 'J_z2.5_3x_overnight', params: { zEnter: 2.5, leverage: 3.0, overnight: true, costBpsRoundTrip: 3 } },
  // Bias-magnitude sized
  { name: 'K_z2.0_1x_sized', params: { zEnter: 2.0, leverage: 1.0, biasProportionalSize: true, biasSizeMultiplier: 0.4 } },
  // Long-only + overnight + 3x — fear is most actionable
  { name: 'L_z2.0_long_3x_overnight', params: { zEnter: 2.0, leverage: 3.0, longOnly: true, overnight: true, costBpsRoundTrip: 3 } },
];

function fmtPct(x) { return (x).toFixed(2).padStart(7); }

async function main() {
  const allResults = {};
  for (const v of VARIANTS) {
    process.stdout.write(`\n=== ${v.name} ===\n`);
    process.stdout.write(`  params: ${JSON.stringify(v.params)}\n`);
    allResults[v.name] = {};
    for (const w of WINDOWS) {
      // eslint-disable-next-line no-await-in-loop
      const r = await runBacktest({ startDate: w.startDate, endDate: w.endDate, params: v.params });
      const s = summarizeTrades(r.trades);
      process.stdout.write(`  ${w.name.padEnd(12)} N=${String(s.trade_count).padStart(3)} gross=${fmtPct(s.total_gross_pct)}% net=${fmtPct(s.total_net_pct)}% hit=${(s.hit_rate*100).toFixed(1).padStart(5)}% sharpe=${s.sharpe_per_trade.toFixed(2).padStart(6)} dd=${s.max_drawdown_pct.toFixed(2)}% open=${r.openPositions.length}\n`);
      allResults[v.name][w.name] = s;
    }
  }
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'occ-pc-contrarian-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), windows: WINDOWS, variants: VARIANTS, results: allResults });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
