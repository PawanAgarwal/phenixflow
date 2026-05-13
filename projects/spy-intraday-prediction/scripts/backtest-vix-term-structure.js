#!/usr/bin/env node
const path = require('node:path');
const { PROJECT_ROOT } = require('../src/config');
const { runBacktest, summarizeTrades } = require('../src/vix-term-structure');
const { writeJson } = require('../src/strategy-runner');

const WINDOWS = [
  { name: 'train_2025', startDate: '2025-01-02', endDate: '2025-12-31' },
  { name: 'test_2026', startDate: '2026-01-02', endDate: '2026-05-11' },
  { name: 'full_16mo', startDate: '2025-01-02', endDate: '2026-05-11' },
];

const VARIANTS = [
  // VIX1D/VIX3M, varying z-threshold
  { name: 'A_1d3m_z1.5_1x', params: { metric: 'vix1d_over_vix3m', zEnter: 1.5, leverage: 1.0 } },
  { name: 'B_1d3m_z2.0_1x', params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, leverage: 1.0 } },
  { name: 'C_1d3m_z2.5_1x', params: { metric: 'vix1d_over_vix3m', zEnter: 2.5, leverage: 1.0 } },
  // VIX/VIX3M alternate metric
  { name: 'D_vix3m_z1.5_1x', params: { metric: 'vix_over_vix3m', zEnter: 1.5, leverage: 1.0 } },
  { name: 'E_vix3m_z2.0_1x', params: { metric: 'vix_over_vix3m', zEnter: 2.0, leverage: 1.0 } },
  // Inversion-only (LONG only on positive z) — the cleanest contrarian-fear setup
  { name: 'F_1d3m_z1.5_inversion_long_only_1x', params: { metric: 'vix1d_over_vix3m', zEnter: 1.5, inversionLongOnly: true, leverage: 1.0 } },
  { name: 'G_1d3m_z2.0_inversion_long_only_1x', params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, inversionLongOnly: true, leverage: 1.0 } },
  // Contango-only (SHORT on negative z — extreme complacency)
  { name: 'H_1d3m_z2.0_contango_short_only_1x', params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, contangoShortOnly: true, leverage: 1.0 } },
  // 3x leveraged
  { name: 'I_1d3m_z2.0_3x', params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, leverage: 3.0, costBpsRoundTrip: 3 } },
  { name: 'J_1d3m_z2.0_inversion_long_3x', params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, inversionLongOnly: true, leverage: 3.0, costBpsRoundTrip: 3 } },
  // Overnight
  { name: 'K_1d3m_z2.0_inv_long_1x_overnight', params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, inversionLongOnly: true, leverage: 1.0, overnight: true } },
  { name: 'L_1d3m_z2.0_inv_long_3x_overnight', params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, inversionLongOnly: true, leverage: 3.0, overnight: true, costBpsRoundTrip: 3 } },
];

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
      process.stdout.write(`  ${w.name.padEnd(12)} N=${String(s.trade_count).padStart(3)} gross=${s.total_gross_pct.toFixed(2).padStart(7)}% net=${s.total_net_pct.toFixed(2).padStart(7)}% hit=${(s.hit_rate*100).toFixed(1).padStart(5)}% sharpe=${s.sharpe_per_trade.toFixed(2).padStart(6)} dd=${s.max_drawdown_pct.toFixed(2)}% open=${r.openPositions.length}\n`);
      allResults[v.name][w.name] = s;
    }
  }
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'vix-term-structure-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), windows: WINDOWS, variants: VARIANTS, results: allResults });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
