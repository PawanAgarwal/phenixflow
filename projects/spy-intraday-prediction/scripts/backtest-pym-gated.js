#!/usr/bin/env node
const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT } = require('../src/config');
const { writeJson } = require('../src/strategy-runner');
const { loadPymHoldings, backtestPymGated } = require('../src/pym-bias-strategy');

const PYM_ARTIFACT = '/Users/pawanagarwal/github/phenixflow/projects/pym-v5-replication/artifacts/pym-v5-backtest-massive-eod-rsi-wilder-next_close-2025-01-02-2026-05-06.json';

const WINDOWS = [
  { name: 'train_2025', startDate: '2025-01-02', endDate: '2025-12-31' },
  { name: 'test_2026', startDate: '2026-01-02', endDate: '2026-04-27' },
  { name: 'full_16mo', startDate: '2025-01-02', endDate: '2026-04-27' },
];

const VARIANTS = [
  { name: 'A_pure_pym_gate', params: { biasLong: 0.10, biasShort: -0.10, flowFilter: false } },
  { name: 'B_pym_gate_tighter', params: { biasLong: 0.20, biasShort: -0.20, flowFilter: false } },
  { name: 'C_pym_gate_long_only_loose', params: { biasLong: 0.05, biasShort: -10, flowFilter: false } },
  { name: 'D_pym_gate_with_flow_confirm', params: { biasLong: 0.10, biasShort: -0.10, flowFilter: true } },
  { name: 'E_pym_gate_full_session', params: { biasLong: 0.10, biasShort: -0.10, flowFilter: false, entryMinuteEt: 570, exitMinuteEt: 959 } },
  { name: 'F_pym_sign_any', params: { biasLong: 0.0001, biasShort: -0.0001, flowFilter: false } },
  { name: 'G_pym_gate_long_only_010', params: { biasLong: 0.10, biasShort: -10, flowFilter: false } },
  { name: 'H_pym_gate_long_only_020', params: { biasLong: 0.20, biasShort: -10, flowFilter: false } },
  { name: 'I_pym_gate_short_only_010', params: { biasLong: 10, biasShort: -0.10, flowFilter: false } },
  { name: 'J_pym_gate_short_only_020', params: { biasLong: 10, biasShort: -0.20, flowFilter: false } },
  // Position-sized variants
  { name: 'K_bias_proportional_size', params: { biasLong: 0.10, biasShort: -0.10, biasProportionalSize: true, biasSizeMultiplier: 5 } },
  { name: 'L_flow_sizing_modifier', params: { biasLong: 0.20, biasShort: -0.20, flowSizing: true, flowAgreeMultiplier: 1.5 } },
  { name: 'M_low_vix_only', params: { biasLong: 0.10, biasShort: -0.10, maxVixClose: 25 } },
  { name: 'N_long_only_high_conviction', params: { biasLong: 0.15, biasShort: -10, biasProportionalSize: true, biasSizeMultiplier: 4 } },
  // Push the flow-sizing winner further
  { name: 'O_flow_sizing_2x', params: { biasLong: 0.20, biasShort: -0.20, flowSizing: true, flowAgreeMultiplier: 2.0 } },
  { name: 'P_flow_sizing_3x', params: { biasLong: 0.20, biasShort: -0.20, flowSizing: true, flowAgreeMultiplier: 3.0, flowDisagreeMultiplier: 0.5 } },
  { name: 'Q_flow_sizing_tight_bias', params: { biasLong: 0.30, biasShort: -0.30, flowSizing: true, flowAgreeMultiplier: 2.0 } },
  { name: 'R_flow_sizing_loose_bias', params: { biasLong: 0.10, biasShort: -0.10, flowSizing: true, flowAgreeMultiplier: 1.5 } },
  { name: 'S_combined_flow_bias_size', params: { biasLong: 0.10, biasShort: -0.10, flowSizing: true, flowAgreeMultiplier: 1.5, biasProportionalSize: true, biasSizeMultiplier: 4 } },
  // Sample flow LATER (10:30) to avoid noise
  { name: 'T_flow_sizing_1030', params: { biasLong: 0.20, biasShort: -0.20, flowSizing: true, flowAgreeMultiplier: 1.5, flowMinuteEt: 630 } },
  // Entry-time sweep on baseline B
  { name: 'U_entry_1030', params: { biasLong: 0.20, biasShort: -0.20, entryMinuteEt: 630 } },
  { name: 'V_entry_1130', params: { biasLong: 0.20, biasShort: -0.20, entryMinuteEt: 690 } },
  { name: 'W_entry_1230', params: { biasLong: 0.20, biasShort: -0.20, entryMinuteEt: 750 } },
  { name: 'X_entry_1330', params: { biasLong: 0.20, biasShort: -0.20, entryMinuteEt: 810 } },
  { name: 'Y_entry_1430', params: { biasLong: 0.20, biasShort: -0.20, entryMinuteEt: 870 } },
];

async function main() {
  if (!fs.existsSync(PYM_ARTIFACT)) {
    process.stderr.write(`PYM artifact missing: ${PYM_ARTIFACT}\n`);
    process.exit(1);
  }
  process.stdout.write(`Loading PYM holdings from ${PYM_ARTIFACT}\n`);
  const pymByDate = loadPymHoldings(PYM_ARTIFACT);
  process.stdout.write(`Loaded ${pymByDate.size} PYM trading days\n`);
  // bias distribution summary
  const biases = Array.from(pymByDate.values()).map((v) => v.bias).sort((a, b) => a - b);
  const median = biases[Math.floor(biases.length / 2)];
  const min = biases[0];
  const max = biases[biases.length - 1];
  const longCount = biases.filter((b) => b >= 0.10).length;
  const shortCount = biases.filter((b) => b <= -0.10).length;
  const neutralCount = biases.length - longCount - shortCount;
  process.stdout.write(`Bias stats: min=${min.toFixed(2)} median=${median.toFixed(2)} max=${max.toFixed(2)} | long_days(>=0.10)=${longCount} short_days(<=-0.10)=${shortCount} neutral=${neutralCount}\n`);

  const results = {};
  for (const v of VARIANTS) {
    process.stdout.write(`\n=== ${v.name} ===\n`);
    process.stdout.write(`  params: ${JSON.stringify(v.params)}\n`);
    results[v.name] = {};
    for (const w of WINDOWS) {
      // eslint-disable-next-line no-await-in-loop
      const r = await backtestPymGated({
        projectRoot: PROJECT_ROOT,
        root: 'SPY',
        pymByDate,
        startDate: w.startDate,
        endDate: w.endDate,
        params: v.params,
      });
      process.stdout.write(`  ${w.name.padEnd(12)} N=${String(r.trade_count).padStart(4)} net=${r.total_net_pct.toFixed(2).padStart(7)}% hit=${(r.hit_rate*100).toFixed(1).padStart(5)}% sharpe=${r.sharpe_per_trade.toFixed(2).padStart(6)} dd=${r.max_drawdown_pct.toFixed(2)}%\n`);
      results[v.name][w.name] = {
        trade_count: r.trade_count,
        hit_rate: Number(r.hit_rate.toFixed(4)),
        total_gross_pct: Number(r.total_gross_pct.toFixed(2)),
        total_net_pct: Number(r.total_net_pct.toFixed(2)),
        avg_net_bps: Number(r.avg_net_bps.toFixed(2)),
        sharpe_per_trade: Number(r.sharpe_per_trade.toFixed(2)),
        max_drawdown_pct: Number(r.max_drawdown_pct.toFixed(2)),
      };
      // Save trade-level detail for the full window
      if (w.name === 'full_16mo') {
        const tradesPath = path.join(PROJECT_ROOT, 'artifacts', 'pym-gated-trades', `${v.name}.json`);
        writeJson(tradesPath, { variant: v.name, window: w, params: v.params, stats: results[v.name][w.name], trades: r.trades });
      }
    }
  }

  // Save
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'pym-gated-summary.json');
  writeJson(outPath, { generated_at: new Date().toISOString(), pym_artifact: PYM_ARTIFACT, variants: VARIANTS, results });
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
