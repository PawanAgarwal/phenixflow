#!/usr/bin/env node
// S3 — 3-of-3 stack: PYM bias > 0 AND OCC z >= +2 AND VIX1D/VIX3M z >= +2
//
// All three "fear-extreme" signals must fire same day before entering an
// overnight 3× SPXL long.  The thesis: the OCC and VIX signals catch the
// capitulation, the PYM bias requirement filters out down-trend continuations
// (PYM is already bullish for tomorrow, so the dip is buyable).
//
// Entry T 15:55 ET, exit T+1 15:55 ET.

const path = require('node:path');
const fs = require('node:fs');
const {
  loadOccZScoreSeriesWide, loadVixTermZSeriesWide, loadPymBiasByDay,
  stockTradingDaysInRange, executeTrade, summarizeTrades, spyBuyHoldBaseline,
  printResultTable, OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../../src/research-utils');

const PARAMS = {
  occZ: 2.0, vixZ: 2.0, pymBiasMin: 0,
  leverage: 3.0,
  entryMinuteEt: 955, exitMinuteEt: 955,
  costBpsRoundTrip: 3,
};

async function runWindow(occByDay, vixByDay, pymByDay, startDate, endDate, opts = {}) {
  const occGate = opts.occGate ?? PARAMS.occZ;
  const vixGate = opts.vixGate ?? PARAMS.vixZ;
  const biasMin = opts.biasMin ?? PARAMS.pymBiasMin;
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  const debug = { all_3_fire: 0, occ_vix_only: 0, pym_only: 0, occ_only: 0, vix_only: 0 };
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    if (signalDay < startDate || signalDay > endDate) continue;
    const occ = occByDay.get(signalDay);
    const vix = vixByDay.get(signalDay);
    const pym = pymByDay.get(signalDay);
    if (!occ || !vix || !pym) continue;
    const occFires = Number.isFinite(occ.z) && occ.z >= occGate;
    const vixFires = Number.isFinite(vix.z_1d_3m) && vix.z_1d_3m >= vixGate;
    const pymFires = Number.isFinite(pym.bias) && pym.bias > biasMin;
    const all3 = occFires && vixFires && pymFires;
    if (all3) debug.all_3_fire += 1;
    else if (occFires && vixFires) debug.occ_vix_only += 1;
    else if (pymFires) debug.pym_only += 1;
    else if (occFires) debug.occ_only += 1;
    else if (vixFires) debug.vix_only += 1;
    if (!all3) continue;
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: 'LONG', leverage: PARAMS.leverage,
      signalDay, entryDay: signalDay, exitDay: days[i + 1],
      entryMinuteEt: PARAMS.entryMinuteEt, exitMinuteEt: PARAMS.exitMinuteEt,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (!trade) continue;
    trade.occZ = occ.z; trade.vixZ = vix.z_1d_3m; trade.pymBias = pym.bias;
    trades.push(trade);
  }
  return { trades, debug };
}

async function main() {
  process.stdout.write('Loading OCC + VIX + PYM signals...\n');
  const t0 = Date.now();
  const [occByDay, vixByDay, pymByDay] = await Promise.all([
    loadOccZScoreSeriesWide(),
    loadVixTermZSeriesWide(20),
    loadPymBiasByDay(),
  ]);
  process.stdout.write(`Loaded ${occByDay.size} OCC + ${vixByDay.size} VIX + ${pymByDay.size} PYM in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  const allResults = {};
  const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
  for (const w of officialWindows) {
    // eslint-disable-next-line no-await-in-loop
    const { trades, debug } = await runWindow(occByDay, vixByDay, pymByDay, w.startDate, w.endDate);
    allResults[w.name] = { ...summarizeTrades(trades), ...debug };
  }
  for (const w of [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full]) {
    // eslint-disable-next-line no-await-in-loop
    const { trades, debug } = await runWindow(occByDay, vixByDay, pymByDay, w.startDate, w.endDate);
    allResults[w.name] = { ...summarizeTrades(trades), ...debug };
  }

  printResultTable('S3 — 3-of-3 stack (PYM bias>0 + OCC z>=+2 + VIX1D/VIX3M z>=+2, 3× SPXL overnight)', allResults);

  // Show fire counts to gauge data sparsity
  process.stdout.write('\nFire counts per window:\n');
  for (const [name, r] of Object.entries(allResults)) {
    process.stdout.write(`  ${name}: all3=${r.all_3_fire || 0} (occ+vix only=${r.occ_vix_only || 0}, pym only=${r.pym_only || 0})\n`);
  }

  // Compare 3-of-3 vs 2-of-2 (S1) on full window
  const full = SENSITIVITY_WINDOWS.full;
  const { trades: trades3 } = await runWindow(occByDay, vixByDay, pymByDay, full.startDate, full.endDate);
  const stats3 = summarizeTrades(trades3);

  const bh = await spyBuyHoldBaseline(full.startDate, full.endDate);
  process.stdout.write(`\nSPY buy-and-hold baseline (${full.startDate} → ${full.endDate}): ${bh.totalReturnPct.toFixed(2)}%\n`);

  const testStats = allResults['test_2026_sensitivity'];
  let verdict;
  if (stats3.trade_count < 5) verdict = 'reject (too few trades)';
  else if (stats3.total_net_pct > 0 && testStats.total_net_pct > 0 && stats3.sharpe_per_trade > 1.5) verdict = 'strong';
  else if (stats3.total_net_pct > 0 && testStats.total_net_pct > 0) verdict = 'marginal';
  else verdict = 'reject (failed walk-forward)';

  process.stdout.write(`\nVerdict: ${verdict}\n`);

  const out = { generated_at: new Date().toISOString(), params: PARAMS, results: allResults, buy_hold: bh, verdict };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 's3-3of3-stack.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
