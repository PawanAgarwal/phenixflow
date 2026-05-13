#!/usr/bin/env node
// S1 — 2-of-2 fear-extreme confirmation
//
// Trade on days where BOTH the OCC put/call z-score AND the VIX1D/VIX3M term-
// structure z-score signal "extreme fear" (z >= +2) on the same session T.
// Entry at T 15:55 ET (prior close), exit at T+1 15:55 ET (overnight + intraday
// = ~24h hold).  Long-only (asymmetric-fear lesson from items 1 & 2).  3× via
// SPXL.

const path = require('node:path');
const fs = require('node:fs');
const {
  loadOccZScoreSeriesWide, loadVixTermZSeriesWide, stockTradingDaysInRange,
  executeTrade, summarizeTrades, spyBuyHoldBaseline, printResultTable,
  OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../../src/research-utils');

const PARAMS = {
  occZ: 2.0,
  vixZ: 2.0,
  leverage: 3.0,
  entryMinuteEt: 955, // 15:55 ET prior close
  exitMinuteEt: 955,
  costBpsRoundTrip: 3,
};

async function runWindow(occByDay, vixByDay, startDate, endDate, { occGate = 2.0, vixGate = 2.0 } = {}) {
  // Need a buffer in front of startDate so rolling z-scores stabilize.
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);

  const trades = [];
  const debug = { both_fire: 0, occ_only: 0, vix_only: 0 };
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    if (signalDay < startDate || signalDay > endDate) continue;
    const occ = occByDay.get(signalDay);
    const vix = vixByDay.get(signalDay);
    if (!occ || !vix) continue;
    const occFires = Number.isFinite(occ.z) && occ.z >= occGate;
    const vixFires = Number.isFinite(vix.z_1d_3m) && vix.z_1d_3m >= vixGate;
    if (occFires && vixFires) debug.both_fire += 1;
    else if (occFires) debug.occ_only += 1;
    else if (vixFires) debug.vix_only += 1;
    if (!(occFires && vixFires)) continue;
    const exitDay = days[i + 1];
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: 'LONG',
      leverage: PARAMS.leverage,
      signalDay,
      entryDay: signalDay,
      exitDay,
      entryMinuteEt: PARAMS.entryMinuteEt,
      exitMinuteEt: PARAMS.exitMinuteEt,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (!trade) continue;
    trade.occZ = occ.z;
    trade.vixZ = vix.z_1d_3m;
    trades.push(trade);
  }
  return { trades, debug };
}

async function runOccOnly(occByDay, startDate, endDate, { occGate = 2.0 } = {}) {
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    if (signalDay < startDate || signalDay > endDate) continue;
    const occ = occByDay.get(signalDay);
    if (!occ || !Number.isFinite(occ.z) || occ.z < occGate) continue;
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: 'LONG', leverage: PARAMS.leverage,
      signalDay, entryDay: signalDay, exitDay: days[i + 1],
      entryMinuteEt: PARAMS.entryMinuteEt, exitMinuteEt: PARAMS.exitMinuteEt,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (trade) trades.push(trade);
  }
  return trades;
}

async function runVixOnly(vixByDay, startDate, endDate, { vixGate = 2.0 } = {}) {
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    if (signalDay < startDate || signalDay > endDate) continue;
    const vix = vixByDay.get(signalDay);
    if (!vix || !Number.isFinite(vix.z_1d_3m) || vix.z_1d_3m < vixGate) continue;
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: 'LONG', leverage: PARAMS.leverage,
      signalDay, entryDay: signalDay, exitDay: days[i + 1],
      entryMinuteEt: PARAMS.entryMinuteEt, exitMinuteEt: PARAMS.exitMinuteEt,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (trade) trades.push(trade);
  }
  return trades;
}

async function main() {
  process.stdout.write('Loading OCC + VIX signals once for widest range...\n');
  const t0 = Date.now();
  const occByDay = await loadOccZScoreSeriesWide();
  const vixByDay = await loadVixTermZSeriesWide(20);
  process.stdout.write(`Loaded ${occByDay.size} OCC + ${vixByDay.size} VIX day records in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  const allResults = {};
  // ---- Official protocol (Jan 2026 train, Feb-Apr 2026 test) ----
  const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
  for (const w of officialWindows) {
    // eslint-disable-next-line no-await-in-loop
    const { trades, debug } = await runWindow(occByDay, vixByDay, w.startDate, w.endDate);
    const stats = summarizeTrades(trades);
    allResults[w.name] = { ...stats, fireCount: debug.both_fire, occOnlyCount: debug.occ_only, vixOnlyCount: debug.vix_only };
  }
  // ---- Sensitivity (full 2025 train, 2026 test) ----
  const sensitivityWindows = [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full];
  for (const w of sensitivityWindows) {
    // eslint-disable-next-line no-await-in-loop
    const { trades, debug } = await runWindow(occByDay, vixByDay, w.startDate, w.endDate);
    const stats = summarizeTrades(trades);
    allResults[w.name] = { ...stats, fireCount: debug.both_fire, occOnlyCount: debug.occ_only, vixOnlyCount: debug.vix_only };
  }

  printResultTable('S1 — 2-of-2 fear confirmation (OCC z>=+2 AND VIX1D/VIX3M z>=+2, long 3× SPXL, overnight)', allResults);

  // ---- Compare to each single-signal version on the full window ----
  process.stdout.write('\nSingle-signal comparison (full 16-mo sensitivity window):\n');
  const fullStart = SENSITIVITY_WINDOWS.full.startDate;
  const fullEnd = SENSITIVITY_WINDOWS.full.endDate;
  const occTrades = await runOccOnly(occByDay, fullStart, fullEnd);
  const vixTrades = await runVixOnly(vixByDay, fullStart, fullEnd);
  const occStats = summarizeTrades(occTrades);
  const vixStats = summarizeTrades(vixTrades);
  printResultTable('  full 16-mo, alternative gates', {
    'S1 2-of-2': allResults['full_16mo_sensitivity'],
    'OCC z>=2 only': occStats,
    'VIX1D/VIX3M z>=2 only': vixStats,
  });

  // ---- SPY buy-hold baseline ----
  const bh = await spyBuyHoldBaseline(fullStart, fullEnd);
  process.stdout.write(`\nSPY buy-and-hold baseline (${fullStart} → ${fullEnd}): ${bh.totalReturnPct.toFixed(2)}%\n`);

  // ---- Verdict ----
  const fullStats = allResults['full_16mo_sensitivity'];
  const testStats = allResults['test_2026_sensitivity'];
  let verdict;
  if (fullStats.trade_count < 5) verdict = 'reject (too few trades to evaluate)';
  else if (fullStats.total_net_pct > 0 && testStats.total_net_pct > 0 && fullStats.sharpe_per_trade > 1.5) verdict = 'strong';
  else if (fullStats.total_net_pct > 0 && testStats.total_net_pct > 0) verdict = 'marginal';
  else verdict = 'reject (failed walk-forward)';
  process.stdout.write(`\nVerdict: ${verdict}\n`);

  const out = { generated_at: new Date().toISOString(), params: PARAMS, results: allResults, single_signal_comparison: { 'OCC_z2_only': occStats, 'VIX1D/VIX3M_z2_only': vixStats }, buy_hold: bh, verdict };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 's1-fear-2of2-confirmation.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
