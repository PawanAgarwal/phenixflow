#!/usr/bin/env node
// A3 — VVIX spike contrarian
//
// Hypothesis: VVIX measures "vol of vol" (implied vol on VIX options).  VVIX
// spikes mark "fear about fear" / option dealer panic — historically these
// coincide with short-term S&P lows.  Trade overnight 3× SPXL long when VVIX
// z-score >= +2 on a 20-day rolling window.  Asymmetric: only the spike side
// (greed side does not walk forward).
//
// Includes correlation check vs existing VIX1D/VIX3M signal to gauge whether
// VVIX adds anything new.

const path = require('node:path');
const fs = require('node:fs');
const {
  loadVixTermZSeriesWide, stockTradingDaysInRange, executeTrade,
  summarizeTrades, spyBuyHoldBaseline, printResultTable,
  OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../../src/research-utils');

const PARAMS = {
  vvixZ: 2.0, leverage: 3.0,
  entryMinuteEt: 955, exitMinuteEt: 955, costBpsRoundTrip: 3,
};

async function runWindow(vixByDay, startDate, endDate, { vvixGate = 2.0 } = {}) {
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  let fires = 0;
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    if (signalDay < startDate || signalDay > endDate) continue;
    const v = vixByDay.get(signalDay);
    if (!v || !Number.isFinite(v.z_vvix) || v.z_vvix < vvixGate) continue;
    fires += 1;
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: 'LONG', leverage: PARAMS.leverage,
      signalDay, entryDay: signalDay, exitDay: days[i + 1],
      entryMinuteEt: PARAMS.entryMinuteEt, exitMinuteEt: PARAMS.exitMinuteEt,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (trade) { trade.vvixZ = v.z_vvix; trade.vix1d3mZ = v.z_1d_3m; trades.push(trade); }
  }
  return { trades, fires };
}

// Pairwise correlation of (VVIX z, VIX1D/VIX3M z) across all days in window.
function correlateVvixVixTerm(vixByDay, startDate, endDate) {
  const xs = []; const ys = [];
  for (const [day, v] of vixByDay.entries()) {
    if (day < startDate || day > endDate) continue;
    if (!Number.isFinite(v.z_vvix) || !Number.isFinite(v.z_1d_3m)) continue;
    xs.push(v.z_vvix); ys.push(v.z_1d_3m);
  }
  if (xs.length < 5) return null;
  const mx = xs.reduce((a, b) => a + b, 0) / xs.length;
  const my = ys.reduce((a, b) => a + b, 0) / ys.length;
  let num = 0; let dx = 0; let dy = 0;
  for (let i = 0; i < xs.length; i += 1) {
    num += (xs[i] - mx) * (ys[i] - my);
    dx += (xs[i] - mx) ** 2; dy += (ys[i] - my) ** 2;
  }
  if (dx === 0 || dy === 0) return null;
  return { corr: num / Math.sqrt(dx * dy), n: xs.length };
}

// How often do VVIX-fire days overlap with VIX1D/VIX3M-fire days?
function overlapCounts(vixByDay, startDate, endDate, vvixGate, termGate) {
  let vvixOnly = 0; let termOnly = 0; let both = 0;
  for (const [day, v] of vixByDay.entries()) {
    if (day < startDate || day > endDate) continue;
    const vvixFires = Number.isFinite(v.z_vvix) && v.z_vvix >= vvixGate;
    const termFires = Number.isFinite(v.z_1d_3m) && v.z_1d_3m >= termGate;
    if (vvixFires && termFires) both += 1;
    else if (vvixFires) vvixOnly += 1;
    else if (termFires) termOnly += 1;
  }
  return { vvixOnly, termOnly, both };
}

async function main() {
  process.stdout.write('Loading VIX series (incl. VVIX)...\n');
  const t0 = Date.now();
  const vixByDay = await loadVixTermZSeriesWide(20);
  process.stdout.write(`Loaded ${vixByDay.size} VIX day records in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  const allResults = {};
  const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
  for (const w of officialWindows) {
    // eslint-disable-next-line no-await-in-loop
    const { trades, fires } = await runWindow(vixByDay, w.startDate, w.endDate);
    allResults[w.name] = { ...summarizeTrades(trades), fires };
  }
  for (const w of [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full]) {
    // eslint-disable-next-line no-await-in-loop
    const { trades, fires } = await runWindow(vixByDay, w.startDate, w.endDate);
    allResults[w.name] = { ...summarizeTrades(trades), fires };
  }

  printResultTable('A3 — VVIX z>=+2 contrarian (long 3× SPXL overnight)', allResults);

  // Correlation + overlap diagnostics on full window
  const full = SENSITIVITY_WINDOWS.full;
  const corr = correlateVvixVixTerm(vixByDay, full.startDate, full.endDate);
  const overlap = overlapCounts(vixByDay, full.startDate, full.endDate, 2.0, 2.0);
  process.stdout.write(`\nVVIX z vs VIX1D/VIX3M z correlation (full window): ${corr ? corr.corr.toFixed(3) : 'n/a'} (n=${corr ? corr.n : 0})\n`);
  process.stdout.write(`Fire-day overlap (z>=2 gates): vvix-only=${overlap.vvixOnly}, term-only=${overlap.termOnly}, both=${overlap.both}\n`);

  const bh = await spyBuyHoldBaseline(full.startDate, full.endDate);
  process.stdout.write(`\nSPY buy-and-hold baseline: ${bh.totalReturnPct.toFixed(2)}%\n`);

  const fullStats = allResults['full_16mo_sensitivity'];
  const testStats = allResults['test_2026_sensitivity'];
  let verdict;
  if (fullStats.trade_count < 5) verdict = 'reject (too few trades)';
  else if (corr && Math.abs(corr.corr) > 0.85) verdict = 'reject (redundant — too correlated with existing VIX1D/VIX3M signal)';
  else if (fullStats.total_net_pct > 0 && testStats.total_net_pct > 0 && fullStats.sharpe_per_trade > 1.5) verdict = 'strong';
  else if (fullStats.total_net_pct > 0 && testStats.total_net_pct > 0) verdict = 'marginal';
  else verdict = 'reject (failed walk-forward)';

  process.stdout.write(`\nVerdict: ${verdict}\n`);

  const out = {
    generated_at: new Date().toISOString(),
    params: PARAMS,
    results: allResults,
    vvix_vs_vix_term_correlation_full: corr,
    fire_overlap_full: overlap,
    buy_hold: bh,
    verdict,
  };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 'a3-vvix-spike-contrarian.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
