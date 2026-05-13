#!/usr/bin/env node
// A1 — HYG/LQD credit-stress contrarian
//
// Hypothesis: when credit spreads widen sharply (HYG sells off relative to
// LQD), equity follows but eventually reverts.  Compute daily log return of
// (HYG/LQD) ratio, take a rolling 20-day z-score; when z <= -2 (HYG weak vs
// LQD = credit stress), go LONG SPY/SPXL overnight.
//
// Asymmetric: only the stress side (positive z = credit easy = doesn't walk
// forward as a long signal).

const path = require('node:path');
const fs = require('node:fs');
const {
  loadDailyBars, stockTradingDaysInRange, executeTrade, summarizeTrades,
  spyBuyHoldBaseline, printResultTable, rollingZ,
  OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../../src/research-utils');

const PARAMS = {
  hygLqdZ: -2.0,
  costBpsRoundTrip3x: 3, costBpsRoundTrip1x: 2,
  entryMinuteEt: 955, exitMinuteEt: 955,
};

async function buildCreditZ(startDate, endDate) {
  const hyg = await loadDailyBars('HYG', startDate, endDate);
  const lqd = await loadDailyBars('LQD', startDate, endDate);
  const days = stockTradingDaysInRange(startDate, endDate);
  const ratios = [];
  const validDays = [];
  for (const d of days) {
    const h = hyg.get(d); const l = lqd.get(d);
    if (!h || !l || !Number.isFinite(h.close) || !Number.isFinite(l.close) || l.close === 0) continue;
    ratios.push(Math.log(h.close / l.close));
    validDays.push(d);
  }
  // 1-day log return of HYG/LQD ratio
  const dailyLogReturn = new Array(ratios.length).fill(null);
  for (let i = 1; i < ratios.length; i += 1) dailyLogReturn[i] = ratios[i] - ratios[i - 1];
  const z = rollingZ(dailyLogReturn, 20);
  const out = new Map();
  for (let i = 0; i < validDays.length; i += 1) {
    if (Number.isFinite(z[i])) out.set(validDays[i], { ratio: ratios[i], dailyLog: dailyLogReturn[i], z: z[i] });
  }
  return out;
}

async function runWindow(creditZByDay, leverage, startDate, endDate) {
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  let fires = 0;
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    if (signalDay < startDate || signalDay > endDate) continue;
    const c = creditZByDay.get(signalDay);
    if (!c || !Number.isFinite(c.z) || c.z > PARAMS.hygLqdZ) continue;
    fires += 1;
    const costBps = leverage > 1 ? PARAMS.costBpsRoundTrip3x : PARAMS.costBpsRoundTrip1x;
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: 'LONG', leverage,
      signalDay, entryDay: signalDay, exitDay: days[i + 1],
      entryMinuteEt: PARAMS.entryMinuteEt, exitMinuteEt: PARAMS.exitMinuteEt,
      costBpsRoundTrip: costBps,
    });
    if (trade) { trade.creditZ = c.z; trades.push(trade); }
  }
  return { trades, fires };
}

async function main() {
  process.stdout.write('Building HYG/LQD credit-stress z series...\n');
  const t0 = Date.now();
  const creditZByDay = await buildCreditZ('2024-09-01', '2026-05-12');
  process.stdout.write(`Built ${creditZByDay.size} credit-stress days in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  const variants = { 'lev-1x': 1.0, 'lev-3x': 3.0 };
  const results = {};
  for (const [label, lev] of Object.entries(variants)) {
    results[label] = {};
    const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
    for (const w of officialWindows) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(creditZByDay, lev, w.startDate, w.endDate);
      results[label][w.name] = { ...summarizeTrades(trades), fires };
    }
    for (const w of [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full]) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(creditZByDay, lev, w.startDate, w.endDate);
      results[label][w.name] = { ...summarizeTrades(trades), fires };
    }
    printResultTable(`A1-${label} — HYG/LQD log-return z <= -2 → overnight long SPY (${lev}×)`, results[label]);
  }

  const full = SENSITIVITY_WINDOWS.full;
  const bh = await spyBuyHoldBaseline(full.startDate, full.endDate);
  process.stdout.write(`\nSPY buy-and-hold baseline: ${bh.totalReturnPct.toFixed(2)}%\n`);

  function verdict(stats) {
    const fullS = stats['full_16mo_sensitivity'];
    const testS = stats['test_2026_sensitivity'];
    if (fullS.trade_count < 5) return 'reject (too few trades)';
    if (fullS.total_net_pct > 0 && testS.total_net_pct > 0 && fullS.sharpe_per_trade > 1.5) return 'strong';
    if (fullS.total_net_pct > 0 && testS.total_net_pct > 0) return 'marginal';
    return 'reject (failed walk-forward)';
  }

  const verdicts = {};
  for (const label of Object.keys(variants)) verdicts[label] = verdict(results[label]);
  process.stdout.write(`\nVerdicts:\n`);
  for (const [k, v] of Object.entries(verdicts)) process.stdout.write(`  ${k}: ${v}\n`);

  const out = { generated_at: new Date().toISOString(), params: PARAMS, results, buy_hold: bh, verdicts };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 'a1-credit-stress-contrarian.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
