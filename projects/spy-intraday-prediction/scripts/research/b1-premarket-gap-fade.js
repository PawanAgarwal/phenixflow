#!/usr/bin/env node
// B1 — Pre-market gap fade
//
// Signal: SPY open vs prior close gap.
//   - Gap-up:   open / prior_close - 1 >=  +0.5%   → SHORT SPXU (1×) or SPXU (3×)
//   - Gap-down: open / prior_close - 1 <=  -0.5%   → LONG SPXL (3×)
// Entry T 09:35 ET, exit T 15:55 ET.  Intraday only.
//
// Long-only + short-only variants reported separately so we can see whether
// the asymmetry holds.

const path = require('node:path');
const fs = require('node:fs');
const {
  loadDailyBars, loadSpyMinuteBars, stockTradingDaysInRange,
  executeTrade, summarizeTrades, spyBuyHoldBaseline, printResultTable,
  OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../../src/research-utils');

const PARAMS = {
  gapThreshold: 0.005, // 0.5%
  leverage: 3.0,
  costBpsRoundTrip: 3,
};

// Given a SPY signalDay, compute gap = open / prior_close - 1 using daily bars.
function gapOnDay(spyBars, prevDay, day) {
  const a = spyBars.get(prevDay);
  const b = spyBars.get(day);
  if (!a || !b || !Number.isFinite(a.close) || !Number.isFinite(b.open)) return null;
  return b.open / a.close - 1;
}

async function runWindow(spyBars, mode, startDate, endDate) {
  // mode: 'gap-up-short' | 'gap-down-long' | 'both'
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  let fires = 0;
  for (let i = 1; i < days.length; i += 1) {
    const day = days[i];
    if (day < startDate || day > endDate) continue;
    const gap = gapOnDay(spyBars, days[i - 1], day);
    if (!Number.isFinite(gap)) continue;
    let side = null;
    if ((mode === 'gap-up-short' || mode === 'both') && gap >= PARAMS.gapThreshold) side = 'SHORT';
    else if ((mode === 'gap-down-long' || mode === 'both') && gap <= -PARAMS.gapThreshold) side = 'LONG';
    if (!side) continue;
    fires += 1;
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side, leverage: PARAMS.leverage,
      signalDay: day, entryDay: day, exitDay: day,
      entryMinuteEt: 575, exitMinuteEt: 955,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (trade) { trade.gap = gap; trades.push(trade); }
  }
  return { trades, fires };
}

async function main() {
  process.stdout.write('Loading SPY daily bars...\n');
  const t0 = Date.now();
  const spyBars = await loadDailyBars('SPY', '2024-11-01', '2026-05-12');
  process.stdout.write(`Loaded ${spyBars.size} SPY daily bars in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  const variants = {
    'gap-down-long-3x': 'gap-down-long',
    'gap-up-short-3x': 'gap-up-short',
    'both-fade-3x': 'both',
  };
  const results = {};
  for (const [label, mode] of Object.entries(variants)) {
    results[label] = {};
    const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
    for (const w of officialWindows) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(spyBars, mode, w.startDate, w.endDate);
      results[label][w.name] = { ...summarizeTrades(trades), fires };
    }
    for (const w of [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full]) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(spyBars, mode, w.startDate, w.endDate);
      results[label][w.name] = { ...summarizeTrades(trades), fires };
    }
  }

  for (const label of Object.keys(variants)) {
    printResultTable(`B1 — ${label} (gap >= 0.5%, 3× SPXL/SPXU intraday)`, results[label]);
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
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 'b1-premarket-gap-fade.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
