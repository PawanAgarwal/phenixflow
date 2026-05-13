#!/usr/bin/env node
// B2 — First-hour range break
//
// During the first 30/60 minutes, record SPY's high/low.  If price breaks
// above the first-hour high after the first hour, go LONG.  If it breaks
// below the first-hour low, go SHORT.  Exit at 15:55 ET.  3× SPXL/SPXU.
//
// Two sensitivity windows: 30-min range break, 60-min range break.

const path = require('node:path');
const fs = require('node:fs');
const {
  loadSpyMinuteBars, prefetchSpyMinuteBarsBulk, stockTradingDaysInRange,
  executeTrade, summarizeTrades, spyBuyHoldBaseline, printResultTable,
  OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../../src/research-utils');

const PARAMS = {
  leverage: 3.0,
  costBpsRoundTrip: 3,
  // 9:30 ET = 570 min-of-day.  First 60 min = ends at minute 630 (10:30 ET).
  // First 30 min = ends at minute 600 (10:00 ET).
  rangeEndsByMode: { '30m': 600, '60m': 630 },
  exitMinuteEt: 955, // 15:55 ET
};

function findBreak(bars, rangeEnd, exitMinute) {
  // Compute first-rangeEnd high/low; then find first minute > rangeEnd where
  // close > high (long break) or close < low (short break).
  let high = -Infinity; let low = Infinity;
  for (const b of bars) {
    if (b.minute_of_day_et > rangeEnd) break;
    if (Number.isFinite(b.high) && b.high > high) high = b.high;
    if (Number.isFinite(b.low) && b.low < low) low = b.low;
  }
  if (high === -Infinity || low === Infinity) return null;
  for (const b of bars) {
    if (b.minute_of_day_et <= rangeEnd) continue;
    if (b.minute_of_day_et >= exitMinute) break;
    if (Number.isFinite(b.close)) {
      if (b.close > high) return { side: 'LONG', triggerMinute: b.minute_of_day_et, triggerPrice: b.close, high, low };
      if (b.close < low) return { side: 'SHORT', triggerMinute: b.minute_of_day_et, triggerPrice: b.close, high, low };
    }
  }
  return null;
}

async function runWindow(mode, startDate, endDate) {
  const rangeEnd = PARAMS.rangeEndsByMode[mode];
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  let fires = 0;
  for (const day of days) {
    if (day < startDate || day > endDate) continue;
    // eslint-disable-next-line no-await-in-loop
    const bars = await loadSpyMinuteBars(day);
    if (!bars) continue;
    const br = findBreak(bars, rangeEnd, PARAMS.exitMinuteEt);
    if (!br) continue;
    fires += 1;
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: br.side, leverage: PARAMS.leverage,
      signalDay: day, entryDay: day, exitDay: day,
      entryMinuteEt: br.triggerMinute, exitMinuteEt: PARAMS.exitMinuteEt,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (trade) { trade.mode = mode; trade.rangeHigh = br.high; trade.rangeLow = br.low; trades.push(trade); }
  }
  return { trades, fires };
}

async function main() {
  process.stdout.write('Bulk-prefetching SPY minute bars...\n');
  const t0 = Date.now();
  await prefetchSpyMinuteBarsBulk();
  process.stdout.write(`SPY 1m prefetch done in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);
  const results = { '30m': {}, '60m': {} };
  const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
  for (const mode of ['30m', '60m']) {
    process.stdout.write(`\nRunning B2 (${mode} range)...\n`);
    for (const w of officialWindows) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(mode, w.startDate, w.endDate);
      results[mode][w.name] = { ...summarizeTrades(trades), fires };
    }
    for (const w of [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full]) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(mode, w.startDate, w.endDate);
      results[mode][w.name] = { ...summarizeTrades(trades), fires };
    }
    printResultTable(`B2 — first ${mode} range break (3× SPXL/SPXU intraday)`, results[mode]);
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

  const verdicts = { '30m': verdict(results['30m']), '60m': verdict(results['60m']) };
  process.stdout.write(`\nVerdicts: 30m=${verdicts['30m']}, 60m=${verdicts['60m']}\n`);

  const out = { generated_at: new Date().toISOString(), params: PARAMS, results, buy_hold: bh, verdicts };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 'b2-first-hour-range-break.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
