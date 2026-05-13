#!/usr/bin/env node
// A2 — Multi-day SPY drawdown buy-the-dip
//
// Signal:  5-day SPY drawdown <= -3% AND VIX1D/VIX3M z >= +1 on the same day.
// (Looser VIX gate than S1 to get more trades; the multi-day DD is the
// dominant confirmation.)
//
// Two hold variants:
//   - intraday-only: enter T+1 09:35, exit T+1 15:55 (no overnight)
//   - overnight:     enter T 15:55, exit T+1 15:55 (~24h)
// Long 3× SPXL.

const path = require('node:path');
const fs = require('node:fs');
const {
  loadVixTermZSeriesWide, loadDailyBars, stockTradingDaysInRange,
  executeTrade, summarizeTrades, spyBuyHoldBaseline, printResultTable,
  OFFICIAL_WINDOWS, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../../src/research-utils');

const PARAMS = {
  ddLookback: 5,
  ddThreshold: -0.03,
  vixZ: 1.0,
  leverage: 3.0,
  costBpsRoundTrip: 3,
};

function rollingDrawdown(spyClosesByDate, dates, signalDay, lookback) {
  // Drawdown = (close[signalDay] / max(close over last `lookback` days incl signalDay) - 1).
  const i = dates.indexOf(signalDay);
  if (i < lookback) return null;
  let maxClose = -Infinity;
  for (let k = i - lookback; k <= i; k += 1) {
    const c = spyClosesByDate.get(dates[k]);
    if (Number.isFinite(c) && c > maxClose) maxClose = c;
  }
  const signalClose = spyClosesByDate.get(signalDay);
  if (!Number.isFinite(signalClose) || maxClose === -Infinity) return null;
  return signalClose / maxClose - 1;
}

async function runWindow(spyClosesByDate, vixByDay, holdMode, startDate, endDate) {
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  let fires = 0;
  for (let i = 0; i < days.length - 1; i += 1) {
    const signalDay = days[i];
    if (signalDay < startDate || signalDay > endDate) continue;
    const dd = rollingDrawdown(spyClosesByDate, days, signalDay, PARAMS.ddLookback);
    if (!Number.isFinite(dd) || dd > PARAMS.ddThreshold) continue;
    const vix = vixByDay.get(signalDay);
    if (!vix || !Number.isFinite(vix.z_1d_3m) || vix.z_1d_3m < PARAMS.vixZ) continue;
    fires += 1;
    const nextDay = days[i + 1];
    let entryDay; let entryMinuteEt; let exitDay; let exitMinuteEt;
    if (holdMode === 'intraday') {
      // Enter T+1 09:35 → exit T+1 15:55 (no overnight)
      entryDay = nextDay; entryMinuteEt = 575;
      exitDay = nextDay; exitMinuteEt = 955;
    } else {
      // Overnight: enter T 15:55 → exit T+1 15:55
      entryDay = signalDay; entryMinuteEt = 955;
      exitDay = nextDay; exitMinuteEt = 955;
    }
    // eslint-disable-next-line no-await-in-loop
    const trade = await executeTrade({
      side: 'LONG', leverage: PARAMS.leverage,
      signalDay, entryDay, exitDay, entryMinuteEt, exitMinuteEt,
      costBpsRoundTrip: PARAMS.costBpsRoundTrip,
    });
    if (trade) { trade.dd5d = dd; trade.vixZ = vix.z_1d_3m; trade.holdMode = holdMode; trades.push(trade); }
  }
  return { trades, fires };
}

async function main() {
  process.stdout.write('Loading SPY daily bars + VIX series...\n');
  const t0 = Date.now();
  const spyBars = await loadDailyBars('SPY', '2024-11-01', '2026-05-12');
  const vixByDay = await loadVixTermZSeriesWide(20);
  const spyClosesByDate = new Map();
  for (const [d, b] of spyBars.entries()) if (Number.isFinite(b.close)) spyClosesByDate.set(d, b.close);
  process.stdout.write(`Loaded ${spyClosesByDate.size} SPY closes + ${vixByDay.size} VIX days in ${((Date.now()-t0)/1000).toFixed(1)}s\n`);

  const results = { intraday: {}, overnight: {} };
  const officialWindows = [OFFICIAL_WINDOWS.train, ...OFFICIAL_WINDOWS.tests];
  for (const holdMode of ['intraday', 'overnight']) {
    for (const w of officialWindows) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(spyClosesByDate, vixByDay, holdMode, w.startDate, w.endDate);
      results[holdMode][w.name] = { ...summarizeTrades(trades), fires };
    }
    for (const w of [SENSITIVITY_WINDOWS.train, SENSITIVITY_WINDOWS.test, SENSITIVITY_WINDOWS.full]) {
      // eslint-disable-next-line no-await-in-loop
      const { trades, fires } = await runWindow(spyClosesByDate, vixByDay, holdMode, w.startDate, w.endDate);
      results[holdMode][w.name] = { ...summarizeTrades(trades), fires };
    }
  }

  printResultTable('A2-intraday — 5d DD<=-3% + VIX1D/VIX3M z>=+1 → T+1 intraday 3× SPXL', results.intraday);
  printResultTable('A2-overnight — 5d DD<=-3% + VIX1D/VIX3M z>=+1 → overnight 3× SPXL', results.overnight);

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

  const vIntra = verdict(results.intraday);
  const vOver = verdict(results.overnight);
  process.stdout.write(`\nVerdicts: intraday=${vIntra}, overnight=${vOver}\n`);

  const out = {
    generated_at: new Date().toISOString(), params: PARAMS,
    results, buy_hold: bh,
    verdicts: { intraday: vIntra, overnight: vOver },
  };
  const outPath = path.join(PROJECT_ROOT, 'artifacts', 'research', 'a2-multiday-drawdown-buythedip.json');
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(out, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
