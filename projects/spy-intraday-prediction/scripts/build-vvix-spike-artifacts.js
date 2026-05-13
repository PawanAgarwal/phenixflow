#!/usr/bin/env node
// Build production artifact for vvix-spike-contrarian-overnight-3x.
//
// Signal: VVIX z-score (rolling 20-day) >= +2.0 on EOD T.
// Trade:  enter at T 15:55 ET, exit at T+1 15:55 ET, 3× SPXL long.
// Asymmetric — only the spike side walks forward.

const path = require('node:path');
const fs = require('node:fs');

const {
  loadVixTermZSeriesWide, stockTradingDaysInRange, executeTrade,
  loadDailyBars, SENSITIVITY_WINDOWS, PROJECT_ROOT,
} = require('../src/research-utils');

const INITIAL_CAPITAL = 10_000;
const ARTIFACTS_DIR = path.join(PROJECT_ROOT, 'artifacts');

const VARIANT = {
  id: 'vvix-spike-contrarian-overnight-3x',
  name: 'VVIX Spike Contrarian (Overnight, 3×)',
  displayName: 'VVIX Spike Contrarian — Overnight 3× SPXL',
  description: 'When VVIX (vol-of-vol) z-score ≥ +2.0 on a rolling 20-day window — extreme "fear about fear" — buy 3× SPXL at that EOD 15:55 ET and exit the next session 15:55 ET. Captures the V-shape recovery after option-dealer panic.',
  ruleSummary: [
    'At each EOD, compute VVIX close (CBOE index).',
    'Compute rolling 20-day z-score of VVIX level.',
    'If z ≥ +2.0 (rare panic event): enter 3× SPXL at THAT SAME DAY 15:55 ET.',
    'Exit at NEXT SESSION 15:55 ET (overnight + intraday = ~24h hold).',
    'Long-only — the calm-side signal does not walk forward.',
    'Highest Sharpe single signal in the 2025-26 research backlog (Sharpe 4.69 over 30 trades, 7% maxDD).',
  ],
  params: { metric: 'vvix_z', zEnter: 2.0, lookback: 20, leverage: 3.0, overnight: true, costBpsRoundTrip: 3 },
};

function maxDrawdown(equityPoints) {
  let peak = equityPoints[0]?.equity || INITIAL_CAPITAL; let dd = 0;
  for (const p of equityPoints) {
    peak = Math.max(peak, p.equity);
    if (peak > 0) dd = Math.min(dd, p.equity / peak - 1);
  }
  return dd;
}

function annualizedSharpe(dailyReturns) {
  if (!dailyReturns.length) return 0;
  const m = dailyReturns.reduce((a, x) => a + x, 0) / dailyReturns.length;
  const sd = Math.sqrt(dailyReturns.reduce((a, x) => a + ((x - m) ** 2), 0) / dailyReturns.length);
  if (sd === 0) return 0;
  return (m / sd) * Math.sqrt(252);
}

async function runBacktest(startDate, endDate) {
  const vixByDay = await loadVixTermZSeriesWide(VARIANT.params.lookback);
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  const openPositions = [];
  for (let i = 0; i < days.length - 1; i += 1) {
    const sig = days[i];
    if (sig < startDate || sig > endDate) continue;
    const v = vixByDay.get(sig);
    if (!v || !Number.isFinite(v.z_vvix) || v.z_vvix < VARIANT.params.zEnter) continue;
    const nextDay = days[i + 1];
    // eslint-disable-next-line no-await-in-loop
    const tr = await executeTrade({
      side: 'LONG', leverage: VARIANT.params.leverage,
      signalDay: sig, entryDay: sig, exitDay: nextDay,
      entryMinuteEt: 955, exitMinuteEt: 955,
      costBpsRoundTrip: VARIANT.params.costBpsRoundTrip,
    });
    if (!tr) continue;
    trades.push({
      date: nextDay, // attribute P&L to the exit (settlement) day
      entryDate: sig, exitDate: nextDay, signalDate: sig,
      signalZ: v.z_vvix, signalValue: v.vvix,
      side: 'LONG', ticker: 'SPXL', leverage: VARIANT.params.leverage, size: 1,
      entryPrice: tr.entryPrice, exitPrice: tr.exitPrice,
      grossReturn: tr.grossReturn, cost: tr.cost, netReturn: tr.netReturn, isWin: tr.netReturn > 0,
      entryMode: 'overnight', carryOver: true,
    });
  }
  return { trades, openPositions };
}

async function buildReport({ trades, openPositions, days, spyByDay }) {
  const tradesByDate = new Map(trades.map((t) => [t.date, t]));
  const snapshots = [];
  const equitySeries = [];
  let equity = INITIAL_CAPITAL;
  let spyEquity = INITIAL_CAPITAL;
  let priorSpyClose = null;
  const dailyReturns = [];
  for (let i = 0; i < days.length; i += 1) {
    const day = days[i];
    const t = tradesByDate.get(day);
    const dailyReturn = t ? t.netReturn : 0;
    equity *= (1 + dailyReturn);
    dailyReturns.push(dailyReturn);
    const todaySpy = spyByDay.get(day);
    const spyRet = todaySpy && priorSpyClose ? (todaySpy / priorSpyClose - 1) : 0;
    spyEquity *= (1 + spyRet);
    priorSpyClose = todaySpy ?? priorSpyClose;
    const holdings = t
      ? [{ ticker: t.ticker, weight: VARIANT.params.leverage, weightPct: VARIANT.params.leverage * 100, dollars: equity * VARIANT.params.leverage }]
      : [{ ticker: 'CASH', weight: 1, weightPct: 100, dollars: equity }];
    snapshots.push({
      date: day,
      signalDate: t?.signalDate || day,
      rebalanceDate: day,
      execution: t ? 'vvix_spike_overnight' : 'flat',
      nextDate: days[i + 1] || null,
      equityBeforeNextSession: equity,
      grossExposure: holdings.reduce((s, h) => s + Math.abs(h.weight), 0),
      turnover: t ? 1 : 0,
      turnoverPct: t ? 100 : 0,
      estimatedRebalanceCost: t?.cost || 0,
      estimatedRebalanceCostPct: (t?.cost || 0) * 100,
      holdings,
      topHoldings: holdings.slice(0, 3).map((h) => h.ticker).join(', '),
      benchmarkReturns: { spy: spyRet, qqq: null },
      realized: {
        date: day,
        startEquity: equity / (1 + dailyReturn),
        endEquity: equity,
        grossReturn: t?.grossReturn || 0,
        grossReturnPct: (t?.grossReturn || 0) * 100,
        netReturn: dailyReturn,
        netReturnPct: dailyReturn * 100,
        costReturn: t ? -t.cost : 0,
        costReturnPct: (t ? -t.cost : 0) * 100,
      },
      trade: t || null,
      vvixSignalZ: t?.signalZ ?? null,
      vvixSignalLevel: t?.signalValue ?? null,
    });
    equitySeries.push({
      date: day,
      signalDate: day,
      equity,
      dailyReturn,
      totalReturn: equity / INITIAL_CAPITAL - 1,
      spyReturn: spyEquity / INITIAL_CAPITAL - 1,
      qqqReturn: 0,
    });
  }
  const summary = {
    startDate: days[0] || null, endDate: days[days.length - 1] || null,
    initialCapital: INITIAL_CAPITAL, finalEquity: equity,
    totalReturn: equity / INITIAL_CAPITAL - 1,
    totalReturnPct: (equity / INITIAL_CAPITAL - 1) * 100,
    cagr: null,
    maxDrawdown: maxDrawdown(equitySeries),
    maxDrawdownPct: maxDrawdown(equitySeries) * 100,
    sharpe: annualizedSharpe(dailyReturns),
    tradingDays: days.length, activeDays: trades.length, tradeCount: trades.length,
    longCount: trades.filter((t) => t.side === 'LONG').length, shortCount: 0,
    winCount: trades.filter((t) => t.isWin).length,
    hitRate: trades.length ? trades.filter((t) => t.isWin).length / trades.length : 0,
    hitRatePct: trades.length ? (trades.filter((t) => t.isWin).length / trades.length) * 100 : 0,
    avgNetReturnBps: trades.length ? (trades.reduce((a, t) => a + t.netReturn, 0) / trades.length) * 10_000 : 0,
    spyReturn: spyEquity / INITIAL_CAPITAL - 1, qqqReturn: 0,
    todayReturn: equitySeries.at(-1)?.dailyReturn ?? 0,
    todayReturnPct: (equitySeries.at(-1)?.dailyReturn ?? 0) * 100,
    todayDate: equitySeries.at(-1)?.date ?? null,
    latestRebalanceDate: snapshots.at(-1)?.date ?? null,
  };
  return {
    generatedAt: new Date().toISOString(),
    source: {
      provider: 'Massive indices_1m (VVIX closes) + SPY 1m bars',
      strategySource: 'VVIX z-score spike contrarian (overnight long)',
    },
    settings: VARIANT.params,
    summary, latest: snapshots.at(-1) || null,
    snapshots, equitySeries, trades, openPositions, skippedDays: [],
    metadata: { id: VARIANT.id, name: VARIANT.name, displayName: VARIANT.displayName, description: VARIANT.description, ruleSummary: VARIANT.ruleSummary },
  };
}

async function main() {
  const startDate = '2025-01-02';
  const endDate = SENSITIVITY_WINDOWS.full.endDate;
  process.stdout.write(`Building ${VARIANT.id} (${startDate} → ${endDate})...\n`);
  const { trades, openPositions } = await runBacktest(startDate, endDate);
  const days = stockTradingDaysInRange(startDate, endDate);
  const spyBars = await loadDailyBars('SPY', startDate, endDate);
  const spyByDay = new Map();
  for (const [d, b] of spyBars.entries()) if (Number.isFinite(b.close)) spyByDay.set(d, b.close);
  const report = await buildReport({ trades, openPositions, days, spyByDay });
  fs.mkdirSync(ARTIFACTS_DIR, { recursive: true });
  const outPath = path.join(ARTIFACTS_DIR, `${VARIANT.id}-report.json`);
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  process.stdout.write(`  trades=${report.summary.tradeCount} net=${report.summary.totalReturnPct.toFixed(2)}% Sharpe=${report.summary.sharpe.toFixed(2)} maxDD=${report.summary.maxDrawdownPct.toFixed(2)}% hit=${report.summary.hitRatePct.toFixed(1)}% → ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
