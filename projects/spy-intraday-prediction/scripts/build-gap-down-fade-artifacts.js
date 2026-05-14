#!/usr/bin/env node
// Build production artifact for gap-down-fade-intraday-3x.
//
// Signal: SPY open / prior_close - 1 <= -0.5% (gap down).
// Trade:  enter 09:35 ET → exit 15:55 ET, 3× SPXL long. Intraday only.
// Long-only (the gap-up-short variant failed walk-forward).

const path = require('node:path');
const fs = require('node:fs');

const { loadConfig, resolveEndDate } = require('../src/config');
const {
  loadDailyBars, stockTradingDaysInRange, executeTrade,
  PROJECT_ROOT,
} = require('../src/research-utils');

const INITIAL_CAPITAL = 10_000;
const ARTIFACTS_DIR = path.join(PROJECT_ROOT, 'artifacts');

const VARIANT = {
  id: 'gap-down-fade-intraday-3x',
  name: 'SPY Gap-Down Fade (Intraday, 3×)',
  displayName: 'SPY Gap-Down Fade — Intraday 3× SPXL',
  description: 'Classic intraday mean-reversion. When SPY opens ≥ 0.5% below the prior close (gap down), buy 3× SPXL at 09:35 ET and exit at 15:55 ET. Long-only — the symmetric gap-up-short leg failed walk-forward.',
  ruleSummary: [
    'At 09:30 ET compute gap = SPY.open / SPY.prior_close - 1.',
    'If gap ≤ -0.5%: buy 3× SPXL at 09:35 ET.',
    'Exit at 15:55 ET (intraday only, no overnight).',
    'Long-only — gap-up-short leg lost on 2026 test, excluded.',
    'Backtested: 46 trades, +42% net, Sharpe 2.67, hit 54%, maxDD 15.4% (Jan 2025 → May 2026).',
  ],
  params: { gapThresholdPct: -0.005, leverage: 3.0, entryMinuteEt: 575, exitMinuteEt: 955, costBpsRoundTrip: 3 },
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

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--start') out.startDate = argv[++i];
    else if (arg === '--end') out.endDate = argv[++i];
  }
  return out;
}

async function runBacktest(startDate, endDate) {
  const spyBars = await loadDailyBars('SPY', '2024-11-01', endDate);
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const trades = [];
  for (let i = 1; i < days.length; i += 1) {
    const d = days[i];
    if (d < startDate || d > endDate) continue;
    const a = spyBars.get(days[i - 1]); const b = spyBars.get(d);
    if (!a || !b || !Number.isFinite(a.close) || !Number.isFinite(b.open)) continue;
    const gap = b.open / a.close - 1;
    if (!Number.isFinite(gap) || gap > VARIANT.params.gapThresholdPct) continue;
    // eslint-disable-next-line no-await-in-loop
    const tr = await executeTrade({
      side: 'LONG', leverage: VARIANT.params.leverage,
      signalDay: d, entryDay: d, exitDay: d,
      entryMinuteEt: VARIANT.params.entryMinuteEt,
      exitMinuteEt: VARIANT.params.exitMinuteEt,
      costBpsRoundTrip: VARIANT.params.costBpsRoundTrip,
    });
    if (!tr) continue;
    trades.push({
      date: d, entryDate: d, exitDate: d, signalDate: d,
      gap, gapPct: gap * 100, side: 'LONG', ticker: 'SPXL',
      leverage: VARIANT.params.leverage, size: 1,
      entryPrice: tr.entryPrice, exitPrice: tr.exitPrice,
      grossReturn: tr.grossReturn, cost: tr.cost, netReturn: tr.netReturn, isWin: tr.netReturn > 0,
      entryMode: 'intraday', carryOver: false,
    });
  }
  return { trades, openPositions: [] };
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
      date: day, signalDate: t?.signalDate || day, rebalanceDate: day,
      execution: t ? 'gap_down_fade_intraday' : 'flat',
      nextDate: days[i + 1] || null, equityBeforeNextSession: equity,
      grossExposure: holdings.reduce((s, h) => s + Math.abs(h.weight), 0),
      turnover: t ? 1 : 0, turnoverPct: t ? 100 : 0,
      estimatedRebalanceCost: t?.cost || 0,
      estimatedRebalanceCostPct: (t?.cost || 0) * 100,
      holdings, topHoldings: holdings.slice(0, 3).map((h) => h.ticker).join(', '),
      benchmarkReturns: { spy: spyRet, qqq: null },
      realized: {
        date: day,
        startEquity: equity / (1 + dailyReturn), endEquity: equity,
        grossReturn: t?.grossReturn || 0, grossReturnPct: (t?.grossReturn || 0) * 100,
        netReturn: dailyReturn, netReturnPct: dailyReturn * 100,
        costReturn: t ? -t.cost : 0, costReturnPct: (t ? -t.cost : 0) * 100,
      },
      trade: t || null, gapSignal: t?.gap ?? null, gapSignalPct: t?.gapPct ?? null,
    });
    equitySeries.push({
      date: day, signalDate: day, equity, dailyReturn,
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
      provider: 'Massive EOD adjusted daily bars (SPY open/close) + SPY 1m bars',
      strategySource: 'Pre-market gap fade — long-only intraday mean reversion',
    },
    settings: VARIANT.params,
    summary, latest: snapshots.at(-1) || null,
    snapshots, equitySeries, trades, openPositions, skippedDays: [],
    metadata: { id: VARIANT.id, name: VARIANT.name, displayName: VARIANT.displayName, description: VARIANT.description, ruleSummary: VARIANT.ruleSummary },
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const endDate = resolveEndDate(config, args.endDate || 'auto');
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
