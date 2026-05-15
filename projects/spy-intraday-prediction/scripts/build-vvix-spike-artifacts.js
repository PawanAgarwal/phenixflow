#!/usr/bin/env node
// Build production artifact for vvix-spike-contrarian-overnight-3x.
//
// Signal: VVIX z-score (rolling 20-day) >= +2.0 on EOD T.
// Trade:  enter at T 15:55 ET, exit at T+1 15:55 ET, 3× SPXL long.
// Asymmetric — only the spike side walks forward.

const path = require('node:path');
const fs = require('node:fs');

const { loadConfig, resolveEndDate } = require('../src/config');
const {
  loadVixTermZSeriesWide, stockTradingDaysInRange, executeTrade,
  loadVixTermZSeries, loadDailyBars, PROJECT_ROOT,
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

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--start') out.startDate = argv[++i];
    else if (arg === '--end') out.endDate = argv[++i];
    else if (arg === '--force') out.force = true;
  }
  return out;
}

function minusCalendarDays(dayIso, count) {
  const date = new Date(`${dayIso}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() - count);
  return date.toISOString().slice(0, 10);
}

async function runBacktest(startDate, endDate, targetDays = null) {
  const vixStart = targetDays?.length
    ? minusCalendarDays(targetDays[0], 90)
    : null;
  const vixByDay = vixStart
    ? await loadVixTermZSeries(vixStart, endDate, VARIANT.params.lookback)
    : await loadVixTermZSeriesWide(VARIANT.params.lookback, endDate);
  const buffer = '2024-11-01';
  const fullStart = buffer < startDate ? buffer : startDate;
  const days = stockTradingDaysInRange(fullStart, endDate);
  const targetSet = targetDays ? new Set(targetDays) : null;
  const trades = [];
  const openPositions = [];
  for (let i = 0; i < days.length - 1; i += 1) {
    const sig = days[i];
    const nextDay = days[i + 1];
    if (sig < startDate || sig > endDate) continue;
    if (targetSet && !targetSet.has(nextDay)) continue;
    const v = vixByDay.get(sig);
    if (!v || !Number.isFinite(v.z_vvix) || v.z_vvix < VARIANT.params.zEnter) continue;
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

function previousTradingDay(allDays, day) {
  const index = allDays.indexOf(day);
  return index > 0 ? allDays[index - 1] : null;
}

async function buildReport({ trades, openPositions, days, allDays, spyByDay, baseReport = null }) {
  const tradesByDate = new Map(trades.map((t) => [t.date, t]));
  const snapshots = baseReport ? [...(baseReport.snapshots || [])] : [];
  const equitySeries = baseReport ? [...(baseReport.equitySeries || [])] : [];
  const combinedTrades = [...(baseReport?.trades || []), ...trades];
  let equity = Number.isFinite(baseReport?.summary?.finalEquity) ? baseReport.summary.finalEquity : INITIAL_CAPITAL;
  let spyEquity = INITIAL_CAPITAL * (1 + (equitySeries.at(-1)?.spyReturn || 0));
  for (let i = 0; i < days.length; i += 1) {
    const day = days[i];
    const t = tradesByDate.get(day);
    const dailyReturn = t ? t.netReturn : 0;
    equity *= (1 + dailyReturn);
    const todaySpy = spyByDay.get(day);
    const priorSpyClose = spyByDay.get(previousTradingDay(allDays, day));
    const spyRet = todaySpy && priorSpyClose ? (todaySpy / priorSpyClose - 1) : 0;
    spyEquity *= (1 + spyRet);
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
  const dailyReturns = equitySeries.map((point) => point.dailyReturn || 0);
  const summary = {
    startDate: equitySeries[0]?.date || null, endDate: equitySeries.at(-1)?.date || null,
    initialCapital: INITIAL_CAPITAL, finalEquity: equity,
    totalReturn: equity / INITIAL_CAPITAL - 1,
    totalReturnPct: (equity / INITIAL_CAPITAL - 1) * 100,
    cagr: null,
    maxDrawdown: maxDrawdown(equitySeries),
    maxDrawdownPct: maxDrawdown(equitySeries) * 100,
    sharpe: annualizedSharpe(dailyReturns),
    tradingDays: equitySeries.length, activeDays: combinedTrades.length, tradeCount: combinedTrades.length,
    longCount: combinedTrades.filter((t) => t.side === 'LONG').length, shortCount: 0,
    winCount: combinedTrades.filter((t) => t.isWin).length,
    hitRate: combinedTrades.length ? combinedTrades.filter((t) => t.isWin).length / combinedTrades.length : 0,
    hitRatePct: combinedTrades.length ? (combinedTrades.filter((t) => t.isWin).length / combinedTrades.length) * 100 : 0,
    avgNetReturnBps: combinedTrades.length ? (combinedTrades.reduce((a, t) => a + t.netReturn, 0) / combinedTrades.length) * 10_000 : 0,
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
    snapshots, equitySeries, trades: combinedTrades, openPositions, skippedDays: [],
    metadata: { id: VARIANT.id, name: VARIANT.name, displayName: VARIANT.displayName, description: VARIANT.description, ruleSummary: VARIANT.ruleSummary },
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const endDate = resolveEndDate(config, args.endDate || 'auto');
  const outPath = path.join(ARTIFACTS_DIR, `${VARIANT.id}-report.json`);
  let baseReport = null;
  const days = stockTradingDaysInRange(startDate, endDate);
  if (!args.force && fs.existsSync(outPath)) {
    const prior = JSON.parse(fs.readFileSync(outPath, 'utf8'));
    if (prior.summary?.startDate === startDate && prior.summary?.endDate >= endDate) {
      process.stdout.write(JSON.stringify({ status: 'current', id: VARIANT.id, endDate, outPath }, null, 2));
      process.stdout.write('\n');
      return;
    }
    if (prior.summary?.startDate === startDate && prior.summary?.endDate) baseReport = prior;
  }
  const targetDays = baseReport ? days.filter((day) => day > baseReport.summary.endDate) : days;
  process.stdout.write(`Building ${VARIANT.id} (${startDate} → ${endDate}) ${baseReport ? `incremental days=${targetDays.length}` : 'full'}...\n`);
  const { trades, openPositions } = await runBacktest(startDate, endDate, targetDays);
  const spyBars = await loadDailyBars('SPY', startDate, endDate);
  const spyByDay = new Map();
  for (const [d, b] of spyBars.entries()) if (Number.isFinite(b.close)) spyByDay.set(d, b.close);
  const report = await buildReport({ trades, openPositions, days: targetDays, allDays: days, spyByDay, baseReport });
  fs.mkdirSync(ARTIFACTS_DIR, { recursive: true });
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  process.stdout.write(`  trades=${report.summary.tradeCount} net=${report.summary.totalReturnPct.toFixed(2)}% Sharpe=${report.summary.sharpe.toFixed(2)} maxDD=${report.summary.maxDrawdownPct.toFixed(2)}% hit=${report.summary.hitRatePct.toFixed(1)}% → ${outPath}\n`);
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
