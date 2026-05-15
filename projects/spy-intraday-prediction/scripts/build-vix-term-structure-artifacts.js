#!/usr/bin/env node
// Build production artifacts for the surviving VIX term-structure variants.

const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT, loadConfig, resolveEndDate } = require('../src/config');
const {
  stockTradingDaysInRange,
  loadDailyBars,
  loadVixTermZSeries,
  loadVixTermZSeriesWide,
  executeTrade,
} = require('../src/research-utils');

const INITIAL_CAPITAL = 10_000;
const ARTIFACTS_OUT_DIR = path.join(PROJECT_ROOT, 'artifacts');

const VARIANTS = [
  {
    id: 'vix-term-contrarian-intraday-vix3m-1x',
    name: 'VIX Term Contrarian Intraday (VIX/VIX3M, 1x)',
    displayName: 'VIX Term Contrarian — VIX/VIX3M 1×',
    description: 'Contrarian on extremes of the VIX/VIX3M term-structure ratio. When the rolling 20-day z-score crosses ±2.0, trade SPY 9:35 → 15:55 ET next day. Inversion (z>0, fear) → LONG; steep contango (z<0, complacency) → SHORT.',
    ruleSummary: [
      'At each EOD, compute the VIX/VIX3M ratio (front vs 3-month implied vol).',
      'Compute trailing 20-day z-score of that ratio.',
      'If z ≥ +2.0 (curve inverting, front-end panic): next-day LONG SPY 9:35 → 15:55 ET.',
      'If z ≤ -2.0 (steep contango, complacency): next-day SHORT SPY 9:35 → 15:55 ET.',
      'No leverage, no overnight; lowest-risk production cut.',
    ],
    params: { metric: 'vix_over_vix3m', zEnter: 2.0, leverage: 1.0, costBpsRoundTrip: 2 },
  },
  {
    id: 'vix-term-contrarian-intraday-inv-long-3x-overnight',
    name: 'VIX Term Contrarian Overnight (Inversion Long, 3x)',
    displayName: 'VIX Term Contrarian — Inversion-Long 3× Overnight',
    description: 'Inversion-only contrarian: when VIX1D/VIX3M z-score ≥ +2.0 (front-end panic), buy TQQQ at prior-day 15:55 ET and exit at next-session 15:55 ET. 3× leverage on the "V-shape recovery" thesis. Highest-conviction speculative variant.',
    ruleSummary: [
      'At EOD, compute VIX1D/VIX3M ratio; if 20-day z-score ≥ +2.0 (extreme inversion / panic):',
      'Enter TQQQ at THAT SAME DAY 15:55 ET (captures overnight V-shape).',
      'Exit at NEXT SESSION 15:55 ET.',
      'Steep-contango shorts are deliberately skipped — they did not walk forward.',
      'Higher drawdown risk vs the 1× intraday variant; treat as a satellite, not a core position.',
    ],
    params: { metric: 'vix1d_over_vix3m', zEnter: 2.0, inversionLongOnly: true, leverage: 3.0, overnight: true, costBpsRoundTrip: 3 },
  },
];

function maxDrawdown(equityPoints) {
  let peak = equityPoints[0]?.equity || INITIAL_CAPITAL;
  let dd = 0;
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

function previousTradingDay(allDays, day) {
  const index = allDays.indexOf(day);
  return index > 0 ? allDays[index - 1] : null;
}

function vixSignalForVariant(variant, vixRow) {
  if (!vixRow) return null;
  if (variant.params.metric === 'vix_over_vix3m') {
    return { z: vixRow.z_vix_3m, ratio: vixRow.ratio_vix_3m };
  }
  return { z: vixRow.z_1d_3m, ratio: vixRow.ratio_1d_3m };
}

function minusCalendarDays(dayIso, count) {
  const date = new Date(`${dayIso}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() - count);
  return date.toISOString().slice(0, 10);
}

async function buildTradesForVariant({ variant, allDays, targetDays, endDate }) {
  const lookback = variant.params.lookback || 20;
  const vixStart = targetDays?.length ? minusCalendarDays(targetDays[0], 90) : null;
  const vixByDay = vixStart
    ? await loadVixTermZSeries(vixStart, endDate, lookback)
    : await loadVixTermZSeriesWide(lookback, endDate);
  const targetSet = new Set(targetDays);
  const trades = [];
  const openPositions = [];
  for (let i = 0; i < allDays.length - 1; i += 1) {
    const signalDay = allDays[i];
    const tradeDay = allDays[i + 1];
    if (!targetSet.has(tradeDay)) continue;
    const signal = vixSignalForVariant(variant, vixByDay.get(signalDay));
    if (!signal || !Number.isFinite(signal.z) || !Number.isFinite(signal.ratio)) continue;
    const { ratio, z } = signal;
    if (Math.abs(z) < variant.params.zEnter) continue;
    let side = null;
    if (z >= variant.params.zEnter && !variant.params.contangoShortOnly) side = 'LONG';
    else if (z <= -variant.params.zEnter && !variant.params.inversionLongOnly) side = 'SHORT';
    if (!side) continue;
    const entryDay = variant.params.overnight ? signalDay : tradeDay;
    const entryMinuteEt = variant.params.overnight ? 955 : 575;
    const exitMinuteEt = 955;
    const ticker = side === 'LONG'
      ? (variant.params.overnight && variant.params.leverage > 1 ? 'TQQQ' : (variant.params.leverage > 1 ? 'SPXL' : 'SPY'))
      : (variant.params.overnight && variant.params.leverage > 1 ? 'SQQQ' : (variant.params.leverage > 1 ? 'SPXU' : 'SH'));
    // eslint-disable-next-line no-await-in-loop
    const tr = await executeTrade({
      side,
      leverage: variant.params.leverage,
      signalDay,
      entryDay,
      exitDay: tradeDay,
      entryMinuteEt,
      exitMinuteEt,
      costBpsRoundTrip: variant.params.costBpsRoundTrip,
    });
    if (!tr) {
      openPositions.push({
        signalDate: signalDay,
        signalZ: z,
        signalRatio: ratio,
        entryDate: entryDay,
        expectedExitDate: tradeDay,
        side,
        ticker,
        leverage: variant.params.leverage,
        entryMode: variant.params.overnight ? 'overnight' : 'intraday',
        carryOver: Boolean(variant.params.overnight),
      });
      continue;
    }
    trades.push({
      date: tradeDay,
      entryDate: entryDay,
      exitDate: tradeDay,
      signalDate: signalDay,
      signalZ: z,
      signalRatio: ratio,
      side,
      ticker,
      leverage: variant.params.leverage,
      entryPrice: tr.entryPrice,
      exitPrice: tr.exitPrice,
      grossReturn: tr.grossReturn,
      cost: tr.cost,
      netReturn: tr.netReturn,
      isWin: tr.netReturn > 0,
      entryMode: variant.params.overnight ? 'overnight' : 'intraday',
      carryOver: Boolean(variant.params.overnight),
    });
  }
  return { trades, openPositions };
}

async function buildReport({ variant, trades, openPositions, days, allDays, spyByDay, baseReport = null }) {
  const tradesByDate = new Map(trades.map((t) => [t.date, t]));
  const snapshots = baseReport ? [...(baseReport.snapshots || [])] : [];
  const equitySeries = baseReport ? [...(baseReport.equitySeries || [])] : [];
  const combinedTrades = [...(baseReport?.trades || []), ...trades];
  const combinedOpenPositions = [...(baseReport?.openPositions || []), ...openPositions];
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
      ? [{
        ticker: t.ticker,
        weight: t.side === 'LONG' ? +variant.params.leverage : -variant.params.leverage,
        weightPct: (t.side === 'LONG' ? +variant.params.leverage : -variant.params.leverage) * 100,
        dollars: equity * variant.params.leverage,
      }]
      : [{ ticker: 'CASH', weight: 1, weightPct: 100, dollars: equity }];
    snapshots.push({
      date: day,
      signalDate: t?.signalDate || day,
      rebalanceDate: day,
      execution: t ? (t.entryMode === 'overnight' ? 'vix_term_contrarian_overnight' : 'vix_term_contrarian_intraday') : 'flat',
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
      vixSignalZ: t?.signalZ ?? null,
      vixSignalRatio: t?.signalRatio ?? null,
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
    startDate: equitySeries[0]?.date || null,
    endDate: equitySeries.at(-1)?.date || null,
    initialCapital: INITIAL_CAPITAL,
    finalEquity: equity,
    totalReturn: equity / INITIAL_CAPITAL - 1,
    totalReturnPct: (equity / INITIAL_CAPITAL - 1) * 100,
    cagr: null,
    maxDrawdown: maxDrawdown(equitySeries),
    maxDrawdownPct: maxDrawdown(equitySeries) * 100,
    sharpe: annualizedSharpe(dailyReturns),
    tradingDays: equitySeries.length,
    activeDays: combinedTrades.length,
    tradeCount: combinedTrades.length,
    longCount: combinedTrades.filter((t) => t.side === 'LONG').length,
    shortCount: combinedTrades.filter((t) => t.side === 'SHORT').length,
    winCount: combinedTrades.filter((t) => t.isWin).length,
    hitRate: combinedTrades.length ? combinedTrades.filter((t) => t.isWin).length / combinedTrades.length : 0,
    hitRatePct: combinedTrades.length ? (combinedTrades.filter((t) => t.isWin).length / combinedTrades.length) * 100 : 0,
    avgNetReturnBps: combinedTrades.length ? (combinedTrades.reduce((a, t) => a + t.netReturn, 0) / combinedTrades.length) * 10_000 : 0,
    spyReturn: spyEquity / INITIAL_CAPITAL - 1,
    qqqReturn: 0,
    todayReturn: equitySeries.at(-1)?.dailyReturn ?? 0,
    todayReturnPct: (equitySeries.at(-1)?.dailyReturn ?? 0) * 100,
    todayDate: equitySeries.at(-1)?.date ?? null,
    latestRebalanceDate: snapshots.at(-1)?.date ?? null,
  };

  return {
    generatedAt: new Date().toISOString(),
    source: {
      provider: 'Massive indices_1m (VIX/VIX1D/VIX3M closes) + SPY 1m bars',
      strategySource: 'VIX term-structure z-score contrarian',
    },
    settings: variant.params,
    summary,
    latest: snapshots.at(-1) || null,
    snapshots,
    equitySeries,
    trades: combinedTrades,
    openPositions: combinedOpenPositions,
    skippedDays: [],
    metadata: {
      id: variant.id,
      name: variant.name,
      displayName: variant.displayName,
      description: variant.description,
      ruleSummary: variant.ruleSummary,
    },
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const endDate = resolveEndDate(config, args.endDate || 'auto');

  const days = stockTradingDaysInRange(startDate, endDate);
  process.stdout.write(`Trading days in window: ${days.length}\n`);

  const spyBars = await loadDailyBars('SPY', startDate, endDate);
  const spyByDay = new Map();
  for (const [day, bar] of spyBars.entries()) {
    if (Number.isFinite(bar.close)) spyByDay.set(day, bar.close);
  }

  fs.mkdirSync(ARTIFACTS_OUT_DIR, { recursive: true });
  for (const variant of VARIANTS) {
    process.stdout.write(`\n=== ${variant.id} ===\n`);
    const outPath = path.join(ARTIFACTS_OUT_DIR, `${variant.id}-report.json`);
    let baseReport = null;
    if (!args.force && fs.existsSync(outPath)) {
      const prior = JSON.parse(fs.readFileSync(outPath, 'utf8'));
      if (prior.summary?.startDate === startDate && prior.summary?.endDate >= endDate) {
        process.stdout.write(`  current through ${endDate} → ${outPath}\n`);
        continue;
      }
      if (prior.summary?.startDate === startDate && prior.summary?.endDate) baseReport = prior;
    }
    const targetDays = baseReport ? days.filter((day) => day > baseReport.summary.endDate) : days;
    process.stdout.write(`  mode=${baseReport ? 'incremental' : 'full'} targetDays=${targetDays.length}\n`);
    // eslint-disable-next-line no-await-in-loop
    const r = await buildTradesForVariant({ variant, allDays: days, targetDays, endDate });
    // eslint-disable-next-line no-await-in-loop
    const report = await buildReport({ variant, trades: r.trades, openPositions: r.openPositions, days: targetDays, allDays: days, spyByDay, baseReport });
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
    process.stdout.write(`  trades=${report.summary.tradeCount} open=${r.openPositions.length} net=${report.summary.totalReturnPct.toFixed(2)}% Sharpe=${report.summary.sharpe.toFixed(2)} maxDD=${report.summary.maxDrawdownPct.toFixed(2)}% hit=${report.summary.hitRatePct.toFixed(1)}% → ${outPath}\n`);
  }
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
