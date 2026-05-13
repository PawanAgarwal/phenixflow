#!/usr/bin/env node
// Build production artifacts for the surviving VIX term-structure variants.

const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT } = require('../src/config');
const { runBacktest } = require('../src/vix-term-structure');
const { defaultFeaturesPath } = require('../src/build-features-1m');
const zlib = require('node:zlib');
const readline = require('node:readline');

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

async function loadFeatures(day) {
  const p = defaultFeaturesPath(PROJECT_ROOT, 'SPY', day);
  if (!fs.existsSync(p)) return null;
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = [];
  for await (const line of rl) {
    if (!line) continue;
    out.push(JSON.parse(line));
  }
  return out;
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

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

async function buildReport({ variant, trades, openPositions, days, spyByDay }) {
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

  const summary = {
    startDate: days[0] || null,
    endDate: days[days.length - 1] || null,
    initialCapital: INITIAL_CAPITAL,
    finalEquity: equity,
    totalReturn: equity / INITIAL_CAPITAL - 1,
    totalReturnPct: (equity / INITIAL_CAPITAL - 1) * 100,
    cagr: null,
    maxDrawdown: maxDrawdown(equitySeries),
    maxDrawdownPct: maxDrawdown(equitySeries) * 100,
    sharpe: annualizedSharpe(dailyReturns),
    tradingDays: days.length,
    activeDays: trades.length,
    tradeCount: trades.length,
    longCount: trades.filter((t) => t.side === 'LONG').length,
    shortCount: trades.filter((t) => t.side === 'SHORT').length,
    winCount: trades.filter((t) => t.isWin).length,
    hitRate: trades.length ? trades.filter((t) => t.isWin).length / trades.length : 0,
    hitRatePct: trades.length ? (trades.filter((t) => t.isWin).length / trades.length) * 100 : 0,
    avgNetReturnBps: trades.length ? (trades.reduce((a, t) => a + t.netReturn, 0) / trades.length) * 10_000 : 0,
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
    trades,
    openPositions,
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
  const startDate = '2025-01-02';
  const endDate = '2026-05-11';

  const featuresRoot = path.join(PROJECT_ROOT, 'runtime', 'features-1m', 'SPY');
  const allDays = fs.existsSync(featuresRoot)
    ? fs.readdirSync(featuresRoot).filter((d) => d.startsWith('date=')).map((d) => d.slice('date='.length)).sort()
    : [];
  const days = allDays.filter((d) => d >= startDate && d <= endDate && !isWeekend(d));
  process.stdout.write(`Trading days in window: ${days.length}\n`);

  const spyByDay = new Map();
  for (const day of days) {
    // eslint-disable-next-line no-await-in-loop
    const rows = await loadFeatures(day);
    if (rows && rows.length > 0) {
      const last = rows[rows.length - 1];
      if (Number.isFinite(last.spy_close)) spyByDay.set(day, last.spy_close);
    }
  }

  fs.mkdirSync(ARTIFACTS_OUT_DIR, { recursive: true });
  for (const variant of VARIANTS) {
    process.stdout.write(`\n=== ${variant.id} ===\n`);
    // eslint-disable-next-line no-await-in-loop
    const r = await runBacktest({ startDate, endDate, params: variant.params });
    // eslint-disable-next-line no-await-in-loop
    const report = await buildReport({ variant, trades: r.trades, openPositions: r.openPositions, days, spyByDay });
    const outPath = path.join(ARTIFACTS_OUT_DIR, `${variant.id}-report.json`);
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
    process.stdout.write(`  trades=${report.summary.tradeCount} open=${r.openPositions.length} net=${report.summary.totalReturnPct.toFixed(2)}% Sharpe=${report.summary.sharpe.toFixed(2)} maxDD=${report.summary.maxDrawdownPct.toFixed(2)}% hit=${report.summary.hitRatePct.toFixed(1)}% → ${outPath}\n`);
  }
}

main().catch((err) => { process.stderr.write(`${err.stack || err.message}\n`); process.exit(1); });
