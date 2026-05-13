#!/usr/bin/env node
// Build production artifacts for the two walk-forward-surviving OCC P/C contrarian variants.
// Output: projects/spy-intraday-prediction/artifacts/occ-pc-contrarian-{variantId}-report.json
// in the standard strategy-service report schema.

const path = require('node:path');
const fs = require('node:fs');

const { PROJECT_ROOT, loadConfig, resolveEndDate } = require('../src/config');
const { runBacktest, loadSpyMinuteBars } = require('../src/occ-pc-contrarian');

const INITIAL_CAPITAL = 10_000;
const ARTIFACTS_OUT_DIR = path.join(PROJECT_ROOT, 'artifacts');

const VARIANTS = [
  {
    id: 'occ-pc-contrarian-intraday-1x-long-only',
    name: 'OCC Put/Call Intraday Contrarian (Long-Only 1x)',
    displayName: 'OCC P/C Intraday Contrarian — Long-Only 1×',
    description: 'When OCC equity put/call ratio z-score ≥ +2.0 (extreme put-buying = fear), go LONG SPY at 9:35 ET next day, exit 15:55 ET. Long-only — the greed-side signal does not walk forward.',
    ruleSummary: [
      'At each EOD, compute the OCC equity put/call ratio (puts ÷ calls).',
      'Compute the trailing 20-day z-score of that ratio.',
      'If z ≥ +2.0 (extreme fear), enter SPY LONG at next-day 9:35 ET, exit 15:55 ET.',
      'Greed-side extremes (z ≤ -2.0) do not reverse cleanly and are skipped.',
      'No overnight, no leverage on this variant (lowest-risk production version).',
    ],
    params: { zEnter: 2.0, leverage: 1.0, longOnly: true, entryMinuteEt: 575, exitMinuteEt: 955, costBpsRoundTrip: 2 },
  },
  {
    id: 'occ-pc-contrarian-intraday-3x',
    name: 'OCC Put/Call Intraday Contrarian (3x SPXL/SPXU)',
    displayName: 'OCC P/C Intraday Contrarian — 3× SPXL/SPXU',
    description: 'When OCC equity put/call ratio z-score ≥ +2.5 (extreme fear), go LONG SPXL next day 9:35→15:55 ET. When z ≤ -2.5 (extreme greed), SHORT via SPXU. Tighter threshold + 3× leverage for amplified return.',
    ruleSummary: [
      'OCC equity put/call ratio z-score (trailing 20 days) at the prior EOD.',
      'If z ≥ +2.5: buy SPXL (3× SPY long) at 9:35 ET, sell at 15:55 ET (contrarian fear).',
      'If z ≤ -2.5: buy SPXU (3× SPY short) at 9:35 ET, sell at 15:55 ET (contrarian greed).',
      'Tighter z threshold (2.5 vs 2.0) and 3× leverage; realistic 3 bps RT cost on leveraged ETFs.',
    ],
    params: { zEnter: 2.5, leverage: 3.0, longOnly: false, entryMinuteEt: 575, exitMinuteEt: 955, costBpsRoundTrip: 3 },
  },
];

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
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

function stockTradingDays(config, startDate, endDate) {
  const roots = [config.roots.historical, config.roots.liveParquet].filter(Boolean);
  const dates = new Set();
  for (const root of roots) {
    const datasetRoot = path.join(root, config.datasets.stockBars);
    if (!fs.existsSync(datasetRoot)) continue;
    fs.readdirSync(datasetRoot)
      .filter((entry) => entry.startsWith('date='))
      .map((entry) => entry.slice('date='.length))
      .filter((day) => day >= startDate && day <= endDate && !isWeekend(day))
      .forEach((day) => dates.add(day));
  }
  return [...dates].sort();
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
      execution: t ? 'occ_pc_contrarian_intraday' : 'flat',
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
      occZScore: t?.signalZ ?? null,
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
      provider: 'OCC EOD put/call ratio + SPY 1m bars (historical CSV or live parquet)',
      strategySource: 'OCC P/C contrarian z-score on equity ratio',
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
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const endDate = resolveEndDate(config, args.endDate || 'auto');

  // Pre-load SPY closes for the benchmark series
  const days = stockTradingDays(config, startDate, endDate);
  process.stdout.write(`Trading days in window: ${days.length}\n`);

  const spyByDay = new Map();
  for (const day of days) {
    // eslint-disable-next-line no-await-in-loop
    const rows = await loadSpyMinuteBars(day);
    if (rows && rows.length > 0) {
      const last = rows[rows.length - 1];
      if (Number.isFinite(last.close)) spyByDay.set(day, last.close);
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
