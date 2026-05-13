// Strategy module for composite-portfolio variants.
//
// A composite reads the per-component report artifacts (already produced by
// each component strategy's own builder) and combines their daily returns
// with fixed weights.  No new signal logic — pure portfolio overlay.
//
// recompute() reads every component report and rebuilds the composite
// equitySeries + snapshots on the union of dates.  refreshData() invokes
// each component's underlying refresh chain so all artifacts are current,
// then re-recomputes.

const fs = require('node:fs');
const path = require('node:path');

const { regularSessionExecution } = require('./execution');
const { runRefreshSequence, refreshEodInputsStep } = require('./refresh-helpers');

const REPO_ROOT = path.resolve(__dirname, '..', '..', '..', '..');
const ARTIFACTS_DIR = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'artifacts');
const SPY_INTRADAY_SCRIPTS = path.join(REPO_ROOT, 'projects', 'spy-intraday-prediction', 'scripts');

const COMPONENT_BUILDERS = {
  'occ-pc-contrarian-intraday-1x-long-only': 'build-occ-pc-contrarian-artifacts.js',
  'occ-pc-contrarian-intraday-3x': 'build-occ-pc-contrarian-artifacts.js',
  'vix-term-contrarian-intraday-vix3m-1x': 'build-vix-term-structure-artifacts.js',
  'vix-term-contrarian-intraday-inv-long-3x-overnight': 'build-vix-term-structure-artifacts.js',
  'vvix-spike-contrarian-overnight-3x': 'build-vvix-spike-artifacts.js',
  'gap-down-fade-intraday-3x': 'build-gap-down-fade-artifacts.js',
};

const VARIANTS = {
  'fear-extreme-portfolio-equalweight-4x': {
    name: 'Fear-Extreme Equal-Weight Portfolio (4 variants)',
    displayName: 'Fear-Extreme Portfolio — equal-weight 4 variants',
    description: 'Equal-weight (25%) combination of the 4 existing fear-extreme intraday variants: OCC P/C 1×, OCC P/C 3×, VIX/VIX3M 1×, VIX1D/VIX3M 3× overnight. Diversifies across the OCC and VIX legs (correlation ≈ 0.05).',
    family: 'composite-volatility-contrarian',
    components: [
      { id: 'occ-pc-contrarian-intraday-1x-long-only', weight: 0.25 },
      { id: 'occ-pc-contrarian-intraday-3x', weight: 0.25 },
      { id: 'vix-term-contrarian-intraday-vix3m-1x', weight: 0.25 },
      { id: 'vix-term-contrarian-intraday-inv-long-3x-overnight', weight: 0.25 },
    ],
  },
  'fear-basket-vvix-occ3x-vix3xon-3x': {
    name: 'Fear Basket — VVIX + OCC-3× + VIX-3× Overnight',
    displayName: 'Fear Basket — VVIX + OCC-3× + VIX-3× ON (best Sharpe)',
    description: 'Equal-weight (33%) composite of the 3 best-Sharpe fear signals: VVIX spike contrarian, OCC P/C 3× contrarian, and VIX1D/VIX3M 3× overnight. Highest portfolio Sharpe in the orthogonality study (1.84).',
    family: 'composite-volatility-contrarian',
    components: [
      { id: 'vvix-spike-contrarian-overnight-3x', weight: 1 / 3 },
      { id: 'occ-pc-contrarian-intraday-3x', weight: 1 / 3 },
      { id: 'vix-term-contrarian-intraday-inv-long-3x-overnight', weight: 1 / 3 },
    ],
  },
  'fear-basket-vvix-vix3xon-3x': {
    name: 'Fear Basket — VVIX + VIX-3× Overnight',
    displayName: 'Fear Basket — VVIX + VIX-3× ON (high return)',
    description: 'Equal-weight (50/50) composite of VVIX spike contrarian and VIX1D/VIX3M 3× overnight. Higher absolute return than the 3-way basket (+42% vs +32%) with slightly higher drawdown (6.6% vs 4.1%).',
    family: 'composite-volatility-contrarian',
    components: [
      { id: 'vvix-spike-contrarian-overnight-3x', weight: 0.5 },
      { id: 'vix-term-contrarian-intraday-inv-long-3x-overnight', weight: 0.5 },
    ],
  },
  'gap-fade-vix3xon-hedge-3x': {
    name: 'Gap-Fade + VIX-3× Overnight Hedge',
    displayName: 'Gap-Fade + VIX-3× ON Hedge',
    description: 'Equal-weight (50/50) composite of SPY gap-down fade and VIX1D/VIX3M 3× overnight. Pairing the two near-orthogonal signals (correlation 0.01) cuts gap-fade-only maxDD from 15.4% to 7.7% with similar Sharpe.',
    family: 'composite-mean-reversion-hedged',
    components: [
      { id: 'gap-down-fade-intraday-3x', weight: 0.5 },
      { id: 'vix-term-contrarian-intraday-inv-long-3x-overnight', weight: 0.5 },
    ],
  },
};

function resolveArtifactPath(componentId) {
  return path.join(ARTIFACTS_DIR, `${componentId}-report.json`);
}

function readComponentReport(componentId) {
  const fp = resolveArtifactPath(componentId);
  if (!fs.existsSync(fp)) {
    throw new Error(`missing_composite_component:${componentId} at ${fp}`);
  }
  return JSON.parse(fs.readFileSync(fp, 'utf8'));
}

function unionDates(...maps) {
  const s = new Set();
  for (const m of maps) for (const d of m.keys()) s.add(d);
  return [...s].sort();
}

function maxDrawdown(equityPoints) {
  let peak = equityPoints[0]?.equity || 10_000; let dd = 0;
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

// Build a composite report from component reports.
function buildCompositeReport(variantId, spec) {
  const componentReports = spec.components.map((c) => ({ ...c, report: readComponentReport(c.id) }));
  // Per-component daily-return maps.
  const drMaps = componentReports.map((c) => {
    const m = new Map();
    for (const r of c.report.equitySeries || []) {
      if (r.date && Number.isFinite(r.dailyReturn)) m.set(r.date, r.dailyReturn);
    }
    return { weight: c.weight, dailyReturns: m, id: c.id };
  });
  const allDates = unionDates(...drMaps.map((c) => c.dailyReturns));
  // Per-component SPY benchmark by date (use the first component's benchmark; they should agree).
  const spyByDate = new Map();
  for (const c of componentReports) {
    for (const r of c.report.equitySeries || []) {
      if (r.date && Number.isFinite(r.spyReturn) && !spyByDate.has(r.date)) spyByDate.set(r.date, r.spyReturn);
    }
  }
  const initial = 10_000;
  let equity = initial;
  let spyEquity = initial; let priorSpy = null;
  const equitySeries = [];
  const snapshots = [];
  const dailyReturns = [];
  const composedTrades = [];
  for (let i = 0; i < allDates.length; i += 1) {
    const day = allDates[i];
    let combined = 0;
    const componentBreakdown = [];
    for (const c of drMaps) {
      const r = c.dailyReturns.get(day) || 0;
      combined += c.weight * r;
      if (r !== 0) componentBreakdown.push({ component: c.id, weight: c.weight, dailyReturn: r });
    }
    equity *= (1 + combined);
    dailyReturns.push(combined);
    // Track SPY from the cumulative total return in component reports — fallback to 0.
    const todaySpy = spyByDate.get(day);
    const spyRet = todaySpy !== undefined && priorSpy !== null ? todaySpy - priorSpy : 0;
    if (todaySpy !== undefined) priorSpy = todaySpy;
    spyEquity *= (1 + spyRet);
    const holdings = componentBreakdown.length
      ? componentBreakdown.map((b) => ({
        ticker: `[${b.component}]`,
        weight: b.weight,
        weightPct: b.weight * 100,
        dollars: equity * b.weight,
      }))
      : [{ ticker: 'CASH', weight: 1, weightPct: 100, dollars: equity }];
    snapshots.push({
      date: day,
      signalDate: day,
      rebalanceDate: day,
      execution: componentBreakdown.length ? 'composite_blend' : 'flat',
      nextDate: allDates[i + 1] || null,
      equityBeforeNextSession: equity,
      grossExposure: holdings.reduce((s, h) => s + Math.abs(h.weight), 0),
      turnover: componentBreakdown.length ? 1 : 0,
      turnoverPct: componentBreakdown.length ? 100 : 0,
      holdings,
      topHoldings: holdings.slice(0, 3).map((h) => h.ticker).join(', '),
      benchmarkReturns: { spy: spyRet, qqq: null },
      realized: {
        date: day,
        startEquity: equity / (1 + combined),
        endEquity: equity,
        grossReturn: combined,
        grossReturnPct: combined * 100,
        netReturn: combined,
        netReturnPct: combined * 100,
        costReturn: 0,
        costReturnPct: 0,
      },
      trade: null,
      componentBreakdown,
    });
    equitySeries.push({
      date: day, signalDate: day, equity, dailyReturn: combined,
      totalReturn: equity / initial - 1, spyReturn: spyEquity / initial - 1, qqqReturn: 0,
    });
    if (componentBreakdown.length) {
      composedTrades.push({
        date: day,
        side: combined >= 0 ? 'LONG' : 'SHORT',
        ticker: '[composite]',
        leverage: 1,
        size: 1,
        entryPrice: 1, exitPrice: 1 + combined,
        grossReturn: combined, cost: 0, netReturn: combined, isWin: combined > 0,
        entryMode: 'composite', carryOver: false,
        componentBreakdown,
      });
    }
  }
  const tradeCount = composedTrades.length;
  const winCount = composedTrades.filter((t) => t.isWin).length;
  const summary = {
    startDate: allDates[0] || null, endDate: allDates[allDates.length - 1] || null,
    initialCapital: initial, finalEquity: equity,
    totalReturn: equity / initial - 1, totalReturnPct: (equity / initial - 1) * 100,
    cagr: null,
    maxDrawdown: maxDrawdown(equitySeries),
    maxDrawdownPct: maxDrawdown(equitySeries) * 100,
    sharpe: annualizedSharpe(dailyReturns),
    tradingDays: allDates.length, activeDays: tradeCount, tradeCount,
    longCount: composedTrades.filter((t) => t.side === 'LONG').length,
    shortCount: composedTrades.filter((t) => t.side === 'SHORT').length,
    winCount,
    hitRate: tradeCount ? winCount / tradeCount : 0,
    hitRatePct: tradeCount ? (winCount / tradeCount) * 100 : 0,
    avgNetReturnBps: tradeCount ? (composedTrades.reduce((a, t) => a + t.netReturn, 0) / tradeCount) * 10_000 : 0,
    spyReturn: spyEquity / initial - 1, qqqReturn: 0,
    todayReturn: equitySeries.at(-1)?.dailyReturn ?? 0,
    todayReturnPct: (equitySeries.at(-1)?.dailyReturn ?? 0) * 100,
    todayDate: equitySeries.at(-1)?.date ?? null,
    latestRebalanceDate: snapshots.at(-1)?.date ?? null,
  };
  return {
    generatedAt: new Date().toISOString(),
    source: {
      provider: `Composite of: ${spec.components.map((c) => c.id).join(', ')}`,
      strategySource: 'Equal/weighted composite of component artifacts',
    },
    settings: { components: spec.components },
    summary, latest: snapshots.at(-1) || null,
    snapshots, equitySeries, trades: composedTrades, openPositions: [], skippedDays: [],
    metadata: {
      id: variantId, name: spec.name, displayName: spec.displayName,
      description: spec.description,
      ruleSummary: [
        `Equal/weighted blend of ${spec.components.length} component strategies.`,
        ...spec.components.map((c) => `  - ${(c.weight * 100).toFixed(0)}% ${c.id}`),
        'Composite daily return = Σᵢ wᵢ × component_i daily_return.',
        'No new signal logic; pure portfolio overlay over already-walked-forward components.',
      ],
    },
  };
}

function createCompositePortfolioStrategy({ variantId } = {}) {
  if (!VARIANTS[variantId]) throw new Error(`unknown_composite_variant:${variantId}`);
  const spec = VARIANTS[variantId];
  const state = { report: null, loadedAt: null, refresh: null };

  const baseRuleSummary = [
    `Equal/weighted blend of ${spec.components.length} component strategies.`,
    ...spec.components.map((c) => `  - ${(c.weight * 100).toFixed(0)}% ${c.id}`),
    'Composite daily return = Σᵢ wᵢ × component_i daily_return.',
    'No new signal logic; pure portfolio overlay over already-walked-forward components.',
  ];

  function getMetadata() {
    const report = state.report;
    return {
      id: variantId,
      name: spec.name,
      displayName: spec.displayName,
      family: spec.family,
      cadence: 'composite',
      actionType: 'portfolio',
      execution: regularSessionExecution({ startTime: '09:35', endTime: '15:55' }),
      dataProvider: `Composite over: ${spec.components.map((c) => c.id).join(', ')}`,
      strategySource: 'Equal/weighted composite of component artifacts',
      description: spec.description,
      ruleSummary: report?.metadata?.ruleSummary || baseRuleSummary,
      sourceLinks: [],
      defaultStartDate: report?.summary?.startDate || '2025-01-02',
      artifactPath: null, // generated in memory
      components: spec.components,
      supports: ['chart', 'values', 'latest_portfolio', 'portfolio_change', 'refresh_data', 'trade_log', 'sharpe'],
    };
  }

  function recompute() {
    state.report = buildCompositeReport(variantId, spec);
    state.loadedAt = new Date().toISOString();
    return state.report;
  }

  function getReport() {
    if (!state.report) recompute();
    return state.report;
  }

  function refreshData() {
    // Refresh each component's underlying artifact, then re-recompute.
    const uniqueBuilders = new Set();
    for (const c of spec.components) {
      const builder = COMPONENT_BUILDERS[c.id];
      if (builder) uniqueBuilders.add(builder);
    }
    const steps = [refreshEodInputsStep()];
    for (const b of uniqueBuilders) {
      steps.push({
        label: `composite-component:${b}`,
        command: process.execPath,
        args: [path.join(SPY_INTRADAY_SCRIPTS, b)],
      });
    }
    return runRefreshSequence(state, steps, recompute);
  }

  return { state, getMetadata, getReport, recompute, refreshData };
}

module.exports = {
  createCompositePortfolioStrategy,
  COMPOSITE_VARIANTS: Object.keys(VARIANTS),
};
