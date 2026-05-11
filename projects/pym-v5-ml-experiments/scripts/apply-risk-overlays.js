#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const { readOptionFeatureJsonl } = require('../../pym-v5-replication/src/option-features');

const DEFAULT_ML_REPORT = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-micro-features-2025-02-01-2026-05-08.json';
const DEFAULT_DATASET = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-walkforward-dataset-micro-features-2025-01-02-2026-05-08.jsonl';
const DEFAULT_OPTION_FEATURES = 'projects/pym-v5-replication/runtime/pym-v5-option-bar-features-2025-01-02-2026-05-08.jsonl';
const DEFAULT_OPTION_OVERLAY_REPORT = 'projects/pym-v5-replication/artifacts/pym-v5-option-overlay-suite-grid-top8-zm0p5-2025-01-02-2026-05-08.json';
const DEFAULT_OPTION_OVERLAY_STRATEGY = 'grid_pym_option_rank_top8_zm0p5';
const DEFAULT_OUTPUT = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-two-speed-risk-overlays-2025-02-01-2026-05-08.json';
const DEFAULT_STRATEGY = 'two_speed_attention_pym_light_governed';
const DEFAULT_INITIAL_CAPITAL = 10000;
const DEFAULT_COST_BPS = 2;

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--ml-report') args.mlReport = argv[++index];
    else if (arg === '--dataset') args.dataset = argv[++index];
    else if (arg === '--option-features') args.optionFeatures = argv[++index];
    else if (arg === '--option-overlay-report') args.optionOverlayReport = argv[++index];
    else if (arg === '--option-overlay-strategy') args.optionOverlayStrategy = argv[++index];
    else if (arg === '--strategy') args.strategy = argv[++index];
    else if (arg === '--out') args.out = argv[++index];
    else if (arg === '--cost-bps') args.costBps = Number(argv[++index]);
  }
  return args;
}

function finite(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function cleanHoldings(holdings) {
  const out = {};
  Object.entries(holdings || {}).forEach(([ticker, weight]) => {
    const value = finite(weight);
    if (value > 1e-10) out[ticker] = value;
  });
  return out;
}

function addScaled(target, source, scale) {
  Object.entries(source || {}).forEach(([ticker, weight]) => {
    const value = finite(weight) * scale;
    if (Math.abs(value) > 1e-12) target[ticker] = finite(target[ticker]) + value;
  });
  return target;
}

function scaledWithBil(holdings, scale, safeTicker = 'BIL') {
  const out = addScaled({}, holdings, Math.max(0, Math.min(1, finite(scale))));
  out[safeTicker] = finite(out[safeTicker]) + (1 - Math.max(0, Math.min(1, finite(scale))));
  return cleanHoldings(out);
}

function blendHoldings(left, right, leftWeight) {
  const out = {};
  addScaled(out, left, leftWeight);
  addScaled(out, right, 1 - leftWeight);
  return cleanHoldings(out);
}

function turnover(previous, current) {
  const keys = new Set([...Object.keys(previous || {}), ...Object.keys(current || {})]);
  let total = 0;
  keys.forEach((ticker) => {
    total += Math.abs(finite(current?.[ticker]) - finite(previous?.[ticker]));
  });
  return total;
}

function portfolioReturn(holdings, nextReturns) {
  let total = 0;
  Object.entries(holdings || {}).forEach(([ticker, weight]) => {
    total += finite(weight) * finite(nextReturns?.[ticker]);
  });
  return total;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function monthlyReturns(points) {
  const months = new Map();
  points.forEach((point) => {
    const month = point.date.slice(0, 7);
    if (!months.has(month)) months.set(month, { startEquity: point.startEquity, endEquity: point.equity });
    months.get(month).endEquity = point.equity;
  });
  return [...months.entries()].map(([month, values]) => {
    const ret = values.startEquity > 0 ? values.endEquity / values.startEquity - 1 : 0;
    return { month, return: ret, returnPct: ret * 100 };
  });
}

function summarize(id, description, points, initialCapital) {
  let peak = initialCapital;
  let maxDrawdown = 0;
  points.forEach((point) => {
    peak = Math.max(peak, point.equity);
    maxDrawdown = Math.min(maxDrawdown, point.equity / peak - 1);
  });
  const returns = points.map((point) => point.netReturn);
  const finalEquity = points.at(-1)?.equity || initialCapital;
  const totalReturn = finalEquity / initialCapital - 1;
  const volatility = standardDeviation(returns) * Math.sqrt(252);
  const avgDaily = mean(returns);
  return {
    id,
    description,
    startDate: points[0]?.signalDate || null,
    endDate: points.at(-1)?.date || null,
    tradingDays: points.length,
    finalEquity,
    totalReturn,
    totalReturnPct: totalReturn * 100,
    maxDrawdown,
    maxDrawdownPct: maxDrawdown * 100,
    annualizedVolatility: volatility,
    annualizedVolatilityPct: volatility * 100,
    sharpe: volatility > 0 ? (avgDaily * 252) / volatility : 0,
    averageDailyTurnover: mean(points.map((point) => point.turnover)),
    averageDailyTurnoverPct: mean(points.map((point) => point.turnover)) * 100,
    monthlyReturns: monthlyReturns(points),
  };
}

function loadSamples(datasetPath) {
  const samples = new Map();
  for (const line of fs.readFileSync(datasetPath, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    const row = JSON.parse(line);
    if (row.type === 'sample') samples.set(row.date, row);
  }
  return samples;
}

function rollingValue(optionFeatures, root, key) {
  const value = optionFeatures?.roots?.[root]?.rolling?.[key];
  return Number.isFinite(value) ? value : 0;
}

function rootPutPressure(optionFeatures, root) {
  return Math.max(
    rollingValue(optionFeatures, root, 'putCallPremiumRatioZ20'),
    -rollingValue(optionFeatures, root, 'premiumImbalanceZ20'),
    0,
  );
}

function indexPutPressure(optionFeatures) {
  return Math.max(
    rootPutPressure(optionFeatures, 'SPY'),
    rootPutPressure(optionFeatures, 'QQQ'),
    rootPutPressure(optionFeatures, 'SPX'),
    rootPutPressure(optionFeatures, 'SPXW'),
    0,
  );
}

function shortDatedPutPressure(optionFeatures) {
  return Math.max(...['SPY', 'QQQ'].map((root) => {
    const feature = optionFeatures?.roots?.[root];
    const imbalance = feature?.premiumImbalance;
    const proxyZ = rollingValue(optionFeatures, root, 'shortDatedAtmFlowProxyZ20');
    return Number.isFinite(imbalance) && imbalance < 0 ? proxyZ : 0;
  }), 0);
}

function hedgeWithVixy() {
  return { BIL: 0.8, VIXY: 0.2 };
}

function realizedVol(returns, lookback) {
  const recent = returns.slice(-lookback);
  return standardDeviation(recent) * Math.sqrt(252);
}

function loadBasePoints(reportPath, strategyId) {
  const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  const strategy = report.strategies?.[strategyId];
  if (!strategy) throw new Error(`missing_strategy:${strategyId}`);
  return strategy.equityCurve.map((point) => ({
    signalDate: point.signalDate,
    date: point.date,
    baseHoldings: cleanHoldings(point.holdings),
  }));
}

function loadOptionOverlayHoldings(reportPath, strategyId) {
  const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  const strategy = report.strategies?.find((item) => item.summary?.id === strategyId);
  if (!strategy) throw new Error(`missing_option_overlay_strategy:${strategyId}`);
  return new Map(strategy.daySummaries.map((point) => [point.signalDate, cleanHoldings(point.holdings)]));
}

function makeOverlays() {
  const overlays = [
    {
      id: 'raw_two_speed',
      description: 'No risk overlay; recomputed from daily holdings.',
      weights: ({ baseHoldings }) => baseHoldings,
    },
    {
      id: 'drawdown_10_half_15_bil',
      description: 'If prior strategy drawdown is worse than -10%, run half exposure; worse than -15%, move to BIL.',
      weights: ({ baseHoldings, state }) => {
        const drawdown = state.peak > 0 ? state.equity / state.peak - 1 : 0;
        if (drawdown <= -0.15) return { BIL: 1 };
        if (drawdown <= -0.10) return scaledWithBil(baseHoldings, 0.5);
        return baseHoldings;
      },
    },
    {
      id: 'vol_target_25',
      description: 'Scale exposure to target 25% annualized realized strategy volatility over trailing 21 days.',
      weights: ({ baseHoldings, state }) => {
        const vol = realizedVol(state.returns, 21);
        if (!Number.isFinite(vol) || vol <= 0.25) return baseHoldings;
        return scaledWithBil(baseHoldings, Math.max(0.25, Math.min(1, 0.25 / vol)));
      },
    },
    {
      id: 'vol_target_30',
      description: 'Scale exposure to target 30% annualized realized strategy volatility over trailing 21 days.',
      weights: ({ baseHoldings, state }) => {
        const vol = realizedVol(state.returns, 21);
        if (!Number.isFinite(vol) || vol <= 0.30) return baseHoldings;
        return scaledWithBil(baseHoldings, Math.max(0.30, Math.min(1, 0.30 / vol)));
      },
    },
    {
      id: 'blend_75_ml_25_pym',
      description: 'Hold 75% ML portfolio and 25% PYM baseline.',
      weights: ({ baseHoldings, sample }) => blendHoldings(baseHoldings, sample.teacherWeights, 0.75),
    },
    {
      id: 'blend_50_ml_50_pym',
      description: 'Hold 50% ML portfolio and 50% PYM baseline.',
      weights: ({ baseHoldings, sample }) => blendHoldings(baseHoldings, sample.teacherWeights, 0.5),
    },
    {
      id: 'blend_90_ml_10_option_top8',
      description: 'Hold 90% ML portfolio and 10% option top-8 overlay.',
      weights: ({ baseHoldings, optionOverlayHoldings }) => blendHoldings(baseHoldings, optionOverlayHoldings, 0.90),
    },
    {
      id: 'blend_75_ml_25_option_top8',
      description: 'Hold 75% ML portfolio and 25% option top-8 overlay.',
      weights: ({ baseHoldings, optionOverlayHoldings }) => blendHoldings(baseHoldings, optionOverlayHoldings, 0.75),
    },
    {
      id: 'blend_50_ml_50_option_top8',
      description: 'Hold 50% ML portfolio and 50% option top-8 overlay.',
      weights: ({ baseHoldings, optionOverlayHoldings }) => blendHoldings(baseHoldings, optionOverlayHoldings, 0.50),
    },
    {
      id: 'blend_25_ml_75_option_top8',
      description: 'Hold 25% ML portfolio and 75% option top-8 overlay.',
      weights: ({ baseHoldings, optionOverlayHoldings }) => blendHoldings(baseHoldings, optionOverlayHoldings, 0.25),
    },
  ];

  [1, 1.5, 2, 2.5].forEach((threshold) => {
    const label = String(threshold).replace('.', 'p');
    overlays.push({
      id: `spy_put_z${label}_to_bil`,
      description: `Move fully to BIL when SPY option put-pressure z-score is >= ${threshold}.`,
      weights: ({ baseHoldings, optionFeatures }) => (
        rootPutPressure(optionFeatures, 'SPY') >= threshold ? { BIL: 1 } : baseHoldings
      ),
    });
    overlays.push({
      id: `spy_put_z${label}_half_bil`,
      description: `Run half exposure when SPY option put-pressure z-score is >= ${threshold}.`,
      weights: ({ baseHoldings, optionFeatures }) => (
        rootPutPressure(optionFeatures, 'SPY') >= threshold ? scaledWithBil(baseHoldings, 0.5) : baseHoldings
      ),
    });
    overlays.push({
      id: `index_put_z${label}_to_bil`,
      description: `Move fully to BIL when SPY/QQQ/SPX/SPXW put-pressure z-score is >= ${threshold}.`,
      weights: ({ baseHoldings, optionFeatures }) => (
        indexPutPressure(optionFeatures) >= threshold ? { BIL: 1 } : baseHoldings
      ),
    });
    overlays.push({
      id: `index_put_z${label}_half_bil`,
      description: `Run half exposure when SPY/QQQ/SPX/SPXW put-pressure z-score is >= ${threshold}.`,
      weights: ({ baseHoldings, optionFeatures }) => (
        indexPutPressure(optionFeatures) >= threshold ? scaledWithBil(baseHoldings, 0.5) : baseHoldings
      ),
    });
  });

  [1, 1.5, 2].forEach((threshold) => {
    const label = String(threshold).replace('.', 'p');
    overlays.push({
      id: `short_atm_put_z${label}_to_bil`,
      description: `Move fully to BIL when short-dated ATM put-flow pressure is >= ${threshold}.`,
      weights: ({ baseHoldings, optionFeatures }) => (
        shortDatedPutPressure(optionFeatures) >= threshold ? { BIL: 1 } : baseHoldings
      ),
    });
    overlays.push({
      id: `short_atm_put_z${label}_half_bil`,
      description: `Run half exposure when short-dated ATM put-flow pressure is >= ${threshold}.`,
      weights: ({ baseHoldings, optionFeatures }) => (
        shortDatedPutPressure(optionFeatures) >= threshold ? scaledWithBil(baseHoldings, 0.5) : baseHoldings
      ),
    });
  });

  overlays.push(
    {
      id: 'index_put_z1p5_vixy_hedge',
      description: 'Replace ML portfolio with 80% BIL / 20% VIXY when broad index option put pressure is >= 1.5.',
      weights: ({ baseHoldings, optionFeatures }) => (
        indexPutPressure(optionFeatures) >= 1.5 ? hedgeWithVixy() : baseHoldings
      ),
    },
    {
      id: 'index_put_z1p5_to_option_top8',
      description: 'Switch to option top-8 overlay when broad index option put pressure is >= 1.5.',
      weights: ({ baseHoldings, optionFeatures, optionOverlayHoldings }) => (
        indexPutPressure(optionFeatures) >= 1.5 ? optionOverlayHoldings : baseHoldings
      ),
    },
    {
      id: 'drawdown_10_to_option_top8',
      description: 'Switch to option top-8 overlay when prior strategy drawdown is worse than -10%.',
      weights: ({ baseHoldings, optionOverlayHoldings, state }) => {
        const drawdown = state.peak > 0 ? state.equity / state.peak - 1 : 0;
        return drawdown <= -0.10 ? optionOverlayHoldings : baseHoldings;
      },
    },
    {
      id: 'vol30_to_option_top8',
      description: 'Switch to option top-8 overlay when trailing 21-day annualized strategy volatility exceeds 30%.',
      weights: ({ baseHoldings, optionOverlayHoldings, state }) => (
        realizedVol(state.returns, 21) > 0.30 ? optionOverlayHoldings : baseHoldings
      ),
    },
    {
      id: 'index_put_z2_or_drawdown_10_half',
      description: 'Half exposure when broad put pressure is >= 2 or prior strategy drawdown is worse than -10%.',
      weights: ({ baseHoldings, optionFeatures, state }) => {
        const drawdown = state.peak > 0 ? state.equity / state.peak - 1 : 0;
        return indexPutPressure(optionFeatures) >= 2 || drawdown <= -0.10
          ? scaledWithBil(baseHoldings, 0.5)
          : baseHoldings;
      },
    },
    {
      id: 'spy_put_z2p5_or_drawdown_10_to_bil',
      description: 'Move to BIL when SPY put pressure is >= 2.5 or prior strategy drawdown is worse than -10%.',
      weights: ({ baseHoldings, optionFeatures, state }) => {
        const drawdown = state.peak > 0 ? state.equity / state.peak - 1 : 0;
        return rootPutPressure(optionFeatures, 'SPY') >= 2.5 || drawdown <= -0.10 ? { BIL: 1 } : baseHoldings;
      },
    },
  );

  return overlays;
}

function simulateOverlay({ overlay, basePoints, samplesByDate, optionByDate, optionOverlayByDate, costBps, initialCapital }) {
  const state = { equity: initialCapital, peak: initialCapital, previousHoldings: {}, returns: [] };
  const points = [];
  basePoints.forEach((basePoint) => {
    const sample = samplesByDate.get(basePoint.signalDate);
    if (!sample) throw new Error(`missing_sample:${basePoint.signalDate}`);
    const optionFeatures = optionByDate.get(basePoint.signalDate);
    const optionOverlayHoldings = optionOverlayByDate.get(basePoint.signalDate) || { BIL: 1 };
    const desired = cleanHoldings(overlay.weights({
      baseHoldings: basePoint.baseHoldings,
      optionOverlayHoldings,
      sample,
      optionFeatures,
      state,
      basePoint,
    }));
    const dayTurnover = turnover(state.previousHoldings, desired);
    const grossReturn = portfolioReturn(desired, sample.nextReturns);
    const costReturn = dayTurnover * costBps / 10000;
    const netReturn = grossReturn - costReturn;
    const startEquity = state.equity;
    state.equity *= 1 + netReturn;
    state.peak = Math.max(state.peak, state.equity);
    state.previousHoldings = desired;
    state.returns.push(netReturn);
    points.push({
      signalDate: basePoint.signalDate,
      date: basePoint.date,
      startEquity,
      equity: state.equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover: dayTurnover,
      holdings: desired,
      diagnostics: {
        spyPutPressure: rootPutPressure(optionFeatures, 'SPY'),
        indexPutPressure: indexPutPressure(optionFeatures),
        shortDatedPutPressure: shortDatedPutPressure(optionFeatures),
      },
    });
  });
  return {
    summary: summarize(overlay.id, overlay.description, points, initialCapital),
    points,
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const mlReport = args.mlReport || DEFAULT_ML_REPORT;
  const dataset = args.dataset || DEFAULT_DATASET;
  const optionFeatures = args.optionFeatures || DEFAULT_OPTION_FEATURES;
  const optionOverlayReport = args.optionOverlayReport || DEFAULT_OPTION_OVERLAY_REPORT;
  const optionOverlayStrategy = args.optionOverlayStrategy || DEFAULT_OPTION_OVERLAY_STRATEGY;
  const strategy = args.strategy || DEFAULT_STRATEGY;
  const outputPath = args.out || DEFAULT_OUTPUT;
  const costBps = Number.isFinite(args.costBps) ? args.costBps : DEFAULT_COST_BPS;

  const basePoints = loadBasePoints(mlReport, strategy);
  const samplesByDate = loadSamples(dataset);
  const optionRows = readOptionFeatureJsonl(optionFeatures, { rollingWindow: 20 });
  const optionByDate = new Map(optionRows.map((row) => [row.date, row]));
  const optionOverlayByDate = loadOptionOverlayHoldings(optionOverlayReport, optionOverlayStrategy);

  const overlays = makeOverlays();
  const results = overlays.map((overlay) => {
    const result = simulateOverlay({
      overlay,
      basePoints,
      samplesByDate,
      optionByDate,
      optionOverlayByDate,
      costBps,
      initialCapital: DEFAULT_INITIAL_CAPITAL,
    });
    return {
      id: overlay.id,
      description: overlay.description,
      summary: result.summary,
      points: result.points,
    };
  });
  const raw = results.find((result) => result.id === 'raw_two_speed').summary;
  const rankings = results.map((result) => ({
    id: result.id,
    totalReturnPct: result.summary.totalReturnPct,
    excessVsRawPct: result.summary.totalReturnPct - raw.totalReturnPct,
    sharpe: result.summary.sharpe,
    maxDrawdownPct: result.summary.maxDrawdownPct,
    drawdownImprovementPct: result.summary.maxDrawdownPct - raw.maxDrawdownPct,
    averageDailyTurnoverPct: result.summary.averageDailyTurnoverPct,
  })).sort((left, right) => {
    const leftScore = left.totalReturnPct + (left.drawdownImprovementPct * 4);
    const rightScore = right.totalReturnPct + (right.drawdownImprovementPct * 4);
    return rightScore - leftScore;
  });

  const report = {
    generatedAt: new Date().toISOString(),
    source: {
      mlReport,
      dataset,
      optionFeatures,
      optionOverlayReport,
      optionOverlayStrategy,
      strategy,
      note: 'Risk overlays are causal wrappers around precomputed ML daily holdings. Returns are recomputed from Massive-derived next-day returns.',
    },
    settings: {
      costBps,
      initialCapital: DEFAULT_INITIAL_CAPITAL,
      firstSignalDate: basePoints[0]?.signalDate,
      lastSignalDate: basePoints.at(-1)?.signalDate,
      timing: 'EOD signal date X; overlay uses data through X and realizes close-to-close return through X+1.',
    },
    rankings,
    overlays: Object.fromEntries(results.map((result) => [result.id, {
      summary: result.summary,
      points: result.points,
    }])),
  };

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath,
    topByReturn: rankings.slice().sort((left, right) => right.totalReturnPct - left.totalReturnPct).slice(0, 12),
    topByDrawdown: rankings.slice().sort((left, right) => right.drawdownImprovementPct - left.drawdownImprovementPct).slice(0, 12),
  }, null, 2));
}

if (require.main === module) {
  main();
}
