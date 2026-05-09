const fs = require('node:fs');

const { readDailyBarsJsonl, tickerReturn } = require('./backtest');
const { loadConfig } = require('./config');
const { buildDailyRebalanceReport, defaultScorePath, findLatestMassiveEodBarsPath } = require('./rebalance-report');
const { collectTickers } = require('./symphony');
const { readOptionFeatureJsonl } = require('./option-features');

const SAFE_TICKERS = new Set(['AGG', 'BIL', 'BND', 'BSV', 'EDV', 'IEF', 'IEI', 'IGIB', 'MUB', 'SHV', 'SHY', 'TIP', 'TLT']);
const DEFAULT_INDEX_ROOTS = Object.freeze(['SPY', 'QQQ', 'SPX', 'SPXW']);

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function maxDrawdown(equityCurve) {
  let peak = equityCurve[0]?.equity || 1;
  let drawdown = 0;
  equityCurve.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function weightTurnover(previous, next) {
  const keys = new Set([...previous.keys(), ...next.keys()]);
  let turnover = 0;
  keys.forEach((ticker) => {
    turnover += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return turnover;
}

function cleanWeights(weights, maxExposure = 1) {
  const out = new Map();
  let total = 0;
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 1e-10) {
      out.set(ticker, weight);
      total += weight;
    }
  });
  if (total > maxExposure && total > 0) {
    out.forEach((weight, ticker) => out.set(ticker, weight * maxExposure / total));
  }
  return out;
}

function normalizeWeights(weights, maxExposure = 1) {
  let total = 0;
  weights.forEach((weight) => {
    if (Number.isFinite(weight) && weight > 0) total += weight;
  });
  if (total <= 0) return new Map();
  const out = new Map();
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 0) out.set(ticker, weight * maxExposure / total);
  });
  return out;
}

function holdTicker(ticker, weight = 1) {
  return new Map([[ticker, weight]]);
}

function targetWeights(ctx) {
  return new Map((ctx.targetSnapshot?.holdings || []).map((holding) => [holding.ticker, holding.weight]));
}

function topTargetWeights(ctx, count) {
  return normalizeWeights(new Map([...targetWeights(ctx).entries()]
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))
    .slice(0, count)));
}

function scaledWithBil(baseWeights, scale) {
  const weights = new Map();
  baseWeights.forEach((weight, ticker) => weights.set(ticker, weight * scale));
  weights.set('BIL', (weights.get('BIL') || 0) + (1 - scale));
  return cleanWeights(weights);
}

function hedgeWithVixy() {
  return new Map([
    ['BIL', 0.8],
    ['VIXY', 0.2],
  ]);
}

function rootFeature(ctx, root) {
  return ctx.optionFeatures?.roots?.[root] || null;
}

function rollingValue(ctx, root, key) {
  const value = rootFeature(ctx, root)?.rolling?.[key];
  return Number.isFinite(value) ? value : 0;
}

function clamp(value, min, max) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(min, Math.min(max, value));
}

function rootPutPressure(ctx, root) {
  return Math.max(
    rollingValue(ctx, root, 'putCallPremiumRatioZ20'),
    -rollingValue(ctx, root, 'premiumImbalanceZ20'),
  );
}

function indexPutPressure(ctx, roots = DEFAULT_INDEX_ROOTS) {
  return Math.max(...roots.map((root) => rootPutPressure(ctx, root)), 0);
}

function indexCallConfirmation(ctx, roots = ['SPY', 'QQQ']) {
  return mean(roots.map((root) => rollingValue(ctx, root, 'premiumImbalanceZ20')));
}

function shortDatedPutPressure(ctx, roots = ['SPY', 'QQQ']) {
  return Math.max(...roots.map((root) => {
    const feature = rootFeature(ctx, root);
    const imbalance = feature?.premiumImbalance;
    const proxyZ = rollingValue(ctx, root, 'shortDatedAtmFlowProxyZ20');
    return Number.isFinite(imbalance) && imbalance < 0 ? proxyZ : 0;
  }), 0);
}

function optionMomentumScore(ctx, root) {
  const feature = rootFeature(ctx, root);
  if (!feature || feature.totalPremium <= 0) return null;
  return (
    rollingValue(ctx, root, 'premiumImbalanceZ20')
    + (0.35 * clamp(feature.rolling?.callPremiumMomentum5 || 0, -3, 3))
    - (0.25 * clamp(feature.rolling?.putPremiumMomentum5 || 0, -3, 3))
    + (0.15 * rollingValue(ctx, root, 'shortDatedAtmFlowProxyZ20') * Math.sign(feature.premiumImbalance || 0))
  );
}

function rankedOptionRoots(ctx, roots, { minScore = 0, reverse = false, minPremium = 50_000 } = {}) {
  return roots
    .map((root) => ({
      root,
      score: optionMomentumScore(ctx, root),
      premium: rootFeature(ctx, root)?.totalPremium || 0,
    }))
    .filter((item) => (
      Number.isFinite(item.score)
      && item.premium >= minPremium
      && (reverse ? item.score <= -minScore : item.score >= minScore)
      && ctx.market.closes.has(item.root)
    ))
    .sort((left, right) => (reverse ? left.score - right.score : right.score - left.score) || left.root.localeCompare(right.root));
}

function pymOptionRank(ctx, { count = 5, minScore = 0, reverse = false, equalWeight = false } = {}) {
  const base = targetWeights(ctx);
  const ranked = rankedOptionRoots(ctx, [...base.keys()], { minScore, reverse });
  const selected = ranked.slice(0, count);
  if (!selected.length) return holdTicker('BIL');
  const raw = new Map();
  selected.forEach(({ root }) => raw.set(root, equalWeight ? 1 : (base.get(root) || 0.0001)));
  return normalizeWeights(raw);
}

function universeOptionRank(ctx, { count = 5, minScore = 0.25, reverse = false } = {}) {
  const ranked = rankedOptionRoots(ctx, ctx.optionUniverse, { minScore, reverse, minPremium: 100_000 });
  const selected = ranked
    .filter((item) => !SAFE_TICKERS.has(item.root))
    .slice(0, count);
  if (!selected.length) return holdTicker('BIL');
  return normalizeWeights(new Map(selected.map((item) => [item.root, 1])));
}

function riskOffToBil(threshold, roots = DEFAULT_INDEX_ROOTS) {
  return (ctx) => (indexPutPressure(ctx, roots) >= threshold ? holdTicker('BIL') : targetWeights(ctx));
}

function riskOffHalfBil(threshold, roots = DEFAULT_INDEX_ROOTS) {
  return (ctx) => (indexPutPressure(ctx, roots) >= threshold ? scaledWithBil(targetWeights(ctx), 0.5) : targetWeights(ctx));
}

function riskOffVixHedge(threshold, roots = DEFAULT_INDEX_ROOTS) {
  return (ctx) => (indexPutPressure(ctx, roots) >= threshold ? hedgeWithVixy() : targetWeights(ctx));
}

function callConfirmToBil(threshold) {
  return (ctx) => (indexCallConfirmation(ctx) <= threshold ? holdTicker('BIL') : targetWeights(ctx));
}

function topTargetWhenCallConfirmed(count, riskOnThreshold, riskOffThreshold) {
  return (ctx) => {
    if (indexPutPressure(ctx) >= riskOffThreshold) return holdTicker('BIL');
    if (indexCallConfirmation(ctx) >= riskOnThreshold) return topTargetWeights(ctx, count);
    return targetWeights(ctx);
  };
}

function optionRootMomentum(ticker, threshold) {
  return (ctx) => ((optionMomentumScore(ctx, ticker) || 0) >= threshold ? holdTicker(ticker) : holdTicker('BIL'));
}

function optionRootContrarian(ticker, threshold) {
  return (ctx) => (rootPutPressure(ctx, ticker) >= threshold ? holdTicker(ticker) : holdTicker('BIL'));
}

function spyQqqOptionRank(threshold) {
  return (ctx) => {
    const ranked = rankedOptionRoots(ctx, ['SPY', 'QQQ'], { minScore: threshold, minPremium: 100_000 });
    if (!ranked.length) return holdTicker('BIL');
    return holdTicker(ranked[0].root);
  };
}

function generatedOptionOverlayStrategies() {
  const strategies = [];
  [0.5, 0.75, 1, 1.25, 1.5, 1.75, 2, 2.5].forEach((threshold) => {
    strategies.push({
      id: `grid_pym_spy_put_z${String(threshold).replace('.', 'p')}_to_bil`,
      name: `Grid PYM SPY put z >= ${threshold} to BIL`,
      family: 'grid_pym_option_overlay',
      description: 'Grid search: SPY option put-pressure risk-off threshold.',
      weights: riskOffToBil(threshold, ['SPY']),
    });
    strategies.push({
      id: `grid_pym_index_put_z${String(threshold).replace('.', 'p')}_to_bil`,
      name: `Grid PYM index put z >= ${threshold} to BIL`,
      family: 'grid_pym_option_overlay',
      description: 'Grid search: broad index option put-pressure risk-off threshold.',
      weights: riskOffToBil(threshold),
    });
    strategies.push({
      id: `grid_pym_index_put_z${String(threshold).replace('.', 'p')}_half_bil`,
      name: `Grid PYM index put z >= ${threshold} half BIL`,
      family: 'grid_pym_option_overlay',
      description: 'Grid search: broad index option put-pressure half-risk threshold.',
      weights: riskOffHalfBil(threshold),
    });
  });

  [0.5, 0.75, 1, 1.25, 1.5, 2].forEach((threshold) => {
    strategies.push({
      id: `grid_pym_short_atm_put_z${String(threshold).replace('.', 'p')}_to_bil`,
      name: `Grid PYM short ATM put z >= ${threshold} to BIL`,
      family: 'grid_pym_option_overlay',
      description: 'Grid search: short-dated ATM put-flow proxy risk-off threshold.',
      weights: (ctx) => (shortDatedPutPressure(ctx) >= threshold ? holdTicker('BIL') : targetWeights(ctx)),
    });
    strategies.push({
      id: `grid_pym_short_atm_put_z${String(threshold).replace('.', 'p')}_half_bil`,
      name: `Grid PYM short ATM put z >= ${threshold} half BIL`,
      family: 'grid_pym_option_overlay',
      description: 'Grid search: short-dated ATM put-flow proxy half-risk threshold.',
      weights: (ctx) => (shortDatedPutPressure(ctx) >= threshold ? scaledWithBil(targetWeights(ctx), 0.5) : targetWeights(ctx)),
    });
  });

  [3, 5, 8, 10].forEach((count) => {
    [-0.5, 0, 0.25, 0.5, 0.75, 1].forEach((threshold) => {
      const label = String(threshold).replace('-', 'm').replace('.', 'p');
      strategies.push({
        id: `grid_pym_option_rank_top${count}_z${label}`,
        name: `Grid PYM option-rank top ${count}, z >= ${threshold}`,
        family: 'grid_pym_option_direct',
        description: 'Grid search: current PYM holdings ranked by option-flow momentum using PYM target weights.',
        weights: (ctx) => pymOptionRank(ctx, { count, minScore: threshold }),
      });
      strategies.push({
        id: `grid_pym_option_rank_top${count}_equal_z${label}`,
        name: `Grid PYM option-rank top ${count} equal, z >= ${threshold}`,
        family: 'grid_pym_option_direct',
        description: 'Grid search: current PYM holdings ranked by option-flow momentum using equal weights.',
        weights: (ctx) => pymOptionRank(ctx, { count, minScore: threshold, equalWeight: true }),
      });
    });
  });

  ['SPY', 'QQQ'].forEach((ticker) => {
    [-0.5, 0, 0.25, 0.5, 0.75, 1].forEach((threshold) => {
      strategies.push({
        id: `grid_${ticker.toLowerCase()}_option_momentum_z${String(threshold).replace('-', 'm').replace('.', 'p')}`,
        name: `Grid ${ticker} option momentum z >= ${threshold}`,
        family: 'grid_direct_option_signal',
        description: `Grid search: holds ${ticker} when option-flow momentum exceeds threshold, otherwise BIL.`,
        weights: optionRootMomentum(ticker, threshold),
      });
    });
    [0.5, 0.75, 1, 1.25, 1.5].forEach((threshold) => {
      strategies.push({
        id: `grid_${ticker.toLowerCase()}_put_contrarian_z${String(threshold).replace('.', 'p')}`,
        name: `Grid ${ticker} put contrarian z >= ${threshold}`,
        family: 'grid_direct_option_signal',
        description: `Grid search: contrarian ${ticker} buy after elevated option put pressure.`,
        weights: optionRootContrarian(ticker, threshold),
      });
    });
  });

  [-0.5, 0, 0.25, 0.5, 0.75, 1].forEach((threshold) => {
    strategies.push({
      id: `grid_spy_qqq_option_rank_top1_z${String(threshold).replace('-', 'm').replace('.', 'p')}`,
      name: `Grid SPY/QQQ option-rank top 1, z >= ${threshold}`,
      family: 'grid_direct_option_signal',
      description: 'Grid search: ranks SPY and QQQ by option-flow momentum and holds the leader.',
      weights: spyQqqOptionRank(threshold),
    });
  });

  return strategies;
}

const OPTION_OVERLAY_STRATEGIES = Object.freeze([
  {
    id: 'pym_base_eod',
    name: 'PYM base EOD',
    family: 'baseline',
    description: 'Composer PYM V5 weights using the replicated EOD tree.',
    weights: targetWeights,
  },
  {
    id: 'spy_buy_hold',
    name: 'SPY buy and hold',
    family: 'baseline',
    description: 'SPY close-to-close benchmark.',
    weights: () => holdTicker('SPY'),
  },
  {
    id: 'qqq_buy_hold',
    name: 'QQQ buy and hold',
    family: 'baseline',
    description: 'QQQ close-to-close benchmark.',
    weights: () => holdTicker('QQQ'),
  },
  {
    id: 'bil_cash',
    name: 'BIL cash proxy',
    family: 'baseline',
    description: 'Treasury bill ETF cash proxy.',
    weights: () => holdTicker('BIL'),
  },
  {
    id: 'pym_spy_put_z1_to_bil',
    name: 'PYM, SPY put-pressure z >= 1 to BIL',
    family: 'pym_option_overlay',
    description: 'Moves fully to BIL when SPY option put pressure is one rolling standard deviation above normal.',
    weights: riskOffToBil(1, ['SPY']),
  },
  {
    id: 'pym_spy_put_z15_to_bil',
    name: 'PYM, SPY put-pressure z >= 1.5 to BIL',
    family: 'pym_option_overlay',
    description: 'Uses a stricter SPY option put-pressure risk-off gate.',
    weights: riskOffToBil(1.5, ['SPY']),
  },
  {
    id: 'pym_index_put_z1_to_bil',
    name: 'PYM, index put-pressure z >= 1 to BIL',
    family: 'pym_option_overlay',
    description: 'Moves to BIL when SPY, QQQ, SPX, or SPXW option put pressure is elevated.',
    weights: riskOffToBil(1),
  },
  {
    id: 'pym_index_put_z15_to_bil',
    name: 'PYM, index put-pressure z >= 1.5 to BIL',
    family: 'pym_option_overlay',
    description: 'Uses a stricter broad-index option put-pressure risk-off gate.',
    weights: riskOffToBil(1.5),
  },
  {
    id: 'pym_index_put_z1_half_bil',
    name: 'PYM, index put-pressure z >= 1 half BIL',
    family: 'pym_option_overlay',
    description: 'Cuts PYM exposure in half and parks the rest in BIL under elevated index put pressure.',
    weights: riskOffHalfBil(1),
  },
  {
    id: 'pym_index_put_z1_vixy_hedge',
    name: 'PYM, index put-pressure z >= 1 BIL/VIXY hedge',
    family: 'pym_option_overlay',
    description: 'Replaces PYM with 80% BIL and 20% VIXY when index option put pressure is elevated.',
    weights: riskOffVixHedge(1),
  },
  {
    id: 'pym_call_confirm_to_bil',
    name: 'PYM, call confirmation gate',
    family: 'pym_option_overlay',
    description: 'Holds PYM unless SPY/QQQ option premium imbalance turns meaningfully negative.',
    weights: callConfirmToBil(-0.5),
  },
  {
    id: 'pym_top3_on_call_confirm',
    name: 'PYM top-3 when calls confirm',
    family: 'pym_option_overlay',
    description: 'Concentrates in the top three PYM weights when SPY/QQQ option flow is risk-on; exits on put pressure.',
    weights: topTargetWhenCallConfirmed(3, 0.5, 1.25),
  },
  {
    id: 'pym_short_dated_atm_put_to_bil',
    name: 'PYM, short-dated ATM put-flow to BIL',
    family: 'pym_option_overlay',
    description: 'Moves to BIL when short-dated ATM option-flow proxy spikes on put-heavy SPY/QQQ flow.',
    weights: (ctx) => (shortDatedPutPressure(ctx) >= 1 ? holdTicker('BIL') : targetWeights(ctx)),
  },
  {
    id: 'pym_option_rank_top3',
    name: 'PYM holdings ranked by option momentum, top 3',
    family: 'pym_option_direct',
    description: 'Applies PYM-style ranking to option-flow momentum across the current PYM holdings.',
    weights: (ctx) => pymOptionRank(ctx, { count: 3, minScore: 0.25 }),
  },
  {
    id: 'pym_option_rank_top5',
    name: 'PYM holdings ranked by option momentum, top 5',
    family: 'pym_option_direct',
    description: 'Applies PYM-style ranking to option-flow momentum across the current PYM holdings.',
    weights: (ctx) => pymOptionRank(ctx, { count: 5, minScore: 0.25 }),
  },
  {
    id: 'pym_option_rank_top5_equal',
    name: 'PYM option-rank top 5 equal weight',
    family: 'pym_option_direct',
    description: 'Equal-weights the current PYM holdings with the strongest option-flow momentum.',
    weights: (ctx) => pymOptionRank(ctx, { count: 5, minScore: 0.25, equalWeight: true }),
  },
  {
    id: 'pym_option_rank_bottom5_contrarian',
    name: 'PYM option-rank bottom 5 contrarian',
    family: 'pym_option_direct',
    description: 'Contrarian test: buys current PYM holdings with the weakest option-flow momentum.',
    weights: (ctx) => pymOptionRank(ctx, { count: 5, minScore: 0.75, reverse: true, equalWeight: true }),
  },
  {
    id: 'spy_option_momentum_z0',
    name: 'SPY option momentum z >= 0',
    family: 'direct_option_signal',
    description: 'Holds SPY when SPY option-flow momentum is positive, otherwise BIL.',
    weights: optionRootMomentum('SPY', 0),
  },
  {
    id: 'spy_option_momentum_z05',
    name: 'SPY option momentum z >= 0.5',
    family: 'direct_option_signal',
    description: 'Holds SPY only on stronger positive SPY option-flow momentum.',
    weights: optionRootMomentum('SPY', 0.5),
  },
  {
    id: 'spy_option_put_contrarian_z1',
    name: 'SPY put-pressure contrarian z >= 1',
    family: 'direct_option_signal',
    description: 'Contrarian test: buys SPY after elevated SPY option put pressure, otherwise BIL.',
    weights: optionRootContrarian('SPY', 1),
  },
  {
    id: 'qqq_option_momentum_z0',
    name: 'QQQ option momentum z >= 0',
    family: 'direct_option_signal',
    description: 'Holds QQQ when QQQ option-flow momentum is positive, otherwise BIL.',
    weights: optionRootMomentum('QQQ', 0),
  },
  {
    id: 'qqq_option_momentum_z05',
    name: 'QQQ option momentum z >= 0.5',
    family: 'direct_option_signal',
    description: 'Holds QQQ only on stronger positive QQQ option-flow momentum.',
    weights: optionRootMomentum('QQQ', 0.5),
  },
  {
    id: 'qqq_option_put_contrarian_z1',
    name: 'QQQ put-pressure contrarian z >= 1',
    family: 'direct_option_signal',
    description: 'Contrarian test: buys QQQ after elevated QQQ option put pressure, otherwise BIL.',
    weights: optionRootContrarian('QQQ', 1),
  },
  {
    id: 'spy_qqq_option_rank_top1',
    name: 'SPY/QQQ option-rank top 1',
    family: 'direct_option_signal',
    description: 'Ranks SPY and QQQ by option-flow momentum and holds the leader, otherwise BIL.',
    weights: spyQqqOptionRank(0.25),
  },
  {
    id: 'pym_universe_option_rank_top3',
    name: 'PYM universe option-rank top 3',
    family: 'direct_option_signal',
    description: 'Ranks the full PYM tradable universe by option-flow momentum and equal-weights the top three.',
    weights: (ctx) => universeOptionRank(ctx, { count: 3, minScore: 0.5 }),
  },
  {
    id: 'pym_universe_option_rank_top5',
    name: 'PYM universe option-rank top 5',
    family: 'direct_option_signal',
    description: 'Ranks the full PYM tradable universe by option-flow momentum and equal-weights the top five.',
    weights: (ctx) => universeOptionRank(ctx, { count: 5, minScore: 0.5 }),
  },
  ...generatedOptionOverlayStrategies(),
]);

function benchmarkReturn(market, ticker, startIndex, endIndex) {
  const values = market.closes.get(ticker) || [];
  const start = values[startIndex];
  const end = values[endIndex];
  if (!Number.isFinite(start) || !Number.isFinite(end) || start <= 0) return null;
  return (end / start) - 1;
}

function emptyState(strategy, initialCapital) {
  return {
    strategy,
    equity: initialCapital,
    previousWeights: new Map(),
    dailyReturns: [],
    equityCurve: [],
    daySummaries: [],
    totalTurnover: 0,
    activeDays: 0,
    missingReturnEvents: 0,
    firstSignalDate: null,
    lastSignalDate: null,
    lastRealizedDate: null,
  };
}

function portfolioReturn(weights, market, nextIndex) {
  let grossReturn = 0;
  let missing = 0;
  weights.forEach((weight, ticker) => {
    const ret = tickerReturn(market.closes, ticker, nextIndex);
    if (ret === null) {
      missing += 1;
      return;
    }
    grossReturn += weight * ret;
  });
  return { grossReturn, missing };
}

function simulateDay({ state, market, signalIndex, signalDate, targetSnapshot, optionFeatures, optionUniverse, costBps }) {
  const ctx = {
    market,
    signalIndex,
    signalDate,
    targetSnapshot,
    optionFeatures,
    optionUniverse,
  };
  const desired = cleanWeights(state.strategy.weights(ctx));
  const turnover = weightTurnover(state.previousWeights, desired);
  const costReturn = turnover * costBps / 10000;
  const { grossReturn, missing } = portfolioReturn(desired, market, signalIndex + 1);
  const netReturn = grossReturn - costReturn;
  const startEquity = state.equity;
  state.equity *= (1 + netReturn);
  state.totalTurnover += turnover;
  state.missingReturnEvents += missing;
  state.dailyReturns.push(netReturn);
  state.equityCurve.push({
    date: market.dates[signalIndex + 1],
    signalDate,
    equity: state.equity,
    dailyReturn: netReturn,
  });
  state.daySummaries.push({
    signalDate,
    realizedDate: market.dates[signalIndex + 1],
    startEquity,
    endEquity: state.equity,
    grossReturn,
    costReturn,
    netReturn,
    turnover,
    holdings: Object.fromEntries(desired),
    optionDiagnostics: {
      spyOptionScore: optionMomentumScore(ctx, 'SPY'),
      qqqOptionScore: optionMomentumScore(ctx, 'QQQ'),
      indexPutPressure: indexPutPressure(ctx),
      indexCallConfirmation: indexCallConfirmation(ctx),
      shortDatedPutPressure: shortDatedPutPressure(ctx),
    },
  });
  if (desired.size) state.activeDays += 1;
  if (!state.firstSignalDate) state.firstSignalDate = signalDate;
  state.lastSignalDate = signalDate;
  state.lastRealizedDate = market.dates[signalIndex + 1];
  state.previousWeights = desired;
}

function summarizeState(state, initialCapital) {
  const totalReturn = (state.equity / initialCapital) - 1;
  const annualizedVolatility = standardDeviation(state.dailyReturns) * Math.sqrt(252);
  const avgDaily = mean(state.dailyReturns);
  return {
    id: state.strategy.id,
    name: state.strategy.name,
    family: state.strategy.family,
    description: state.strategy.description,
    firstSignalDate: state.firstSignalDate,
    lastSignalDate: state.lastSignalDate,
    lastRealizedDate: state.lastRealizedDate,
    finalEquity: state.equity,
    totalReturn,
    totalReturnPct: totalReturn * 100,
    cagr: state.dailyReturns.length ? ((1 + totalReturn) ** (252 / state.dailyReturns.length)) - 1 : 0,
    cagrPct: state.dailyReturns.length ? (((1 + totalReturn) ** (252 / state.dailyReturns.length)) - 1) * 100 : 0,
    maxDrawdown: maxDrawdown(state.equityCurve),
    maxDrawdownPct: maxDrawdown(state.equityCurve) * 100,
    annualizedVolatility,
    annualizedVolatilityPct: annualizedVolatility * 100,
    sharpe: annualizedVolatility > 0 ? (avgDaily * 252) / annualizedVolatility : 0,
    tradingDays: state.dailyReturns.length,
    activeDays: state.activeDays,
    winRate: state.dailyReturns.length ? state.dailyReturns.filter((value) => value > 0).length / state.dailyReturns.length : 0,
    averageDailyTurnover: state.dailyReturns.length ? state.totalTurnover / state.dailyReturns.length : 0,
    totalTurnover: state.totalTurnover,
    missingReturnEvents: state.missingReturnEvents,
  };
}

function buildTargetReport({ config, market, scorePath, startDate, rsiMode }) {
  const resolvedScorePath = scorePath || defaultScorePath(config);
  if (!fs.existsSync(resolvedScorePath)) throw new Error(`missing_score_snapshot:${resolvedScorePath}`);
  const score = JSON.parse(fs.readFileSync(resolvedScorePath, 'utf8'));
  return buildDailyRebalanceReport({
    market,
    score,
    startDate,
    rsiMode,
    initialCapital: 10000,
    transactionCostBps: 0,
    slippageBps: 0,
    source: { scorePath },
  });
}

function optionUniverseFromScore(score, market) {
  const marketTickers = new Set(market.tickers);
  return [...collectTickers(score)]
    .filter((ticker) => marketTickers.has(ticker))
    .sort();
}

function selectStrategies(strategyIds) {
  if (!strategyIds?.length || strategyIds.includes('all')) return OPTION_OVERLAY_STRATEGIES;
  const selected = OPTION_OVERLAY_STRATEGIES.filter((strategy) => strategyIds.includes(strategy.id));
  const missing = strategyIds.filter((id) => !OPTION_OVERLAY_STRATEGIES.some((strategy) => strategy.id === id));
  if (missing.length) throw new Error(`unknown_option_overlay_strategies:${missing.join(',')}`);
  return selected;
}

function monthlyReturns(equityCurve) {
  const months = new Map();
  equityCurve.forEach((point) => {
    const month = point.date.slice(0, 7);
    if (!months.has(month)) months.set(month, { startEquity: point.equity / (1 + point.dailyReturn), endEquity: point.equity });
    months.get(month).endEquity = point.equity;
  });
  return [...months.entries()].map(([month, value]) => ({
    month,
    return: value.startEquity > 0 ? (value.endEquity / value.startEquity) - 1 : 0,
  }));
}

async function runOptionOverlaySuite(settings = {}) {
  const config = settings.config || loadConfig();
  const dailyBarsPath = settings.dailyBarsPath || findLatestMassiveEodBarsPath();
  if (!dailyBarsPath || !fs.existsSync(dailyBarsPath)) throw new Error('missing_massive_eod_bars');
  if (!settings.optionFeaturesPath || !fs.existsSync(settings.optionFeaturesPath)) {
    throw new Error('missing_option_features: run npm run pym-v5:build-option-features first');
  }

  const scorePath = settings.scorePath || defaultScorePath(config);
  if (!fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const market = settings.market || readDailyBarsJsonl(dailyBarsPath);
  const startDate = settings.startDate || '2025-01-02';
  const endDate = settings.endDate || market.dates.at(-2);
  const costBps = Number.isFinite(settings.costBps) ? settings.costBps : (
    (config.execution.transactionCostBps || 0) + (config.execution.slippageBps || 0)
  );
  const initialCapital = Number.isFinite(settings.initialCapital) ? settings.initialCapital : 10000;
  const strategies = selectStrategies(settings.strategyIds);
  const targetReport = settings.targetReport || buildTargetReport({
    config,
    market,
    scorePath,
    startDate,
    rsiMode: settings.rsiMode || 'wilder',
  });
  const targetByDate = new Map(targetReport.snapshots.map((snapshot) => [snapshot.date, snapshot]));
  const optionRows = readOptionFeatureJsonl(settings.optionFeaturesPath, { rollingWindow: settings.rollingWindow || 20 });
  const featuresByDate = new Map(optionRows.map((row) => [row.date, row]));
  const optionUniverse = optionUniverseFromScore(score, market);
  const states = strategies.map((strategy) => emptyState(strategy, initialCapital));
  const skippedDays = [];
  let firstSignalIndex = null;
  let lastRealizedIndex = null;

  for (let signalIndex = 0; signalIndex < market.dates.length - 1; signalIndex += 1) {
    const signalDate = market.dates[signalIndex];
    if (signalDate < startDate || signalDate > endDate) continue;
    const targetSnapshot = targetByDate.get(signalDate);
    if (!targetSnapshot) {
      skippedDays.push({ date: signalDate, reason: 'missing_pym_target' });
      continue;
    }
    const optionFeatures = featuresByDate.get(signalDate);
    if (!optionFeatures) {
      skippedDays.push({ date: signalDate, reason: 'missing_option_features' });
      continue;
    }
    states.forEach((state) => simulateDay({
      state,
      market,
      signalIndex,
      signalDate,
      targetSnapshot,
      optionFeatures,
      optionUniverse,
      costBps,
    }));
    if (firstSignalIndex === null) firstSignalIndex = signalIndex;
    lastRealizedIndex = signalIndex + 1;
  }

  const summaries = states.map((state) => summarizeState(state, initialCapital))
    .sort((left, right) => right.totalReturn - left.totalReturn);
  const benchmarks = firstSignalIndex === null ? {} : {
    spy: benchmarkReturn(market, 'SPY', firstSignalIndex, lastRealizedIndex),
    qqq: benchmarkReturn(market, 'QQQ', firstSignalIndex, lastRealizedIndex),
    bil: benchmarkReturn(market, 'BIL', firstSignalIndex, lastRealizedIndex),
  };
  return {
    generatedAt: new Date().toISOString(),
    settings: {
      startDate,
      endDate,
      costBps,
      initialCapital,
      rsiMode: settings.rsiMode || 'wilder',
      timing: 'option_flow_and_pym_signal_at_day_x_eod_then_close_to_close_return_day_x_to_x_plus_1',
      optionRollingWindow: settings.rollingWindow || 20,
    },
    source: {
      dailyBarsPath,
      scorePath,
      optionFeaturesPath: settings.optionFeaturesPath,
      note: 'Uses local Massive option aggregate bars only. No historical Greeks/open-interest files were present locally; gamma-style fields are short-dated ATM option-flow proxies, not true gamma exposure.',
    },
    benchmarks,
    skippedDays,
    summaries,
    strategies: states.map((state) => ({
      summary: summarizeState(state, initialCapital),
      monthlyReturns: monthlyReturns(state.equityCurve),
      daySummaries: state.daySummaries,
      equityCurve: state.equityCurve,
    })),
  };
}

module.exports = {
  DEFAULT_INDEX_ROOTS,
  OPTION_OVERLAY_STRATEGIES,
  cleanWeights,
  indexPutPressure,
  optionMomentumScore,
  rankedOptionRoots,
  runOptionOverlaySuite,
  selectStrategies,
  summarizeState,
};
