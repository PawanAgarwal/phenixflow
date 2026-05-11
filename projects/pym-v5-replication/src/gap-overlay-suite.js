const fs = require('node:fs');

const { readDailyBarsJsonl } = require('./backtest');
const { loadConfig } = require('./config');
const { defaultScorePath, findLatestMassiveEodBarsPath } = require('./rebalance-report');
const { evaluateSymphony } = require('./symphony');

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

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

function cleanWeights(weights) {
  const out = new Map();
  weights.forEach((weight, ticker) => {
    if (isFiniteNumber(weight) && weight > 1e-10) out.set(ticker, weight);
  });
  return out;
}

function weightTurnover(previous, next) {
  const tickers = new Set([...previous.keys(), ...next.keys()]);
  let turnover = 0;
  tickers.forEach((ticker) => {
    turnover += Math.abs((next.get(ticker) || 0) - (previous.get(ticker) || 0));
  });
  return turnover;
}

function barFor(market, date, ticker) {
  return market.byDate.get(date)?.get(ticker) || null;
}

function adjustedClose(market, ticker, index) {
  return market.closes.get(ticker)?.[index] ?? null;
}

function portfolioReturnComponents({ market, weights, index }) {
  const date = market.dates[index];
  let overnightReturn = 0;
  const openLegs = [];
  const missing = [];

  weights.forEach((weight, ticker) => {
    const previousClose = adjustedClose(market, ticker, index - 1);
    const close = adjustedClose(market, ticker, index);
    const bar = barFor(market, date, ticker);
    const open = bar?.open;
    if (
      !isFiniteNumber(previousClose)
      || !isFiniteNumber(open)
      || !isFiniteNumber(close)
      || previousClose <= 0
      || open <= 0
    ) {
      missing.push({ ticker, weight });
      return;
    }
    const openFactor = open / previousClose;
    overnightReturn += weight * (openFactor - 1);
    openLegs.push({
      ticker,
      openValueWeight: weight * openFactor,
      intradayReturn: close / open - 1,
    });
  });

  const openCapital = 1 + overnightReturn;
  let intradayReturn = 0;
  if (openCapital > 0) {
    openLegs.forEach((leg) => {
      intradayReturn += (leg.openValueWeight / openCapital) * leg.intradayReturn;
    });
  }
  return {
    date,
    overnightReturn,
    intradayReturn,
    closeToCloseReturn: ((1 + overnightReturn) * (1 + intradayReturn)) - 1,
    missing,
  };
}

function simulateSpyGapFadeFromDailyBars({ market, index, fillFraction = 1 }) {
  const date = market.dates[index];
  const previousClose = adjustedClose(market, 'SPY', index - 1);
  const spyBar = barFor(market, date, 'SPY');
  if (!spyBar || !isFiniteNumber(previousClose) || previousClose <= 0 || !isFiniteNumber(spyBar.open) || spyBar.open <= 0) {
    return {
      date,
      valid: false,
      reason: 'missing_spy_bar',
      grossReturn: 0,
    };
  }
  const entryPrice = spyBar.open;
  const gapReturn = entryPrice / previousClose - 1;
  const gapBps = gapReturn * 10000;
  const direction = gapReturn < 0 ? 1 : (gapReturn > 0 ? -1 : 0);
  if (!direction) {
    return {
      date,
      valid: true,
      gapReturn,
      gapBps,
      gapDirection: 'flat',
      grossReturn: 0,
      targetHit: false,
    };
  }
  const targetPrice = entryPrice + ((previousClose - entryPrice) * fillFraction);
  const targetHit = direction > 0 ? spyBar.high >= targetPrice : spyBar.low <= targetPrice;
  const exitPrice = targetHit ? targetPrice : spyBar.close;
  const grossReturn = direction * (exitPrice / entryPrice - 1);
  return {
    date,
    valid: true,
    previousClose,
    entryPrice,
    targetPrice,
    exitPrice,
    gapReturn,
    gapBps,
    gapDirection: gapReturn < 0 ? 'gap_down' : 'gap_up',
    direction,
    targetHit,
    grossReturn,
  };
}

function overlayIsActive(variant, gap) {
  if (!gap.valid || !isFiniteNumber(gap.gapBps)) return false;
  if (Math.abs(gap.gapBps) < variant.thresholdBps) return false;
  if (variant.directionMode === 'gap_down') return gap.gapDirection === 'gap_down';
  if (variant.directionMode === 'gap_up') return gap.gapDirection === 'gap_up';
  return gap.gapDirection === 'gap_down' || gap.gapDirection === 'gap_up';
}

function defaultVariants() {
  const variants = [{
    id: 'pym_base',
    name: 'Base PYM',
    kind: 'base',
    sleeve: 0,
    thresholdBps: null,
    fillFraction: null,
    directionMode: 'none',
  }];
  const thresholds = [50, 100];
  const sleeves = [0.05, 0.10, 0.20, 0.30];
  const fillFractions = [0.5, 1];
  const directionModes = ['both', 'gap_down', 'gap_up'];

  thresholds.forEach((thresholdBps) => {
    sleeves.forEach((sleeve) => {
      fillFractions.forEach((fillFraction) => {
        directionModes.forEach((directionMode) => {
          const fillLabel = fillFraction === 1 ? 'full' : 'half';
          variants.push({
            id: `pym_gap_sleeve_${directionMode}_${fillLabel}_t${thresholdBps}_s${Math.round(sleeve * 100)}`,
            name: `PYM ${(1 - sleeve) * 100}% + ${sleeve * 100}% ${directionMode} ${fillLabel} gap sleeve, ${thresholdBps}bps`,
            kind: 'gap_sleeve',
            sleeve,
            thresholdBps,
            fillFraction,
            directionMode,
          });
        });
      });
      ['both', 'gap_up', 'gap_down'].forEach((directionMode) => {
        variants.push({
          id: `pym_cash_throttle_${directionMode}_t${thresholdBps}_s${Math.round(sleeve * 100)}`,
          name: `PYM cash throttle ${directionMode}, ${sleeve * 100}% sleeve, ${thresholdBps}bps`,
          kind: 'cash_throttle',
          sleeve,
          thresholdBps,
          fillFraction: null,
          directionMode,
        });
      });
    });
  });
  return variants;
}

function emptyState(variant, initialCapital) {
  return {
    variant,
    equity: initialCapital,
    dailyReturns: [],
    equityCurve: [],
    dailyRecords: [],
    activeDays: 0,
    gapUpActiveDays: 0,
    gapDownActiveDays: 0,
    totalOverlayCostReturn: 0,
    totalPymTurnover: 0,
    missingReturnEvents: 0,
  };
}

function applyVariantDay({
  state,
  date,
  signalDate,
  baseComponents,
  spyGap,
  pymTurnover,
  totalCostBps,
}) {
  const variant = state.variant;
  const isBase = variant.kind === 'base';
  const active = !isBase && overlayIsActive(variant, spyGap);
  const sleeve = active ? variant.sleeve : 0;
  const gapReturn = active && variant.kind === 'gap_sleeve' ? spyGap.grossReturn : 0;
  const overlayIntradayReturn = variant.kind === 'cash_throttle' ? 0 : gapReturn;
  const intradayReturn = ((1 - sleeve) * baseComponents.intradayReturn) + (sleeve * overlayIntradayReturn);
  const grossReturn = ((1 + baseComponents.overnightReturn) * (1 + intradayReturn)) - 1;
  const pymCostReturn = pymTurnover * totalCostBps / 10000;
  const overlayCostReturn = active ? (2 * sleeve * totalCostBps / 10000) : 0;
  const netReturn = grossReturn - pymCostReturn - overlayCostReturn;
  const startEquity = state.equity;
  state.equity *= (1 + netReturn);
  state.dailyReturns.push(netReturn);
  state.totalPymTurnover += pymTurnover;
  state.totalOverlayCostReturn += overlayCostReturn;
  state.missingReturnEvents += baseComponents.missing.length;
  if (active) {
    state.activeDays += 1;
    if (spyGap.gapDirection === 'gap_up') state.gapUpActiveDays += 1;
    if (spyGap.gapDirection === 'gap_down') state.gapDownActiveDays += 1;
  }
  const record = {
    date,
    signalDate,
    startEquity,
    endEquity: state.equity,
    netReturn,
    grossReturn,
    overnightReturn: baseComponents.overnightReturn,
    pymIntradayReturn: baseComponents.intradayReturn,
    overlayIntradayReturn,
    pymTurnover,
    pymCostReturn,
    overlayCostReturn,
    active,
    sleeve,
    gapBps: spyGap.gapBps ?? null,
    gapDirection: spyGap.gapDirection ?? null,
    gapTargetHit: spyGap.targetHit ?? false,
  };
  state.equityCurve.push({ date, equity: state.equity, dailyReturn: netReturn });
  state.dailyRecords.push(record);
  return record;
}

function summarizeRecords(records, initialCapital) {
  if (!records.length) {
    return {
      tradingDays: 0,
      totalReturn: 0,
      cagr: 0,
      maxDrawdown: 0,
      annualizedVolatility: 0,
      sharpe: 0,
    };
  }
  let finalEquity = initialCapital;
  const equityCurve = [];
  records.forEach((record) => {
    finalEquity *= (1 + record.netReturn);
    equityCurve.push({ equity: finalEquity });
  });
  const totalReturn = finalEquity / initialCapital - 1;
  const returns = records.map((record) => record.netReturn);
  const annualizedVolatility = standardDeviation(returns) * Math.sqrt(252);
  const avgDaily = mean(returns);
  return {
    tradingDays: records.length,
    finalEquity,
    totalReturn,
    totalReturnPct: totalReturn * 100,
    cagr: ((1 + totalReturn) ** (252 / records.length)) - 1,
    maxDrawdown: maxDrawdown(equityCurve),
    annualizedVolatility,
    sharpe: annualizedVolatility > 0 ? (avgDaily * 252) / annualizedVolatility : 0,
    winRate: records.filter((record) => record.netReturn > 0).length / records.length,
  };
}

function summarizeState(state, initialCapital, windows = []) {
  const summary = {
    ...summarizeRecords(state.dailyRecords, initialCapital),
    id: state.variant.id,
    name: state.variant.name,
    kind: state.variant.kind,
    sleeve: state.variant.sleeve,
    thresholdBps: state.variant.thresholdBps,
    fillFraction: state.variant.fillFraction,
    directionMode: state.variant.directionMode,
    activeDays: state.activeDays,
    activeShare: state.dailyRecords.length ? state.activeDays / state.dailyRecords.length : 0,
    gapUpActiveDays: state.gapUpActiveDays,
    gapDownActiveDays: state.gapDownActiveDays,
    averageDailyPymTurnover: state.dailyRecords.length ? state.totalPymTurnover / state.dailyRecords.length : 0,
    averageDailyOverlayCostReturn: state.dailyRecords.length ? state.totalOverlayCostReturn / state.dailyRecords.length : 0,
    missingReturnEvents: state.missingReturnEvents,
  };
  return {
    ...summary,
    windows: windows.map((window) => ({
      ...window,
      summary: summarizeRecords(
        state.dailyRecords.filter((record) => record.date >= window.startDate && record.date <= window.endDate),
        initialCapital,
      ),
    })),
  };
}

function compareToBase(summary, baseSummary) {
  return {
    totalReturnDelta: summary.totalReturn - baseSummary.totalReturn,
    cagrDelta: summary.cagr - baseSummary.cagr,
    sharpeDelta: summary.sharpe - baseSummary.sharpe,
    maxDrawdownDelta: summary.maxDrawdown - baseSummary.maxDrawdown,
  };
}

function enrichComparisons(summaries, baseSummary) {
  return summaries.map((summary) => ({
    ...summary,
    vsBase: compareToBase(summary, baseSummary),
    windows: summary.windows.map((window, index) => ({
      ...window,
      vsBase: compareToBase(window.summary, baseSummary.windows[index].summary),
    })),
  }));
}

function round(value, digits = 6) {
  return isFiniteNumber(value) ? Number(value.toFixed(digits)) : value;
}

function compactSummary(summary) {
  return {
    id: summary.id,
    name: summary.name,
    kind: summary.kind,
    sleeve: summary.sleeve,
    thresholdBps: summary.thresholdBps,
    fillFraction: summary.fillFraction,
    directionMode: summary.directionMode,
    tradingDays: summary.tradingDays,
    activeDays: summary.activeDays,
    totalReturn: round(summary.totalReturn),
    cagr: round(summary.cagr),
    maxDrawdown: round(summary.maxDrawdown),
    sharpe: round(summary.sharpe),
    activeShare: round(summary.activeShare),
    vsBase: summary.vsBase ? {
      totalReturnDelta: round(summary.vsBase.totalReturnDelta),
      cagrDelta: round(summary.vsBase.cagrDelta),
      sharpeDelta: round(summary.vsBase.sharpeDelta),
      maxDrawdownDelta: round(summary.vsBase.maxDrawdownDelta),
    } : null,
  };
}

function defaultWindows(startDate, endDate) {
  return [
    { name: 'train_2025', startDate, endDate: '2025-12-31' },
    { name: 'holdout_2026_ytd', startDate: '2026-01-02', endDate },
  ];
}

function rankByDelta(summaries, windowName = null, limit = 12) {
  return summaries
    .filter((summary) => summary.id !== 'pym_base')
    .map((summary) => {
      if (!windowName) return { summary, score: summary.vsBase.totalReturnDelta };
      const window = summary.windows.find((item) => item.name === windowName);
      return { summary, window, score: window?.vsBase.totalReturnDelta ?? -Infinity };
    })
    .sort((left, right) => right.score - left.score)
    .slice(0, limit);
}

function compactRanked(item) {
  const summary = compactSummary(item.summary);
  if (!item.window) return summary;
  return {
    ...summary,
    rankedWindow: item.window.name,
    windowSummary: {
      totalReturn: round(item.window.summary.totalReturn),
      cagr: round(item.window.summary.cagr),
      maxDrawdown: round(item.window.summary.maxDrawdown),
      sharpe: round(item.window.summary.sharpe),
      vsBase: {
        totalReturnDelta: round(item.window.vsBase.totalReturnDelta),
        cagrDelta: round(item.window.vsBase.cagrDelta),
        sharpeDelta: round(item.window.vsBase.sharpeDelta),
        maxDrawdownDelta: round(item.window.vsBase.maxDrawdownDelta),
      },
    },
  };
}

function runGapOverlaySuite(settings = {}) {
  const config = settings.config || loadConfig();
  const startDate = settings.startDate || config.windows.backtestStartDate || '2025-01-02';
  const endDate = settings.endDate || '2026-05-08';
  const initialCapital = Number.isFinite(settings.initialCapital) ? settings.initialCapital : config.execution.initialCapital;
  const totalCostBps = Number.isFinite(settings.totalCostBps)
    ? settings.totalCostBps
    : (config.execution.transactionCostBps || 0) + (config.execution.slippageBps || 0);
  const rsiMode = settings.rsiMode || 'wilder';
  const scorePath = settings.scorePath || defaultScorePath(config);
  const dailyBarsPath = settings.dailyBarsPath || findLatestMassiveEodBarsPath();
  if (!scorePath || !fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
  if (!dailyBarsPath || !fs.existsSync(dailyBarsPath)) throw new Error(`missing_daily_bars:${dailyBarsPath}`);

  const score = settings.score || JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const market = settings.market || readDailyBarsJsonl(dailyBarsPath);
  const variants = settings.variants || defaultVariants();
  const windows = settings.windows || defaultWindows(startDate, endDate);
  const states = variants.map((variant) => emptyState(variant, initialCapital));
  let previousWeights = new Map();
  const coverage = {
    startDate,
    endDate,
    marketDateMin: market.dates[0] || null,
    marketDateMax: market.dates.at(-1) || null,
    processedDays: 0,
    skippedDays: [],
  };

  for (let index = 1; index < market.dates.length; index += 1) {
    const date = market.dates[index];
    if (date < startDate || date > endDate) continue;
    const signalIndex = index - 1;
    const signalDate = market.dates[signalIndex];
    const weights = cleanWeights(evaluateSymphony(score, market, signalIndex, { rsiMode }));
    const pymTurnover = weightTurnover(previousWeights, weights);
    const baseComponents = portfolioReturnComponents({ market, weights, index });
    if (baseComponents.missing.length) {
      coverage.skippedDays.push({ date, reason: 'missing_pym_open_close', missing: baseComponents.missing.slice(0, 10) });
    }
    states.forEach((state) => {
      const fillFraction = state.variant.fillFraction || 1;
      const spyGap = simulateSpyGapFadeFromDailyBars({ market, index, fillFraction });
      applyVariantDay({
        state,
        date,
        signalDate,
        baseComponents,
        spyGap,
        pymTurnover,
        totalCostBps,
      });
    });
    previousWeights = weights;
    coverage.processedDays += 1;
    if (settings.onProgress && coverage.processedDays % 25 === 0) {
      settings.onProgress({ date, processedDays: coverage.processedDays });
    }
  }

  const rawSummaries = states.map((state) => summarizeState(state, initialCapital, windows));
  const baseSummary = rawSummaries.find((summary) => summary.id === 'pym_base');
  const summaries = enrichComparisons(rawSummaries, baseSummary)
    .sort((left, right) => right.totalReturn - left.totalReturn);
  const enrichedBase = summaries.find((summary) => summary.id === 'pym_base');
  return {
    generatedAt: new Date().toISOString(),
    settings: {
      startDate,
      endDate,
      initialCapital,
      totalCostBps,
      rsiMode,
      timing: 'base_pym_close_to_close_with_open_known_gap_overlay',
      source: {
        scorePath,
        dailyBarsPath,
      },
    },
    coverage,
    baseSummary: enrichedBase,
    summaries,
    compactResults: {
      base: compactSummary(enrichedBase),
      topFullWindowDelta: rankByDelta(summaries, null, 12).map(compactRanked),
      topHoldoutDelta: rankByDelta(summaries, 'holdout_2026_ytd', 12).map(compactRanked),
      topTrainDelta: rankByDelta(summaries, 'train_2025', 12).map(compactRanked),
    },
    strategies: states.map((state) => ({
      summary: summaries.find((summary) => summary.id === state.variant.id),
      dailyRecords: state.dailyRecords,
      equityCurve: state.equityCurve,
    })),
  };
}

module.exports = {
  cleanWeights,
  compareToBase,
  defaultVariants,
  overlayIsActive,
  portfolioReturnComponents,
  runGapOverlaySuite,
  simulateSpyGapFadeFromDailyBars,
  summarizeRecords,
  weightTurnover,
};
