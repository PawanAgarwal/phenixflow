const fs = require('node:fs');
const readline = require('node:readline');

const { selectPredictionsByHorizon } = require('./backtest');

const ET_FORMATTER = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
});

function round(value, digits = 6) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function resultVariantKey(result) {
  return `${result.trainMode}|${result.phase2Strategy}|${result.baseStrategy}|${result.experiment}|${result.horizon}`;
}

function resultFamilyKey(result) {
  return `${result.phase2Strategy}|${result.baseStrategy}|${result.horizon}`;
}

function minuteOfDayEt(minuteUtc) {
  const parts = ET_FORMATTER.formatToParts(new Date(minuteUtc));
  const hour = Number(parts.find((part) => part.type === 'hour')?.value);
  const minute = Number(parts.find((part) => part.type === 'minute')?.value);
  if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
  return (hour * 60) + minute;
}

function hydratePrediction(prediction) {
  const minute = prediction.minuteOfDayEt ?? minuteOfDayEt(prediction.minuteUtc);
  return {
    ...prediction,
    row: {
      rowId: prediction.rowId,
      tradeDate: prediction.tradeDate,
      minuteUtc: prediction.minuteUtc,
      minuteOfDayEt: minute,
      spy_close: prediction.spyClose,
    },
    minuteOfDayEt: minute,
  };
}

async function readPredictionsByResultKey(filePath, wantedKeys) {
  const out = new Map();
  wantedKeys.forEach((key) => out.set(key, []));
  const reader = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });
  for await (const line of reader) {
    if (!line.trim()) continue;
    const prediction = JSON.parse(line);
    if (out.has(prediction.resultKey)) out.get(prediction.resultKey).push(hydratePrediction(prediction));
  }
  return out;
}

function positionFor(prediction, confidenceThreshold, positionMode) {
  let position = 0;
  if (prediction.directionProbability >= confidenceThreshold) position = 1;
  if (prediction.directionProbability <= 1 - confidenceThreshold) position = -1;
  if (positionMode === 'long_cash' && position < 0) return 0;
  return position;
}

function predictionLookup(predictions) {
  const out = new Map();
  predictions.forEach((prediction) => {
    const minute = prediction.minuteOfDayEt;
    if (Number.isFinite(minute)) out.set(`${prediction.tradeDate}|${minute}`, prediction);
  });
  return out;
}

function compoundedReturn(returns) {
  return returns.reduce((equity, value) => equity * (1 + value), 1) - 1;
}

function maxDrawdown(equityCurve) {
  let peak = 1;
  let drawdown = 0;
  equityCurve.forEach((point) => {
    peak = Math.max(peak, point.equity);
    drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function dayReturns(equityCurve) {
  const grouped = new Map();
  equityCurve.forEach((point) => {
    const list = grouped.get(point.tradeDate) || [];
    list.push(point.strategyReturn);
    grouped.set(point.tradeDate, list);
  });
  return [...grouped.entries()]
    .map(([tradeDate, returns]) => ({
      tradeDate,
      return: compoundedReturn(returns),
      trades: returns.length,
    }))
    .sort((left, right) => left.tradeDate.localeCompare(right.tradeDate));
}

function contributionStats(equityCurve) {
  const daily = dayReturns(equityCurve);
  const bestDay = daily.slice().sort((left, right) => right.return - left.return)[0] || null;
  const worstDay = daily.slice().sort((left, right) => left.return - right.return)[0] || null;
  const withoutBest = bestDay
    ? compoundedReturn(daily.filter((day) => day.tradeDate !== bestDay.tradeDate).map((day) => day.return))
    : null;
  const positiveSum = daily
    .filter((day) => day.return > 0)
    .reduce((sum, day) => sum + day.return, 0);
  const bestDayShareOfPositive = bestDay && positiveSum > 0 ? bestDay.return / positiveSum : null;
  return {
    dayCount: daily.length,
    bestDay: bestDay ? { ...bestDay, return: round(bestDay.return) } : null,
    worstDay: worstDay ? { ...worstDay, return: round(worstDay.return) } : null,
    returnWithoutBestDay: round(withoutBest),
    bestDayShareOfPositive: round(bestDayShareOfPositive),
  };
}

function runValidationBacktest(predictions, {
  confidenceThreshold,
  transactionCostBps,
  slippageBps,
  horizonName,
  positionMode = 'long_short',
  delayRows = 0,
} = {}) {
  const { selected, policy } = selectPredictionsByHorizon(predictions, horizonName);
  const delayedLookup = delayRows > 0 ? predictionLookup(predictions) : null;
  const costRate = (transactionCostBps + slippageBps) / 10_000;
  const equityCurve = [];
  let equity = 1;
  let previousPosition = 0;
  let turnover = 0;
  let skippedForDelay = 0;

  selected.forEach((prediction) => {
    let actualReturn = prediction.actualReturn;
    if (delayRows > 0) {
      const delayed = delayedLookup.get(`${prediction.tradeDate}|${prediction.minuteOfDayEt + delayRows}`);
      if (!delayed) {
        skippedForDelay += 1;
        return;
      }
      actualReturn = delayed.actualReturn;
    }
    const position = positionFor(prediction, confidenceThreshold, positionMode);
    const tradeSize = Math.abs(position - previousPosition);
    const strategyReturn = (position * actualReturn) - (tradeSize * costRate);
    turnover += tradeSize;
    equity *= (1 + strategyReturn);
    equityCurve.push({
      rowId: prediction.rowId,
      tradeDate: prediction.tradeDate,
      minuteUtc: prediction.minuteUtc,
      position,
      actualReturn,
      strategyReturn,
      equity,
    });
    previousPosition = position;
  });

  const observations = equityCurve.length;
  return {
    observations,
    inputObservations: predictions.length,
    skippedForDelay,
    executionPolicy: policy,
    confidenceThreshold,
    transactionCostBps,
    slippageBps,
    positionMode,
    delayRows,
    totalReturn: equity - 1,
    maxDrawdown: maxDrawdown(equityCurve),
    turnover,
    averageTurnover: observations ? turnover / observations : 0,
    longShare: observations ? equityCurve.filter((row) => row.position === 1).length / observations : 0,
    shortShare: observations ? equityCurve.filter((row) => row.position === -1).length / observations : 0,
    cashShare: observations ? equityCurve.filter((row) => row.position === 0).length / observations : 0,
    equityCurve,
  };
}

function compactBacktest(result) {
  return {
    observations: result.observations,
    inputObservations: result.inputObservations,
    skippedForDelay: result.skippedForDelay,
    confidenceThreshold: round(result.confidenceThreshold),
    transactionCostBps: round(result.transactionCostBps),
    slippageBps: round(result.slippageBps),
    positionMode: result.positionMode,
    delayRows: result.delayRows,
    totalReturn: round(result.totalReturn),
    maxDrawdown: round(result.maxDrawdown),
    turnover: round(result.turnover),
    averageTurnover: round(result.averageTurnover),
    longShare: round(result.longShare),
    shortShare: round(result.shortShare),
    cashShare: round(result.cashShare),
  };
}

function gateValue(prediction, result) {
  if (result.phase2Strategy === 'volatility_gated' && Number.isFinite(prediction.magnitudeProbability)) {
    return prediction.magnitudeProbability;
  }
  if (result.phase2Strategy === 'enhanced_meta_labeling' && Number.isFinite(prediction.metaAcceptProbability)) {
    return prediction.metaAcceptProbability;
  }
  return prediction.confidence;
}

function stricterThresholdPredictions(predictions, result, threshold) {
  return predictions.map((prediction) => {
    const accepted = gateValue(prediction, result) >= threshold;
    if (accepted) return { ...prediction, accepted };
    return {
      ...prediction,
      directionProbability: 0.5,
      predictedReturn: 0,
      predictedDirection: 0,
      confidence: 0.5,
      accepted: false,
    };
  });
}

function thresholdGrid(result) {
  const selected = result.selectedPolicy?.selectedThreshold;
  if (!Number.isFinite(selected)) return [];
  return [...new Set([selected, selected + 0.05, selected + 0.1]
    .filter((value) => value <= 0.95)
    .map((value) => Number(value.toFixed(4))))];
}

function validateMonthlyResult(result, predictions, config) {
  const confidenceThreshold = result.backtest?.confidenceThreshold ?? config.execution.confidenceThreshold;
  const common = {
    confidenceThreshold,
    horizonName: result.horizon,
  };
  const defaultBacktest = runValidationBacktest(predictions, {
    ...common,
    transactionCostBps: config.execution.transactionCostBps,
    slippageBps: config.execution.slippageBps,
  });
  const doubleCost = runValidationBacktest(predictions, {
    ...common,
    transactionCostBps: 2,
    slippageBps: 2,
  });
  const highCost = runValidationBacktest(predictions, {
    ...common,
    transactionCostBps: 5,
    slippageBps: 5,
  });
  const longOnly = runValidationBacktest(predictions, {
    ...common,
    transactionCostBps: config.execution.transactionCostBps,
    slippageBps: config.execution.slippageBps,
    positionMode: 'long_cash',
  });
  const delayed = result.horizon.startsWith('next_')
    ? runValidationBacktest(predictions, {
      ...common,
      transactionCostBps: config.execution.transactionCostBps,
      slippageBps: config.execution.slippageBps,
      delayRows: 1,
    })
    : null;
  const thresholds = thresholdGrid(result).map((threshold) => {
    const thresholdPredictions = stricterThresholdPredictions(predictions, result, threshold);
    return {
      threshold,
      backtest: compactBacktest(runValidationBacktest(thresholdPredictions, {
        ...common,
        transactionCostBps: config.execution.transactionCostBps,
        slippageBps: config.execution.slippageBps,
      })),
    };
  });

  return {
    resultKey: result.resultKey,
    split: result.split,
    selectedThreshold: result.selectedPolicy?.selectedThreshold ?? null,
    defaultBacktest: compactBacktest(defaultBacktest),
    doubleCost: compactBacktest(doubleCost),
    highCost: compactBacktest(highCost),
    longOnly: compactBacktest(longOnly),
    delayedOneMinute: delayed ? compactBacktest(delayed) : null,
    contribution: contributionStats(defaultBacktest.equityCurve),
    thresholds,
  };
}

function aggregateMonthly(months) {
  function totalFor(selector) {
    return compoundedReturn(months
      .map(selector)
      .filter((value) => Number.isFinite(value)));
  }
  const defaultTotal = totalFor((month) => month.defaultBacktest.totalReturn);
  const doubleCostTotal = totalFor((month) => month.doubleCost.totalReturn);
  const highCostTotal = totalFor((month) => month.highCost.totalReturn);
  const longOnlyTotal = totalFor((month) => month.longOnly.totalReturn);
  const delayedMonths = months.filter((month) => month.delayedOneMinute && month.delayedOneMinute.observations > 0);
  const delayedTotal = delayedMonths.length
    ? compoundedReturn(delayedMonths.map((month) => month.delayedOneMinute.totalReturn))
    : null;
  const returnWithoutBestDayTotal = compoundedReturn(months
    .map((month) => month.contribution.returnWithoutBestDay)
    .filter((value) => Number.isFinite(value)));
  const strictThresholdTotals = new Map();
  months.forEach((month) => {
    month.thresholds.forEach((item) => {
      const list = strictThresholdTotals.get(item.threshold) || [];
      list.push(item.backtest.totalReturn);
      strictThresholdTotals.set(item.threshold, list);
    });
  });
  const thresholdStability = [...strictThresholdTotals.entries()].map(([threshold, returns]) => ({
    threshold,
    monthCount: returns.length,
    totalReturn: round(compoundedReturn(returns)),
    positiveMonths: returns.filter((value) => value > 0).length,
  }));

  return {
    months: months.length,
    positiveMonths: months.filter((month) => month.defaultBacktest.totalReturn > 0).length,
    doubleCostPositiveMonths: months.filter((month) => month.doubleCost.totalReturn > 0).length,
    highCostPositiveMonths: months.filter((month) => month.highCost.totalReturn > 0).length,
    longOnlyPositiveMonths: months.filter((month) => month.longOnly.totalReturn > 0).length,
    delayedPositiveMonths: delayedMonths.filter((month) => month.delayedOneMinute.totalReturn > 0).length,
    defaultTotalReturn: round(defaultTotal),
    doubleCostTotalReturn: round(doubleCostTotal),
    highCostTotalReturn: round(highCostTotal),
    longOnlyTotalReturn: round(longOnlyTotal),
    delayedOneMinuteTotalReturn: round(delayedTotal),
    returnWithoutBestDayTotal: round(returnWithoutBestDayTotal),
    thresholdStability,
  };
}

function validationVerdict(summary) {
  const selectedThreshold = summary.thresholdStability[0];
  const stricterThreshold = summary.thresholdStability[1];
  const thresholdStable = !stricterThreshold || stricterThreshold.totalReturn > 0;
  const delayedOk = summary.delayedOneMinuteTotalReturn === null || summary.delayedOneMinuteTotalReturn > 0;
  const passed = (
    summary.positiveMonths >= 2
    && summary.doubleCostPositiveMonths >= 2
    && summary.defaultTotalReturn > 0
    && summary.doubleCostTotalReturn > 0
    && summary.returnWithoutBestDayTotal > 0
    && thresholdStable
    && delayedOk
  );
  return {
    promoteToPaper: passed,
    positiveMonthsPass: summary.positiveMonths >= 2,
    doubleCostPass: summary.doubleCostPositiveMonths >= 2 && summary.doubleCostTotalReturn > 0,
    singleDayPass: summary.returnWithoutBestDayTotal > 0,
    thresholdStable,
    delayedPass: delayedOk,
    selectedThresholdTotalReturn: selectedThreshold?.totalReturn ?? null,
    stricterThresholdTotalReturn: stricterThreshold?.totalReturn ?? null,
  };
}

function familySummary(variants) {
  return variants.reduce((families, variant) => {
    const list = families.get(variant.familyKey) || [];
    list.push(variant);
    families.set(variant.familyKey, list);
    return families;
  }, new Map());
}

async function validatePhase2Signals({ reportPath, predictionsPath, config }) {
  const report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  const promisingKeys = new Set(report.promising.map((item) => item.key));
  const candidateResults = report.results.filter((result) => promisingKeys.has(resultVariantKey(result)));
  const wantedResultKeys = new Set(candidateResults.map((result) => result.resultKey));
  const predictionsByKey = await readPredictionsByResultKey(predictionsPath || report.predictionsPath, wantedResultKeys);
  const byVariant = new Map();

  candidateResults.forEach((result) => {
    const key = resultVariantKey(result);
    const list = byVariant.get(key) || [];
    const predictions = predictionsByKey.get(result.resultKey) || [];
    list.push({
      result,
      validation: validateMonthlyResult(result, predictions, config),
    });
    byVariant.set(key, list);
  });

  const variants = [...byVariant.entries()].map(([variantKey, items]) => {
    const first = items[0].result;
    const monthly = items
      .map((item) => item.validation)
      .sort((left, right) => left.split.localeCompare(right.split));
    const summary = aggregateMonthly(monthly);
    return {
      variantKey,
      familyKey: resultFamilyKey(first),
      trainMode: first.trainMode,
      phase2Strategy: first.phase2Strategy,
      baseStrategy: first.baseStrategy,
      experiment: first.experiment,
      horizon: first.horizon,
      summary,
      verdict: validationVerdict(summary),
      monthly,
    };
  }).sort((left, right) => {
    if (Number(right.verdict.promoteToPaper) !== Number(left.verdict.promoteToPaper)) {
      return Number(right.verdict.promoteToPaper) - Number(left.verdict.promoteToPaper);
    }
    return right.summary.defaultTotalReturn - left.summary.defaultTotalReturn;
  });

  const families = [...familySummary(variants).entries()].map(([familyKey, familyVariants]) => ({
    familyKey,
    variantCount: familyVariants.length,
    promotedVariantCount: familyVariants.filter((variant) => variant.verdict.promoteToPaper).length,
    bestVariant: familyVariants
      .slice()
      .sort((left, right) => right.summary.defaultTotalReturn - left.summary.defaultTotalReturn)[0]?.variantKey,
    bestDefaultTotalReturn: round(Math.max(...familyVariants.map((variant) => variant.summary.defaultTotalReturn))),
    variants: familyVariants.map((variant) => variant.variantKey),
  })).sort((left, right) => right.bestDefaultTotalReturn - left.bestDefaultTotalReturn);

  return {
    generatedAt: new Date().toISOString(),
    reportPath,
    predictionsPath: predictionsPath || report.predictionsPath,
    candidateSignalVariantCount: variants.length,
    candidateSignalFamilyCount: families.length,
    promotionRule: 'positive in at least 2 holdout months, survives 2+2 bps stress, remains positive without best day, and survives nearby stricter threshold/delay checks',
    promotedVariantCount: variants.filter((variant) => variant.verdict.promoteToPaper).length,
    families,
    variants,
  };
}

module.exports = {
  aggregateMonthly,
  contributionStats,
  hydratePrediction,
  readPredictionsByResultKey,
  resultFamilyKey,
  resultVariantKey,
  runValidationBacktest,
  stricterThresholdPredictions,
  validatePhase2Signals,
  validationVerdict,
};
