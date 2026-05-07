function maxDrawdown(equityCurve) {
  let peak = 1;
  let drawdown = 0;
  equityCurve.forEach((point) => {
    peak = Math.max(peak, point.equity);
    drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function predictionDate(prediction) {
  return prediction.tradeDate || prediction.row?.tradeDate || '';
}

function predictionMinute(prediction) {
  return prediction.minuteUtc || prediction.row?.minuteUtc || '';
}

function predictionMinuteOfDay(prediction) {
  return prediction.minuteOfDayEt ?? prediction.row?.minuteOfDayEt ?? null;
}

function predictionRowId(prediction) {
  return prediction.rowId || prediction.row?.rowId;
}

function horizonPolicy(horizonName) {
  if (horizonName === 'next_5m') return { mode: 'step', stepMinutes: 5, note: 'sampled every 5 minutes to reduce overlapping forward-return windows' };
  if (horizonName === 'next_60m') return { mode: 'step', stepMinutes: 60, note: 'sampled every 60 minutes to reduce overlapping forward-return windows' };
  if (horizonName === 'eod_close') return { mode: 'daily-last', note: 'one EOD trade per day, using the latest eligible prediction row per day' };
  if (horizonName === 'last_30m') return { mode: 'daily-first', note: 'one last-30m trade per day, using the first eligible entry row per day' };
  return { mode: 'all', stepMinutes: 1, note: 'uses every prediction row' };
}

function selectPredictionsByHorizon(predictions, horizonName) {
  const policy = horizonPolicy(horizonName);
  const sorted = predictions
    .slice()
    .sort((left, right) => {
      const leftDate = predictionDate(left);
      const rightDate = predictionDate(right);
      const leftMinute = predictionMinute(left);
      const rightMinute = predictionMinute(right);
      if (leftDate !== rightDate) return leftDate.localeCompare(rightDate);
      return leftMinute.localeCompare(rightMinute);
    });
  if (policy.mode === 'all') return { selected: sorted, policy };

  const selected = [];
  const byDay = new Map();
  sorted.forEach((prediction) => {
    const day = predictionDate(prediction);
    const list = byDay.get(day) || [];
    list.push(prediction);
    byDay.set(day, list);
  });

  byDay.forEach((dayPredictions) => {
    if (policy.mode === 'daily-first') {
      selected.push(dayPredictions[0]);
      return;
    }
    if (policy.mode === 'daily-last') {
      selected.push(dayPredictions[dayPredictions.length - 1]);
      return;
    }
    let lastSelectedMinute = null;
    dayPredictions.forEach((prediction) => {
      const minute = predictionMinuteOfDay(prediction);
      if (!Number.isFinite(minute)) {
        if (lastSelectedMinute === null) selected.push(prediction);
        return;
      }
      if (lastSelectedMinute === null || minute - lastSelectedMinute >= policy.stepMinutes) {
        selected.push(prediction);
        lastSelectedMinute = minute;
      }
    });
  });
  return { selected, policy };
}

function computePolicyBacktest(predictions, {
  confidenceThreshold = 0.52,
  transactionCostBps = 1,
  slippageBps = 1,
  horizonName = 'next_1m',
} = {}) {
  const { selected: sorted, policy } = selectPredictionsByHorizon(predictions, horizonName);
  let equity = 1;
  let buyHold = 1;
  let previousPosition = 0;
  let turnover = 0;
  const costRate = (transactionCostBps + slippageBps) / 10_000;
  const equityCurve = [];

  sorted.forEach((prediction) => {
    let position = 0;
    if (prediction.directionProbability >= confidenceThreshold) position = 1;
    if (prediction.directionProbability <= 1 - confidenceThreshold) position = -1;
    const tradeSize = Math.abs(position - previousPosition);
    const cost = tradeSize * costRate;
    turnover += tradeSize;
    const strategyReturn = (position * prediction.actualReturn) - cost;
    equity *= (1 + strategyReturn);
    buyHold *= (1 + prediction.actualReturn);
    equityCurve.push({
      rowId: predictionRowId(prediction),
      tradeDate: predictionDate(prediction),
      minuteUtc: predictionMinute(prediction),
      position,
      actualReturn: prediction.actualReturn,
      strategyReturn,
      equity,
      buyHold,
    });
    previousPosition = position;
  });

  return {
    observations: sorted.length,
    inputObservations: predictions.length,
    horizonName,
    executionPolicy: policy,
    confidenceThreshold,
    transactionCostBps,
    slippageBps,
    finalEquity: equity,
    totalReturn: equity - 1,
    buyAndHoldReturn: buyHold - 1,
    excessReturn: (equity - 1) - (buyHold - 1),
    maxDrawdown: maxDrawdown(equityCurve),
    turnover,
    averageTurnover: sorted.length ? turnover / sorted.length : 0,
    longShare: sorted.length ? equityCurve.filter((row) => row.position === 1).length / sorted.length : 0,
    shortShare: sorted.length ? equityCurve.filter((row) => row.position === -1).length / sorted.length : 0,
    cashShare: sorted.length ? equityCurve.filter((row) => row.position === 0).length / sorted.length : 0,
    equityCurve,
    notes: [
      'Secondary policy check only; accuracy metrics rank models first.',
      policy.note,
    ],
  };
}

module.exports = {
  horizonPolicy,
  selectPredictionsByHorizon,
  computePolicyBacktest,
};
