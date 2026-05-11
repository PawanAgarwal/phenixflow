function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function round(value, digits = 6) {
  return isFiniteNumber(value) ? Number(value.toFixed(digits)) : value;
}

function mean(values) {
  const finite = values.filter(isFiniteNumber);
  if (!finite.length) return null;
  return finite.reduce((sum, value) => sum + value, 0) / finite.length;
}

function sampleStd(values) {
  const finite = values.filter(isFiniteNumber);
  if (finite.length < 2) return null;
  const avg = mean(finite);
  const variance = finite.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (finite.length - 1);
  return Math.sqrt(variance);
}

function resolveExitIndex(rows, maxHoldMinutes) {
  if (!rows.length) return -1;
  if (maxHoldMinutes === 'eod') return rows.length - 1;
  const parsed = Number(maxHoldMinutes);
  if (!Number.isFinite(parsed)) return rows.length - 1;
  let exitIndex = rows.length - 1;
  for (let index = 0; index < rows.length; index += 1) {
    if (rows[index].minFromOpen >= parsed) {
      exitIndex = index;
      break;
    }
  }
  return exitIndex;
}

function targetWasHit(row, direction, targetPrice) {
  if (direction > 0) return row.high >= targetPrice;
  if (direction < 0) return row.low <= targetPrice;
  return false;
}

function findTargetHit(rows, { entryIndex = 0, exitIndex, direction, targetPrice }) {
  for (let index = entryIndex; index <= exitIndex && index < rows.length; index += 1) {
    if (targetWasHit(rows[index], direction, targetPrice)) {
      return { index, row: rows[index] };
    }
  }
  return null;
}

function fillTargetPrice({ entryPrice, previousClose, fillFraction }) {
  return entryPrice + ((previousClose - entryPrice) * fillFraction);
}

function simulateOpeningGapFillDay({
  date,
  rows,
  previousClose,
  thresholdBps = 0,
  fillFraction = 1,
  maxHoldMinutes = 30,
  costBpsPerSide = 0,
  slippageBpsPerSide = 0,
  entryDelayMinutes = 0,
}) {
  const base = {
    date,
    traded: false,
    skippedReason: null,
    previousClose: isFiniteNumber(previousClose) ? previousClose : null,
    entryPrice: null,
    exitPrice: null,
    targetPrice: null,
    direction: 0,
    gapDirection: 'flat',
    gapReturn: null,
    gapBps: null,
    thresholdBps,
    fillFraction,
    maxHoldMinutes,
    entryDelayMinutes,
    targetHit: false,
    exitReason: null,
    entryMinuteFromOpen: null,
    exitMinuteFromOpen: null,
    minutesHeld: null,
    grossReturn: 0,
    netReturn: 0,
    roundTripCostReturn: 0,
    spyOpenToExitReturn: null,
    spyCloseToCloseReturn: null,
  };

  if (!rows.length) return { ...base, skippedReason: 'missing_rows' };
  const closePrice = rows[rows.length - 1].close;
  const openPrice = rows[0].open;
  if (!isFiniteNumber(previousClose) || previousClose <= 0) {
    return { ...base, skippedReason: 'missing_previous_close' };
  }
  if (!isFiniteNumber(openPrice) || openPrice <= 0) {
    return { ...base, skippedReason: 'bad_open' };
  }

  const gapReturn = openPrice / previousClose - 1;
  const gapBps = gapReturn * 10000;
  const benchmark = {
    gapReturn,
    gapBps,
    spyCloseToCloseReturn: isFiniteNumber(closePrice) ? closePrice / previousClose - 1 : null,
  };
  if (Math.abs(gapBps) < thresholdBps) {
    return {
      ...base,
      ...benchmark,
      skippedReason: 'below_gap_threshold',
      gapDirection: gapBps > 0 ? 'gap_up' : (gapBps < 0 ? 'gap_down' : 'flat'),
    };
  }

  const entryIndex = Math.min(Math.max(0, Number(entryDelayMinutes) || 0), rows.length - 1);
  const entryRow = rows[entryIndex];
  const entryPrice = entryRow.open;
  const targetPrice = fillTargetPrice({ entryPrice, previousClose, fillFraction });
  const direction = targetPrice > entryPrice ? 1 : (targetPrice < entryPrice ? -1 : 0);
  if (!direction) {
    return {
      ...base,
      ...benchmark,
      skippedReason: 'flat_gap_after_entry',
      entryPrice,
      targetPrice,
    };
  }

  const exitIndex = resolveExitIndex(rows, maxHoldMinutes);
  if (exitIndex < entryIndex) {
    return {
      ...base,
      ...benchmark,
      skippedReason: 'missing_exit_row',
      entryPrice,
      targetPrice,
      direction,
    };
  }

  const hit = findTargetHit(rows, {
    entryIndex,
    exitIndex,
    direction,
    targetPrice,
  });
  const exitRow = hit ? hit.row : rows[exitIndex];
  const exitPrice = hit ? targetPrice : exitRow.close;
  const grossReturn = direction * (exitPrice / entryPrice - 1);
  const roundTripCostReturn = (2 * (costBpsPerSide + slippageBpsPerSide)) / 10000;
  const netReturn = grossReturn - roundTripCostReturn;

  return {
    ...base,
    ...benchmark,
    traded: true,
    previousClose,
    entryPrice,
    exitPrice,
    targetPrice,
    direction,
    gapDirection: gapBps > 0 ? 'gap_up' : 'gap_down',
    targetHit: Boolean(hit),
    exitReason: hit ? 'gap_target_hit' : 'time_exit',
    entryMinuteFromOpen: entryRow.minFromOpen,
    exitMinuteFromOpen: exitRow.minFromOpen,
    minutesHeld: Math.max(0, exitRow.minFromOpen - entryRow.minFromOpen + 1),
    grossReturn,
    netReturn,
    roundTripCostReturn,
    spyOpenToExitReturn: exitPrice / entryPrice - 1,
  };
}

function summarizeReturnSeries(records, key = 'netReturn') {
  const returns = records.map((record) => record[key]).filter(isFiniteNumber);
  if (!returns.length) {
    return {
      observations: 0,
      totalReturn: null,
      annualizedReturn: null,
      annualizedVolatility: null,
      sharpe: null,
      maxDrawdown: null,
    };
  }

  let equity = 1;
  let peak = 1;
  let maxDrawdown = 0;
  for (const ret of returns) {
    equity *= 1 + ret;
    if (equity > peak) peak = equity;
    maxDrawdown = Math.min(maxDrawdown, equity / peak - 1);
  }
  const avg = mean(returns);
  const vol = sampleStd(returns);
  return {
    observations: returns.length,
    totalReturn: equity - 1,
    annualizedReturn: (equity ** (252 / returns.length)) - 1,
    annualizedVolatility: isFiniteNumber(vol) ? vol * Math.sqrt(252) : null,
    sharpe: isFiniteNumber(vol) && vol > 0 ? (avg / vol) * Math.sqrt(252) : null,
    maxDrawdown,
  };
}

function summarizeGapFillRecords(records) {
  const traded = records.filter((record) => record.traded);
  const wins = traded.filter((record) => record.netReturn > 0);
  const grossWins = traded.filter((record) => record.grossReturn > 0);
  const targetHits = traded.filter((record) => record.targetHit);
  const gapUps = traded.filter((record) => record.gapDirection === 'gap_up');
  const gapDowns = traded.filter((record) => record.gapDirection === 'gap_down');
  const benchmark = summarizeReturnSeries(records, 'spyCloseToCloseReturn');
  const activeNet = traded.map((record) => record.netReturn).filter(isFiniteNumber);
  const activeGross = traded.map((record) => record.grossReturn).filter(isFiniteNumber);

  return {
    ...summarizeReturnSeries(records, 'netReturn'),
    benchmark,
    reportDays: records.length,
    tradeDays: traded.length,
    skippedDays: records.length - traded.length,
    targetHitRate: traded.length ? targetHits.length / traded.length : null,
    winRateNet: traded.length ? wins.length / traded.length : null,
    winRateGross: traded.length ? grossWins.length / traded.length : null,
    avgNetReturnActive: mean(activeNet),
    avgGrossReturnActive: mean(activeGross),
    avgGapBpsActive: mean(traded.map((record) => Math.abs(record.gapBps))),
    gapUpTrades: gapUps.length,
    gapDownTrades: gapDowns.length,
    gapUpHitRate: gapUps.length
      ? gapUps.filter((record) => record.targetHit).length / gapUps.length
      : null,
    gapDownHitRate: gapDowns.length
      ? gapDowns.filter((record) => record.targetHit).length / gapDowns.length
      : null,
  };
}

function compactSummary(summary) {
  return {
    observations: summary.observations,
    reportDays: summary.reportDays,
    tradeDays: summary.tradeDays,
    totalReturn: round(summary.totalReturn),
    annualizedReturn: round(summary.annualizedReturn),
    annualizedVolatility: round(summary.annualizedVolatility),
    sharpe: round(summary.sharpe),
    maxDrawdown: round(summary.maxDrawdown),
    targetHitRate: round(summary.targetHitRate),
    winRateNet: round(summary.winRateNet),
    avgNetReturnActive: round(summary.avgNetReturnActive),
    avgGapBpsActive: round(summary.avgGapBpsActive),
    gapUpTrades: summary.gapUpTrades,
    gapDownTrades: summary.gapDownTrades,
    gapUpHitRate: round(summary.gapUpHitRate),
    gapDownHitRate: round(summary.gapDownHitRate),
    benchmark: summary.benchmark ? {
      observations: summary.benchmark.observations,
      totalReturn: round(summary.benchmark.totalReturn),
      annualizedReturn: round(summary.benchmark.annualizedReturn),
      annualizedVolatility: round(summary.benchmark.annualizedVolatility),
      sharpe: round(summary.benchmark.sharpe),
      maxDrawdown: round(summary.benchmark.maxDrawdown),
    } : undefined,
  };
}

module.exports = {
  compactSummary,
  fillTargetPrice,
  findTargetHit,
  isFiniteNumber,
  resolveExitIndex,
  round,
  sampleStd,
  simulateOpeningGapFillDay,
  summarizeGapFillRecords,
  summarizeReturnSeries,
  targetWasHit,
};
