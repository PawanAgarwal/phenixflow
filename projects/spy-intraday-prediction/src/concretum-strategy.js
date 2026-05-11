const DEFAULT_CONCRETUM_PARAMS = {
  initialAum: 100000,
  commissionPerShare: 0.0035,
  minCommissionPerOrder: 0.35,
  bandMult: 1,
  tradeFreq: 30,
  targetVol: 0.02,
  maxLeverage: 4,
  sigmaLookbackDays: 14,
  dailyVolLookbackDays: 15,
};

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
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

function normalizeParams(params = {}) {
  return { ...DEFAULT_CONCRETUM_PARAMS, ...params };
}

function computeVwap(rows) {
  let cumulativeVolume = 0;
  let cumulativeDollarVolume = 0;
  return rows.map((row) => {
    const typicalPrice = (row.high + row.low + row.close) / 3;
    const volume = Math.max(0, row.volume || 0);
    cumulativeVolume += volume;
    cumulativeDollarVolume += typicalPrice * volume;
    return cumulativeVolume > 0 ? cumulativeDollarVolume / cumulativeVolume : typicalPrice;
  });
}

function buildSigmaByMinute(moveOpenHistoryByMinute, lookbackDays) {
  const sigmaByMinute = new Map();
  for (const [minute, history] of moveOpenHistoryByMinute.entries()) {
    if (history.length < lookbackDays) continue;
    sigmaByMinute.set(Number(minute), mean(history.slice(-lookbackDays)));
  }
  return sigmaByMinute;
}

function updateMoveOpenHistory(moveOpenHistoryByMinute, rows, maxRetained = 128) {
  if (!rows.length || !isFiniteNumber(rows[0].open) || rows[0].open <= 0) return;
  const open = rows[0].open;
  for (const row of rows) {
    if (!isFiniteNumber(row.close) || !isFiniteNumber(row.minFromOpen)) continue;
    const moveOpen = Math.abs(row.close / open - 1);
    const history = moveOpenHistoryByMinute.get(row.minFromOpen) || [];
    history.push(moveOpen);
    if (history.length > maxRetained) history.splice(0, history.length - maxRetained);
    moveOpenHistoryByMinute.set(row.minFromOpen, history);
  }
}

function computeConcretumBands({ rows, previousClose, sigmaByMinute, bandMult }) {
  if (!rows.length || !isFiniteNumber(previousClose)) return [];
  const open = rows[0].open;
  const upperAnchor = Math.max(open, previousClose);
  const lowerAnchor = Math.min(open, previousClose);
  return rows.map((row) => {
    const sigmaOpen = sigmaByMinute.get(row.minFromOpen);
    if (!isFiniteNumber(sigmaOpen)) {
      return { upperBand: null, lowerBand: null, sigmaOpen: null };
    }
    return {
      upperBand: upperAnchor * (1 + (bandMult * sigmaOpen)),
      lowerBand: lowerAnchor * (1 - (bandMult * sigmaOpen)),
      sigmaOpen,
    };
  });
}

function computeMomentumSignals({ rows, previousClose, sigmaByMinute, params }) {
  const vwap = computeVwap(rows);
  const bands = computeConcretumBands({
    rows,
    previousClose,
    sigmaByMinute,
    bandMult: params.bandMult,
  });
  const signals = rows.map((row, index) => {
    const band = bands[index];
    if (!band || !isFiniteNumber(band.upperBand) || !isFiniteNumber(band.lowerBand)) return 0;
    if (row.close > band.upperBand && row.close > vwap[index]) return 1;
    if (row.close < band.lowerBand && row.close < vwap[index]) return -1;
    return 0;
  });
  return { vwap, bands, signals };
}

function buildMomentumExposure(rows, signals, tradeFreq) {
  const rebalanceExposure = Array(rows.length).fill(null);
  rows.forEach((row, index) => {
    if (row.minFromOpen % tradeFreq === 0) {
      rebalanceExposure[index] = signals[index] || 0;
    }
  });

  let lastExposure = null;
  const filledExposure = rebalanceExposure.map((value) => {
    if (value !== null) lastExposure = value;
    return lastExposure;
  });

  return filledExposure.map((_, index) => {
    if (index === 0) return 0;
    return filledExposure[index - 1] ?? 0;
  });
}

function countExposureTrades(exposure) {
  if (!exposure.length) return 0;
  let trades = 0;
  let previous = exposure[0] || 0;
  for (let index = 1; index < exposure.length; index += 1) {
    const current = exposure[index] || 0;
    trades += Math.abs(current - previous);
    previous = current;
  }
  trades += Math.abs(previous);
  return trades;
}

function simulateConcretumDay({
  date,
  rows,
  previousClose,
  previousAum,
  dailyVol,
  sigmaByMinute,
  overnightThreshold,
  params: rawParams = {},
}) {
  const params = normalizeParams(rawParams);
  const baseRecord = {
    date,
    ret: null,
    aum: previousAum,
    retSpy: null,
    close: rows.length ? rows[rows.length - 1].close : null,
    skippedReason: null,
    shares: 0,
    leverage: 0,
    dailyVol,
    overnightMove: null,
    gapSignal: 0,
    gapGrossPnl: 0,
    gapCommission: 0,
    momentumGrossPnl: 0,
    momentumCommission: 0,
    netPnl: 0,
    tradesCount: 0,
    longSignalCount: 0,
    shortSignalCount: 0,
  };

  if (!rows.length) return { ...baseRecord, skippedReason: 'missing_rows' };
  const openPrice = rows[0].open;
  const closePrice = rows[rows.length - 1].close;
  if (!isFiniteNumber(previousClose) || previousClose <= 0) {
    return { ...baseRecord, skippedReason: 'missing_previous_close' };
  }
  const retSpy = closePrice / previousClose - 1;
  const recordWithBenchmark = { ...baseRecord, retSpy, close: closePrice };
  if (!isFiniteNumber(sigmaByMinute.get(rows[0].minFromOpen))) {
    return { ...recordWithBenchmark, skippedReason: 'warmup_sigma' };
  }
  if (!isFiniteNumber(dailyVol) || dailyVol <= 0) {
    return { ...recordWithBenchmark, skippedReason: 'warmup_daily_vol' };
  }
  const leverage = Math.min(params.targetVol / dailyVol, params.maxLeverage);
  const shares = Math.round((previousAum / openPrice) * leverage);
  if (!Number.isFinite(shares) || shares <= 0) {
    return { ...recordWithBenchmark, skippedReason: 'zero_shares', leverage };
  }

  const { signals } = computeMomentumSignals({ rows, previousClose, sigmaByMinute, params });
  const exposure = buildMomentumExposure(rows, signals, params.tradeFreq);
  const tradesCount = countExposureTrades(exposure);
  let momentumGrossPnl = 0;
  for (let index = 1; index < rows.length; index += 1) {
    momentumGrossPnl += (exposure[index] || 0) * (rows[index].close - rows[index - 1].close) * shares;
  }

  const commissionPerUnit = Math.max(params.minCommissionPerOrder, params.commissionPerShare * shares);
  const momentumCommission = tradesCount * commissionPerUnit;
  const overnightMove = openPrice / previousClose - 1;
  const gapSignal = Math.abs(overnightMove) > overnightThreshold ? -Math.sign(overnightMove) : 0;
  const gapExitRow = rows.find((row) => row.minFromOpen === params.tradeFreq);
  const gapGrossPnl = gapSignal && gapExitRow
    ? gapSignal * shares * (gapExitRow.close - openPrice)
    : 0;
  const gapCommission = gapSignal && gapExitRow ? 2 * commissionPerUnit : 0;
  const netPnl = momentumGrossPnl - momentumCommission + gapGrossPnl - gapCommission;
  const ret = netPnl / previousAum;
  const aum = previousAum + netPnl;

  return {
    ...recordWithBenchmark,
    ret,
    aum,
    shares,
    leverage,
    overnightMove,
    gapSignal,
    gapGrossPnl,
    gapCommission,
    momentumGrossPnl,
    momentumCommission,
    netPnl,
    tradesCount,
    longSignalCount: signals.filter((signal) => signal > 0).length,
    shortSignalCount: signals.filter((signal) => signal < 0).length,
  };
}

module.exports = {
  DEFAULT_CONCRETUM_PARAMS,
  mean,
  sampleStd,
  normalizeParams,
  computeVwap,
  buildSigmaByMinute,
  updateMoveOpenHistory,
  computeConcretumBands,
  computeMomentumSignals,
  buildMomentumExposure,
  countExposureTrades,
  simulateConcretumDay,
};
