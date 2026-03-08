function toFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function normalizeRight(value) {
  if (typeof value !== 'string') return null;
  const normalized = value.trim().toUpperCase();
  if (normalized === 'CALL' || normalized === 'C') return 'CALL';
  if (normalized === 'PUT' || normalized === 'P') return 'PUT';
  return null;
}

function parseUtcMs(isoTs) {
  const parsed = Date.parse(isoTs);
  return Number.isFinite(parsed) ? parsed : null;
}

const MAX_BUCKET_CACHE_SIZE = 50000;
const minuteBucketCache = new Map();
const etClockCache = new Map();
const etClockFormatter = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  hour12: false,
  hour: '2-digit',
  minute: '2-digit',
});

function maybeClearCache(cache) {
  if (cache.size >= MAX_BUCKET_CACHE_SIZE) {
    cache.clear();
  }
}

function tryExtractUtcMinuteKey(isoTs) {
  if (typeof isoTs !== 'string') return null;
  const value = isoTs.trim();
  if (
    value.length >= 17
    && value[4] === '-'
    && value[7] === '-'
    && value[10] === 'T'
    && value[13] === ':'
    && value.endsWith('Z')
  ) {
    return value.slice(0, 16);
  }
  return null;
}

function computeValue(price, size) {
  const p = toFiniteNumber(price);
  const s = toFiniteNumber(size);
  if (p === null || s === null) return null;
  return p * s * 100;
}

function computeDte(tradeTsUtc, expirationDate) {
  if (typeof expirationDate !== 'string' || !expirationDate.trim()) return null;
  const tradeMs = parseUtcMs(tradeTsUtc);
  if (tradeMs === null) return null;

  const expirationIso = expirationDate.length === 10
    ? `${expirationDate}T21:00:00.000Z`
    : expirationDate;

  const expirationMs = parseUtcMs(expirationIso);
  if (expirationMs === null) return null;

  return Math.ceil((expirationMs - tradeMs) / 86400000);
}

function computeExecutionFlags({
  right,
  price,
  bid,
  ask,
  lastTradePrice = null,
  lastExecutionSide = null,
}) {
  const normalizedRight = normalizeRight(right);
  const tradePrice = toFiniteNumber(price);
  const tradeBid = toFiniteNumber(bid);
  const tradeAsk = toFiniteNumber(ask);
  const previousTradePrice = toFiniteNumber(lastTradePrice);
  const previousExecution = typeof lastExecutionSide === 'string'
    ? lastExecutionSide.trim().toUpperCase()
    : null;

  const hasQuotes = tradePrice !== null && tradeBid !== null && tradeAsk !== null;
  const spread = hasQuotes ? tradeAsk - tradeBid : null;
  const aaThreshold = hasQuotes ? tradeAsk + Math.max(0.01, 0.10 * spread) : null;
  const mid = hasQuotes ? ((tradeAsk + tradeBid) / 2) : null;

  const isAA = hasQuotes ? tradePrice >= aaThreshold : false;
  const isAsk = hasQuotes ? tradePrice >= tradeAsk && !isAA : false;
  const isBid = hasQuotes ? tradePrice <= tradeBid : false;
  const isInsideSpread = hasQuotes && !isAA && !isAsk && !isBid && tradePrice > tradeBid && tradePrice < tradeAsk;

  let executionSide = 'OTHER';
  if (isAA) executionSide = 'AA';
  else if (isAsk) executionSide = 'ASK';
  else if (isBid) executionSide = 'BID';
  else if (isInsideSpread && mid !== null) {
    if (tradePrice > mid) executionSide = 'ASK';
    else if (tradePrice < mid) executionSide = 'BID';
    else if (previousTradePrice !== null) {
      if (tradePrice > previousTradePrice) executionSide = 'ASK';
      else if (tradePrice < previousTradePrice) executionSide = 'BID';
      else if (previousExecution === 'ASK' || previousExecution === 'AA') executionSide = 'ASK';
      else if (previousExecution === 'BID') executionSide = 'BID';
    } else if (previousExecution === 'ASK' || previousExecution === 'AA') {
      executionSide = 'ASK';
    } else if (previousExecution === 'BID') {
      executionSide = 'BID';
    }
  }

  return {
    calls: normalizedRight === 'CALL',
    puts: normalizedRight === 'PUT',
    bid: isBid,
    ask: isAsk,
    aa: isAA,
    executionSide,
  };
}

function computeSentiment({ right, executionSide }) {
  const normalizedRight = normalizeRight(right);
  if (!normalizedRight) return 'neutral';

  if ((normalizedRight === 'CALL' && (executionSide === 'ASK' || executionSide === 'AA'))
    || (normalizedRight === 'PUT' && executionSide === 'BID')) {
    return 'bullish';
  }

  if ((normalizedRight === 'PUT' && (executionSide === 'ASK' || executionSide === 'AA'))
    || (normalizedRight === 'CALL' && executionSide === 'BID')) {
    return 'bearish';
  }

  return 'neutral';
}

function isStandardMonthly(expirationDate) {
  if (typeof expirationDate !== 'string' || expirationDate.length < 10) return false;

  const dt = new Date(`${expirationDate.slice(0, 10)}T12:00:00.000Z`);
  if (Number.isNaN(dt.getTime())) return false;

  const dayOfWeek = dt.getUTCDay();
  const dayOfMonth = dt.getUTCDate();
  return dayOfWeek === 5 && dayOfMonth >= 15 && dayOfMonth <= 21;
}

function computeSpot(rawPayload = {}) {
  const candidateKeys = [
    'underlying_price',
    'underlyingPrice',
    'underlying',
    'spot',
    'underlier',
    'underlier_price',
    'stock_price',
    'stockPrice',
  ];

  for (const key of candidateKeys) {
    const value = toFiniteNumber(rawPayload[key]);
    if (value !== null) return value;
  }

  return null;
}

function computeOtmPct({ right, strike, spot }) {
  const normalizedRight = normalizeRight(right);
  const s = toFiniteNumber(strike);
  const sp = toFiniteNumber(spot);

  if (!normalizedRight || s === null || sp === null || sp <= 0) return null;

  if (normalizedRight === 'CALL') {
    return ((s - sp) / sp) * 100;
  }

  return ((sp - s) / sp) * 100;
}

const SWEEP_CODES = new Set(['95', '126', '128']);

function isSweep(conditionCode) {
  if (conditionCode === null || conditionCode === undefined) return false;
  return SWEEP_CODES.has(String(conditionCode).trim());
}

function isMultilegByCode(conditionCode) {
  if (conditionCode === null || conditionCode === undefined) return false;
  const code = Number(String(conditionCode).trim());
  return Number.isFinite(code) && code >= 130 && code <= 143;
}

function computeOtmNormBellCurve(otmPct) {
  if (otmPct === null || otmPct === undefined || !Number.isFinite(otmPct)) return 0;
  if (otmPct <= 0) return 0;
  return Math.exp(-Math.pow((otmPct - 10) / 10, 2));
}

function computeMinuteOfDayEt(isoTs) {
  if (isoTs === null || isoTs === undefined) return null;
  const et = getEtClock(isoTs);
  if (!et) return null;
  return (et.hour * 60) + et.minute;
}

function computeTimeNorm(minuteOfDayEt) {
  if (minuteOfDayEt === null || minuteOfDayEt === undefined || !Number.isFinite(minuteOfDayEt)) return 0;
  const marketOpen = 570;  // 9:30 ET
  const marketClose = 960; // 16:00 ET
  if (minuteOfDayEt < marketOpen || minuteOfDayEt > marketClose) return 0;
  const peak = 645; // 10:45 ET
  const sigma = 45;
  return Math.exp(-Math.pow((minuteOfDayEt - peak) / sigma, 2));
}

function computeIvSkewNorm(callIv, putIv) {
  if (callIv === null || callIv === undefined || putIv === null || putIv === undefined) return 0;
  const c = toFiniteNumber(callIv);
  const p = toFiniteNumber(putIv);
  if (c === null || p === null) return 0;
  const avg = (c + p) / 2;
  if (avg <= 0) return 0;
  return Math.min(1, Math.abs(c - p) / avg);
}

function clamp01(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  if (n <= 0) return 0;
  if (n >= 1) return 1;
  return n;
}

function computeDteSwingNorm(dte) {
  const value = toFiniteNumber(dte);
  if (value === null || value < 0) return 0;
  if (value <= 2) return 0.3;
  const center = 35;
  const sigma = 28;
  return Math.exp(-Math.pow((value - center) / sigma, 2));
}

function computeFlowImbalanceNorm(signedPremium, totalPremium) {
  const signed = toFiniteNumber(signedPremium);
  const total = toFiniteNumber(totalPremium);
  if (signed === null || total === null || total <= 0) return 0;
  return clamp01(Math.abs(signed) / total);
}

function computeDeltaPressureNorm(signedDeltaNotional, totalDeltaNotional) {
  const signed = toFiniteNumber(signedDeltaNotional);
  const total = toFiniteNumber(totalDeltaNotional);
  if (signed === null || total === null || total <= 0) return 0;
  return clamp01(Math.abs(signed) / total);
}

function computeCpOiPressureNorm(callPressure, putPressure) {
  const calls = toFiniteNumber(callPressure);
  const puts = toFiniteNumber(putPressure);
  if (calls === null || puts === null) return 0;
  const denom = Math.abs(calls) + Math.abs(puts);
  if (denom <= 0) return 0;
  return clamp01(Math.abs(calls - puts) / denom);
}

function computeIvTermSlopeNorm(frontIv, backIv) {
  const front = toFiniteNumber(frontIv);
  const back = toFiniteNumber(backIv);
  if (front === null || back === null || back <= 0) return 0;
  const slope = (front - back) / back;
  return clamp01(0.5 + (0.5 * Math.tanh(slope * 2.5)));
}

function computeUnderlyingTrendConfirmNorm(sentiment, signedTrendSignal) {
  const signal = toFiniteNumber(signedTrendSignal);
  if (signal === null || !sentiment) return 0;
  if (sentiment !== 'bullish' && sentiment !== 'bearish') return 0;
  return clamp01(0.5 + (0.5 * Math.tanh(signal * 1.5)));
}

function computeDeltaProxy({ right, strike, spot, dte }) {
  const normalizedRight = normalizeRight(right);
  const strikeValue = toFiniteNumber(strike);
  const spotValue = toFiniteNumber(spot);
  const dteValue = toFiniteNumber(dte);
  if (!normalizedRight || strikeValue === null || spotValue === null || spotValue <= 0) return null;

  const signedMoneyness = normalizedRight === 'CALL'
    ? ((spotValue - strikeValue) / spotValue)
    : ((strikeValue - spotValue) / spotValue);
  const boundedDte = dteValue === null ? 30 : Math.max(0, Math.min(120, dteValue));
  const timeBoost = 1 - (boundedDte / 120);
  const absDelta = clamp01(0.5 + (0.45 * Math.tanh((signedMoneyness * 12) + (0.8 * timeBoost))));
  const signedDelta = normalizedRight === 'CALL' ? absDelta : -absDelta;
  return Number(signedDelta.toFixed(6));
}

function computeLiquidityQualityNorm({ price, bid, ask, executionSide }) {
  const tradePrice = toFiniteNumber(price);
  const tradeBid = toFiniteNumber(bid);
  const tradeAsk = toFiniteNumber(ask);
  const side = typeof executionSide === 'string' ? executionSide.toUpperCase() : 'OTHER';

  if (tradePrice === null || tradeBid === null || tradeAsk === null || tradeAsk <= tradeBid) {
    if (side === 'AA') return 0.9;
    if (side === 'ASK' || side === 'BID') return 0.75;
    return 0.2;
  }

  const mid = (tradeAsk + tradeBid) / 2;
  const spreadPct = ((tradeAsk - tradeBid) / Math.max(Math.abs(mid), 0.0001)) * 100;
  const spreadQuality = clamp01(1 - (spreadPct / 12));
  const location = clamp01((tradePrice - tradeBid) / Math.max(tradeAsk - tradeBid, 0.0001));
  const edgeLocation = Math.max(location, 1 - location);
  const sideBoost = side === 'AA' ? 1 : (side === 'ASK' || side === 'BID' ? 0.85 : 0.5);
  return clamp01((spreadQuality * 0.6) + (edgeLocation * 0.25) + (sideBoost * 0.15));
}

function computeValueShockNorm(value, baseline = {}) {
  const current = toFiniteNumber(value);
  if (current === null || current <= 0) return 0;

  const mean = toFiniteNumber(baseline.mean);
  const std = toFiniteNumber(baseline.std);
  if (mean !== null && std !== null && std > 0) {
    const z = (Math.log1p(current) - mean) / std;
    return clamp01(0.5 + (0.5 * Math.tanh(z / 2)));
  }

  const median = toFiniteNumber(baseline.median);
  const mad = toFiniteNumber(baseline.mad);
  if (median !== null && mad !== null && mad > 0) {
    const robustZ = (current - median) / (1.4826 * mad);
    return clamp01(0.5 + (0.5 * Math.tanh(robustZ / 2)));
  }

  const min = toFiniteNumber(baseline.min);
  const max = toFiniteNumber(baseline.max);
  if (min !== null && max !== null && max > min) {
    return clamp01((current - min) / (max - min));
  }

  return 0;
}

function normalizeModel(model) {
  if (typeof model !== 'string') return 'v4_expanded';
  const normalized = model.trim().toLowerCase();
  if (normalized === 'v1' || normalized === 'baseline' || normalized === 'v1_baseline') return 'v1_baseline';
  if (normalized === 'v4' || normalized === 'expanded' || normalized === 'v4_expanded') return 'v4_expanded';
  if (normalized === 'v5' || normalized === 'swing' || normalized === 'v5_swing') return 'v5_swing';
  return 'v4_expanded';
}

function computeSigScore({
  valuePctile,
  valueShockNorm,
  volOiNorm,
  repeatNorm,
  otmNorm,
  sideConfidence,
  dteNorm,
  dteSwingNorm,
  spreadNorm,
  liquidityQualityNorm,
  sweepNorm,
  multilegNorm,
  multilegPenaltyNorm,
  timeNorm,
  deltaNorm,
  ivSkewNorm,
  flowImbalanceNorm,
  deltaPressureNorm,
  cpOiPressureNorm,
  ivSkewSurfaceNorm,
  ivTermSlopeNorm,
  underlyingTrendConfirmNorm,
  model = 'v4_expanded',
  weights = null,
  availability = null,
  returnDetails = false,
}) {
  const normalizedModel = normalizeModel(model);
  const componentValues = {
    valuePctile: clamp01(valuePctile),
    valueShockNorm: clamp01(valueShockNorm),
    volOiNorm: clamp01(volOiNorm),
    repeatNorm: clamp01(repeatNorm),
    otmNorm: clamp01(otmNorm),
    sideConfidence: clamp01(sideConfidence),
    dteNorm: clamp01(dteNorm),
    dteSwingNorm: clamp01(dteSwingNorm),
    spreadNorm: clamp01(spreadNorm),
    liquidityQualityNorm: clamp01(liquidityQualityNorm),
    sweepNorm: clamp01(sweepNorm),
    multilegNorm: clamp01(multilegNorm),
    multilegPenaltyNorm: clamp01(multilegPenaltyNorm),
    timeNorm: clamp01(timeNorm),
    deltaNorm: clamp01(deltaNorm),
    ivSkewNorm: clamp01(ivSkewNorm),
    flowImbalanceNorm: clamp01(flowImbalanceNorm),
    deltaPressureNorm: clamp01(deltaPressureNorm),
    cpOiPressureNorm: clamp01(cpOiPressureNorm),
    ivSkewSurfaceNorm: clamp01(ivSkewSurfaceNorm),
    ivTermSlopeNorm: clamp01(ivTermSlopeNorm),
    underlyingTrendConfirmNorm: clamp01(underlyingTrendConfirmNorm),
  };

  const componentAvailability = Object.keys(componentValues).reduce((acc, key) => {
    if (availability && Object.prototype.hasOwnProperty.call(availability, key)) {
      acc[key] = Boolean(availability[key]);
      return acc;
    }
    const raw = {
      valuePctile,
      valueShockNorm,
      volOiNorm,
      repeatNorm,
      otmNorm,
      sideConfidence,
      dteNorm,
      dteSwingNorm,
      spreadNorm,
      liquidityQualityNorm,
      sweepNorm,
      multilegNorm,
      multilegPenaltyNorm,
      timeNorm,
      deltaNorm,
      ivSkewNorm,
      flowImbalanceNorm,
      deltaPressureNorm,
      cpOiPressureNorm,
      ivSkewSurfaceNorm,
      ivTermSlopeNorm,
      underlyingTrendConfirmNorm,
    }[key];
    acc[key] = toFiniteNumber(raw) !== null;
    return acc;
  }, {});

  if (normalizedModel === 'v1_baseline') {
    const baseline = {
      valuePctile: 0.35,
      volOiNorm: 0.25,
      repeatNorm: 0.20,
      otmNorm: 0.10,
      sideConfidence: 0.10,
      ...(weights || {}),
    };

    const score = (baseline.valuePctile * componentValues.valuePctile)
      + (baseline.volOiNorm * componentValues.volOiNorm)
      + (baseline.repeatNorm * componentValues.repeatNorm)
      + (baseline.otmNorm * componentValues.otmNorm)
      + (baseline.sideConfidence * componentValues.sideConfidence);
    const normalizedScore = Number(Math.min(1, Math.max(0, score)).toFixed(6));
    const unavailableComponents = Object.keys(baseline).filter((key) => !componentAvailability[key]);
    const totalAbsWeight = Object.values(baseline).reduce((acc, weight) => acc + Math.abs(weight), 0);
    const usedAbsWeight = Object.entries(baseline).reduce((acc, [key, weight]) => (
      componentAvailability[key] ? acc + Math.abs(weight) : acc
    ), 0);
    if (!returnDetails) return normalizedScore;
    return {
      score: normalizedScore,
      model: normalizedModel,
      usedAbsWeight,
      totalAbsWeight,
      unavailableComponents,
      components: baseline,
    };
  }

  const expandedWeights = {
    valuePctile: 0.18,
    volOiNorm: 0.15,
    repeatNorm: 0.08,
    otmNorm: 0.08,
    sideConfidence: 0.06,
    dteNorm: 0.04,
    spreadNorm: 0.04,
    sweepNorm: 0.12,
    multilegNorm: -0.12,
    timeNorm: 0.07,
    deltaNorm: 0.08,
    ivSkewNorm: 0.06,
    ...(weights || {}),
  };

  if (normalizedModel === 'v4_expanded') {
    const score = (expandedWeights.valuePctile * componentValues.valuePctile)
      + (expandedWeights.volOiNorm * componentValues.volOiNorm)
      + (expandedWeights.repeatNorm * componentValues.repeatNorm)
      + (expandedWeights.otmNorm * componentValues.otmNorm)
      + (expandedWeights.sideConfidence * componentValues.sideConfidence)
      + (expandedWeights.dteNorm * componentValues.dteNorm)
      + (expandedWeights.spreadNorm * componentValues.spreadNorm)
      + (expandedWeights.sweepNorm * componentValues.sweepNorm)
      + (expandedWeights.multilegNorm * componentValues.multilegNorm)
      + (expandedWeights.timeNorm * componentValues.timeNorm)
      + (expandedWeights.deltaNorm * componentValues.deltaNorm)
      + (expandedWeights.ivSkewNorm * componentValues.ivSkewNorm);
    const normalizedScore = Number(Math.min(1, Math.max(0, score)).toFixed(6));
    const unavailableComponents = Object.keys(expandedWeights).filter((key) => !componentAvailability[key]);
    const totalAbsWeight = Object.values(expandedWeights).reduce((acc, weight) => acc + Math.abs(weight), 0);
    const usedAbsWeight = Object.entries(expandedWeights).reduce((acc, [key, weight]) => (
      componentAvailability[key] ? acc + Math.abs(weight) : acc
    ), 0);
    if (!returnDetails) return normalizedScore;
    return {
      score: normalizedScore,
      model: normalizedModel,
      usedAbsWeight,
      totalAbsWeight,
      unavailableComponents,
      components: expandedWeights,
    };
  }

  const swingWeights = {
    valueShockNorm: 0.10,
    volOiNorm: 0.10,
    repeatNorm: 0.06,
    otmNorm: 0.05,
    dteSwingNorm: 0.06,
    flowImbalanceNorm: 0.12,
    deltaPressureNorm: 0.12,
    cpOiPressureNorm: 0.08,
    ivSkewSurfaceNorm: 0.08,
    ivTermSlopeNorm: 0.06,
    underlyingTrendConfirmNorm: 0.10,
    liquidityQualityNorm: 0.07,
    sweepNorm: 0.06,
    multilegPenaltyNorm: -0.08,
    ...(weights || {}),
  };

  let weightedSum = 0;
  let usedAbsWeight = 0;
  const unavailableComponents = [];
  const contributions = {};

  Object.entries(swingWeights).forEach(([key, weight]) => {
    const absWeight = Math.abs(weight);
    if (absWeight <= 0) return;
    if (!componentAvailability[key]) {
      unavailableComponents.push(key);
      return;
    }
    const value = componentValues[key];
    const weighted = weight * value;
    weightedSum += weighted;
    usedAbsWeight += absWeight;
    contributions[key] = {
      weight,
      value,
      weighted,
    };
  });

  const totalAbsWeight = Object.values(swingWeights).reduce((acc, weight) => acc + Math.abs(weight), 0);
  const renormalized = usedAbsWeight > 0 ? (weightedSum / usedAbsWeight) : 0;
  const normalizedScore = Number(Math.min(1, Math.max(0, renormalized)).toFixed(6));
  if (!returnDetails) return normalizedScore;
  return {
    score: normalizedScore,
    model: normalizedModel,
    usedAbsWeight,
    totalAbsWeight,
    unavailableComponents,
    components: contributions,
  };
}

function toMinuteBucketUtc(isoTs) {
  const minuteKey = tryExtractUtcMinuteKey(isoTs);
  if (minuteKey) {
    const cached = minuteBucketCache.get(minuteKey);
    if (cached) return cached;
    maybeClearCache(minuteBucketCache);
    const minuteBucket = `${minuteKey}:00.000Z`;
    minuteBucketCache.set(minuteKey, minuteBucket);
    return minuteBucket;
  }

  const ms = parseUtcMs(isoTs);
  if (ms === null) return null;
  const floored = ms - (ms % 60000);
  const flooredKey = String(floored);
  const cached = minuteBucketCache.get(flooredKey);
  if (cached) return cached;
  maybeClearCache(minuteBucketCache);
  const minuteBucket = new Date(floored).toISOString();
  minuteBucketCache.set(flooredKey, minuteBucket);
  return minuteBucket;
}

function getEtClock(isoTs) {
  let cacheKey = tryExtractUtcMinuteKey(isoTs);
  let dt = null;
  if (cacheKey) {
    const cached = etClockCache.get(cacheKey);
    if (cached) return cached;
    dt = new Date(`${cacheKey}:00.000Z`);
  } else {
    const ms = parseUtcMs(isoTs);
    if (ms === null) return null;
    const floored = ms - (ms % 60000);
    cacheKey = String(floored);
    const cached = etClockCache.get(cacheKey);
    if (cached) return cached;
    dt = new Date(floored);
  }

  if (!dt || Number.isNaN(dt.getTime())) return null;
  const parts = etClockFormatter.formatToParts(dt);
  const byType = {};
  parts.forEach((part) => {
    byType[part.type] = part.value;
  });

  const resolved = {
    hour: Number(byType.hour),
    minute: Number(byType.minute),
    second: 0,
  };
  maybeClearCache(etClockCache);
  etClockCache.set(cacheKey, resolved);
  return resolved;
}

function isAmSpikeWindow(isoTs) {
  const et = getEtClock(isoTs);
  if (!et) return false;

  const minuteOfDay = (et.hour * 60) + et.minute;
  const start = (9 * 60) + 30;
  const end = (10 * 60) + 30;
  return minuteOfDay >= start && minuteOfDay <= end;
}

module.exports = {
  toFiniteNumber,
  normalizeRight,
  computeValue,
  computeDte,
  computeExecutionFlags,
  computeSentiment,
  isStandardMonthly,
  computeSpot,
  computeOtmPct,
  computeSigScore,
  toMinuteBucketUtc,
  getEtClock,
  isAmSpikeWindow,
  isSweep,
  isMultilegByCode,
  computeOtmNormBellCurve,
  computeMinuteOfDayEt,
  computeTimeNorm,
  computeIvSkewNorm,
  computeDteSwingNorm,
  computeFlowImbalanceNorm,
  computeDeltaPressureNorm,
  computeCpOiPressureNorm,
  computeIvTermSlopeNorm,
  computeUnderlyingTrendConfirmNorm,
  computeDeltaProxy,
  computeLiquidityQualityNorm,
  computeValueShockNorm,
};
