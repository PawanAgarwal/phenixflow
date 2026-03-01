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

function computeExecutionFlags({ right, price, bid, ask }) {
  const normalizedRight = normalizeRight(right);
  const tradePrice = toFiniteNumber(price);
  const tradeBid = toFiniteNumber(bid);
  const tradeAsk = toFiniteNumber(ask);

  const hasQuotes = tradePrice !== null && tradeBid !== null && tradeAsk !== null;
  const spread = hasQuotes ? tradeAsk - tradeBid : null;
  const aaThreshold = hasQuotes ? tradeAsk + Math.max(0.01, 0.10 * spread) : null;

  const isAA = hasQuotes ? tradePrice >= aaThreshold : false;
  const isAsk = hasQuotes ? tradePrice >= tradeAsk && !isAA : false;
  const isBid = hasQuotes ? tradePrice <= tradeBid : false;

  let executionSide = 'OTHER';
  if (isAA) executionSide = 'AA';
  else if (isAsk) executionSide = 'ASK';
  else if (isBid) executionSide = 'BID';

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

function computeSigScore({
  valuePctile,
  volOiNorm,
  repeatNorm,
  otmNorm,
  sideConfidence,
  dteNorm,
  spreadNorm,
  sweepNorm,
  multilegNorm,
  timeNorm,
  deltaNorm,
  ivSkewNorm,
  model = 'v4_expanded',
  weights = null,
}) {
  const vp = Math.min(1, Math.max(0, valuePctile || 0));
  const vo = Math.min(1, Math.max(0, volOiNorm || 0));
  const rp = Math.min(1, Math.max(0, repeatNorm || 0));
  const op = Math.min(1, Math.max(0, otmNorm || 0));
  const sc = Math.min(1, Math.max(0, sideConfidence || 0));
  const dn = Math.min(1, Math.max(0, dteNorm || 0));
  const sn = Math.min(1, Math.max(0, spreadNorm || 0));
  const sw = Math.min(1, Math.max(0, sweepNorm || 0));
  const ml = Math.min(1, Math.max(0, multilegNorm || 0));
  const tn = Math.min(1, Math.max(0, timeNorm || 0));
  const dl = Math.min(1, Math.max(0, deltaNorm || 0));
  const iv = Math.min(1, Math.max(0, ivSkewNorm || 0));

  if (model === 'v1_baseline') {
    const baseline = {
      valuePctile: 0.35,
      volOiNorm: 0.25,
      repeatNorm: 0.20,
      otmNorm: 0.10,
      sideConfidence: 0.10,
      ...(weights || {}),
    };

    const score = (baseline.valuePctile * vp)
      + (baseline.volOiNorm * vo)
      + (baseline.repeatNorm * rp)
      + (baseline.otmNorm * op)
      + (baseline.sideConfidence * sc);
    return Number(Math.min(1, Math.max(0, score)).toFixed(6));
  }

  const expanded = {
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

  const score = (expanded.valuePctile * vp)
    + (expanded.volOiNorm * vo)
    + (expanded.repeatNorm * rp)
    + (expanded.otmNorm * op)
    + (expanded.sideConfidence * sc)
    + (expanded.dteNorm * dn)
    + (expanded.spreadNorm * sn)
    + (expanded.sweepNorm * sw)
    + (expanded.multilegNorm * ml)
    + (expanded.timeNorm * tn)
    + (expanded.deltaNorm * dl)
    + (expanded.ivSkewNorm * iv);
  return Number(Math.min(1, Math.max(0, score)).toFixed(6));
}

function toMinuteBucketUtc(isoTs) {
  const ms = parseUtcMs(isoTs);
  if (ms === null) return null;
  const floored = ms - (ms % 60000);
  return new Date(floored).toISOString();
}

function getEtClock(isoTs) {
  const dt = new Date(isoTs);
  if (Number.isNaN(dt.getTime())) return null;

  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    hour12: false,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });

  const parts = formatter.formatToParts(dt);
  const byType = {};
  parts.forEach((part) => {
    byType[part.type] = part.value;
  });

  return {
    year: Number(byType.year),
    month: Number(byType.month),
    day: Number(byType.day),
    hour: Number(byType.hour),
    minute: Number(byType.minute),
    second: Number(byType.second),
  };
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
};
