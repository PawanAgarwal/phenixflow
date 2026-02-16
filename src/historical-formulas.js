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

function computeSigScore({ valuePctile, volOiNorm, repeatNorm, otmNorm, sideConfidence }) {
  const vp = Math.min(1, Math.max(0, valuePctile || 0));
  const vo = Math.min(1, Math.max(0, volOiNorm || 0));
  const rp = Math.min(1, Math.max(0, repeatNorm || 0));
  const op = Math.min(1, Math.max(0, otmNorm || 0));
  const sc = Math.min(1, Math.max(0, sideConfidence || 0));

  const score = (0.35 * vp) + (0.25 * vo) + (0.20 * rp) + (0.10 * op) + (0.10 * sc);
  return Number(score.toFixed(6));
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
};
