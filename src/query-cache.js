const crypto = require('crypto');

const QUERY_CACHE = new Map();

const FILTER_KEYS = [
  'id',
  'symbol',
  'status',
  'strategy',
  'timeframe',
  'search',
  'from',
  'to',
  'minPnl',
  'maxPnl',
  'minVolume',
  'maxVolume',
];

function canonicalize(value) {
  if (Array.isArray(value)) return value.map((item) => canonicalize(item));
  if (value && typeof value === 'object') {
    const out = {};
    Object.keys(value).sort().forEach((key) => {
      const normalized = canonicalize(value[key]);
      if (normalized !== undefined) out[key] = normalized;
    });
    return out;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : undefined;
  }
  return value;
}

function buildNormalizedQueryFingerprint(filters, filterVersion) {
  const payload = { filterVersion };

  FILTER_KEYS.forEach((key) => {
    if (filters[key] !== undefined) payload[key] = filters[key];
  });

  return canonicalize(payload);
}

function hashNormalizedQuery(normalizedQuery) {
  return crypto.createHash('sha256').update(JSON.stringify(normalizedQuery)).digest('hex');
}

function getCachedEventIds(queryHash) {
  const cached = QUERY_CACHE.get(queryHash);
  if (!cached) return null;
  return new Set(cached);
}

function setCachedEventIds(queryHash, eventIds) {
  QUERY_CACHE.set(queryHash, new Set(eventIds));
}

function resetQueryCache() {
  QUERY_CACHE.clear();
}

module.exports = {
  buildNormalizedQueryFingerprint,
  hashNormalizedQuery,
  getCachedEventIds,
  setCachedEventIds,
  resetQueryCache,
};
