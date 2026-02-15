const OPERATOR_BY_LEGACY_KEY = {
  id: { field: 'id', op: 'eq' },
  symbol: { field: 'symbol', op: 'eq' },
  status: { field: 'status', op: 'eq' },
  strategy: { field: 'strategy', op: 'eq' },
  timeframe: { field: 'timeframe', op: 'eq' },
  from: { field: 'createdAt', op: 'gte' },
  to: { field: 'createdAt', op: 'lte' },
  minPnl: { field: 'pnl', op: 'gte' },
  maxPnl: { field: 'pnl', op: 'lte' },
  minVolume: { field: 'volume', op: 'gte' },
  maxVolume: { field: 'volume', op: 'lte' },
  search: { field: 'fullText', op: 'contains' },
};

const LEGACY_KEY_BY_OPERATOR = Object.entries(OPERATOR_BY_LEGACY_KEY)
  .reduce((acc, [legacyKey, descriptor]) => {
    acc[`${descriptor.field}:${descriptor.op}`] = legacyKey;
    return acc;
  }, {});

const stores = {
  presets: new Map(),
  alerts: new Map(),
};

let sequence = 1;

function isDslV2(payload) {
  return Boolean(payload && payload.version === 2 && Array.isArray(payload.clauses));
}

function toDslV2(payload = {}) {
  if (isDslV2(payload)) {
    return {
      version: 2,
      combinator: payload.combinator === 'or' ? 'or' : 'and',
      clauses: payload.clauses
        .filter((clause) => clause && clause.field && clause.op)
        .map((clause) => ({ field: clause.field, op: clause.op, value: clause.value })),
    };
  }

  const clauses = [];

  Object.entries(OPERATOR_BY_LEGACY_KEY).forEach(([legacyKey, descriptor]) => {
    const value = payload[legacyKey];
    if (value !== undefined && value !== null && value !== '') {
      clauses.push({ field: descriptor.field, op: descriptor.op, value });
    }
  });

  return {
    version: 2,
    combinator: 'and',
    clauses,
  };
}

function toLegacyPayload(dslV2 = {}) {
  const legacy = {};

  if (!Array.isArray(dslV2.clauses)) {
    return legacy;
  }

  dslV2.clauses.forEach((clause) => {
    const key = LEGACY_KEY_BY_OPERATOR[`${clause.field}:${clause.op}`];
    if (key) {
      legacy[key] = clause.value;
    }
  });

  return legacy;
}

function serializeRecord(record, payloadVersion = 'v2') {
  const normalizedVersion = payloadVersion === 'legacy' ? 'legacy' : 'v2';
  return {
    id: record.id,
    name: record.name,
    payloadVersion: normalizedVersion,
    payload: normalizedVersion === 'legacy' ? toLegacyPayload(record.queryDslV2) : record.queryDslV2,
    queryDslV2: record.queryDslV2,
    createdAt: record.createdAt,
    updatedAt: record.updatedAt,
  };
}

function createSaved(kind, input = {}) {
  const store = stores[kind];
  const id = `${kind.slice(0, -1)}_${String(sequence).padStart(3, '0')}`;
  sequence += 1;

  const queryDslV2 = toDslV2(input.payload || input.query || {});
  const now = new Date().toISOString();

  const record = {
    id,
    name: typeof input.name === 'string' && input.name.trim() ? input.name.trim() : id,
    queryDslV2,
    createdAt: now,
    updatedAt: now,
  };

  store.set(id, record);
  return record;
}

function getSaved(kind, id) {
  return stores[kind].get(id) || null;
}

function resolvePayloadVersion(requestedVersion, defaultVersion = 'v2') {
  if (requestedVersion === 'legacy') {
    return 'legacy';
  }
  return defaultVersion;
}

module.exports = {
  createSaved,
  getSaved,
  isDslV2,
  toDslV2,
  toLegacyPayload,
  serializeRecord,
  resolvePayloadVersion,
};
