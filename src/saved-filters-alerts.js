const {
  THRESHOLD_FILTER_DEFINITIONS,
  getThresholdFilterSettings,
} = require('./flow-filter-definitions');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const Database = require('better-sqlite3');

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
  calls: { field: 'execution.calls', op: 'eq' },
  puts: { field: 'execution.puts', op: 'eq' },
  bid: { field: 'execution.bid', op: 'eq' },
  ask: { field: 'execution.ask', op: 'eq' },
  aa: { field: 'execution.aa', op: 'eq' },
  sweeps: { field: 'execution.sweeps', op: 'eq' },
};

THRESHOLD_FILTER_DEFINITIONS.forEach((definition) => {
  OPERATOR_BY_LEGACY_KEY[definition.key] = {
    field: definition.clauseField,
    op: 'gte',
    thresholdKey: definition.key,
  };
});

const LEGACY_KEY_BY_OPERATOR = Object.entries(OPERATOR_BY_LEGACY_KEY)
  .reduce((acc, [legacyKey, descriptor]) => {
    if (!descriptor.thresholdKey) {
      acc[`${descriptor.field}:${descriptor.op}`] = legacyKey;
    }
    return acc;
  }, {});

function resolveDbPath(env = process.env) {
  const configuredPath = env.PHENIX_DB_PATH || path.resolve(__dirname, '..', 'data', 'phenixflow.sqlite');
  return path.resolve(configuredPath);
}

function ensureDbDir(dbPath) {
  fs.mkdirSync(path.dirname(dbPath), { recursive: true });
}

function ensureSavedQuerySchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS saved_queries (
      id TEXT PRIMARY KEY,
      kind TEXT NOT NULL CHECK (kind IN ('preset', 'alert')),
      name TEXT NOT NULL,
      payload_version TEXT NOT NULL CHECK (payload_version IN ('legacy', 'v2')),
      query_dsl_v2_json TEXT NOT NULL,
      created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE INDEX IF NOT EXISTS idx_saved_queries_kind_updated
      ON saved_queries(kind, updated_at_utc DESC);
  `);
}

function openDb(env = process.env) {
  const dbPath = resolveDbPath(env);
  ensureDbDir(dbPath);
  const db = new Database(dbPath);
  ensureSavedQuerySchema(db);
  return db;
}

function toStoredKind(kind) {
  if (kind === 'presets') return 'preset';
  if (kind === 'alerts') return 'alert';
  throw new Error(`invalid_saved_query_kind:${kind}`);
}

function fromStoredKind(kind) {
  if (kind === 'preset') return 'presets';
  if (kind === 'alert') return 'alerts';
  return kind;
}

function createSavedId(kind) {
  const prefix = kind === 'presets' ? 'preset' : 'alert';
  return `${prefix}_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`;
}

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

  const thresholdSettings = getThresholdFilterSettings(process.env);
  const clauses = [];

  Object.entries(OPERATOR_BY_LEGACY_KEY).forEach(([legacyKey, descriptor]) => {
    const value = payload[legacyKey];
    if (value === undefined || value === null || value === '') return;

    if (descriptor.thresholdKey) {
      if (value === true || value === 'true' || value === 1 || value === '1') {
        clauses.push({ field: descriptor.field, op: descriptor.op, value: thresholdSettings[descriptor.thresholdKey] });
      }
      return;
    }

    clauses.push({ field: descriptor.field, op: descriptor.op, value });
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

  const thresholdSettings = getThresholdFilterSettings(process.env);

  dslV2.clauses.forEach((clause) => {
    const matchedThreshold = THRESHOLD_FILTER_DEFINITIONS.find((definition) => (
      clause.field === definition.clauseField
      && clause.op === 'gte'
      && Number(clause.value) === Number(thresholdSettings[definition.key])
    ));

    if (matchedThreshold) {
      legacy[matchedThreshold.key] = true;
      return;
    }

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
  const db = openDb(process.env);
  try {
    const id = createSavedId(kind);
    const storedKind = toStoredKind(kind);
    const payloadInput = input.payload !== undefined
      ? input.payload
      : (input.query !== undefined ? input.query : input);
    const queryDslV2 = toDslV2(payloadInput || {});
    const now = new Date().toISOString();
    const name = typeof input.name === 'string' && input.name.trim() ? input.name.trim() : id;

    db.prepare(`
      INSERT INTO saved_queries (
        id,
        kind,
        name,
        payload_version,
        query_dsl_v2_json,
        created_at_utc,
        updated_at_utc
      )
      VALUES (
        @id,
        @kind,
        @name,
        'v2',
        @queryDslV2Json,
        @createdAt,
        @updatedAt
      )
    `).run({
      id,
      kind: storedKind,
      name,
      queryDslV2Json: JSON.stringify(queryDslV2),
      createdAt: now,
      updatedAt: now,
    });

    return {
      id,
      name,
      kind,
      queryDslV2,
      createdAt: now,
      updatedAt: now,
    };
  } finally {
    db.close();
  }
}

function getSaved(kind, id) {
  const db = openDb(process.env);
  try {
    const storedKind = toStoredKind(kind);
    const row = db.prepare(`
      SELECT
        id,
        kind,
        name,
        query_dsl_v2_json AS queryDslV2Json,
        created_at_utc AS createdAt,
        updated_at_utc AS updatedAt
      FROM saved_queries
      WHERE id = @id
        AND kind = @kind
      LIMIT 1
    `).get({ id, kind: storedKind });

    if (!row) return null;

    let queryDslV2 = null;
    try {
      queryDslV2 = JSON.parse(row.queryDslV2Json);
    } catch {
      return null;
    }

    return {
      id: row.id,
      kind: fromStoredKind(row.kind),
      name: row.name,
      queryDslV2,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    };
  } finally {
    db.close();
  }
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
  __private: {
    resolveDbPath,
    ensureSavedQuerySchema,
    toStoredKind,
    fromStoredKind,
    createSavedId,
  },
};
