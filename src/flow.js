const fs = require('node:fs');
const path = require('node:path');
const {
  THRESHOLD_FILTER_DEFINITIONS,
  getThresholdFilterSettings,
  findThresholdDefinition,
} = require('./flow-filter-definitions');

const FLOW_FIXTURES = [
  { id: 'flow_001', symbol: 'AAPL', strategy: 'breakout', status: 'open', timeframe: '1h', pnl: 120.5, volume: 1250, createdAt: '2026-01-05T14:30:00.000Z', updatedAt: '2026-01-06T09:15:00.000Z' },
  { id: 'flow_002', symbol: 'TSLA', strategy: 'mean-reversion', status: 'closed', timeframe: '4h', pnl: -45, volume: 980, createdAt: '2026-01-04T12:00:00.000Z', updatedAt: '2026-01-05T08:10:00.000Z' },
  { id: 'flow_003', symbol: 'NVDA', strategy: 'trend-following', status: 'open', timeframe: '1d', pnl: 300, volume: 2100, createdAt: '2026-01-03T10:30:00.000Z', updatedAt: '2026-01-06T11:20:00.000Z' },
  { id: 'flow_004', symbol: 'MSFT', strategy: 'breakout', status: 'paused', timeframe: '15m', pnl: 22.2, volume: 700, createdAt: '2026-01-02T09:45:00.000Z', updatedAt: '2026-01-02T10:00:00.000Z' },
  { id: 'flow_005', symbol: 'AAPL', strategy: 'mean-reversion', status: 'closed', timeframe: '1h', pnl: 88, volume: 1600, createdAt: '2026-01-01T16:30:00.000Z', updatedAt: '2026-01-02T13:00:00.000Z' },
  { id: 'flow_006', symbol: 'AMZN', strategy: 'trend-following', status: 'open', timeframe: '4h', pnl: -12, volume: 400, createdAt: '2025-12-30T14:00:00.000Z', updatedAt: '2026-01-03T13:45:00.000Z' },
  { id: 'flow_007', symbol: 'META', strategy: 'breakout', status: 'open', timeframe: '1h', pnl: 150, volume: 1120, createdAt: '2025-12-29T08:20:00.000Z', updatedAt: '2026-01-04T17:45:00.000Z' },
  { id: 'flow_008', symbol: 'TSLA', strategy: 'breakout', status: 'paused', timeframe: '1d', pnl: -90, volume: 2400, createdAt: '2025-12-28T11:00:00.000Z', updatedAt: '2026-01-05T07:30:00.000Z' },
  { id: 'flow_009', symbol: 'NFLX', strategy: 'mean-reversion', status: 'closed', timeframe: '15m', pnl: 12, volume: 520, createdAt: '2025-12-27T09:10:00.000Z', updatedAt: '2026-01-01T18:15:00.000Z' },
  { id: 'flow_010', symbol: 'NVDA', strategy: 'breakout', status: 'open', timeframe: '4h', pnl: 300, volume: 1800, createdAt: '2025-12-26T07:15:00.000Z', updatedAt: '2026-01-06T12:05:00.000Z' },
];

const ALLOWED_SORT_FIELDS = new Set(['id', 'symbol', 'strategy', 'status', 'timeframe', 'pnl', 'volume', 'createdAt', 'updatedAt']);
const EXECUTION_FILTERS = new Set(['calls', 'puts', 'bid', 'ask', 'aa', 'sweeps']);
const THRESHOLD_FILTER_KEYS = new Set(THRESHOLD_FILTER_DEFINITIONS.map((definition) => definition.key));

function parseNumber(value) {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseBoolean(value) {
  if (typeof value === 'boolean') return value;
  if (typeof value !== 'string') return false;
  const normalized = value.trim().toLowerCase();
  return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on';
}

function normalizeExecutionToken(value) {
  if (typeof value !== 'string') return null;
  const normalized = value.trim().toLowerCase();
  return EXECUTION_FILTERS.has(normalized) ? normalized : null;
}

function parseExecutionFilterSet(rawQuery = {}) {
  const filters = new Set();
  const executionTokens = [];

  if (typeof rawQuery.execution === 'string') {
    executionTokens.push(...rawQuery.execution.split(','));
  }

  if (typeof rawQuery.chips === 'string') {
    executionTokens.push(...rawQuery.chips.split(','));
  }

  executionTokens.forEach((token) => {
    const normalized = normalizeExecutionToken(token);
    if (normalized) filters.add(normalized);
  });

  EXECUTION_FILTERS.forEach((token) => {
    if (parseBoolean(rawQuery[token])) {
      filters.add(token);
    }
  });

  return filters;
}

function parseThresholdFilterSet(rawQuery = {}) {
  const filters = new Set();
  const thresholdTokens = [];

  if (typeof rawQuery.chips === 'string') {
    thresholdTokens.push(...rawQuery.chips.split(','));
  }

  if (typeof rawQuery.sizeValue === 'string') {
    thresholdTokens.push(...rawQuery.sizeValue.split(','));
  }

  thresholdTokens.forEach((token) => {
    const definition = findThresholdDefinition(token);
    if (definition) filters.add(definition.key);
  });

  THRESHOLD_FILTER_DEFINITIONS.forEach((definition) => {
    if (parseBoolean(rawQuery[definition.key])) {
      filters.add(definition.key);
    }
  });

  return filters;
}

function normalizeRight(value) {
  if (typeof value !== 'string') return null;
  const normalized = value.trim().toUpperCase();
  if (normalized === 'C' || normalized === 'CALL' || normalized === 'CALLS') return 'CALL';
  if (normalized === 'P' || normalized === 'PUT' || normalized === 'PUTS') return 'PUT';
  return null;
}

function normalizeSweepFlag(flow = {}) {
  if (flow.isSweep === true || flow.sweep === true) return true;

  const stringCandidates = [flow.executionType, flow.saleCondition, flow.condition, flow.tradeType, flow.type]
    .filter((candidate) => typeof candidate === 'string')
    .map((candidate) => candidate.trim().toLowerCase());

  if (stringCandidates.some((candidate) => candidate === 'sweep' || candidate === 'sweeps')) return true;

  if (Array.isArray(flow.chips)) {
    return flow.chips.some((chip) => typeof chip === 'string' && chip.trim().toLowerCase() === 'sweeps');
  }

  return false;
}

function getCanonicalPremium(flow = {}) {
  const explicitPremium = parseNumber(flow.premium ?? flow.optionPremium ?? flow.totalPremium);
  if (explicitPremium !== undefined) return explicitPremium;

  const price = parseNumber(flow.price ?? flow.markPrice);
  const size = parseNumber(flow.size ?? flow.contractSize ?? flow.contracts ?? flow.qty ?? flow.quantity);

  if (price !== undefined && size !== undefined) {
    return price * size * 100;
  }

  return undefined;
}

function getCanonicalSize(flow = {}) {
  return parseNumber(flow.size ?? flow.contractSize ?? flow.contracts ?? flow.qty ?? flow.quantity);
}

function buildExecutionFlags(flow = {}) {
  const right = normalizeRight(flow.right);
  const price = parseNumber(flow.price);
  const bid = parseNumber(flow.bid);
  const ask = parseNumber(flow.ask);

  const hasQuote = price !== undefined && bid !== undefined && ask !== undefined;
  const spread = hasQuote ? ask - bid : undefined;
  const aaThreshold = hasQuote ? ask + Math.max(0.01, 0.10 * spread) : undefined;

  const isAA = hasQuote ? price >= aaThreshold : false;
  const isAsk = hasQuote ? price >= ask && !isAA : false;
  const isBid = hasQuote ? price <= bid : false;

  return {
    calls: right === 'CALL',
    puts: right === 'PUT',
    bid: isBid,
    ask: isAsk,
    aa: isAA,
    sweeps: normalizeSweepFlag(flow),
  };
}

function buildThresholdFlags(flow = {}, thresholdSettings = {}) {
  const canonicalMetrics = {
    premium: getCanonicalPremium(flow),
    size: getCanonicalSize(flow),
  };

  const flags = {};

  THRESHOLD_FILTER_DEFINITIONS.forEach((definition) => {
    const metricValue = canonicalMetrics[definition.metric];
    const threshold = thresholdSettings[definition.key];
    flags[definition.key] = metricValue !== undefined && metricValue >= threshold;
  });

  return flags;
}

function decodeCursor(cursor) {
  try {
    return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    return null;
  }
}

function encodeCursor(payload) {
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
}

function buildComparator(sortBy, sortOrder) {
  const direction = sortOrder === 'asc' ? 1 : -1;

  return (left, right) => {
    const lv = left[sortBy];
    const rv = right[sortBy];

    if (lv === rv) return left.id.localeCompare(right.id) * direction;
    return lv > rv ? direction : -direction;
  };
}

function compareCursorRecord(record, cursorRecord, sortBy, sortOrder) {
  const direction = sortOrder === 'asc' ? 1 : -1;
  const rv = record[sortBy];
  const cv = cursorRecord.value;

  if (rv === cv) return record.id.localeCompare(cursorRecord.id) * direction;
  return rv > cv ? direction : -direction;
}

function normalizeFilterVersion(rawVersion) {
  return rawVersion === 'candidate' ? 'candidate' : 'legacy';
}

function parseRealIngestRows(rawContent) {
  const parsed = JSON.parse(rawContent);
  if (Array.isArray(parsed)) return parsed;
  if (parsed && Array.isArray(parsed.rows)) return parsed.rows;
  if (parsed && Array.isArray(parsed.data)) return parsed.data;
  throw new Error('artifact_missing_rows');
}

function resolveSourceData(rawQuery) {
  if (rawQuery.source !== 'real-ingest') {
    return {
      flows: FLOW_FIXTURES,
      observability: {
        source: 'fixtures',
        artifactPath: null,
        rowCount: FLOW_FIXTURES.length,
        fallbackReason: null,
      },
    };
  }

  const configuredPath = rawQuery.artifactPath || process.env.FLOW_INGEST_ARTIFACT_PATH;
  if (!configuredPath) {
    return {
      flows: FLOW_FIXTURES,
      observability: {
        source: 'fixtures',
        artifactPath: null,
        rowCount: FLOW_FIXTURES.length,
        fallbackReason: 'artifact_path_missing',
      },
    };
  }

  const artifactPath = path.resolve(configuredPath);

  try {
    const rows = parseRealIngestRows(fs.readFileSync(artifactPath, 'utf8'));
    return {
      flows: rows,
      observability: {
        source: 'real-ingest',
        artifactPath,
        rowCount: rows.length,
        fallbackReason: null,
      },
    };
  } catch (error) {
    const fallbackReason = error.message === 'artifact_missing_rows'
      ? 'artifact_rows_missing'
      : 'artifact_read_error';

    return {
      flows: FLOW_FIXTURES,
      observability: {
        source: 'fixtures',
        artifactPath,
        rowCount: FLOW_FIXTURES.length,
        fallbackReason,
      },
    };
  }
}

function filterFlows(flows, filters, filterVersion) {
  const normalize = filterVersion === 'candidate' ? (value) => value.toLowerCase() : (value) => value;

  return flows.filter((flow) => {
    if (filters.execution.size) {
      const executionFlags = buildExecutionFlags(flow);
      const allMatch = Array.from(filters.execution).every((filterName) => executionFlags[filterName]);
      if (!allMatch) return false;
    }

    if (filters.thresholds.size) {
      const thresholdFlags = buildThresholdFlags(flow, filters.thresholdSettings);
      const allMatch = Array.from(filters.thresholds).every((filterName) => thresholdFlags[filterName]);
      if (!allMatch) return false;
    }

    if (filters.id && normalize(flow.id) !== filters.id) return false;
    if (filters.symbol && normalize(flow.symbol) !== filters.symbol) return false;
    if (filters.status && normalize(flow.status) !== filters.status) return false;
    if (filters.strategy && normalize(flow.strategy) !== filters.strategy) return false;
    if (filters.timeframe && normalize(flow.timeframe) !== filters.timeframe) return false;
    if (filters.from && flow.createdAt < filters.from) return false;
    if (filters.to && flow.createdAt > filters.to) return false;
    if (filters.minPnl !== undefined && flow.pnl < filters.minPnl) return false;
    if (filters.maxPnl !== undefined && flow.pnl > filters.maxPnl) return false;
    if (filters.minVolume !== undefined && flow.volume < filters.minVolume) return false;
    if (filters.maxVolume !== undefined && flow.volume > filters.maxVolume) return false;
    if (filters.search) {
      const haystack = `${flow.id} ${flow.symbol} ${flow.strategy} ${flow.status} ${flow.timeframe}`.toLowerCase();
      return haystack.includes(filters.search);
    }
    return true;
  });
}

function buildFilters(rawQuery, filterVersion) {
  const normalizeExact = filterVersion === 'candidate'
    ? (value) => (typeof value === 'string' && value.trim().length ? value.trim().toLowerCase() : undefined)
    : (value) => value;

  const thresholdSettings = getThresholdFilterSettings(process.env);
  const thresholds = parseThresholdFilterSet(rawQuery);

  return {
    id: normalizeExact(rawQuery.id),
    symbol: normalizeExact(rawQuery.symbol),
    status: normalizeExact(rawQuery.status),
    strategy: normalizeExact(rawQuery.strategy),
    timeframe: normalizeExact(rawQuery.timeframe),
    search: typeof rawQuery.search === 'string' ? rawQuery.search.trim().toLowerCase() : undefined,
    from: rawQuery.from,
    to: rawQuery.to,
    minPnl: parseNumber(rawQuery.minPnl),
    maxPnl: parseNumber(rawQuery.maxPnl),
    minVolume: parseNumber(rawQuery.minVolume),
    maxVolume: parseNumber(rawQuery.maxVolume),
    execution: parseExecutionFilterSet(rawQuery),
    thresholds,
    thresholdSettings,
  };
}

function queryFlow(rawQuery, options = {}) {
  const filterVersion = normalizeFilterVersion(options.filterVersion);
  const sortBy = ALLOWED_SORT_FIELDS.has(rawQuery.sortBy) ? rawQuery.sortBy : 'createdAt';
  const sortOrder = rawQuery.sortOrder === 'asc' ? 'asc' : 'desc';
  const limitValue = parseNumber(rawQuery.limit);
  const limit = limitValue && limitValue >= 1 ? Math.min(100, limitValue) : 25;

  const filters = buildFilters(rawQuery, filterVersion);
  const sourceData = resolveSourceData(rawQuery);
  const sorted = filterFlows(sourceData.flows, filters, filterVersion).sort(buildComparator(sortBy, sortOrder));

  let startIndex = 0;
  if (rawQuery.cursor) {
    const cursor = decodeCursor(rawQuery.cursor);
    if (cursor && cursor.sortBy === sortBy && cursor.sortOrder === sortOrder) {
      const idx = sorted.findIndex((flow) => compareCursorRecord(flow, cursor, sortBy, sortOrder) > 0);
      startIndex = idx === -1 ? sorted.length : idx;
    }
  }

  const data = sorted.slice(startIndex, startIndex + limit);
  const hasMore = startIndex + limit < sorted.length;
  const nextCursor = hasMore
    ? encodeCursor({ sortBy, sortOrder, value: data[data.length - 1][sortBy], id: data[data.length - 1].id })
    : null;

  return {
    data,
    page: { limit, hasMore, nextCursor, sortBy, sortOrder, total: sorted.length },
    meta: { filterVersion, observability: sourceData.observability },
  };
}

function buildFlowFacets(query = {}, options = {}) {
  const filterVersion = normalizeFilterVersion(options.filterVersion);
  const filters = buildFilters(query, filterVersion);
  const filtered = filterFlows(FLOW_FIXTURES, filters, filterVersion);

  const bySymbol = {};
  const byStatus = {};

  filtered.forEach((flow) => {
    bySymbol[flow.symbol] = (bySymbol[flow.symbol] || 0) + 1;
    byStatus[flow.status] = (byStatus[flow.status] || 0) + 1;
  });

  return {
    facets: {
      symbol: bySymbol,
      status: byStatus,
    },
    total: filtered.length,
    meta: { filterVersion },
  };
}

function buildFlowStream(query = {}, options = {}) {
  const base = queryFlow(query, options);
  return {
    data: base.data.map((flow, index) => ({
      sequence: index + 1,
      eventType: 'flow.updated',
      flow,
    })),
    page: base.page,
    meta: base.meta,
  };
}

function getFlowDetail(id) {
  return FLOW_FIXTURES.find((item) => item.id === id) || null;
}

module.exports = {
  queryFlow,
  buildFlowFacets,
  buildFlowStream,
  getFlowDetail,
  __private: {
    parseExecutionFilterSet,
    parseThresholdFilterSet,
    parseRealIngestRows,
    buildExecutionFlags,
    buildThresholdFlags,
    getCanonicalPremium,
    getCanonicalSize,
    THRESHOLD_FILTER_KEYS,
  },
};
