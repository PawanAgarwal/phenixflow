const {
  buildNormalizedQueryFingerprint,
  hashNormalizedQuery,
  getCachedEventIds,
  setCachedEventIds,
} = require('./query-cache');

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

function parseNumber(value) {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
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

function filterFlows(flows, filters, filterVersion) {
  const normalize = filterVersion === 'candidate' ? (value) => value.toLowerCase() : (value) => value;

  return flows.filter((flow) => {
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
  };
}

function emitMetric(options, name, payload) {
  if (typeof options.emitMetric === 'function') {
    options.emitMetric(name, payload);
  }
}

function resolveMatchingEventIds(rawQuery, filterVersion, options = {}) {
  const filters = buildFilters(rawQuery, filterVersion);
  const normalizedQuery = buildNormalizedQueryFingerprint(filters, filterVersion);
  const queryHash = hashNormalizedQuery(normalizedQuery);

  const cachedIds = getCachedEventIds(queryHash);
  if (cachedIds) {
    emitMetric(options, 'cache-hit', { queryHash, size: cachedIds.size });
    return { filters, queryHash, matchingIds: cachedIds };
  }

  const filtered = filterFlows(FLOW_FIXTURES, filters, filterVersion);
  const eventIds = new Set(filtered.map((flow) => flow.id));
  setCachedEventIds(queryHash, eventIds);
  emitMetric(options, 'cache-miss', { queryHash, size: eventIds.size });

  return { filters, queryHash, matchingIds: eventIds };
}

function queryFlow(rawQuery, options = {}) {
  const filterVersion = normalizeFilterVersion(options.filterVersion);
  const sortBy = ALLOWED_SORT_FIELDS.has(rawQuery.sortBy) ? rawQuery.sortBy : 'createdAt';
  const sortOrder = rawQuery.sortOrder === 'asc' ? 'asc' : 'desc';
  const limitValue = parseNumber(rawQuery.limit);
  const limit = limitValue && limitValue >= 1 ? Math.min(100, limitValue) : 25;

  const { matchingIds } = resolveMatchingEventIds(rawQuery, filterVersion, options);
  const sorted = FLOW_FIXTURES.filter((flow) => matchingIds.has(flow.id)).sort(buildComparator(sortBy, sortOrder));

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
    meta: { filterVersion },
  };
}

function buildFlowFacets(query = {}, options = {}) {
  const filterVersion = normalizeFilterVersion(options.filterVersion);
  const { matchingIds } = resolveMatchingEventIds(query, filterVersion, options);
  const filtered = FLOW_FIXTURES.filter((flow) => matchingIds.has(flow.id));

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

module.exports = { queryFlow, buildFlowFacets, buildFlowStream, getFlowDetail };
