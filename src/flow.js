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

const REFERENCE_NOW_ISO = '2026-01-07T00:00:00.000Z';
const REFERENCE_NOW_MS = Date.parse(REFERENCE_NOW_ISO);

const ALLOWED_SORT_FIELDS = new Set(['id', 'symbol', 'strategy', 'status', 'timeframe', 'pnl', 'volume', 'createdAt', 'updatedAt']);
const ALLOWED_QUICK_FILTERS = new Set(['openOnly', 'closedOnly', 'winners', 'losers', 'highVolume', 'recentlyUpdated']);

const FILTER_ALIASES = {
  createdFrom: ['from'],
  createdTo: ['to'],
  minPnl: ['pnlMin'],
  maxPnl: ['pnlMax'],
  minVolume: ['volumeMin'],
  maxVolume: ['volumeMax'],
};

class FlowQueryValidationError extends Error {}

function encodeCursor(payload) {
  return Buffer.from(JSON.stringify(payload), 'utf8').toString('base64url');
}

function decodeCursor(cursor) {
  try {
    return JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    throw new FlowQueryValidationError('Invalid cursor.');
  }
}

function parseNumber(value, field) {
  if (value === undefined) return undefined;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new FlowQueryValidationError(`${field} must be a valid number.`);
  return parsed;
}

function parseDate(value, field) {
  if (value === undefined) return undefined;
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) throw new FlowQueryValidationError(`${field} must be a valid ISO-8601 date.`);
  return new Date(timestamp).toISOString();
}

function parseLimit(value) {
  if (value === undefined) return 25;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 1) throw new FlowQueryValidationError('limit must be a positive integer.');
  return Math.min(parsed, 100);
}

function parseQuickFilters(rawQuickFilters) {
  if (rawQuickFilters === undefined) return [];

  const values = Array.isArray(rawQuickFilters)
    ? rawQuickFilters
    : String(rawQuickFilters)
      .split(',')
      .map((token) => token.trim())
      .filter(Boolean);

  const unique = [...new Set(values)];
  for (const filter of unique) {
    if (!ALLOWED_QUICK_FILTERS.has(filter)) {
      throw new FlowQueryValidationError(`quickFilters contains unsupported value: ${filter}.`);
    }
  }

  if (unique.includes('openOnly') && unique.includes('closedOnly')) {
    throw new FlowQueryValidationError('quickFilters cannot include both openOnly and closedOnly.');
  }

  return unique;
}

function getCanonicalQueryValue(rawQuery, key) {
  if (rawQuery[key] !== undefined) return rawQuery[key];
  const aliases = FILTER_ALIASES[key] ?? [];
  for (const alias of aliases) {
    if (rawQuery[alias] !== undefined) return rawQuery[alias];
  }
  return undefined;
}

function parseQuery(rawQuery) {
  const sortBy = rawQuery.sortBy ?? 'createdAt';
  const sortOrder = rawQuery.sortOrder ?? 'desc';

  if (!ALLOWED_SORT_FIELDS.has(sortBy)) throw new FlowQueryValidationError(`sortBy must be one of: ${[...ALLOWED_SORT_FIELDS].join(', ')}`);
  if (!['asc', 'desc'].includes(sortOrder)) throw new FlowQueryValidationError('sortOrder must be asc or desc.');

  const canonical = {
    id: rawQuery.id,
    symbol: rawQuery.symbol,
    strategy: rawQuery.strategy,
    status: rawQuery.status,
    timeframe: rawQuery.timeframe,
    search: rawQuery.search,
    createdFrom: parseDate(getCanonicalQueryValue(rawQuery, 'createdFrom'), 'createdFrom'),
    createdTo: parseDate(getCanonicalQueryValue(rawQuery, 'createdTo'), 'createdTo'),
    updatedFrom: parseDate(rawQuery.updatedFrom, 'updatedFrom'),
    updatedTo: parseDate(rawQuery.updatedTo, 'updatedTo'),
    minPnl: parseNumber(getCanonicalQueryValue(rawQuery, 'minPnl'), 'minPnl'),
    maxPnl: parseNumber(getCanonicalQueryValue(rawQuery, 'maxPnl'), 'maxPnl'),
    minVolume: parseNumber(getCanonicalQueryValue(rawQuery, 'minVolume'), 'minVolume'),
    maxVolume: parseNumber(getCanonicalQueryValue(rawQuery, 'maxVolume'), 'maxVolume'),
  };

  return {
    sortBy,
    sortOrder,
    limit: parseLimit(rawQuery.limit),
    filters: canonical,
    quickFilters: parseQuickFilters(rawQuery.quickFilters),
    cursor: rawQuery.cursor,
  };
}

function applyQuickFilters(flow, quickFilters) {
  if (quickFilters.includes('openOnly') && flow.status !== 'open') return false;
  if (quickFilters.includes('closedOnly') && flow.status !== 'closed') return false;
  if (quickFilters.includes('winners') && flow.pnl <= 0) return false;
  if (quickFilters.includes('losers') && flow.pnl >= 0) return false;
  if (quickFilters.includes('highVolume') && flow.volume < 1500) return false;
  if (quickFilters.includes('recentlyUpdated') && flow.updatedAt < '2026-01-06T00:00:00.000Z') return false;
  return true;
}

function filterFlows(flows, filters, quickFilters) {
  return flows.filter((flow) => {
    if (filters.id && flow.id !== filters.id) return false;
    if (filters.symbol && flow.symbol !== filters.symbol) return false;
    if (filters.strategy && flow.strategy !== filters.strategy) return false;
    if (filters.status && flow.status !== filters.status) return false;
    if (filters.timeframe && flow.timeframe !== filters.timeframe) return false;
    if (filters.createdFrom && flow.createdAt < filters.createdFrom) return false;
    if (filters.createdTo && flow.createdAt > filters.createdTo) return false;
    if (filters.updatedFrom && flow.updatedAt < filters.updatedFrom) return false;
    if (filters.updatedTo && flow.updatedAt > filters.updatedTo) return false;
    if (filters.minPnl !== undefined && flow.pnl < filters.minPnl) return false;
    if (filters.maxPnl !== undefined && flow.pnl > filters.maxPnl) return false;
    if (filters.minVolume !== undefined && flow.volume < filters.minVolume) return false;
    if (filters.maxVolume !== undefined && flow.volume > filters.maxVolume) return false;
    if (filters.search) {
      const haystack = `${flow.id} ${flow.symbol} ${flow.strategy} ${flow.status} ${flow.timeframe}`.toLowerCase();
      if (!haystack.includes(filters.search.toLowerCase())) return false;
    }

    return applyQuickFilters(flow, quickFilters);
  });
}

function buildComparator(sortBy, sortOrder) {
  const direction = sortOrder === 'asc' ? 1 : -1;
  return (l, r) => {
    if (l[sortBy] === r[sortBy]) return l.id.localeCompare(r.id) * direction;
    return l[sortBy] > r[sortBy] ? direction : -direction;
  };
}

function compareCursorRecord(record, cursorRecord, sortBy, sortOrder) {
  const direction = sortOrder === 'asc' ? 1 : -1;
  if (record[sortBy] === cursorRecord.value) return record.id.localeCompare(cursorRecord.id) * direction;
  return record[sortBy] > cursorRecord.value ? direction : -direction;
}

function enrichRow(flow) {
  const createdAtMs = Date.parse(flow.createdAt);
  const updatedAtMs = Date.parse(flow.updatedAt);
  return {
    ...flow,
    isProfitable: flow.pnl > 0,
    pnlDirection: flow.pnl > 0 ? 'up' : flow.pnl < 0 ? 'down' : 'flat',
    createdAtEpochMs: createdAtMs,
    updatedAtEpochMs: updatedAtMs,
    ageHours: Math.floor((REFERENCE_NOW_MS - createdAtMs) / 3_600_000),
  };
}

function buildCursorFingerprint(query) {
  return JSON.stringify({ filters: query.filters, quickFilters: query.quickFilters });
}

function queryFlow(rawQuery) {
  const query = parseQuery(rawQuery);
  const sorted = filterFlows(FLOW_FIXTURES, query.filters, query.quickFilters).sort(buildComparator(query.sortBy, query.sortOrder));

  const fingerprint = buildCursorFingerprint(query);
  let startIndex = 0;

  if (query.cursor) {
    const cursor = decodeCursor(query.cursor);
    if (!cursor || cursor.sortBy !== query.sortBy || cursor.sortOrder !== query.sortOrder || cursor.fingerprint !== fingerprint) {
      throw new FlowQueryValidationError('Cursor does not match current query.');
    }

    const idx = sorted.findIndex((flow) => compareCursorRecord(flow, cursor, query.sortBy, query.sortOrder) > 0);
    startIndex = idx === -1 ? sorted.length : idx;
  }

  const data = sorted.slice(startIndex, startIndex + query.limit).map(enrichRow);
  const hasMore = startIndex + query.limit < sorted.length;
  const nextCursor = hasMore
    ? encodeCursor({
      sortBy: query.sortBy,
      sortOrder: query.sortOrder,
      value: data[data.length - 1][query.sortBy],
      id: data[data.length - 1].id,
      fingerprint,
    })
    : null;

  return {
    data,
    page: {
      limit: query.limit,
      hasMore,
      nextCursor,
      sortBy: query.sortBy,
      sortOrder: query.sortOrder,
      total: sorted.length,
    },
    meta: {
      appliedFilters: query.filters,
      appliedQuickFilters: query.quickFilters,
      returnedCount: data.length,
      cursorRequested: Boolean(query.cursor),
      referenceNow: REFERENCE_NOW_ISO,
    },
  };
}

module.exports = { queryFlow, FlowQueryValidationError };
