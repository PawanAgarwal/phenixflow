const { queryFlow, buildFlowFacets } = require('../src/flow');
const {
  buildNormalizedQueryFingerprint,
  hashNormalizedQuery,
  resetQueryCache,
} = require('../src/query-cache');

describe('query normalization + event-id cache', () => {
  beforeEach(() => {
    resetQueryCache();
  });

  it('builds deterministic query hashes regardless of input key order', () => {
    const normalizedA = buildNormalizedQueryFingerprint({ symbol: 'AAPL', status: 'open', minVolume: 1000 }, 'legacy');
    const normalizedB = buildNormalizedQueryFingerprint({ minVolume: 1000, status: 'open', symbol: 'AAPL' }, 'legacy');

    expect(hashNormalizedQuery(normalizedA)).toBe(hashNormalizedQuery(normalizedB));
  });

  it('emits cache-miss then cache-hit for repeated equivalent list queries', () => {
    const metrics = [];
    const emitMetric = (name, payload) => metrics.push({ name, payload });

    const first = queryFlow({ symbol: 'AAPL', limit: 1 }, { emitMetric });
    const second = queryFlow({ limit: 5, symbol: 'AAPL', sortBy: 'pnl', sortOrder: 'asc' }, { emitMetric });

    expect(first.page.total).toBe(2);
    expect(second.page.total).toBe(2);
    expect(metrics.map((metric) => metric.name)).toEqual(['cache-miss', 'cache-hit']);
    expect(metrics[0].payload.queryHash).toBe(metrics[1].payload.queryHash);
  });

  it('reuses cached event-id sets for facet queries', () => {
    const metrics = [];
    const emitMetric = (name, payload) => metrics.push({ name, payload });

    queryFlow({ status: 'open' }, { emitMetric });
    const facets = buildFlowFacets({ status: 'open' }, { emitMetric });

    expect(facets.total).toBe(5);
    expect(metrics.map((metric) => metric.name)).toEqual(['cache-miss', 'cache-hit']);
    expect(metrics[0].payload.queryHash).toBe(metrics[1].payload.queryHash);
  });
});
