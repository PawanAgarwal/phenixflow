const { toDslV2, toLegacyPayload, isDslV2 } = require('../src/saved-filters-alerts');

describe('MON-75 saved filters/alerts compatibility', () => {
  it('normalizes legacy payloads into query DSL v2', () => {
    expect(toDslV2({ symbol: 'AAPL', status: 'open', minVolume: 1000, calls: true, sweeps: true })).toEqual({
      version: 2,
      combinator: 'and',
      clauses: [
        { field: 'symbol', op: 'eq', value: 'AAPL' },
        { field: 'status', op: 'eq', value: 'open' },
        { field: 'volume', op: 'gte', value: 1000 },
        { field: 'execution.calls', op: 'eq', value: true },
        { field: 'execution.sweeps', op: 'eq', value: true },
      ],
    });
  });

  it('passes through v2 payloads as v2', () => {
    const payload = {
      version: 2,
      combinator: 'or',
      clauses: [{ field: 'status', op: 'eq', value: 'paused' }],
    };

    expect(isDslV2(payload)).toBe(true);
    expect(toDslV2(payload)).toEqual(payload);
  });

  it('maps v2 clauses back to legacy payload for backward-compatible UX', () => {
    expect(toLegacyPayload({
      version: 2,
      combinator: 'and',
      clauses: [
        { field: 'symbol', op: 'eq', value: 'NVDA' },
        { field: 'pnl', op: 'gte', value: 10 },
        { field: 'execution.aa', op: 'eq', value: true },
      ],
    })).toEqual({ symbol: 'NVDA', minPnl: 10, aa: true });
  });

  it('wires threshold filter definitions into DSL using configurable registry thresholds', () => {
    const previous = process.env.FLOW_FILTER_PREMIUM_100K_MIN;
    process.env.FLOW_FILTER_PREMIUM_100K_MIN = '125000';

    try {
      expect(toDslV2({ '100k': true })).toEqual({
        version: 2,
        combinator: 'and',
        clauses: [{ field: 'canonical.premium', op: 'gte', value: 125000 }],
      });

      expect(toLegacyPayload({
        version: 2,
        combinator: 'and',
        clauses: [{ field: 'canonical.premium', op: 'gte', value: 125000 }],
      })).toEqual({ '100k': true });
    } finally {
      if (previous === undefined) delete process.env.FLOW_FILTER_PREMIUM_100K_MIN;
      else process.env.FLOW_FILTER_PREMIUM_100K_MIN = previous;
    }
  });
});
