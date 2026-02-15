const { toDslV2, toLegacyPayload, isDslV2 } = require('../src/saved-filters-alerts');

describe('MON-75 saved filters/alerts compatibility', () => {
  it('normalizes legacy payloads into query DSL v2', () => {
    expect(toDslV2({ symbol: 'AAPL', status: 'open', minVolume: 1000 })).toEqual({
      version: 2,
      combinator: 'and',
      clauses: [
        { field: 'symbol', op: 'eq', value: 'AAPL' },
        { field: 'status', op: 'eq', value: 'open' },
        { field: 'volume', op: 'gte', value: 1000 },
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
      ],
    })).toEqual({ symbol: 'NVDA', minPnl: 10 });
  });
});
