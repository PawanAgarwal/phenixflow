const { parseCsvLine } = require('../src/csv');
const { simulateLongScalp } = require('../src/backtest');

function row(overrides = {}) {
  return {
    tradeDate: '2026-01-02',
    tsUtc: '2026-01-02T14:30:00.000Z',
    open: 10,
    high: 10,
    low: 10,
    close: 10,
    minutes_from_open: 10,
    minutes_to_close: 100,
    market_ok_1m: 1,
    tsla_ret_1m: 0.002,
    qqq_ret_1m: 0,
    ret_60s_cents: 0,
    ret_3bar_cents: 1,
    ...overrides,
  };
}

describe('TSLL scalping utilities', () => {
  it('parses quoted Massive condition lists without shifting columns', () => {
    const fields = parseCsvLine('TSLL,"12,37",0,8,1,1767344400000427000,19.73');
    expect(fields).toEqual(['TSLL', '12,37', '0', '8', '1', '1767344400000427000', '19.73']);
  });

  it('applies next-bar entry and per-side costs to a target scalp', () => {
    const rows = [
      row({ ret_60s_cents: 1, tsUtc: '2026-01-02T14:30:00.000Z' }),
      row({ tsUtc: '2026-01-02T14:30:05.000Z', open: 10, high: 10.03, low: 10, close: 10.02 }),
      row({ tsUtc: '2026-01-02T14:30:10.000Z', open: 10.02, high: 10.04, low: 10.02, close: 10.03 }),
    ];
    const result = simulateLongScalp(rows, {
      name: 'tsla_lead_lag',
      params: {
        minTslaRet1m: 0.001,
        minQqqRet1m: -0.0001,
        maxTsllLagCents: 2,
        minTurnCents: 1,
      },
      execution: {
        targetCents: 3,
        stopCents: 6,
        maxHoldBars: 5,
      },
    }, {
      costCentsPerSide: 0.5,
      cooldownBars: 0,
      noEntryFirstMinutes: 0,
      noEntryLastMinutes: 0,
    });
    expect(result.summary.trades).toBe(1);
    expect(result.trades[0].exitReason).toBe('target');
    expect(result.trades[0].netCents).toBe(2);
  });
});
