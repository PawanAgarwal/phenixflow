const { parseCsvLine } = require('../src/csv');
const { simulateLongScalp } = require('../src/backtest');
const { parseQuoteRow } = require('../src/quotes');
const { simulatePassiveMarketMaking } = require('../src/passive-mm');
const { simulateSecondPassiveScalp } = require('../src/second-passive');

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

  it('parses Massive top-of-book quote rows', () => {
    const quote = parseQuoteRow({
      ticker: 'TSLL',
      bid_price: '10.01',
      ask_price: '10.04',
      bid_size: '3',
      ask_size: '4',
      sip_timestamp: '1767364500000000000',
    });
    expect(quote.symbol).toBe('TSLL');
    expect(quote.bidPrice).toBe(10.01);
    expect(quote.askPrice).toBe(10.04);
    expect(quote.bidSize).toBe(3);
  });

  it('simulates a passive bid fill followed by an ask fill', () => {
    const quotes = [{
      dayIso: '2026-01-02',
      tsMs: Date.parse('2026-01-02T14:35:00.000Z'),
      minuteOfDayEt: 575,
      bidPrice: 10,
      askPrice: 10.03,
      bidSize: 10,
      askSize: 10,
    }];
    const trades = [
      {
        dayIso: '2026-01-02',
        tsMs: Date.parse('2026-01-02T14:35:00.100Z'),
        price: 10,
        size: 100,
      },
      {
        dayIso: '2026-01-02',
        tsMs: Date.parse('2026-01-02T14:35:00.300Z'),
        price: 10.03,
        size: 100,
      },
    ];
    const result = simulatePassiveMarketMaking({
      quotes,
      trades,
      settings: {
        costCentsPerSide: 0.5,
        minSpreadCents: 2,
        maxSpreadCents: 5,
        minProfitCents: 1,
        stopCents: 5,
        maxHoldMs: 1000,
        noEntryFirstMinutes: 0,
        noEntryLastMinutes: 0,
      },
    });
    expect(result.summary.trades).toBe(1);
    expect(result.trades[0].exitReason).toBe('target');
    expect(result.trades[0].netCents).toBe(2);
  });

  it('simulates seconds-derived passive limit fills conservatively', () => {
    const rows = [
      row({
        tsUtc: '2026-01-02T14:35:00.000Z',
        tsMs: Date.parse('2026-01-02T14:35:00.000Z'),
        close: 10.02,
        high: 10.02,
        low: 10.01,
        trade_count: 2,
        range_60s_cents: 5,
        minutes_from_open: 5,
        minutes_to_close: 100,
      }),
      row({
        tsUtc: '2026-01-02T14:35:01.000Z',
        tsMs: Date.parse('2026-01-02T14:35:01.000Z'),
        open: 10.02,
        close: 10.01,
        high: 10.02,
        low: 10,
        trade_count: 3,
        range_60s_cents: 5,
        minutes_from_open: 5,
        minutes_to_close: 100,
      }),
      row({
        tsUtc: '2026-01-02T14:35:02.000Z',
        tsMs: Date.parse('2026-01-02T14:35:02.000Z'),
        open: 10.01,
        close: 10.03,
        high: 10.03,
        low: 10.01,
        trade_count: 3,
        range_60s_cents: 5,
        minutes_from_open: 5,
        minutes_to_close: 100,
      }),
    ];
    const result = simulateSecondPassiveScalp(rows, {
      costCentsPerSide: 0.5,
      buyBelowCloseCents: 2,
      targetCents: 3,
      stopCents: 5,
      maxHoldBars: 3,
      noEntryFirstMinutes: 0,
      noEntryLastMinutes: 0,
      minRange60sCents: 1,
    });
    expect(result.summary.trades).toBe(1);
    expect(result.trades[0].exitReason).toBe('target');
    expect(result.trades[0].netCents).toBe(2);
  });
});
