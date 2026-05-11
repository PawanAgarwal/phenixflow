const {
  buildMomentumExposure,
  computeVwap,
  countExposureTrades,
  simulateConcretumDay,
} = require('../src/concretum-strategy');

function makeRows(count, overrides = {}) {
  return Array.from({ length: count }, (_, index) => {
    const minute = index + 1;
    const close = overrides.closeByMinute?.[minute] ?? (overrides.close ?? 100);
    const open = overrides.openByMinute?.[minute] ?? (overrides.open ?? close);
    const high = overrides.highByMinute?.[minute] ?? Math.max(open, close);
    const low = overrides.lowByMinute?.[minute] ?? Math.min(open, close);
    return {
      date: '2026-01-20',
      minuteMs: Date.UTC(2026, 0, 20, 14, 30 + index),
      minuteUtc: new Date(Date.UTC(2026, 0, 20, 14, 30 + index)).toISOString(),
      minuteOfDayEt: 570 + index,
      minFromOpen: minute,
      open,
      high,
      low,
      close,
      volume: overrides.volume ?? 1000,
      transactions: 10,
    };
  });
}

describe('Concretum SPY intraday strategy helpers', () => {
  it('computes VWAP from cumulative HLC3 dollar volume', () => {
    const rows = [
      { high: 12, low: 9, close: 9, volume: 100 },
      { high: 15, low: 12, close: 15, volume: 300 },
    ];
    expect(computeVwap(rows)).toEqual([10, 13]);
  });

  it('lags 30-minute rebalance signals before they affect PnL', () => {
    const rows = makeRows(35);
    const signals = Array(35).fill(0);
    signals[29] = 1;
    const exposure = buildMomentumExposure(rows, signals, 30);
    expect(exposure[29]).toBe(0);
    expect(exposure[30]).toBe(1);
    expect(countExposureTrades(exposure)).toBe(2);
  });

  it('shorts a positive overnight gap and exits at minute 30', () => {
    const rows = makeRows(35, {
      open: 102,
      close: 102,
      closeByMinute: { 30: 101 },
      lowByMinute: { 30: 101 },
    });
    const sigmaByMinute = new Map(rows.map((row) => [row.minFromOpen, 0.5]));
    const result = simulateConcretumDay({
      date: '2026-01-20',
      rows,
      previousClose: 100,
      previousAum: 100000,
      dailyVol: 0.01,
      sigmaByMinute,
      overnightThreshold: 0.02,
      params: {
        tradeFreq: 30,
        targetVol: 0.02,
        maxLeverage: 4,
        commissionPerShare: 0,
        minCommissionPerOrder: 0,
      },
    });
    expect(result.gapSignal).toBe(-1);
    expect(result.gapGrossPnl).toBeGreaterThan(0);
    expect(result.ret).toBeGreaterThan(0);
  });
});
