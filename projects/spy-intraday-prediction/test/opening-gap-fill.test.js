const {
  fillTargetPrice,
  simulateOpeningGapFillDay,
} = require('../src/opening-gap-fill');

function makeRows(count, overrides = {}) {
  return Array.from({ length: count }, (_, index) => {
    const minute = index + 1;
    const open = overrides.openByMinute?.[minute] ?? overrides.open ?? 100;
    const close = overrides.closeByMinute?.[minute] ?? overrides.close ?? open;
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
      volume: 1000,
      transactions: 10,
    };
  });
}

describe('opening gap fill simulator', () => {
  it('prices partial fills between the entry and prior close', () => {
    expect(fillTargetPrice({ entryPrice: 102, previousClose: 100, fillFraction: 0.5 })).toBe(101);
    expect(fillTargetPrice({ entryPrice: 98, previousClose: 100, fillFraction: 1 })).toBe(100);
  });

  it('shorts a gap-up and exits when the full fill target trades', () => {
    const rows = makeRows(35, {
      open: 102,
      close: 102,
      lowByMinute: { 7: 99.95 },
    });
    const result = simulateOpeningGapFillDay({
      date: '2026-01-20',
      rows,
      previousClose: 100,
      thresholdBps: 100,
      fillFraction: 1,
      maxHoldMinutes: 30,
    });
    expect(result.traded).toBe(true);
    expect(result.direction).toBe(-1);
    expect(result.targetHit).toBe(true);
    expect(result.exitPrice).toBe(100);
    expect(result.grossReturn).toBeCloseTo((102 - 100) / 102);
  });

  it('buys a gap-down and exits when a half fill trades', () => {
    const rows = makeRows(35, {
      open: 98,
      close: 98,
      highByMinute: { 12: 99.05 },
    });
    const result = simulateOpeningGapFillDay({
      date: '2026-01-20',
      rows,
      previousClose: 100,
      thresholdBps: 100,
      fillFraction: 0.5,
      maxHoldMinutes: 30,
    });
    expect(result.traded).toBe(true);
    expect(result.direction).toBe(1);
    expect(result.targetHit).toBe(true);
    expect(result.exitPrice).toBe(99);
    expect(result.grossReturn).toBeCloseTo((99 - 98) / 98);
  });

  it('skips gaps below the configured threshold', () => {
    const rows = makeRows(35, { open: 100.2, close: 100.2 });
    const result = simulateOpeningGapFillDay({
      date: '2026-01-20',
      rows,
      previousClose: 100,
      thresholdBps: 50,
      fillFraction: 1,
      maxHoldMinutes: 30,
    });
    expect(result.traded).toBe(false);
    expect(result.skippedReason).toBe('below_gap_threshold');
  });

  it('charges round-trip cost and slippage only on active trades', () => {
    const rows = makeRows(35, {
      open: 101,
      close: 101,
      lowByMinute: { 3: 100 },
    });
    const result = simulateOpeningGapFillDay({
      date: '2026-01-20',
      rows,
      previousClose: 100,
      thresholdBps: 50,
      fillFraction: 1,
      maxHoldMinutes: 30,
      costBpsPerSide: 1,
      slippageBpsPerSide: 1,
    });
    expect(result.roundTripCostReturn).toBeCloseTo(0.0004);
    expect(result.netReturn).toBeCloseTo(result.grossReturn - 0.0004);
  });
});
