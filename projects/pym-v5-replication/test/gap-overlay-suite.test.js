const {
  portfolioReturnComponents,
  simulateSpyGapFadeFromDailyBars,
  summarizeRecords,
  weightTurnover,
} = require('../src/gap-overlay-suite');

function mockMarket() {
  const dates = ['2026-01-02', '2026-01-05'];
  const byDate = new Map(dates.map((date) => [date, new Map()]));
  byDate.get('2026-01-02').set('SPY', { open: 100, high: 101, low: 99, close: 100 });
  byDate.get('2026-01-05').set('SPY', { open: 98, high: 100.5, low: 97.5, close: 99 });
  byDate.get('2026-01-02').set('AAA', { open: 50, high: 51, low: 49, close: 50 });
  byDate.get('2026-01-05').set('AAA', { open: 55, high: 56, low: 54, close: 55.5 });
  byDate.get('2026-01-02').set('BBB', { open: 100, high: 101, low: 99, close: 100 });
  byDate.get('2026-01-05').set('BBB', { open: 99, high: 100, low: 98, close: 98 });
  return {
    dates,
    byDate,
    closes: new Map([
      ['SPY', [100, 99]],
      ['AAA', [50, 55.5]],
      ['BBB', [100, 98]],
    ]),
  };
}

describe('PYM gap overlay helpers', () => {
  it('splits portfolio return into overnight and open-to-close components', () => {
    const market = mockMarket();
    const weights = new Map([
      ['AAA', 0.5],
      ['BBB', 0.5],
    ]);
    const result = portfolioReturnComponents({ market, weights, index: 1 });
    expect(result.closeToCloseReturn).toBeCloseTo((0.5 * (55.5 / 50 - 1)) + (0.5 * (98 / 100 - 1)));
    expect(result.missing).toEqual([]);
  });

  it('models a gap-down SPY full-fill sleeve from daily OHLC', () => {
    const result = simulateSpyGapFadeFromDailyBars({ market: mockMarket(), index: 1, fillFraction: 1 });
    expect(result.gapDirection).toBe('gap_down');
    expect(result.targetHit).toBe(true);
    expect(result.grossReturn).toBeCloseTo((100 - 98) / 98);
  });

  it('counts daily target turnover by absolute weight deltas', () => {
    const previous = new Map([
      ['AAA', 0.7],
      ['BBB', 0.3],
    ]);
    const next = new Map([
      ['AAA', 0.5],
      ['CCC', 0.5],
    ]);
    expect(weightTurnover(previous, next)).toBeCloseTo(1);
  });

  it('summarizes a sliced window from its own starting equity', () => {
    const summary = summarizeRecords([
      { netReturn: 0.1, endEquity: 200 },
      { netReturn: -0.05, endEquity: 190 },
    ], 100);
    expect(summary.totalReturn).toBeCloseTo(0.045);
    expect(summary.finalEquity).toBeCloseTo(104.5);
  });
});
