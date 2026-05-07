const {
  activePredictionStats,
} = require('../src/monthly-best-signal');

describe('monthly best-signal reporting helpers', () => {
  it('counts only active predictions as successes or failures', () => {
    const rows = [
      {
        row: { rowId: 'a', tradeDate: '2026-02-02', minuteUtc: '2026-02-02T14:30:00.000Z', minuteOfDayEt: 570 },
        tradeDate: '2026-02-02',
        minuteUtc: '2026-02-02T14:30:00.000Z',
        actualReturn: 0.01,
        directionProbability: 0.6,
      },
      {
        row: { rowId: 'b', tradeDate: '2026-02-02', minuteUtc: '2026-02-02T15:30:00.000Z', minuteOfDayEt: 630 },
        tradeDate: '2026-02-02',
        minuteUtc: '2026-02-02T15:30:00.000Z',
        actualReturn: 0.01,
        directionProbability: 0.4,
      },
      {
        row: { rowId: 'c', tradeDate: '2026-02-02', minuteUtc: '2026-02-02T16:30:00.000Z', minuteOfDayEt: 690 },
        tradeDate: '2026-02-02',
        minuteUtc: '2026-02-02T16:30:00.000Z',
        actualReturn: -0.01,
        directionProbability: 0.5,
      },
    ];
    const stats = activePredictionStats(rows, 'next_60m', 0.52);
    expect(stats.observations).toBe(3);
    expect(stats.activePredictionCount).toBe(2);
    expect(stats.succeeded).toBe(1);
    expect(stats.failed).toBe(1);
    expect(stats.successRatePct).toBe(50);
    expect(stats.abstained).toBe(1);
  });
});
