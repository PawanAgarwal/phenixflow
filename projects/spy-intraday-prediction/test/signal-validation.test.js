const {
  contributionStats,
  hydratePrediction,
  runValidationBacktest,
  validationVerdict,
} = require('../src/signal-validation');

describe('signal validation helpers', () => {
  it('hydrates saved predictions with an ET minute for horizon selection', () => {
    const row = hydratePrediction({
      rowId: 'r1',
      tradeDate: '2026-02-02',
      minuteUtc: '2026-02-02T14:30:00.000Z',
      actualReturn: 0.001,
      directionProbability: 0.6,
      confidence: 0.6,
    });
    expect(row.minuteOfDayEt).toBe(570);
    expect(row.row.minuteOfDayEt).toBe(570);
  });

  it('runs long/cash stress by dropping short positions', () => {
    const predictions = [
      hydratePrediction({
        rowId: 'a',
        tradeDate: '2026-02-02',
        minuteUtc: '2026-02-02T14:30:00.000Z',
        actualReturn: -0.01,
        directionProbability: 0.4,
        confidence: 0.6,
      }),
      hydratePrediction({
        rowId: 'b',
        tradeDate: '2026-02-02',
        minuteUtc: '2026-02-02T14:31:00.000Z',
        actualReturn: 0.01,
        directionProbability: 0.6,
        confidence: 0.6,
      }),
    ];
    const longShort = runValidationBacktest(predictions, {
      confidenceThreshold: 0.52,
      transactionCostBps: 0,
      slippageBps: 0,
      horizonName: 'next_1m',
    });
    const longOnly = runValidationBacktest(predictions, {
      confidenceThreshold: 0.52,
      transactionCostBps: 0,
      slippageBps: 0,
      horizonName: 'next_1m',
      positionMode: 'long_cash',
    });
    expect(longShort.totalReturn).toBeGreaterThan(longOnly.totalReturn);
    expect(longOnly.shortShare).toBe(0);
  });

  it('measures best-day concentration', () => {
    const stats = contributionStats([
      { tradeDate: '2026-02-02', strategyReturn: 0.05, equity: 1.05 },
      { tradeDate: '2026-02-03', strategyReturn: -0.01, equity: 1.0395 },
      { tradeDate: '2026-02-04', strategyReturn: 0.01, equity: 1.049895 },
    ]);
    expect(stats.bestDay.tradeDate).toBe('2026-02-02');
    expect(stats.returnWithoutBestDay).toBeCloseTo(-0.0001, 6);
  });

  it('requires cost, concentration, and threshold stability for promotion', () => {
    const verdict = validationVerdict({
      positiveMonths: 3,
      doubleCostPositiveMonths: 2,
      defaultTotalReturn: 0.03,
      doubleCostTotalReturn: 0.02,
      returnWithoutBestDayTotal: 0.01,
      delayedOneMinuteTotalReturn: 0.005,
      thresholdStability: [
        { threshold: 0.6, totalReturn: 0.03 },
        { threshold: 0.65, totalReturn: 0.01 },
      ],
    });
    expect(verdict.promoteToPaper).toBe(true);
  });
});
