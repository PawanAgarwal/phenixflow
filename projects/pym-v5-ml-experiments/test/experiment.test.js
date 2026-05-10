const {
  backtestPolicy,
  buildFeatureVector,
  fitLogisticBinary,
  fitRidgeMulti,
  normalizeLongOnly,
  predictLinearMulti,
  predictLogistic,
  splitSamples,
} = require('../src/experiment');

function syntheticMarket() {
  const dates = ['2025-01-01', '2025-01-02', '2025-01-03'];
  const closes = new Map([
    ['A', [100, 110, 121]],
    ['BIL', [100, 100.01, 100.02]],
  ]);
  return { dates, tickers: ['A', 'BIL'], closes };
}

describe('pym v5 ml experiment helpers', () => {
  it('fits a small ridge model with multiple outputs', () => {
    const x = [[0], [1], [2], [3]];
    const y = [[1, 2], [3, 5], [5, 8], [7, 11]];
    const beta = fitRidgeMulti(x, y, 1e-9);
    const predicted = predictLinearMulti(beta, [4]);
    expect(predicted[0]).toBeCloseTo(9, 5);
    expect(predicted[1]).toBeCloseTo(14, 5);
  });

  it('fits a small ridge model with one output', () => {
    const x = [[0], [1], [2], [3]];
    const y = [[1], [3], [5], [7]];
    const beta = fitRidgeMulti(x, y, 1e-9);
    const predicted = predictLinearMulti(beta, [4]);
    expect(predicted[0]).toBeCloseTo(9, 5);
  });

  it('fits a logistic gate on separable rows', () => {
    const x = [[-2], [-1], [1], [2]];
    const y = [0, 0, 1, 1];
    const weights = fitLogisticBinary(x, y, { lambda: 0.01, iterations: 600, learningRate: 0.2 });
    expect(predictLogistic(weights, [-2])).toBeLessThan(0.5);
    expect(predictLogistic(weights, [2])).toBeGreaterThan(0.5);
  });

  it('normalizes positive predictions and falls back to safe asset', () => {
    expect(normalizeLongOnly({ A: -1, BIL: 0 }, ['A', 'BIL'], 'BIL')).toEqual({ BIL: 1 });
    const weights = normalizeLongOnly({ A: 2, BIL: 1 }, ['A', 'BIL'], 'BIL', 10);
    expect(weights.A).toBeCloseTo(2 / 3);
    expect(weights.BIL).toBeCloseTo(1 / 3);
  });

  it('backtests next-session policy returns with turnover costs', () => {
    const market = syntheticMarket();
    const samples = [{
      index: 1,
      date: '2025-01-02',
      nextDate: '2025-01-03',
      teacherWeights: { A: 1 },
    }];
    const result = backtestPolicy(samples, market, () => ({ A: 1 }), { initialCapital: 100, totalCostBps: 0 });
    expect(result.finalEquity).toBeCloseTo(110);
    expect(result.totalReturnPct).toBeCloseTo(10);
  });

  it('splits samples by date windows', () => {
    const samples = [
      { date: '2025-01-02' },
      { date: '2025-11-03' },
      { date: '2025-12-31' },
      { date: '2026-01-02' },
    ];
    const splits = splitSamples(samples, {
      trainEnd: '2025-10-31',
      validationStart: '2025-11-01',
      validationEnd: '2025-12-31',
      testStart: '2026-01-01',
    });
    expect(splits.train).toHaveLength(1);
    expect(splits.validation).toHaveLength(2);
    expect(splits.fit).toHaveLength(3);
    expect(splits.test).toHaveLength(1);
  });

  it('builds a stable feature vector', () => {
    const market = {
      dates: ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-06'],
      tickers: ['SPY', 'BIL'],
      closes: new Map([
        ['SPY', [100, 101, 99, 102]],
        ['BIL', [100, 100.01, 100.02, 100.03]],
      ]),
    };
    const sample = { index: 3, date: '2025-01-06', teacherWeights: { SPY: 0.5, BIL: 0.5 } };
    const context = {
      market,
      coreTickers: ['SPY', 'BIL'],
      outputTickers: ['BIL', 'SPY'],
      optionByDate: new Map(),
      optionRoots: [],
      optionFields: [],
      lookback: 3,
    };
    const feature = buildFeatureVector(sample, context, ['price', 'attention', 'pym'], true);
    expect(feature.values).toHaveLength(feature.names.length);
    expect(feature.values.every(Number.isFinite)).toBe(true);
  });
});
