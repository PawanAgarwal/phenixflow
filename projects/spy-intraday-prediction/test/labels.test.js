const { assignForwardLabels, assignTripleBarrierLabels } = require('../src/labels');
const { computePredictionMetrics } = require('../src/metrics');
const { computePolicyBacktest, selectPredictionsByHorizon } = require('../src/backtest');

describe('labels, metrics, and policy math', () => {
  it('assigns causal forward labels inside each trade date', () => {
    const rows = [
      { tradeDate: '2026-01-02', minuteMs: 1, spy_close: 100 },
      { tradeDate: '2026-01-02', minuteMs: 2, spy_close: 101 },
      { tradeDate: '2026-01-02', minuteMs: 3, spy_close: 102 },
      { tradeDate: '2026-01-05', minuteMs: 4, spy_close: 200 },
      { tradeDate: '2026-01-05', minuteMs: 5, spy_close: 198 },
    ];
    assignForwardLabels(rows, [
      { name: 'next_1m', minutes: 1 },
      { name: 'eod_close', minutes: 'eod' },
    ]);
    expect(rows[0].label_next_1m_return).toBeCloseTo(0.01, 10);
    expect(rows[2].label_next_1m_return).toBeNull();
    expect(rows[0].label_eod_close_return).toBeCloseTo(0.02, 10);
    expect(rows[2].label_eod_close_return).toBeNull();
    expect(rows[3].label_next_1m_return).toBeCloseTo(-0.01, 10);
    expect(rows[0].label_abs_return_5m_return).toBeNull();
  });

  it('assigns last-30m and magnitude labels', () => {
    const rows = [
      { tradeDate: '2026-01-02', minuteMs: 1, minuteOfDayEt: 929, spy_close: 100, high: 100, low: 100 },
      { tradeDate: '2026-01-02', minuteMs: 2, minuteOfDayEt: 930, spy_close: 101, high: 101, low: 101 },
      { tradeDate: '2026-01-02', minuteMs: 3, minuteOfDayEt: 931, spy_close: 102, high: 102, low: 102 },
      { tradeDate: '2026-01-02', minuteMs: 4, minuteOfDayEt: 959, spy_close: 103, high: 103, low: 103 },
    ];
    assignForwardLabels(rows, [{ name: 'eod_close', minutes: 'eod' }], { lastThirtyEntryMinuteEt: 930 });
    expect(rows[0].label_last_30m_return).toBeNull();
    expect(rows[1].label_last_30m_return).toBeCloseTo((103 / 101) - 1, 10);
    expect(rows[1].label_abs_return_eod_return).toBeCloseTo(Math.abs((103 / 101) - 1), 10);
  });

  it('assigns triple-barrier labels from the first path touch', () => {
    const rows = [
      { tradeDate: '2026-01-02', minuteMs: 1, minuteOfDayEt: 570, spy_close: 100, high: 100, low: 100 },
      { tradeDate: '2026-01-02', minuteMs: 2, minuteOfDayEt: 571, spy_close: 100.1, high: 100.3, low: 100 },
      { tradeDate: '2026-01-02', minuteMs: 3, minuteOfDayEt: 572, spy_close: 99.5, high: 99.8, low: 99.4 },
    ];
    assignTripleBarrierLabels(rows, { horizons: [2], volatilityMultiple: 1, minimumBarrierBps: 20, volatilityLookbackMinutes: 2 });
    expect(rows[0].label_tb_2m_direction).toBe(1);
    expect(rows[0].label_tb_2m_hit).toBe('upper');
    expect(rows[0].label_tb_2m_profitable_long).toBe(1);
  });

  it('computes accuracy-first metrics and cost-aware policy stats', () => {
    const predictions = [
      { tradeDate: '2026-02-02', minuteUtc: '2026-02-02T14:30:00.000Z', actualReturn: 0.001, actualDirection: 1, predictedReturn: 0.001, predictedDirection: 1, directionProbability: 0.60, confidence: 0.60 },
      { tradeDate: '2026-02-02', minuteUtc: '2026-02-02T14:31:00.000Z', actualReturn: -0.002, actualDirection: 0, predictedReturn: -0.001, predictedDirection: 0, directionProbability: 0.40, confidence: 0.60 },
      { tradeDate: '2026-02-02', minuteUtc: '2026-02-02T14:32:00.000Z', actualReturn: 0.001, actualDirection: 1, predictedReturn: -0.001, predictedDirection: 0, directionProbability: 0.44, confidence: 0.56 },
    ];
    const metrics = computePredictionMetrics(predictions);
    expect(metrics.directionalAccuracy).toBeCloseTo(2 / 3, 10);
    expect(metrics.balancedAccuracy).toBeCloseTo(0.75, 10);
    expect(metrics.confusion).toEqual({ tp: 1, tn: 1, fp: 0, fn: 1 });

    const backtest = computePolicyBacktest(predictions, { confidenceThreshold: 0.55, transactionCostBps: 0, slippageBps: 0 });
    expect(backtest.observations).toBe(3);
    expect(backtest.longShare).toBeCloseTo(1 / 3, 10);
    expect(backtest.shortShare).toBeCloseTo(2 / 3, 10);
    expect(backtest.totalReturn).toBeGreaterThan(0);
  });

  it('samples overlapping horizons with horizon-aware execution policy', () => {
    const predictions = Array.from({ length: 10 }, (_, index) => ({
      tradeDate: '2026-02-02',
      minuteUtc: `2026-02-02T14:${String(30 + index).padStart(2, '0')}:00.000Z`,
      minuteOfDayEt: 570 + index,
      actualReturn: 0.001,
      actualDirection: 1,
      predictedReturn: 0.001,
      predictedDirection: 1,
      directionProbability: 0.6,
      confidence: 0.6,
    }));
    expect(selectPredictionsByHorizon(predictions, 'next_5m').selected).toHaveLength(2);
    const backtest = computePolicyBacktest(predictions, { horizonName: 'next_5m', transactionCostBps: 0, slippageBps: 0 });
    expect(backtest.inputObservations).toBe(10);
    expect(backtest.observations).toBe(2);
    expect(backtest.executionPolicy.mode).toBe('step');
  });
});
