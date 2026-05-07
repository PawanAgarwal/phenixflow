const {
  applyMagnitudeGate,
  computeRegimeDiagnostics,
  splitRowsForValidation,
} = require('../src/phase2-research');

function prediction(row, probability, actualReturn = 0.001) {
  return {
    row,
    actualReturn,
    actualDirection: actualReturn > 0 ? 1 : 0,
    predictedDirection: probability >= 0.5 ? 1 : 0,
    directionProbability: probability,
    predictedReturn: probability >= 0.5 ? 0.001 : -0.001,
    confidence: Math.max(probability, 1 - probability),
  };
}

describe('phase2 helpers', () => {
  it('splits the final dates into validation rows', () => {
    const rows = Array.from({ length: 10 }, (_, index) => ({
      tradeDate: `2026-01-${String(index + 1).padStart(2, '0')}`,
      rowId: String(index),
    }));
    const split = splitRowsForValidation(rows, 0.3);
    expect(split.fitRows).toHaveLength(7);
    expect(split.validationRows).toHaveLength(3);
    expect(split.validationRows[0].tradeDate).toBe('2026-01-08');
  });

  it('turns low-magnitude predictions into cash predictions', () => {
    const rows = [
      { rowId: 'a', tradeDate: '2026-02-02', minuteUtc: '2026-02-02T14:30:00Z' },
      { rowId: 'b', tradeDate: '2026-02-02', minuteUtc: '2026-02-02T14:35:00Z' },
    ];
    const gated = applyMagnitudeGate(
      [prediction(rows[0], 0.7), prediction(rows[1], 0.7)],
      [prediction(rows[0], 0.8), prediction(rows[1], 0.4)],
      0.6,
    );
    expect(gated[0].accepted).toBe(true);
    expect(gated[0].directionProbability).toBe(0.7);
    expect(gated[1].accepted).toBe(false);
    expect(gated[1].directionProbability).toBe(0.5);
    expect(gated[1].predictedReturn).toBe(0);
  });

  it('builds regime diagnostics with prediction metrics and policy stats', () => {
    const config = {
      execution: { confidenceThreshold: 0.52, transactionCostBps: 1, slippageBps: 1 },
    };
    const predictions = Array.from({ length: 30 }, (_, index) => prediction({
      rowId: String(index),
      tradeDate: '2026-02-02',
      minuteUtc: `2026-02-02T15:${String(index).padStart(2, '0')}:00Z`,
      minuteOfDayEt: 570 + index,
      vix_close: 15 + index,
      vix1d_over_vix: 0.8 + (index / 100),
      vix9d_over_vix: 0.9 + (index / 100),
      spy_rv_30m: index / 10_000,
      gamma_proxy_short_dte_pressure: index,
      opening_30m_return: index % 2 ? 0.001 : -0.001,
      opt_spy_trade_premium_imbalance: index % 3 ? 0.2 : -0.2,
    }, index % 2 ? 0.6 : 0.4, index % 2 ? 0.001 : -0.001));
    const diagnostics = computeRegimeDiagnostics(predictions, config, 'eod_close');
    expect(diagnostics.some((item) => item.regime === 'time_of_day')).toBe(true);
    expect(diagnostics.some((item) => item.regime === 'vix_close')).toBe(true);
    expect(diagnostics[0].metrics.count).toBeGreaterThan(0);
    expect(diagnostics[0].backtest.observations).toBeGreaterThan(0);
  });
});
