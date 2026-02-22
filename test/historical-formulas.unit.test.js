const {
  computeExecutionFlags,
  computeSentiment,
  isStandardMonthly,
  computeValue,
  computeDte,
  computeSigScore,
  isAmSpikeWindow,
} = require('../src/historical-formulas');

describe('historical formulas', () => {
  it('classifies bid/ask/AA boundaries correctly', () => {
    const aa = computeExecutionFlags({ right: 'CALL', price: 1.13, bid: 1.0, ask: 1.1 });
    const ask = computeExecutionFlags({ right: 'CALL', price: 1.1, bid: 1.0, ask: 1.1 });
    const bid = computeExecutionFlags({ right: 'PUT', price: 1.0, bid: 1.0, ask: 1.1 });

    expect(aa).toMatchObject({ aa: true, ask: false, bid: false, calls: true });
    expect(ask).toMatchObject({ aa: false, ask: true, bid: false });
    expect(bid).toMatchObject({ aa: false, ask: false, bid: true, puts: true });
  });

  it('maps right + side to sentiment', () => {
    expect(computeSentiment({ right: 'CALL', executionSide: 'ASK' })).toBe('bullish');
    expect(computeSentiment({ right: 'PUT', executionSide: 'ASK' })).toBe('bearish');
    expect(computeSentiment({ right: 'CALL', executionSide: 'OTHER' })).toBe('neutral');
  });

  it('detects standard monthly options and weeklies', () => {
    expect(isStandardMonthly('2026-02-20')).toBe(true);
    expect(isStandardMonthly('2026-02-13')).toBe(false);
  });

  it('computes deterministic numeric formulas', () => {
    expect(computeValue(1.25, 100)).toBe(12500);
    expect(computeDte('2026-02-13T14:35:00.000Z', '2026-02-20')).toBeGreaterThanOrEqual(6);

    const score = computeSigScore({ valuePctile: 0.8, volOiNorm: 0.5, repeatNorm: 1, otmNorm: 0.2, sideConfidence: 0.9, dteNorm: 0.5, spreadNorm: 0.3 });
    expect(score).toBeGreaterThan(0.5);
    expect(score).toBeLessThan(1);

    // Verify new components contribute to score
    const scoreWithoutNew = computeSigScore({ valuePctile: 0.8, volOiNorm: 0.5, repeatNorm: 1, otmNorm: 0.2, sideConfidence: 0.9, dteNorm: 0, spreadNorm: 0 });
    const scoreWithNew = computeSigScore({ valuePctile: 0.8, volOiNorm: 0.5, repeatNorm: 1, otmNorm: 0.2, sideConfidence: 0.9, dteNorm: 1, spreadNorm: 1 });
    expect(scoreWithNew).toBeGreaterThan(scoreWithoutNew);
  });

  it('detects AM spike window in ET', () => {
    expect(isAmSpikeWindow('2026-02-13T14:45:00.000Z')).toBe(true);
    expect(isAmSpikeWindow('2026-02-13T17:45:00.000Z')).toBe(false);
  });
});
