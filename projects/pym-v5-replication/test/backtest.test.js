const { detectSplitRatio } = require('../src/backtest');

describe('raw Massive split adjustment helpers', () => {
  it('detects likely reverse splits without treating normal moves as splits', () => {
    const split = detectSplitRatio(4.17, 40.53);
    expect(split.ratio).toBe(10);
    expect(split.correctedReturn).toBeCloseTo(-0.027, 2);
    expect(detectSplitRatio(100, 112)).toBeNull();
  });
});
