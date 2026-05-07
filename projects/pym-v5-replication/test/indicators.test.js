const {
  cumulativeReturn,
  maxDrawdown,
  movingAveragePrice,
  relativeStrengthIndex,
} = require('../src/indicators');

function ctx(closes) {
  return { closes: new Map([['SPY', closes]]) };
}

describe('Composer-style daily indicators', () => {
  it('uses percent units for cumulative return', () => {
    expect(cumulativeReturn(ctx([100, 105, 110]), 'SPY', 2, 2)).toBeCloseTo(10);
  });

  it('computes moving average price over the inclusive trailing window', () => {
    expect(movingAveragePrice(ctx([100, 110, 120]), 'SPY', 2, 2)).toBeCloseTo(115);
  });

  it('computes RSI on a 0-100 scale', () => {
    expect(relativeStrengthIndex(ctx([100, 101, 102, 103]), 'SPY', 3, 3)).toBeCloseTo(100);
    expect(relativeStrengthIndex(ctx([103, 102, 101, 100]), 'SPY', 3, 3)).toBeCloseTo(0);
  });

  it('reports max drawdown as a negative percent', () => {
    expect(maxDrawdown(ctx([100, 110, 99, 120]), 'SPY', 3, 4)).toBeCloseTo(-10);
  });
});
