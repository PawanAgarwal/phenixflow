const {
  inverseTargetWeights,
  normalizeWeights,
  returnLookback,
  returnSinceOpen,
} = require('../src/intraday-strategies');

function ctx() {
  return {
    minuteEt: 600,
    minutes: [570, 585, 600],
    dayBars: {
      barsByTicker: new Map([
        ['SPY', new Map([
          [570, { close: 100, volume: 10 }],
          [585, { close: 101, volume: 10 }],
          [600, { close: 102, volume: 10 }],
        ])],
      ]),
    },
  };
}

describe('intraday strategy helpers', () => {
  it('normalizes active weights to target exposure', () => {
    const weights = normalizeWeights(new Map([['SPY', 2], ['QQQ', 1]]), 0.9);
    expect(weights.get('SPY')).toBeCloseTo(0.6);
    expect(weights.get('QQQ')).toBeCloseTo(0.3);
  });

  it('calculates returns from open and lookback minute closes', () => {
    expect(returnSinceOpen(ctx(), 'SPY')).toBeCloseTo(0.02);
    expect(returnLookback(ctx(), 'SPY', 15)).toBeCloseTo(1 / 101);
  });

  it('maps PYM targets to inverse intraday counterparts', () => {
    const weights = inverseTargetWeights({
      holdings: [
        { ticker: 'VIXY', weight: 0.2 },
        { ticker: 'UVXY', weight: 0.1 },
        { ticker: 'EDZ', weight: 0.3 },
      ],
    });
    expect(weights.get('SVIX')).toBeCloseTo(0.3);
    expect(weights.get('EDC')).toBeCloseTo(0.3);
  });
});
