const {
  rsiFromRows,
  tickerFeatures,
  signedVolumeImbalanceFromRows,
  volumeRatioFromRows,
} = require('../src/multitimeframe-prediction');

function rows(values) {
  return values.map((close, index) => ({ minute: 570 + index, close }));
}

describe('multi-timeframe prediction helpers', () => {
  it('computes bounded RSI from a fixed bar window', () => {
    const value = rsiFromRows(rows([
      100, 101, 102, 101, 103, 104, 103, 105, 106, 107, 106,
      108, 109, 110, 111, 110, 112, 113, 114, 115, 116,
    ]), 20);
    expect(value).toBeGreaterThan(50);
    expect(value).toBeLessThanOrEqual(100);
  });

  it('aligns 1m and 5m features on the same 20-bar length', () => {
    const minuteMap = new Map();
    for (let minute = 570; minute <= 675; minute += 1) {
      minuteMap.set(minute, {
        close: 100 + ((minute - 570) * 0.1),
        volume: 100,
      });
    }
    const features = tickerFeatures({
      barsByTicker: new Map([['SPY', minuteMap]]),
    }, 'SPY', 675);
    expect(features.rsi1m20).toBe(100);
    expect(features.rsi5m20).toBe(100);
    expect(features.ret1m20).toBeGreaterThan(0);
    expect(features.ret5m20).toBeGreaterThan(0);
    expect(features.aboveVwap).toBe(true);
    expect(features.volumeRatio20).toBeCloseTo(1);
    expect(features.signedVolumeImbalance20).toBe(1);
  });

  it('computes causal relative-volume and signed-volume features', () => {
    const featureRows = rows([
      100, 101, 102, 101, 103, 104, 103, 105, 106, 107, 106,
      108, 109, 110, 111, 110, 112, 113, 114, 115, 116,
    ]).map((row, index) => ({
      ...row,
      volume: index === 20 ? 400 : 100,
    }));
    expect(volumeRatioFromRows(featureRows, 20)).toBeCloseTo(4);
    expect(signedVolumeImbalanceFromRows(featureRows, 20)).toBeGreaterThan(0);
  });
});
