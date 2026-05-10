const {
  blackScholesPrice,
  computeEquity,
  impliedVolatility,
  intrinsicValue,
  makeStrategyState,
  mergeStrategyConfig,
  normalizeSymbols,
  summarizeStrategy,
} = require('../src/wheel-backtest');

function fakeStockDay(symbol, close) {
  return {
    eodClose: new Map([[symbol, close]]),
  };
}

describe('wheel backtest accounting helpers', () => {
  it('normalizes equity symbols and excludes index roots', () => {
    expect(normalizeSymbols(['spy', 'SPX', 'AAPL', 'I:VIX', 'QQQ', 'VIXW', 'AAPL'])).toEqual([
      'AAPL',
      'QQQ',
      'SPY',
    ]);
  });

  it('computes option intrinsic value by right', () => {
    expect(intrinsicValue({ right: 'PUT', strike: 100 }, 95)).toBe(5);
    expect(intrinsicValue({ right: 'PUT', strike: 100 }, 105)).toBe(0);
    expect(intrinsicValue({ right: 'CALL', strike: 100 }, 105)).toBe(5);
    expect(intrinsicValue({ right: 'CALL', strike: 100 }, 95)).toBe(0);
  });

  it('round-trips Black-Scholes implied volatility estimates', () => {
    const price = blackScholesPrice({
      right: 'PUT',
      spot: 100,
      strike: 95,
      years: 30 / 365,
      volatility: 0.35,
    });
    const iv = impliedVolatility({
      right: 'PUT',
      spot: 100,
      strike: 95,
      years: 30 / 365,
      price,
    });
    expect(iv).toBeCloseTo(0.35, 3);
  });

  it('marks short-option liability against cash and shares', () => {
    const config = mergeStrategyConfig({
      id: 'unit_wheel',
      mode: 'wheel',
      label: 'Unit wheel',
      minDte: 5,
      maxDte: 10,
      putTargetMoneyness: 0.95,
      callTargetMoneyness: 1.05,
    }, { initialCapital: 100_000 });
    const state = makeStrategyState(config, 100_000);
    state.cash += 200;
    state.openShorts.push({
      symbol: 'SPY',
      right: 'PUT',
      strike: 95,
      contracts: 1,
      markPrice: 1.25,
    });
    const snapshot = computeEquity(state, fakeStockDay('SPY', 100));
    expect(snapshot.equity).toBe(100_075);
    expect(snapshot.optionLiability).toBe(125);
    expect(snapshot.reservedCollateral).toBe(9_500);
  });

  it('summarizes daily returns and drawdown', () => {
    const config = mergeStrategyConfig({
      id: 'unit_cash_put',
      mode: 'cash_put',
      label: 'Unit cash put',
      minDte: 5,
      maxDte: 10,
      putTargetMoneyness: 0.95,
    }, { initialCapital: 100_000 });
    const state = makeStrategyState(config, 100_000);
    state.daily.push({ date: '2026-01-02', equity: 101_000, dailyReturn: 0.01 });
    state.daily.push({ date: '2026-01-05', equity: 99_000, dailyReturn: -0.01980198 });
    const summary = summarizeStrategy(state);
    expect(summary.totalReturn).toBe(-0.01);
    expect(summary.maxDrawdown).toBe(-0.019802);
  });
});
