const {
  normCdf,
  bsPrice,
  solveImpliedVol,
  computeGreeks,
  intrinsicValue,
} = require('../src/greeks');

const R = 0.045; // risk-free
const Q = 0.013; // SPY dividend yield (~)

describe('Black-Scholes greeks', () => {
  it('approximates standard normal CDF', () => {
    expect(normCdf(0)).toBeCloseTo(0.5, 6);
    expect(normCdf(1.96)).toBeCloseTo(0.975, 3);
    expect(normCdf(-1.96)).toBeCloseTo(0.025, 3);
  });

  it('prices an ATM SPY call near the textbook value', () => {
    // 30-day ATM call on $500 spot, 25% vol, ~5% r, ~1.3% q
    const spot = 500;
    const strike = 500;
    const T = 30 / 365.25;
    const sigma = 0.25;
    const price = bsPrice({ spot, strike, T, sigma, r: R, q: Q, right: 'CALL' });
    // Sanity bounds: ATM call ~ 0.4 * S * sigma * sqrt(T) heuristic ≈ 14.4
    expect(price).toBeGreaterThan(12);
    expect(price).toBeLessThan(17);
  });

  it('puts and calls satisfy put-call parity', () => {
    const spot = 500;
    const strike = 510;
    const T = 14 / 365.25;
    const sigma = 0.18;
    const call = bsPrice({ spot, strike, T, sigma, r: R, q: Q, right: 'CALL' });
    const put = bsPrice({ spot, strike, T, sigma, r: R, q: Q, right: 'PUT' });
    // C - P = S*e^(-qT) - K*e^(-rT)
    const expected = spot * Math.exp(-Q * T) - strike * Math.exp(-R * T);
    expect(call - put).toBeCloseTo(expected, 3);
  });

  it('recovers IV from a synthetic price within 1e-3', () => {
    const trueSigma = 0.27;
    const spot = 500;
    const strike = 495;
    const T = 7 / 365.25;
    const synth = bsPrice({ spot, strike, T, sigma: trueSigma, r: R, q: Q, right: 'CALL' });
    const { iv, status } = solveImpliedVol({
      targetPrice: synth,
      spot,
      strike,
      T,
      r: R,
      q: Q,
      right: 'CALL',
    });
    expect(status).toBe('ok');
    expect(iv).toBeCloseTo(trueSigma, 3);
  });

  it('flags intrinsic-only puts as unsolvable', () => {
    const spot = 500;
    const strike = 510;
    const T = 1 / 365.25;
    const intrinsic = intrinsicValue({ spot, strike, right: 'PUT' });
    const { status } = solveImpliedVol({
      targetPrice: intrinsic,
      spot,
      strike,
      T,
      r: R,
      q: Q,
      right: 'PUT',
    });
    expect(status).toBe('intrinsic');
  });

  it('produces sensible greeks for an ATM call', () => {
    const spot = 500;
    const strike = 500;
    const T = 30 / 365.25;
    const sigma = 0.20;
    const g = computeGreeks({ spot, strike, T, sigma, r: R, q: Q, right: 'CALL' });
    // ATM call delta ~0.52 (with positive carry)
    expect(g.delta).toBeGreaterThan(0.45);
    expect(g.delta).toBeLessThan(0.60);
    expect(g.gamma).toBeGreaterThan(0);
    expect(g.vega_annual).toBeGreaterThan(0);
    expect(g.theta_annual).toBeLessThan(0);
    // Vanna for ATM is near zero
    expect(Math.abs(g.vanna)).toBeLessThan(0.5);
  });

  it('OTM call has lower delta than ATM call', () => {
    const T = 14 / 365.25;
    const atm = computeGreeks({ spot: 500, strike: 500, T, sigma: 0.20, r: R, q: Q, right: 'CALL' });
    const otm = computeGreeks({ spot: 500, strike: 520, T, sigma: 0.20, r: R, q: Q, right: 'CALL' });
    expect(otm.delta).toBeLessThan(atm.delta);
    expect(otm.delta).toBeGreaterThan(0);
  });
});
