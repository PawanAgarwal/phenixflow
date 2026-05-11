// Black-Scholes pricer with continuous dividend yield + IV solver + first/second-order greeks.
// All time inputs are in years. Vega/Theta/Vanna/Charm/Vomma are returned in "per unit" terms;
// caller can scale (e.g. theta_per_day = theta_annual / 365.25; vega_per_1pct = vega_annual / 100).

const SQRT_2PI = Math.sqrt(2 * Math.PI);

function normPdf(x) {
  return Math.exp(-0.5 * x * x) / SQRT_2PI;
}

// Abramowitz-Stegun 7.1.26 approximation for erf; CDF derived from it.
function erf(x) {
  const sign = x < 0 ? -1 : 1;
  const ax = Math.abs(x);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;
  const t = 1 / (1 + p * ax);
  const y = 1 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * Math.exp(-ax * ax);
  return sign * y;
}

function normCdf(x) {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

function isCallRight(right) {
  if (right === true) return true;
  if (right === false) return false;
  const r = String(right || '').toUpperCase();
  return r === 'C' || r === 'CALL';
}

function intrinsicValue({ spot, strike, right }) {
  if (isCallRight(right)) return Math.max(spot - strike, 0);
  return Math.max(strike - spot, 0);
}

// d1, d2 with continuous dividend q
function computeD1D2({ spot, strike, T, sigma, r, q }) {
  if (sigma <= 0 || T <= 0 || spot <= 0 || strike <= 0) return null;
  const sigmaSqrtT = sigma * Math.sqrt(T);
  const d1 = (Math.log(spot / strike) + (r - q + 0.5 * sigma * sigma) * T) / sigmaSqrtT;
  const d2 = d1 - sigmaSqrtT;
  return { d1, d2, sigmaSqrtT };
}

function bsPrice({ spot, strike, T, sigma, r, q, right }) {
  if (T <= 0) return intrinsicValue({ spot, strike, right });
  const d = computeD1D2({ spot, strike, T, sigma, r, q });
  if (!d) return intrinsicValue({ spot, strike, right });
  const { d1, d2 } = d;
  const discS = Math.exp(-q * T);
  const discK = Math.exp(-r * T);
  if (isCallRight(right)) {
    return spot * discS * normCdf(d1) - strike * discK * normCdf(d2);
  }
  return strike * discK * normCdf(-d2) - spot * discS * normCdf(-d1);
}

function bsVegaAnnual({ spot, T, sigma, r, q, strike }) {
  if (T <= 0 || sigma <= 0) return 0;
  const d = computeD1D2({ spot, strike, T, sigma, r, q });
  if (!d) return 0;
  return spot * Math.exp(-q * T) * normPdf(d.d1) * Math.sqrt(T);
}

// Solve implied volatility from target price.
// Newton-Raphson with vega; fall back to bisection on bracketed bounds if NR misbehaves.
// Returns { iv, status } where status ∈ {ok, intrinsic, no_solution, low_vega, out_of_bounds}.
function solveImpliedVol({
  targetPrice,
  spot,
  strike,
  T,
  r,
  q,
  right,
  initialGuess = 0.25,
  minSigma = 0.01,
  maxSigma = 5.0,
  tolerance = 1e-4,
  maxIterations = 32,
}) {
  if (!Number.isFinite(targetPrice) || targetPrice <= 0) return { iv: null, status: 'no_price' };
  if (!(spot > 0) || !(strike > 0) || !(T > 0)) return { iv: null, status: 'bad_inputs' };

  const intrinsic = intrinsicValue({ spot, strike, right });
  // If the trade prints essentially at intrinsic, IV is ill-defined — flag and skip.
  if (targetPrice <= intrinsic + 1e-4) return { iv: null, status: 'intrinsic' };

  // Theoretical upper bound: for a call ≈ S * e^(-qT); for a put ≈ K * e^(-rT)
  const upperBound = isCallRight(right)
    ? spot * Math.exp(-q * T)
    : strike * Math.exp(-r * T);
  if (targetPrice >= upperBound - 1e-6) return { iv: null, status: 'above_max' };

  let sigma = Math.max(minSigma, Math.min(maxSigma, initialGuess));
  let lo = minSigma;
  let hi = maxSigma;
  let priceLo = bsPrice({ spot, strike, T, sigma: lo, r, q, right });
  let priceHi = bsPrice({ spot, strike, T, sigma: hi, r, q, right });
  if (priceLo > targetPrice) return { iv: lo, status: 'below_min_iv' };
  if (priceHi < targetPrice) return { iv: hi, status: 'above_max_iv' };

  for (let i = 0; i < maxIterations; i += 1) {
    const price = bsPrice({ spot, strike, T, sigma, r, q, right });
    const diff = price - targetPrice;
    if (Math.abs(diff) < tolerance) {
      return { iv: sigma, status: 'ok' };
    }
    // Maintain bracket
    if (diff < 0) {
      lo = sigma;
      priceLo = price;
    } else {
      hi = sigma;
      priceHi = price;
    }
    const vega = bsVegaAnnual({ spot, strike, T, sigma, r, q });
    let nextSigma;
    if (vega > 1e-8) {
      nextSigma = sigma - diff / vega;
    } else {
      nextSigma = 0.5 * (lo + hi);
    }
    // If NR steps outside bracket or makes minimal progress, fall back to bisection
    if (!(nextSigma > lo && nextSigma < hi)) {
      nextSigma = 0.5 * (lo + hi);
    }
    if (Math.abs(nextSigma - sigma) < 1e-8) {
      return { iv: nextSigma, status: 'low_vega' };
    }
    sigma = nextSigma;
  }
  return { iv: sigma, status: 'no_solution' };
}

// Full greek set from sigma. Returns annualized vega/theta/charm; caller scales.
function computeGreeks({ spot, strike, T, sigma, r, q, right }) {
  if (!(T > 0) || !(sigma > 0)) {
    return {
      delta: null,
      gamma: null,
      vega_annual: null,
      theta_annual: null,
      vanna: null,
      charm_annual: null,
      vomma: null,
    };
  }
  const d = computeD1D2({ spot, strike, T, sigma, r, q });
  if (!d) {
    return {
      delta: null,
      gamma: null,
      vega_annual: null,
      theta_annual: null,
      vanna: null,
      charm_annual: null,
      vomma: null,
    };
  }
  const { d1, d2 } = d;
  const sqrtT = Math.sqrt(T);
  const phiD1 = normPdf(d1);
  const discS = Math.exp(-q * T);
  const discK = Math.exp(-r * T);
  const isCall = isCallRight(right);

  const delta = isCall ? discS * normCdf(d1) : discS * (normCdf(d1) - 1);
  const gamma = (discS * phiD1) / (spot * sigma * sqrtT);
  const vegaAnnual = spot * discS * phiD1 * sqrtT; // per 1.0 unit of sigma
  // Vomma = ∂Vega/∂σ
  const vomma = vegaAnnual * (d1 * d2) / sigma;
  // Vanna = ∂Delta/∂σ
  const vanna = -discS * phiD1 * (d2 / sigma);

  let thetaAnnual;
  let charmAnnual;
  if (isCall) {
    thetaAnnual = -(spot * discS * phiD1 * sigma) / (2 * sqrtT)
      - r * strike * discK * normCdf(d2)
      + q * spot * discS * normCdf(d1);
    charmAnnual = -q * discS * normCdf(d1)
      + discS * phiD1 * (2 * (r - q) * T - d2 * sigma * sqrtT) / (2 * T * sigma * sqrtT);
    charmAnnual = -charmAnnual; // standard convention: ∂Δ/∂t where t is time-to-expiry; flip sign
  } else {
    thetaAnnual = -(spot * discS * phiD1 * sigma) / (2 * sqrtT)
      + r * strike * discK * normCdf(-d2)
      - q * spot * discS * normCdf(-d1);
    charmAnnual = q * discS * normCdf(-d1)
      + discS * phiD1 * (2 * (r - q) * T - d2 * sigma * sqrtT) / (2 * T * sigma * sqrtT);
    charmAnnual = -charmAnnual;
  }

  return {
    delta,
    gamma,
    vega_annual: vegaAnnual,
    theta_annual: thetaAnnual,
    vanna,
    charm_annual: charmAnnual,
    vomma,
  };
}

module.exports = {
  normPdf,
  normCdf,
  bsPrice,
  bsVegaAnnual,
  intrinsicValue,
  solveImpliedVol,
  computeGreeks,
  isCallRight,
};
