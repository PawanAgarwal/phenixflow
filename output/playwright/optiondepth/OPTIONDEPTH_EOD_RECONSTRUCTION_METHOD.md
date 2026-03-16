# OptionDepth EOD Reconstruction Method

Captured on 2026-03-15.

Purpose: define a concrete methodology for reconstructing an `EOD` OptionDepth-style heatmap and its derived levels from our ThetaData-backed warehouse, including:

- pricing and exposure formulas
- `DEX`, `GEX`, `VEX`, `CEX`, and related surfaces
- `speed`, `color`, `centroid`, `pivots`, `vanna flips`, and `speed flips`
- hidden-pattern inference rules
- data requirements and exact mapping to current ClickHouse tables

This document is written to be implementable inside PhenixFlow.

It separates:

- exact formulas we can use with confidence
- recommended numerical definitions
- educated-guess extensions for the more proprietary parts of Alma's framework

## 1. What We Are Reconstructing

The target object is a next-session `EOD` surface built from the prior close.

At a high level:

1. Build an end-of-day chain snapshot.
2. Estimate signed customer positioning by contract.
3. Convert customer positioning into market-maker inventory.
4. Reprice the inventory on a spot-time grid for the next session.
5. Aggregate Greek exposures into a heatmap.
6. Differentiate the surface to get `speed`, `color`, and flip lines.
7. Reduce the surface into trading levels:
   - centroid
   - downside pivot
   - upside pivot
   - targets
   - local pin zones
   - vanna/speed flips

## 2. Conventions

### 2.1 Contract-level notation

For contract `i`:

- `K_i` = strike
- `tau_i(t)` = time to expiry in years at time `t`
- `r_i(t)` = risk-free rate
- `q_i(t)` = dividend yield
- `sigma_i(S,t)` = implied volatility
- `M_i` = contract multiplier
- `right_i in {CALL, PUT}`

Underlying:

- `S` = spot level on the heatmap y-axis
- `S0` = end-of-day reference spot

Positioning:

- `Q_i^cust` = estimated net customer contracts
- `Q_i^mm = -Q_i^cust` = estimated market-maker contracts

Sign convention:

- `Q_i^cust > 0` means customers are net long options
- `Q_i^mm > 0` means market makers are net long options

### 2.2 Time convention

Use forward calendar time for the projected next session:

- snapshot taken at end of day `d`
- evaluate heatmap on `t in session(d+1)`
- `tau_i(t) = max((expiry_ts_i - t) / year_basis, eps)`

Recommended:

- `year_basis = 365.25`
- `eps = 1e-8`

### 2.3 Pricing-engine convention

For consistency with our current warehouse:

- base model: Black-Scholes-Merton with continuous dividend yield
- use the same inputs as `options.option_calculated_greeks_minute`

Practical note:

- this is exact enough for `SPX`, `VIX`, `SPY`, `QQQ`, `IWM` style research
- for single-name American options with material dividend effects, later versions should support an American approximation

## 3. End-of-Day Snapshot Construction

### 3.1 Snapshot timestamp

For each underlying and trade date `d`:

1. `S0` = last reliable underlying close from the final regular-trading-hours minute.
2. Option quotes = last reliable quote minute for each contract on `d`.
3. `OI` = end-of-day open interest for `d`.
4. `sigma` / first-order greeks = last reliable calculated-greeks row for `d`.

Recommended snapshot rules:

- use the last minute at or before `15:59 ET`
- drop stale contracts whose last quote is older than a configured tolerance
- exclude contracts with invalid or absurd mids

### 3.2 Snapshot fields required per contract

For each contract, keep:

- `symbol`
- `expiration`
- `strike`
- `option_right`
- `bid`
- `ask`
- `last`
- `mid_price`
- `underlying_price`
- `time_to_expiry_years`
- `implied_vol`
- `delta`
- `gamma`
- `vega`
- `theta`
- `risk_free_rate`
- `dividend_yield`
- `oi`

## 4. Pricing And Greek Formulas

### 4.1 Black-Scholes-Merton core terms

Let:

```text
F = S * exp((r - q) * tau)
d1 = (ln(F / K) + 0.5 * sigma^2 * tau) / (sigma * sqrt(tau))
d2 = d1 - sigma * sqrt(tau)
phi(x) = exp(-x^2 / 2) / sqrt(2*pi)
N(x) = standard normal CDF
```

Equivalent `d1` form:

```text
d1 = [ln(S / K) + (r - q + 0.5*sigma^2)*tau] / [sigma*sqrt(tau)]
```

### 4.2 Price

Call:

```text
C = exp(-q*tau) * S * N(d1) - exp(-r*tau) * K * N(d2)
```

Put:

```text
P = exp(-r*tau) * K * N(-d2) - exp(-q*tau) * S * N(-d1)
```

### 4.3 Delta

Call:

```text
Delta_call = exp(-q*tau) * N(d1)
```

Put:

```text
Delta_put = exp(-q*tau) * (N(d1) - 1)
```

### 4.4 Gamma

```text
Gamma = exp(-q*tau) * phi(d1) / (S * sigma * sqrt(tau))
```

### 4.5 Vega

In price units per `1.00` volatility:

```text
Vega = S * exp(-q*tau) * phi(d1) * sqrt(tau)
```

Per `1 vol point`:

```text
Vega_1pct = Vega * 0.01
```

### 4.6 Higher-order Greeks we should use directly

Vanna:

```text
Vanna = dDelta / dSigma = dVega / dS
      = Vega / S * (1 - d1 / (sigma * sqrt(tau)))
      = -exp(-q*tau) * phi(d1) * d2 / sigma
```

Vomma:

```text
Vomma = dVega / dSigma
      = Vega * d1 * d2 / sigma
```

Zomma:

```text
Zomma = dGamma / dSigma
      = Gamma * (d1 * d2 - 1) / sigma
```

Speed:

```text
Speed = dGamma / dS
      = -(Gamma / S) * (d1 / (sigma * sqrt(tau)) + 1)
```

These higher-order identities are standard BSM results. For a general higher-order differentiation reference that supports the implementation choice of mixing closed forms with numerical derivatives, see the appendix paper `vibrato-and-automatic-differentiation-for-high-order-derivatives-and-sensitivities-of-financial-options-arxiv-1606.06143.pdf`.

## 5. Greeks Best Computed Numerically

For `charm` and `color`, sign conventions differ across literature because some definitions use calendar time while others use time-to-expiry.

To avoid silent sign bugs, the preferred implementation is numerical forward-time differencing on the same pricing engine.

### 5.1 Forward-time charm

Let `dt` be a forward calendar step and `tau' = max(tau - dt, eps)`.

```text
Charm_fwd(S,t) ~= [Delta(S, tau', sigma') - Delta(S, tau, sigma)] / dt
```

Interpretation:

- this is the change in delta as time passes forward
- it matches Alma's use of charm as a time-passage effect

### 5.2 Forward-time color

```text
Color_fwd(S,t) ~= [Gamma(S, tau', sigma') - Gamma(S, tau, sigma)] / dt
```

Interpretation:

- this is gamma decay as time passes forward
- it matches Alma's use of color as gamma decay / slope of charm

### 5.3 Why numerical is preferred here

- it keeps sign conventions explicit
- it naturally incorporates sticky-strike or sticky-moneyness IV assumptions
- it aligns with Alma's surface-based language better than an isolated closed form

## 6. Volatility-Surface Assumption

This is one of the most important choices in the whole build.

### 6.1 Minimal base case: sticky strike

Hold each contract's EOD IV fixed:

```text
sigma_i(S,t) = sigma_i,EOD
```

Use this for:

- first heatmap prototype
- fast backtests
- simple gamma maps

### 6.2 Better base case: sticky moneyness / sticky delta

Fit a smoothed surface:

```text
sigma_hat = sigma_hat(m, tau)
m = ln(K / F)
F = S * exp((r - q) * tau)
```

Then evaluate each contract at node `(S,t)` as:

```text
sigma_i(S,t) = sigma_hat(m_i(S,t), tau_i(t))
```

This is the preferred base for:

- vanna
- flip lines
- Alma-style speed/color guesses

### 6.3 Surface-fitting recommendation

Use one of:

- tensor spline over `(m, tau)`
- SVI per expiry plus interpolation across `tau`

For a first implementation, spline smoothing is fine.

## 7. Position Estimation

This is the largest non-observable component.

### 7.1 Exact mode

If a future data source gives direct net customer positioning by contract, use:

```text
Q_i^cust = direct directional position
Q_i^mm   = -Q_i^cust
```

### 7.2 ThetaData proxy mode

With current public data, we need a directional-position proxy.

#### Step 1. Signed trade flow

At trade level `n`:

```text
s_n = +1  if trade lifts ask / is above midpoint
s_n = -1  if trade hits bid / is below midpoint
s_n =  0  otherwise
```

This quote-aware direction logic is deliberate. It is better aligned with market-microstructure literature than inferring direction from trade price alone; see the appendix brief `options-market-makers-hedging-and-informed-trading-source-brief.pdf`.

Contract-day signed flow:

```text
SF_i,d = sum_n (s_n * size_n)
VOL_i,d = sum_n size_n
```

#### Step 2. OI-anchored opening-intent weight

```text
DeltaOI_i,d = OI_i,d - OI_i,d-1
eta_i,d = min(1, abs(DeltaOI_i,d) / max(VOL_i,d, 1))
```

Interpretation:

- if `eta` is near `1`, that day likely carried large net opening activity
- if `eta` is near `0`, the day's flow was mostly churn or closing

#### Step 3. Rolling directional score

Use a decayed score over the last `L` days:

```text
score_i,d = sum_{u=d-L+1}^{d} lambda^(d-u) * eta_i,u * (SF_i,u / max(VOL_i,u, 1))
```

Recommended:

- `lambda in [0.85, 0.97]`
- `L in [5, 20]`

#### Step 4. Signed customer inventory proxy

Map the bounded score into contracts:

```text
Q_i^cust(d) = OI_i,d * tanh(kappa * score_i,d)
Q_i^mm(d)   = -Q_i^cust(d)
```

Recommended:

- `kappa in [1.5, 3.0]`

Why this works:

- `OI` caps the possible net directional inventory
- the rolling signed-flow score provides direction
- `tanh` keeps the estimate bounded and robust

### 7.3 LOB-assisted directional-index mode

If we can obtain limit-order-book or richer microstructure classification for a subset of contracts, we should blend that into the directional score rather than relying only on quote-aware prints.

Define a contract-level directional probability:

```text
p_i,d = P(customer net long option on contract i by end of day d | microstructure features)
```

Map that into a bounded signed score:

```text
score_lob_i,d = 2 * p_i,d - 1
```

Let `n_i,d^lob` be the number of contracts or observations supported by the richer feed, and choose a confidence weight:

```text
w_i,d^lob = min(1, n_i,d^lob / n0)
```

Recommended starting point:

```text
n0 in [25, 100]
```

Blend the LOB-informed score with the rolling ThetaData proxy score:

```text
score_blend_i,d
  = w_i,d^lob * score_lob_i,d
    + (1 - w_i,d^lob) * score_i,d
```

Then estimate signed customer inventory as:

```text
Q_i^cust(d) = OI_i,d * tanh(kappa * score_blend_i,d)
Q_i^mm(d)   = -Q_i^cust(d)
```

Interpretation:

- the public-data proxy still supplies the baseline
- the richer microstructure feed overrides contracts where we have stronger directional evidence
- this is the closest implementable analogue to the kind of "Directional Index" logic exposed in recent public `GEX` dashboards

### 7.4 Minimal same-day fallback

If we do not want a rolling inventory model yet:

```text
Q_i^cust(d) = SF_i,d
Q_i^mm(d)   = -SF_i,d
```

This is much noisier and should only be used for a canary.

## 8. Core Exposure Formulas

### 8.1 Position count

```text
Position_i = Q_i
```

Use customer sign for sentiment views and market-maker sign for hedging views.

### 8.2 DEX

Delta-equivalent shares:

```text
DEX_i_shares = Q_i * M_i * Delta_i
```

Dollar delta exposure:

```text
DEX_i_dollars = Q_i * M_i * S * Delta_i
```

For OptionDepth-style `DEX - δ`, the cleaner default is the delta-equivalent shares view.

### 8.3 GEX

Share gamma exposure per `$1` spot move:

```text
GEX_i_shares_per_dollar = Q_i * M_i * Gamma_i
```

Dollar hedge notional change per `1 point` move:

```text
GEX_i_dollars_per_point = Q_i * M_i * S * Gamma_i
```

Dollar hedge notional change per `1%` move:

```text
GEX_i_dollars_per_1pct = Q_i * M_i * S^2 * Gamma_i * 0.01
```

For OptionDepth-style `GEX - $M/pt`, use:

```text
GEX_i_M_per_pt = GEX_i_dollars_per_point / 1e6
```

### 8.4 VEX

Option value sensitivity per `1 vol point`:

```text
VEX_i_dollars_per_volpt = Q_i * M_i * Vega_i * 0.01
```

If matching the app's `$M/σ%` style:

```text
VEX_i_M_per_volpt = VEX_i_dollars_per_volpt / 1e6
```

### 8.5 CEX

Projected delta-hedge notional change from charm over a 5-minute interval:

```text
dt_5m = 5 / (390 * 252)
CEX_i_dollars_per_5m = Q_i * M_i * S * Charm_fwd_i * dt_5m
```

If matching the app's `$M/5min` style:

```text
CEX_i_M_per_5m = CEX_i_dollars_per_5m / 1e6
```

### 8.6 Vanna hedge exposure

Delta-hedge notional change per `1 vol point`:

```text
VannaX_i_dollars_per_volpt = Q_i * M_i * S * Vanna_i * 0.01
```

This is the more useful quantity for vanna heatmaps and flip detection.

## 9. EOD Heatmap Surface

### 9.1 Grid definition

Spot grid:

```text
S_j = S0 + j * dS
```

Recommended:

- `dS = 0.5 * modal strike spacing`
- for `SPX`, if strikes are mostly `5`, then `dS = 2.5`

Range:

- at least from `min_relevant_strike - buffer` to `max_relevant_strike + buffer`
- or `S0 +/- 3 to 4 daily expected moves`

Time grid:

```text
t_k in {09:30, 09:35, ..., 16:00 ET}
```

or any user-selected bucket such as `1m`, `2m`, `5m`, `15m`, `65m`.

### 9.2 Gamma heatmap

Market-maker gamma heatmap:

```text
H_gamma_mm(S_j, t_k) = sum_i Q_i^mm * M_i * S_j * Gamma_i(S_j, t_k)
```

Customer gamma heatmap:

```text
H_gamma_cust(S_j, t_k) = -H_gamma_mm(S_j, t_k)
```

### 9.3 Other heatmaps

DEX:

```text
H_delta(S_j, t_k) = sum_i Q_i * M_i * Delta_i(S_j, t_k)
```

VEX:

```text
H_vega(S_j, t_k) = sum_i Q_i * M_i * Vega_i(S_j, t_k) * 0.01
```

CEX:

```text
H_charm(S_j, t_k) = sum_i Q_i * M_i * S_j * Charm_fwd_i(S_j, t_k) * dt_bucket
```

Vanna:

```text
H_vanna(S_j, t_k) = sum_i Q_i * M_i * S_j * Vanna_i(S_j, t_k) * 0.01
```

### 9.4 Spot x IV shock risk surface

This is not the native OptionDepth heatmap, but it is a very useful adjacent surface and matches the structure used in recent public `GEX + vanna` dashboards.

Define a spot-shock grid and IV-shock grid:

```text
u in U = spot move fractions, e.g. {-0.15, ..., 0.10}
v in V = IV shocks in vol points, e.g. {-10, ..., 40}
```

Map shocks into scenario state:

```text
S(u) = S0 * (1 + u)
DeltaS(u) = S(u) - S0
DeltaSigma(v) = 0.01 * v
```

For each contract, evaluate Greeks on the shocked node:

```text
Gamma_i(u,v) = Gamma_i(S(u), t0, sigma_i(S(u), t0) + DeltaSigma(v))
Vanna_i(u,v) = Vanna_i(S(u), t0, sigma_i(S(u), t0) + DeltaSigma(v))
```

Then define the combined dealer hedge-flow shock proxy:

```text
R_i(u,v)
  = Q_i^mm * M_i * S(u)
    * [Gamma_i(u,v) * DeltaS(u) + Vanna_i(u,v) * DeltaSigma(v)]
```

Aggregate:

```text
R_total(u,v) = sum_i R_i(u,v)
```

Interpretation:

- positive values indicate a dampening response
- negative values indicate same-direction dealer hedging pressure
- the quadrant `u < 0, v > 0` is the natural crash-fragility quadrant

This is the best practical public benchmark for a `gamma + vanna` stress surface even if we do not know a proprietary vendor's exact `GEX+` formula.

## 10. Peaks, Troughs, And Zero Lines

At each time slice `t_k`:

- `gamma peak` = local maxima of `H_gamma_mm(S, t_k)`
- `gamma trough` = local minima of `H_gamma_mm(S, t_k)`
- `gamma zero` = roots of `H_gamma_mm(S, t_k) = 0`

Recommended implementation:

- smooth with a light Savitzky-Golay or cubic spline filter
- then locate extrema and sign changes

## 11. Speed And Color

This section has two levels:

- exact baseline
- Alma-style educated guess

### 11.1 Baseline surface definitions

If the charted quantity is the market-maker gamma heatmap `H_gamma_mm`, define:

```text
Speed_surface_raw(S,t) = dH_gamma_mm / dS
Color_surface_raw(S,t) = dH_gamma_mm / dt
```

Numerically:

```text
Speed_surface_raw(S_j, t_k)
  ~= [H_gamma_mm(S_{j+1}, t_k) - H_gamma_mm(S_{j-1}, t_k)] / (2*dS)

Color_surface_raw(S_j, t_k)
  ~= [H_gamma_mm(S_j, t_{k+1}) - H_gamma_mm(S_j, t_k)] / dt
```

These are the safest first implementation.

### 11.2 Educated guess for Alma-style speed

Alma repeatedly says she accounts for time decay and changes in volatility.

The cleanest quantitative guess is:

1. let the IV surface move with spot and tenor
2. differentiate the gamma exposure surface with the chain rule

For one contract:

```text
GEX_i(S,t) = Q_i^mm * M_i * S * Gamma_i(S,t,sigma_i(S,t))
```

Then:

```text
dGEX_i/dS
  = Q_i^mm * M_i * [Gamma_i + S * dGamma_i/dS_total]
```

with

```text
dGamma_i/dS_total = Speed_i + Zomma_i * sigma_S,i
sigma_S,i = d sigma_i(S,t) / dS
```

So:

```text
Speed_surface_adv(S,t)
  = sum_i Q_i^mm * M_i * [Gamma_i + S * (Speed_i + Zomma_i * sigma_S,i)]
```

Interpretation:

- raw speed gives direct gamma slope
- `zomma * sigma_S` adds the change in gamma caused by the IV surface moving with spot

This is the best quantitative guess for Alma's statement that she uses speed while taking volatility changes into account.

### 11.3 Educated guess for Alma-style color

Similarly:

```text
dGEX_i/dt
  = Q_i^mm * M_i * S * dGamma_i/dt_total
```

with

```text
dGamma_i/dt_total = Color_fwd_i + Zomma_i * sigma_t,i
sigma_t,i = d sigma_i(S,t) / dt
```

So:

```text
Color_surface_adv(S,t)
  = sum_i Q_i^mm * M_i * S * [Color_fwd_i + Zomma_i * sigma_t,i]
```

Interpretation:

- raw color is gamma decay from time passage
- `zomma * sigma_t` adds gamma change caused by time-evolving IV

This is the best quantitative guess for Alma's "gamma decay plus net changes to expected volatilities" language.

### 11.4 Surface-level implementation recommendation

Rather than relying on isolated contract formulas, the best implementation is:

1. build `H_gamma_mm(S,t)` on a dynamic IV surface
2. compute speed and color by finite differencing the surface itself

This automatically includes:

- time decay
- IV skew dynamics
- tenor interpolation
- cross-effects from vanna and zomma

The decision to compute `speed` and `color` from the surface itself, rather than trusting a single isolated closed form, is also consistent with using numerical higher-order differentiation as a practical implementation device; see `vibrato-and-automatic-differentiation-for-high-order-derivatives-and-sensitivities-of-financial-options-arxiv-1606.06143.pdf`.

## 12. Centroid And Pivots

### 12.1 Define a pressure surface

Use the customer-signed speed surface as the realized-volatility pressure surface:

```text
P(S,t) = -Speed_surface_adv(S,t)
```

Then average or integrate across the next session:

```text
P_bar(S) = average_k P(S, t_k)
```

### 12.2 Net centroid

Alma says centroid is the center of mass / balance point of the whole profile.

Recommended operational definition:

```text
B(S) = integral_{S_min}^{S} P_bar(u) du

centroid = argmin_S |B(S) - 0.5 * B(S_max)|
```

Interpretation:

- this splits the signed pressure profile into two balanced halves
- it is the most practical "cuts the whole profile in half" implementation

### 12.3 Upside and downside pivots

Alma says the pivots are local centroids of the `ATM` vs `22-delta` skews.

Recommended implementation:

1. define delta buckets on the EOD snapshot:
   - `ATM bucket`: `|delta| in [0.40, 0.60]`
   - `22-delta call bucket`: `delta in [0.15, 0.30]`
   - `22-delta put bucket`: `delta in [-0.30, -0.15]`
2. build one-sided pressure surfaces:
   - downside side from `ATM + 22-delta puts`
   - upside side from `ATM + 22-delta calls`
3. compute local centroids by the same balance formula, restricted to each side of the net centroid

Formally:

```text
P_down(S) = pressure surface from downside-relevant bucket set
P_up(S)   = pressure surface from upside-relevant bucket set
```

Then:

```text
downside_pivot = local centroid of P_down on [S_min, centroid]
upside_pivot   = local centroid of P_up   on [centroid, S_max]
```

This is the closest implementable interpretation of Alma's math description.

## 13. Vanna Flips And Speed Flips

### 13.1 Vanna flips

Because Alma says vanna is tenor-conditional, the best implementation is bucketed.

For tenor bucket `b`:

```text
V_b(S,t) = sum_{i in b} Q_i^mm * M_i * S * Vanna_i(S,t) * 0.01
```

Base aggregate:

```text
V_agg(S,t) = sum_b omega_b * V_b(S,t)
```

If no empirical hedge covariance model exists yet, use:

```text
omega_b = 1
```

Then:

```text
vanna_flip = roots of V_agg(S,t) = 0
```

Why tenor buckets matter:

- Alma explicitly says vanna should not be aggregated naively across maturities

### 13.2 Speed flips

Base definition:

```text
speed_flip = roots of Speed_surface_adv(S,t) = 0
```

### 13.3 Zero-speed-gamma educated guess

Alma sometimes refers to critical local pivots as:

- `zero speed-gamma`
- `zeros of the 4th derivative of delta to spot`

The most practical implementation guess is:

```text
speed_gamma(S,t) = d Speed_surface_adv(S,t) / dS
```

Then critical local convexity pivots are:

```text
roots of speed_gamma(S,t) = 0
```

or equivalently local extrema of `Speed_surface_adv`.

This should be treated as an educated proxy, not a proven literal reproduction.

## 14. Hidden Pattern Classification

These are heuristic rules, not hard identities.

### 14.1 Dealer short fly / customer long fly

Classify as dealer short fly if:

- `H_gamma_mm(center) > 0`
- `H_gamma_mm(left wing) < 0`
- `H_gamma_mm(right wing) < 0`
- `Speed_surface_adv(left of center) > 0`
- `Speed_surface_adv(right of center) < 0`

### 14.2 Dealer long fly / customer short fly

Classify as dealer long fly if:

- `H_gamma_mm(center) < 0`
- `H_gamma_mm(left wing) > 0`
- `H_gamma_mm(right wing) > 0`
- `Speed_surface_adv(left of center) < 0`
- `Speed_surface_adv(right of center) > 0`

### 14.3 Risk reversal

Classify as risk reversal if:

- one wing dominates the other in `|integrated speed pressure|`
- vanna is strongly one-sided
- the profile is asymmetric rather than wing-balanced

Use:

```text
tail_asymmetry
  = | integral_{centroid}^{S_max} P_bar(S) dS |
    /
    | integral_{S_min}^{centroid} P_bar(S) dS |
```

If `tail_asymmetry` is far from `1`, treat as risk-reversal-like rather than fly-like.

### 14.4 Ratio spread clue

Use the relative steepness of `DEX` and `GEX` by wing.

For each wing `w`:

```text
slope_DEX_w = local slope of H_delta(S)
slope_GEX_w = local slope of H_gamma_mm(S)
ratio_w = |slope_DEX_w| / max(|slope_GEX_w|, eps)
```

Large asymmetry between left and right `ratio_w` values is a useful heuristic for ratio-spread-like positioning.

## 15. Probability Bands

This is not required for the heatmap itself, but it belongs to the same quantitative stack.

### 15.1 Symmetric base version

For horizon `h` in years:

```text
sigma_eff^2 = w_iv * sigma_ATM^2 + (1 - w_iv) * RV_target^2
std_h = sigma_eff * sqrt(h)
```

Then:

```text
Upper_z = S0 * exp(z * std_h)
Lower_z = S0 * exp(-z * std_h)
```

for:

- `z = 1` -> `68.2%`
- `z = 2` -> `95.4%`
- `z = 3` -> `99.73%`

### 15.2 Asymmetric educated version

To reflect speed/vanna asymmetry:

```text
sigma_eff_up   = sigma_eff * (1 + alpha_up)
sigma_eff_down = sigma_eff * (1 + alpha_down)
```

where:

```text
alpha_up   = c1 * normalized_upside_speed + c2 * upside_vanna_asymmetry
alpha_down = c1 * normalized_downside_speed + c2 * downside_vanna_asymmetry
```

This is a reasonable way to encode Alma's skewed scenario logic without pretending we know her exact proprietary method.

### 15.3 Breeden-Litzenberger density overlay

If we want a stronger option-implied forecast layer, recover the expiry-horizon risk-neutral density from OTM option prices.

For strike `K` and expiry `T`:

```text
q(K) = exp(rT) * d^2 C_OTM(K, T) / dK^2
```

Practical implementation:

- use OTM puts below spot and OTM calls above spot
- smooth mids lightly across strike before differencing
- use finite differences on strike

Moments from the density:

```text
mu_T      = integral K * q(K) dK
var_T     = integral (K - mu_T)^2 * q(K) dK
skew_T    = integral ((K - mu_T)^3 / var_T^(3/2)) * q(K) dK
kurt_T    = integral ((K - mu_T)^4 / var_T^2) * q(K) dK
```

### 15.4 Cornish-Fisher short-horizon forecast layer

For a shorter horizon `h < T`, scale variance by time:

```text
sigma_h = sqrt(var_T) * sqrt(h / T)
```

Then use Cornish-Fisher adjusted quantiles:

```text
z_cf
  = z
    + (skew_T / 6) * (z^2 - 1)
    + ((kurt_T - 3) / 24) * (z^3 - 3z)
    - (skew_T^2 / 36) * (2z^3 - 5z)
```

Forecast percentile:

```text
Q_h(p) = F_h + sigma_h * z_cf
```

where `F_h` is the forward or local mean level.

This gives a much stronger baseline distribution than a plain lognormal assumption.

Recommended use:

- keep the `BL + Cornish-Fisher` forecast as the baseline
- use positioning regime labels as a conditioning overlay
- do not pretend the positioning surface alone is a literal probability model

## 16. Data Needed

### 16.1 Required to build the heatmap

- contract identifier
- option side
- strike
- expiration
- EOD option quotes
- EOD underlying spot
- end-of-day open interest
- implied vol or enough data to solve IV
- risk-free rate
- dividend assumption
- trade-level direction and size for position proxy

### 16.2 Needed to reproduce Alma-style derived objects well

- fitted IV surface by moneyness and tenor
- tenor-bucket aggregation
- rolling signed-flow history
- realized-volatility validation features
- fixed-strike skew change features

### 16.3 Needed for a stronger public benchmark

- contract-level or strike-level `LOB` or richer trade-classification feed
- OTM option mids across the chain for `BL` density extraction
- scenario shock grids for spot and IV
- a sign-disagreement audit against naive assumptions

## 17. Mapping To Current ClickHouse Tables

| Need | Table / column(s) | Status | Notes |
|---|---|---|---|
| Contract identity | `options.option_quote_minute_raw` / `options.option_calculated_greeks_minute` / `options.option_trades` -> `symbol`, `expiration`, `strike`, `option_right` | Available | Core contract key exists everywhere |
| EOD option bid/ask/last | `options.option_quote_minute_raw.bid`, `ask`, `last`, `minute_bucket_utc` | Available | Need last reliable minute selector |
| EOD mid price | `options.option_calculated_greeks_minute.mid_price` or derive from quote raw | Available | Calculated table already stores mid |
| EOD underlying spot | `options.stock_ohlc_minute_raw.close` or `options.option_calculated_greeks_minute.underlying_price` | Available | Prefer synced close minute |
| EOD OI | `options.option_open_interest_raw.oi` | Available | Daily only |
| IV | `options.option_calculated_greeks_minute.implied_vol` or `options.option_greeks_minute_raw.implied_vol` | Available | Prefer calculated greeks for coverage |
| Delta | `options.option_calculated_greeks_minute.delta` | Available | Good |
| Gamma | `options.option_calculated_greeks_minute.gamma` | Available | Good |
| Vega | `options.option_calculated_greeks_minute.vega_annual`, `vega_per_1pct` | Available | Good |
| Theta | `options.option_calculated_greeks_minute.theta_annual`, `theta_per_day` | Available | Good |
| Risk-free rate | `options.reference_sofr_daily.rate_decimal` and `options.option_calculated_greeks_minute.risk_free_rate` | Available | Good |
| Dividend yield | `options.option_calculated_greeks_minute.dividend_yield` | Partially available | Assumption-driven today |
| Time to expiry | `options.option_calculated_greeks_minute.time_to_expiry_years` | Available | Good |
| Trade direction proxy | `options.option_trade_enriched.execution_side`, `sentiment`, `size` | Available | Heuristic, not exchange truth |
| Contract-level signed flow aggregates | `options.option_contract_minute_derived` plus `options.option_trade_enriched` | Available | Need new EOD rollups |
| Symbol-level validation features | `options.option_symbol_minute_derived` | Available | Useful for intraday confirmation |
| Contract-level LOB or richer order classification | none | Missing | Best upgrade path for directional-index style models |
| Direct customer/dealer inventory | none | Missing | Biggest gap |
| Open vs close flags | none | Missing | Biggest gap for exact directional positioning |
| Contract multiplier reference | none dedicated | Missing | Can assume `100` for standard contracts initially |
| Dividend reference table | none dedicated | Missing | Should add if we want broad single-name accuracy |
| Fitted IV surface table | none | Missing | Derived table needed |
| EOD chain snapshot table | none | Missing | Derived table recommended |
| OTM-chain density table for `BL` forecast extraction | none | Missing | Derived table recommended |
| EOD position-proxy table | none | Missing | Derived table recommended |
| EOD heatmap surface table | none | Missing | Derived table recommended |
| EOD structure-level table | none | Missing | Derived table recommended |

## 18. Recommended New Derived Tables

### 18.1 `options.option_eod_chain_snapshot`

One row per contract per date with:

- cleaned EOD quote
- `S0`
- `OI`
- IV
- first-order greeks
- `r`, `q`

### 18.2 `options.option_eod_position_proxy`

One row per contract per date with:

- rolling signed flow
- `DeltaOI`
- `eta`
- directional score
- `Q_cust`
- `Q_mm`

### 18.3 `options.option_eod_surface`

One row per `(symbol, trade_date, S_grid, t_grid, metric)` with:

- `gamma`
- `delta`
- `vega`
- `vanna`
- `speed`
- `color`
- `zomma`
- `vomma`

### 18.4 `options.option_eod_structure_levels`

One row per `(symbol, trade_date)` with:

- centroid
- downside pivot
- upside pivot
- downside target
- upside target
- local pin zones
- vanna flips
- speed flips
- hidden-pattern label

## 19. Research-Informed Modeling Choices

The internet research is most useful for how aggressively we should interpret these surfaces.

What it suggests:

- dealer hedging can move the underlying, but the pass-through is not mechanically `100%`
- 0DTE activity materially affects volatility, especially in indices
- trade-direction classification should be quote-based, not guessed from price alone

Appendix anchors for those claims:

- `does-0dte-options-trading-increase-volatility-source-brief.pdf`
- `retail-traders-love-0dte-options-but-should-they-working-paper.pdf`
- `0dte-index-options-and-market-volatility-how-large-is-their-impact-cboe.pdf`
- `options-market-makers-hedging-and-informed-trading-source-brief.pdf`

So the practical implication is:

- build the structural map from full inventory-weighted exposures
- backtest hedge pass-through separately instead of baking it in as certainty

Recommended optional calibration layer:

```text
Q_i^mm,eff = lambda_i * Q_i^mm
```

where `lambda_i in [0,1]` can later be estimated as a function of:

- tenor
- moneyness
- liquidity
- 0DTE status
- historical hedge pass-through

For the first EOD heatmap version:

```text
lambda_i = 1
```

## 20. Bottom Line

The cleanest implementable reconstruction is:

1. build an `EOD` chain snapshot
2. estimate signed customer inventory with an OI-anchored rolling flow model
3. convert to market-maker inventory
4. fit an IV surface
5. build the next-session gamma / delta / vega / vanna / charm surfaces
6. compute `speed` and `color` as derivatives of the exposure surface itself
7. derive centroid, pivots, flips, and hidden patterns from the smoothed pressure surfaces

The most important implementation choices are:

- whether IV is sticky-strike or sticky-moneyness
- how signed inventory is estimated
- whether speed/color are taken as raw derivatives or dynamic-surface derivatives

My recommendation:

- first version: sticky-moneyness surface, OI-anchored flow proxy, numerical surface derivatives
- then add calibration for hedge pass-through and flip stability

## 21. Reference Pointers

Internal references:

- [ALMA_CALCULATION_METHOD.md](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/ALMA_CALCULATION_METHOD.md)
- [ALMA_OPTIONDEPTH_USAGE.md](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/ALMA_OPTIONDEPTH_USAGE.md)
- [COMPUTE_AND_BACKTEST_FEASIBILITY.md](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/COMPUTE_AND_BACKTEST_FEASIBILITY.md)
- [MARKET_BEHAVIOR_CLAIMS.md](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/MARKET_BEHAVIOR_CLAIMS.md)
- [PHENIX_ARCHITECTURE.md](/Users/pawanagarwal/github/phenixflow/docs/PHENIX_ARCHITECTURE.md)
- [THETADATA_GREEKS_INPUT_SOURCES.md](/Users/pawanagarwal/github/phenixflow/docs/THETADATA_GREEKS_INPUT_SOURCES.md)
- [init-options-schema.sql](/Users/pawanagarwal/github/phenixflow/scripts/clickhouse/init-options-schema.sql)

External references used for methodology calibration:

- [SOURCE_INDEX.md](/Users/pawanagarwal/github/phenixflow/output/playwright/optiondepth/appendix/SOURCE_INDEX.md)
- `appendix/does-0dte-options-trading-increase-volatility-source-brief.pdf` -> canonical URL `https://ssrn.com/abstract=4426358`
- `appendix/retail-traders-love-0dte-options-but-should-they-working-paper.pdf` -> working paper mirror used locally
- `appendix/options-market-makers-hedging-and-informed-trading-source-brief.pdf` -> canonical DOI `https://doi.org/10.1016/j.finmar.2015.01.001`
- `appendix/vibrato-and-automatic-differentiation-for-high-order-derivatives-and-sensitivities-of-financial-options-arxiv-1606.06143.pdf`
- `appendix/0dte-index-options-and-market-volatility-how-large-is-their-impact-cboe.pdf`
