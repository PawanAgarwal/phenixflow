# `pym-v5-option-rank-top8`

## Purpose

`pym-v5-option-rank-top8` starts from the normal PYM V5 target portfolio, then
uses Massive option-flow features to keep only the strongest option-momentum
names from the current PYM holdings.

It is a concentration overlay, not a standalone option strategy.

## Plain-English Rule

At each EOD signal:

```text
pym_candidates = current PYM V5 target holdings
score each candidate by option-flow momentum
keep the top 8 candidates with score >= -0.5
renormalize selected PYM weights to 100%
if no candidate passes: hold BIL
```

The selected names preserve the relative PYM target weights unless the option
overlay strategy explicitly says otherwise. The tracked live id uses the
non-equal-weight grid winner:

- option overlay id: `grid_pym_option_rank_top8_zm0p5`

## Service Adapter

- Adapter: `apps/strategy-service/src/strategies/pym-v5-option-rank.js`
- Registry id: `pym-v5-option-rank-top8`
- Family: `pym_option_flow`
- Underlying option overlay strategy: `grid_pym_option_rank_top8_zm0p5`

The adapter recomputes from mounted runtime data, unlike the ML artifact-backed
strategies.

Core option logic lives in:

- `projects/pym-v5-replication/src/option-overlay-suite.js`

## Inputs

- PYM V5 target weights for the signal date.
- Massive adjusted EOD bars for realized close-to-close returns.
- Massive option aggregate features built under
  `projects/pym-v5-replication/runtime/`.

Option feature examples used by the scoring family:

- premium imbalance z-score
- call premium momentum
- put premium momentum
- short-dated ATM-flow proxy
- total premium liquidity filter

## Timing

```text
day X close: build base PYM target and read option-flow features through X
day X close: rank current PYM holdings by option-flow momentum
day X+1 close: realize close-to-close return
```

## API Surface

- `GET /api/strategies/pym-v5-option-rank-top8`
- `GET /api/strategies/pym-v5-option-rank-top8/chart`
- `GET /api/strategies/pym-v5-option-rank-top8/values`
- `GET /api/strategies/pym-v5-option-rank-top8/portfolio/latest`
- `GET /api/strategies/pym-v5-option-rank-top8/portfolio/:date`
- `POST /api/strategies/pym-v5-option-rank-top8/recompute`

## How To Refresh Inputs

Build or refresh option-flow features before recomputing:

```bash
npm run pym-v5:build-option-features -- --start 2025-01-02 --end YYYY-MM-DD
```

Then recompute the strategy in memory through the API or by restarting the
service.

## Caveats

- The top-8 and `z >= -0.5` parameters were selected from research sweeps and
  should be treated as research-selected until hardened by walk-forward
  validation.
- Historical Greeks and open interest were not available in the local Massive
  cache used for the original sweep, so this is option-flow momentum, not true
  dealer gamma exposure.
- Turnover is materially higher than base PYM.
