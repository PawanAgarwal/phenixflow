# ThetaData Availability Rules For Backfill

This document defines the safe operating windows for downloading and enriching end-of-day backfill data from ThetaData.

All times below are America/New_York / Eastern Time because that is how ThetaData documents publication timing.

## Primary ThetaData timing facts

- Previous-day historical data is not available from `00:00 ET` to `01:45 ET`.
  Source: https://docs.thetadata.us/Articles/Data-And-Requests/Data-Issues.html
- ThetaData generates the options EOD report at `17:15 ET`.
  Source: https://docs.thetadata.us/operations/option_history_eod.html
- OPRA open interest is reported around `06:30 ET` each morning and reflects the previous trading day.
  Source: https://docs.thetadata.us/operations/option_snapshot_open_interest.html

## Operational rules

### 1. Raw trade / quote / stock downloads

- Same-day canary downloads for trade, quote, and stock can start after the market session is complete.
- Do not treat the day as publication-stable during ThetaData's midnight reset window.
- Safe final raw backfill window for trade, quote, and stock is:
  - `01:45 ET` or later on `D+1`

Recommended rule:

- If you want fast same-evening visibility, run a canary after close.
- If you want deterministic previous-day repair, schedule the main raw backfill after `01:45 ET`.

### 2. Open interest downloads

- Do not expect previous-day OI to be complete on trade date `D`.
- Safe OI backfill window is:
  - `06:30 ET` or later on `D+1`
- Prefer adding a small safety buffer and scheduling OI after `06:45 ET` or `07:00 ET`.

### 3. Greeks downloads

- ThetaData documents option EOD report generation at `17:15 ET`, but does not publish a separate guaranteed release time for the minute-history greeks endpoints used by this backfill.
- Treat minute-history greeks as previous-day historical data for scheduling purposes unless a stronger provider guarantee is documented later.

Safe rule:

- If downloading raw minute greeks, schedule them no earlier than `01:45 ET` on `D+1`.
- If using EOD-only greeks, they can be fetched after `17:15 ET` on trade date `D`.

This is an inference from ThetaData docs, not an explicit minute-greeks publication SLA.

### 4. Enrichment timing

- Provisional enrichment can run once raw trade, quote, and stock are complete.
- Final enrichment for a day should wait until every dependency required by the chosen enrichment mode is available.

Recommended production rules:

- If enrichment depends only on trade + quote + stock:
  - run after raw completion, preferably `01:45 ET+` on `D+1`
- If enrichment depends on OI:
  - run after OI is available, preferably `06:45 ET+` on `D+1`
- If enrichment requires provider-calculated greeks:
  - run only after the relevant greeks source is confirmed available

### 5. Gap-fill policy by component

- Trade / quote / stock gaps for day `D`:
  - fill after `01:45 ET` on `D+1`
- OI gaps for day `D`:
  - fill after `06:30 ET` on `D+1`
- Enrichment gaps for day `D`:
  - fill only after raw dependencies for `D` are complete
- Final enrich rerun for OI-dependent fields:
  - run after `06:45 ET` on `D+1`

## Suggested daily schedule

### Trade date `D`, after close

- Optional canary for raw trade / quote / stock
- Optional EOD report pull after `17:15 ET`

### `D+1` early morning

- `01:45 ET+`: main raw trade / quote / stock remediation
- `06:45 ET+`: OI remediation
- `07:00 ET+`: final enrichment pass

## Practical March guidance

- For the current March backfill, treat `March 23, 2026` raw trade / quote / stock as eligible after `01:45 ET` on `March 24, 2026`.
- Treat `March 23, 2026` OI as eligible after about `06:30 ET` on `March 24, 2026`.
- Run the final enrich pass for `March 23, 2026` only after the OI step is complete.
