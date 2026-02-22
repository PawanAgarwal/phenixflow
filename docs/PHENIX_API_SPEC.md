# Phenix API Contract (Real-Time + Historical)

## 1. Purpose
This document defines the API surface and parameter-level contract required to satisfy:
- `/Users/pawanagarwal/github/phenixflow/docs/PHENIX_PROJECT_GOALS.md`
- Architecture blueprint from commit `41c8075` (`docs/PHENIX_ARCHITECTURE.md` in that commit)

Scope includes:
- Real-time flow APIs
- Historical flow APIs
- Saved filter/alert APIs
- Operational APIs
- ThetaData dependency endpoint matrix

Date baseline: February 16, 2026.

## 2. Versioning and Base Paths
- Current base path: `/api`
- Backward-compatible alias path: `/api/v1`
- Rule/filter version selector: `filterVersion=legacy|candidate`

Alias rule:
- Every flow endpoint in `/api/flow/...` must have `/api/v1/flow/...` alias with equivalent behavior.
- Saved payload read defaults:
  - `/api`: default payload version `v2`
  - `/api/v1`: default payload version `legacy`

## 3. Common Conventions

### 3.1 Content and Encoding
- Content type: `application/json` unless explicitly SSE.
- List query params use comma-separated values (for example: `chips=calls,100k+,otm`).
- Boolean query params accept: `true|false|1|0|yes|on`.

### 3.2 Time and Date
- API timestamps are ISO-8601 UTC strings.
- Historical endpoint enforces single UTC day (`from` and `to` on same date).
- Session logic for AM window is `America/New_York`.

### 3.3 Pagination and Sorting
- Cursor pagination uses stable composite ordering `(sortBy value, id)`.
- `limit` is endpoint-specific:
  - `/api/flow`: default `25`, max `100`
  - `/api/flow/historical`: default `100`, max `1000`

### 3.4 Filter Semantics
- Multiple chips use `AND` semantics.
- Scalar filters and chips are both applied; all conditions must pass.
- Unknown chip/filter tokens are ignored only where backward-compatible behavior requires; strict-mode validation may return `400 invalid_query` for malformed values.

### 3.5 Error Envelope
All failures use:

```json
{
  "error": {
    "code": "string_code",
    "message": "human-readable message",
    "details": []
  }
}
```

## 4. Canonical Data Models

### 4.1 `FlowRowV2`
Fields required for full V1 functionality:
- `id: string`
- `tradeTsUtc: string`
- `symbol: string`
- `expiration: string` (`YYYY-MM-DD`)
- `strike: number`
- `right: "CALL" | "PUT"`
- `price: number`
- `size: number`
- `bid: number | null`
- `ask: number | null`
- `conditionCode: string | null`
- `exchange: string | null`
- `value: number | null`
- `dte: number | null`
- `spot: number | null`
- `otmPct: number | null`
- `dayVolume: number | null`
- `oi: number | null`
- `volOiRatio: number | null`
- `repeat3m: number | null`
- `sigScore: number | null` (0 to 1)
- `sentiment: "bullish" | "bearish" | "neutral"`
- `chips: string[]`

### 4.2 `PageMeta`
- `limit: number`
- `hasMore: boolean`
- `nextCursor: string | null`
- `sortBy: string`
- `sortOrder: "asc" | "desc"`
- `total: number`

### 4.3 `HistoricalSyncMeta`
- `synced: boolean`
- `reason: string | null` (`day_cache_full`, `metric_cache_full`, etc.)
- `fetchedRows: number`
- `upsertedRows: number`
- `cachedRows: number`
- `cacheStatus: "full" | "partial"` (when available)

### 4.4 `HistoricalEnrichmentMeta`
- `synced: boolean`
- `reason: string | null`
- `rowCount: number`

## 5. Chip and Filter Dictionary

### 5.1 Execution Chips
- `calls`: `right = CALL`
- `puts`: `right = PUT`
- `bid`: `price <= bid`
- `ask`: `price >= ask` and not `AA`
- `aa`: `price >= ask + max(0.01, 0.10 * (ask - bid))`
- `sweeps` (legacy compatibility)

### 5.2 Size/Value Chips
- `100k+`: `value >= 100000`
- `sizable`: `value >= 250000`
- `whales`: `value >= 500000`
- `large-size`: `size >= 1000`

### 5.3 Advanced Chips
- `leaps`: `dte >= 365`
- `weeklies`: expiration is not standard monthly third-Friday
- `repeat-flow`: `repeat3m >= 20`
- `otm`: `otmPct > 0`
- `vol>oi`: `volOiRatio > 1.0`
- `rising-vol`: `symbolVol1m >= 2.5 * symbolVolBaseline15m`
- `am-spike`: ET 09:30-10:30 and `symbolVol1m >= 3.0 * openWindowBaseline`
- `bullflow`: `bullishRatio15m >= 0.65` and `sentiment = bullish`
- `high-sig`: `sigScore >= 0.90`
- `unusual`: `value >= 100000` and `volOiRatio >= 2.0`
- `urgent`: `repeat3m >= 20` OR (`value >= 250000` and `dte <= 14` and `volOiRatio >= 2.5`)
- `position-builders`: `21 <= dte <= 180`, `abs(otmPct) <= 15`, `size >= 250`, side in `ASK|AA`
- `grenade`: `dte <= 7`, `otmPct >= 5`, `value >= 100000`

### 5.4 Filter-Term Definitions and Dependency Table
This table defines terms used by chips and enriched filters. If a term is direct-from-feed, `Direct Source` is listed.

| Term | Definition / Formula | Depends On | Direct Source |
|---|---|---|---|
| `tradeTsUtc` | Trade timestamp in UTC. | none | ThetaData historical option trades (`trade_timestamp`/equivalent). |
| `symbol` | Underlying/root symbol (normalized uppercase). | none | ThetaData historical option trades (`symbol`/`root`). |
| `expiration` | Option expiration date (`YYYY-MM-DD`). | none | ThetaData historical option trades (`expiration`/`exp`). |
| `strike` | Option strike price. | none | ThetaData historical option trades (`strike`). |
| `right` | Contract side (`CALL`/`PUT`). | none | ThetaData historical option trades (`right`/`option_right`). |
| `price` | Trade price. | none | ThetaData historical option trades (`price`/`trade_price`). |
| `size` | Trade size (contracts). | none | ThetaData historical option trades (`size`/`trade_size`/`quantity`). |
| `bid` | Bid at trade time (when provided). | none | ThetaData historical option trades (`bid`). |
| `ask` | Ask at trade time (when provided). | none | ThetaData historical option trades (`ask`). |
| `conditionCode` | Trade condition/sale condition code. | none | ThetaData historical option trades (`condition_code`/`condition`). |
| `exchange` | Exchange identifier/code. | none | ThetaData historical option trades (`exchange`). |
| `sweeps` | Legacy sweep flag from configured condition-code mapping. | `conditionCode` | ThetaData `conditionCode` + Phenix rule config. |
| `value` | `price * size * 100` (premium notional). | `price`, `size` | Derived (from ThetaData trade fields). |
| `dte` | `ceil((expiration@21:00:00Z - tradeTsUtc) / 86400000)`. | `expiration`, `tradeTsUtc` | Derived (from ThetaData trade fields). |
| `spot` | Underlying spot/quote near trade time. | `symbol`, `tradeTsUtc` | ThetaData underlying quote endpoint class (candidate path: stock quote/snapshot). |
| `otmPct` | CALL: `((strike - spot)/spot)*100`; PUT: `((spot - strike)/spot)*100`. | `right`, `strike`, `spot` | Derived (requires `spot`). |
| `executionSide` (`side`) | `AA` if `price >= ask + max(0.01,0.10*(ask-bid))`; else `ASK` if `price >= ask`; else `BID` if `price <= bid`; else `OTHER`. | `price`, `bid`, `ask` | Derived (from ThetaData trade+quote-at-trade fields). |
| `sentiment` | `bullish` if (`CALL` and `ASK|AA`) or (`PUT` and `BID`); `bearish` if (`PUT` and `ASK|AA`) or (`CALL` and `BID`); else `neutral`. | `right`, `executionSide` | Derived. |
| `dayVolume` | Running contract-day volume for `(symbol,expiration,strike,right,trade_day_utc)`. | `symbol`, `expiration`, `strike`, `right`, `size`, `tradeTsUtc` | Derived (from ThetaData trades). |
| `oi` | Open interest for contract/day reference. | `symbol`, `expiration`, `strike`, `right`, `trade_day_utc` | ThetaData `/v3/option/history/open_interest` (`symbol`,`expiration`,`strike`,`right`,`date`). |
| `volOiRatio` | `dayVolume / max(oi,1)`. | `dayVolume`, `oi` | Derived (depends on ThetaData historical OI). |
| `repeat3m` | Count of same `(symbol,expiration,strike,right,executionSide)` in trailing 180s window. | `symbol`, `expiration`, `strike`, `right`, `executionSide`, `tradeTsUtc` | Derived. |
| `symbolVol1m` | Per-symbol 1-minute contract volume sum. | `symbol`, `size`, `tradeTsUtc` | Derived. |
| `symbolVolBaseline15m` | Mean of prior rolling 15 one-minute `symbolVol1m` buckets. | `symbolVol1m`, `tradeTsUtc` | Derived. |
| `openWindowBaseline` | Baseline `symbolVol1m` in ET open window context (09:30-10:30). | `symbolVol1m`, `tradeTsUtc` | Derived. |
| `bullishRatio15m` | Rolling 15-minute `bullish / (bullish + bearish)` directional ratio. | `sentiment`, `tradeTsUtc`, `symbol` | Derived. |
| `valuePctile` | Normalized percentile-like value of `value` within current enrichment scope/day. | `value`, enrichment cohort | Derived. |
| `volOiNorm` | Normalized `volOiRatio` input used by scoring (`clamp(volOiRatio/5,0,1)`). | `volOiRatio` | Derived. |
| `repeatNorm` | Normalized repeat input (`clamp(repeat3m/repeatFlowMin,0,1)`). | `repeat3m` | Derived. |
| `otmNorm` | Normalized moneyness input (`clamp(abs(otmPct)/25,0,1)`). | `otmPct` | Derived. |
| `sideConfidence` | Confidence weight from execution side (`AA=1`, `ASK=0.85`, `BID=0.7`, `OTHER=0.25`). | `executionSide` | Derived (rule constant). |
| `sigScore` | `0.35*valuePctile + 0.25*volOiNorm + 0.20*repeatNorm + 0.10*otmNorm + 0.10*sideConfidence` (clamped to `[0,1]`). | `valuePctile`, `volOiNorm`, `repeatNorm`, `otmNorm`, `sideConfidence` | Derived. |
| `standardMonthlyThirdFriday` | Boolean calendar helper: expiration date is Friday and day-of-month in `[15..21]`. | `expiration` | Derived. |

Notes:
- Terms depending on `oi` (especially `volOiRatio`, `unusual`, parts of `urgent`) depend on successful Theta OI hydration for the requested day/contract; when unavailable, API returns `metric_unavailable`.
- `/api/flow/oi*` remains a reference override/fallback store and does not replace Theta as the primary OI source for historical enrichment.
- `side` filter parameter maps to `executionSide` in implementation and this spec.

## 6. Public Endpoint Matrix

| Method | Path | Mode | Purpose |
|---|---|---|---|
| GET | `/health` | Ops | Liveness |
| GET | `/ready` | Ops | Readiness (Theta + DB + backlog checks) |
| GET | `/api/flow` | Real-time | Snapshot query |
| GET | `/api/flow/stream` | Real-time | Live updates (SSE target, poll-compatible fallback) |
| GET | `/api/flow/facets` | Real-time | Facet counts |
| GET | `/api/flow/summary` | Real-time | Top tiles and summary ratios |
| GET | `/api/flow/filters/catalog` | Real-time | Filter catalog, thresholds, ranges |
| GET | `/api/flow/oi` | Reference data | Query reference/fallback option OI cache |
| GET | `/api/flow/oi/sources` | Reference data | List OI source/date coverage in local cache |
| POST | `/api/flow/oi/sync` | Reference data | Fetch and upsert OI rows from configured external source URL |
| GET | `/api/flow/:id` | Real-time | Row detail |
| GET | `/api/flow/historical` | Historical | Day-bound query with sync + enrichment |
| POST | `/api/flow/presets` | Saved queries | Create preset |
| GET | `/api/flow/presets/:id` | Saved queries | Read preset |
| POST | `/api/flow/alerts` | Saved queries | Create alert |
| GET | `/api/flow/alerts/:id` | Saved queries | Read alert |

`/api/v1` aliases apply to all `/api/flow...` endpoints.

## 7. Real-Time APIs

### 7.1 `GET /api/flow`
Snapshot list API for flow rows.

#### Query Parameters

##### Pagination and sort
- `limit: integer` (optional, default `25`, max `100`)
- `cursor: string` (optional)
- `sortBy: string` (optional)
  - Current baseline: `id,symbol,strategy,status,timeframe,pnl,volume,createdAt,updatedAt`
  - Target enriched support: `tradeTsUtc,value,size,dte,otmPct,volOiRatio,repeat3m,sigScore`
- `sortOrder: asc|desc` (optional, default `desc`)

##### Core row filters
- `id: string`
- `symbol: string`
- `status: string`
- `strategy: string`
- `timeframe: string`
- `search: string`
- `from: ISO-8601 UTC`
- `to: ISO-8601 UTC`
- `minPnl: number`
- `maxPnl: number`
- `minVolume: number`
- `maxVolume: number`

##### Execution and threshold compatibility filters
- `execution: csv` (tokens: `calls,puts,bid,ask,aa,sweeps`)
- `chips: csv` (supports both execution and advanced chips)
- Boolean aliases: `calls,puts,bid,ask,aa,sweeps`
- Threshold aliases:
  - `sizeValue: csv` (`100k+`, `sizable`, `whales`, `large size` aliases)
  - `100k: boolean`
  - `sizable: boolean`
  - `whales: boolean`
  - `largeSize: boolean`

##### Enriched V1 filters (target)
- `right: CALL|PUT|C|P`
- `type: call|put` (alias to `right`)
- `expiration: YYYY-MM-DD` (exact or bounded variant by implementation)
- `side: BID|ASK|AA|OTHER`
- `sentiment: bullish|bearish|neutral`
- `minValue: number`
- `maxValue: number`
- `minSize: number`
- `maxSize: number`
- `minDte: number`
- `maxDte: number`
- `minOtmPct: number`
- `maxOtmPct: number`
- `minVolOi: number`
- `minRepeat3m: number`
- `minSigScore: number`
- `maxSigScore: number`

##### Runtime/version controls
- `filterVersion: legacy|candidate`
- `source: fixtures|real-ingest` (integration/test mode)
- `artifactPath: string` (integration/test mode)

#### Response (200)

```json
{
  "data": ["FlowRowV2"],
  "page": {
    "limit": 25,
    "hasMore": true,
    "nextCursor": "...",
    "sortBy": "createdAt",
    "sortOrder": "desc",
    "total": 123
  },
  "meta": {
    "filterVersion": "legacy",
    "observability": {
      "source": "fixtures|real-ingest",
      "artifactPath": null,
      "rowCount": 0,
      "fallbackReason": null
    }
  }
}
```

#### Errors
- `400 invalid_query`
- `500 query_failed`

### 7.2 `GET /api/flow/stream`
Live flow updates.

#### Query Parameters
- Supports all `/api/flow` filter and pagination params.
- `watermark: string|number` (optional; resume token)
- `transport: sse|poll` (optional; default implementation-defined)
- `heartbeatSec: integer` (optional)

#### SSE Event Contract (target)
- Event: `flow.updated`

```json
{
  "sequence": 123,
  "watermark": "opaque-token",
  "eventType": "flow.updated",
  "flow": "FlowRowV2"
}
```

- Event: `keepalive`

```json
{
  "sequence": 124,
  "watermark": "opaque-token",
  "eventType": "keepalive"
}
```

#### Poll-compatible JSON Contract (current-compatible)

```json
{
  "data": [
    {
      "sequence": 1,
      "eventType": "flow.updated",
      "flow": "FlowRow"
    }
  ],
  "page": "PageMeta",
  "meta": { "filterVersion": "legacy" }
}
```

### 7.3 `GET /api/flow/facets`
Facet aggregations for UI controls.

#### Query Parameters
- Accepts same filters as `/api/flow` (except pagination/cursor).

#### Response (200)

```json
{
  "facets": {
    "symbol": { "AAPL": 10 },
    "status": { "open": 8 },
    "sentiment": { "bullish": 5, "bearish": 3, "neutral": 2 },
    "chips": { "calls": 7, "100k+": 6 }
  },
  "total": 10,
  "meta": { "filterVersion": "legacy", "ruleVersion": "historical-v1" }
}
```

### 7.4 `GET /api/flow/summary`
Top tiles and summary ratios for dashboard header.

#### Query Parameters
- Accepts same filters as `/api/flow`.
- `topSymbolsLimit: integer` (optional, default `10`, max `50`).

#### Response (200)

```json
{
  "data": {
    "totals": {
      "rows": 0,
      "contracts": 0,
      "premium": 0,
      "bullish": 0,
      "bearish": 0,
      "neutral": 0
    },
    "ratios": {
      "bullishRatio": 0,
      "highSigRatio": 0,
      "unusualRatio": 0
    },
    "topSymbols": [
      { "symbol": "AAPL", "rows": 10, "premium": 123456.78 }
    ]
  },
  "meta": { "filterVersion": "legacy", "ruleVersion": "historical-v1" }
}
```

### 7.5 `GET /api/flow/filters/catalog`
Catalog of supported chips, thresholds, ranges, and enum filters.

#### Query Parameters
- `filterVersion: legacy|candidate` (optional)
- `includeDisabled: boolean` (optional, default `false`)

#### Response (200)

```json
{
  "data": {
    "ruleVersion": "historical-v1",
    "thresholds": {
      "premium100kMin": 100000,
      "premiumSizableMin": 250000,
      "premiumWhalesMin": 500000,
      "sizeLargeMin": 1000,
      "repeatFlowMin": 20,
      "highSigMin": 0.9
    },
    "chips": [
      {
        "id": "calls",
        "label": "Calls",
        "aliases": ["calls", "call", "c"],
        "category": "execution",
        "requiredMetrics": ["execution"],
        "rule": "right = CALL"
      }
    ],
    "enums": {
      "right": ["CALL", "PUT"],
      "sentiment": ["bullish", "bearish", "neutral"],
      "side": ["BID", "ASK", "AA", "OTHER"]
    },
    "ranges": {
      "sigScore": { "min": 0, "max": 1 },
      "dte": { "min": -30, "max": 3650 },
      "otmPct": { "min": -100, "max": 1000 }
    }
  },
  "meta": { "filterVersion": "legacy" }
}
```

### 7.6 `GET /api/flow/:id`
Get a single flow row by identifier.

#### Path Parameters
- `id: string` (required)

#### Response
- `200` with `{ "data": FlowRow }`
- `404` with `not_found`

### 7.7 `GET /api/flow/oi`
Query cached external OI rows (source-tagged).

Query params:
- `symbol` (optional)
- `asOfDate` or `date` (`YYYY-MM-DD`, optional)
- `expiration` (`YYYY-MM-DD`, optional)
- `right` (`CALL|PUT|C|P`, optional)
- `strike` (optional)
- `source` (optional)
- `limit` (optional, default `100`, max `2000`)

Response:
- `200` with `{ data: OIRow[], meta }`

### 7.8 `GET /api/flow/oi/sources`
List source/date coverage currently cached in SQLite.

Response:
- `200` with grouped source/date rows and counts.

### 7.9 `POST /api/flow/oi/sync`
Fetch and upsert OI rows from configured government/regulatory source URL.

Request body:
- `source: string` (required, e.g., `FINRA`, `CFTC`, `CME`)
- `sourceUrl: string` (optional if env mapping exists for `source`)
- `asOfDate: YYYY-MM-DD` (optional default for rows missing date fields)

Response:
- `200` with sync result (`fetchedRows`, `acceptedRows`, `rejectedRows`, `upsertedRows`)
- `400 invalid_query` for missing source/url
- `502 gov_source_fetch_failed` for upstream fetch failures

## 8. Historical API

### 8.1 `GET /api/flow/historical`
Historical day-bound query with Theta sync + SQLite cache + enrichment.

#### Required Query Parameters
- `from: ISO-8601 UTC`
- `to: ISO-8601 UTC`
- `symbol: string`

#### Validation Rules
- `from` and `to` must be valid ISO UTC timestamps.
- `from <= to`.
- `from` and `to` must be same UTC calendar date.

#### Optional Query Parameters
- `limit: integer` (default `100`, max `1000`)
- All enriched filter params supported by `/api/flow` target:
  - `chips,right,type,side,sentiment,minValue,maxValue,minSize,maxSize,minDte,maxDte,minOtmPct,maxOtmPct,minVolOi,minRepeat3m,minSigScore,maxSigScore,expiration`

#### Cache Behavior Contract
- Day cache table: `option_trade_day_cache`
  - `cache_status = full|partial`
- Metric cache table: `option_trade_metric_day_cache`
  - per metric, per symbol, per day
- If explicit `limit` is present during initial sync, day/metric cache is marked `partial`.
- No-limit successful sync can upgrade cache to `full`.
- If day cache is `full`, Theta fetch is skipped.

#### Response (200)

```json
{
  "data": ["FlowRowV2"],
  "meta": {
    "source": "sqlite",
    "dbPath": "/abs/path/data/phenixflow.sqlite",
    "dateRange": {
      "from": "2026-02-13T00:00:00.000Z",
      "to": "2026-02-13T23:59:59.999Z"
    },
    "filter": {
      "symbol": "AAPL",
      "chips": ["calls"],
      "right": "CALL",
      "sentiment": "bullish"
    },
    "total": 100,
    "sync": {
      "synced": false,
      "reason": "day_cache_full",
      "fetchedRows": 0,
      "upsertedRows": 0,
      "cachedRows": 163274,
      "cacheStatus": "full"
    },
    "enrichment": {
      "synced": false,
      "reason": "metric_cache_full",
      "rowCount": 163274
    }
  }
}
```

#### Historical Error Codes
- `400 invalid_query`
- `422 metric_unavailable`
- `500 enrichment_failed`
- `500 query_failed`
- `502 thetadata_sync_failed`
- `503 db_unavailable`
- `503 thetadata_not_configured`

#### `metric_unavailable` details shape

```json
{
  "error": {
    "code": "metric_unavailable",
    "message": "Required metric cache is not full for: otmPct, volOiRatio",
    "details": [
      {
        "metric": "otmPct",
        "cacheStatus": "partial",
        "lastError": null
      }
    ]
  }
}
```

## 9. Saved Filters and Alerts APIs

### 9.1 `POST /api/flow/presets`
### 9.2 `POST /api/flow/alerts`
Create saved query state.

#### Request Body
- `name: string` (optional)
- One of:
  - `payload` (legacy contract object)
  - `query` (legacy alias)
  - direct V2 DSL object

#### V2 DSL Shape

```json
{
  "version": 2,
  "combinator": "and|or",
  "clauses": [
    { "field": "symbol", "op": "eq", "value": "AAPL" }
  ]
}
```

#### Response (201)

```json
{
  "data": {
    "id": "preset_001",
    "name": "My Preset",
    "payloadVersion": "v2",
    "payload": { "version": 2, "combinator": "and", "clauses": [] },
    "queryDslV2": { "version": 2, "combinator": "and", "clauses": [] },
    "createdAt": "...",
    "updatedAt": "..."
  }
}
```

### 9.3 `GET /api/flow/presets/:id`
### 9.4 `GET /api/flow/alerts/:id`
Read saved state.

#### Query Parameters
- `payloadVersion: v2|legacy` (optional)

#### Response
- `200` with saved record.
- `404 not_found` if id missing.

## 10. Operational APIs

### 10.1 `GET /health`
Liveness check.

Response:

```json
{ "status": "ok" }
```

### 10.2 `GET /ready` (target contract)
Readiness gate for deployment/orchestration.

Response (200 example):

```json
{
  "status": "ready",
  "checks": {
    "db": "ok",
    "thetadata": "ok",
    "enrichmentBacklog": "ok"
  },
  "version": "0.1.0"
}
```

Response (503 example):

```json
{
  "status": "not_ready",
  "checks": {
    "db": "ok",
    "thetadata": "fail",
    "enrichmentBacklog": "ok"
  },
  "reason": "thetadata_unreachable"
}
```

## 11. External ThetaData Endpoint Matrix (Dependency APIs)

These are not Phenix public endpoints; they are required upstream APIs.

### 11.1 Historical Option Trades (required)
- Path default: `/v3/option/history/trade_quote`
- Env: `THETADATA_HISTORICAL_OPTION_PATH`
- Required params:
  - `symbol`
  - `date` (`YYYYMMDD`)
  - `expiration=*` (or explicit expiration)
  - `format=json`

### 11.2 Option Stream Ingest (required for real-time target)
- Path env: `THETADATA_INGEST_PATH`
- Required contract:
  - reconnect-safe streaming or poll loop
  - watermark/checkpoint support
  - JSON payload with option trade + quote-at-trade fields

### 11.3 Underlying Spot/Quote (required for `spot`, `otmPct`)
- Candidate path class: stock snapshot/history quote endpoint
- Required params:
  - `symbol`
  - `timestamp` or bounded time window
  - `format=json`

### 11.4 Open Interest (required for `oi`, `volOiRatio`)
- Path default: `/v3/option/history/open_interest`
- Path env override: `THETADATA_OI_PATH`
- Required params:
  - `symbol`
  - `date` (`YYYYMMDD`)
  - `expiration` (`*` for bulk hydration or explicit contract expiration)
  - `strike` and `right` (required for per-contract fallback request)
  - `format=json`

### 11.5 Universe/Reference Endpoints (supported in current entitlement guide)
- `/v3/stock/list/symbols?format=json`
- `/v3/stock/snapshot/quote?symbol=AAPL&format=json`
- `/v3/option/list/roots?format=json`

## 12. Error Code Registry

| HTTP | code | Meaning |
|---|---|---|
| 400 | `invalid_query` | Parameter validation failed |
| 404 | `not_found` | Resource not found |
| 422 | `metric_unavailable` | Required metric cache not full for requested filters |
| 500 | `query_failed` | Query execution failure |
| 500 | `enrichment_failed` | Derived-metric pipeline failed |
| 500 | `gov_oi_sync_failed` | OI sync/parsing failed before successful upsert |
| 502 | `thetadata_sync_failed` | Upstream Theta sync failed |
| 502 | `gov_source_fetch_failed` | Upstream gov/reg source fetch failed |
| 502 | `gov_source_payload_invalid` | OI source payload was empty or had no usable OI rows |
| 503 | `db_unavailable` | SQLite unavailable or schema bootstrap failed |
| 503 | `thetadata_not_configured` | Missing `THETADATA_BASE_URL` |

## 13. Performance and SLO Contract
- Ingest-to-UI lag target: `<= 5s p95` during regular market hours.
- `GET /api/flow` target: `<= 350ms p95` for `limit=50` with 3 active filters.
- Historical endpoint must avoid duplicate upstream fetch via day cache.
- Read path must not block on live Theta call when data for requested day/symbol is already full-cached.

## 14. Backward Compatibility Requirements
- Keep `/api/v1/...` aliases for all flow endpoints.
- Preserve legacy saved query payload behavior.
- Maintain existing compatibility params while introducing V2 enriched filters:
  - Existing: `minValue,maxValue,right,expiration,cursor pagination`
  - Legacy aliases: `execution,sizeValue,calls,puts,bid,ask,aa,sweeps,100k,sizable,whales,largeSize`

## 15. Implementation Checklist for Full Goal Coverage
- `/api/flow` must be moved from fixture mode to SQLite enriched-query mode.
- Implement `/api/flow/summary`.
- Implement `/api/flow/filters/catalog`.
- Implement readiness endpoint `/ready` with Theta + DB + backlog checks.
- Complete Theta spot integration so `metric_unavailable` for `otm`/spot-dependent chips is eliminated on full sync days.
