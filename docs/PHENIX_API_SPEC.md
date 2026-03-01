# Phenix API Spec (Current Runtime)

Last updated: 2026-03-01

## 1. Scope
This document describes the **current implemented API contract** for:
1. Flow query/read APIs (`/api/flow*` and `/api/v1/flow*` aliases).
2. Historical sync/enrichment API (`/api/flow/historical`).
3. Saved presets/alerts APIs.
4. OI reference APIs.
5. Operational health/readiness APIs.

## 2. Base Paths and Versioning
1. Primary base path: `/api`.
2. Backward-compatible alias path: `/api/v1` for all `/api/flow*` endpoints.
3. Filter version selector: `filterVersion=legacy|candidate`.
4. Saved payload default on reads:
   - `/api/.../presets/:id` and `/api/.../alerts/:id` default to `payloadVersion=v2`.
   - `/api/v1/.../presets/:id` and `/api/v1/.../alerts/:id` default to `payloadVersion=legacy`.

## 3. Common Conventions
1. Content type is JSON except SSE mode on `/api/flow/stream`.
2. Timestamps are ISO-8601 UTC strings.
3. Comma-separated lists are used for multi-select query params (for example, `chips=calls,100k+`).
4. Boolean query values accepted: `true|false|1|0|yes|on`.
5. `/api/flow` pagination:
   - `limit` default `25`, max `100`.
6. `/api/flow/historical` pagination:
   - `limit` default `100`, max `1000`.

## 4. Core Row Contract (`FlowRowV2`)
Common row fields returned from flow/historical endpoints:
1. Identity and contract:
   - `id`, `tradeTsUtc`, `symbol`, `expiration`, `strike`, `right`.
2. Trade/quote:
   - `price`, `size`, `bid`, `ask`, `conditionCode`, `exchange`.
3. Enriched metrics:
   - `value`, `dte`, `spot`, `otmPct`, `dayVolume`, `oi`, `volOiRatio`, `repeat3m`.
4. Score and explainability:
   - `sigScore`, `sigScoreComponents`, `scoreQuality`, `missingMetrics`.
5. Rule metadata:
   - `ruleVersion`, `targetHorizon`.
6. Classification:
   - `sentiment`, `chips`.

Backward-compatible fields may also appear (`strategy`, `status`, `timeframe`, `pnl`, `volume`, `createdAt`, `updatedAt`).

## 5. Flow Chips and Score Gating
1. Score-dependent chips: `high-sig`, `unusual`, `urgent`.
2. These require:
   - non-degraded mode,
   - quality eligibility (`scoreQuality` rules),
   - directional eligibility (`sentiment` must be `bullish` or `bearish`).

## 6. Endpoint Matrix
1. `GET /health`
2. `GET /ready`
3. `GET /api/flow`
4. `GET /api/flow/stream`
5. `GET /api/flow/facets`
6. `GET /api/flow/summary`
7. `GET /api/flow/filters/catalog`
8. `GET /api/flow/:id`
9. `GET /api/flow/historical`
10. `GET /api/flow/oi`
11. `GET /api/flow/oi/sources`
12. `POST /api/flow/oi/sync`
13. `POST /api/flow/presets`
14. `GET /api/flow/presets/:id`
15. `POST /api/flow/alerts`
16. `GET /api/flow/alerts/:id`

All above have `/api/v1` aliases for `/api/flow*` routes.

## 7. Operational APIs
### 7.1 `GET /health`
Response `200`:
```json
{ "status": "ok" }
```

### 7.2 `GET /ready`
Response `200` when ready:
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
Response `503` when not ready:
```json
{
  "status": "not_ready",
  "checks": {
    "db": "fail",
    "thetadata": "ok",
    "enrichmentBacklog": "ok"
  },
  "reason": "db_unavailable"
}
```

## 8. Real-Time Flow APIs
### 8.1 `GET /api/flow`
Primary snapshot query API.

Supported query groups:
1. Pagination/sort: `limit`, `cursor`, `sortBy`, `sortOrder`.
2. Generic filters: `id`, `symbol`, `status`, `strategy`, `timeframe`, `search`, `from`, `to`, `minPnl`, `maxPnl`, `minVolume`, `maxVolume`.
3. Execution/chips: `execution`, `chips`, boolean aliases (`calls`, `puts`, `bid`, `ask`, `aa`, `sweeps`), threshold aliases (`100k`, `sizable`, `whales`, `largeSize`, `sizeValue`).
4. Enriched filters: `right`, `type`, `expiration`, `side`, `sentiment`, `minValue`, `maxValue`, `minSize`, `maxSize`, `minDte`, `maxDte`, `minOtmPct`, `maxOtmPct`, `minVolOi`, `maxVolOi`, `minRepeat3m`, `minSigScore`, `maxSigScore`.
5. Runtime/testing controls: `filterVersion`, `source`, `artifactPath`, `shadow`/`shadowCompare`.

Response shape:
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
    "ruleVersion": "v5_swing_default",
    "scoringModel": "v5_swing",
    "targetHorizon": "swing_1_5d",
    "observability": {
      "source": "sqlite",
      "artifactPath": "/abs/path/data/phenixflow.sqlite",
      "rowCount": 100,
      "fallbackReason": null
    },
    "degraded": false,
    "degradedReason": [],
    "lagSeconds": null,
    "lagThresholdSeconds": 30
  }
}
```

### 8.2 `GET /api/flow/stream`
1. Uses same filters as `/api/flow`.
2. If `transport=sse` or `Accept: text/event-stream`, emits SSE events:
   - `flow.updated`
   - `keepalive`
3. Otherwise returns JSON stream payload:
```json
{
  "data": [{ "sequence": 1, "eventType": "flow.updated", "flow": "FlowRowV2" }],
  "page": { "limit": 25, "hasMore": false, "nextCursor": null, "sortBy": "createdAt", "sortOrder": "desc", "total": 1 },
  "meta": { "filterVersion": "legacy" }
}
```

### 8.3 `GET /api/flow/facets`
Returns aggregated facets from current filtered result:
- `symbol`, `status`, `sentiment`, `chips`, plus `meta` (`filterVersion`, `ruleVersion`, `scoringModel`, optional `targetHorizon`).

### 8.4 `GET /api/flow/summary`
Returns:
1. `totals` (`rows`, `contracts`, `premium`, `bullish`, `bearish`, `neutral`)
2. `ratios` (`bullishRatio`, `highSigRatio`, `unusualRatio`)
3. `topSymbols` (configurable by `topSymbolsLimit`, default `10`, max `50`)

### 8.5 `GET /api/flow/filters/catalog`
Returns active runtime filter catalog:
1. `ruleVersion`, `scoringModel`, optional `targetHorizon`.
2. Active thresholds.
3. Chip definitions (`id`, `label`, `aliases`, `category`, `requiredMetrics`, `rule`).
4. Enums (`right`, `sentiment`, `side`) and ranges (`sigScore`, `dte`, `otmPct`).

### 8.6 `GET /api/flow/:id`
1. `200`: `{ "data": FlowRowV2 }`.
2. `404`: `{ "error": { "code": "not_found", ... } }`.

## 9. Historical API
### 9.1 `GET /api/flow/historical`
Single-day historical query with on-demand sync/enrichment.

Required query params:
1. `symbol`
2. `from` (ISO-8601 UTC)
3. `to` (ISO-8601 UTC)

Validation rules:
1. `from` and `to` are required and valid.
2. `from <= to`.
3. `from` and `to` must be same UTC day.

Optional filters:
- Same enriched filters/chips as supported by historical parser (`chips,right,type,expiration,side,sentiment,minValue,maxValue,minSize,maxSize,minDte,maxDte,minOtmPct,maxOtmPct,minVolOi,maxVolOi,minRepeat3m,minSigScore,maxSigScore`).

Response shape:
```json
{
  "data": ["FlowRowV2"],
  "meta": {
    "source": "sqlite",
    "dbPath": "/abs/path/data/phenixflow.sqlite",
    "dateRange": {
      "from": "2026-02-27T00:00:00.000Z",
      "to": "2026-02-27T23:59:59.999Z"
    },
    "filter": {
      "symbol": "AAPL",
      "chips": ["calls"],
      "right": "CALL",
      "expiration": null,
      "side": null,
      "sentiment": "bullish"
    },
    "total": 152137,
    "sync": {
      "synced": false,
      "reason": "day_cache_full",
      "fetchedRows": 0,
      "upsertedRows": 0,
      "cachedRows": 152137,
      "cacheStatus": "full"
    },
    "enrichment": {
      "synced": false,
      "reason": "metric_cache_full",
      "rowCount": 152137,
      "ruleVersion": "v5_swing_default",
      "scoringModel": "v5_swing",
      "targetHorizon": "swing_1_5d",
      "supplementalCache": {
        "spotHit": 1,
        "spotMiss": 0,
        "stockHit": 1,
        "stockMiss": 0,
        "oiHit": 14,
        "oiMiss": 0,
        "greeksHit": 27,
        "greeksMiss": 0
      }
    }
  }
}
```

Historical errors:
1. `400 invalid_query`
2. `422 metric_unavailable`
3. `500 enrichment_failed`
4. `500 query_failed`
5. `502 thetadata_sync_failed`
6. `503 db_unavailable`
7. `503 thetadata_not_configured`

## 10. OI Reference APIs
### 10.1 `GET /api/flow/oi`
Query local OI reference cache (`option_open_interest_reference`).

Optional filters:
- `symbol`, `asOfDate`/`date`, `expiration`, `right`, `source`, `strike`, `limit`.
- `limit` default `100`, max `2000`.

### 10.2 `GET /api/flow/oi/sources`
Returns grouped source/date coverage rows from local OI reference data.

### 10.3 `POST /api/flow/oi/sync`
Fetches and upserts OI rows from configured external source.

Body:
1. `source` (required)
2. `sourceUrl` (optional if mapped by source)
3. `asOfDate` (optional)

Error mapping:
1. `400 invalid_query` for missing required source inputs.
2. `502 gov_source_fetch_failed` for upstream fetch failure.
3. `502 gov_source_payload_invalid` for empty/unusable payload.
4. `500 gov_oi_sync_failed` for internal sync failure.

## 11. Saved Presets and Alerts
1. `POST /api/flow/presets`
2. `GET /api/flow/presets/:id`
3. `POST /api/flow/alerts`
4. `GET /api/flow/alerts/:id`

Create body accepts:
1. `name` (optional)
2. one of `payload`, `query`, or direct V2 DSL object.

Read supports:
- `payloadVersion=v2|legacy`.

## 12. Error Envelope and Codes
Standard envelope:
```json
{
  "error": {
    "code": "string_code",
    "message": "human-readable message",
    "details": []
  }
}
```

Common codes used by current runtime:
1. `invalid_query`
2. `not_found`
3. `metric_unavailable`
4. `db_unavailable`
5. `thetadata_not_configured`
6. `thetadata_sync_failed`
7. `enrichment_failed`
8. `query_failed`
9. `gov_source_fetch_failed`
10. `gov_source_payload_invalid`
11. `gov_oi_sync_failed`

## 13. Notes on Runtime Source Modes
1. Production path is SQLite-backed (`observability.source = sqlite`).
2. Fixture and artifact modes exist for testing (`source=fixtures|real-ingest`).
3. Degraded metadata is returned when fallback/no-live-data/lag conditions apply.
