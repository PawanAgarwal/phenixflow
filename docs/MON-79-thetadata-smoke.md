# MON-79 — ThetaData Connectivity + Entitlement Smoke

## Purpose
Validate live ThetaData access (non-fixture) by:
1. probing entitlement endpoint
2. downloading a real artifact endpoint
3. persisting artifact + machine-readable report for PM/CI evidence

## Command

```bash
npm run mon79:thetadata:smoke
```

## Required environment

- `THETADATA_BASE_URL` (unless full URLs are passed in path vars)
- `THETADATA_DOWNLOAD_PATH` (**required**; real data endpoint path or full URL)

Authentication (choose one):
- `THETADATA_API_KEY`
- `THETADATA_USERNAME` + `THETADATA_PASSWORD`

Optional:
- `THETADATA_ENTITLEMENT_PATH` (default: `/v2/system/entitlements`)
- `THETADATA_CONNECT_TIMEOUT_MS` (default: `8000`)
- `THETADATA_DOWNLOAD_TIMEOUT_MS` (default: `20000`)
- `THETADATA_RETRY_DELAYS_MS` CSV (default: `2000,5000,15000`)
- `THETADATA_OUTPUT_DIR` (default: `artifacts/mon-79`)

## Output evidence
- Download artifact: `artifacts/mon-79/thetadata-download-<timestamp>.<ext>`
- Report: `artifacts/mon-79/thetadata-smoke-report-<timestamp>.json`

Report includes:
- status assertions (`entitlementOk`, `artifactDownloadOk`, `artifactBytesGtZero`)
- endpoint status + attempts
- retry/backoff policy
- timeout taxonomy
- artifact path/bytes/sha256/(rowCount if parseable)
- step logs (attempt-level)

## CI usage
Set the env vars as CI secrets, then run:

```bash
npm ci
npm run mon79:thetadata:smoke
```

Treat non-zero exit as smoke failure.
