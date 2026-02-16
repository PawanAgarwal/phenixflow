# ThetaData Developer Guide (Options Standard + Stocks Value)

**Scope:** This workspace is entitled for **Options Standard** + **Stocks Value** only.  
Use this guide before writing or running dev/qa tests that hit ThetaData via local ThetaTerminal v3.

---

## 1) Base URL + quick setup

```bash
# Default local ThetaTerminal v3 URL (from openapi docs)
export THETADATA_BASE_URL="http://127.0.0.1:25503"

# ThetaTerminal auth is provided via local creds.txt used by the Java process.
# Optional diagnostic pointer:
export THETADATA_CREDS_FILE="./creds.txt"
```

If your environment uses a different host/port, override `THETADATA_BASE_URL` and keep all commands below unchanged.

---


## 1b) MON-79 unblock requirements (runtime env)

When PM asks to unblock MON-79 ThetaData smoke execution, set these exact vars:

```bash
# Required for MON-79 smoke scripts
export THETADATA_BASE_URL="http://127.0.0.1:25503"   # default ThetaTerminal v3 host:port
export THETADATA_DOWNLOAD_PATH="/v3/stock/list/symbols?format=json"

# ThetaTerminal auth is handled by local creds.txt used by the Java process.
# (Optional) keep this for diagnostics only:
export THETADATA_CREDS_FILE="./creds.txt"

# Reference only (human docs/bootstrap)
export THETADATA_GETTING_STARTED_URL="https://docs.thetadata.us/Articles/Getting-Started/Getting-Started.html"
export THETADATA_JAR_URL="https://download-unstable.thetadata.us/ThetaTerminalv3.jar"
```

Then run exactly:

```bash
cd /Users/pawanagarwal/github/phenixflow
npm run -s mon79:thetadata:config:check
npm run -s mon79:thetadata:smoke
npm run -s mon79:thetadata:smoke:runner
```

Expected:
- `config:check` returns `ok: true`
- `smoke`/`smoke:runner` produce real artifacts under `artifacts/mon-79/` (or configured output path)

If `THETADATA_DOWNLOAD_PATH` is relative and `THETADATA_BASE_URL` is missing, preflight should fail by design.

---

## 2) Fast connectivity checks (copy/paste)

```bash
# A) Basic TCP/HTTP reachability
curl -i "$THETADATA_BASE_URL/"

# B) Options universe/metadata style check (should return JSON, not HTML/login error)
curl -sS "$THETADATA_BASE_URL/v3/option/list/roots?format=json" | head -c 400 && echo

# C) Stocks universe/metadata style check
curl -sS "$THETADATA_BASE_URL/v3/stock/list/symbols?format=json" | head -c 400 && echo
```

**Expected outcome:** HTTP response + JSON payload (or a structured API error), not connection refused/timeout.

---

## 3) Endpoint examples expected to work with this tier

> These are practical examples for **options standard historical/reference** and **stocks value-level reference/history** usage.

```bash
# Free/value-safe symbol list
curl -sS "$THETADATA_BASE_URL/v3/stock/list/symbols?format=json" | head -c 500 && echo

# Value-tier snapshot quote example
curl -sS "$THETADATA_BASE_URL/v3/stock/snapshot/quote?symbol=AAPL&format=json" | head -c 500 && echo

# Option roots metadata
curl -sS "$THETADATA_BASE_URL/v3/option/list/roots?format=json" | head -c 500 && echo
```

**Expected outcome:** Valid JSON with rows/data arrays (or explicit "no data" for date/symbol issues).

---

## 4) Examples likely to fail due to entitlement limits

Use these to confirm entitlement boundaries (helpful for QA).

```bash
# Likely premium-only / higher-tier features (examples)
curl -sS "$THETADATA_BASE_URL/v3/option/hist/greeks?root=AAPL&expiration=20260116&strike=200&right=C&start_date=20260101&end_date=20260131&format=json"

curl -sS "$THETADATA_BASE_URL/v3/stock/hist/nbbo?symbol=AAPL&start_date=20260101&end_date=20260102&format=json"

curl -sS "$THETADATA_BASE_URL/v3/stock/hist/trade?symbol=AAPL&start_date=20260102&end_date=20260102&format=json"
```

**Expected outcome:** entitlement/permission/plan error (or endpoint unavailable) rather than normal data rows.

---

## 5) Error interpretation cheatsheet

- **Connection refused / timeout** → ThetaData service not running or wrong `THETADATA_BASE_URL`.
- **401/403-like auth/permission errors** → terminal credentials/entitlement issue (check local creds.txt + subscription).
- **Structured error mentioning plan/entitlement** → endpoint is outside Options Standard + Stocks Value.
- **200 with empty dataset** → request shape valid, but no data for symbol/date/session filters.
- **4xx validation error** → bad params (date format, expiration, strike/right, etc.).

When logging failures in tests, always capture:
1) full request URL (minus secrets), 2) HTTP code, 3) raw error body.

---

## 6) Agent playbook (must run before coding/tests)

For spawned **dev/qa agents**:

1. `export THETADATA_BASE_URL=...` (or confirm already set).  
2. Run the 3 connectivity checks in section 2.  
3. Run at least 1 expected-success endpoint from section 3.  
4. Run 1 expected-fail entitlement probe from section 4.  
5. Record results in test notes:
   - reachable? (Y/N)
   - success endpoint returned data/valid JSON? (Y/N)
   - entitlement boundary error observed? (Y/N)
6. Only then start implementation/test execution.

If step 2 fails, stop and fix environment first. If step 4 unexpectedly succeeds, note that entitlements may have changed and update this guide.

---

## 7) Do / Don’t for this project

### Do
- Treat **Options Standard + Stocks Value** as hard constraints.
- Build tests around reference/historical endpoints first.
- Fail fast with clear logs when entitlement errors occur.
- Keep endpoint usage centralized (single config/module for base URL + routes).

### Don’t
- Don’t assume premium datasets (full tick, NBBO depth, advanced greeks, full real-time feeds) are available.
- Don’t silently swallow entitlement errors and mark tests flaky.
- Don’t hardcode host/port in code; use `THETADATA_BASE_URL`.

---

## 8) Copy/paste smoke checklist (one block)

```bash
export THETADATA_BASE_URL="http://127.0.0.1:25503"

set -e

echo "[1/5] Reachability"
curl -fsS "$THETADATA_BASE_URL/" >/dev/null && echo "OK"

echo "[2/5] Options roots"
curl -fsS "$THETADATA_BASE_URL/v3/option/list/roots?format=json" | head -c 200 && echo

echo "[3/5] Stock roots"
curl -fsS "$THETADATA_BASE_URL/v3/stock/list/symbols?format=json" | head -c 200 && echo

echo "[4/5] Expected success"
curl -fsS "$THETADATA_BASE_URL/v3/stock/snapshot/quote?symbol=AAPL&format=json" | head -c 200 && echo

echo "[5/5] Expected entitlement boundary"
curl -sS "$THETADATA_BASE_URL/v3/stock/hist/trade?symbol=AAPL&start_date=20260102&end_date=20260102&format=json" | head -c 300 && echo

echo "Done"
```

Interpretation:
- Steps 1–4 should succeed with JSON output.
- Step 5 should return a clear permission/entitlement-style error in current tier.

---

## 9) MON-86 bootstrap + runtime preflight guard

Use MON-86 to automate local ThetaData service readiness before MON-79/80/81/82 flows.

### Command

```bash
cd /Users/pawanagarwal/github/phenixflow

# Normal mode (downloads jar if missing, starts service only when needed, waits for health)
npm run -s mon86:thetadata:bootstrap

# Dry run (safe for CI/local verification)
npm run -s mon86:thetadata:bootstrap -- --dry-run
```

### Env

```bash
export THETADATA_BASE_URL="http://127.0.0.1:25503"
export THETADATA_HEALTH_PATH="/"
export THETADATA_JAR_PATH="artifacts/thetadata/ThetaTerminal.jar"
export THETADATA_JAR_URL="https://download-unstable.thetadata.us/ThetaTerminalv3.jar"  # required only if jar missing
export THETADATA_GETTING_STARTED_URL="https://docs.thetadata.us/Articles/Getting-Started/Getting-Started.html"  # reference only

# ThetaTerminal must be started with valid creds.txt (email on line 1, password on line 2).
# Optional diagnostic pointer:
export THETADATA_CREDS_FILE="./creds.txt"
```

### Failure taxonomy (explicit)

- `MON86_ERR_MISSING_CREDS` → ThetaTerminal creds file missing/invalid (expects email line 1, password line 2)
- `MON86_ERR_PORT_IN_USE` → target port occupied while health check fails
- `MON86_ERR_DOWNLOAD_FAILED` → jar missing and download failed (or URL missing)
- `MON86_ERR_HEALTH_TIMEOUT` → start attempted but service did not become healthy in time
- `MON86_ERR_CONFIG` → invalid base URL or malformed config

Each run emits a summary JSON artifact under `artifacts/mon-86/` and prints `SUMMARY=...`.
