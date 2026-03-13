#!/usr/bin/env node

const { execQuerySync, insertJsonRowsSync, queryRowsSync } = require('../../src/storage/clickhouse');

const DEFAULT_YEARS = 3;
const DEFAULT_REFRESH_LOOKBACK_DAYS = 10;
const DEFAULT_SOFR_API_URL = 'https://markets.newyorkfed.org/api/rates/secured/sofr/search.json';
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const maybeValue = argv[i + 1];
    if (!maybeValue || maybeValue.startsWith('--')) {
      out[key] = '1';
      continue;
    }
    out[key] = maybeValue;
    i += 1;
  }
  return out;
}

function parseBool(value, defaultValue = false) {
  if (value === undefined) return defaultValue;
  const raw = String(value).trim().toLowerCase();
  if (raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on') return true;
  if (raw === '0' || raw === 'false' || raw === 'no' || raw === 'off') return false;
  return defaultValue;
}

function parsePositiveInt(value, defaultValue) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1) return defaultValue;
  return Math.trunc(parsed);
}

function isIsoDate(value) {
  return DATE_RE.test(String(value || '').trim());
}

function normalizeIsoDate(value, fieldName) {
  const raw = String(value || '').trim();
  if (!isIsoDate(raw)) {
    throw new Error(`${fieldName}_must_be_YYYY-MM-DD:${raw || 'empty'}`);
  }
  return raw;
}

function toUtcDate(isoDate) {
  return new Date(`${isoDate}T00:00:00.000Z`);
}

function toIsoDate(value) {
  return value.toISOString().slice(0, 10);
}

function addDays(isoDate, days) {
  const date = toUtcDate(isoDate);
  date.setUTCDate(date.getUTCDate() + days);
  return toIsoDate(date);
}

function subtractYears(isoDate, years) {
  const date = toUtcDate(isoDate);
  date.setUTCFullYear(date.getUTCFullYear() - years);
  return toIsoDate(date);
}

function ensureSofrTable(env = process.env) {
  execQuerySync(`
    CREATE TABLE IF NOT EXISTS options.reference_sofr_daily
    (
      effective_date Date,
      rate_percent Float64,
      rate_decimal Float64,
      percentile_1 Nullable(Float64),
      percentile_25 Nullable(Float64),
      percentile_75 Nullable(Float64),
      percentile_99 Nullable(Float64),
      volume_billions Nullable(Float64),
      revision_indicator Nullable(String),
      source_url String,
      ingested_at_utc DateTime64(3, 'UTC')
    )
    ENGINE = ReplacingMergeTree(ingested_at_utc)
    PARTITION BY toYYYYMM(effective_date)
    ORDER BY (effective_date)
  `, {}, env);
}

function loadMaxEffectiveDate(env = process.env) {
  const rows = queryRowsSync(`
    SELECT
      count() AS row_count,
      max(effective_date) AS max_date
    FROM options.reference_sofr_daily
  `, {}, env);
  const rowCount = Number(rows?.[0]?.row_count || 0);
  if (!Number.isFinite(rowCount) || rowCount <= 0) return null;
  const maxDate = rows?.[0]?.max_date ? String(rows[0].max_date).slice(0, 10) : null;
  if (!maxDate || !isIsoDate(maxDate)) return null;
  return maxDate;
}

function clearSofrRange({ fromIso, toIso, env = process.env }) {
  execQuerySync(`
    ALTER TABLE options.reference_sofr_daily
    DELETE WHERE effective_date >= toDate({fromIso:String})
      AND effective_date <= toDate({toIso:String})
    SETTINGS mutations_sync = 1
  `, { fromIso, toIso }, env);
}

function toFiniteNumber(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  return parsed;
}

async function fetchSofrRows({ fromIso, toIso, sourceBaseUrl = DEFAULT_SOFR_API_URL }) {
  const endpoint = new URL(sourceBaseUrl);
  endpoint.searchParams.set('startDate', fromIso);
  endpoint.searchParams.set('endDate', toIso);

  const response = await fetch(endpoint.toString(), {
    headers: { accept: 'application/json' },
  });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(`sofr_http_${response.status}:${body.slice(0, 400)}`);
  }

  const payload = await response.json();
  const rows = Array.isArray(payload?.refRates) ? payload.refRates : [];
  return { rows, sourceUrl: endpoint.toString() };
}

function mapRows(rawRows = [], sourceUrl) {
  const ingestedAtIso = new Date().toISOString();
  return rawRows
    .map((row) => {
      const effectiveDate = String(row.effectiveDate || '').trim();
      const ratePercent = toFiniteNumber(row.percentRate);
      if (!isIsoDate(effectiveDate) || ratePercent === null) return null;
      return {
        effective_date: effectiveDate,
        rate_percent: ratePercent,
        rate_decimal: ratePercent / 100,
        percentile_1: toFiniteNumber(row.percentPercentile1),
        percentile_25: toFiniteNumber(row.percentPercentile25),
        percentile_75: toFiniteNumber(row.percentPercentile75),
        percentile_99: toFiniteNumber(row.percentPercentile99),
        volume_billions: toFiniteNumber(row.volumeInBillions),
        revision_indicator: row.revisionIndicator === undefined || row.revisionIndicator === null
          ? null
          : String(row.revisionIndicator),
        source_url: sourceUrl,
        ingested_at_utc: ingestedAtIso,
      };
    })
    .filter(Boolean)
    .sort((left, right) => String(left.effective_date).localeCompare(String(right.effective_date)));
}

function buildRange({ args, env = process.env }) {
  const todayIso = toIsoDate(new Date());
  const refreshMode = parseBool(args.refresh, true);
  const years = parsePositiveInt(args.years, DEFAULT_YEARS);
  const lookbackDays = parsePositiveInt(args['lookback-days'], DEFAULT_REFRESH_LOOKBACK_DAYS);

  const toIso = args.to ? normalizeIsoDate(args.to, 'to') : todayIso;

  let fromIso = null;
  if (args.from) {
    fromIso = normalizeIsoDate(args.from, 'from');
  } else if (refreshMode) {
    const maxDate = loadMaxEffectiveDate(env);
    if (maxDate) {
      fromIso = addDays(maxDate, -lookbackDays);
    } else {
      fromIso = subtractYears(toIso, years);
    }
  } else {
    fromIso = subtractYears(toIso, years);
  }

  if (fromIso > toIso) {
    throw new Error(`invalid_range:from(${fromIso})>to(${toIso})`);
  }

  return {
    fromIso,
    toIso,
    refreshMode,
    years,
    lookbackDays,
  };
}

function printSummary(summary) {
  console.log('[SOFR_SYNC_SUMMARY]', JSON.stringify(summary));
}

async function main() {
  const args = parseArgs(process.argv);
  const dryRun = parseBool(args['dry-run'], false);
  const sourceBaseUrl = String(args.url || DEFAULT_SOFR_API_URL).trim() || DEFAULT_SOFR_API_URL;

  ensureSofrTable(process.env);
  const range = buildRange({ args, env: process.env });
  const { rows: rawRows, sourceUrl } = await fetchSofrRows({
    fromIso: range.fromIso,
    toIso: range.toIso,
    sourceBaseUrl,
  });
  const mappedRows = mapRows(rawRows, sourceUrl);

  if (mappedRows.length === 0) {
    printSummary({
      inserted: 0,
      fetched: 0,
      from: range.fromIso,
      to: range.toIso,
      refreshMode: range.refreshMode,
      dryRun,
      sourceUrl,
    });
    return;
  }

  if (dryRun) {
    printSummary({
      inserted: 0,
      fetched: mappedRows.length,
      firstEffectiveDate: mappedRows[0].effective_date,
      lastEffectiveDate: mappedRows[mappedRows.length - 1].effective_date,
      from: range.fromIso,
      to: range.toIso,
      refreshMode: range.refreshMode,
      dryRun,
      sourceUrl,
    });
    return;
  }

  const insertQuery = `
    INSERT INTO options.reference_sofr_daily
    (
      effective_date,
      rate_percent,
      rate_decimal,
      percentile_1,
      percentile_25,
      percentile_75,
      percentile_99,
      volume_billions,
      revision_indicator,
      source_url,
      ingested_at_utc
    )
  `;
  clearSofrRange({ fromIso: range.fromIso, toIso: range.toIso, env: process.env });
  const inserted = insertJsonRowsSync(insertQuery, mappedRows, process.env, {
    chunkSize: 1000,
    maxChunkBytes: 4 * 1024 * 1024,
  });

  printSummary({
    inserted,
    fetched: mappedRows.length,
    firstEffectiveDate: mappedRows[0].effective_date,
    lastEffectiveDate: mappedRows[mappedRows.length - 1].effective_date,
    from: range.fromIso,
    to: range.toIso,
    refreshMode: range.refreshMode,
    dryRun,
    sourceUrl,
  });
}

main().catch((error) => {
  console.error('[SOFR_SYNC_ERROR]', error.message || String(error));
  process.exitCode = 1;
});
