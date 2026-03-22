#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { queryRowsSync, resolveFlowReadBackend, buildArtifactPath } = require('../../src/storage/clickhouse');
const { computeCoverageReport, DEFAULT_REQUIRED_SYMBOLS } = require('../src/vix-regime');

const START_DATE = String(process.env.START_DATE || '2025-01-02').trim();
const END_DATE = String(process.env.END_DATE || new Date().toISOString().slice(0, 10)).trim();
const REQUIRED_SYMBOLS = (process.env.REQUIRED_SYMBOLS || DEFAULT_REQUIRED_SYMBOLS.join(','))
  .split(',')
  .map((value) => value.trim().toUpperCase())
  .filter(Boolean);
const OUTPUT_PATH = path.resolve(
  process.env.OUTPUT_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', `vixregime-coverage-${START_DATE}-${END_DATE}.json`),
);

function buildCoverageQuery(symbols = []) {
  const escaped = symbols.map((symbol) => `'${symbol.replace(/'/g, "''")}'`).join(', ');
  return `
    SELECT
      symbol,
      toString(trade_date_utc) AS tradeDateUtc,
      toString(minute_bucket_utc) AS minuteUtc,
      close
    FROM options.stock_ohlc_minute_raw
    WHERE symbol IN (${escaped})
      AND trade_date_utc >= toDate({startDate:String})
      AND trade_date_utc <= toDate({endDate:String})
    ORDER BY symbol ASC, minute_bucket_utc ASC
  `;
}

function run() {
  const backend = resolveFlowReadBackend(process.env);
  if (backend !== 'clickhouse') {
    throw new Error(`clickhouse_backend_required:${backend}`);
  }

  const rows = queryRowsSync(buildCoverageQuery(REQUIRED_SYMBOLS), {
    startDate: START_DATE,
    endDate: END_DATE,
  }, process.env);

  const coverage = computeCoverageReport(rows, { requiredSymbols: REQUIRED_SYMBOLS });
  const output = {
    generatedAt: new Date().toISOString(),
    artifactPath: buildArtifactPath(process.env),
    startDate: START_DATE,
    endDate: END_DATE,
    rowCount: rows.length,
    coverage,
  };

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(output, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify(output, null, 2));
  if (!coverage.datasetReady) {
    process.exitCode = 2;
  }
}

run();
