#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { computeCoverageReport, DEFAULT_REQUIRED_SYMBOLS } = require('../src/vix-regime');
const { loadMassiveMinuteRows, resolveMassiveDatasetRoots } = require('../src/massive-data');

const START_DATE = String(process.env.START_DATE || '2025-01-02').trim();
const END_DATE = String(process.env.END_DATE || new Date().toISOString().slice(0, 10)).trim();
const REQUIRED_SYMBOLS = (process.env.REQUIRED_SYMBOLS || DEFAULT_REQUIRED_SYMBOLS.join(','))
  .split(',')
  .map((value) => value.trim().toUpperCase())
  .filter(Boolean);
const OUTPUT_PATH = path.resolve(
  process.env.OUTPUT_PATH
    || path.join(__dirname, '..', 'artifacts', 'reports', `vixregime-coverage-${START_DATE}-${END_DATE}.json`),
);

async function run() {
  const rows = await loadMassiveMinuteRows({
    startDate: START_DATE,
    endDate: END_DATE,
    requiredSymbols: REQUIRED_SYMBOLS,
    env: process.env,
  });
  const dataSource = resolveMassiveDatasetRoots(process.env);

  const coverage = computeCoverageReport(rows, { requiredSymbols: REQUIRED_SYMBOLS });
  const output = {
    generatedAt: new Date().toISOString(),
    dataSource: {
      type: 'massive_csv',
      ...dataSource,
    },
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

run().catch((error) => {
  console.error(error.stack || error.message || String(error));
  process.exitCode = 1;
});
