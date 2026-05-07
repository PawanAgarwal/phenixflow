#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, artifactPath, ensureDir } = require('../src/config');
const { openCalendarDays, resolveEndDate } = require('../src/calendar');
const { fileCoverageForDay } = require('../src/market-data');

function countByStatus(rows) {
  return rows.reduce((acc, row) => {
    acc[row.status] = (acc[row.status] || 0) + 1;
    return acc;
  }, {});
}

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
  }
  return out;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || config.windows.buildStartDate;
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const days = openCalendarDays(config.roots.calendar, startDate, endDate);
  const coverage = days.map((day) => fileCoverageForDay(config, day));
  const statusCounts = countByStatus(coverage);
  const report = {
    generatedAt: new Date().toISOString(),
    provider: 'Massive',
    dataset: config.datasets.stockBars,
    startDate,
    endDate,
    openDayCount: days.length,
    statusCounts,
    ready: (statusCounts.attempted_missing || 0) === 0,
    missingOpenDays: coverage.filter((row) => row.status !== 'ready'),
  };
  const outPath = artifactPath(`coverage-${startDate}-${endDate}.json`);
  ensureDir(artifactPath());
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
  console.log(`wrote ${outPath}`);
  if (!report.ready) process.exitCode = 2;
}

main();
