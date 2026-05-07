#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs } = require('../src/cli');
const {
  coverageForDataset,
  liveParquetCoverage,
  openCalendarDays,
} = require('../src/coverage');

function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args['start-date'] || config.windows.train.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  const outputPath = path.resolve(
    args.output || path.join(PROJECT_ROOT, 'artifacts', `massive-coverage-${startDate}-${endDate}.json`),
  );
  const calendarDays = openCalendarDays(config.roots.calendar, startDate, endDate);
  const datasetIds = Object.values(config.datasets);
  const datasets = datasetIds.map((datasetId) => coverageForDataset({
    root: config.roots.historical,
    datasetId,
    calendarDays,
  }));
  const provisional = liveParquetCoverage({
    root: config.roots.liveParquet,
    datasetIds,
    dayIso: config.dataPolicy.intradayProvisionalDate,
  });
  const report = {
    generatedAt: new Date().toISOString(),
    project: config.projectName,
    provider: config.dataPolicy.provider,
    historicalRoot: config.roots.historical,
    liveParquetRoot: config.roots.liveParquet,
    calendarPath: config.roots.calendar,
    startDate,
    endDate,
    historicalCutoffDate: config.dataPolicy.historicalCutoffDate,
    intradayProvisionalDate: config.dataPolicy.intradayProvisionalDate,
    openCalendarDayCount: calendarDays.length,
    datasets,
    historicalReady: datasets.every((dataset) => dataset.ready),
    provisionalLiveParquet: provisional,
    formalReportingNote: `Formal April reporting stops at ${config.dataPolicy.historicalCutoffDate}; ${config.dataPolicy.intradayProvisionalDate} is paper/live-style only.`,
  };

  ensureDir(path.dirname(outputPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify(report, null, 2));
  if (!report.historicalReady) process.exitCode = 2;
}

main();
