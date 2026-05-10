#!/usr/bin/env node

const {
  runWeeklyReturnExperiment,
  writeWeeklyReport,
} = require('../src/weekly-return');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--target') out.targetTicker = argv[++index].toUpperCase();
    else if (arg === '--safe') out.safeTicker = argv[++index].toUpperCase();
    else if (arg === '--target-mode') out.targetMode = argv[++index];
    else if (arg === '--predict-start') out.predictStart = argv[++index];
    else if (arg === '--train-start') out.trainStart = argv[++index];
    else if (arg === '--validation-start') out.validationStart = argv[++index];
    else if (arg === '--validation-end') out.validationEnd = argv[++index];
    else if (arg === '--test-start') out.testStart = argv[++index];
    else if (arg === '--test-end') out.testEnd = argv[++index];
    else if (arg === '--min-train-weeks') out.minTrainWeeks = Number(argv[++index]);
    else if (arg === '--min-lookback-days') out.minLookbackDays = Number(argv[++index]);
    else if (arg === '--cost-bps') out.costBps = Number(argv[++index]);
    else if (arg === '--initial-capital') out.initialCapital = Number(argv[++index]);
    else if (arg === '--strategies') out.strategyIds = argv[++index].split(',').map((value) => value.trim()).filter(Boolean);
    else if (arg === '--out') out.outPath = argv[++index];
  }
  return out;
}

function round(value, digits = 2) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : null;
}

function compactRows(rows, limit = 12) {
  return rows.slice(0, limit).map((row) => ({
    id: row.id,
    weeks: row.weeks,
    totalReturnPct: round(row.totalReturnPct),
    cagrPct: round(row.cagrPct),
    sharpe: round(row.sharpe, 3),
    maxDrawdownPct: round(row.maxDrawdownPct),
    avgExposurePct: round(row.averageExposurePct),
    signAccuracyPct: round(row.signAccuracyPct),
    correlation: round(row.correlation, 3),
  }));
}

function main() {
  const options = parseArgs(process.argv.slice(2));
  const report = runWeeklyReturnExperiment(options);
  const outputPath = writeWeeklyReport(report, options.outPath);
  console.log(JSON.stringify({
    outputPath,
    targetTicker: report.settings.targetTicker,
    targetMode: report.settings.targetMode,
    marketEndDate: report.data.marketEndDate,
    predictionWeeks: report.data.predictionWeeks,
    validationWeeks: report.data.validationWeeks,
    testWeeks: report.data.testWeeks,
    selectedOverallByValidation: report.selected.overallByValidation,
    selectedModelByValidation: report.selected.modelByValidation,
    latestOverall: report.selected.latestOverall,
    latestModel: report.selected.latestModel,
    validationTop: compactRows(report.rankings.validation, 8),
    testTop: compactRows(report.rankings.test, 8),
    fullPeriodTop: compactRows(report.rankings.fullPeriod, 8),
  }, null, 2));
}

main();
