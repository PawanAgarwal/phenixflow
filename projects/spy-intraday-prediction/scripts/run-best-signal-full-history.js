#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs, asNumber } = require('../src/cli');
const { readJsonlStreaming } = require('../src/dataset');
const { runBestSignalFullHistory } = require('../src/monthly-best-signal');

function defaultDatasetPath(config) {
  return path.join(
    PROJECT_ROOT,
    'runtime',
    `spy-intraday-dataset-${config.windows.sensitivityTrain?.startDate || config.windows.train.startDate}-${config.dataPolicy.historicalCutoffDate}-with-option-features.jsonl`,
  );
}

function compactMonthly(month) {
  return {
    month: month.month,
    status: month.status,
    trainRows: month.trainRows,
    selectedMagnitudeThreshold: month.selectedMagnitudeThreshold,
    activePredictionCount: month.longShort?.activePredictionCount,
    succeeded: month.longShort?.succeeded,
    failed: month.longShort?.failed,
    successRatePct: month.longShort?.successRatePct,
    failureRatePct: month.longShort?.failureRatePct,
    abstained: month.longShort?.abstained,
    totalReturn: month.backtest?.totalReturn,
    longOnlySuccessRatePct: month.longOnly?.successRatePct,
    longOnlyTotalReturn: month.longOnlyBacktest?.totalReturn,
  };
}

function compactSweep(sweep) {
  return {
    protocol: sweep.protocol,
    status: sweep.status,
    summary: sweep.summary,
    monthly: sweep.monthly?.map(compactMonthly),
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const datasetPath = path.resolve(args.dataset || defaultDatasetPath(config));
  const outputPath = path.resolve(args.output || path.join(
    PROJECT_ROOT,
    'artifacts',
    'best-signal-full-history-2025-01-02-2026-04-27.json',
  ));
  const dailyMaxTrainRows = asNumber(args.dailyMaxTrainRows || args['daily-max-train-rows'], 25_000);
  const rows = await readJsonlStreaming(datasetPath);
  const report = runBestSignalFullHistory(rows, config, {
    dailyMaxTrainRows,
    onDayStart: ({ tradeDate, index, total, protocol }) => {
      if (index === 1 || index % 25 === 0 || index === total) {
        process.stderr.write(`[spy-intraday-best-signal] ${protocol} ${index}/${total} ${tradeDate}\n`);
      }
    },
  });
  ensureDir(path.dirname(outputPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath,
    signal: report.signal,
    frozenSweeps: report.frozenSweeps.map(compactSweep),
    dailyRetrainSweeps: report.dailyRetrainSweeps.map(compactSweep),
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = { main };
