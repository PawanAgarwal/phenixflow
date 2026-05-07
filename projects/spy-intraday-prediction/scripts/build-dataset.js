#!/usr/bin/env node
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs, asNumber } = require('../src/cli');
const { datesForRange } = require('../src/splits');
const { buildDatasetToJsonl } = require('../src/dataset');

function outputName(startDate, endDate, includeOptions) {
  const suffix = includeOptions ? 'with-option-features' : 'no-option-features';
  return `spy-intraday-dataset-${startDate}-${endDate}-${suffix}.jsonl`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args['start-date'] || config.windows.train.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  if (endDate > config.dataPolicy.historicalCutoffDate) {
    throw new Error(
      `sealed_history_only:${endDate}:this builder intentionally stops at ${config.dataPolicy.historicalCutoffDate}; keep ${config.dataPolicy.intradayProvisionalDate} in a separate paper/live scoring path`,
    );
  }
  const includeOptions = args.options !== false;
  const maxDays = asNumber(args['max-days'], null);
  const dates = datesForRange(config, startDate, endDate);
  const selectedDates = maxDays ? dates.slice(0, maxDays) : dates;
  const outputPath = path.resolve(
    args.output || path.join(PROJECT_ROOT, 'runtime', outputName(startDate, endDate, includeOptions)),
  );

  ensureDir(path.dirname(outputPath));
  console.error(`[spy-intraday] building dataset for ${selectedDates.length} open days (${startDate}..${endDate})`);
  console.error(`[spy-intraday] options features: ${includeOptions ? 'enabled' : 'disabled'}`);
  const result = await buildDatasetToJsonl(config, selectedDates, outputPath, {
    includeOptions,
    onDayComplete: ({ dayIso, dayCount, rowCount, dayRows, elapsedMs }) => {
      console.error(`[spy-intraday] ${dayCount}/${selectedDates.length} ${dayIso} rows=${dayRows} total=${rowCount} elapsed=${(elapsedMs / 1000).toFixed(1)}s`);
    },
  });
  const report = {
    generatedAt: new Date().toISOString(),
    outputPath,
    rowCount: result.rowCount,
    startDate,
    endDate,
    selectedDateCount: result.dayCount,
    selectedDates,
    includeOptions,
    formalHistorical: endDate <= config.dataPolicy.historicalCutoffDate,
  };
  console.log(JSON.stringify(report, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
