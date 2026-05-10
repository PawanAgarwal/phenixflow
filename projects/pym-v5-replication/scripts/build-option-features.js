#!/usr/bin/env node
const fs = require('node:fs');

const { latestDatasetDate, openCalendarDays, resolveEndDate } = require('../src/calendar');
const { loadConfig } = require('../src/config');
const { readDailyBarsJsonl } = require('../src/backtest');
const { defaultScorePath, findLatestMassiveEodBarsPath } = require('../src/rebalance-report');
const { buildOptionFeatureFile, defaultOptionRoots, latestOptionBarsDate } = require('../src/option-features');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--output') out.outputPath = argv[++index];
    else if (arg === '--roots') out.roots = argv[++index].split(',').map((value) => value.trim().toUpperCase()).filter(Boolean);
  }
  return out;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || '2025-01-02';
  const requestedEnd = args.endDate || config.windows.endDate;
  const endDate = requestedEnd === 'auto'
    ? [
      resolveEndDate(config, requestedEnd),
      latestOptionBarsDate(config) || latestDatasetDate(config.roots.historical, config.datasets.optionBars || 'option_quotes_1m'),
    ].filter(Boolean).sort().at(0)
    : requestedEnd;
  const dailyBarsPath = args.dailyBarsPath || findLatestMassiveEodBarsPath();
  const scorePath = args.scorePath || defaultScorePath(config);
  if (!dailyBarsPath || !fs.existsSync(dailyBarsPath)) throw new Error('missing_massive_eod_bars');
  if (!fs.existsSync(scorePath)) throw new Error(`missing_score_snapshot:${scorePath}`);
  const market = readDailyBarsJsonl(dailyBarsPath);
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const roots = args.roots || defaultOptionRoots(score, market);
  const days = openCalendarDays(config.roots.calendar, startDate, endDate)
    .filter((day) => market.dates.includes(day.date));

  const result = await buildOptionFeatureFile({
    config,
    market,
    score,
    days,
    startDate,
    endDate,
    roots,
    outputPath: args.outputPath,
    onProgress: ({ day, processedDays, result: dayResult }) => {
      if (processedDays === 1 || processedDays % 20 === 0 || day.date === endDate) {
        console.error(JSON.stringify({
          processedDays,
          date: day.date,
          rowsRead: dayResult.rowsRead,
          rowsUsed: dayResult.rowsUsed,
          activeRoots: Object.keys(dayResult.roots).length,
          missingFile: dayResult.missingFile,
        }));
      }
    },
  });

  console.log(JSON.stringify({
    outputPath: result.outputPath,
    manifestPath: result.manifestPath,
    processedDays: result.manifest.processedDays,
    totalRowsRead: result.manifest.totalRowsRead,
    totalRowsUsed: result.manifest.totalRowsUsed,
    missingFileDays: result.manifest.missingFileDays.length,
    selectedRoots: result.manifest.selectedRoots.length,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exit(1);
});
