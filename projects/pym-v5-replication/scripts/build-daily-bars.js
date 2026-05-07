#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, ensureDir, runtimePath } = require('../src/config');
const { openCalendarDays, resolveEndDate } = require('../src/calendar');
const { readDailyBarsForDay } = require('../src/market-data');
const { collectTickers } = require('../src/symphony');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
  }
  return out;
}

function defaultScorePath(config) {
  return runtimePath('source', `composer-${config.source.composerSymphonyId}-score.json`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const scorePath = args.scorePath || defaultScorePath(config);
  if (!fs.existsSync(scorePath)) {
    throw new Error(`Missing Composer score snapshot: ${scorePath}. Run npm run pym-v5:fetch-sources first.`);
  }
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const tickers = collectTickers(score);
  const startDate = args.startDate || config.windows.buildStartDate;
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const days = openCalendarDays(config.roots.calendar, startDate, endDate);
  const outPath = runtimePath(`pym-v5-daily-bars-${startDate}-${endDate}.jsonl`);
  const manifestPath = runtimePath(`pym-v5-daily-bars-${startDate}-${endDate}.manifest.json`);
  ensureDir(runtimePath());
  const stream = fs.createWriteStream(outPath);
  const missingByDay = [];
  let written = 0;

  for (let index = 0; index < days.length; index += 1) {
    const day = days[index];
    const bars = await readDailyBarsForDay(config, day, tickers);
    const missing = [...tickers].filter((ticker) => !bars.has(ticker));
    if (missing.length) missingByDay.push({ date: day.date, missing });
    [...bars.values()].sort((left, right) => left.ticker.localeCompare(right.ticker)).forEach((bar) => {
      stream.write(`${JSON.stringify(bar)}\n`);
      written += 1;
    });
    if ((index + 1) % 10 === 0 || index === days.length - 1) {
      console.log(`processed ${index + 1}/${days.length} open days through ${day.date}; rows=${written}`);
    }
  }

  await new Promise((resolve) => stream.end(resolve));
  const manifest = {
    generatedAt: new Date().toISOString(),
    provider: 'Massive',
    dataset: config.datasets.stockBars,
    startDate,
    endDate,
    openDayCount: days.length,
    tickerCount: tickers.size,
    tickers: [...tickers].sort(),
    rowsWritten: written,
    missingTickerDayCount: missingByDay.length,
    missingByDay,
    outputPath: outPath,
  };
  fs.writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`wrote ${outPath}`);
  console.log(`wrote ${manifestPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
