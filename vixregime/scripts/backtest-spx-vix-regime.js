#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const {
  queryRowsSync,
  resolveFlowReadBackend,
  buildArtifactPath,
} = require('../../src/storage/clickhouse');
const {
  DEFAULT_REQUIRED_SYMBOLS,
  readThresholdConfig,
  computeCoverageReport,
  buildMinuteAlignment,
  buildDailyCloses,
  buildDailyFirstMinuteCloses,
  buildDailyFeatures,
  buildMinuteFeatures,
  computePortfolioPath,
  computeDailyNextOpenToClosePath,
} = require('../src/vix-regime');
const {
  loadEventCalendar,
  annotateDailyEventFeatures,
} = require('../src/event-days');

const START_DATE = String(process.env.START_DATE || '2025-01-02').trim();
const END_DATE = String(process.env.END_DATE || new Date().toISOString().slice(0, 10)).trim();
const REQUIRED_SYMBOLS = (process.env.REQUIRED_SYMBOLS || DEFAULT_REQUIRED_SYMBOLS.join(','))
  .split(',')
  .map((value) => value.trim().toUpperCase())
  .filter(Boolean);
const OUTPUT_PATH = path.resolve(
  process.env.OUTPUT_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', `vixregime-backtest-${START_DATE}-${END_DATE}.json`),
);
const THRESHOLD_PATH = path.resolve(
  process.env.THRESHOLD_PATH || path.join(__dirname, '..', 'config', 'vix-regime-thresholds.json'),
);
const BLS_CALENDAR_PATH = path.resolve(
  process.env.BLS_CALENDAR_PATH || path.join(__dirname, '..', 'config', 'bls-major-release-calendar.json'),
);
const FOMC_CALENDAR_PATH = path.resolve(
  process.env.FOMC_CALENDAR_PATH || path.join(__dirname, '..', 'config', 'fomc-dates.json'),
);
const EARNINGS_CALENDAR_PATH = path.resolve(
  process.env.EARNINGS_CALENDAR_PATH || path.join(__dirname, '..', 'config', 'major-earnings-days.json'),
);

function buildDatasetQuery(symbols = []) {
  const escaped = symbols.map((symbol) => `'${symbol.replace(/'/g, "''")}'`).join(', ');
  return `
    SELECT
      symbol,
      toString(trade_date_utc) AS tradeDateUtc,
      toString(minute_bucket_utc) AS minuteUtc,
      open,
      high,
      low,
      close,
      volume
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

  const thresholdConfig = readThresholdConfig(THRESHOLD_PATH);
  const rawRows = queryRowsSync(buildDatasetQuery(REQUIRED_SYMBOLS), {
    startDate: START_DATE,
    endDate: END_DATE,
  }, process.env);

  const coverage = computeCoverageReport(rawRows, { requiredSymbols: REQUIRED_SYMBOLS });
  if (!coverage.datasetReady) {
    throw new Error(`dataset_not_ready:${OUTPUT_PATH}`);
  }

  const alignedMinuteRows = buildMinuteAlignment(rawRows)
    .filter((row) => REQUIRED_SYMBOLS.every((symbol) => row.prices[symbol] !== undefined && row.prices[symbol] !== null));
  const alignedDailyRows = buildDailyCloses(rawRows)
    .filter((row) => REQUIRED_SYMBOLS.every((symbol) => row.closeBySymbol[symbol] !== undefined && row.closeBySymbol[symbol] !== null));
  const alignedDailyFirstMinuteRows = buildDailyFirstMinuteCloses(rawRows)
    .filter((row) => REQUIRED_SYMBOLS.every((symbol) => row.closeBySymbol[symbol] !== undefined && row.closeBySymbol[symbol] !== null));

  const eventCalendar = loadEventCalendar({
    startDate: START_DATE,
    endDate: END_DATE,
    blsPath: BLS_CALENDAR_PATH,
    fomcPath: FOMC_CALENDAR_PATH,
    earningsPath: EARNINGS_CALENDAR_PATH,
  });

  const dailyFeatures = annotateDailyEventFeatures(buildDailyFeatures(alignedDailyRows, thresholdConfig), eventCalendar);
  const minuteFeatures = buildMinuteFeatures(alignedMinuteRows, thresholdConfig, dailyFeatures);
  const dailyBacktest = computePortfolioPath(dailyFeatures, {
    transactionCostBps: thresholdConfig.execution.dailyTransactionCostBps,
    periodsPerYear: 252,
  });
  const dailyNextOpenBacktest = computeDailyNextOpenToClosePath(dailyFeatures, alignedDailyFirstMinuteRows, {
    transactionCostBps: thresholdConfig.execution.dailyTransactionCostBps,
    periodsPerYear: 252,
  });
  const minuteBacktest = computePortfolioPath(minuteFeatures, {
    transactionCostBps: thresholdConfig.execution.minuteTransactionCostBps,
    periodsPerYear: 252 * 390,
  });

  const output = {
    generatedAt: new Date().toISOString(),
    artifactPath: buildArtifactPath(process.env),
    startDate: START_DATE,
    endDate: END_DATE,
    thresholdPath: THRESHOLD_PATH,
    eventCalendarPaths: {
      bls: BLS_CALENDAR_PATH,
      fomc: FOMC_CALENDAR_PATH,
      earnings: EARNINGS_CALENDAR_PATH,
    },
    rowCount: rawRows.length,
    coverage,
    summaries: {
      daily: dailyBacktest.summary,
      dailyNextOpen: dailyNextOpenBacktest.summary,
      minute: minuteBacktest.summary,
    },
    daily: {
      features: dailyFeatures,
      observations: dailyBacktest.observations,
    },
    dailyNextOpen: {
      features: dailyFeatures,
      observations: dailyNextOpenBacktest.observations,
    },
    minute: {
      features: minuteFeatures,
      observations: minuteBacktest.observations,
    },
  };

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(output, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath: OUTPUT_PATH,
    coverageReady: coverage.datasetReady,
    dailySummary: dailyBacktest.summary,
    dailyNextOpenSummary: dailyNextOpenBacktest.summary,
    minuteSummary: minuteBacktest.summary,
  }, null, 2));
}

run();
