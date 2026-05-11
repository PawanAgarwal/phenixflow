#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const zlib = require('node:zlib');

const { parseArgs, asNumber } = require('../src/cli');
const {
  PROJECT_ROOT,
  ensureDir,
  loadConfig,
  datasetCsvPath,
} = require('../src/config');
const { toNumber } = require('../src/csv');
const {
  getEtParts,
  isRegularSessionMinute,
  nsToMinuteMs,
} = require('../src/time');
const {
  loadCalendar,
  listDatasetDates,
  openCalendarDays,
} = require('../src/coverage');
const {
  DEFAULT_CONCRETUM_PARAMS,
  buildSigmaByMinute,
  sampleStd,
  simulateConcretumDay,
  updateMoveOpenHistory,
} = require('../src/concretum-strategy');

const SOURCE_LINKS = [
  {
    name: 'Concretum official Alpaca/Python article',
    url: 'https://concretumgroup.com/backtesting-7-years-of-free-data-beat-the-market-an-effective-intraday-momentum-strategy-for-the-sp500-etf-spy/',
  },
  {
    name: 'Concretum official SPY intraday momentum paper page',
    url: 'https://concretumgroup.com/beat-the-market-an-effective-intraday-momentum-strategy-for-sp500-etf-spy/',
  },
  {
    name: 'Concretum X post',
    url: 'https://x.com/ConcretumR/status/1799083286175641781',
  },
];

function round(value, digits = 6) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function pct(value) {
  return typeof value === 'number' && Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : 'n/a';
}

function fmt(value, digits = 2) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : 'n/a';
}

function listAvailableStockDates(config) {
  return listDatasetDates(config.roots.historical, config.datasets.stockBars);
}

function calendarCoverage({ config, startDate, endDate, availableDates }) {
  let openDays = [];
  try {
    openDays = openCalendarDays(config.roots.calendar, startDate, endDate);
  } catch {
    openDays = [];
  }
  const availableSet = new Set(availableDates);
  const cacheMin = availableDates[0] || null;
  const cacheMax = availableDates[availableDates.length - 1] || null;
  const unattempted = openDays.filter((day) => (cacheMin && day < cacheMin) || (cacheMax && day > cacheMax));
  const attemptedMissing = openDays.filter((day) => {
    if (!cacheMin || !cacheMax || day < cacheMin || day > cacheMax) return false;
    return !availableSet.has(day);
  });
  return {
    openDayCount: openDays.length,
    cacheMin,
    cacheMax,
    unattemptedCount: unattempted.length,
    unattempted,
    attemptedMissingCount: attemptedMissing.length,
    attemptedMissing,
  };
}

function calendarByDate(config) {
  try {
    return new Map(loadCalendar(config.roots.calendar).map((day) => [day.date, day]));
  } catch {
    return new Map();
  }
}

function isExpectedShortSession(dayInfo, rowCount) {
  if (!dayInfo || !dayInfo.isEarlyClose) return false;
  const expected = Number(dayInfo.expectedEquitiesMinutes);
  if (!Number.isFinite(expected)) return rowCount >= 180;
  return rowCount >= expected - 2;
}

async function readSpyRowsForDay({ config, dayIso, symbol }) {
  const filePath = datasetCsvPath(config, 'stockBars', dayIso);
  if (!fs.existsSync(filePath)) return { rows: [], missingFile: true };
  const rows = [];
  const stream = fs.createReadStream(filePath).pipe(zlib.createGunzip());
  const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let headers = null;
  let symbolPrefix = `${symbol},`;
  for await (const line of reader) {
    if (!headers) {
      headers = line.split(',');
      symbolPrefix = `${symbol},`;
      continue;
    }
    if (!line.startsWith(symbolPrefix)) continue;
    const values = line.split(',');
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });
    const minuteMs = nsToMinuteMs(row.window_start);
    if (!Number.isFinite(minuteMs) || !isRegularSessionMinute(minuteMs, config.session)) continue;
    const et = getEtParts(minuteMs);
    const open = toNumber(row.open);
    const high = toNumber(row.high);
    const low = toNumber(row.low);
    const close = toNumber(row.close);
    const volume = toNumber(row.volume) || 0;
    if (![open, high, low, close].every((value) => Number.isFinite(value))) continue;
    rows.push({
      date: dayIso,
      minuteMs,
      minuteUtc: new Date(minuteMs).toISOString(),
      minuteOfDayEt: et.minuteOfDayEt,
      minFromOpen: et.minuteOfDayEt - config.session.regularOpenMinuteEt + 1,
      open,
      high,
      low,
      close,
      volume,
      transactions: toNumber(row.transactions) || 0,
    });
  }
  rows.sort((left, right) => left.minuteMs - right.minuteMs);
  return { rows, missingFile: false };
}

function summariseReturns(records, key = 'ret') {
  const returns = records.map((record) => record[key]).filter((value) => Number.isFinite(value));
  if (!returns.length) {
    return {
      observations: 0,
      totalReturn: null,
      annualizedReturn: null,
      annualizedVolatility: null,
      sharpe: null,
      hitRate: null,
      maxDrawdown: null,
    };
  }
  let equity = 1;
  let peak = 1;
  let maxDrawdown = 0;
  let wins = 0;
  let active = 0;
  for (const value of returns) {
    equity *= 1 + value;
    if (equity > peak) peak = equity;
    maxDrawdown = Math.min(maxDrawdown, equity / peak - 1);
    if (value !== 0) active += 1;
    if (value > 0) wins += 1;
  }
  const avg = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const vol = sampleStd(returns);
  return {
    observations: returns.length,
    totalReturn: equity - 1,
    annualizedReturn: (equity ** (252 / returns.length)) - 1,
    annualizedVolatility: Number.isFinite(vol) ? vol * Math.sqrt(252) : null,
    sharpe: Number.isFinite(vol) && vol > 0 ? (avg / vol) * Math.sqrt(252) : null,
    hitRate: active ? wins / active : null,
    maxDrawdown,
  };
}

function enrichSummary(records) {
  const summary = summariseReturns(records, 'ret');
  const benchmark = summariseReturns(records, 'retSpy');
  const traded = records.filter((record) => Number.isFinite(record.ret));
  const gapTrades = traded.filter((record) => record.gapSignal !== 0).length;
  return {
    ...summary,
    benchmark,
    avgTradesPerDay: traded.length
      ? traded.reduce((sum, record) => sum + record.tradesCount, 0) / traded.length
      : null,
    avgLeverage: traded.length
      ? traded.reduce((sum, record) => sum + record.leverage, 0) / traded.length
      : null,
    gapTradeDays: gapTrades,
    skippedDays: records.filter((record) => record.skippedReason).length,
  };
}

function groupBy(records, keyFn) {
  const groups = new Map();
  for (const record of records) {
    const key = keyFn(record);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(record);
  }
  return [...groups.entries()].map(([key, groupRecords]) => ({
    key,
    summary: enrichSummary(groupRecords),
  }));
}

function officialWindowSummaries(config, records) {
  const windows = [config.windows.train, ...config.windows.tests];
  return windows.map((window) => ({
    name: window.name,
    startDate: window.startDate,
    endDate: window.endDate,
    summary: enrichSummary(records.filter(
      (record) => record.date >= window.startDate && record.date <= window.endDate,
    )),
  }));
}

function compactSummary(summary) {
  return {
    observations: summary.observations,
    totalReturn: round(summary.totalReturn),
    annualizedReturn: round(summary.annualizedReturn),
    annualizedVolatility: round(summary.annualizedVolatility),
    sharpe: round(summary.sharpe),
    hitRate: round(summary.hitRate),
    maxDrawdown: round(summary.maxDrawdown),
    benchmark: summary.benchmark ? {
      observations: summary.benchmark.observations,
      totalReturn: round(summary.benchmark.totalReturn),
      annualizedReturn: round(summary.benchmark.annualizedReturn),
      annualizedVolatility: round(summary.benchmark.annualizedVolatility),
      sharpe: round(summary.benchmark.sharpe),
      hitRate: round(summary.benchmark.hitRate),
      maxDrawdown: round(summary.benchmark.maxDrawdown),
    } : undefined,
    avgTradesPerDay: round(summary.avgTradesPerDay),
    avgLeverage: round(summary.avgLeverage),
    gapTradeDays: summary.gapTradeDays,
    skippedDays: summary.skippedDays,
  };
}

function markdownTable(rows) {
  if (!rows.length) return '';
  const headers = ['Window', 'Obs', 'Total', 'Ann.', 'Vol', 'Sharpe', 'Max DD', 'SPY Total', 'Gap Days', 'Trades/Day'];
  const body = rows.map((row) => {
    const summary = row.summary;
    return [
      row.key || row.name,
      summary.observations,
      pct(summary.totalReturn),
      pct(summary.annualizedReturn),
      pct(summary.annualizedVolatility),
      fmt(summary.sharpe),
      pct(summary.maxDrawdown),
      pct(summary.benchmark.totalReturn),
      summary.gapTradeDays,
      fmt(summary.avgTradesPerDay),
    ];
  });
  return [
    `| ${headers.join(' | ')} |`,
    `| ${headers.map(() => '---').join(' | ')} |`,
    ...body.map((row) => `| ${row.join(' | ')} |`),
  ].join('\n');
}

function buildMarkdown(report) {
  const overall = report.results.overall;
  const officialRows = report.results.officialWindows.map((item) => ({
    key: item.name,
    summary: item.summary,
  }));
  return [
    '# Concretum SPY Intraday Momentum + Overnight Gap Reversal',
    '',
    `Generated: ${report.generatedAt}`,
    `Data provider: ${report.provider} flat files`,
    `Requested window: ${report.startDate} to ${report.endDate}`,
    `Processed warmup start: ${report.processStartDate}`,
    '',
    '## Sources',
    ...report.sources.map((source) => `- ${source.name}: ${source.url}`),
    '',
    '## Coverage',
    `- Cache span: ${report.coverage.cacheMin || 'n/a'} to ${report.coverage.cacheMax || 'n/a'}`,
    `- Open calendar days in requested window: ${report.coverage.openDayCount}`,
    `- Unattempted dates/files: ${report.coverage.unattemptedCount}`,
    `- Attempted missing dates/files: ${report.coverage.attemptedMissingCount}`,
    `- Provider sparse dates/files: ${report.coverage.providerSparse.length}`,
    `- Report dates with SPY rows: ${report.coverage.reportDateCount}`,
    '',
    '## Parameters',
    `- band_mult=${report.params.bandMult}, trade_freq=${report.params.tradeFreq}, target_vol=${pct(report.params.targetVol)}, max_leverage=${report.params.maxLeverage}`,
    `- sigma lookback=${report.params.sigmaLookbackDays} trading days, daily vol lookback=${report.params.dailyVolLookbackDays} prior close-to-close returns`,
    `- overnight threshold=${pct(report.overnightThreshold)}, commission=$${report.params.commissionPerShare}/share, min commission=$${report.params.minCommissionPerOrder}/order`,
    '',
    '## Overall',
    markdownTable([{ key: 'overall', summary: overall }]),
    '',
    '## Official Protocol Windows',
    markdownTable(officialRows),
    '',
    '## Monthly Results',
    markdownTable(report.results.monthly),
    '',
    '## Assumptions',
    '- Uses Massive 1-minute regular-session SPY bars only.',
    '- Uses previous regular-session close for overnight gap and band anchors.',
    '- Does not apply the dividend adjustment shown in the Concretum sample code because no local dividend feed is part of this Massive cache.',
    '- Uses the Concretum article commission model by default and no additional slippage.',
    '- Strategy returns are research backtests, not investment advice or a recommendation to trade.',
    '',
  ].join('\n');
}

async function runBacktest({ config, args }) {
  const symbol = args.symbol || config.target || 'SPY';
  const availableDates = listAvailableStockDates(config);
  const cacheMin = availableDates[0];
  const cacheMax = availableDates[availableDates.length - 1];
  const startDate = args['start-date'] || config.windows.sensitivityTrain.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  const processStartDate = args['warmup-start-date'] || cacheMin || startDate;
  const overnightThreshold = asNumber(args['overnight-threshold'], 0.02);
  const params = {
    ...DEFAULT_CONCRETUM_PARAMS,
    initialAum: asNumber(args['initial-aum'], DEFAULT_CONCRETUM_PARAMS.initialAum),
    commissionPerShare: asNumber(args['commission-per-share'], DEFAULT_CONCRETUM_PARAMS.commissionPerShare),
    minCommissionPerOrder: asNumber(args['min-commission'], DEFAULT_CONCRETUM_PARAMS.minCommissionPerOrder),
    bandMult: asNumber(args['band-mult'], DEFAULT_CONCRETUM_PARAMS.bandMult),
    tradeFreq: asNumber(args['trade-freq'], DEFAULT_CONCRETUM_PARAMS.tradeFreq),
    targetVol: asNumber(args['target-vol'], DEFAULT_CONCRETUM_PARAMS.targetVol),
    maxLeverage: asNumber(args['max-leverage'], DEFAULT_CONCRETUM_PARAMS.maxLeverage),
    sigmaLookbackDays: asNumber(args['sigma-lookback-days'], DEFAULT_CONCRETUM_PARAMS.sigmaLookbackDays),
    dailyVolLookbackDays: asNumber(args['daily-vol-lookback-days'], DEFAULT_CONCRETUM_PARAMS.dailyVolLookbackDays),
  };

  const processDates = availableDates.filter((day) => day >= processStartDate && day <= endDate);
  const reportDates = availableDates.filter((day) => day >= startDate && day <= endDate);
  const tradingCalendarByDate = calendarByDate(config);
  const coverage = {
    ...calendarCoverage({ config, startDate, endDate, availableDates }),
    processDateCount: processDates.length,
    reportDateCount: reportDates.length,
    providerSparse: [],
    missingFiles: [],
  };

  const moveOpenHistoryByMinute = new Map();
  const dailyReturns = [];
  const allRecords = [];
  let previousClose = null;
  let previousAum = params.initialAum;

  for (let index = 0; index < processDates.length; index += 1) {
    const dayIso = processDates[index];
    const { rows, missingFile } = await readSpyRowsForDay({ config, dayIso, symbol });
    if (missingFile) {
      coverage.missingFiles.push(dayIso);
      continue;
    }
    if (rows.length < 300 && !isExpectedShortSession(tradingCalendarByDate.get(dayIso), rows.length)) {
      coverage.providerSparse.push({ date: dayIso, rows: rows.length });
    }

    const dailyVol = dailyReturns.length >= params.dailyVolLookbackDays
      ? sampleStd(dailyReturns.slice(-params.dailyVolLookbackDays))
      : null;
    const sigmaByMinute = buildSigmaByMinute(moveOpenHistoryByMinute, params.sigmaLookbackDays);
    const record = simulateConcretumDay({
      date: dayIso,
      rows,
      previousClose,
      previousAum,
      dailyVol,
      sigmaByMinute,
      overnightThreshold,
      params,
    });
    if (Number.isFinite(record.ret)) previousAum = record.aum;
    if (dayIso >= startDate && dayIso <= endDate) allRecords.push(record);

    if (rows.length && Number.isFinite(previousClose) && previousClose > 0) {
      dailyReturns.push(rows[rows.length - 1].close / previousClose - 1);
    }
    if (rows.length) {
      updateMoveOpenHistory(moveOpenHistoryByMinute, rows, params.sigmaLookbackDays * 6);
      previousClose = rows[rows.length - 1].close;
    }

    if ((index + 1) % 25 === 0 || index === processDates.length - 1) {
      console.error(`[concretum] processed ${index + 1}/${processDates.length} dates through ${dayIso}`);
    }
  }

  const overall = enrichSummary(allRecords);
  const monthly = groupBy(allRecords, (record) => record.date.slice(0, 7));
  const annual = groupBy(allRecords, (record) => record.date.slice(0, 4));
  return {
    generatedAt: new Date().toISOString(),
    provider: config.dataPolicy.provider,
    project: config.projectName,
    symbol,
    sources: SOURCE_LINKS,
    claim: {
      sample: '2018-2024 per Concretum article',
      reportedAnnualizedReturn: 0.313,
      reportedSharpe: 1.95,
      reportedMaxDrawdown: -0.10,
      localReplicationNote: cacheMin && cacheMin > '2018-01-02'
        ? `Local Massive stock_quotes_1m cache starts at ${cacheMin}, so this run cannot reproduce the full 2018-2024 claim window.`
        : null,
    },
    startDate,
    endDate,
    processStartDate,
    overnightThreshold,
    params,
    coverage,
    assumptions: {
      dividendAdjustment: false,
      regularSessionOnly: true,
      gapUsesPreviousRegularClose: true,
      costs: 'Concretum article commission model only; no extra slippage by default.',
    },
    results: {
      overall,
      annual,
      monthly,
      officialWindows: officialWindowSummaries(config, allRecords),
    },
    compactResults: {
      overall: compactSummary(overall),
      annual: annual.map((item) => ({ key: item.key, summary: compactSummary(item.summary) })),
      monthly: monthly.map((item) => ({ key: item.key, summary: compactSummary(item.summary) })),
      officialWindows: officialWindowSummaries(config, allRecords)
        .map((item) => ({ ...item, summary: compactSummary(item.summary) })),
    },
    dailyRecords: allRecords,
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args['start-date'] || config.windows.sensitivityTrain.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  const basePath = path.resolve(
    args.output || path.join(PROJECT_ROOT, 'artifacts', `concretum-spy-intraday-${startDate}-${endDate}`),
  );
  const jsonPath = basePath.endsWith('.json') ? basePath : `${basePath}.json`;
  const markdownPath = jsonPath.replace(/\.json$/, '.md');
  const dailyPath = path.resolve(
    args['daily-output'] || path.join(PROJECT_ROOT, 'runtime', `concretum-spy-intraday-${startDate}-${endDate}-daily.jsonl`),
  );

  const report = await runBacktest({ config, args });
  ensureDir(path.dirname(jsonPath));
  ensureDir(path.dirname(markdownPath));
  ensureDir(path.dirname(dailyPath));
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  fs.writeFileSync(markdownPath, buildMarkdown(report), 'utf8');
  fs.writeFileSync(
    dailyPath,
    `${report.dailyRecords.map((record) => JSON.stringify(record)).join('\n')}\n`,
    'utf8',
  );

  console.log(JSON.stringify({
    jsonPath,
    markdownPath,
    dailyPath,
    compactResults: report.compactResults,
    coverage: report.coverage,
    claim: report.claim,
  }, null, 2));
  if (report.coverage.attemptedMissingCount > 0 || report.coverage.missingFiles.length > 0) {
    process.exitCode = 2;
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = {
  runBacktest,
  summariseReturns,
  enrichSummary,
  buildMarkdown,
};
