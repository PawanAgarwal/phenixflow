#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { parseArgs } = require('../src/cli');
const {
  PROJECT_ROOT,
  ensureDir,
  loadConfig,
} = require('../src/config');
const {
  loadCalendar,
  listDatasetDates,
  openCalendarDays,
} = require('../src/coverage');
const { readRegularSessionRowsForDay } = require('../src/spy-minute-bars');
const {
  compactSummary,
  round,
  simulateOpeningGapFillDay,
  summarizeGapFillRecords,
} = require('../src/opening-gap-fill');

const SOURCE_LINKS = [
  {
    name: 'edgeful X post on ES gap fills',
    url: 'https://x.com/edgeful/status/2051448720474989055',
    note: 'Reported ES full gap fills around 59% to 64% and 50% fills around 68% to 86% over a recent six-month New York-session sample.',
  },
  {
    name: 'Seth Golden X post on gap-fill caveat',
    url: 'https://x.com/SethCL/status/2053468096069853537',
    note: 'Contrary context: warns against the blanket claim that all gaps fill.',
  },
];

function pct(value) {
  return typeof value === 'number' && Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : 'n/a';
}

function fmt(value, digits = 2) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : 'n/a';
}

function parseNumberList(value, defaults) {
  if (value === undefined) return defaults;
  const parsed = String(value)
    .split(',')
    .map((part) => Number(part.trim()))
    .filter((number) => Number.isFinite(number));
  return parsed.length ? parsed : defaults;
}

function parseHorizonList(value, defaults) {
  if (value === undefined) return defaults;
  const parsed = String(value)
    .split(',')
    .map((part) => part.trim().toLowerCase())
    .filter(Boolean)
    .map((part) => (part === 'eod' ? 'eod' : Number(part)))
    .filter((part) => part === 'eod' || Number.isFinite(part));
  return parsed.length ? parsed : defaults;
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

function horizonLabel(maxHoldMinutes) {
  return maxHoldMinutes === 'eod' ? 'eod' : `${maxHoldMinutes}m`;
}

function fillLabel(fillFraction) {
  if (fillFraction === 0.5) return 'half_fill';
  if (fillFraction === 1) return 'full_fill';
  return `${String(fillFraction).replace('.', 'p')}_fill`;
}

function costModels(config) {
  const defaultCost = Number(config.execution?.transactionCostBps ?? 1);
  const defaultSlippage = Number(config.execution?.slippageBps ?? 1);
  return [
    {
      label: 'no_cost',
      costBpsPerSide: 0,
      slippageBpsPerSide: 0,
    },
    {
      label: `default_${defaultCost}bp_cost_${defaultSlippage}bp_slippage`,
      costBpsPerSide: defaultCost,
      slippageBpsPerSide: defaultSlippage,
    },
    {
      label: 'stress_2bp_cost_2bp_slippage',
      costBpsPerSide: 2,
      slippageBpsPerSide: 2,
    },
    {
      label: 'stress_5bp_cost_5bp_slippage',
      costBpsPerSide: 5,
      slippageBpsPerSide: 5,
    },
  ];
}

function buildVariants({ config, args }) {
  const thresholdsBps = parseNumberList(args['thresholds-bps'], [0, 10, 20, 50, 100]);
  const fillFractions = parseNumberList(args['fill-fractions'], [0.5, 1]);
  const horizons = parseHorizonList(args.horizons, [30, 60, 'eod']);
  const costs = costModels(config);
  const variants = [];
  for (const thresholdBps of thresholdsBps) {
    for (const fillFraction of fillFractions) {
      for (const maxHoldMinutes of horizons) {
        for (const cost of costs) {
          const target = fillLabel(fillFraction);
          const horizon = horizonLabel(maxHoldMinutes);
          variants.push({
            id: `gap_${thresholdBps}bps_${target}_${horizon}_${cost.label}`,
            thresholdBps,
            fillFraction,
            target,
            maxHoldMinutes,
            horizon,
            ...cost,
          });
        }
      }
    }
  }
  return variants;
}

function groupBy(records, keyFn) {
  const groups = new Map();
  for (const record of records) {
    const key = keyFn(record);
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(record);
  }
  return [...groups.entries()]
    .sort(([left], [right]) => String(left).localeCompare(String(right)))
    .map(([key, groupRecords]) => ({
      key,
      summary: summarizeGapFillRecords(groupRecords),
    }));
}

function officialWindowSummaries(config, records) {
  const windows = [config.windows.train, ...config.windows.tests];
  return windows.map((window) => ({
    name: window.name,
    startDate: window.startDate,
    endDate: window.endDate,
    summary: summarizeGapFillRecords(records.filter(
      (record) => record.date >= window.startDate && record.date <= window.endDate,
    )),
  }));
}

function compactWindowSummaries(config, records) {
  return officialWindowSummaries(config, records)
    .map((item) => ({ ...item, summary: compactSummary(item.summary) }));
}

function compactVariantResult(result) {
  return {
    id: result.id,
    thresholdBps: result.thresholdBps,
    target: result.target,
    horizon: result.horizon,
    costLabel: result.costLabel,
    summary: compactSummary(result.summary),
    officialHoldout: compactSummary(result.officialHoldout),
  };
}

function selectTopVariants(results, { costLabel, limit = 12 }) {
  return results
    .filter((result) => result.costLabel === costLabel && result.officialHoldout.tradeDays > 0)
    .sort((left, right) => {
      const returnDiff = (right.officialHoldout.totalReturn ?? -Infinity) - (left.officialHoldout.totalReturn ?? -Infinity);
      if (returnDiff !== 0) return returnDiff;
      return (right.officialHoldout.targetHitRate ?? -Infinity) - (left.officialHoldout.targetHitRate ?? -Infinity);
    })
    .slice(0, limit);
}

function selectTopVariantsByWindow(results, { costLabel, windowName, limit = 12 }) {
  return results
    .map((result) => ({
      result,
      window: result.officialWindows.find((item) => item.name === windowName),
    }))
    .filter((item) => item.result.costLabel === costLabel && item.window?.summary.tradeDays > 0)
    .sort((left, right) => {
      const returnDiff = (right.window.summary.totalReturn ?? -Infinity) - (left.window.summary.totalReturn ?? -Infinity);
      if (returnDiff !== 0) return returnDiff;
      return (right.window.summary.targetHitRate ?? -Infinity) - (left.window.summary.targetHitRate ?? -Infinity);
    })
    .slice(0, limit);
}

function compactTrainRankedResult(item) {
  return {
    ...compactVariantResult(item.result),
    trainWindow: item.window.name,
    trainSummary: compactSummary(item.window.summary),
  };
}

function markdownTable(rows) {
  if (!rows.length) return '';
  const headers = ['Variant', 'Trades', 'Fill Hit', 'Net Total', 'Sharpe', 'Max DD', 'SPY Total', 'Gap Up Hit', 'Gap Down Hit'];
  const body = rows.map((row) => {
    const summary = row.summary || row.officialHoldout;
    return [
      row.name || row.id,
      summary.tradeDays,
      pct(summary.targetHitRate),
      pct(summary.totalReturn),
      fmt(summary.sharpe),
      pct(summary.maxDrawdown),
      pct(summary.benchmark?.totalReturn),
      pct(summary.gapUpHitRate),
      pct(summary.gapDownHitRate),
    ];
  });
  return [
    `| ${headers.join(' | ')} |`,
    `| ${headers.map(() => '---').join(' | ')} |`,
    ...body.map((row) => `| ${row.join(' | ')} |`),
  ].join('\n');
}

function buildMarkdown(report) {
  const trainRows = report.compactResults.topDefaultCostTrain.map((result) => ({
    id: `${result.thresholdBps}bps ${result.target} ${result.horizon}`,
    summary: result.trainSummary,
  }));
  const trainHoldoutRows = report.compactResults.topDefaultCostTrain.map((result) => ({
    id: `${result.thresholdBps}bps ${result.target} ${result.horizon}`,
    summary: result.officialHoldout,
  }));
  const topRows = report.compactResults.topDefaultCostOfficialHoldout.map((result) => ({
    id: `${result.thresholdBps}bps ${result.target} ${result.horizon}`,
    summary: result.officialHoldout,
  }));
  const best = report.results.variantSummaries.find(
    (result) => result.id === report.compactResults.topDefaultCostOfficialHoldout[0]?.id,
  );
  const bestWindowRows = best ? best.officialWindows.map((window) => ({
    id: window.name,
    summary: window.summary,
  })) : [];

  return [
    '# SPY Opening Gap Fill Validation',
    '',
    `Generated: ${report.generatedAt}`,
    `Data provider: ${report.provider} flat files`,
    `Requested window: ${report.startDate} to ${report.endDate}`,
    `Processed seed start: ${report.processStartDate}`,
    '',
    '## Sources',
    ...report.sources.map((source) => `- ${source.name}: ${source.url}`),
    '',
    '## Claim Tested',
    '- Edgeful claimed ES gap-fill hit rates can be useful, especially for 50% fills, but the evidence was not SPY-specific.',
    '- Seth Golden supplied the contrary framing: do not assume all gaps fill.',
    '- This run tests SPY open-entry gap fades to 50% and 100% gap-fill targets over 30m, 60m, and EOD horizons.',
    '',
    '## Coverage',
    `- Cache span: ${report.coverage.cacheMin || 'n/a'} to ${report.coverage.cacheMax || 'n/a'}`,
    `- Open calendar days in requested window: ${report.coverage.openDayCount}`,
    `- Unattempted dates/files: ${report.coverage.unattemptedCount}`,
    `- Attempted missing dates/files: ${report.coverage.attemptedMissingCount}`,
    `- Provider sparse dates/files: ${report.coverage.providerSparse.length}`,
    `- Report dates with SPY rows: ${report.coverage.reportDateCount}`,
    '',
    '## Top Default-Cost Variants Ranked On January Train',
    markdownTable(trainRows),
    '',
    '## Holdout Results For January-Selected Variants',
    markdownTable(trainHoldoutRows),
    '',
    '## Top Default-Cost Official Holdout Variants (Diagnostic)',
    markdownTable(topRows),
    '',
    best ? `## Official Windows For Best Default-Cost Variant: ${best.thresholdBps}bps ${best.target} ${best.horizon}` : '## Official Windows For Best Default-Cost Variant',
    markdownTable(bestWindowRows),
    '',
    '## Parameters',
    `- Thresholds tested: ${report.params.thresholdsBps.join(', ')} bps`,
    `- Fill fractions tested: ${report.params.fillFractions.join(', ')}`,
    `- Horizons tested: ${report.params.horizons.map(horizonLabel).join(', ')}`,
    `- Cost models tested: ${report.params.costModels.map((item) => item.label).join(', ')}`,
    '',
    '## Assumptions',
    '- Uses Massive 1-minute regular-session SPY bars only.',
    '- The overnight gap is measured from the prior regular-session close to the current regular-session open.',
    '- Gap-up trades short SPY; gap-down trades long SPY; entry is the opening bar open.',
    '- If the fill target trades inside the horizon, exit at the target price; otherwise exit at the horizon close.',
    '- Strategy returns are research backtests, not investment advice or a recommendation to trade.',
    '',
  ].join('\n');
}

async function runBacktest({ config, args }) {
  const symbol = args.symbol || config.target || 'SPY';
  const availableDates = listAvailableStockDates(config);
  const cacheMin = availableDates[0];
  const startDate = args['start-date'] || config.windows.sensitivityTrain.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  const seedDate = [...availableDates].reverse().find((day) => day < startDate) || startDate;
  const processStartDate = args['process-start-date'] || seedDate;
  const processDates = availableDates.filter((day) => day >= processStartDate && day <= endDate);
  const reportDates = availableDates.filter((day) => day >= startDate && day <= endDate);
  const variants = buildVariants({ config, args });
  const recordsByVariant = new Map(variants.map((variant) => [variant.id, []]));
  const dailyRecords = [];
  const tradingCalendarByDate = calendarByDate(config);
  const coverage = {
    ...calendarCoverage({ config, startDate, endDate, availableDates }),
    processDateCount: processDates.length,
    reportDateCount: 0,
    providerSparse: [],
    missingFiles: [],
  };

  let previousClose = null;
  for (let index = 0; index < processDates.length; index += 1) {
    const dayIso = processDates[index];
    const { rows, missingFile } = await readRegularSessionRowsForDay({ config, dayIso, symbol });
    if (missingFile) {
      coverage.missingFiles.push(dayIso);
      continue;
    }
    if (rows.length < 300 && !isExpectedShortSession(tradingCalendarByDate.get(dayIso), rows.length)) {
      coverage.providerSparse.push({ date: dayIso, rows: rows.length });
    }

    if (dayIso >= startDate && dayIso <= endDate) {
      if (rows.length) coverage.reportDateCount += 1;
      for (const variant of variants) {
        const record = simulateOpeningGapFillDay({
          date: dayIso,
          rows,
          previousClose,
          thresholdBps: variant.thresholdBps,
          fillFraction: variant.fillFraction,
          maxHoldMinutes: variant.maxHoldMinutes,
          costBpsPerSide: variant.costBpsPerSide,
          slippageBpsPerSide: variant.slippageBpsPerSide,
        });
        const enriched = {
          variantId: variant.id,
          thresholdBps: variant.thresholdBps,
          target: variant.target,
          fillFraction: variant.fillFraction,
          horizon: variant.horizon,
          costLabel: variant.label,
          costBpsPerSide: variant.costBpsPerSide,
          slippageBpsPerSide: variant.slippageBpsPerSide,
          ...record,
          gapBps: round(record.gapBps),
          gapReturn: round(record.gapReturn),
          previousClose: round(record.previousClose),
          entryPrice: round(record.entryPrice),
          exitPrice: round(record.exitPrice),
          targetPrice: round(record.targetPrice),
          grossReturn: round(record.grossReturn),
          netReturn: round(record.netReturn),
          spyOpenToExitReturn: round(record.spyOpenToExitReturn),
          spyCloseToCloseReturn: round(record.spyCloseToCloseReturn),
        };
        recordsByVariant.get(variant.id).push(enriched);
        dailyRecords.push(enriched);
      }
    }

    if (rows.length) previousClose = rows[rows.length - 1].close;

    if ((index + 1) % 25 === 0 || index === processDates.length - 1) {
      console.error(`[gap-fill] processed ${index + 1}/${processDates.length} dates through ${dayIso}`);
    }
  }

  const testStartDate = config.windows.tests[0].startDate;
  const testEndDate = config.windows.tests[config.windows.tests.length - 1].endDate;
  const variantSummaries = variants.map((variant) => {
    const records = recordsByVariant.get(variant.id) || [];
    const officialHoldoutRecords = records.filter(
      (record) => record.date >= testStartDate && record.date <= testEndDate,
    );
    return {
      id: variant.id,
      thresholdBps: variant.thresholdBps,
      target: variant.target,
      fillFraction: variant.fillFraction,
      horizon: variant.horizon,
      maxHoldMinutes: variant.maxHoldMinutes,
      costLabel: variant.label,
      costBpsPerSide: variant.costBpsPerSide,
      slippageBpsPerSide: variant.slippageBpsPerSide,
      summary: summarizeGapFillRecords(records),
      officialHoldout: summarizeGapFillRecords(officialHoldoutRecords),
      officialWindows: officialWindowSummaries(config, records),
      monthly: groupBy(records, (record) => record.date.slice(0, 7)),
    };
  });

  const defaultCostLabel = costModels(config)[1].label;
  const topDefaultCostOfficialHoldout = selectTopVariants(variantSummaries, {
    costLabel: defaultCostLabel,
    limit: 12,
  });
  const topDefaultCostTrain = selectTopVariantsByWindow(variantSummaries, {
    costLabel: defaultCostLabel,
    windowName: config.windows.train.name,
    limit: 12,
  });

  return {
    generatedAt: new Date().toISOString(),
    provider: config.dataPolicy.provider,
    project: config.projectName,
    symbol,
    sources: SOURCE_LINKS,
    claim: {
      assetInSource: 'ES futures',
      assetTestedHere: symbol,
      sourceSample: 'Recent six-month New York-session sample per edgeful post.',
      validationNote: 'This validates the opening-gap idea on SPY Massive 1-minute bars and should not be read as a direct ES replication.',
    },
    startDate,
    endDate,
    processStartDate,
    params: {
      thresholdsBps: parseNumberList(args['thresholds-bps'], [0, 10, 20, 50, 100]),
      fillFractions: parseNumberList(args['fill-fractions'], [0.5, 1]),
      horizons: parseHorizonList(args.horizons, [30, 60, 'eod']),
      costModels: costModels(config),
      entryDelayMinutes: 0,
    },
    coverage,
    results: {
      variantSummaries,
    },
    compactResults: {
      defaultCostLabel,
      topDefaultCostTrain: topDefaultCostTrain.map(compactTrainRankedResult),
      topDefaultCostOfficialHoldout: topDefaultCostOfficialHoldout.map(compactVariantResult),
      allVariants: variantSummaries.map(compactVariantResult),
      officialWindowsForTopDefaultCost: topDefaultCostOfficialHoldout[0]
        ? compactWindowSummaries(config, recordsByVariant.get(topDefaultCostOfficialHoldout[0].id) || [])
        : [],
    },
    dailyRecords,
    cacheMin,
    reportDates,
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args['start-date'] || config.windows.sensitivityTrain.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  const basePath = path.resolve(
    args.output || path.join(PROJECT_ROOT, 'artifacts', `opening-gap-fill-${startDate}-${endDate}`),
  );
  const jsonPath = basePath.endsWith('.json') ? basePath : `${basePath}.json`;
  const markdownPath = jsonPath.replace(/\.json$/, '.md');
  const dailyPath = path.resolve(
    args['daily-output'] || path.join(PROJECT_ROOT, 'runtime', `opening-gap-fill-${startDate}-${endDate}-daily.jsonl`),
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
    compactResults: {
      defaultCostLabel: report.compactResults.defaultCostLabel,
      topDefaultCostTrain: report.compactResults.topDefaultCostTrain,
      topDefaultCostOfficialHoldout: report.compactResults.topDefaultCostOfficialHoldout,
      officialWindowsForTopDefaultCost: report.compactResults.officialWindowsForTopDefaultCost,
      allVariantCount: report.compactResults.allVariants.length,
    },
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
  buildMarkdown,
  runBacktest,
};
