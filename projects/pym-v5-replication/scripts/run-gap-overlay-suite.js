#!/usr/bin/env node
const fs = require('node:fs');

const { artifactPath, ensureDir, loadConfig, runtimePath } = require('../src/config');
const { resolveEndDate } = require('../src/calendar');
const { runGapOverlaySuite } = require('../src/gap-overlay-suite');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--rsi-mode') out.rsiMode = argv[++index];
    else if (arg === '--cost-bps') out.totalCostBps = Number(argv[++index]);
    else if (arg === '--label') out.label = argv[++index];
  }
  return out;
}

function pct(value) {
  return Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : 'n/a';
}

function fmt(value, digits = 2) {
  return Number.isFinite(value) ? value.toFixed(digits) : 'n/a';
}

function row(summary, windowName = null) {
  if (!windowName) {
    return [
      summary.id,
      pct(summary.totalReturn),
      pct(summary.vsBase?.totalReturnDelta),
      fmt(summary.sharpe),
      pct(summary.maxDrawdown),
      summary.activeDays,
    ];
  }
  const window = summary.windows.find((item) => item.name === windowName);
  return [
    summary.id,
    pct(window?.summary.totalReturn),
    pct(window?.vsBase.totalReturnDelta),
    fmt(window?.summary.sharpe),
    pct(window?.summary.maxDrawdown),
    summary.activeDays,
  ];
}

function markdownTable(rows) {
  const headers = ['Variant', 'Return', 'Delta vs Base', 'Sharpe', 'Max DD', 'Active Days'];
  return [
    `| ${headers.join(' | ')} |`,
    `| ${headers.map(() => '---').join(' | ')} |`,
    ...rows.map((values) => `| ${values.join(' | ')} |`),
  ].join('\n');
}

function buildMarkdown(report) {
  const base = report.baseSummary;
  const topFull = report.summaries
    .filter((summary) => summary.id !== 'pym_base')
    .sort((left, right) => right.vsBase.totalReturnDelta - left.vsBase.totalReturnDelta)
    .slice(0, 10);
  const topHoldout = report.summaries
    .filter((summary) => summary.id !== 'pym_base')
    .sort((left, right) => {
      const leftWindow = left.windows.find((item) => item.name === 'holdout_2026_ytd');
      const rightWindow = right.windows.find((item) => item.name === 'holdout_2026_ytd');
      return (rightWindow?.vsBase.totalReturnDelta ?? -Infinity) - (leftWindow?.vsBase.totalReturnDelta ?? -Infinity);
    })
    .slice(0, 10);
  const trainTop = report.compactResults.topTrainDelta.slice(0, 5);
  const trainTopRows = trainTop.map((item) => {
    const full = report.summaries.find((summary) => summary.id === item.id);
    const holdout = full?.windows.find((window) => window.name === 'holdout_2026_ytd');
    return [
      item.id,
      pct(item.windowSummary.totalReturn),
      pct(item.windowSummary.vsBase.totalReturnDelta),
      pct(holdout?.summary.totalReturn),
      pct(holdout?.vsBase.totalReturnDelta),
    ];
  });

  return [
    '# PYM V5 SPY Opening Gap Overlay Suite',
    '',
    `Generated: ${report.generatedAt}`,
    `Window: ${report.settings.startDate} to ${report.settings.endDate}`,
    `Cost model: ${report.settings.totalCostBps} bps per turnover side/equivalent`,
    `Daily bars: ${report.settings.source.dailyBarsPath}`,
    '',
    '## Base PYM',
    `- Return: ${pct(base.totalReturn)}`,
    `- CAGR: ${pct(base.cagr)}`,
    `- Sharpe: ${fmt(base.sharpe)}`,
    `- Max drawdown: ${pct(base.maxDrawdown)}`,
    '',
    '## Top Full-Window Overlay Deltas',
    markdownTable(topFull.map((summary) => row(summary))),
    '',
    '## Top 2026 Holdout Overlay Deltas',
    markdownTable(topHoldout.map((summary) => row(summary, 'holdout_2026_ytd'))),
    '',
    '## 2025-Selected Variants, Then 2026 Holdout',
    '| Variant | Train Return | Train Delta | Holdout Return | Holdout Delta |',
    '| --- | --- | --- | --- | --- |',
    ...trainTopRows.map((values) => `| ${values.join(' | ')} |`),
    '',
    '## Interpretation Notes',
    '- The overlay preserves the overnight PYM return, then reallocates only the same-day open-to-close sleeve after the SPY gap is known.',
    '- Gap sleeve variants replace part of PYM intraday exposure with a synthetic SPY gap-fade trade.',
    '- Cash throttle variants move part of PYM intraday exposure to cash on qualifying SPY gap days.',
    '- Reported deltas include the base PYM rebalance cost plus a conservative round-trip overlay cost on active overlay days.',
    '',
  ].join('\n');
}

function compactConsole(report) {
  return {
    outputSummary: {
      base: report.compactResults.base,
      topFullWindowDelta: report.compactResults.topFullWindowDelta.slice(0, 5),
      topHoldoutDelta: report.compactResults.topHoldoutDelta.slice(0, 5),
      topTrainDelta: report.compactResults.topTrainDelta.slice(0, 5),
    },
    coverage: report.coverage,
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || config.windows.backtestStartDate;
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const report = runGapOverlaySuite({
    config,
    startDate,
    endDate,
    dailyBarsPath: args.dailyBarsPath,
    scorePath: args.scorePath,
    rsiMode: args.rsiMode || 'wilder',
    totalCostBps: Number.isFinite(args.totalCostBps) ? args.totalCostBps : undefined,
    onProgress: ({ date, processedDays }) => {
      if (processedDays % 50 === 0) console.error(`[gap-overlay] processed ${processedDays} days through ${date}`);
    },
  });
  const labelPart = args.label ? `${args.label}-` : '';
  const jsonPath = artifactPath(`pym-v5-gap-overlay-suite-${labelPart}${startDate}-${endDate}.json`);
  const markdownPath = jsonPath.replace(/\.json$/, '.md');
  const dailyPath = runtimePath(`pym-v5-gap-overlay-suite-${labelPart}${startDate}-${endDate}-daily.jsonl`);
  ensureDir(artifactPath());
  ensureDir(runtimePath());
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(markdownPath, buildMarkdown(report));
  fs.writeFileSync(
    dailyPath,
    `${report.strategies.flatMap((strategy) => strategy.dailyRecords.map((record) => JSON.stringify({
      variantId: strategy.summary.id,
      ...record,
    }))).join('\n')}\n`,
  );
  console.log(JSON.stringify({
    jsonPath,
    markdownPath,
    dailyPath,
    ...compactConsole(report),
  }, null, 2));
}

main();
