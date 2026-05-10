#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { availableDates } = require('../src/calendar');
const { buildDailyContextByDate, buildScalpingBarsForDay } = require('../src/data');
const { buildStrategyGrid } = require('../src/strategies');
const { simulateLongScalp, rankResults } = require('../src/backtest');

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    if (key.startsWith('no-')) {
      out[key.slice(3)] = false;
      continue;
    }
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    index += 1;
  }
  return out;
}

function asNumber(value, defaultValue) {
  if (value === undefined) return defaultValue;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : defaultValue;
}

function asBoolean(value, defaultValue = false) {
  if (value === undefined) return defaultValue;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'y'].includes(String(value).trim().toLowerCase());
}

function cachePath(dayIso, settings) {
  const optionTag = settings.includeOptions ? 'opt' : 'noopt';
  const dailyTag = settings.includeDailyContext ? 'daily' : 'nodaily';
  return path.join(PROJECT_ROOT, 'runtime', `tsll-scalping-bars-${dayIso}-${settings.barSeconds}s-${dailyTag}-${optionTag}.jsonl`);
}

function writeJsonl(filePath, rows) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, rows.map((row) => JSON.stringify(row)).join('\n') + '\n');
}

function readJsonl(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

async function loadOrBuildDay(config, dayIso, settings) {
  const filePath = cachePath(dayIso, settings);
  if (!settings.rebuildCache && fs.existsSync(filePath)) {
    const rows = readJsonl(filePath);
    return {
      dayIso,
      rows,
      counts: {
        bars: rows.length,
        cacheHit: true,
        includeOptions: settings.includeOptions,
      },
      cachePath: filePath,
    };
  }
  const result = await buildScalpingBarsForDay(config, dayIso, settings);
  writeJsonl(filePath, result.rows);
  return {
    ...result,
    cachePath: filePath,
  };
}

function compactResult(result, includeTrades = false) {
  const out = {
    strategy: result.strategy,
    name: result.strategy,
    params: result.params,
    execution: result.execution,
    summary: result.summary,
  };
  if (includeTrades) out.trades = result.trades;
  return out;
}

function renderMarkdown(report) {
  const lines = [];
  lines.push(`# TSLL Scalping Canary`);
  lines.push('');
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Window: ${report.startDate} to ${report.endDate}`);
  lines.push(`Open dates used: ${report.selectedDates.join(', ')}`);
  lines.push(`Bars: ${report.totalBars.toLocaleString()} (${report.barSeconds}s), cost: ${report.costCentsPerSide} cents per side`);
  lines.push(`Options: ${report.includeOptions ? 'enabled' : 'disabled'}, daily context: ${report.includeDailyContext ? 'enabled' : 'disabled'}`);
  lines.push('');
  lines.push(`This is a research backtest on Massive local flat files only. Entry uses the next 5-second bar open after a completed signal bar. If target and stop both touch in one bar, the stop is assumed first. The default grid excludes option data and uses prior-day daily-chart context.`);
  lines.push('');
  lines.push(`## Top Results`);
  lines.push('');
  lines.push('| Rank | Strategy | Trades | Net c/share | Avg c/trade | Win | PF | DD c | Positive days | Params | Execution |');
  lines.push('| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |');
  report.topResults.slice(0, 20).forEach((result, index) => {
    const s = result.summary;
    lines.push([
      index + 1,
      result.strategy,
      s.trades,
      s.netCents,
      s.avgNetCents,
      `${(s.winRate * 100).toFixed(1)}%`,
      s.profitFactor ?? 'inf',
      s.maxDrawdownCents,
      `${s.positiveDays}/${s.tradedDays}`,
      `\`${JSON.stringify(result.params)}\``,
      `\`${JSON.stringify(result.execution)}\``,
    ].join(' | '));
  });
  lines.push('');
  lines.push(`## Coverage`);
  lines.push('');
  report.dayCounts.forEach((day) => {
    lines.push(`- ${day.dayIso}: bars=${day.counts.bars || 0}, trades=${day.counts.trades || 'cache'}, optionMinutes=${day.counts.optionMinutes || 0}, options=${day.counts.includeOptions ? 'yes' : 'no'}, cache=${day.counts.cacheHit ? 'hit' : 'built'}`);
  });
  lines.push('');
  lines.push(`## Scale-Up Gate`);
  lines.push('');
  lines.push(`Before running January 2025 onward, require enough trades, positive average net cents after costs, positive day breadth, and tolerable drawdown. The canary should be treated as hypothesis generation, not a live-trading claim.`);
  lines.push('');
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args['start-date'] || '2026-01-02';
  const endDate = args['end-date'] || '2026-01-09';
  const maxDays = asNumber(args['max-days'], null);
  const barSeconds = asNumber(args['bar-seconds'], config.execution.barSeconds || 5);
  const minTrades = asNumber(args['min-trades'], 20);
  const costCentsPerSide = asNumber(args['cost-cents-per-side'], config.execution.costCentsPerSide ?? 0.5);
  const includeOptions = asBoolean(args['include-options'], config.research?.useOptionFeaturesDefault === true);
  const includeDailyContext = args['daily-context'] !== false;
  const includeBaselineStrategies = asBoolean(args['include-baseline-strategies'], false);
  const selectedDates = availableDates(config, startDate, endDate)
    .slice(0, maxDays || undefined);
  const outputBase = args.output
    ? path.resolve(args.output).replace(/\.json$/, '')
    : path.join(
      PROJECT_ROOT,
      'artifacts',
      `tsll-scalping-canary-${startDate}-${endDate}-${barSeconds}s-${includeDailyContext ? 'daily' : 'nodaily'}-${includeOptions ? 'opt' : 'noopt'}-${includeBaselineStrategies ? 'with-baseline' : 'daily-grid'}-cost${String(costCentsPerSide).replace('.', 'p')}`,
    );
  const settings = {
    barSeconds,
    costCentsPerSide,
    includeOptions,
    includeDailyContext,
    includeBaselineStrategies,
    cooldownBars: asNumber(args['cooldown-bars'], config.execution.cooldownBars ?? 2),
    noEntryFirstMinutes: asNumber(args['no-entry-first-minutes'], config.execution.noEntryFirstMinutes ?? 5),
    noEntryLastMinutes: asNumber(args['no-entry-last-minutes'], config.execution.noEntryLastMinutes ?? 5),
    rebuildCache: Boolean(args['rebuild-cache']),
  };

  if (!selectedDates.length) throw new Error(`no_available_dates:${startDate}:${endDate}`);
  ensureDir(path.join(PROJECT_ROOT, 'runtime'));
  ensureDir(path.join(PROJECT_ROOT, 'artifacts'));
  console.error(`[tsll-scalping] dates=${selectedDates.join(', ')} barSeconds=${barSeconds} costCentsPerSide=${costCentsPerSide} options=${includeOptions ? 'yes' : 'no'} daily=${includeDailyContext ? 'yes' : 'no'}`);
  if (includeDailyContext) {
    const startedAt = Date.now();
    const dailyContext = await buildDailyContextByDate(config, selectedDates, {
      warmupDays: asNumber(args['daily-warmup-days'], config.research?.dailyWarmupDays || 35),
    });
    settings.dailyContextByDate = dailyContext.dailyContextByDate;
    console.error(`[tsll-scalping] daily context dates=${dailyContext.dates.length} symbols=${dailyContext.symbols.join(',')} elapsed=${((Date.now() - startedAt) / 1000).toFixed(1)}s`);
  }

  const dayCounts = [];
  const allRows = [];
  for (const dayIso of selectedDates) {
    const startedAt = Date.now();
    const result = await loadOrBuildDay(config, dayIso, settings);
    allRows.push(...result.rows);
    dayCounts.push({ dayIso, counts: result.counts, cachePath: result.cachePath });
    console.error(`[tsll-scalping] ${dayIso} bars=${result.rows.length} counts=${JSON.stringify(result.counts)} elapsed=${((Date.now() - startedAt) / 1000).toFixed(1)}s`);
  }

  const grid = buildStrategyGrid({
    includeOptions,
    includeBaselineStrategies,
  });
  console.error(`[tsll-scalping] evaluating ${grid.length} strategy variants on ${allRows.length} bars`);
  const results = [];
  const startedAt = Date.now();
  grid.forEach((strategy, index) => {
    const result = simulateLongScalp(allRows, strategy, settings);
    if (result.summary.trades >= minTrades) results.push(compactResult(result, false));
    if ((index + 1) % 1000 === 0) {
      console.error(`[tsll-scalping] evaluated=${index + 1}/${grid.length} kept=${results.length} elapsed=${((Date.now() - startedAt) / 1000).toFixed(1)}s`);
    }
  });

  const ranked = rankResults(results, { minTrades });
  const topResults = ranked.slice(0, 50);
  const bestWithTrades = topResults.length
    ? compactResult(simulateLongScalp(allRows, {
      name: topResults[0].strategy,
      params: topResults[0].params,
      execution: topResults[0].execution,
    }, settings), true)
    : null;
  const report = {
    generatedAt: new Date().toISOString(),
    project: config.projectName,
    provider: config.dataPolicy.provider,
    startDate,
    endDate,
    selectedDates,
    barSeconds,
    costCentsPerSide,
    includeOptions,
    includeDailyContext,
    includeBaselineStrategies,
    minTrades,
    totalBars: allRows.length,
    strategyVariants: grid.length,
    keptResults: results.length,
    dayCounts,
    topResults,
    bestWithTrades,
  };

  const jsonPath = `${outputBase}.json`;
  const mdPath = `${outputBase}.md`;
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(mdPath, renderMarkdown(report));
  console.log(JSON.stringify({
    jsonPath,
    mdPath,
    totalBars: report.totalBars,
    strategyVariants: report.strategyVariants,
    keptResults: report.keptResults,
    best: topResults[0] || null,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
