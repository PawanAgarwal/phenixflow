#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, artifactPath, ensureDir, loadConfig } = require('../src/config');
const { availableDates } = require('../src/calendar');
const { buildDailyContextByDate, buildScalpingBarsForDay } = require('../src/data');
const { simulateSecondPassiveScalp, summarizeSecondScalps } = require('../src/second-passive');

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

function asNumber(value, fallback) {
  if (value === undefined || value === true) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function asBoolean(value, fallback = false) {
  if (value === undefined) return fallback;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'y'].includes(String(value).trim().toLowerCase());
}

function cachePath(dayIso, settings) {
  const dailyTag = settings.includeDailyContext ? 'daily' : 'nodaily';
  return path.join(PROJECT_ROOT, 'runtime', `tsll-second-bars-${dayIso}-${settings.barSeconds}s-${dailyTag}.jsonl`);
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
      cachePath: filePath,
      counts: {
        bars: rows.length,
        cacheHit: true,
      },
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
    settings: result.settings,
    summary: result.summary,
  };
  if (includeTrades) out.trades = result.trades;
  return out;
}

function buildGrid({ costCentsPerSide, noEntryFirstMinutes, noEntryLastMinutes }) {
  const grid = [];
  const common = {
    costCentsPerSide,
    noEntryFirstMinutes,
    noEntryLastMinutes,
    minTradeCount: 1,
    requireMarketOk: true,
    minSpyRet1m: -0.001,
    minQqqRet1m: -0.0012,
    minTslaRet1m: -0.002,
    maxLastBarUpCents: 12,
    cooldownBars: 2,
  };
  [1, 2, 3, 4].forEach((buyBelowCloseCents) => {
    [1, 2, 3, 4].forEach((targetCents) => {
      [3, 5, 8].forEach((stopCents) => {
        [2, 5, 10].forEach((maxHoldBars) => {
          [3, 6, 10].forEach((minRange60sCents) => {
            [-20, -5, 0].forEach((minRet60sCents) => {
              [0, 0.5, 1].forEach((throughCents) => {
                [false, true].forEach((dailyStrict) => {
                  grid.push({
                    ...common,
                    name: `sec_passive_b${buyBelowCloseCents}_t${targetCents}_s${stopCents}_h${maxHoldBars}_r${minRange60sCents}_thr${String(throughCents).replace('.', 'p')}${dailyStrict ? '_daily' : ''}`,
                    buyBelowCloseCents,
                    targetCents,
                    stopCents,
                    maxHoldBars,
                    minRange60sCents,
                    minRet60sCents,
                    throughCents,
                    requireDailyContext: dailyStrict,
                    requireDailyMacroTrend: dailyStrict,
                    maxAbsFromPrevCloseAtr: dailyStrict ? 1.5 : null,
                    maxRangeSoFarAtr: dailyStrict ? 1.2 : null,
                  });
                });
              });
            });
          });
        });
      });
    });
  });
  return grid;
}

function buildFixedCandidate({ costCentsPerSide, noEntryFirstMinutes, noEntryLastMinutes }) {
  return [{
    name: 'sec_passive_b3_t3_s5_h10_r3_thr0_fixed',
    costCentsPerSide,
    buyBelowCloseCents: 3,
    targetCents: 3,
    stopCents: 5,
    maxHoldBars: 10,
    cooldownBars: 2,
    throughCents: 0,
    noEntryFirstMinutes,
    noEntryLastMinutes,
    minTradeCount: 1,
    minRange60sCents: 3,
    minRet60sCents: -20,
    maxLastBarUpCents: 12,
    requireMarketOk: true,
    minSpyRet1m: -0.001,
    minQqqRet1m: -0.0012,
    minTslaRet1m: -0.002,
    requireDailyContext: false,
    requireDailyMacroTrend: false,
    maxAbsFromPrevCloseAtr: null,
    maxRangeSoFarAtr: null,
  }];
}

function rankResults(results, minTrades) {
  return results
    .filter((result) => result.summary.trades >= minTrades)
    .sort((left, right) => {
      if (right.summary.avgNetCents !== left.summary.avgNetCents) return right.summary.avgNetCents - left.summary.avgNetCents;
      return right.summary.netCents - left.summary.netCents;
    });
}

function renderMarkdown(report) {
  const lines = [];
  lines.push(`# TSLL Seconds Passive Limit Scalping`);
  lines.push('');
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Window: ${report.startDate} to ${report.endDate}`);
  lines.push(`Open dates used: ${report.selectedDates.join(', ')}`);
  lines.push(`Bars: ${report.totalBars.toLocaleString()} (${report.barSeconds}s), cost: ${report.costCentsPerSide} cents per side`);
  lines.push('');
  const targetData = report.useRestSeconds
    ? 'Massive REST 1-second TSLL aggregates expanded into 1-second OHLCV bars'
    : 'TSLL tick trades converted into 1-second OHLCV bars';
  lines.push(`This uses ${targetData}. It is a proxy for passive bid-to-ask market making: buy limits are placed below the last completed second close, then exits try for a fixed cent target. Without top-of-book quotes, this cannot model the actual bid, ask, queue position, or maker/taker rebates.`);
  lines.push('');
  lines.push('## Top Results');
  lines.push('');
  lines.push('| Rank | Strategy | Trades | Net c/share | Avg c/trade | Win | PF | DD c | Positive days | Settings |');
  lines.push('| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |');
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
      `\`${JSON.stringify(result.settings)}\``,
    ].join(' | '));
  });
  lines.push('');
  lines.push('## Coverage');
  lines.push('');
  report.dayCounts.forEach((day) => {
    lines.push(`- ${day.dayIso}: bars=${day.counts.bars || 0}, trades=${day.counts.trades || 'cache'}, stockMinuteSymbols=${day.counts.stockMinuteSymbols || 'cache'}, cache=${day.counts.cacheHit ? 'hit' : 'built'}`);
  });
  lines.push('');
  lines.push('## Caveats');
  lines.push('');
  lines.push('- Seconds OHLC can confirm a price traded, not that our passive order had queue priority.');
  lines.push('- When target and stop touch in the same second, the simulator assumes the stop first.');
  lines.push('- A positive one- or two-day result is only a candidate for full quote/long-window validation, not a live trading claim.');
  lines.push('');
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args['start-date'] || args.date || '2026-01-08';
  const endDate = args['end-date'] || startDate;
  const maxDays = asNumber(args['max-days'], null);
  const minTrades = asNumber(args['min-trades'], 20);
  const costCentsPerSide = asNumber(args['cost-cents-per-side'], config.execution?.costCentsPerSide ?? 0.5);
  const includeDailyContext = args['daily-context'] !== false;
  const useRestSeconds = asBoolean(args['rest-seconds'], false);
  const requiredDatasets = useRestSeconds ? ['stockBars'] : ['stockTrades', 'stockBars'];
  const selectedDates = availableDates(config, startDate, endDate, requiredDatasets).slice(0, maxDays || undefined);
  if (!selectedDates.length) throw new Error(`no_available_dates:${startDate}:${endDate}`);
  const settings = {
    barSeconds: 1,
    includeOptions: false,
    includeDailyContext,
    costCentsPerSide,
    useRestSeconds,
    rebuildCache: asBoolean(args['rebuild-cache'], false),
  };
  if (includeDailyContext) {
    const startedAt = Date.now();
    const dailyContext = await buildDailyContextByDate(config, selectedDates, {
      warmupDays: asNumber(args['daily-warmup-days'], config.research?.dailyWarmupDays || 35),
    });
    settings.dailyContextByDate = dailyContext.dailyContextByDate;
    console.error(`[tsll-seconds-mm] daily context dates=${dailyContext.dates.length} elapsed=${((Date.now() - startedAt) / 1000).toFixed(1)}s`);
  }

  const gridOptions = {
    costCentsPerSide,
    noEntryFirstMinutes: asNumber(args['no-entry-first-minutes'], 5),
    noEntryLastMinutes: asNumber(args['no-entry-last-minutes'], 10),
  };
  const fixedCandidate = asBoolean(args['fixed-candidate'], false);
  const grid = fixedCandidate ? buildFixedCandidate(gridOptions) : buildGrid(gridOptions);
  const streamedTrades = fixedCandidate ? [] : null;
  let totalBars = 0;
  const allRows = fixedCandidate ? null : [];
  const dayCounts = [];
  for (const dayIso of selectedDates) {
    const startedAt = Date.now();
    const result = await loadOrBuildDay(config, dayIso, settings);
    totalBars += result.rows.length;
    if (fixedCandidate) {
      const dayResult = simulateSecondPassiveScalp(result.rows, grid[0]);
      streamedTrades.push(...dayResult.trades);
    } else {
      allRows.push(...result.rows);
    }
    dayCounts.push({ dayIso, counts: result.counts, cachePath: result.cachePath });
    console.error(`[tsll-seconds-mm] ${dayIso} bars=${result.rows.length} counts=${JSON.stringify(result.counts)} elapsed=${((Date.now() - startedAt) / 1000).toFixed(1)}s`);
  }

  console.error(`[tsll-seconds-mm] evaluating ${grid.length} variants on ${totalBars} one-second bars`);
  let results;
  let ranked;
  let bestWithTrades;
  if (fixedCandidate) {
    const streamedResult = {
      strategy: grid[0].name,
      settings: grid[0],
      summary: summarizeSecondScalps(streamedTrades, totalBars),
      trades: streamedTrades,
    };
    results = [compactResult(streamedResult, false)];
    ranked = rankResults(results, minTrades);
    bestWithTrades = ranked.length ? compactResult(streamedResult, true) : null;
  } else {
    results = grid.map((strategy) => compactResult(simulateSecondPassiveScalp(allRows, strategy), false));
    ranked = rankResults(results, minTrades);
    bestWithTrades = ranked.length
      ? compactResult(simulateSecondPassiveScalp(allRows, ranked[0].settings), true)
      : null;
  }
  const report = {
    generatedAt: new Date().toISOString(),
    project: config.projectName,
    provider: config.dataPolicy.provider,
    startDate,
    endDate,
    selectedDates,
    barSeconds: 1,
    costCentsPerSide,
    includeDailyContext,
    useRestSeconds,
    minTrades,
    fixedCandidate,
    streamedByDay: fixedCandidate,
    totalBars,
    strategyVariants: grid.length,
    keptResults: ranked.length,
    dayCounts,
    topResults: ranked.slice(0, 50),
    bestWithTrades,
  };
  const outputBase = args.output
    ? path.resolve(args.output).replace(/\.json$/, '')
    : artifactPath(`tsll-seconds-passive-mm-${startDate}-${endDate}-cost${String(costCentsPerSide).replace('.', 'p')}`);
  ensureDir(path.dirname(outputBase));
  const jsonPath = `${outputBase}.json`;
  const mdPath = `${outputBase}.md`;
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`);
  fs.writeFileSync(mdPath, renderMarkdown(report));
  console.log(JSON.stringify({
    jsonPath,
    mdPath,
    selectedDates,
    totalBars,
    strategyVariants: grid.length,
    top: report.topResults.slice(0, 5),
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
