#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_ML_REPORTS = Object.freeze([
  'projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-expanded-features-2025-02-01-2026-05-08.json',
  'projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-micro-features-2025-02-01-2026-05-08.json',
  'projects/pym-v5-ml-experiments/artifacts/pym-v5-daily-walkforward-two-speed-light-2025-02-01-2026-05-08.json',
]);

const DEFAULT_EXISTING_STRATEGIES = Object.freeze([
  'two_speed_attention_pym_light_governed',
  'topk_attention_pym',
  'topk_attention_micro_pym_a100',
  'pym_v5_base',
  'two_speed_attention_micro_pym_light_governed_a100',
  'topk_attention_liquidity_pym',
  'two_speed_attention_liquidity_pym_light_governed',
]);

const DEFAULT_OPTION_REPORT = 'projects/pym-v5-replication/artifacts/pym-v5-option-overlay-suite-grid-top8-zm0p5-2025-01-02-2026-05-08.json';
const DEFAULT_OPTION_STRATEGY = 'grid_pym_option_rank_top8_zm0p5';
const DEFAULT_OUTPUT = 'projects/pym-v5-ml-experiments/artifacts/pym-v5-combined-option-top8-existing-7-2025-02-01-2026-05-08.json';
const DEFAULT_INITIAL_CAPITAL = 10000;

function parseArgs(argv) {
  const args = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--ml-reports') args.mlReports = argv[++index].split(',').map((item) => item.trim()).filter(Boolean);
    else if (arg === '--strategies') args.strategyIds = argv[++index].split(',').map((item) => item.trim()).filter(Boolean);
    else if (arg === '--option-report') args.optionReport = argv[++index];
    else if (arg === '--option-strategy') args.optionStrategy = argv[++index];
    else if (arg === '--out') args.out = argv[++index];
    else if (arg === '--cost-bps') args.costBps = Number(argv[++index]);
    else if (arg === '--initial-capital') args.initialCapital = Number(argv[++index]);
  }
  return args;
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function finite(value, fallback = 0) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
}

function cleanHoldings(holdings) {
  const out = {};
  Object.entries(holdings || {}).forEach(([ticker, weight]) => {
    const value = finite(weight);
    if (value > 1e-10) out[ticker] = value;
  });
  return out;
}

function addHoldings(target, source, scale = 1) {
  Object.entries(source || {}).forEach(([ticker, weight]) => {
    const value = finite(weight) * scale;
    if (Math.abs(value) > 1e-12) target[ticker] = finite(target[ticker]) + value;
  });
  return target;
}

function turnover(previous, current) {
  const keys = new Set([...Object.keys(previous || {}), ...Object.keys(current || {})]);
  let total = 0;
  keys.forEach((ticker) => {
    total += Math.abs(finite(current?.[ticker]) - finite(previous?.[ticker]));
  });
  return total;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function maxDrawdown(points, initialCapital) {
  let peak = initialCapital;
  let drawdown = 0;
  points.forEach((point) => {
    peak = Math.max(peak, point.equity);
    if (peak > 0) drawdown = Math.min(drawdown, point.equity / peak - 1);
  });
  return drawdown;
}

function monthlyReturns(points) {
  const months = new Map();
  points.forEach((point) => {
    const month = point.date.slice(0, 7);
    if (!months.has(month)) months.set(month, { startEquity: point.startEquity, endEquity: point.equity });
    months.get(month).endEquity = point.equity;
  });
  return [...months.entries()].map(([month, values]) => {
    const ret = values.startEquity > 0 ? values.endEquity / values.startEquity - 1 : 0;
    return { month, return: ret, returnPct: pct(ret) };
  });
}

function summarize(id, family, points, initialCapital, extra = {}) {
  const returns = points.map((point) => point.netReturn);
  const finalEquity = points.at(-1)?.equity || initialCapital;
  const totalReturn = finalEquity / initialCapital - 1;
  const volatility = standardDeviation(returns) * Math.sqrt(252);
  const avgDaily = mean(returns);
  const avgTurnover = mean(points.map((point) => point.turnover));
  const cagr = points.length && totalReturn > -1 ? ((1 + totalReturn) ** (252 / points.length)) - 1 : 0;
  return {
    id,
    family,
    startDate: points[0]?.signalDate || null,
    endDate: points.at(-1)?.date || null,
    tradingDays: points.length,
    finalEquity,
    totalReturn,
    totalReturnPct: pct(totalReturn),
    cagr,
    cagrPct: pct(cagr),
    maxDrawdown: maxDrawdown(points, initialCapital),
    maxDrawdownPct: pct(maxDrawdown(points, initialCapital)),
    annualizedVolatility: volatility,
    annualizedVolatilityPct: pct(volatility),
    sharpe: volatility > 0 ? (avgDaily * 252) / volatility : 0,
    averageDailyTurnover: avgTurnover,
    averageDailyTurnoverPct: pct(avgTurnover),
    monthlyReturns: monthlyReturns(points),
    ...extra,
  };
}

function loadMlStrategies(reportPaths, strategyIds) {
  const strategies = new Map();
  reportPaths.forEach((reportPath) => {
    const report = readJson(reportPath);
    strategyIds.forEach((id) => {
      if (!strategies.has(id) && report.strategies?.[id]) {
        strategies.set(id, {
          id,
          family: 'existing_pym_ml',
          sourcePath: reportPath,
          rawPoints: report.strategies[id].equityCurve.map((point) => ({
            signalDate: point.signalDate,
            date: point.date,
            grossReturn: finite(point.grossReturn, finite(point.netReturn) + finite(point.costReturn)),
            holdings: cleanHoldings(point.holdings),
          })),
        });
      }
    });
  });
  const missing = strategyIds.filter((id) => !strategies.has(id));
  if (missing.length) throw new Error(`missing_ml_strategies:${missing.join(',')}`);
  return strategies;
}

function loadOptionStrategy(reportPath, strategyId) {
  const report = readJson(reportPath);
  const entry = (report.strategies || []).find((strategy) => strategy.summary?.id === strategyId);
  if (!entry) throw new Error(`missing_option_strategy:${strategyId}`);
  return {
    id: strategyId,
    family: 'option_overlay',
    sourcePath: reportPath,
    rawSummary: entry.summary,
    rawPoints: entry.daySummaries.map((point) => ({
      signalDate: point.signalDate,
      date: point.realizedDate,
      grossReturn: finite(point.grossReturn, finite(point.netReturn) + finite(point.costReturn)),
      holdings: cleanHoldings(point.holdings),
      optionDiagnostics: point.optionDiagnostics,
    })),
  };
}

function dateSet(points) {
  return new Set(points.map((point) => point.date));
}

function intersectDates(series) {
  const sets = series.map((item) => dateSet(item.rawPoints));
  const [first, ...rest] = sets;
  return [...first].filter((date) => rest.every((set) => set.has(date))).sort();
}

function buildSeries(item, commonDates, costBps, initialCapital) {
  const byDate = new Map(item.rawPoints.map((point) => [point.date, point]));
  let equity = initialCapital;
  let previousHoldings = {};
  const points = commonDates.map((date) => {
    const raw = byDate.get(date);
    const holdings = cleanHoldings(raw.holdings);
    const dayTurnover = turnover(previousHoldings, holdings);
    const costReturn = dayTurnover * costBps / 10000;
    const grossReturn = finite(raw.grossReturn);
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= 1 + netReturn;
    previousHoldings = holdings;
    return {
      signalDate: raw.signalDate,
      date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover: dayTurnover,
      holdings,
      optionDiagnostics: raw.optionDiagnostics,
    };
  });
  return {
    id: item.id,
    family: item.family,
    sourcePath: item.sourcePath,
    points,
    summary: summarize(item.id, item.family, points, initialCapital),
  };
}

function recentScore(points, currentIndex, lookback) {
  const startIndex = Number.isFinite(lookback) ? Math.max(0, currentIndex - lookback) : 0;
  const recent = points.slice(startIndex, currentIndex);
  if (!recent.length) return 0;
  let logReturn = 0;
  let equity = 1;
  let peak = 1;
  let drawdown = 0;
  recent.forEach((point) => {
    logReturn += Math.log(Math.max(1e-9, 1 + point.netReturn));
    equity *= 1 + point.netReturn;
    peak = Math.max(peak, equity);
    drawdown = Math.min(drawdown, equity / peak - 1);
  });
  return logReturn + 0.5 * drawdown;
}

function lookbackLabel(lookback) {
  return Number.isFinite(lookback) ? String(lookback) : 'expanding';
}

function buildSelector(id, candidates, candidateIds, commonDates, lookback, costBps, initialCapital, seedId = candidateIds[0]) {
  const byId = Object.fromEntries(candidateIds.map((candidateId) => [candidateId, candidates.get(candidateId)]));
  let equity = initialCapital;
  let previousHoldings = {};
  const selectionCounts = {};
  const selections = [];
  const points = commonDates.map((date, index) => {
    let chosenId = seedId;
    if (index > 0) {
      chosenId = candidateIds
        .map((candidateId) => ({
          candidateId,
          score: recentScore(byId[candidateId].points, index, lookback),
        }))
        .sort((left, right) => right.score - left.score || left.candidateId.localeCompare(right.candidateId))[0].candidateId;
    }
    const chosen = byId[chosenId].points[index];
    const holdings = cleanHoldings(chosen.holdings);
    const dayTurnover = turnover(previousHoldings, holdings);
    const costReturn = dayTurnover * costBps / 10000;
    const grossReturn = finite(chosen.grossReturn);
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= 1 + netReturn;
    previousHoldings = holdings;
    selectionCounts[chosenId] = (selectionCounts[chosenId] || 0) + 1;
    selections.push({ signalDate: chosen.signalDate, date, chosenStrategy: chosenId });
    return {
      signalDate: chosen.signalDate,
      date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover: dayTurnover,
      holdings,
      chosenStrategy: chosenId,
    };
  });
  return {
    id,
    family: 'strategy_selector',
    points,
    summary: summarize(id, 'strategy_selector', points, initialCapital, {
      lookback,
      candidateIds,
      selectionCounts: Object.fromEntries(Object.entries(selectionCounts).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))),
      selections,
    }),
  };
}

function buildLookbackMetaSelector(
  id,
  selectorCandidates,
  selectorIds,
  commonDates,
  metaLookback,
  costBps,
  initialCapital,
  seedId = selectorIds[0],
) {
  const byId = Object.fromEntries(selectorCandidates.map((candidate) => [candidate.id, candidate]));
  let equity = initialCapital;
  let previousHoldings = {};
  const selectionCounts = {};
  const selectedLookbackCounts = {};
  const selections = [];
  const points = commonDates.map((date, index) => {
    let chosenId = seedId;
    if (index > 0) {
      chosenId = selectorIds
        .map((selectorId) => ({
          selectorId,
          score: recentScore(byId[selectorId].points, index, metaLookback),
        }))
        .sort((left, right) => right.score - left.score || left.selectorId.localeCompare(right.selectorId))[0].selectorId;
    }
    const chosen = byId[chosenId].points[index];
    const holdings = cleanHoldings(chosen.holdings);
    const dayTurnover = turnover(previousHoldings, holdings);
    const costReturn = dayTurnover * costBps / 10000;
    const grossReturn = finite(chosen.grossReturn);
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= 1 + netReturn;
    previousHoldings = holdings;
    selectionCounts[chosenId] = (selectionCounts[chosenId] || 0) + 1;
    if (Number.isFinite(byId[chosenId].summary?.lookback)) {
      const chosenLookback = String(byId[chosenId].summary.lookback);
      selectedLookbackCounts[chosenLookback] = (selectedLookbackCounts[chosenLookback] || 0) + 1;
    }
    selections.push({
      signalDate: chosen.signalDate,
      date,
      chosenSelector: chosenId,
      chosenStrategy: chosen.chosenStrategy,
      chosenLookback: byId[chosenId].summary?.lookback,
    });
    return {
      signalDate: chosen.signalDate,
      date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover: dayTurnover,
      holdings,
      chosenSelector: chosenId,
      chosenStrategy: chosen.chosenStrategy,
    };
  });
  return {
    id,
    family: 'walkforward_lookback_selector',
    points,
    summary: summarize(id, 'walkforward_lookback_selector', points, initialCapital, {
      metaLookback,
      candidateSelectorIds: selectorIds,
      selectionCounts: Object.fromEntries(Object.entries(selectionCounts).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))),
      selectedLookbackCounts: Object.fromEntries(Object.entries(selectedLookbackCounts).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]))),
      selections,
    }),
  };
}

function buildBlend(id, left, right, leftWeight, costBps, initialCapital) {
  let equity = initialCapital;
  let previousHoldings = {};
  const rightWeight = 1 - leftWeight;
  const points = left.points.map((leftPoint, index) => {
    const rightPoint = right.points[index];
    const holdings = cleanHoldings(addHoldings(
      addHoldings({}, leftPoint.holdings, leftWeight),
      rightPoint.holdings,
      rightWeight,
    ));
    const grossReturn = (leftWeight * finite(leftPoint.grossReturn)) + (rightWeight * finite(rightPoint.grossReturn));
    const dayTurnover = turnover(previousHoldings, holdings);
    const costReturn = dayTurnover * costBps / 10000;
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= 1 + netReturn;
    previousHoldings = holdings;
    return {
      signalDate: leftPoint.signalDate,
      date: leftPoint.date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover: dayTurnover,
      holdings,
    };
  });
  return {
    id,
    family: 'strategy_blend',
    points,
    summary: summarize(id, 'strategy_blend', points, initialCapital, {
      blend: {
        [left.id]: leftWeight,
        [right.id]: rightWeight,
      },
    }),
  };
}

function compactRanking(summary, baselineReturn) {
  return {
    id: summary.id,
    family: summary.family,
    totalReturnPct: summary.totalReturnPct,
    excessVsPymPct: pct(summary.totalReturn - baselineReturn),
    sharpe: summary.sharpe,
    maxDrawdownPct: summary.maxDrawdownPct,
    averageDailyTurnoverPct: summary.averageDailyTurnoverPct,
    tradingDays: summary.tradingDays,
  };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const mlReports = args.mlReports || DEFAULT_ML_REPORTS;
  const strategyIds = args.strategyIds || DEFAULT_EXISTING_STRATEGIES;
  const optionReport = args.optionReport || DEFAULT_OPTION_REPORT;
  const optionStrategy = args.optionStrategy || DEFAULT_OPTION_STRATEGY;
  const outputPath = args.out || DEFAULT_OUTPUT;
  const costBps = Number.isFinite(args.costBps) ? args.costBps : 2;
  const initialCapital = Number.isFinite(args.initialCapital) ? args.initialCapital : DEFAULT_INITIAL_CAPITAL;

  const mlStrategies = loadMlStrategies(mlReports, strategyIds);
  const option = loadOptionStrategy(optionReport, optionStrategy);
  const rawSeries = [...strategyIds.map((id) => mlStrategies.get(id)), option];
  const commonDates = intersectDates(rawSeries);
  if (!commonDates.length) throw new Error('no_common_dates');

  const independent = new Map(rawSeries.map((item) => {
    const series = buildSeries(item, commonDates, costBps, initialCapital);
    return [series.id, series];
  }));

  const bestExistingId = strategyIds
    .map((id) => independent.get(id).summary)
    .sort((left, right) => right.totalReturn - left.totalReturn)[0].id;
  const optionId = optionStrategy;
  const plusOptionIds = [...strategyIds, optionId];
  const selectorLookbacks = [5, 10, 15, 21, 30, 42, 50, 63, 84, 126];
  const metaLookbacks = [Infinity, 21, 42, 63, 126];
  const blendWeights = [0.9, 0.75, 0.5, 0.25, 0.1];

  const existingSelectors = selectorLookbacks.map((lookback) => buildSelector(
      `daily_best_recent_${lookback}_existing_7`,
      independent,
      strategyIds,
      commonDates,
      lookback,
      costBps,
      initialCapital,
      bestExistingId,
    ));
  const plusOptionSelectors = selectorLookbacks.map((lookback) => buildSelector(
      `daily_best_recent_${lookback}_plus_option_top8`,
      independent,
      plusOptionIds,
      commonDates,
      lookback,
      costBps,
      initialCapital,
      bestExistingId,
    ));
  const twoStrategySelectors = selectorLookbacks.map((lookback) => buildSelector(
      `best_of_two_speed_or_option_recent${lookback}`,
      independent,
      [bestExistingId, optionId],
      commonDates,
      lookback,
      costBps,
      initialCapital,
      bestExistingId,
    ));

  const combined = [
    ...existingSelectors,
    ...plusOptionSelectors,
    ...twoStrategySelectors,
    ...metaLookbacks.map((metaLookback) => buildLookbackMetaSelector(
      `walkforward_lookback_best_of_two_speed_or_option_meta${lookbackLabel(metaLookback)}`,
      twoStrategySelectors,
      twoStrategySelectors.map((selector) => selector.id),
      commonDates,
      metaLookback,
      costBps,
      initialCapital,
      'best_of_two_speed_or_option_recent21',
    )),
    ...metaLookbacks.map((metaLookback) => buildLookbackMetaSelector(
      `walkforward_lookback_plus_option_top8_meta${lookbackLabel(metaLookback)}`,
      plusOptionSelectors,
      plusOptionSelectors.map((selector) => selector.id),
      commonDates,
      metaLookback,
      costBps,
      initialCapital,
      'daily_best_recent_21_plus_option_top8',
    )),
    ...blendWeights.map((leftWeight) => buildBlend(
      `blend_${Math.round(leftWeight * 100)}_two_speed_${Math.round((1 - leftWeight) * 100)}_option_top8`,
      independent.get(bestExistingId),
      independent.get(optionId),
      leftWeight,
      costBps,
      initialCapital,
    )),
  ];

  const allSeries = [...independent.values(), ...combined];
  const pymReturn = independent.get('pym_v5_base')?.summary.totalReturn || 0;
  const rankings = allSeries
    .map((series) => compactRanking(series.summary, pymReturn))
    .sort((left, right) => right.totalReturnPct - left.totalReturnPct);

  const report = {
    generatedAt: new Date().toISOString(),
    source: {
      mlReports,
      optionReport,
      optionStrategy,
      note: 'All series are aligned on common realized dates and turnover costs are recomputed from the common start date.',
    },
    settings: {
      commonStartSignalDate: allSeries[0].points[0]?.signalDate,
      commonStartRealizedDate: commonDates[0],
      commonEndRealizedDate: commonDates.at(-1),
      tradingDays: commonDates.length,
      costBps,
      initialCapital,
      existingStrategyIds: strategyIds,
      optionStrategy,
      selectorLookbacks,
      metaLookbacks: metaLookbacks.map(lookbackLabel),
      blendWeights,
      timing: 'EOD signal date X, close-to-close realized date X+1. Selector choices use only prior realized strategy returns.',
    },
    summaries: Object.fromEntries(allSeries.map((series) => [series.id, series.summary])),
    rankings,
  };

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath,
    commonStartRealizedDate: report.settings.commonStartRealizedDate,
    commonEndRealizedDate: report.settings.commonEndRealizedDate,
    tradingDays: commonDates.length,
    top: rankings.slice(0, 12).map((row) => ({
      id: row.id,
      family: row.family,
      totalReturnPct: Number(row.totalReturnPct.toFixed(2)),
      excessVsPymPct: Number(row.excessVsPymPct.toFixed(2)),
      sharpe: Number(row.sharpe.toFixed(3)),
      maxDrawdownPct: Number(row.maxDrawdownPct.toFixed(2)),
      avgTurnoverPct: Number(row.averageDailyTurnoverPct.toFixed(2)),
    })),
  }, null, 2));
}

if (require.main === module) {
  main();
}
