#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const {
  queryRowsSync,
} = require('../../src/storage/clickhouse');

const DEFAULT_BACKTEST_REPORT = path.join(process.cwd(), 'artifacts', 'reports', 'flag30-ema8-backtest-latest.json');
const DEFAULT_PRIMARY_EXIT_FIELD = 'closeOrStopR';
const EXIT_FIELDS = ['closeOrStopR', 'hodOrStopR', 'scaleout1r2rBeR'];
const DEFAULT_TARGET_R = 5;
const DEFAULT_MIN_TRADES = 40;
const DEFAULT_TRAIN_FROM = '2025-09-01';
const DEFAULT_TRAIN_TO = '2025-12-31';
const DEFAULT_TEST_FROM = '2026-01-01';
const DEFAULT_TEST_TO = '2026-03-12';

const ROBUST_SYMBOLS = new Set([
  'AAPL', 'AVGO', 'BA', 'COP', 'CRM', 'CVS', 'CVX', 'GE', 'GIS', 'GOOGL',
  'NKE', 'PLTR', 'PM', 'PYPL', 'RF', 'RTX', 'SHOP', 'SLB', 'SOFI', 'SPGI',
]);

const FEATURE_NAMES = [
  'tradeCount',
  'totalValue',
  'callSize',
  'putSize',
  'callPutSizeRatio',
  'netCallFlow',
  'bullishShare',
  'bullBearRatio',
  'sweepTradeShare',
  'valuePerTrade',
  'avgSigScore',
  'netSigScore',
  'sweepValueRatio',
  'multilegPct',
  'flowImbalanceNorm',
  'deltaPressureNorm',
  'trendConfirmNorm',
  'ivSpread',
  'avgIv',
];

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith('--')) {
      out[key] = '1';
      continue;
    }
    out[key] = value;
    i += 1;
  }
  return out;
}

function nowIso() {
  return new Date().toISOString();
}

function parseNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function parseDateIso(value, fallback) {
  const raw = String(value || '').trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  return fallback;
}

function parseBooleanLike(value, fallback = false) {
  if (value === undefined || value === null || value === '') return fallback;
  const raw = String(value).trim().toLowerCase();
  if (!raw) return fallback;
  if (['1', 'true', 'yes', 'on'].includes(raw)) return true;
  if (['0', 'false', 'no', 'off'].includes(raw)) return false;
  return fallback;
}

function toFinite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function avg(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length === 0) return null;
  return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
}

function sum(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length === 0) return null;
  return filtered.reduce((acc, value) => acc + value, 0);
}

function quantile(values, p) {
  const filtered = values
    .filter((value) => Number.isFinite(value))
    .slice()
    .sort((a, b) => a - b);
  if (filtered.length === 0) return null;
  const q = Math.max(0, Math.min(1, p));
  const idx = (filtered.length - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return filtered[lo];
  const weight = idx - lo;
  return filtered[lo] + ((filtered[hi] - filtered[lo]) * weight);
}

function toClickHouseDateTime64(msUtc) {
  return new Date(msUtc).toISOString().replace('T', ' ').replace('Z', '');
}

function normalizeMinuteUtc(rawValue) {
  if (rawValue === null || rawValue === undefined) return null;
  return String(rawValue).trim().replace('T', ' ').replace('Z', '');
}

function escapeSqlString(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function chunkArray(items, chunkSize) {
  const out = [];
  for (let i = 0; i < items.length; i += chunkSize) {
    out.push(items.slice(i, i + chunkSize));
  }
  return out;
}

function inDateRange(dayEt, fromIso, toIso) {
  return dayEt >= fromIso && dayEt <= toIso;
}

function conditionLabel(name, op, threshold) {
  const formatted = Number.isFinite(threshold)
    ? threshold.toFixed(6).replace(/0+$/, '').replace(/\.$/, '')
    : String(threshold);
  return `${name} ${op} ${formatted}`;
}

function conditionPass(row, condition) {
  const value = row[condition.feature];
  if (!Number.isFinite(value)) return false;
  if (condition.op === '>=') return value >= condition.threshold;
  return value <= condition.threshold;
}

function sanitizeCondition(condition) {
  if (!condition) return null;
  return {
    type: condition.type,
    label: condition.label,
    legs: Array.isArray(condition.legs) ? condition.legs : undefined,
    feature: condition.feature,
    op: condition.op,
    threshold: condition.threshold,
    trades: condition.trades,
    hitTargetRate: condition.hitTargetRate,
    avgExitR: condition.avgExitR,
    totalExitR: condition.totalExitR,
    winRate: condition.winRate,
    avgMaxR: condition.avgMaxR,
    avgWinR: condition.avgWinR,
    avgLossR: condition.avgLossR,
    deltaHitTargetVsBaseline: condition.deltaHitTargetVsBaseline,
    deltaAvgExitRVsBaseline: condition.deltaAvgExitRVsBaseline,
  };
}

function computeMetrics(rows, targetR, exitField) {
  const count = rows.length;
  if (count === 0) {
    return {
      trades: 0,
      hitTargetRate: null,
      avgExitR: null,
      totalExitR: null,
      winRate: null,
      avgMaxR: null,
      avgWinR: null,
      avgLossR: null,
    };
  }

  const exitValues = rows
    .map((row) => toFinite(row[exitField]))
    .filter((value) => Number.isFinite(value));
  const maxValues = rows.map((row) => row.maxR).filter((value) => Number.isFinite(value));
  const hitTargetCount = rows.filter((row) => Number.isFinite(row.maxR) && row.maxR >= targetR).length;
  const wins = exitValues.filter((value) => value > 0);
  const losses = exitValues.filter((value) => value < 0);

  return {
    trades: count,
    hitTargetRate: hitTargetCount / count,
    avgExitR: avg(exitValues),
    totalExitR: sum(exitValues),
    winRate: exitValues.length > 0 ? (wins.length / exitValues.length) : null,
    avgMaxR: avg(maxValues),
    avgWinR: avg(wins),
    avgLossR: avg(losses),
  };
}

function computeMetricsByExit(rows, targetR) {
  const out = {};
  EXIT_FIELDS.forEach((exitField) => {
    out[exitField] = computeMetrics(rows, targetR, exitField);
  });
  return out;
}

function normalizeTrades(backtest, targetR, includeAllSymbols) {
  const sourceTrades = Array.isArray(backtest.trades) ? backtest.trades : [];
  const parsed = sourceTrades
    .map((trade) => {
      const entryMs = Date.parse(String(trade.entryTsUtc || '').trim());
      const dayEt = String(trade.dayEt || '').trim();
      const symbol = String(trade.symbol || '').trim().toUpperCase();
      const maxR = toFinite(trade.maxR);
      const closeOrStopR = toFinite(trade.closeOrStopR);
      const hodOrStopR = toFinite(trade.hodOrStopR);
      const scaleout1r2rBeR = toFinite(trade.scaleout1r2rBeR);
      if (!Number.isFinite(entryMs) || !dayEt || !symbol || maxR === null) return null;
      if (closeOrStopR === null || hodOrStopR === null || scaleout1r2rBeR === null) return null;
      return {
        symbol,
        dayEt,
        entryTsUtc: new Date(entryMs).toISOString(),
        entryMsUtc: entryMs,
        priorMinuteUtc: toClickHouseDateTime64(entryMs - (60 * 1000)),
        maxR,
        closeOrStopR,
        hodOrStopR,
        scaleout1r2rBeR,
        hitTarget: maxR >= targetR,
      };
    })
    .filter(Boolean);

  if (includeAllSymbols) return parsed;
  return parsed.filter((trade) => ROBUST_SYMBOLS.has(trade.symbol));
}

function buildOptionFeatureMap(trades) {
  const neededMinutesBySymbol = new Map();
  trades.forEach((trade) => {
    if (!neededMinutesBySymbol.has(trade.symbol)) neededMinutesBySymbol.set(trade.symbol, new Set());
    neededMinutesBySymbol.get(trade.symbol).add(trade.priorMinuteUtc);
  });

  const optionFeatureMap = new Map();
  const selectedColumns = [
    'trade_count',
    'total_value',
    'call_size',
    'put_size',
    'bullish_count',
    'bearish_count',
    'avg_sig_score',
    'net_sig_score',
    'sweep_count',
    'sweep_value_ratio',
    'multileg_pct',
    'avg_flow_imbalance_norm',
    'avg_delta_pressure_norm',
    'avg_underlying_trend_confirm_norm',
    'iv_spread',
    'avg_iv',
  ];

  Array.from(neededMinutesBySymbol.keys()).sort().forEach((symbol) => {
    const minuteList = Array.from(neededMinutesBySymbol.get(symbol)).sort();
    chunkArray(minuteList, 300).forEach((minuteChunk) => {
      const minuteInClause = minuteChunk.map((minute) => `'${escapeSqlString(minute)}'`).join(',');
      const rows = queryRowsSync(`
        SELECT
          symbol,
          toString(minute_bucket_utc) AS minute_utc,
          ${selectedColumns.join(',\n          ')}
        FROM options.option_symbol_minute_derived
        WHERE symbol = '${escapeSqlString(symbol)}'
          AND toString(minute_bucket_utc) IN (${minuteInClause})
      `);
      rows.forEach((row) => {
        const minuteUtc = normalizeMinuteUtc(row.minute_utc);
        if (!minuteUtc) return;
        const key = `${String(row.symbol || '').trim().toUpperCase()}\t${minuteUtc}`;
        optionFeatureMap.set(key, row);
      });
    });
  });

  return optionFeatureMap;
}

function enrichWithOptionFeatures(trades, optionFeatureMap) {
  return trades.map((trade) => {
    const row = optionFeatureMap.get(`${trade.symbol}\t${trade.priorMinuteUtc}`);
    if (!row) return null;

    const tradeCount = toFinite(row.trade_count) || 0;
    const totalValue = toFinite(row.total_value) || 0;
    const callSize = toFinite(row.call_size) || 0;
    const putSize = toFinite(row.put_size) || 0;
    const totalSize = callSize + putSize;
    const bullishCount = toFinite(row.bullish_count) || 0;
    const bearishCount = toFinite(row.bearish_count) || 0;
    const sweepCount = toFinite(row.sweep_count) || 0;

    return {
      ...trade,
      tradeCount,
      totalValue,
      callSize,
      putSize,
      callPutSizeRatio: callSize / Math.max(1, putSize),
      netCallFlow: (callSize - putSize) / Math.max(1, totalSize),
      bullishShare: bullishCount / Math.max(1, tradeCount),
      bullBearRatio: bullishCount / Math.max(1, bearishCount),
      sweepTradeShare: sweepCount / Math.max(1, tradeCount),
      valuePerTrade: totalValue / Math.max(1, tradeCount),
      avgSigScore: toFinite(row.avg_sig_score),
      netSigScore: toFinite(row.net_sig_score),
      sweepValueRatio: toFinite(row.sweep_value_ratio),
      multilegPct: toFinite(row.multileg_pct),
      flowImbalanceNorm: toFinite(row.avg_flow_imbalance_norm),
      deltaPressureNorm: toFinite(row.avg_delta_pressure_norm),
      trendConfirmNorm: toFinite(row.avg_underlying_trend_confirm_norm),
      ivSpread: toFinite(row.iv_spread),
      avgIv: toFinite(row.avg_iv),
    };
  }).filter(Boolean);
}

function buildSingleConditions(trainRows, targetR, primaryExitField, minTrades) {
  const baselinePrimary = computeMetrics(trainRows, targetR, primaryExitField);
  const singleConditions = [];

  FEATURE_NAMES.forEach((feature) => {
    const values = trainRows
      .map((row) => row[feature])
      .filter((value) => Number.isFinite(value));
    if (values.length < minTrades) return;

    const thresholds = {
      q20: quantile(values, 0.2),
      q30: quantile(values, 0.3),
      q40: quantile(values, 0.4),
      q60: quantile(values, 0.6),
      q70: quantile(values, 0.7),
      q80: quantile(values, 0.8),
    };
    const specs = [
      { op: '>=', key: 'q60' },
      { op: '>=', key: 'q70' },
      { op: '>=', key: 'q80' },
      { op: '<=', key: 'q40' },
      { op: '<=', key: 'q30' },
      { op: '<=', key: 'q20' },
    ];

    specs.forEach((spec) => {
      const threshold = thresholds[spec.key];
      if (!Number.isFinite(threshold)) return;
      const condition = {
        type: 'single',
        feature,
        op: spec.op,
        threshold,
        label: conditionLabel(feature, spec.op, threshold),
      };
      const filtered = trainRows.filter((row) => conditionPass(row, condition));
      if (filtered.length < minTrades) return;
      const metrics = computeMetrics(filtered, targetR, primaryExitField);
      singleConditions.push({
        ...condition,
        ...metrics,
        deltaHitTargetVsBaseline: (metrics.hitTargetRate ?? 0) - (baselinePrimary.hitTargetRate ?? 0),
        deltaAvgExitRVsBaseline: (metrics.avgExitR ?? 0) - (baselinePrimary.avgExitR ?? 0),
      });
    });
  });

  return { baselinePrimary, singleConditions };
}

function buildComboConditions(trainRows, targetR, primaryExitField, minTrades, singleConditions) {
  const baselinePrimary = computeMetrics(trainRows, targetR, primaryExitField);
  const topSingles = singleConditions
    .filter((row) => row.deltaHitTargetVsBaseline > 0 || row.deltaAvgExitRVsBaseline > 0)
    .slice(0, 12);

  const combos = [];
  for (let i = 0; i < topSingles.length; i += 1) {
    for (let j = i + 1; j < topSingles.length; j += 1) {
      const c1 = topSingles[i];
      const c2 = topSingles[j];
      const filtered = trainRows.filter((row) => conditionPass(row, c1) && conditionPass(row, c2));
      if (filtered.length < Math.max(20, Math.floor(minTrades * 0.67))) continue;
      const metrics = computeMetrics(filtered, targetR, primaryExitField);
      combos.push({
        type: 'combo',
        label: `${c1.label} AND ${c2.label}`,
        legs: [c1.label, c2.label],
        conditions: [
          { feature: c1.feature, op: c1.op, threshold: c1.threshold, label: c1.label },
          { feature: c2.feature, op: c2.op, threshold: c2.threshold, label: c2.label },
        ],
        ...metrics,
        deltaHitTargetVsBaseline: (metrics.hitTargetRate ?? 0) - (baselinePrimary.hitTargetRate ?? 0),
        deltaAvgExitRVsBaseline: (metrics.avgExitR ?? 0) - (baselinePrimary.avgExitR ?? 0),
      });
    }
  }

  return combos;
}

function sortByExpectancyThenHit(rows) {
  return rows.slice().sort((a, b) => {
    if ((b.avgExitR ?? -999) !== (a.avgExitR ?? -999)) return (b.avgExitR ?? -999) - (a.avgExitR ?? -999);
    if ((b.hitTargetRate ?? -1) !== (a.hitTargetRate ?? -1)) return (b.hitTargetRate ?? -1) - (a.hitTargetRate ?? -1);
    return (b.trades || 0) - (a.trades || 0);
  });
}

function sortByHitThenExpectancy(rows) {
  return rows.slice().sort((a, b) => {
    if ((b.hitTargetRate ?? -1) !== (a.hitTargetRate ?? -1)) return (b.hitTargetRate ?? -1) - (a.hitTargetRate ?? -1);
    if ((b.avgExitR ?? -999) !== (a.avgExitR ?? -999)) return (b.avgExitR ?? -999) - (a.avgExitR ?? -999);
    return (b.trades || 0) - (a.trades || 0);
  });
}

function selectCondition(singleConditions, comboConditions, baselinePrimary, minTrades) {
  const byExp = sortByExpectancyThenHit([...comboConditions, ...singleConditions]);
  const byHit = sortByHitThenExpectancy([...comboConditions, ...singleConditions]);

  const bestForExpectancy = byExp.find((row) => row.trades >= minTrades) || null;
  const bestForHit = byHit.find((row) => row.trades >= minTrades) || null;
  const preferred = byExp.find((row) => row.trades >= minTrades && (row.avgExitR ?? -999) > (baselinePrimary.avgExitR ?? -999))
    || bestForExpectancy;

  return {
    bestForExpectancy,
    bestForHit,
    preferred,
  };
}

function applyCondition(rows, condition) {
  if (!condition) return [];
  if (condition.type === 'single') {
    return rows.filter((row) => conditionPass(row, condition));
  }
  const legs = Array.isArray(condition.conditions) ? condition.conditions : [];
  if (legs.length === 0) return [];
  return rows.filter((row) => legs.every((leg) => conditionPass(row, leg)));
}

function computeDelta(filteredMetrics, baselineMetrics) {
  return {
    deltaTrades: (filteredMetrics.trades ?? 0) - (baselineMetrics.trades ?? 0),
    deltaHitTargetRate: (filteredMetrics.hitTargetRate ?? 0) - (baselineMetrics.hitTargetRate ?? 0),
    deltaAvgExitR: (filteredMetrics.avgExitR ?? 0) - (baselineMetrics.avgExitR ?? 0),
    deltaTotalExitR: (filteredMetrics.totalExitR ?? 0) - (baselineMetrics.totalExitR ?? 0),
  };
}

function run() {
  const args = parseArgs(process.argv);
  const backtestPath = path.resolve(args.backtest || DEFAULT_BACKTEST_REPORT);
  const primaryExitField = String(
    args.primaryExitField || process.env.WF_OPTION_FILTER_PRIMARY_EXIT_FIELD || DEFAULT_PRIMARY_EXIT_FIELD,
  ).trim();
  if (!EXIT_FIELDS.includes(primaryExitField)) {
    throw new Error(`invalid_primary_exit_field:${primaryExitField}`);
  }

  const targetR = Math.max(1, parseNumber(args.targetR || process.env.WF_OPTION_FILTER_TARGET_R, DEFAULT_TARGET_R));
  const minTrades = Math.max(10, Math.trunc(parseNumber(args.minTrades || process.env.WF_OPTION_FILTER_MIN_TRADES, DEFAULT_MIN_TRADES)));
  const includeAllSymbols = parseBooleanLike(
    args.includeAllSymbols || process.env.WF_OPTION_FILTER_INCLUDE_ALL_SYMBOLS,
    true,
  );

  const backtest = JSON.parse(fs.readFileSync(backtestPath, 'utf8'));
  if (!Array.isArray(backtest.trades) || backtest.trades.length === 0) {
    throw new Error('backtest_report_missing_trade_rows: rerun with --includeTrades 1');
  }

  const reportToIso = parseDateIso(backtest?.config?.toIso, DEFAULT_TEST_TO);
  const trainFrom = parseDateIso(args.trainFrom || process.env.WF_OPTION_FILTER_TRAIN_FROM, DEFAULT_TRAIN_FROM);
  const trainTo = parseDateIso(args.trainTo || process.env.WF_OPTION_FILTER_TRAIN_TO, DEFAULT_TRAIN_TO);
  const testFrom = parseDateIso(args.testFrom || process.env.WF_OPTION_FILTER_TEST_FROM, DEFAULT_TEST_FROM);
  const testTo = parseDateIso(args.testTo || process.env.WF_OPTION_FILTER_TEST_TO, reportToIso || DEFAULT_TEST_TO);

  const normalizedTrades = normalizeTrades(backtest, targetR, includeAllSymbols);
  if (normalizedTrades.length === 0) {
    throw new Error('no_trades_after_symbol_filter');
  }

  const optionFeatureMap = buildOptionFeatureMap(normalizedTrades);
  const enriched = enrichWithOptionFeatures(normalizedTrades, optionFeatureMap);
  if (enriched.length < minTrades) {
    throw new Error(`not_enough_enriched_trades:${enriched.length}`);
  }

  const trainRows = enriched.filter((row) => inDateRange(row.dayEt, trainFrom, trainTo));
  const testRows = enriched.filter((row) => inDateRange(row.dayEt, testFrom, testTo));
  if (trainRows.length < minTrades) {
    throw new Error(`train_rows_below_min_trades:${trainRows.length}`);
  }
  if (testRows.length === 0) {
    throw new Error('test_rows_empty');
  }

  const { baselinePrimary, singleConditions } = buildSingleConditions(
    trainRows,
    targetR,
    primaryExitField,
    minTrades,
  );
  const comboConditions = buildComboConditions(
    trainRows,
    targetR,
    primaryExitField,
    minTrades,
    sortByHitThenExpectancy(singleConditions),
  );
  const selection = selectCondition(singleConditions, comboConditions, baselinePrimary, minTrades);
  const selectedCondition = selection.preferred;
  const filteredTrainRows = applyCondition(trainRows, selectedCondition);
  const filteredTestRows = applyCondition(testRows, selectedCondition);

  const baselineTrainByExit = computeMetricsByExit(trainRows, targetR);
  const baselineTestByExit = computeMetricsByExit(testRows, targetR);
  const filteredTrainByExit = computeMetricsByExit(filteredTrainRows, targetR);
  const filteredTestByExit = computeMetricsByExit(filteredTestRows, targetR);

  const testDeltaByExit = {};
  EXIT_FIELDS.forEach((exitField) => {
    testDeltaByExit[exitField] = computeDelta(filteredTestByExit[exitField], baselineTestByExit[exitField]);
  });

  const trainFilteredExitRanking = EXIT_FIELDS.map((exitField) => ({
    exitField,
    avgExitR: filteredTrainByExit[exitField]?.avgExitR,
    totalExitR: filteredTrainByExit[exitField]?.totalExitR,
  })).sort((a, b) => (b.avgExitR ?? -999) - (a.avgExitR ?? -999));
  const selectedExitByTrain = trainFilteredExitRanking[0]?.exitField || primaryExitField;

  const result = {
    generatedAt: nowIso(),
    sourceBacktestReport: backtestPath,
    config: {
      targetR,
      minTrades,
      includeAllSymbols,
      symbolScope: includeAllSymbols ? 'all_symbols' : 'robust20',
      trainFrom,
      trainTo,
      testFrom,
      testTo,
      primaryExitField,
      testedExitFields: EXIT_FIELDS,
      minuteFeatureSource: 'options.option_symbol_minute_derived',
      featureMinuteLag: 'prior_1m',
      strictWalkForward: true,
    },
    coverage: {
      sourceTrades: Array.isArray(backtest.trades) ? backtest.trades.length : 0,
      analyzedTrades: normalizedTrades.length,
      tradesWithOptionFeatures: enriched.length,
      optionFeatureCoverageRate: normalizedTrades.length > 0 ? (enriched.length / normalizedTrades.length) : null,
      trainTrades: trainRows.length,
      testTrades: testRows.length,
      filteredTrainTrades: filteredTrainRows.length,
      filteredTestTrades: filteredTestRows.length,
    },
    train: {
      baselineByExit: baselineTrainByExit,
      selectedFilterByPrimaryExit: sanitizeCondition(selectedCondition),
      selectedFilterBeatsTrainBaseline: selectedCondition
        ? (selectedCondition.avgExitR ?? -999) > (baselinePrimary.avgExitR ?? -999)
        : false,
      bestForExpectancyByPrimaryExit: sanitizeCondition(selection.bestForExpectancy),
      bestForHitByPrimaryExit: sanitizeCondition(selection.bestForHit),
      topSingleByExpectancy: sortByExpectancyThenHit(singleConditions).slice(0, 10).map(sanitizeCondition),
      topComboByExpectancy: sortByExpectancyThenHit(comboConditions).slice(0, 10).map(sanitizeCondition),
      filteredByExit: filteredTrainByExit,
    },
    test: {
      baselineByExit: baselineTestByExit,
      filteredByExit: filteredTestByExit,
      deltaByExit: testDeltaByExit,
    },
    exitModelSelection: {
      selectedOnTrainFiltered: selectedExitByTrain,
      trainFilteredExitRanking,
      selectedExitTestBaseline: baselineTestByExit[selectedExitByTrain],
      selectedExitTestFiltered: filteredTestByExit[selectedExitByTrain],
      selectedExitTestDelta: testDeltaByExit[selectedExitByTrain],
    },
  };

  const outputPath = path.resolve(
    args.outputPath
      || process.env.WF_OPTION_FILTER_OUTPUT_PATH
      || path.join(
        process.cwd(),
        'artifacts',
        'reports',
        `walkforward-option-1m-filter-${includeAllSymbols ? 'all-symbols' : 'robust20'}-r${targetR}-${nowIso().replace(/[:.]/g, '').replace(/-/g, '')}.json`,
      ),
  );
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');

  const latestPath = path.resolve(process.cwd(), 'artifacts', 'reports', 'walkforward-option-1m-filter-latest.json');
  fs.writeFileSync(latestPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({
    outputPath,
    latestPath,
    coverage: result.coverage,
    trainSelectedFilter: result.train.selectedFilterByPrimaryExit,
    testBaselineByExit: result.test.baselineByExit,
    testFilteredByExit: result.test.filteredByExit,
    exitModelSelection: result.exitModelSelection,
  }, null, 2));
}

try {
  run();
} catch (error) {
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
}
