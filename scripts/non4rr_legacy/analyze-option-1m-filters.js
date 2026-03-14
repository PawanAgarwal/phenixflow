#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const {
  queryRowsSync,
} = require('../../src/storage/clickhouse');

const DEFAULT_BACKTEST_REPORT = path.join(process.cwd(), 'artifacts', 'reports', 'flag30-ema8-backtest-latest.json');
const DEFAULT_EXIT_FIELD = 'closeOrStopR';
const DEFAULT_TARGET_R = 4;
const DEFAULT_MIN_TRADES = 30;

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
  const filtered = values.filter((value) => Number.isFinite(value)).sort((a, b) => a - b);
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

function computeMetrics(rows) {
  const count = rows.length;
  if (count === 0) {
    return {
      trades: 0,
      hit4Rate: null,
      avgExitR: null,
      totalExitR: null,
      winRate: null,
      avgMaxR: null,
      avgWinR: null,
      avgLossR: null,
    };
  }

  const exitValues = rows.map((row) => row.exitR);
  const maxValues = rows.map((row) => row.maxR);
  const wins = rows.filter((row) => row.exitR > 0);
  const losses = rows.filter((row) => row.exitR < 0);
  const hit4Count = rows.filter((row) => row.hitTarget).length;

  return {
    trades: count,
    hit4Rate: hit4Count / count,
    avgExitR: avg(exitValues),
    totalExitR: sum(exitValues),
    winRate: wins.length / count,
    avgMaxR: avg(maxValues),
    avgWinR: avg(wins.map((row) => row.exitR)),
    avgLossR: avg(losses.map((row) => row.exitR)),
  };
}

function conditionLabel(name, op, threshold) {
  const formatted = Number.isFinite(threshold) ? threshold.toFixed(6).replace(/0+$/, '').replace(/\.$/, '') : String(threshold);
  return `${name} ${op} ${formatted}`;
}

function run() {
  const args = parseArgs(process.argv);
  const backtestPath = path.resolve(args.backtest || DEFAULT_BACKTEST_REPORT);
  const exitField = String(args.exitField || process.env.OPTION_FILTER_EXIT_FIELD || DEFAULT_EXIT_FIELD).trim();
  const targetR = parseNumber(args.targetR || process.env.OPTION_FILTER_TARGET_R, DEFAULT_TARGET_R);
  const minTrades = Math.max(10, Math.trunc(parseNumber(args.minTrades || process.env.OPTION_FILTER_MIN_TRADES, DEFAULT_MIN_TRADES)));
  const includeAllSymbols = parseBooleanLike(args.includeAllSymbols || process.env.OPTION_FILTER_INCLUDE_ALL_SYMBOLS, false);

  const backtest = JSON.parse(fs.readFileSync(backtestPath, 'utf8'));
  if (!Array.isArray(backtest.trades) || backtest.trades.length === 0) {
    throw new Error('backtest_report_missing_trade_rows: rerun with --includeTrades 1');
  }

  const sourceTrades = backtest.trades
    .map((trade) => {
      const entryMs = Date.parse(String(trade.entryTsUtc || ''));
      const exitR = toFinite(trade[exitField]);
      const maxR = toFinite(trade.maxR);
      if (!Number.isFinite(entryMs) || exitR === null || maxR === null) return null;
      return {
        symbol: String(trade.symbol || '').trim().toUpperCase(),
        dayEt: String(trade.dayEt || '').trim(),
        entryTsUtc: new Date(entryMs).toISOString(),
        setupBarStartEt: String(trade.setupBarStartEt || '').trim(),
        entryMsUtc: entryMs,
        priorMinuteMsUtc: entryMs - (60 * 1000),
        priorMinuteUtc: toClickHouseDateTime64(entryMs - (60 * 1000)),
        exitR,
        maxR,
        hitTarget: maxR >= targetR,
      };
    })
    .filter(Boolean);

  const robustSymbols = new Set([
    'AAPL', 'AVGO', 'BA', 'COP', 'CRM', 'CVS', 'CVX', 'GE', 'GIS', 'GOOGL',
    'NKE', 'PLTR', 'PM', 'PYPL', 'RF', 'RTX', 'SHOP', 'SLB', 'SOFI', 'SPGI',
  ]);

  const trades = includeAllSymbols
    ? sourceTrades
    : sourceTrades.filter((trade) => robustSymbols.has(trade.symbol));

  if (trades.length === 0) {
    throw new Error('no_trades_after_symbol_filter');
  }

  const neededMinutesBySymbol = new Map();
  trades.forEach((trade) => {
    if (!neededMinutesBySymbol.has(trade.symbol)) neededMinutesBySymbol.set(trade.symbol, new Set());
    neededMinutesBySymbol.get(trade.symbol).add(trade.priorMinuteUtc);
  });

  const optionFeatureMap = new Map();
  const symbols = Array.from(neededMinutesBySymbol.keys()).sort();
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
    'call_iv_avg',
    'put_iv_avg',
  ];

  symbols.forEach((symbol) => {
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

  const enriched = trades.map((trade) => {
    const key = `${trade.symbol}\t${trade.priorMinuteUtc}`;
    const row = optionFeatureMap.get(key);
    if (!row) return null;

    const tradeCount = toFinite(row.trade_count) || 0;
    const callSize = toFinite(row.call_size) || 0;
    const putSize = toFinite(row.put_size) || 0;
    const totalSize = callSize + putSize;
    const bullishCount = toFinite(row.bullish_count) || 0;
    const bearishCount = toFinite(row.bearish_count) || 0;
    const sweepCount = toFinite(row.sweep_count) || 0;
    const totalValue = toFinite(row.total_value) || 0;

    return {
      ...trade,
      tradeCount,
      totalValue,
      callSize,
      putSize,
      totalSize,
      callPutSizeRatio: callSize / Math.max(1, putSize),
      netCallFlow: (callSize - putSize) / Math.max(1, totalSize),
      bullishCount,
      bearishCount,
      bullishShare: bullishCount / Math.max(1, tradeCount),
      bullBearRatio: bullishCount / Math.max(1, bearishCount),
      sweepCount,
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
      callIvAvg: toFinite(row.call_iv_avg),
      putIvAvg: toFinite(row.put_iv_avg),
    };
  }).filter(Boolean);

  const baseline = computeMetrics(enriched);

  const featureNames = [
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

  const singleConditions = [];
  featureNames.forEach((feature) => {
    const values = enriched.map((row) => row[feature]).filter((value) => Number.isFinite(value));
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
      const filtered = enriched.filter((row) => {
        const value = row[feature];
        if (!Number.isFinite(value)) return false;
        if (spec.op === '>=') return value >= threshold;
        return value <= threshold;
      });
      if (filtered.length < minTrades) return;
      const metrics = computeMetrics(filtered);
      singleConditions.push({
        type: 'single',
        feature,
        op: spec.op,
        threshold,
        label: conditionLabel(feature, spec.op, threshold),
        ...metrics,
        deltaHit4VsBaseline: (metrics.hit4Rate ?? 0) - (baseline.hit4Rate ?? 0),
        deltaAvgExitRVsBaseline: (metrics.avgExitR ?? 0) - (baseline.avgExitR ?? 0),
      });
    });
  });

  singleConditions.sort((a, b) => {
    if ((b.hit4Rate ?? -1) !== (a.hit4Rate ?? -1)) return (b.hit4Rate ?? -1) - (a.hit4Rate ?? -1);
    if ((b.avgExitR ?? -999) !== (a.avgExitR ?? -999)) return (b.avgExitR ?? -999) - (a.avgExitR ?? -999);
    return (b.trades || 0) - (a.trades || 0);
  });

  const topSinglesForCombo = singleConditions
    .filter((row) => row.deltaHit4VsBaseline > 0 || row.deltaAvgExitRVsBaseline > 0)
    .slice(0, 12);

  const comboConditions = [];
  for (let i = 0; i < topSinglesForCombo.length; i += 1) {
    for (let j = i + 1; j < topSinglesForCombo.length; j += 1) {
      const c1 = topSinglesForCombo[i];
      const c2 = topSinglesForCombo[j];
      const filtered = enriched.filter((row) => {
        const v1 = row[c1.feature];
        const v2 = row[c2.feature];
        if (!Number.isFinite(v1) || !Number.isFinite(v2)) return false;
        const pass1 = c1.op === '>=' ? v1 >= c1.threshold : v1 <= c1.threshold;
        const pass2 = c2.op === '>=' ? v2 >= c2.threshold : v2 <= c2.threshold;
        return pass1 && pass2;
      });
      if (filtered.length < Math.max(20, Math.floor(minTrades * 0.67))) continue;
      const metrics = computeMetrics(filtered);
      comboConditions.push({
        type: 'combo',
        label: `${c1.label} AND ${c2.label}`,
        legs: [c1.label, c2.label],
        ...metrics,
        deltaHit4VsBaseline: (metrics.hit4Rate ?? 0) - (baseline.hit4Rate ?? 0),
        deltaAvgExitRVsBaseline: (metrics.avgExitR ?? 0) - (baseline.avgExitR ?? 0),
      });
    }
  }

  comboConditions.sort((a, b) => {
    if ((b.hit4Rate ?? -1) !== (a.hit4Rate ?? -1)) return (b.hit4Rate ?? -1) - (a.hit4Rate ?? -1);
    if ((b.avgExitR ?? -999) !== (a.avgExitR ?? -999)) return (b.avgExitR ?? -999) - (a.avgExitR ?? -999);
    return (b.trades || 0) - (a.trades || 0);
  });

  const bestForHit4 = comboConditions.find((row) => row.trades >= minTrades && (row.hit4Rate ?? 0) > (baseline.hit4Rate ?? 0))
    || singleConditions.find((row) => row.trades >= minTrades && (row.hit4Rate ?? 0) > (baseline.hit4Rate ?? 0))
    || null;
  const bestForExit = comboConditions.find((row) => row.trades >= minTrades && (row.avgExitR ?? -999) > (baseline.avgExitR ?? -999))
    || singleConditions.find((row) => row.trades >= minTrades && (row.avgExitR ?? -999) > (baseline.avgExitR ?? -999))
    || null;

  const result = {
    generatedAt: nowIso(),
    sourceBacktestReport: backtestPath,
    config: {
      exitField,
      targetR,
      minTrades,
      includeAllSymbols,
      symbolCount: includeAllSymbols ? null : robustSymbols.size,
      minuteFeatureSource: 'options.option_symbol_minute_derived',
      featureMinuteLag: 'prior_1m',
    },
    coverage: {
      sourceTrades: sourceTrades.length,
      analyzedTrades: trades.length,
      tradesWithOptionFeatures: enriched.length,
      optionFeatureCoverageRate: trades.length > 0 ? (enriched.length / trades.length) : null,
    },
    baseline,
    topSingleConditionsByHit4: singleConditions.slice(0, 20),
    topSingleConditionsByExpectancy: singleConditions
      .slice()
      .sort((a, b) => {
        if ((b.avgExitR ?? -999) !== (a.avgExitR ?? -999)) return (b.avgExitR ?? -999) - (a.avgExitR ?? -999);
        return (b.trades || 0) - (a.trades || 0);
      })
      .slice(0, 20),
    topComboConditionsByHit4: comboConditions.slice(0, 20),
    topComboConditionsByExpectancy: comboConditions
      .slice()
      .sort((a, b) => {
        if ((b.avgExitR ?? -999) !== (a.avgExitR ?? -999)) return (b.avgExitR ?? -999) - (a.avgExitR ?? -999);
        return (b.trades || 0) - (a.trades || 0);
      })
      .slice(0, 20),
    bestForHit4,
    bestForExpectancy: bestForExit,
  };

  const outputPath = path.resolve(
    args.outputPath
      || process.env.OPTION_FILTER_OUTPUT_PATH
      || path.join(
        process.cwd(),
        'artifacts',
        'reports',
        `option-1m-filter-analysis-${includeAllSymbols ? 'all-symbols' : 'robust20'}-${nowIso().replace(/[:.]/g, '').replace(/-/g, '')}.json`,
      ),
  );
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');

  const latestPath = path.resolve(process.cwd(), 'artifacts', 'reports', 'option-1m-filter-analysis-latest.json');
  fs.writeFileSync(latestPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({
    outputPath,
    latestPath,
    baseline,
    bestForHit4,
    bestForExpectancy: bestForExit,
  }, null, 2));
}

try {
  run();
} catch (error) {
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
}
