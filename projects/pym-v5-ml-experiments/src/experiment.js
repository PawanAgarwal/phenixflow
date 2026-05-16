const fs = require('node:fs');
const path = require('node:path');

const { readDailyBarsJsonl, tickerReturn } = require('../../pym-v5-replication/src/backtest');
const { ensureDir, loadConfig, runtimePath: pymRuntimePath } = require('../../pym-v5-replication/src/config');
const { defaultScorePath, findLatestMassiveEodBarsPath } = require('../../pym-v5-replication/src/rebalance-report');
const { evaluateSymphony } = require('../../pym-v5-replication/src/symphony');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const DEFAULT_START_DATE = '2025-01-02';
const DEFAULT_TRAIN_END = '2025-10-31';
const DEFAULT_VALIDATION_START = '2025-11-01';
const DEFAULT_VALIDATION_END = '2025-12-31';
const DEFAULT_TEST_START = '2026-01-01';
const DEFAULT_RSI_MODE = 'wilder';
const DEFAULT_INITIAL_CAPITAL = 10000;
const DEFAULT_COST_BPS = 2;
const DEFAULT_LOOKBACK = 63;
const SAFE_TICKERS = Object.freeze(['BIL', 'SHV', 'BSV', 'SHY']);
const RETURN_HORIZONS = Object.freeze([1, 3, 5, 10, 21, 42, 63]);
const VOL_WINDOWS = Object.freeze([5, 21, 63]);
const LIQUIDITY_WINDOWS = Object.freeze([5, 21, 63]);
const ATTENTION_TEMPERATURES = Object.freeze([0.35, 0.75, 1.5]);
const CORE_TICKERS = Object.freeze([
  'SPY', 'QQQ', 'IWM', 'TLT', 'IEF', 'GLD', 'UUP', 'UDN', 'USO',
  'VIXY', 'UVXY', 'SVIX', 'SVXY', 'BIL', 'SHV',
  'XLK', 'XLU', 'XLP', 'XLV', 'XLF', 'EEM', 'KMLM',
  'TQQQ', 'SQQQ', 'QLD', 'QID', 'UPRO', 'SPXU', 'SOXX', 'SOXL', 'SOXS',
  'TMF', 'TMV', 'EDC', 'EDZ', 'UGL', 'GLL', 'UTSL',
]);
const MICRO_TICKERS = Object.freeze(['SPY', 'QQQ', 'VIXY', 'UVXY', 'TQQQ']);
const MICRO_OPTION_ROOTS = Object.freeze(['SPY', 'SPX', 'SPXW', 'QQQ']);
const MICRO_OPTION_FIELDS = Object.freeze([
  'transactions',
  'totalVolume',
  'totalPremiumLog',
  'premiumImbalance',
  'volumeImbalance',
  'callPremiumShare',
  'putCallPremiumRatio',
  'nearDteVolumeShare',
  'zeroDteVolumeShare',
  'shortDatedAtmFlowProxy',
  'roll20_totalPremiumLog_z',
  'roll20_totalVolume_z',
  'roll20_transactions_z',
  'roll20_putCallPremiumRatio_z',
  'roll20_shortDatedAtmFlowProxy_z',
  'mom5_totalPremium',
  'mom5_totalVolume',
  'mom5_transactions',
]);
const OPTION_ROOTS = Object.freeze([
  'SPY', 'SPX', 'SPXW', 'QQQ', 'IWM', 'TLT', 'IEF', 'GLD', 'VIX', 'VIXW', 'VIXY', 'UVXY',
  'XLK', 'XLU', 'XLP', 'XLV', 'XLF', 'EEM', 'TQQQ', 'SQQQ', 'SOXX',
]);
const OPTION_FIELDS = Object.freeze([
  'contractBars',
  'transactions',
  'totalVolume',
  'totalPremiumLog',
  'callVolume',
  'putVolume',
  'callPremium',
  'putPremium',
  'nearDteVolume',
  'zeroDteVolume',
  'oneDteVolume',
  'twoToSevenDteVolume',
  'eightToThirtyDteVolume',
  'atmVolume',
  'nearAtmVolume',
  'nearAtmPremium',
  'otmCallVolume',
  'otmPutVolume',
  'itmCallVolume',
  'itmPutVolume',
  'premiumImbalance',
  'volumeImbalance',
  'callPremiumShare',
  'putCallPremiumRatio',
  'nearDteVolumeShare',
  'zeroDteVolumeShare',
  'atmVolumeShare',
  'nearAtmPremiumShare',
  'nearAtmPremiumImbalance',
  'shortDatedAtmFlowProxy',
  'roll20_totalPremiumLog_z',
  'roll20_totalVolume_z',
  'roll20_transactions_z',
  'roll20_premiumImbalance_z',
  'roll20_volumeImbalance_z',
  'roll20_putCallPremiumRatio_z',
  'roll20_nearDteVolumeShare_z',
  'roll20_zeroDteVolumeShare_z',
  'roll20_shortDatedAtmFlowProxy_z',
  'mom5_totalPremium',
  'mom5_totalVolume',
  'mom5_transactions',
  'mom5_callPremium',
  'mom5_putPremium',
]);

function artifactPath(...parts) {
  return path.join(PROJECT_ROOT, 'artifacts', ...parts);
}

function pct(value) {
  return Number.isFinite(value) ? value * 100 : null;
}

function finite(value, fallback = 0) {
  return Number.isFinite(value) ? value : fallback;
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function sigmoid(value) {
  if (value >= 35) return 1;
  if (value <= -35) return 0;
  return 1 / (1 + Math.exp(-value));
}

function safeTickerForMarket(market) {
  return SAFE_TICKERS.find((ticker) => market.closes.has(ticker)) || market.tickers[0];
}

function cleanWeightsObject(weights) {
  const out = {};
  Object.entries(weights || {}).forEach(([ticker, weight]) => {
    if (Number.isFinite(weight) && weight > 1e-10) out[ticker] = weight;
  });
  return out;
}

function mapToWeightsObject(weights) {
  const out = {};
  weights.forEach((weight, ticker) => {
    if (Number.isFinite(weight) && weight > 1e-10) out[ticker] = weight;
  });
  return out;
}

function normalizeLongOnly(rawWeights, outputTickers, safeTicker, maxWeight = 1) {
  const clipped = {};
  let sum = 0;
  outputTickers.forEach((ticker) => {
    const value = Math.min(maxWeight, Math.max(0, finite(rawWeights[ticker])));
    if (value > 1e-10) {
      clipped[ticker] = value;
      sum += value;
    }
  });
  if (sum <= 1e-10) return { [safeTicker]: 1 };
  const normalized = {};
  Object.entries(clipped).forEach(([ticker, value]) => {
    normalized[ticker] = value / sum;
  });
  return normalized;
}

function weightTurnover(previous, next) {
  const keys = new Set([...Object.keys(previous || {}), ...Object.keys(next || {})]);
  let turnover = 0;
  keys.forEach((ticker) => {
    turnover += Math.abs((next[ticker] || 0) - (previous[ticker] || 0));
  });
  return turnover;
}

function portfolioReturnForIndex(weights, market, nextIndex) {
  let grossReturn = 0;
  Object.entries(weights || {}).forEach(([ticker, weight]) => {
    const ret = tickerReturn(market.closes, ticker, nextIndex);
    if (ret === null) return;
    grossReturn += weight * ret;
  });
  return grossReturn;
}

function dailyReturn(market, ticker, index) {
  return tickerReturn(market.closes, ticker, index) ?? 0;
}

function horizonReturn(market, ticker, index, horizon) {
  const closes = market.closes.get(ticker) || [];
  const current = closes[index];
  const previous = closes[index - horizon];
  if (!Number.isFinite(current) || !Number.isFinite(previous) || previous <= 0) return 0;
  return (current / previous) - 1;
}

function movingAverageDistance(market, ticker, index, window) {
  const closes = market.closes.get(ticker) || [];
  const current = closes[index];
  const start = Math.max(0, index - window + 1);
  const values = closes.slice(start, index + 1).filter(Number.isFinite);
  if (!Number.isFinite(current) || !values.length) return 0;
  const avg = mean(values);
  return avg > 0 ? (current / avg) - 1 : 0;
}

function realizedVolatility(market, ticker, index, window) {
  const returns = [];
  for (let cursor = Math.max(1, index - window + 1); cursor <= index; cursor += 1) {
    const ret = tickerReturn(market.closes, ticker, cursor);
    if (ret !== null) returns.push(ret);
  }
  return standardDeviation(returns) * Math.sqrt(252);
}

function rollingDrawdown(market, ticker, index, window) {
  const closes = market.closes.get(ticker) || [];
  const current = closes[index];
  if (!Number.isFinite(current)) return 0;
  let peak = current;
  for (let cursor = Math.max(0, index - window + 1); cursor <= index; cursor += 1) {
    if (Number.isFinite(closes[cursor])) peak = Math.max(peak, closes[cursor]);
  }
  return peak > 0 ? (current / peak) - 1 : 0;
}

function marketRow(market, ticker, index) {
  const date = market.dates[index];
  return market.byDate?.get(date)?.get(ticker) || null;
}

function stockMetric(market, ticker, index, metric) {
  const row = marketRow(market, ticker, index);
  if (!row) return 0;
  if (metric === 'logVolume') return row.volume > 0 ? Math.log1p(row.volume) : 0;
  if (metric === 'logTransactions') return row.transactions > 0 ? Math.log1p(row.transactions) : 0;
  if (metric === 'logDollarVolume') return row.volume > 0 && row.close > 0 ? Math.log1p(row.volume * row.close) : 0;
  if (metric === 'rangePct') return row.close > 0 ? (row.high - row.low) / row.close : 0;
  if (metric === 'openClosePct') return row.open > 0 ? (row.close / row.open) - 1 : 0;
  if (metric === 'closeLocation') return row.high > row.low ? ((row.close - row.low) / (row.high - row.low)) - 0.5 : 0;
  if (metric === 'transactionsPerVolume') return row.volume > 0 ? row.transactions / row.volume : 0;
  if (metric === 'volumePerTransaction') return row.transactions > 0 ? Math.log1p(row.volume / row.transactions) : 0;
  return 0;
}

function rollingMetricZ(market, ticker, index, metric, window) {
  const current = stockMetric(market, ticker, index, metric);
  const values = [];
  for (let cursor = Math.max(0, index - window); cursor < index; cursor += 1) {
    values.push(stockMetric(market, ticker, cursor, metric));
  }
  const valid = values.filter(Number.isFinite);
  if (valid.length < 5) return 0;
  const avg = mean(valid);
  const sd = standardDeviation(valid);
  return sd > 0 ? (current - avg) / sd : 0;
}

function softmax(values) {
  const maxValue = Math.max(...values);
  const expValues = values.map((value) => Math.exp(Math.max(-50, Math.min(50, value - maxValue))));
  const sum = expValues.reduce((acc, value) => acc + value, 0);
  return sum > 0 ? expValues.map((value) => value / sum) : values.map(() => 1 / values.length);
}

// --- Overnight-gap features ---
//
// A gap-down day is yesterday-close → today-open negative. The intraday
// open→close ("body") tells us whether the gap filled, ran, or fully
// reversed. Combined with the closing strength (where the close sat in
// today's range), these three features fully characterize a gap session.
//
// Hypothesis being tested: do gap-day features add information the LGBM
// model wouldn't already get from the attention/pym groups (which use
// close-to-close returns only)?

const GAP_TICKERS = Object.freeze(['SPY', 'QQQ', 'IWM', 'TQQQ', 'SOXL']);

function gapToday(market, ticker, index) {
  if (index < 1) return 0;
  const today = marketRow(market, ticker, index);
  const yesterday = marketRow(market, ticker, index - 1);
  if (!today || !yesterday) return 0;
  if (!Number.isFinite(today.open) || !Number.isFinite(yesterday.close) || yesterday.close <= 0) return 0;
  return today.open / yesterday.close - 1;
}

function intradayReturn(market, ticker, index) {
  const today = marketRow(market, ticker, index);
  if (!today) return 0;
  if (!Number.isFinite(today.open) || !Number.isFinite(today.close) || today.open <= 0) return 0;
  return today.close / today.open - 1;
}

function closeLocation(market, ticker, index) {
  const today = marketRow(market, ticker, index);
  if (!today) return 0;
  if (!Number.isFinite(today.high) || !Number.isFinite(today.low) || !Number.isFinite(today.close)) return 0;
  if (today.high <= today.low) return 0;
  return ((today.close - today.low) / (today.high - today.low)) - 0.5;
}

function gapZ21(market, ticker, index) {
  if (index < 21) return 0;
  const current = gapToday(market, ticker, index);
  const values = [];
  for (let cursor = Math.max(1, index - 21); cursor < index; cursor += 1) {
    values.push(gapToday(market, ticker, cursor));
  }
  const valid = values.filter(Number.isFinite);
  if (valid.length < 5) return 0;
  const avg = mean(valid);
  const sd = standardDeviation(valid);
  return sd > 0 ? (current - avg) / sd : 0;
}

function gapAbsAvg5(market, ticker, index) {
  if (index < 5) return 0;
  const values = [];
  for (let cursor = Math.max(1, index - 4); cursor <= index; cursor += 1) {
    values.push(Math.abs(gapToday(market, ticker, cursor)));
  }
  return mean(values.filter(Number.isFinite));
}

function gapContinuation5(market, ticker, index) {
  // Count the last 5 days where intraday return continued the gap direction.
  // Continuation = sign(gap) == sign(intraday). A high count means trending
  // gap-day regime; low count means mean-reverting (fade-friendly) regime.
  if (index < 5) return 0;
  let cont = 0;
  for (let cursor = Math.max(1, index - 4); cursor <= index; cursor += 1) {
    const g = gapToday(market, ticker, cursor);
    const intra = intradayReturn(market, ticker, cursor);
    if (Math.abs(g) < 1e-4) continue;
    if (Math.sign(g) === Math.sign(intra)) cont += 1;
  }
  return cont / 5.0;
}

function appendGapFeatures(out, names, market, index) {
  GAP_TICKERS.forEach((ticker) => {
    out.push(gapToday(market, ticker, index));
    names?.push(`${ticker}_gap_bps`);
    out.push(gapZ21(market, ticker, index));
    names?.push(`${ticker}_gap_z21`);
    out.push(intradayReturn(market, ticker, index));
    names?.push(`${ticker}_intraday_pct`);
    out.push(closeLocation(market, ticker, index));
    names?.push(`${ticker}_close_loc`);
    out.push(gapAbsAvg5(market, ticker, index));
    names?.push(`${ticker}_gap_abs_avg5`);
    out.push(gapContinuation5(market, ticker, index));
    names?.push(`${ticker}_gap_cont5`);
  });
}

function appendPriceFeatures(out, names, market, index, coreTickers) {
  coreTickers.forEach((ticker) => {
    RETURN_HORIZONS.forEach((horizon) => {
      out.push(horizonReturn(market, ticker, index, horizon));
      names?.push(`${ticker}_ret_${horizon}`);
    });
    VOL_WINDOWS.forEach((window) => {
      out.push(realizedVolatility(market, ticker, index, window));
      names?.push(`${ticker}_vol_${window}`);
      out.push(movingAverageDistance(market, ticker, index, window));
      names?.push(`${ticker}_ma_dist_${window}`);
      out.push(rollingDrawdown(market, ticker, index, window));
      names?.push(`${ticker}_drawdown_${window}`);
    });
  });
}

function appendLiquidityFeatures(out, names, market, index, coreTickers) {
  const metrics = [
    'logVolume',
    'logTransactions',
    'logDollarVolume',
    'rangePct',
    'openClosePct',
    'closeLocation',
    'transactionsPerVolume',
    'volumePerTransaction',
  ];
  coreTickers.forEach((ticker) => {
    metrics.forEach((metric) => {
      out.push(stockMetric(market, ticker, index, metric));
      names?.push(`${ticker}_${metric}`);
    });
    LIQUIDITY_WINDOWS.forEach((window) => {
      ['logVolume', 'logTransactions', 'logDollarVolume', 'rangePct'].forEach((metric) => {
        out.push(rollingMetricZ(market, ticker, index, metric, window));
        names?.push(`${ticker}_${metric}_z_${window}`);
      });
    });
  });
}

function appendMicrostructureFeatures(out, names, context, sample) {
  const tickers = MICRO_TICKERS.filter((ticker) => context.market.closes.has(ticker));
  tickers.forEach((ticker) => {
    ['logVolume', 'logTransactions', 'logDollarVolume', 'rangePct', 'closeLocation', 'volumePerTransaction'].forEach((metric) => {
      out.push(stockMetric(context.market, ticker, sample.index, metric));
      names?.push(`micro_${ticker}_${metric}`);
    });
    [5, 21].forEach((window) => {
      ['logVolume', 'logTransactions', 'rangePct'].forEach((metric) => {
        out.push(rollingMetricZ(context.market, ticker, sample.index, metric, window));
        names?.push(`micro_${ticker}_${metric}_z_${window}`);
      });
    });
  });
  const day = context.optionByDate?.get(sample.date);
  MICRO_OPTION_ROOTS.forEach((root) => {
    const row = day?.[root];
    MICRO_OPTION_FIELDS.forEach((field) => {
      out.push(finite(row?.[field]));
      names?.push(`micro_option_${root}_${field}`);
    });
  });
}

function appendTeacherWeightFeatures(out, names, teacherWeights, outputTickers) {
  outputTickers.forEach((ticker) => {
    out.push(teacherWeights[ticker] || 0);
    names?.push(`pym_weight_${ticker}`);
  });
}

function appendOptionFeatures(out, names, optionByDate, date, optionRoots, optionFields) {
  const day = optionByDate?.get(date);
  optionRoots.forEach((root) => {
    const row = day?.[root];
    optionFields.forEach((field) => {
      out.push(finite(row?.[field]));
      names?.push(`option_${root}_${field}`);
    });
  });
}

function appendAttentionFeatures(out, names, market, index, coreTickers, lookback) {
  const latest = coreTickers.map((ticker) => dailyReturn(market, ticker, index));
  const norm = Math.sqrt(Math.max(1, latest.reduce((sum, value) => sum + (value * value), 0)));
  const depth = Math.max(5, Math.min(lookback, index - 1));
  ATTENTION_TEMPERATURES.forEach((temperature) => {
    const rows = [];
    const scores = [];
    for (let offset = 1; offset <= depth; offset += 1) {
      const row = coreTickers.map((ticker) => dailyReturn(market, ticker, index - offset));
      rows.push(row);
      const dot = row.reduce((sum, value, column) => sum + (value * latest[column]), 0);
      scores.push(dot / (norm * Math.max(0.0001, temperature)));
    }
    const weights = softmax(scores);
    coreTickers.forEach((ticker, column) => {
      let pooled = 0;
      let pooledAbs = 0;
      rows.forEach((row, rowIndex) => {
        pooled += weights[rowIndex] * row[column];
        pooledAbs += weights[rowIndex] * Math.abs(row[column]);
      });
      out.push(pooled);
      names?.push(`attention_t${temperature}_${ticker}_return`);
      out.push(pooledAbs);
      names?.push(`attention_t${temperature}_${ticker}_abs_return`);
    });
    out.push(Math.max(...weights));
    names?.push(`attention_t${temperature}_max_weight`);
  });
}

function buildFeatureVector(sample, context, featureSet, includeNames = false) {
  const out = [];
  const names = includeNames ? [] : null;
  if (featureSet.includes('price')) appendPriceFeatures(out, names, context.market, sample.index, context.coreTickers);
  if (featureSet.includes('liquidity')) appendLiquidityFeatures(out, names, context.market, sample.index, context.coreTickers);
  if (featureSet.includes('micro')) appendMicrostructureFeatures(out, names, context, sample);
  if (featureSet.includes('attention')) appendAttentionFeatures(out, names, context.market, sample.index, context.coreTickers, context.lookback);
  if (featureSet.includes('pym')) appendTeacherWeightFeatures(out, names, sample.teacherWeights, context.outputTickers);
  if (featureSet.includes('options')) {
    appendOptionFeatures(out, names, context.optionByDate, sample.date, context.optionRoots, context.optionFields);
  }
  if (featureSet.includes('gap')) appendGapFeatures(out, names, context.market, sample.index);
  out.push(1);
  names?.push('constant_context');
  return includeNames ? { values: out.map((value) => finite(value)), names } : out.map((value) => finite(value));
}

function findLatestOptionFeaturesPath() {
  const explicit = process.env.PYM_V5_OPTION_FEATURES_PATH;
  if (explicit) return path.resolve(explicit);
  const root = pymRuntimePath();
  if (!fs.existsSync(root)) return null;
  const matches = fs.readdirSync(root)
    .map((name) => {
      const match = name.match(/^pym-v5-option-bar-features-(\d{4}-\d{2}-\d{2})-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      return match ? { name, startDate: match[1], endDate: match[2] } : null;
    })
    .filter(Boolean)
    .sort((left, right) => (
      left.endDate.localeCompare(right.endDate)
      || left.startDate.localeCompare(right.startDate)
      || left.name.localeCompare(right.name)
    ));
  return matches.length ? path.join(root, matches.at(-1).name) : null;
}

function readOptionFeatureMap(filePath, roots = OPTION_ROOTS, fields = OPTION_FIELDS) {
  const byDate = new Map();
  if (!filePath || !fs.existsSync(filePath)) return byDate;
  const lines = fs.readFileSync(filePath, 'utf8').split('\n').filter(Boolean);
  const rawRows = lines.map((line) => JSON.parse(line)).sort((left, right) => left.date.localeCompare(right.date));
  const history = new Map(roots.map((root) => [root, []]));

  function optionMetric(source, root, field) {
    if (!source) return 0;
    if (field.startsWith('roll20_') && field.endsWith('_z')) {
      const metric = field.slice('roll20_'.length, -'_z'.length);
      const current = finite(source[metric]);
      const prior = (history.get(root) || []).slice(-20).map((row) => finite(row[metric])).filter(Number.isFinite);
      if (prior.length < 5) return 0;
      const avg = mean(prior);
      const sd = standardDeviation(prior);
      return sd > 0 ? (current - avg) / sd : 0;
    }
    if (field.startsWith('mom5_')) {
      const metric = field.slice('mom5_'.length);
      const current = finite(source[metric]);
      const prior = (history.get(root) || []).slice(-5).map((row) => finite(row[metric])).filter((value) => Number.isFinite(value) && value > 0);
      const avg = mean(prior);
      return avg > 0 ? (current / avg) - 1 : 0;
    }
    return finite(source[field]);
  }

  rawRows.forEach((row) => {
    const day = {};
    roots.forEach((root) => {
      const source = row.roots?.[root];
      if (!source) return;
      day[root] = {};
      fields.forEach((field) => {
        day[root][field] = optionMetric(source, root, field);
      });
    });
    byDate.set(row.date, day);
    roots.forEach((root) => {
      const source = row.roots?.[root];
      if (source) history.get(root).push(source);
    });
  });
  return byDate;
}

function lastEligibleIndex(dates, endDate) {
  if (!endDate) return dates.length - 1;
  for (let index = dates.length - 1; index >= 0; index -= 1) {
    if (dates[index] <= endDate) return index;
  }
  return -1;
}

function buildSamples({
  market,
  score,
  rsiMode = DEFAULT_RSI_MODE,
  lookback = DEFAULT_LOOKBACK,
  startDate = DEFAULT_START_DATE,
  endDate = null,
  includeLatestPrediction = false,
}) {
  const samples = [];
  const outputSet = new Set();
  const teacherByIndex = new Map();
  const latestIndex = lastEligibleIndex(market.dates, endDate);
  const finalTeacherIndex = includeLatestPrediction ? latestIndex : Math.min(latestIndex, market.dates.length - 2);
  for (let index = 0; index <= finalTeacherIndex; index += 1) {
    const weights = mapToWeightsObject(evaluateSymphony(score, market, index, { rsiMode }));
    teacherByIndex.set(index, weights);
    Object.keys(weights).forEach((ticker) => outputSet.add(ticker));
  }
  SAFE_TICKERS.forEach((ticker) => {
    if (market.closes.has(ticker)) outputSet.add(ticker);
  });
  const outputTickers = [...outputSet].sort();
  for (let index = Math.max(lookback, 1); index < market.dates.length - 1; index += 1) {
    const date = market.dates[index];
    if (date < startDate) continue;
    if (endDate && date > endDate) continue;
    const teacherWeights = teacherByIndex.get(index) || {};
    const nextReturns = {};
    outputTickers.forEach((ticker) => {
      nextReturns[ticker] = tickerReturn(market.closes, ticker, index + 1) ?? 0;
    });
    samples.push({
      index,
      date,
      nextDate: market.dates[index + 1],
      teacherWeights,
      nextReturns,
      teacherReturn: portfolioReturnForIndex(teacherWeights, market, index + 1),
    });
  }
  let latestPredictionSample = null;
  if (includeLatestPrediction && latestIndex >= Math.max(lookback, 1) && latestIndex === market.dates.length - 1) {
    const date = market.dates[latestIndex];
    if (date >= startDate && (!endDate || date <= endDate)) {
      latestPredictionSample = {
        index: latestIndex,
        date,
        nextDate: null,
        teacherWeights: teacherByIndex.get(latestIndex) || {},
        predictionOnly: true,
      };
    }
  }
  return { samples, outputTickers, latestPredictionSample };
}

function splitSamples(samples, {
  trainEnd = DEFAULT_TRAIN_END,
  validationStart = DEFAULT_VALIDATION_START,
  validationEnd = DEFAULT_VALIDATION_END,
  testStart = DEFAULT_TEST_START,
  testEnd = null,
} = {}) {
  const train = samples.filter((sample) => sample.date <= trainEnd);
  const validation = samples.filter((sample) => sample.date >= validationStart && sample.date <= validationEnd);
  const fit = samples.filter((sample) => sample.date < testStart);
  const test = samples.filter((sample) => sample.date >= testStart && (!testEnd || sample.date <= testEnd));
  return { train, validation, fit, test };
}

function matrixFromSamples(samples, context, featureSet) {
  return samples.map((sample) => buildFeatureVector(sample, context, featureSet));
}

function targetWeightsMatrix(samples, outputTickers) {
  return samples.map((sample) => outputTickers.map((ticker) => sample.teacherWeights[ticker] || 0));
}

function targetReturnsMatrix(samples, outputTickers) {
  return samples.map((sample) => outputTickers.map((ticker) => sample.nextReturns[ticker] || 0));
}

function binaryGateLabels(samples) {
  return samples.map((sample) => (sample.teacherReturn > 0 ? 1 : 0));
}

function standardizeTrainAndApply(trainX, otherXs) {
  const columns = trainX[0]?.length || 0;
  const means = Array(columns).fill(0);
  const stds = Array(columns).fill(1);
  for (let column = 0; column < columns; column += 1) {
    const values = trainX.map((row) => row[column]);
    means[column] = mean(values);
    stds[column] = standardDeviation(values) || 1;
  }
  const transform = (rows) => rows.map((row) => row.map((value, column) => (value - means[column]) / stds[column]));
  return { trainX: transform(trainX), otherXs: otherXs.map(transform), means, stds };
}

function solveLinearSystem(baseA, baseB) {
  const n = baseA.length;
  const rhsColumns = Array.isArray(baseB[0]) ? baseB[0].length : 1;
  const a = baseA.map((row, index) => [
    ...row,
    ...(rhsColumns === 1 ? [Array.isArray(baseB[index]) ? baseB[index][0] : baseB[index]] : baseB[index]),
  ]);
  for (let pivot = 0; pivot < n; pivot += 1) {
    let best = pivot;
    for (let row = pivot + 1; row < n; row += 1) {
      if (Math.abs(a[row][pivot]) > Math.abs(a[best][pivot])) best = row;
    }
    if (best !== pivot) [a[pivot], a[best]] = [a[best], a[pivot]];
    const pivotValue = Math.abs(a[pivot][pivot]) < 1e-12 ? 1e-12 : a[pivot][pivot];
    for (let column = pivot; column < n + rhsColumns; column += 1) a[pivot][column] /= pivotValue;
    for (let row = 0; row < n; row += 1) {
      if (row === pivot) continue;
      const factor = a[row][pivot];
      if (Math.abs(factor) < 1e-12) continue;
      for (let column = pivot; column < n + rhsColumns; column += 1) {
        a[row][column] -= factor * a[pivot][column];
      }
    }
  }
  const result = a.map((row) => row.slice(n));
  return rhsColumns === 1 ? result.map((row) => row[0]) : result;
}

function fitRidgeMulti(trainX, trainY, lambda = 1) {
  if (!trainX.length) throw new Error('fitRidgeMulti requires training rows');
  const rows = trainX.length;
  const features = trainX[0].length + 1;
  const targets = trainY[0].length;
  const xtx = Array.from({ length: features }, () => Array(features).fill(0));
  const xty = Array.from({ length: features }, () => Array(targets).fill(0));
  for (let row = 0; row < rows; row += 1) {
    const x = [1, ...trainX[row]];
    for (let i = 0; i < features; i += 1) {
      for (let j = 0; j < features; j += 1) xtx[i][j] += x[i] * x[j];
      for (let target = 0; target < targets; target += 1) xty[i][target] += x[i] * trainY[row][target];
    }
  }
  for (let i = 1; i < features; i += 1) xtx[i][i] += lambda;
  const solution = solveLinearSystem(xtx, xty);
  return targets === 1 ? solution.map((value) => [value]) : solution;
}

function predictLinearMulti(beta, row) {
  const x = [1, ...row];
  const targets = beta[0].length;
  const out = Array(targets).fill(0);
  for (let feature = 0; feature < x.length; feature += 1) {
    for (let target = 0; target < targets; target += 1) out[target] += x[feature] * beta[feature][target];
  }
  return out;
}

function fitLogisticBinary(trainX, trainY, { lambda = 0.1, iterations = 900, learningRate = 0.08 } = {}) {
  if (!trainX.length) throw new Error('fitLogisticBinary requires training rows');
  const features = trainX[0].length + 1;
  const weights = Array(features).fill(0);
  const rows = trainX.length;
  for (let iteration = 0; iteration < iterations; iteration += 1) {
    const grad = Array(features).fill(0);
    for (let row = 0; row < rows; row += 1) {
      const x = [1, ...trainX[row]];
      const z = x.reduce((sum, value, column) => sum + (value * weights[column]), 0);
      const error = sigmoid(z) - trainY[row];
      for (let column = 0; column < features; column += 1) grad[column] += error * x[column];
    }
    for (let column = 1; column < features; column += 1) grad[column] += lambda * weights[column];
    const step = learningRate / Math.sqrt(1 + iteration / 80);
    for (let column = 0; column < features; column += 1) weights[column] -= step * grad[column] / rows;
  }
  return weights;
}

function predictLogistic(weights, row) {
  const x = [1, ...row];
  return sigmoid(x.reduce((sum, value, column) => sum + (value * weights[column]), 0));
}

function maxDrawdown(equityCurve) {
  let peak = equityCurve[0]?.equity || DEFAULT_INITIAL_CAPITAL;
  let drawdown = 0;
  equityCurve.forEach((point) => {
    if (point.equity > peak) peak = point.equity;
    if (peak > 0) drawdown = Math.min(drawdown, (point.equity / peak) - 1);
  });
  return drawdown;
}

function monthlyReturns(equityCurve) {
  const months = new Map();
  equityCurve.forEach((point) => {
    const month = point.date.slice(0, 7);
    if (!months.has(month)) months.set(month, { startEquity: point.startEquity, endEquity: point.equity });
    months.get(month).endEquity = point.equity;
  });
  return [...months.entries()].map(([month, value]) => ({
    month,
    return: value.startEquity > 0 ? (value.endEquity / value.startEquity) - 1 : 0,
    returnPct: pct(value.startEquity > 0 ? (value.endEquity / value.startEquity) - 1 : 0),
  }));
}

function topAverageWeights(equityCurve, limit = 12) {
  const totals = new Map();
  equityCurve.forEach((point) => {
    Object.entries(point.holdings || {}).forEach(([ticker, weight]) => {
      totals.set(ticker, (totals.get(ticker) || 0) + weight);
    });
  });
  const count = Math.max(1, equityCurve.length);
  return [...totals.entries()]
    .map(([ticker, total]) => ({ ticker, averageWeight: total / count, averageWeightPct: pct(total / count) }))
    .sort((left, right) => right.averageWeight - left.averageWeight || left.ticker.localeCompare(right.ticker))
    .slice(0, limit);
}

function backtestPolicy(samples, market, predictWeights, {
  initialCapital = DEFAULT_INITIAL_CAPITAL,
  totalCostBps = DEFAULT_COST_BPS,
} = {}) {
  let equity = initialCapital;
  let previousWeights = {};
  const equityCurve = [];
  const dailyReturns = [];
  let totalTurnover = 0;
  let investedDays = 0;
  samples.forEach((sample) => {
    const weights = cleanWeightsObject(predictWeights(sample));
    const turnover = weightTurnover(previousWeights, weights);
    const grossReturn = portfolioReturnForIndex(weights, market, sample.index + 1);
    const costReturn = turnover * totalCostBps / 10000;
    const netReturn = grossReturn - costReturn;
    const startEquity = equity;
    equity *= (1 + netReturn);
    totalTurnover += turnover;
    const grossExposure = Object.values(weights).reduce((sum, value) => sum + value, 0);
    if (grossExposure > 0.001) investedDays += 1;
    equityCurve.push({
      date: sample.nextDate,
      signalDate: sample.date,
      startEquity,
      equity,
      grossReturn,
      costReturn,
      netReturn,
      turnover,
      grossExposure,
      holdings: weights,
    });
    dailyReturns.push(netReturn);
    previousWeights = weights;
  });
  const totalReturn = equityCurve.length ? (equity / initialCapital) - 1 : 0;
  const annualizedVolatility = standardDeviation(dailyReturns) * Math.sqrt(252);
  const avgDaily = mean(dailyReturns);
  const cagr = equityCurve.length ? ((1 + totalReturn) ** (252 / equityCurve.length)) - 1 : 0;
  return {
    startDate: samples[0]?.date || null,
    endDate: equityCurve.at(-1)?.date || null,
    tradingDays: equityCurve.length,
    finalEquity: equity,
    totalReturn,
    totalReturnPct: pct(totalReturn),
    cagr,
    cagrPct: pct(cagr),
    maxDrawdown: maxDrawdown(equityCurve),
    maxDrawdownPct: pct(maxDrawdown(equityCurve)),
    annualizedVolatility,
    annualizedVolatilityPct: pct(annualizedVolatility),
    sharpe: annualizedVolatility > 0 ? (avgDaily * 252) / annualizedVolatility : 0,
    averageDailyTurnover: equityCurve.length ? totalTurnover / equityCurve.length : 0,
    averageDailyTurnoverPct: equityCurve.length ? pct(totalTurnover / equityCurve.length) : 0,
    investedShare: equityCurve.length ? investedDays / equityCurve.length : 0,
    monthlyReturns: monthlyReturns(equityCurve),
    topAverageWeights: topAverageWeights(equityCurve),
    equityCurve,
  };
}

function policyScore(result) {
  return (result.sharpe || 0) + (result.totalReturn || 0) + (result.maxDrawdown || 0) * 0.5;
}

function topHoldingsFromPredictions(predictions, outputTickers, teacherWeights, k, mode, safeTicker) {
  const candidates = outputTickers
    .map((ticker, index) => ({
      ticker,
      score: finite(predictions[index]),
      teacherWeight: teacherWeights[ticker] || 0,
    }))
    .filter((row) => row.teacherWeight > 1e-8 && row.ticker !== safeTicker)
    .sort((left, right) => right.score - left.score)
    .slice(0, k);
  if (!candidates.length) return { [safeTicker]: 1 };
  if (mode === 'teacher') {
    const raw = {};
    candidates.forEach((row) => {
      raw[row.ticker] = row.teacherWeight;
    });
    return normalizeLongOnly(raw, candidates.map((row) => row.ticker), safeTicker);
  }
  const equalWeight = 1 / candidates.length;
  return Object.fromEntries(candidates.map((row) => [row.ticker, equalWeight]));
}

function fitRidgePolicy({
  kind,
  featureSet,
  lambda,
  context,
  fitSamples,
  outputTickers,
  targetKind,
  topK = null,
  topKMode = 'teacher',
  maxWeight = 0.5,
}) {
  const x = matrixFromSamples(fitSamples, context, featureSet);
  const y = targetKind === 'returns' ? targetReturnsMatrix(fitSamples, outputTickers) : targetWeightsMatrix(fitSamples, outputTickers);
  const { trainX, means, stds } = standardizeTrainAndApply(x, []);
  const beta = fitRidgeMulti(trainX, y, lambda);
  const transformRow = (row) => row.map((value, index) => (value - means[index]) / stds[index]);
  return {
    predictWeights(sample) {
      const row = transformRow(buildFeatureVector(sample, context, featureSet));
      const predictions = predictLinearMulti(beta, row);
      if (kind === 'topk') {
        return topHoldingsFromPredictions(predictions, outputTickers, sample.teacherWeights, topK, topKMode, context.safeTicker);
      }
      const raw = {};
      outputTickers.forEach((ticker, index) => {
        raw[ticker] = predictions[index];
      });
      return normalizeLongOnly(raw, outputTickers, context.safeTicker, maxWeight);
    },
  };
}

function fitGatePolicy({ featureSet, lambda, threshold, context, fitSamples }) {
  const x = matrixFromSamples(fitSamples, context, featureSet);
  const y = binaryGateLabels(fitSamples);
  const { trainX, means, stds } = standardizeTrainAndApply(x, []);
  const weights = fitLogisticBinary(trainX, y, { lambda });
  const transformRow = (row) => row.map((value, index) => (value - means[index]) / stds[index]);
  return {
    predictWeights(sample) {
      const row = transformRow(buildFeatureVector(sample, context, featureSet));
      const probability = predictLogistic(weights, row);
      return probability >= threshold ? sample.teacherWeights : { [context.safeTicker]: 1 };
    },
  };
}

function evaluateCandidate(candidate, context, trainSamples, validationSamples, outputTickers) {
  let policy;
  if (candidate.kind === 'gate') {
    policy = fitGatePolicy({ ...candidate, context, fitSamples: trainSamples });
  } else {
    policy = fitRidgePolicy({
      ...candidate,
      context,
      fitSamples: trainSamples,
      outputTickers,
      targetKind: candidate.kind === 'topk' ? 'returns' : 'weights',
    });
  }
  const validation = backtestPolicy(validationSamples, context.market, policy.predictWeights, context.backtest);
  return { candidate, validation, score: policyScore(validation) };
}

function selectAndTestExperiment(definition, context, splits, outputTickers) {
  const evaluated = definition.candidates.map((candidate) => evaluateCandidate(
    candidate,
    context,
    splits.train,
    splits.validation,
    outputTickers,
  ));
  evaluated.sort((left, right) => right.score - left.score);
  const selected = evaluated[0].candidate;
  let policy;
  if (selected.kind === 'gate') {
    policy = fitGatePolicy({ ...selected, context, fitSamples: splits.fit });
  } else {
    policy = fitRidgePolicy({
      ...selected,
      context,
      fitSamples: splits.fit,
      outputTickers,
      targetKind: selected.kind === 'topk' ? 'returns' : 'weights',
    });
  }
  return {
    id: definition.id,
    description: definition.description,
    selected,
    validation: evaluated[0].validation,
    validationCandidates: evaluated.map((row) => ({
      candidate: row.candidate,
      totalReturnPct: row.validation.totalReturnPct,
      sharpe: row.validation.sharpe,
      maxDrawdownPct: row.validation.maxDrawdownPct,
      score: row.score,
    })),
    test: backtestPolicy(splits.test, context.market, policy.predictWeights, context.backtest),
    fullPeriod: backtestPolicy(splits.fullPeriod, context.market, policy.predictWeights, context.backtest),
  };
}

function makeExperimentDefinitions({ useOptions }) {
  const lambdas = [0.1, 1, 10, 100];
  const gateLambdas = [0.01, 0.1, 1];
  const thresholds = [0.45, 0.5, 0.55, 0.6];
  const topKs = [3, 5, 8, 10];
  const definitions = [
    {
      id: 'imitate_price',
      description: 'Ridge imitation of PYM weights from causal EOD price features.',
      candidates: lambdas.map((lambda) => ({ kind: 'imitate', featureSet: ['price'], lambda, maxWeight: 0.5 })),
    },
    {
      id: 'imitate_attention',
      description: 'Ridge imitation using causal self-attention-style sequence features.',
      candidates: lambdas.map((lambda) => ({ kind: 'imitate', featureSet: ['attention'], lambda, maxWeight: 0.5 })),
    },
    {
      id: 'gate_price_pym',
      description: 'Logistic gate: hold PYM only when model expects positive next-session PYM return.',
      candidates: gateLambdas.flatMap((lambda) => thresholds.map((threshold) => ({
        kind: 'gate',
        featureSet: ['price', 'pym'],
        lambda,
        threshold,
      }))),
    },
    {
      id: 'topk_price_pym',
      description: 'Ridge return ranker: hold top current PYM candidates by predicted next-session return.',
      candidates: lambdas.flatMap((lambda) => topKs.map((topK) => ({
        kind: 'topk',
        featureSet: ['price', 'pym'],
        lambda,
        topK,
        topKMode: 'teacher',
      }))),
    },
    {
      id: 'topk_attention_pym',
      description: 'Attention-sequence return ranker over current PYM candidates.',
      candidates: lambdas.flatMap((lambda) => topKs.map((topK) => ({
        kind: 'topk',
        featureSet: ['attention', 'pym'],
        lambda,
        topK,
        topKMode: 'teacher',
      }))),
    },
  ];
  if (!useOptions) return definitions;
  return [
    ...definitions,
    {
      id: 'imitate_price_options',
      description: 'Ridge imitation from causal price plus same-day option-flow features.',
      candidates: lambdas.map((lambda) => ({ kind: 'imitate', featureSet: ['price', 'options'], lambda, maxWeight: 0.5 })),
    },
    {
      id: 'gate_price_pym_options',
      description: 'Logistic PYM/cash gate with price, PYM state, and option-flow features.',
      candidates: gateLambdas.flatMap((lambda) => thresholds.map((threshold) => ({
        kind: 'gate',
        featureSet: ['price', 'pym', 'options'],
        lambda,
        threshold,
      }))),
    },
    {
      id: 'gate_attention_pym_options',
      description: 'Logistic PYM/cash gate with causal attention-sequence and option-flow features.',
      candidates: gateLambdas.flatMap((lambda) => thresholds.map((threshold) => ({
        kind: 'gate',
        featureSet: ['attention', 'pym', 'options'],
        lambda,
        threshold,
      }))),
    },
    {
      id: 'topk_price_pym_options',
      description: 'Ridge return ranker with price, PYM state, and option-flow features.',
      candidates: lambdas.flatMap((lambda) => topKs.map((topK) => ({
        kind: 'topk',
        featureSet: ['price', 'pym', 'options'],
        lambda,
        topK,
        topKMode: 'teacher',
      }))),
    },
    {
      id: 'topk_attention_pym_options',
      description: 'Attention-sequence return ranker with option-flow features.',
      candidates: lambdas.flatMap((lambda) => topKs.map((topK) => ({
        kind: 'topk',
        featureSet: ['attention', 'pym', 'options'],
        lambda,
        topK,
        topKMode: 'teacher',
      }))),
    },
  ];
}

function loadInputs(options = {}) {
  const config = loadConfig();
  const scorePath = options.scorePath || defaultScorePath(config);
  const dailyBarsPath = options.dailyBarsPath || findLatestMassiveEodBarsPath();
  if (!scorePath || !fs.existsSync(scorePath)) throw new Error(`Missing Composer score snapshot: ${scorePath}`);
  if (!dailyBarsPath || !fs.existsSync(dailyBarsPath)) throw new Error('Missing Massive adjusted EOD daily bars.');
  return {
    config,
    scorePath,
    dailyBarsPath,
    score: JSON.parse(fs.readFileSync(scorePath, 'utf8')),
    market: readDailyBarsJsonl(dailyBarsPath),
  };
}

function runExperiments(options = {}) {
  const inputs = loadInputs(options);
  const lookback = options.lookback || DEFAULT_LOOKBACK;
  const optionFeaturesPath = options.useOptions === false ? null : (options.optionFeaturesPath || findLatestOptionFeaturesPath());
  const optionByDate = optionFeaturesPath ? readOptionFeatureMap(optionFeaturesPath) : new Map();
  const { samples, outputTickers } = buildSamples({
    market: inputs.market,
    score: inputs.score,
    rsiMode: options.rsiMode || DEFAULT_RSI_MODE,
    lookback,
    startDate: options.startDate || DEFAULT_START_DATE,
    endDate: options.endDate || null,
  });
  const splits = splitSamples(samples, {
    trainEnd: options.trainEnd || DEFAULT_TRAIN_END,
    validationStart: options.validationStart || DEFAULT_VALIDATION_START,
    validationEnd: options.validationEnd || DEFAULT_VALIDATION_END,
    testStart: options.testStart || DEFAULT_TEST_START,
    testEnd: options.testEnd || null,
  });
  splits.fullPeriod = samples.filter((sample) => (
    sample.date >= (options.fullPeriodStart || DEFAULT_START_DATE)
    && (!options.fullPeriodEnd || sample.date <= options.fullPeriodEnd)
  ));
  if (!splits.train.length || !splits.validation.length || !splits.test.length) {
    throw new Error(`Bad split sizes train=${splits.train.length} validation=${splits.validation.length} test=${splits.test.length}`);
  }
  const coreTickers = CORE_TICKERS.filter((ticker) => inputs.market.closes.has(ticker));
  const context = {
    market: inputs.market,
    lookback,
    coreTickers,
    outputTickers,
    optionByDate,
    optionRoots: OPTION_ROOTS,
    optionFields: OPTION_FIELDS,
    safeTicker: safeTickerForMarket(inputs.market),
    backtest: {
      initialCapital: options.initialCapital || inputs.config.execution?.initialCapital || DEFAULT_INITIAL_CAPITAL,
      totalCostBps: options.totalCostBps ?? ((inputs.config.execution?.transactionCostBps || 0) + (inputs.config.execution?.slippageBps || 0)),
    },
  };
  const baselineValidation = backtestPolicy(splits.validation, inputs.market, (sample) => sample.teacherWeights, context.backtest);
  const baselineTest = backtestPolicy(splits.test, inputs.market, (sample) => sample.teacherWeights, context.backtest);
  const baselineFullPeriod = backtestPolicy(splits.fullPeriod, inputs.market, (sample) => sample.teacherWeights, context.backtest);
  const featurePreview = buildFeatureVector(splits.train[0], context, ['price', 'attention', 'pym', ...(optionByDate.size ? ['options'] : [])], true);
  const definitions = makeExperimentDefinitions({ useOptions: Boolean(optionByDate.size) });
  const experiments = definitions.map((definition) => selectAndTestExperiment(definition, context, splits, outputTickers));
  const rankedByTest = experiments
    .map((experiment) => ({
      id: experiment.id,
      selected: experiment.selected,
      totalReturnPct: experiment.test.totalReturnPct,
      sharpe: experiment.test.sharpe,
      maxDrawdownPct: experiment.test.maxDrawdownPct,
      averageDailyTurnoverPct: experiment.test.averageDailyTurnoverPct,
      beatsBaselineReturn: experiment.test.totalReturn > baselineTest.totalReturn,
      excessReturnPct: pct(experiment.test.totalReturn - baselineTest.totalReturn),
    }))
    .sort((left, right) => right.totalReturnPct - left.totalReturnPct);
  const rankedByFullPeriod = experiments
    .map((experiment) => ({
      id: experiment.id,
      selected: experiment.selected,
      totalReturnPct: experiment.fullPeriod.totalReturnPct,
      sharpe: experiment.fullPeriod.sharpe,
      maxDrawdownPct: experiment.fullPeriod.maxDrawdownPct,
      averageDailyTurnoverPct: experiment.fullPeriod.averageDailyTurnoverPct,
      beatsBaselineReturn: experiment.fullPeriod.totalReturn > baselineFullPeriod.totalReturn,
      excessReturnPct: pct(experiment.fullPeriod.totalReturn - baselineFullPeriod.totalReturn),
    }))
    .sort((left, right) => right.totalReturnPct - left.totalReturnPct);
  return {
    generatedAt: new Date().toISOString(),
    source: {
      dailyBarsPath: inputs.dailyBarsPath,
      scorePath: inputs.scorePath,
      optionFeaturesPath,
      provider: 'Massive adjusted EOD plus Massive OPRA option aggregates when present',
    },
    settings: {
      startDate: options.startDate || DEFAULT_START_DATE,
      lookback,
      rsiMode: options.rsiMode || DEFAULT_RSI_MODE,
      timing: 'signal_eod_close_then_next_close',
      totalCostBps: context.backtest.totalCostBps,
      initialCapital: context.backtest.initialCapital,
      trainEnd: options.trainEnd || DEFAULT_TRAIN_END,
      validationStart: options.validationStart || DEFAULT_VALIDATION_START,
      validationEnd: options.validationEnd || DEFAULT_VALIDATION_END,
      testStart: options.testStart || DEFAULT_TEST_START,
      testEnd: options.testEnd || null,
      fullPeriodStart: options.fullPeriodStart || DEFAULT_START_DATE,
      fullPeriodEnd: options.fullPeriodEnd || null,
      fullPeriodNote: 'ML full-period numbers reuse the selected model refit on all samples before testStart, so dates before testStart are not a clean holdout.',
    },
    data: {
      marketStartDate: inputs.market.dates[0],
      marketEndDate: inputs.market.dates.at(-1),
      marketDays: inputs.market.dates.length,
      samples: samples.length,
      trainSamples: splits.train.length,
      validationSamples: splits.validation.length,
      fitSamples: splits.fit.length,
      testSamples: splits.test.length,
      fullPeriodSamples: splits.fullPeriod.length,
      coreTickers,
      outputTickers,
      featurePreviewCount: featurePreview.names.length,
      optionFeatureDates: optionByDate.size,
    },
    baseline: {
      id: 'pym_v5_base',
      validation: baselineValidation,
      test: baselineTest,
      fullPeriod: baselineFullPeriod,
    },
    experiments,
    rankedByTest,
    rankedByFullPeriod,
  };
}

function parseArgs(argv) {
  const out = { useOptions: true };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--option-features') out.optionFeaturesPath = argv[++index];
    else if (arg === '--no-options') out.useOptions = false;
    else if (arg === '--lookback') out.lookback = Number(argv[++index]);
    else if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--train-end') out.trainEnd = argv[++index];
    else if (arg === '--validation-start') out.validationStart = argv[++index];
    else if (arg === '--validation-end') out.validationEnd = argv[++index];
    else if (arg === '--test-start') out.testStart = argv[++index];
    else if (arg === '--test-end') out.testEnd = argv[++index];
    else if (arg === '--full-period-start') out.fullPeriodStart = argv[++index];
    else if (arg === '--full-period-end') out.fullPeriodEnd = argv[++index];
    else if (arg === '--out') out.outPath = argv[++index];
    else if (arg === '--canary') {
      out.startDate = '2025-01-02';
      out.trainEnd = '2025-01-21';
      out.validationStart = '2025-01-22';
      out.validationEnd = '2025-01-31';
      out.testStart = '2025-02-03';
      out.testEnd = '2025-02-14';
      out.lookback = 21;
      out.useOptions = false;
    }
  }
  return out;
}

function compactRankingsForConsole(baseline, rankings) {
  const rows = [
    {
      id: baseline.id,
      selected: 'deterministic',
      testReturnPct: baseline.result.totalReturnPct,
      testSharpe: baseline.result.sharpe,
      testMaxDdPct: baseline.result.maxDrawdownPct,
      excessReturnPct: 0,
    },
    ...rankings.map((row) => ({
      id: row.id,
      selected: JSON.stringify(row.selected),
      testReturnPct: row.totalReturnPct,
      testSharpe: row.sharpe,
      testMaxDdPct: row.maxDrawdownPct,
      excessReturnPct: row.excessReturnPct,
    })),
  ];
  return rows.map((row) => ({
    id: row.id,
    selected: row.selected.length > 64 ? `${row.selected.slice(0, 61)}...` : row.selected,
    testReturnPct: Number(row.testReturnPct?.toFixed(2)),
    testSharpe: Number(row.testSharpe?.toFixed(3)),
    testMaxDdPct: Number(row.testMaxDdPct?.toFixed(2)),
    excessReturnPct: Number(row.excessReturnPct?.toFixed(2)),
  }));
}

function runCli(argv) {
  const options = parseArgs(argv);
  const report = runExperiments(options);
  const endDate = report.data.marketEndDate;
  const outPath = options.outPath || artifactPath(`pym-v5-ml-experiments-${report.settings.testStart}-${endDate}.json`);
  ensureDir(path.dirname(outPath));
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({
    outputPath: outPath,
    marketEndDate: report.data.marketEndDate,
    trainSamples: report.data.trainSamples,
    validationSamples: report.data.validationSamples,
      testSamples: report.data.testSamples,
      baselineTestReturnPct: Number(report.baseline.test.totalReturnPct.toFixed(2)),
      baselineFullPeriodReturnPct: Number(report.baseline.fullPeriod.totalReturnPct.toFixed(2)),
      bestExperiment: report.rankedByTest[0],
      bestFullPeriodExperiment: report.rankedByFullPeriod[0],
      testRankings: compactRankingsForConsole(
        { id: report.baseline.id, result: report.baseline.test },
        report.rankedByTest,
      ),
      fullPeriodRankings: compactRankingsForConsole(
        { id: report.baseline.id, result: report.baseline.fullPeriod },
        report.rankedByFullPeriod,
      ),
    }, null, 2));
}

module.exports = {
  CORE_TICKERS,
  DEFAULT_LOOKBACK,
  DEFAULT_START_DATE,
  OPTION_FIELDS,
  OPTION_ROOTS,
  artifactPath,
  buildFeatureVector,
  buildSamples,
  splitSamples,
  fitRidgeMulti,
  predictLinearMulti,
  fitLogisticBinary,
  predictLogistic,
  normalizeLongOnly,
  backtestPolicy,
  loadInputs,
  readOptionFeatureMap,
  findLatestOptionFeaturesPath,
  safeTickerForMarket,
  runExperiments,
  runCli,
};
