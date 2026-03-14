#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const { queryRowsSync } = require('../../src/storage/clickhouse');

const DEFAULT_FROM_ISO = '2025-08-01';
const DEFAULT_TO_ISO = '2026-03-12';
const DEFAULT_TRAIN_FROM = '2025-08-01';
const DEFAULT_TRAIN_TO = '2025-12-31';
const DEFAULT_TEST_FROM = '2026-01-01';
const DEFAULT_MIN_TRADE_COUNT = 10;
const DEFAULT_MIN_TOTAL_SIZE = 300;
const DEFAULT_LOOKBACK_BARS = 20;
const DEFAULT_MIN_RISK_PCT = 0.0035;
const DEFAULT_ENTRY_DELAY_MINUTES = 1;
const DEFAULT_MIN_TEST_TRADES = 25;
const TARGET_R_LEVELS = [1, 2, 3, 4, 5];

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

function avg(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length === 0) return null;
  return filtered.reduce((sum, value) => sum + value, 0) / filtered.length;
}

function sum(values) {
  const filtered = values.filter((value) => Number.isFinite(value));
  if (filtered.length === 0) return null;
  return filtered.reduce((total, value) => total + value, 0);
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
  const w = idx - lo;
  return (filtered[lo] * (1 - w)) + (filtered[hi] * w);
}

function parseUtcToMs(value) {
  if (value === null || value === undefined) return Number.NaN;
  const normalized = String(value).trim().replace(' ', 'T');
  const iso = normalized.endsWith('Z') ? normalized : `${normalized}Z`;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : Number.NaN;
}

function toFinite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function computeExpectancyAtTarget(hitRate, targetR) {
  if (!Number.isFinite(hitRate)) return null;
  return (hitRate * targetR) - ((1 - hitRate) * 1);
}

function summarizeTrades(trades) {
  const count = trades.length;
  if (count === 0) {
    return {
      trades: 0,
      hit1Rate: null,
      hit2Rate: null,
      hit3Rate: null,
      hit4Rate: null,
      hit5Rate: null,
      avgMaxR: null,
      medianMaxR: null,
      p90MaxR: null,
      avgCloseOrStopR: null,
      medianCloseOrStopR: null,
      totalCloseOrStopR: null,
      winRate: null,
      avgWinR: null,
      avgLossR: null,
      expectancyAt4R: null,
      expectancyAt5R: null,
    };
  }

  const maxRs = trades.map((trade) => trade.maxR);
  const exitRs = trades.map((trade) => trade.closeOrStopR);
  const wins = exitRs.filter((value) => value > 0);
  const losses = exitRs.filter((value) => value < 0);
  const hitRate = (r) => trades.filter((trade) => trade.maxR >= r).length / count;

  return {
    trades: count,
    hit1Rate: hitRate(1),
    hit2Rate: hitRate(2),
    hit3Rate: hitRate(3),
    hit4Rate: hitRate(4),
    hit5Rate: hitRate(5),
    avgMaxR: avg(maxRs),
    medianMaxR: quantile(maxRs, 0.5),
    p90MaxR: quantile(maxRs, 0.9),
    avgCloseOrStopR: avg(exitRs),
    medianCloseOrStopR: quantile(exitRs, 0.5),
    totalCloseOrStopR: sum(exitRs),
    winRate: wins.length / count,
    avgWinR: avg(wins),
    avgLossR: avg(losses),
    expectancyAt4R: computeExpectancyAtTarget(hitRate(4), 4),
    expectancyAt5R: computeExpectancyAtTarget(hitRate(5), 5),
  };
}

function classifyStrategy(testSummary, monthlyTestSummaries, minTestTrades) {
  const minRobustTrades = Math.max(minTestTrades, 40);
  if (!testSummary || (testSummary.trades || 0) < minRobustTrades) return 'insufficient_test_sample';

  const eligibleMonths = monthlyTestSummaries.filter((row) => (row.summary?.trades ?? 0) >= 8);
  const positiveMonths = eligibleMonths.filter((row) => (row.summary?.avgCloseOrStopR ?? -999) > 0).length;
  const strongMonths = eligibleMonths.filter((row) => (row.summary?.hit4Rate ?? 0) >= 0.16).length;

  const has4RProfile = (testSummary.hit4Rate ?? 0) >= 0.2
    && (testSummary.expectancyAt4R ?? -999) > 0;
  const has5RProfile = (testSummary.hit5Rate ?? 0) >= 0.1667
    && (testSummary.expectancyAt5R ?? -999) > 0;
  const robustForward = positiveMonths >= 2 && strongMonths >= 2;

  if ((has4RProfile || has5RProfile) && robustForward) return 'qualified_4R';
  if (has4RProfile || has5RProfile) return 'fragile_4R';
  return 'watchlist';
}

function queryTradingDays(fromIso, toIso) {
  const rows = queryRowsSync(`
    SELECT toString(trade_date_utc) AS day_iso
    FROM options.option_symbol_minute_derived
    WHERE trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
    GROUP BY day_iso
    ORDER BY day_iso ASC
  `, { fromIso, toIso });
  return rows
    .map((row) => String(row.day_iso || '').trim())
    .filter((dayIso) => /^\d{4}-\d{2}-\d{2}$/.test(dayIso));
}

function queryThresholds({
  trainFrom,
  trainTo,
  minTradeCount,
  minTotalSize,
}) {
  const rows = queryRowsSync(`
    SELECT
      quantile(0.15)((toFloat64(call_size) - toFloat64(put_size)) / greatest(1.0, toFloat64(call_size + put_size))) AS net_call_flow_q15,
      quantile(0.20)((toFloat64(call_size) - toFloat64(put_size)) / greatest(1.0, toFloat64(call_size + put_size))) AS net_call_flow_q20,
      quantile(0.35)((toFloat64(call_size) - toFloat64(put_size)) / greatest(1.0, toFloat64(call_size + put_size))) AS net_call_flow_q35,
      quantile(0.65)((toFloat64(call_size) - toFloat64(put_size)) / greatest(1.0, toFloat64(call_size + put_size))) AS net_call_flow_q65,
      quantile(0.80)((toFloat64(call_size) - toFloat64(put_size)) / greatest(1.0, toFloat64(call_size + put_size))) AS net_call_flow_q80,
      quantile(0.85)((toFloat64(call_size) - toFloat64(put_size)) / greatest(1.0, toFloat64(call_size + put_size))) AS net_call_flow_q85,
      quantile(0.30)(avg_delta_pressure_norm) AS delta_pressure_q30,
      quantile(0.70)(avg_delta_pressure_norm) AS delta_pressure_q70,
      quantile(0.80)(avg_delta_pressure_norm) AS delta_pressure_q80,
      quantile(0.30)(avg_underlying_trend_confirm_norm) AS trend_confirm_q30,
      quantile(0.70)(avg_underlying_trend_confirm_norm) AS trend_confirm_q70,
      quantile(0.30)(avg_flow_imbalance_norm) AS flow_imbalance_q30,
      quantile(0.70)(avg_flow_imbalance_norm) AS flow_imbalance_q70,
      quantile(0.70)(avg_iv) AS avg_iv_q70,
      quantile(0.70)(iv_spread) AS iv_spread_q70,
      quantile(0.80)(iv_spread) AS iv_spread_q80,
      quantile(0.80)(toFloat64(sweep_count) / greatest(1.0, toFloat64(trade_count))) AS sweep_share_q80,
      quantile(0.90)(toFloat64(sweep_count) / greatest(1.0, toFloat64(trade_count))) AS sweep_share_q90,
      quantile(0.70)(sweep_value_ratio) AS sweep_value_ratio_q70,
      quantile(0.60)(toFloat64(trade_count)) AS trade_count_q60,
      quantile(0.30)(avg_sig_score) AS avg_sig_score_q30,
      quantile(0.70)(avg_sig_score) AS avg_sig_score_q70,
      quantile(0.25)(net_sig_score) AS net_sig_score_q25,
      quantile(0.75)(net_sig_score) AS net_sig_score_q75,
      quantile(0.30)(toFloat64(bullish_count) / greatest(1.0, toFloat64(trade_count))) AS bullish_share_q30,
      quantile(0.70)(toFloat64(bullish_count) / greatest(1.0, toFloat64(trade_count))) AS bullish_share_q70,
      quantile(0.40)(multileg_pct) AS multileg_pct_q40,
      quantile(0.60)(avg_liquidity_quality_norm) AS liquidity_q60,
      quantile(0.70)(avg_liquidity_quality_norm) AS liquidity_q70
    FROM options.option_symbol_minute_derived
    WHERE trade_date_utc BETWEEN toDate({trainFrom:String}) AND toDate({trainTo:String})
      AND trade_count >= {minTradeCount:UInt32}
      AND (call_size + put_size) >= {minTotalSize:UInt64}
      AND ((toHour(minute_bucket_utc) * 60) + toMinute(minute_bucket_utc)) BETWEEN 570 AND 959
  `, {
    trainFrom,
    trainTo,
    minTradeCount,
    minTotalSize,
  });
  const first = rows[0] || {};
  const out = {};
  Object.entries(first).forEach(([key, value]) => {
    out[key] = toFinite(value);
  });
  return out;
}

function queryStockRows(dayIso) {
  return queryRowsSync(`
    SELECT
      symbol,
      toString(minute_bucket_utc) AS minute_utc,
      ifNull(open, close) AS open,
      ifNull(high, close) AS high,
      ifNull(low, close) AS low,
      close
    FROM options.stock_ohlc_minute_raw
    WHERE trade_date_utc = toDate({dayIso:String})
      AND ((toHour(minute_bucket_utc) * 60) + toMinute(minute_bucket_utc)) BETWEEN 570 AND 959
    ORDER BY symbol ASC, minute_bucket_utc ASC
  `, { dayIso });
}

function queryOptionFeatureRows(dayIso, minTradeCount, minTotalSize) {
  return queryRowsSync(`
    SELECT
      symbol,
      toString(minute_bucket_utc) AS minute_utc,
      trade_count,
      total_value,
      call_size,
      put_size,
      bullish_count,
      bearish_count,
      sweep_count,
      sweep_value_ratio,
      multileg_pct,
      avg_sig_score,
      net_sig_score,
      avg_iv,
      iv_spread,
      avg_flow_imbalance_norm,
      avg_delta_pressure_norm,
      avg_underlying_trend_confirm_norm,
      avg_liquidity_quality_norm
    FROM options.option_symbol_minute_derived
    WHERE trade_date_utc = toDate({dayIso:String})
      AND trade_count >= {minTradeCount:UInt32}
      AND (call_size + put_size) >= {minTotalSize:UInt64}
      AND ((toHour(minute_bucket_utc) * 60) + toMinute(minute_bucket_utc)) BETWEEN 570 AND 959
    ORDER BY symbol ASC, minute_bucket_utc ASC
  `, {
    dayIso,
    minTradeCount,
    minTotalSize,
  });
}

function buildStockMap(stockRows) {
  const bySymbol = new Map();
  stockRows.forEach((row) => {
    const symbol = String(row.symbol || '').trim().toUpperCase();
    const tsMs = parseUtcToMs(row.minute_utc);
    const open = toFinite(row.open);
    const high = toFinite(row.high);
    const low = toFinite(row.low);
    const close = toFinite(row.close);
    if (!symbol || !Number.isFinite(tsMs)) return;
    if (!Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) return;
    if (!bySymbol.has(symbol)) {
      bySymbol.set(symbol, { rows: [], idxByTs: new Map() });
    }
    const meta = bySymbol.get(symbol);
    const idx = meta.rows.length;
    meta.rows.push({
      tsMs,
      open,
      high,
      low,
      close,
    });
    meta.idxByTs.set(tsMs, idx);
  });
  return bySymbol;
}

function buildEventMap(optionRows, stockMap) {
  const bySymbol = new Map();
  optionRows.forEach((row) => {
    const symbol = String(row.symbol || '').trim().toUpperCase();
    const stockMeta = stockMap.get(symbol);
    if (!stockMeta) return;

    const tsMs = parseUtcToMs(row.minute_utc);
    if (!Number.isFinite(tsMs)) return;
    if (!stockMeta.idxByTs.has(tsMs)) return;

    const tradeCount = toFinite(row.trade_count) || 0;
    const totalValue = toFinite(row.total_value) || 0;
    const callSize = toFinite(row.call_size) || 0;
    const putSize = toFinite(row.put_size) || 0;
    const totalSize = callSize + putSize;
    const bullishCount = toFinite(row.bullish_count) || 0;
    const bearishCount = toFinite(row.bearish_count) || 0;
    const sweepCount = toFinite(row.sweep_count) || 0;

    if (!bySymbol.has(symbol)) bySymbol.set(symbol, []);
    bySymbol.get(symbol).push({
      symbol,
      tsMs,
      tradeCount,
      totalValue,
      callSize,
      putSize,
      totalSize,
      bullishCount,
      bearishCount,
      netCallFlow: (callSize - putSize) / Math.max(1, totalSize),
      bullishShare: bullishCount / Math.max(1, tradeCount),
      sweepTradeShare: sweepCount / Math.max(1, tradeCount),
      valuePerTrade: totalValue / Math.max(1, tradeCount),
      sweepValueRatio: toFinite(row.sweep_value_ratio),
      multilegPct: toFinite(row.multileg_pct),
      avgSigScore: toFinite(row.avg_sig_score),
      netSigScore: toFinite(row.net_sig_score),
      avgIv: toFinite(row.avg_iv),
      ivSpread: toFinite(row.iv_spread),
      flowImbalanceNorm: toFinite(row.avg_flow_imbalance_norm),
      deltaPressureNorm: toFinite(row.avg_delta_pressure_norm),
      trendConfirmNorm: toFinite(row.avg_underlying_trend_confirm_norm),
      liquidityQualityNorm: toFinite(row.avg_liquidity_quality_norm),
    });
  });
  return bySymbol;
}

function allFinite(values = []) {
  return values.every((value) => Number.isFinite(value));
}

function buildStrategies(thresholds) {
  const t = thresholds;

  return [
    {
      id: 'bull_flow_trend',
      label: 'Bull Flow + Trend Continuation',
      direction: 'long',
      signal: (row) => allFinite([
        row.netCallFlow,
        row.deltaPressureNorm,
        row.trendConfirmNorm,
        row.tradeCount,
      ]) && row.netCallFlow >= t.net_call_flow_q80
        && row.deltaPressureNorm >= t.delta_pressure_q70
        && row.trendConfirmNorm >= t.trend_confirm_q70
        && row.tradeCount >= t.trade_count_q60,
    },
    {
      id: 'bull_sweep_conviction',
      label: 'Bull Sweep Conviction',
      direction: 'long',
      signal: (row) => allFinite([
        row.sweepTradeShare,
        row.sweepValueRatio,
        row.netSigScore,
        row.liquidityQualityNorm,
      ]) && row.sweepTradeShare >= t.sweep_share_q80
        && row.sweepValueRatio >= t.sweep_value_ratio_q70
        && row.netSigScore >= t.net_sig_score_q75
        && row.liquidityQualityNorm >= t.liquidity_q60,
    },
    {
      id: 'bull_iv_breakout',
      label: 'Bull IV Breakout',
      direction: 'long',
      signal: (row) => allFinite([
        row.ivSpread,
        row.avgIv,
        row.flowImbalanceNorm,
        row.netCallFlow,
      ]) && row.ivSpread >= t.iv_spread_q80
        && row.avgIv >= t.avg_iv_q70
        && row.flowImbalanceNorm >= t.flow_imbalance_q70
        && row.netCallFlow >= t.net_call_flow_q65,
    },
    {
      id: 'bull_quality_follow',
      label: 'Bull Quality Follow-Through',
      direction: 'long',
      signal: (row) => allFinite([
        row.avgSigScore,
        row.bullishShare,
        row.multilegPct,
        row.liquidityQualityNorm,
      ]) && row.avgSigScore >= t.avg_sig_score_q70
        && row.bullishShare >= t.bullish_share_q70
        && row.multilegPct <= t.multileg_pct_q40
        && row.liquidityQualityNorm >= t.liquidity_q70,
    },
    {
      id: 'bull_absorption_reversal',
      label: 'Bull Absorption Reversal',
      direction: 'long',
      signal: (row) => allFinite([
        row.netCallFlow,
        row.deltaPressureNorm,
        row.trendConfirmNorm,
        row.ivSpread,
      ]) && row.netCallFlow <= t.net_call_flow_q20
        && row.deltaPressureNorm <= t.delta_pressure_q30
        && row.trendConfirmNorm >= t.trend_confirm_q70
        && row.ivSpread >= t.iv_spread_q70,
    },
    {
      id: 'bear_flow_trend',
      label: 'Bear Flow + Trend Continuation',
      direction: 'short',
      signal: (row) => allFinite([
        row.netCallFlow,
        row.deltaPressureNorm,
        row.trendConfirmNorm,
        row.tradeCount,
      ]) && row.netCallFlow <= t.net_call_flow_q20
        && row.deltaPressureNorm <= t.delta_pressure_q30
        && row.trendConfirmNorm <= t.trend_confirm_q30
        && row.tradeCount >= t.trade_count_q60,
    },
    {
      id: 'bear_sweep_conviction',
      label: 'Bear Sweep Conviction',
      direction: 'short',
      signal: (row) => allFinite([
        row.sweepTradeShare,
        row.sweepValueRatio,
        row.netSigScore,
        row.liquidityQualityNorm,
      ]) && row.sweepTradeShare >= t.sweep_share_q80
        && row.sweepValueRatio >= t.sweep_value_ratio_q70
        && row.netSigScore <= t.net_sig_score_q25
        && row.liquidityQualityNorm >= t.liquidity_q60,
    },
    {
      id: 'bear_iv_breakout',
      label: 'Bear IV Breakout',
      direction: 'short',
      signal: (row) => allFinite([
        row.ivSpread,
        row.avgIv,
        row.flowImbalanceNorm,
        row.netCallFlow,
      ]) && row.ivSpread >= t.iv_spread_q80
        && row.avgIv >= t.avg_iv_q70
        && row.flowImbalanceNorm <= t.flow_imbalance_q30
        && row.netCallFlow <= t.net_call_flow_q35,
    },
    {
      id: 'bear_quality_follow',
      label: 'Bear Quality Follow-Through',
      direction: 'short',
      signal: (row) => allFinite([
        row.avgSigScore,
        row.bullishShare,
        row.multilegPct,
        row.liquidityQualityNorm,
      ]) && row.avgSigScore <= t.avg_sig_score_q30
        && row.bullishShare <= t.bullish_share_q30
        && row.multilegPct <= t.multileg_pct_q40
        && row.liquidityQualityNorm >= t.liquidity_q70,
    },
    {
      id: 'bear_quality_follow_flow',
      label: 'Bear Quality Follow-Through + Bear Flow',
      direction: 'short',
      signal: (row) => allFinite([
        row.avgSigScore,
        row.bullishShare,
        row.multilegPct,
        row.liquidityQualityNorm,
        row.netCallFlow,
      ]) && row.avgSigScore <= t.avg_sig_score_q30
        && row.bullishShare <= t.bullish_share_q30
        && row.multilegPct <= t.multileg_pct_q40
        && row.liquidityQualityNorm >= t.liquidity_q70
        && row.netCallFlow <= t.net_call_flow_q20,
    },
    {
      id: 'bear_quality_follow_delta',
      label: 'Bear Quality Follow-Through + Delta Pressure',
      direction: 'short',
      signal: (row) => allFinite([
        row.avgSigScore,
        row.bullishShare,
        row.multilegPct,
        row.liquidityQualityNorm,
        row.deltaPressureNorm,
      ]) && row.avgSigScore <= t.avg_sig_score_q30
        && row.bullishShare <= t.bullish_share_q30
        && row.multilegPct <= t.multileg_pct_q40
        && row.liquidityQualityNorm >= t.liquidity_q70
        && row.deltaPressureNorm <= t.delta_pressure_q30,
    },
    {
      id: 'bear_quality_follow_flow_delta',
      label: 'Bear Quality Follow-Through + Flow + Delta',
      direction: 'short',
      signal: (row) => allFinite([
        row.avgSigScore,
        row.bullishShare,
        row.multilegPct,
        row.liquidityQualityNorm,
        row.netCallFlow,
        row.deltaPressureNorm,
      ]) && row.avgSigScore <= t.avg_sig_score_q30
        && row.bullishShare <= t.bullish_share_q30
        && row.multilegPct <= t.multileg_pct_q40
        && row.liquidityQualityNorm >= t.liquidity_q70
        && row.netCallFlow <= t.net_call_flow_q20
        && row.deltaPressureNorm <= t.delta_pressure_q30,
    },
    {
      id: 'bear_absorption_reversal',
      label: 'Bear Absorption Reversal',
      direction: 'short',
      signal: (row) => allFinite([
        row.netCallFlow,
        row.deltaPressureNorm,
        row.trendConfirmNorm,
        row.ivSpread,
      ]) && row.netCallFlow >= t.net_call_flow_q80
        && row.deltaPressureNorm >= t.delta_pressure_q70
        && row.trendConfirmNorm <= t.trend_confirm_q30
        && row.ivSpread >= t.iv_spread_q70,
    },
  ];
}

function computeTrade({
  strategy,
  event,
  dayIso,
  stockMeta,
  lookbackBars,
  minRiskPct,
  entryDelayMinutes,
}) {
  const entryBaseIdx = stockMeta.idxByTs.get(event.tsMs);
  if (!Number.isInteger(entryBaseIdx)) return null;
  const entryIdx = entryBaseIdx + entryDelayMinutes;
  if (entryIdx >= stockMeta.rows.length) return null;

  const rows = stockMeta.rows;
  const entryBar = rows[entryIdx];
  const entry = entryBar.open;
  if (!Number.isFinite(entry) || entry <= 0) return null;

  const lbStart = Math.max(0, entryIdx - lookbackBars);
  const lbSlice = rows.slice(lbStart, entryIdx);
  if (lbSlice.length === 0) return null;

  const direction = strategy.direction;
  const stopReference = direction === 'long'
    ? lbSlice.reduce((minLow, row) => Math.min(minLow, row.low), Number.POSITIVE_INFINITY)
    : lbSlice.reduce((maxHigh, row) => Math.max(maxHigh, row.high), Number.NEGATIVE_INFINITY);
  if (!Number.isFinite(stopReference)) return null;

  const rawRisk = direction === 'long'
    ? (entry - stopReference)
    : (stopReference - entry);
  const minRisk = entry * minRiskPct;
  const risk = Math.max(rawRisk, minRisk);
  if (!Number.isFinite(risk) || risk <= 0) return null;

  const stop = direction === 'long' ? (entry - risk) : (entry + risk);
  let stopHit = false;
  let stopHitTsMs = null;
  let maxR = 0;
  const firstHitR = {};

  TARGET_R_LEVELS.forEach((r) => {
    firstHitR[r] = null;
  });

  for (let i = entryIdx; i < rows.length; i += 1) {
    const row = rows[i];
    if (direction === 'long') {
      if (row.low <= stop) {
        stopHit = true;
        stopHitTsMs = row.tsMs;
        break;
      }
      maxR = Math.max(maxR, (row.high - entry) / risk);
      TARGET_R_LEVELS.forEach((r) => {
        if (firstHitR[r]) return;
        const threshold = entry + (r * risk);
        if (row.high >= threshold) {
          firstHitR[r] = new Date(row.tsMs).toISOString();
        }
      });
      continue;
    }

    if (row.high >= stop) {
      stopHit = true;
      stopHitTsMs = row.tsMs;
      break;
    }
    maxR = Math.max(maxR, (entry - row.low) / risk);
    TARGET_R_LEVELS.forEach((r) => {
      if (firstHitR[r]) return;
      const threshold = entry - (r * risk);
      if (row.low <= threshold) {
        firstHitR[r] = new Date(row.tsMs).toISOString();
      }
    });
  }

  const close = rows[rows.length - 1].close;
  const closeR = direction === 'long'
    ? (close - entry) / risk
    : (entry - close) / risk;
  const closeOrStopR = stopHit ? -1 : closeR;

  return {
    strategyId: strategy.id,
    strategyLabel: strategy.label,
    direction,
    dayEt: dayIso,
    symbol: event.symbol,
    signalTsUtc: new Date(event.tsMs).toISOString(),
    entryTsUtc: new Date(rows[entryIdx].tsMs).toISOString(),
    stopHitTsUtc: stopHitTsMs ? new Date(stopHitTsMs).toISOString() : null,
    entryPrice: entry,
    stopPrice: stop,
    risk,
    riskPct: (risk / entry) * 100,
    maxR,
    closeR,
    closeOrStopR,
    hit4: maxR >= 4,
    hit5: maxR >= 5,
    firstHitR,
    signalFeatures: {
      tradeCount: event.tradeCount,
      totalValue: event.totalValue,
      netCallFlow: event.netCallFlow,
      bullishShare: event.bullishShare,
      sweepTradeShare: event.sweepTradeShare,
      sweepValueRatio: event.sweepValueRatio,
      multilegPct: event.multilegPct,
      avgSigScore: event.avgSigScore,
      netSigScore: event.netSigScore,
      avgIv: event.avgIv,
      ivSpread: event.ivSpread,
      flowImbalanceNorm: event.flowImbalanceNorm,
      deltaPressureNorm: event.deltaPressureNorm,
      trendConfirmNorm: event.trendConfirmNorm,
      liquidityQualityNorm: event.liquidityQualityNorm,
    },
  };
}

function monthFromDayIso(dayIso) {
  return String(dayIso || '').slice(0, 7);
}

function splitByDate(trades, fromIso, toIso) {
  return trades.filter((trade) => trade.dayEt >= fromIso && trade.dayEt <= toIso);
}

function summarizeByMonth(trades) {
  const byMonth = new Map();
  trades.forEach((trade) => {
    const month = monthFromDayIso(trade.dayEt);
    if (!month) return;
    if (!byMonth.has(month)) byMonth.set(month, []);
    byMonth.get(month).push(trade);
  });
  return Array.from(byMonth.entries())
    .map(([month, rows]) => ({
      month,
      summary: summarizeTrades(rows),
    }))
    .sort((a, b) => a.month.localeCompare(b.month));
}

function run() {
  const args = parseArgs(process.argv);
  const fromIso = parseDateIso(args.from || process.env.OPT_RR_FROM, DEFAULT_FROM_ISO);
  const toIso = parseDateIso(args.to || process.env.OPT_RR_TO, DEFAULT_TO_ISO);
  const trainFrom = parseDateIso(args.trainFrom || process.env.OPT_RR_TRAIN_FROM, DEFAULT_TRAIN_FROM);
  const trainTo = parseDateIso(args.trainTo || process.env.OPT_RR_TRAIN_TO, DEFAULT_TRAIN_TO);
  const testFrom = parseDateIso(args.testFrom || process.env.OPT_RR_TEST_FROM, DEFAULT_TEST_FROM);
  const minTradeCount = Math.max(1, Math.trunc(parseNumber(
    args.minTradeCount || process.env.OPT_RR_MIN_TRADE_COUNT,
    DEFAULT_MIN_TRADE_COUNT,
  )));
  const minTotalSize = Math.max(1, Math.trunc(parseNumber(
    args.minTotalSize || process.env.OPT_RR_MIN_TOTAL_SIZE,
    DEFAULT_MIN_TOTAL_SIZE,
  )));
  const lookbackBars = Math.max(5, Math.trunc(parseNumber(
    args.lookbackBars || process.env.OPT_RR_LOOKBACK_BARS,
    DEFAULT_LOOKBACK_BARS,
  )));
  const minRiskPct = Math.max(0.0005, parseNumber(
    args.minRiskPct || process.env.OPT_RR_MIN_RISK_PCT,
    DEFAULT_MIN_RISK_PCT,
  ));
  const entryDelayMinutes = Math.max(1, Math.trunc(parseNumber(
    args.entryDelayMinutes || process.env.OPT_RR_ENTRY_DELAY_MINUTES,
    DEFAULT_ENTRY_DELAY_MINUTES,
  )));
  const minTestTrades = Math.max(5, Math.trunc(parseNumber(
    args.minTestTrades || process.env.OPT_RR_MIN_TEST_TRADES,
    DEFAULT_MIN_TEST_TRADES,
  )));
  const includeTrades = ['1', 'true', 'yes'].includes(String(
    args.includeTrades || process.env.OPT_RR_INCLUDE_TRADES || '0',
  ).trim().toLowerCase());

  const thresholds = queryThresholds({
    trainFrom,
    trainTo,
    minTradeCount,
    minTotalSize,
  });
  const strategies = buildStrategies(thresholds);
  const days = queryTradingDays(fromIso, toIso);
  if (days.length === 0) {
    throw new Error(`no_trading_days_in_range:${fromIso}:${toIso}`);
  }

  const tradesByStrategy = new Map();
  strategies.forEach((strategy) => {
    tradesByStrategy.set(strategy.id, []);
  });

  const progress = {
    tradingDays: days.length,
    processedDays: 0,
    dayErrors: [],
  };

  days.forEach((dayIso, idx) => {
    try {
      const stockRows = queryStockRows(dayIso);
      const optionRows = queryOptionFeatureRows(dayIso, minTradeCount, minTotalSize);
      const stockMap = buildStockMap(stockRows);
      const eventMap = buildEventMap(optionRows, stockMap);
      const symbols = Array.from(eventMap.keys());

      strategies.forEach((strategy) => {
        symbols.forEach((symbol) => {
          const events = eventMap.get(symbol) || [];
          const stockMeta = stockMap.get(symbol);
          if (!stockMeta || events.length === 0) return;
          for (let i = 0; i < events.length; i += 1) {
            const event = events[i];
            if (!strategy.signal(event)) continue;
            const trade = computeTrade({
              strategy,
              event,
              dayIso,
              stockMeta,
              lookbackBars,
              minRiskPct,
              entryDelayMinutes,
            });
            if (!trade) continue;
            tradesByStrategy.get(strategy.id).push(trade);
            break;
          }
        });
      });

      progress.processedDays += 1;
      if ((idx + 1) % 10 === 0 || (idx + 1) === days.length) {
        console.log(`[progress] processed days ${idx + 1}/${days.length} (${dayIso})`);
      }
    } catch (error) {
      progress.dayErrors.push({
        dayIso,
        error: error?.message || String(error),
      });
    }
  });

  const strategyResults = strategies.map((strategy) => {
    const allTrades = tradesByStrategy.get(strategy.id) || [];
    const trainTrades = splitByDate(allTrades, trainFrom, trainTo);
    const testTrades = splitByDate(allTrades, testFrom, toIso);
    const overallSummary = summarizeTrades(allTrades);
    const trainSummary = summarizeTrades(trainTrades);
    const testSummary = summarizeTrades(testTrades);
    const monthlyTestSummaries = summarizeByMonth(testTrades);

    const classification = classifyStrategy(testSummary, monthlyTestSummaries, minTestTrades);
    const robustScore = (
      ((testSummary.hit4Rate ?? 0) * 2.5)
      + ((testSummary.avgCloseOrStopR ?? 0) * 1.2)
      + ((testSummary.expectancyAt4R ?? 0) * 0.6)
      + (monthlyTestSummaries.filter((row) => (row.summary.avgCloseOrStopR ?? -999) > 0).length * 0.1)
    );

    return {
      strategyId: strategy.id,
      strategyLabel: strategy.label,
      direction: strategy.direction,
      classification,
      robustScore,
      thresholdsUsed: thresholds,
      overall: overallSummary,
      train: trainSummary,
      test: testSummary,
      monthlyForwardTest: monthlyTestSummaries,
      sampleTrades: allTrades.slice(0, 10),
      ...(includeTrades ? { trades: allTrades } : {}),
    };
  });

  strategyResults.sort((a, b) => {
    if (b.robustScore !== a.robustScore) return b.robustScore - a.robustScore;
    return (b.test.hit4Rate ?? -1) - (a.test.hit4Rate ?? -1);
  });

  const qualified = strategyResults.filter((row) => row.classification === 'qualified_4R');
  const fragile = strategyResults.filter((row) => row.classification === 'fragile_4R');

  const report = {
    generatedAt: nowIso(),
    config: {
      fromIso,
      toIso,
      trainFrom,
      trainTo,
      testFrom,
      minTradeCount,
      minTotalSize,
      lookbackBars,
      minRiskPct,
      entryDelayMinutes,
      minTestTrades,
      targetRLevels: TARGET_R_LEVELS,
      oneTradePerSymbolPerStrategyPerDay: true,
      stopRule: 'trailing-lookback extreme with min risk floor',
      fillRule: 'if stop and target touch in same minute, stop assumed first',
    },
    thresholds,
    progress,
    summary: {
      strategyCount: strategyResults.length,
      qualified4RCount: qualified.length,
      fragile4RCount: fragile.length,
      bestStrategyId: strategyResults[0]?.strategyId || null,
      bestStrategyLabel: strategyResults[0]?.strategyLabel || null,
    },
    topStrategies: strategyResults.slice(0, 5),
    allStrategies: strategyResults,
  };

  const outputPath = path.resolve(
    args.outputPath
      || process.env.OPT_RR_OUTPUT_PATH
      || path.join(
        process.cwd(),
        'artifacts',
        'strategy_research',
        `option-rr-strategy-research-${fromIso}-${toIso}-${nowIso().replace(/[:.]/g, '').replace(/-/g, '')}.json`,
      ),
  );
  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  const latestPath = path.resolve(
    process.cwd(),
    'artifacts',
    'strategy_research',
    'option-rr-strategy-research-latest.json',
  );
  fs.writeFileSync(latestPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({
    outputPath,
    latestPath,
    summary: report.summary,
    topStrategies: report.topStrategies.map((row) => ({
      strategyId: row.strategyId,
      strategyLabel: row.strategyLabel,
      classification: row.classification,
      robustScore: row.robustScore,
      test: row.test,
    })),
    dayErrors: progress.dayErrors.slice(0, 20),
  }, null, 2));
}

try {
  run();
} catch (error) {
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
}
