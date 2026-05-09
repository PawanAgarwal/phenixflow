#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs, asNumber } = require('../src/cli');

const DEFAULT_DATASET = path.join(
  PROJECT_ROOT,
  'runtime',
  'spy-intraday-dataset-2025-01-02-2026-04-27-with-option-features.jsonl',
);

const SOURCES = Object.freeze([
  {
    id: 'reddit_orb_0dte_spy',
    platform: 'reddit',
    url: 'https://www.reddit.com/r/options/comments/1rkx5vr/0dte_opening_range_breakout_strategy_on_spy_full/',
    note: '5-minute SPY opening range breakout, first breakout only, Monday/Wednesday/Friday, no overnight.',
  },
  {
    id: 'reddit_ema_0dte_spy',
    platform: 'reddit',
    url: 'https://www.reddit.com/r/algotrading/comments/1t59fsz/my_0dte_spy_backtesting/',
    note: '5-minute SPY 9/21 EMA bullish crossover before 13:00 ET, one trade per day.',
  },
  {
    id: 'github_ai_trader_vwap_momentum',
    platform: 'github',
    url: 'https://github.com/aaryansinha16/AI-trader',
    note: 'VWAP, RSI, EMA trend, and volume-spike confluence for momentum entries.',
  },
  {
    id: 'reddit_rsi_bollinger_sweep',
    platform: 'reddit',
    url: 'https://www.reddit.com/r/Daytrading/comments/1pt0eas/backtested_rsi_bollinger_bands_strategy_across/',
    note: 'RSI plus Bollinger Band mean reversion entries and middle-band/RSI normalization exits.',
  },
  {
    id: 'github_bollinger_mean_reversion',
    platform: 'github',
    url: 'https://github.com/coasensi/bollingerbands-backtest',
    note: 'Bollinger Band mean-reversion framing.',
  },
]);

const STRATEGIES = Object.freeze([
  {
    id: 'orb5_mwf_range_2r',
    sourceIds: ['reddit_orb_0dte_spy'],
    description: 'SPY underlying proxy for the 5-minute 0DTE ORB: M/W/F only, first completed 1-minute breakout after 09:35 ET, 2R target, 1R stop, 15:30 ET time stop.',
  },
  {
    id: 'ema9_21_5m_long_60m',
    sourceIds: ['reddit_ema_0dte_spy'],
    description: '5-minute 9 EMA over 21 EMA bullish crossover before 13:00 ET, long only, one trade per day, 60-minute hold.',
  },
  {
    id: 'vwap_ema_rsi_volume_5m_40m',
    sourceIds: ['github_ai_trader_vwap_momentum'],
    description: '5-minute VWAP/RSI/EMA20-EMA50/volume-spike confluence; long or short if at least 3 of 4 conditions agree, one trade per day, 40-minute hold.',
  },
  {
    id: 'rsi_bollinger_5m_reversion',
    sourceIds: ['reddit_rsi_bollinger_sweep', 'github_bollinger_mean_reversion'],
    description: '5-minute RSI(14) and Bollinger(20,2) mean reversion; enter on band cross plus RSI extreme, exit at middle band or RSI neutral, no overnight.',
  },
]);

function round(value, digits = 6) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function asPct(value) {
  return Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : 'n/a';
}

function weekdayEt(dayIso) {
  const date = new Date(`${dayIso}T12:00:00.000Z`);
  return date.getUTCDay();
}

function minuteLabel(minuteOfDayEt) {
  const hour = Math.floor(minuteOfDayEt / 60);
  const minute = minuteOfDayEt % 60;
  return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
}

function safeReturn(exitPrice, entryPrice, direction) {
  if (!(entryPrice > 0) || !(exitPrice > 0) || !direction) return 0;
  return direction * ((exitPrice / entryPrice) - 1);
}

function drawdownFromDaily(dailyReturns) {
  let equity = 1;
  let peak = 1;
  let maxDrawdown = 0;
  dailyReturns.forEach((dailyReturn) => {
    equity *= (1 + dailyReturn);
    peak = Math.max(peak, equity);
    maxDrawdown = Math.min(maxDrawdown, (equity / peak) - 1);
  });
  return maxDrawdown;
}

function summarizeReturns(values) {
  if (!values.length) {
    return {
      count: 0,
      totalReturn: 0,
      averageReturn: 0,
      winRate: 0,
      profitFactor: null,
      maxDrawdown: 0,
    };
  }
  let equity = 1;
  let grossWins = 0;
  let grossLosses = 0;
  let wins = 0;
  values.forEach((value) => {
    equity *= (1 + value);
    if (value > 0) {
      wins += 1;
      grossWins += value;
    } else if (value < 0) {
      grossLosses += Math.abs(value);
    }
  });
  return {
    count: values.length,
    totalReturn: equity - 1,
    averageReturn: values.reduce((sum, value) => sum + value, 0) / values.length,
    winRate: wins / values.length,
    profitFactor: grossLosses > 0 ? grossWins / grossLosses : null,
    maxDrawdown: drawdownFromDaily(values),
  };
}

function aggregateBars(rows, intervalMinutes) {
  const buckets = [];
  let current = null;
  rows.forEach((row, rowIndex) => {
    const offset = row.minuteOfDayEt - 570;
    if (offset < 0) return;
    const bucketStart = 570 + Math.floor(offset / intervalMinutes) * intervalMinutes;
    if (!current || current.startMinuteOfDayEt !== bucketStart) {
      if (current) buckets.push(current);
      current = {
        startMinuteOfDayEt: bucketStart,
        endMinuteOfDayEt: bucketStart + intervalMinutes - 1,
        startRowIndex: rowIndex,
        endRowIndex: rowIndex,
        open: row.spy_open,
        high: row.spy_high,
        low: row.spy_low,
        close: row.spy_close,
        volume: row.spy_volume || 0,
        rows: [row],
      };
      return;
    }
    current.endRowIndex = rowIndex;
    current.high = Math.max(current.high, row.spy_high);
    current.low = Math.min(current.low, row.spy_low);
    current.close = row.spy_close;
    current.volume += row.spy_volume || 0;
    current.rows.push(row);
  });
  if (current) buckets.push(current);
  return buckets;
}

function addVwap(bars) {
  let cumulativePv = 0;
  let cumulativeVolume = 0;
  bars.forEach((bar) => {
    const typical = (bar.high + bar.low + bar.close) / 3;
    const volume = bar.volume || 0;
    cumulativePv += typical * volume;
    cumulativeVolume += volume;
    bar.vwap = cumulativeVolume > 0 ? cumulativePv / cumulativeVolume : bar.close;
  });
}

function ema(values, period) {
  const out = [];
  const alpha = 2 / (period + 1);
  let previous = null;
  values.forEach((value) => {
    previous = previous === null ? value : (value * alpha) + (previous * (1 - alpha));
    out.push(previous);
  });
  return out;
}

function rsi(values, period = 14) {
  const out = Array(values.length).fill(null);
  let averageGain = 0;
  let averageLoss = 0;
  for (let index = 1; index < values.length; index += 1) {
    const change = values[index] - values[index - 1];
    const gain = Math.max(0, change);
    const loss = Math.max(0, -change);
    if (index <= period) {
      averageGain += gain;
      averageLoss += loss;
      if (index === period) {
        averageGain /= period;
        averageLoss /= period;
      }
    } else {
      averageGain = ((averageGain * (period - 1)) + gain) / period;
      averageLoss = ((averageLoss * (period - 1)) + loss) / period;
    }
    if (index >= period) {
      if (averageLoss === 0) out[index] = 100;
      else {
        const rs = averageGain / averageLoss;
        out[index] = 100 - (100 / (1 + rs));
      }
    }
  }
  return out;
}

function rollingMean(values, period) {
  const out = Array(values.length).fill(null);
  let sum = 0;
  values.forEach((value, index) => {
    sum += value;
    if (index >= period) sum -= values[index - period];
    if (index >= period - 1) out[index] = sum / period;
  });
  return out;
}

function rollingStd(values, means, period) {
  const out = Array(values.length).fill(null);
  values.forEach((_, index) => {
    if (index < period - 1 || !Number.isFinite(means[index])) return;
    let sumSq = 0;
    for (let cursor = index - period + 1; cursor <= index; cursor += 1) {
      sumSq += (values[cursor] - means[index]) ** 2;
    }
    out[index] = Math.sqrt(sumSq / period);
  });
  return out;
}

function addIndicators(bars) {
  addVwap(bars);
  const closes = bars.map((bar) => bar.close);
  const volumes = bars.map((bar) => bar.volume || 0);
  const ema9 = ema(closes, 9);
  const ema20 = ema(closes, 20);
  const ema21 = ema(closes, 21);
  const ema50 = ema(closes, 50);
  const rsi14 = rsi(closes, 14);
  const bbMid = rollingMean(closes, 20);
  const bbStd = rollingStd(closes, bbMid, 20);
  const volumeMean20 = rollingMean(volumes, 20);
  bars.forEach((bar, index) => {
    bar.ema9 = ema9[index];
    bar.ema20 = ema20[index];
    bar.ema21 = ema21[index];
    bar.ema50 = ema50[index];
    bar.rsi14 = rsi14[index];
    bar.bbMid = bbMid[index];
    bar.bbStd = bbStd[index];
    bar.bbUpper = Number.isFinite(bbMid[index]) && Number.isFinite(bbStd[index]) ? bbMid[index] + (2 * bbStd[index]) : null;
    bar.bbLower = Number.isFinite(bbMid[index]) && Number.isFinite(bbStd[index]) ? bbMid[index] - (2 * bbStd[index]) : null;
    bar.volumeMean20 = volumeMean20[index];
    bar.volumeSpike = Number.isFinite(volumeMean20[index]) && volumeMean20[index] > 0 ? volumes[index] > 1.5 * volumeMean20[index] : false;
  });
}

function rowAtOrBefore(rows, minuteOfDayEt) {
  let out = null;
  rows.forEach((row) => {
    if (row.minuteOfDayEt <= minuteOfDayEt) out = row;
  });
  return out || rows[rows.length - 1] || null;
}

function exitByStopTarget({ rows, entryIndex, direction, entryPrice, stopPrice, targetPrice, latestExitMinuteEt }) {
  let fallback = rowAtOrBefore(rows, latestExitMinuteEt);
  let exit = {
    exitPrice: fallback?.spy_close ?? entryPrice,
    exitMinuteOfDayEt: fallback?.minuteOfDayEt ?? rows[rows.length - 1]?.minuteOfDayEt,
    exitReason: 'time_stop',
  };
  for (let index = entryIndex; index < rows.length; index += 1) {
    const row = rows[index];
    if (row.minuteOfDayEt > latestExitMinuteEt) break;
    if (direction > 0) {
      const hitStop = row.spy_low <= stopPrice;
      const hitTarget = row.spy_high >= targetPrice;
      if (hitStop || hitTarget) {
        exit = {
          exitPrice: hitStop ? stopPrice : targetPrice,
          exitMinuteOfDayEt: row.minuteOfDayEt,
          exitReason: hitStop ? 'stop' : 'target',
        };
        break;
      }
    } else {
      const hitStop = row.spy_high >= stopPrice;
      const hitTarget = row.spy_low <= targetPrice;
      if (hitStop || hitTarget) {
        exit = {
          exitPrice: hitStop ? stopPrice : targetPrice,
          exitMinuteOfDayEt: row.minuteOfDayEt,
          exitReason: hitStop ? 'stop' : 'target',
        };
        break;
      }
    }
  }
  return exit;
}

function tradeReturn({ direction, entryPrice, exitPrice, roundTripCostRate }) {
  return safeReturn(exitPrice, entryPrice, direction) - roundTripCostRate;
}

function makeTrade({
  strategyId,
  tradeDate,
  direction,
  entryMinuteOfDayEt,
  exitMinuteOfDayEt,
  entryPrice,
  exitPrice,
  exitReason,
  grossReturn,
  netReturn,
  extra = {},
}) {
  return {
    strategyId,
    tradeDate,
    direction,
    side: direction > 0 ? 'long' : 'short',
    entryMinuteEt: minuteLabel(entryMinuteOfDayEt),
    exitMinuteEt: minuteLabel(exitMinuteOfDayEt),
    entryPrice: round(entryPrice, 4),
    exitPrice: round(exitPrice, 4),
    grossReturn: round(grossReturn),
    netReturn: round(netReturn),
    exitReason,
    ...extra,
  };
}

function simulateOrb5(rows, dayIso, costRate) {
  const weekday = weekdayEt(dayIso);
  if (![1, 3, 5].includes(weekday)) return [];
  const opening = rows.filter((row) => row.minuteOfDayEt >= 570 && row.minuteOfDayEt <= 574);
  if (opening.length < 5) return [];
  const rangeHigh = Math.max(...opening.map((row) => row.spy_high));
  const rangeLow = Math.min(...opening.map((row) => row.spy_low));
  const range = rangeHigh - rangeLow;
  if (!(range > 0)) return [];
  for (let index = 5; index < rows.length - 1; index += 1) {
    const row = rows[index];
    if (row.minuteOfDayEt < 575 || row.minuteOfDayEt > 925) continue;
    const brokeUp = row.spy_high > rangeHigh;
    const brokeDown = row.spy_low < rangeLow;
    if (!brokeUp && !brokeDown) continue;
    if (brokeUp && brokeDown) return [];
    const direction = brokeUp ? 1 : -1;
    const entryRow = rows[index + 1];
    const entryPrice = entryRow.spy_open;
    const stopPrice = direction > 0 ? entryPrice - range : entryPrice + range;
    const targetPrice = direction > 0 ? entryPrice + (2 * range) : entryPrice - (2 * range);
    const exit = exitByStopTarget({
      rows,
      entryIndex: index + 1,
      direction,
      entryPrice,
      stopPrice,
      targetPrice,
      latestExitMinuteEt: 930,
    });
    const grossReturn = safeReturn(exit.exitPrice, entryPrice, direction);
    const netReturn = tradeReturn({ direction, entryPrice, exitPrice: exit.exitPrice, roundTripCostRate: costRate * 2 });
    return [makeTrade({
      strategyId: 'orb5_mwf_range_2r',
      tradeDate: dayIso,
      direction,
      entryMinuteOfDayEt: entryRow.minuteOfDayEt,
      exitMinuteOfDayEt: exit.exitMinuteOfDayEt,
      entryPrice,
      exitPrice: exit.exitPrice,
      exitReason: exit.exitReason,
      grossReturn,
      netReturn,
      extra: {
        openingRangePct: round(range / entryPrice),
      },
    })];
  }
  return [];
}

function fixedHoldExit(bars, entryBarIndex, holdMinutes, latestExitMinuteEt) {
  const entryBar = bars[entryBarIndex];
  const targetMinute = Math.min(entryBar.startMinuteOfDayEt + holdMinutes - 1, latestExitMinuteEt);
  let exitBar = bars.find((bar, index) => index >= entryBarIndex && bar.endMinuteOfDayEt >= targetMinute);
  if (!exitBar || exitBar.endMinuteOfDayEt > latestExitMinuteEt) {
    exitBar = [...bars].reverse().find((bar) => bar.endMinuteOfDayEt <= latestExitMinuteEt) || bars[bars.length - 1];
  }
  return {
    exitPrice: exitBar.close,
    exitMinuteOfDayEt: exitBar.endMinuteOfDayEt,
    exitReason: 'fixed_hold',
  };
}

function simulateEmaMomentum(bars5, dayIso, costRate) {
  for (let index = 1; index < bars5.length - 1; index += 1) {
    const previous = bars5[index - 1];
    const bar = bars5[index];
    if (bar.endMinuteOfDayEt >= 780) break;
    if (!Number.isFinite(previous.ema9) || !Number.isFinite(previous.ema21)) continue;
    const crossedUp = previous.ema9 <= previous.ema21 && bar.ema9 > bar.ema21;
    if (!crossedUp) continue;
    const entryBar = bars5[index + 1];
    const entryPrice = entryBar.open;
    const exit = fixedHoldExit(bars5, index + 1, 60, 930);
    const grossReturn = safeReturn(exit.exitPrice, entryPrice, 1);
    const netReturn = tradeReturn({ direction: 1, entryPrice, exitPrice: exit.exitPrice, roundTripCostRate: costRate * 2 });
    return [makeTrade({
      strategyId: 'ema9_21_5m_long_60m',
      tradeDate: dayIso,
      direction: 1,
      entryMinuteOfDayEt: entryBar.startMinuteOfDayEt,
      exitMinuteOfDayEt: exit.exitMinuteOfDayEt,
      entryPrice,
      exitPrice: exit.exitPrice,
      exitReason: exit.exitReason,
      grossReturn,
      netReturn,
    })];
  }
  return [];
}

function momentumScore(bar, direction) {
  if (direction > 0) {
    return [
      bar.close > bar.vwap,
      bar.rsi14 > 55,
      bar.ema20 > bar.ema50,
      bar.volumeSpike,
    ].filter(Boolean).length;
  }
  return [
    bar.close < bar.vwap,
    bar.rsi14 < 45,
    bar.ema20 < bar.ema50,
    bar.volumeSpike,
  ].filter(Boolean).length;
}

function simulateVwapMomentum(bars5, dayIso, costRate) {
  for (let index = 20; index < bars5.length - 1; index += 1) {
    const bar = bars5[index];
    if (bar.endMinuteOfDayEt < 600 || bar.endMinuteOfDayEt >= 765) continue;
    const longScore = momentumScore(bar, 1);
    const shortScore = momentumScore(bar, -1);
    if (longScore < 3 && shortScore < 3) continue;
    if (longScore >= 3 && shortScore >= 3) continue;
    const direction = longScore > shortScore ? 1 : -1;
    const entryBar = bars5[index + 1];
    const entryPrice = entryBar.open;
    const exit = fixedHoldExit(bars5, index + 1, 40, 930);
    const grossReturn = safeReturn(exit.exitPrice, entryPrice, direction);
    const netReturn = tradeReturn({ direction, entryPrice, exitPrice: exit.exitPrice, roundTripCostRate: costRate * 2 });
    return [makeTrade({
      strategyId: 'vwap_ema_rsi_volume_5m_40m',
      tradeDate: dayIso,
      direction,
      entryMinuteOfDayEt: entryBar.startMinuteOfDayEt,
      exitMinuteOfDayEt: exit.exitMinuteOfDayEt,
      entryPrice,
      exitPrice: exit.exitPrice,
      exitReason: exit.exitReason,
      grossReturn,
      netReturn,
      extra: {
        signalScore: direction > 0 ? longScore : shortScore,
      },
    })];
  }
  return [];
}

function simulateRsiBollinger(bars5, dayIso, costRate) {
  const trades = [];
  let openTrade = null;
  let tradesThisDay = 0;
  for (let index = 20; index < bars5.length - 1; index += 1) {
    const previous = bars5[index - 1];
    const bar = bars5[index];
    const nextBar = bars5[index + 1];
    if (bar.endMinuteOfDayEt > 930) break;
    if (openTrade) {
      const longExit = openTrade.direction > 0 && (bar.close >= bar.bbMid || bar.rsi14 >= 50);
      const shortExit = openTrade.direction < 0 && (bar.close <= bar.bbMid || bar.rsi14 <= 50);
      if (longExit || shortExit || nextBar.startMinuteOfDayEt > 930) {
        const exitPrice = nextBar.startMinuteOfDayEt <= 930 ? nextBar.open : bar.close;
        const exitMinuteOfDayEt = nextBar.startMinuteOfDayEt <= 930 ? nextBar.startMinuteOfDayEt : bar.endMinuteOfDayEt;
        const grossReturn = safeReturn(exitPrice, openTrade.entryPrice, openTrade.direction);
        const netReturn = tradeReturn({
          direction: openTrade.direction,
          entryPrice: openTrade.entryPrice,
          exitPrice,
          roundTripCostRate: costRate * 2,
        });
        trades.push(makeTrade({
          strategyId: 'rsi_bollinger_5m_reversion',
          tradeDate: dayIso,
          direction: openTrade.direction,
          entryMinuteOfDayEt: openTrade.entryMinuteOfDayEt,
          exitMinuteOfDayEt,
          entryPrice: openTrade.entryPrice,
          exitPrice,
          exitReason: longExit || shortExit ? 'mean_reversion_exit' : 'time_stop',
          grossReturn,
          netReturn,
        }));
        openTrade = null;
      }
      continue;
    }
    if (tradesThisDay >= 3 || !Number.isFinite(bar.bbLower) || !Number.isFinite(previous.bbLower) || !Number.isFinite(bar.rsi14)) continue;
    if (bar.endMinuteOfDayEt < 600 || bar.endMinuteOfDayEt >= 900) continue;
    const crossedLower = previous.close >= previous.bbLower && bar.close < bar.bbLower && bar.rsi14 < 25;
    const crossedUpper = previous.close <= previous.bbUpper && bar.close > bar.bbUpper && bar.rsi14 > 75;
    if (!crossedLower && !crossedUpper) continue;
    const direction = crossedLower ? 1 : -1;
    openTrade = {
      direction,
      entryPrice: nextBar.open,
      entryMinuteOfDayEt: nextBar.startMinuteOfDayEt,
    };
    tradesThisDay += 1;
  }
  if (openTrade) {
    const exitBar = [...bars5].reverse().find((bar) => bar.endMinuteOfDayEt <= 930) || bars5[bars5.length - 1];
    const grossReturn = safeReturn(exitBar.close, openTrade.entryPrice, openTrade.direction);
    const netReturn = tradeReturn({
      direction: openTrade.direction,
      entryPrice: openTrade.entryPrice,
      exitPrice: exitBar.close,
      roundTripCostRate: costRate * 2,
    });
    trades.push(makeTrade({
      strategyId: 'rsi_bollinger_5m_reversion',
      tradeDate: dayIso,
      direction: openTrade.direction,
      entryMinuteOfDayEt: openTrade.entryMinuteOfDayEt,
      exitMinuteOfDayEt: exitBar.endMinuteOfDayEt,
      entryPrice: openTrade.entryPrice,
      exitPrice: exitBar.close,
      exitReason: 'time_stop',
      grossReturn,
      netReturn,
    }));
  }
  return trades;
}

function simulateDay(rows, dayIso, costRate) {
  const sorted = rows
    .filter((row) => Number.isFinite(row.spy_open) && Number.isFinite(row.spy_close))
    .sort((left, right) => left.minuteOfDayEt - right.minuteOfDayEt);
  if (sorted.length < 60) return { trades: [], buyHoldReturn: 0 };
  const bars5 = aggregateBars(sorted, 5);
  addIndicators(bars5);
  const trades = [
    ...simulateOrb5(sorted, dayIso, costRate),
    ...simulateEmaMomentum(bars5, dayIso, costRate),
    ...simulateVwapMomentum(bars5, dayIso, costRate),
    ...simulateRsiBollinger(bars5, dayIso, costRate),
  ];
  const first = sorted[0];
  const last = sorted[sorted.length - 1];
  return {
    trades,
    buyHoldReturn: safeReturn(last.spy_close, first.spy_open, 1),
  };
}

function splitForDate(config, dayIso) {
  if (dayIso >= config.windows.train.startDate && dayIso <= config.windows.train.endDate) return config.windows.train.name;
  for (const test of config.windows.tests) {
    if (dayIso >= test.startDate && dayIso <= test.endDate) return test.name;
  }
  if (config.windows.sensitivityTrain && dayIso >= config.windows.sensitivityTrain.startDate && dayIso <= config.windows.sensitivityTrain.endDate) {
    return config.windows.sensitivityTrain.name;
  }
  return 'outside_protocol';
}

function monthForDate(dayIso) {
  return dayIso.slice(0, 7);
}

function compactMetric(metric) {
  return {
    count: metric.count,
    totalReturn: round(metric.totalReturn),
    averageReturn: round(metric.averageReturn),
    winRate: round(metric.winRate),
    profitFactor: round(metric.profitFactor),
    maxDrawdown: round(metric.maxDrawdown),
  };
}

function summarizeStrategy(strategyId, allDays, allTrades, config) {
  const days = [...allDays].sort((left, right) => left.tradeDate.localeCompare(right.tradeDate));
  const trades = allTrades.filter((trade) => trade.strategyId === strategyId);
  const byDate = new Map();
  trades.forEach((trade) => {
    const dayTrades = byDate.get(trade.tradeDate) || [];
    dayTrades.push(trade);
    byDate.set(trade.tradeDate, dayTrades);
  });
  const daily = days.map((day) => {
    const dayTrades = byDate.get(day.tradeDate) || [];
    const strategyReturn = dayTrades.reduce((equity, trade) => equity * (1 + trade.netReturn), 1) - 1;
    return {
      tradeDate: day.tradeDate,
      split: day.split,
      month: monthForDate(day.tradeDate),
      strategyReturn,
      buyHoldReturn: day.buyHoldReturn,
      tradeCount: dayTrades.length,
    };
  });

  const officialTestNames = new Set(config.windows.tests.map((window) => window.name));
  const groups = [
    { name: 'all_2025_2026', rows: daily.filter((day) => day.split !== 'outside_protocol') },
    { name: config.windows.train.name, rows: daily.filter((day) => day.split === config.windows.train.name) },
    { name: 'official_holdout_2026_02_to_04_27', rows: daily.filter((day) => officialTestNames.has(day.split)) },
    ...config.windows.tests.map((window) => ({ name: window.name, rows: daily.filter((day) => day.split === window.name) })),
  ];

  const splitMetrics = {};
  groups.forEach((group) => {
    const groupTrades = trades.filter((trade) => group.rows.some((row) => row.tradeDate === trade.tradeDate));
    const metric = compactMetric(summarizeReturns(group.rows.map((row) => row.strategyReturn)));
    const tradeMetric = compactMetric(summarizeReturns(groupTrades.map((trade) => trade.netReturn)));
    splitMetrics[group.name] = {
      ...metric,
      days: group.rows.length,
      tradedDays: group.rows.filter((row) => row.tradeCount > 0).length,
      trades: groupTrades.length,
      tradeWinRate: tradeMetric.winRate,
      tradeProfitFactor: tradeMetric.profitFactor,
      averageTradeReturn: tradeMetric.averageReturn,
      buyHoldReturn: round(summarizeReturns(group.rows.map((row) => row.buyHoldReturn)).totalReturn),
      exposureDayShare: round(group.rows.length ? group.rows.filter((row) => row.tradeCount > 0).length / group.rows.length : 0),
    };
  });

  const monthMetrics = {};
  const monthNames = [...new Set(daily.map((day) => day.month))].sort();
  monthNames.forEach((month) => {
    const rows = daily.filter((day) => day.month === month);
    const monthTrades = trades.filter((trade) => monthForDate(trade.tradeDate) === month);
    monthMetrics[month] = {
      ...compactMetric(summarizeReturns(rows.map((row) => row.strategyReturn))),
      days: rows.length,
      tradedDays: rows.filter((row) => row.tradeCount > 0).length,
      trades: monthTrades.length,
      buyHoldReturn: round(summarizeReturns(rows.map((row) => row.buyHoldReturn)).totalReturn),
    };
  });

  return {
    strategyId,
    description: STRATEGIES.find((strategy) => strategy.id === strategyId)?.description,
    sourceIds: STRATEGIES.find((strategy) => strategy.id === strategyId)?.sourceIds || [],
    splitMetrics,
    monthMetrics,
    sampleTrades: trades.slice(0, 25),
  };
}

function rankStrategies(summaries) {
  return summaries
    .map((summary) => {
      const holdout = summary.splitMetrics.official_holdout_2026_02_to_04_27;
      const score = holdout.trades >= 10 ? holdout.totalReturn - Math.abs(holdout.maxDrawdown) : null;
      return {
        strategyId: summary.strategyId,
        holdoutTotalReturn: holdout.totalReturn,
        holdoutMaxDrawdown: holdout.maxDrawdown,
        holdoutTrades: holdout.trades,
        holdoutWinRate: holdout.tradeWinRate,
        holdoutProfitFactor: holdout.tradeProfitFactor,
        buyHoldReturn: holdout.buyHoldReturn,
        score: round(score),
      };
    })
    .sort((left, right) => (right.score ?? -Infinity) - (left.score ?? -Infinity));
}

function markdownReport(report) {
  const lines = [];
  lines.push('# Public Intraday Strategy Backtest');
  lines.push('');
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Dataset: \`${report.datasetPath}\``);
  lines.push(`Execution cost: ${report.execution.transactionCostBps} bps transaction + ${report.execution.slippageBps} bps slippage per entry/exit side.`);
  lines.push('');
  lines.push('## Sources');
  report.sources.forEach((source) => {
    lines.push(`- ${source.platform}: [${source.id}](${source.url}) - ${source.note}`);
  });
  lines.push('');
  lines.push('## Official Holdout Ranking');
  lines.push('');
  lines.push('| Rank | Strategy | Return | Max DD | Trades | Win Rate | Profit Factor | Buy/Hold |');
  lines.push('| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |');
  report.ranking.forEach((row, index) => {
    lines.push(`| ${index + 1} | ${row.strategyId} | ${asPct(row.holdoutTotalReturn)} | ${asPct(row.holdoutMaxDrawdown)} | ${row.holdoutTrades} | ${asPct(row.holdoutWinRate)} | ${row.holdoutProfitFactor ?? 'n/a'} | ${asPct(row.buyHoldReturn)} |`);
  });
  lines.push('');
  lines.push('## Month Breakdown');
  report.summaries.forEach((summary) => {
    lines.push('');
    lines.push(`### ${summary.strategyId}`);
    lines.push('');
    lines.push('| Month/Split | Return | Max DD | Trades | Traded Days | Buy/Hold |');
    lines.push('| --- | ---: | ---: | ---: | ---: | ---: |');
    [report.config.windows.train.name, ...report.config.windows.tests.map((window) => window.name)].forEach((splitName) => {
      const metric = summary.splitMetrics[splitName];
      lines.push(`| ${splitName} | ${asPct(metric.totalReturn)} | ${asPct(metric.maxDrawdown)} | ${metric.trades} | ${metric.tradedDays}/${metric.days} | ${asPct(metric.buyHoldReturn)} |`);
    });
  });
  lines.push('');
  lines.push('## Caveats');
  lines.push('');
  lines.push('- These are SPY-underlying signal tests. Option-specific Greeks, spreads, contract selection, and theta decay are not modeled here.');
  lines.push('- Signals enter on the next completed bar open. Ambiguous same-bar stop/target hits are charged conservatively to the stop.');
  lines.push('- Official merit should be judged on February, March, and April 2026 through 2026-04-27, not on January or the 2025 sensitivity context.');
  lines.push('');
  return `${lines.join('\n')}\n`;
}

async function readDatasetByDay(datasetPath, startDate, endDate, onDay) {
  const reader = readline.createInterface({
    input: fs.createReadStream(datasetPath, { encoding: 'utf8' }),
    crlfDelay: Infinity,
  });
  let currentDate = null;
  let rows = [];
  async function flush() {
    if (currentDate && rows.length) await onDay(currentDate, rows);
  }
  for await (const line of reader) {
    if (!line.trim()) continue;
    const row = JSON.parse(line);
    if (row.tradeDate > endDate) {
      break;
    }
    if (row.tradeDate < startDate) continue;
    if (currentDate && row.tradeDate !== currentDate) {
      await flush();
      rows = [];
    }
    currentDate = row.tradeDate;
    rows.push(row);
  }
  await flush();
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const datasetPath = path.resolve(args.dataset || DEFAULT_DATASET);
  if (!fs.existsSync(datasetPath)) throw new Error(`dataset_missing:${datasetPath}`);

  const startDate = args['start-date'] || config.windows.sensitivityTrain?.startDate || config.windows.train.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  const transactionCostBps = asNumber(args['cost-bps'], config.execution.transactionCostBps);
  const slippageBps = asNumber(args['slippage-bps'], config.execution.slippageBps);
  const costRate = (transactionCostBps + slippageBps) / 10_000;
  const outputBase = args.output
    ? path.resolve(args.output).replace(/\.json$/i, '')
    : path.join(PROJECT_ROOT, 'artifacts', `public-intraday-strategy-backtest-${startDate}-${endDate}`);
  const outputJson = `${outputBase}.json`;
  const outputMd = `${outputBase}.md`;

  const allDays = [];
  const allTrades = [];
  let dayCount = 0;
  await readDatasetByDay(datasetPath, startDate, endDate, async (dayIso, rows) => {
    const day = simulateDay(rows, dayIso, costRate);
    const split = splitForDate(config, dayIso);
    allDays.push({ tradeDate: dayIso, split, buyHoldReturn: day.buyHoldReturn });
    allTrades.push(...day.trades);
    dayCount += 1;
    if (dayCount % 25 === 0) {
      process.stderr.write(`[public-intraday] processed ${dayCount} days through ${dayIso}, trades=${allTrades.length}\n`);
    }
  });

  const summaries = STRATEGIES.map((strategy) => summarizeStrategy(strategy.id, allDays, allTrades, config));
  const report = {
    generatedAt: new Date().toISOString(),
    datasetPath,
    startDate,
    endDate,
    dayCount,
    execution: {
      transactionCostBps,
      slippageBps,
      roundTripCostBps: round((transactionCostBps + slippageBps) * 2, 3),
    },
    config: {
      windows: config.windows,
      dataPolicy: config.dataPolicy,
    },
    sources: SOURCES,
    strategies: STRATEGIES,
    ranking: rankStrategies(summaries),
    summaries,
  };

  ensureDir(path.dirname(outputJson));
  fs.writeFileSync(outputJson, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  fs.writeFileSync(outputMd, markdownReport(report), 'utf8');
  console.log(JSON.stringify({
    outputJson,
    outputMd,
    dayCount,
    ranking: report.ranking,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
