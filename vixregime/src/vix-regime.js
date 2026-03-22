const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_REQUIRED_SYMBOLS = Object.freeze(['SPX', 'SPY', 'SPXL', 'SPXS', 'VIX', 'VIX1D', 'VIX3M', 'VIX9D']);
const REGIME_ORDER = Object.freeze(['Calm', 'Normal', 'Stress', 'Crash']);
const TRADING_MINUTES_PER_DAY = 390;
const MIN_PERCENTILE_WINDOW = 20;

function readThresholdConfig(filePath = path.join(__dirname, '..', 'config', 'vix-regime-thresholds.json')) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeRatio(numerator, denominator) {
  const top = toNumber(numerator);
  const bottom = toNumber(denominator);
  if (top === null || bottom === null || bottom === 0) return null;
  return top / bottom;
}

function percentileRank(windowValues = [], currentValue) {
  const current = toNumber(currentValue);
  if (current === null) return null;
  const usable = windowValues.map(toNumber).filter((value) => value !== null);
  if (usable.length === 0) return null;
  let lessThanOrEqual = 0;
  usable.forEach((value) => {
    if (value <= current) lessThanOrEqual += 1;
  });
  return lessThanOrEqual / usable.length;
}

function average(values = []) {
  const usable = values.map(toNumber).filter((value) => value !== null);
  if (!usable.length) return null;
  return usable.reduce((sum, value) => sum + value, 0) / usable.length;
}

function standardDeviation(values = []) {
  const usable = values.map(toNumber).filter((value) => value !== null);
  if (usable.length < 2) return null;
  const avg = average(usable);
  const variance = usable.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / usable.length;
  return Math.sqrt(Math.max(variance, 0));
}

function computeRollingDelta(values = [], currentIndex, lookback) {
  const current = toNumber(values[currentIndex]);
  const previous = toNumber(values[currentIndex - lookback]);
  if (current === null || previous === null) return null;
  return current - previous;
}

function computeRollingReturn(values = [], currentIndex, lookback) {
  const current = toNumber(values[currentIndex]);
  const previous = toNumber(values[currentIndex - lookback]);
  if (current === null || previous === null || previous <= 0) return null;
  return (current / previous) - 1;
}

function computeRollingAverage(values = [], currentIndex, lookback) {
  if (currentIndex - lookback + 1 < 0) return null;
  return average(values.slice(currentIndex - lookback + 1, currentIndex + 1));
}

function computeRollingRealizedVol(returnValues = [], currentIndex, lookback, annualizationFactor = 252) {
  if (currentIndex - lookback + 1 < 0) return null;
  const window = returnValues.slice(currentIndex - lookback + 1, currentIndex + 1).filter((value) => value !== null);
  const stdev = standardDeviation(window);
  if (stdev === null) return null;
  return stdev * Math.sqrt(annualizationFactor);
}

function computeDownsideVol(returnValues = [], currentIndex, lookback, annualizationFactor = 252) {
  if (currentIndex - lookback + 1 < 0) return null;
  const window = returnValues
    .slice(currentIndex - lookback + 1, currentIndex + 1)
    .filter((value) => value !== null && value < 0);
  const stdev = standardDeviation(window);
  if (stdev === null) return null;
  return stdev * Math.sqrt(annualizationFactor);
}

function isUsablePriceRow(row = {}) {
  const close = toNumber(row.close);
  const open = toNumber(row.open);
  const high = toNumber(row.high);
  const low = toNumber(row.low);
  return (
    (close !== null && close > 0)
    || (open !== null && open > 0)
    || (high !== null && high > 0)
    || (low !== null && low > 0)
  );
}

function groupRowsBySymbol(rows = []) {
  const out = new Map();
  rows.forEach((row) => {
    const symbol = String(row.symbol || '').trim().toUpperCase();
    if (!symbol) return;
    const normalizedRow = {
      symbol,
      minuteUtc: String(row.minuteUtc || row.minute_utc || row.minute_bucket_utc || ''),
      tradeDateUtc: String(row.tradeDateUtc || row.trade_date_utc || '').slice(0, 10),
      close: toNumber(row.close),
      open: toNumber(row.open),
      high: toNumber(row.high),
      low: toNumber(row.low),
      volume: toNumber(row.volume),
    };
    if (!isUsablePriceRow(normalizedRow)) return;
    const list = out.get(symbol) || [];
    list.push(normalizedRow);
    out.set(symbol, list);
  });
  out.forEach((list, symbol) => {
    list.sort((left, right) => left.minuteUtc.localeCompare(right.minuteUtc));
    out.set(symbol, list);
  });
  return out;
}

function buildMinuteAlignment(rows = []) {
  const rowsBySymbol = groupRowsBySymbol(rows);
  const minuteMap = new Map();
  rowsBySymbol.forEach((symbolRows, symbol) => {
    symbolRows.forEach((row) => {
      const bucket = minuteMap.get(row.minuteUtc) || {
        minuteUtc: row.minuteUtc,
        tradeDateUtc: row.tradeDateUtc,
        prices: {},
      };
      bucket.prices[symbol] = row.close;
      minuteMap.set(row.minuteUtc, bucket);
    });
  });
  return Array.from(minuteMap.values()).sort((left, right) => left.minuteUtc.localeCompare(right.minuteUtc));
}

function buildDailyCloses(rows = []) {
  const rowsBySymbol = groupRowsBySymbol(rows);
  const dailyMap = new Map();
  rowsBySymbol.forEach((symbolRows, symbol) => {
    symbolRows.forEach((row) => {
      const bucket = dailyMap.get(row.tradeDateUtc) || {
        tradeDateUtc: row.tradeDateUtc,
        closeBySymbol: {},
        barBySymbol: {},
        firstMinuteUtcBySymbol: {},
        lastMinuteUtcBySymbol: {},
      };
      const firstMinute = bucket.firstMinuteUtcBySymbol[symbol] || '';
      const previousMinute = bucket.lastMinuteUtcBySymbol[symbol] || '';
      const currentBar = bucket.barBySymbol[symbol] || {
        open: null,
        high: null,
        low: null,
        close: null,
      };
      if (!firstMinute || row.minuteUtc <= firstMinute) {
        currentBar.open = toNumber(row.open) ?? toNumber(row.close);
        bucket.firstMinuteUtcBySymbol[symbol] = row.minuteUtc;
      }
      currentBar.high = currentBar.high === null ? toNumber(row.high) : Math.max(currentBar.high, toNumber(row.high) ?? currentBar.high);
      currentBar.low = currentBar.low === null ? toNumber(row.low) : Math.min(currentBar.low, toNumber(row.low) ?? currentBar.low);
      if (!previousMinute || row.minuteUtc >= previousMinute) {
        bucket.closeBySymbol[symbol] = row.close;
        currentBar.close = row.close;
        bucket.lastMinuteUtcBySymbol[symbol] = row.minuteUtc;
      }
      bucket.barBySymbol[symbol] = currentBar;
      dailyMap.set(row.tradeDateUtc, bucket);
    });
  });
  return Array.from(dailyMap.values())
    .sort((left, right) => left.tradeDateUtc.localeCompare(right.tradeDateUtc))
    .map((row) => ({
      tradeDateUtc: row.tradeDateUtc,
      closeBySymbol: row.closeBySymbol,
      barBySymbol: row.barBySymbol,
    }));
}

function buildDailyFirstMinuteCloses(rows = []) {
  const rowsBySymbol = groupRowsBySymbol(rows);
  const dailyMap = new Map();
  rowsBySymbol.forEach((symbolRows, symbol) => {
    symbolRows.forEach((row) => {
      const bucket = dailyMap.get(row.tradeDateUtc) || {
        tradeDateUtc: row.tradeDateUtc,
        closeBySymbol: {},
        firstMinuteUtcBySymbol: {},
      };
      const previousMinute = bucket.firstMinuteUtcBySymbol[symbol] || '';
      if (!previousMinute || row.minuteUtc <= previousMinute) {
        bucket.closeBySymbol[symbol] = row.close;
        bucket.firstMinuteUtcBySymbol[symbol] = row.minuteUtc;
      }
      dailyMap.set(row.tradeDateUtc, bucket);
    });
  });
  return Array.from(dailyMap.values())
    .sort((left, right) => left.tradeDateUtc.localeCompare(right.tradeDateUtc))
    .map((row) => ({
      tradeDateUtc: row.tradeDateUtc,
      closeBySymbol: row.closeBySymbol,
    }));
}

function normalizeRequiredSymbols(symbols = DEFAULT_REQUIRED_SYMBOLS) {
  return Array.from(new Set(symbols.map((symbol) => String(symbol || '').trim().toUpperCase()).filter(Boolean)));
}

function computeCoverageReport(rows = [], options = {}) {
  const requiredSymbols = normalizeRequiredSymbols(options.requiredSymbols || DEFAULT_REQUIRED_SYMBOLS);
  const rowsBySymbol = groupRowsBySymbol(rows);
  const spyDates = new Set((rowsBySymbol.get('SPY') || []).map((row) => row.tradeDateUtc));
  const minuteCountsBySymbolDay = new Map();

  rows.forEach((row) => {
    const symbol = String(row.symbol || '').trim().toUpperCase();
    const day = String(row.tradeDateUtc || row.trade_date_utc || '').slice(0, 10);
    if (!symbol || !day) return;
    const key = `${symbol}|${day}`;
    minuteCountsBySymbolDay.set(key, (minuteCountsBySymbolDay.get(key) || 0) + 1);
  });

  const coverage = requiredSymbols.map((symbol) => {
    const symbolRows = rowsBySymbol.get(symbol) || [];
    const dates = Array.from(new Set(symbolRows.map((row) => row.tradeDateUtc))).sort();
    const missingDays = Array.from(spyDates).filter((day) => !dates.includes(day));
    const minuteCountStats = dates.map((day) => minuteCountsBySymbolDay.get(`${symbol}|${day}`) || 0);
    const suspectDays = dates.filter((day) => {
      const count = minuteCountsBySymbolDay.get(`${symbol}|${day}`) || 0;
      return count < TRADING_MINUTES_PER_DAY * 0.5;
    });

    return {
      symbol,
      minDay: dates[0] || null,
      maxDay: dates[dates.length - 1] || null,
      tradingDays: dates.length,
      missingDaysRelativeToSpy: missingDays.length,
      missingDayList: missingDays,
      minuteCountMin: minuteCountStats.length ? Math.min(...minuteCountStats) : null,
      minuteCountMax: minuteCountStats.length ? Math.max(...minuteCountStats) : null,
      suspectMinuteDays: suspectDays,
    };
  });

  return {
    requiredSymbols,
    datasetReady: coverage.every((item) => item.missingDaysRelativeToSpy === 0 && item.tradingDays > 0),
    spyTradingDays: spyDates.size,
    symbols: coverage,
  };
}

function classifyRegime(featureRow, configSection, exposures) {
  const reasons = [];
  const {
    vix,
    vixPctRank,
    delta5,
    delta10,
    delta30,
    delta60,
    ts9d30,
    ts30d90d,
    ts1d9d,
    vix1dOverVix,
  } = featureRow;

  const hardCrash = (
    (vix !== null && vix >= configSection.vixCrashMin)
    || (delta5 !== null && delta5 >= configSection.delta5CrashMin)
    || (delta60 !== null && delta60 >= configSection.delta60CrashMin)
    || (ts30d90d !== null && ts30d90d >= configSection.termHard30d90d)
    || (ts9d30 !== null && ts9d30 >= configSection.termHard9d30)
    || (vix1dOverVix !== null && vix1dOverVix >= configSection.crashBrakeVix1dMultiple)
    || (ts1d9d !== null && ts1d9d >= configSection.crashBrakeVix1d9d)
  );
  if (hardCrash) {
    if (vix !== null && vix >= configSection.vixCrashMin) reasons.push('vix_crash');
    if (delta5 !== null && configSection.delta5CrashMin !== undefined && delta5 >= configSection.delta5CrashMin) reasons.push('d5_crash');
    if (delta60 !== null && configSection.delta60CrashMin !== undefined && delta60 >= configSection.delta60CrashMin) reasons.push('d60_crash');
    if (ts30d90d !== null && ts30d90d >= configSection.termHard30d90d) reasons.push('term_30_90_hard');
    if (ts9d30 !== null && ts9d30 >= configSection.termHard9d30) reasons.push('term_9_30_hard');
    if (vix1dOverVix !== null && vix1dOverVix >= configSection.crashBrakeVix1dMultiple) reasons.push('vix1d_multiple');
    if (ts1d9d !== null && ts1d9d >= configSection.crashBrakeVix1d9d) reasons.push('vix1d_9d_brake');
    return { regime: 'Crash', exposure: exposures.crash, reasons };
  }

  const stress = (
    (vix !== null && vix >= configSection.vixStressMin)
    || (vixPctRank !== null && vixPctRank >= configSection.vixPctRankStressMin)
    || (ts30d90d !== null && ts30d90d >= configSection.termSoft30d90d)
    || (ts9d30 !== null && ts9d30 >= configSection.termSoft9d30)
  );
  if (stress) {
    if (vix !== null && vix >= configSection.vixStressMin) reasons.push('vix_stress');
    if (vixPctRank !== null && vixPctRank >= configSection.vixPctRankStressMin) reasons.push('vix_percentile_stress');
    if (ts30d90d !== null && ts30d90d >= configSection.termSoft30d90d) reasons.push('term_30_90_soft');
    if (ts9d30 !== null && ts9d30 >= configSection.termSoft9d30) reasons.push('term_9_30_soft');
    return { regime: 'Stress', exposure: exposures.stress, reasons };
  }

  const calm = (
    vix !== null
    && vix <= configSection.vixCalmMax
    && (vixPctRank === null || vixPctRank <= configSection.vixPctRankCalmMax)
    && (
      (delta10 !== null && configSection.delta10CalmMax !== undefined && delta10 <= configSection.delta10CalmMax)
      || (delta30 !== null && configSection.delta30NormalMax !== undefined && delta30 <= configSection.delta30NormalMax)
      || (delta10 == null && delta30 == null)
    )
    && (ts30d90d === null || ts30d90d < configSection.termSoft30d90d)
    && (ts9d30 === null || ts9d30 < configSection.termSoft9d30)
  );
  if (calm) {
    reasons.push('vix_calm');
    if (vixPctRank !== null && vixPctRank <= configSection.vixPctRankCalmMax) reasons.push('vix_percentile_calm');
    return { regime: 'Calm', exposure: exposures.calm, reasons };
  }

  if (delta30 !== null && configSection.delta30NormalMax !== undefined && delta30 <= configSection.delta30NormalMax) {
    reasons.push('delta30_normal');
  }
  if (delta5 !== null && configSection.delta5NormalMax !== undefined && delta5 <= configSection.delta5NormalMax) {
    reasons.push('delta5_normal');
  }
  if (vix !== null && vix < configSection.vixNormalMax) {
    reasons.push('vix_normal');
  }

  return { regime: 'Normal', exposure: exposures.normal, reasons: reasons.length ? reasons : ['default_normal'] };
}

function buildDailyFeatures(alignedDailyRows = [], thresholdConfig) {
  const dailyVixValues = alignedDailyRows.map((row) => toNumber(row.closeBySymbol.VIX));
  const dailyVix9dValues = alignedDailyRows.map((row) => toNumber(row.closeBySymbol.VIX9D));
  const dailyVix1dValues = alignedDailyRows.map((row) => toNumber(row.closeBySymbol.VIX1D));
  const dailyVix3mValues = alignedDailyRows.map((row) => toNumber(row.closeBySymbol.VIX3M));
  const dailySpyCloses = alignedDailyRows.map((row) => toNumber(row.closeBySymbol.SPY));
  const dailySpyOpens = alignedDailyRows.map((row) => toNumber(row.barBySymbol?.SPY?.open));
  const dailySpyHighs = alignedDailyRows.map((row) => toNumber(row.barBySymbol?.SPY?.high));
  const dailySpyLows = alignedDailyRows.map((row) => toNumber(row.barBySymbol?.SPY?.low));
  const dailySpyReturns = dailySpyCloses.map((_, index) => computeRollingReturn(dailySpyCloses, index, 1));
  return alignedDailyRows.map((row, index) => {
    const dailyWindow = dailyVixValues.slice(Math.max(0, index - 251), index + 1);
    const spyOpen = dailySpyOpens[index];
    const spyHigh = dailySpyHighs[index];
    const spyLow = dailySpyLows[index];
    const spyClose = dailySpyCloses[index];
    const featureRow = {
      resolution: 'day',
      timestamp: `${row.tradeDateUtc}T21:00:00.000Z`,
      tradeDateUtc: row.tradeDateUtc,
      spxClose: toNumber(row.closeBySymbol.SPX),
      spyOpen,
      spyHigh,
      spyLow,
      spyClose,
      spxlClose: toNumber(row.closeBySymbol.SPXL),
      spxsClose: toNumber(row.closeBySymbol.SPXS),
      vix: toNumber(row.closeBySymbol.VIX),
      vix9d: toNumber(row.closeBySymbol.VIX9D),
      vix1d: toNumber(row.closeBySymbol.VIX1D),
      vix3m: toNumber(row.closeBySymbol.VIX3M),
      delta5: computeRollingDelta(dailyVixValues, index, 5),
      delta10: computeRollingDelta(dailyVixValues, index, 10),
      vixPctRank: dailyWindow.filter((value) => toNumber(value) !== null).length >= MIN_PERCENTILE_WINDOW
        ? percentileRank(dailyWindow, row.closeBySymbol.VIX)
        : null,
      spyReturn1d: computeRollingReturn(dailySpyCloses, index, 1),
      spyReturn3d: computeRollingReturn(dailySpyCloses, index, 3),
      spyReturn5d: computeRollingReturn(dailySpyCloses, index, 5),
      spyReturn10d: computeRollingReturn(dailySpyCloses, index, 10),
      spyMa5: computeRollingAverage(dailySpyCloses, index, 5),
      spyMa10: computeRollingAverage(dailySpyCloses, index, 10),
      spyMa20: computeRollingAverage(dailySpyCloses, index, 20),
      spyRealizedVol5: computeRollingRealizedVol(dailySpyReturns, index, 5),
      spyRealizedVol10: computeRollingRealizedVol(dailySpyReturns, index, 10),
      spyRealizedVol20: computeRollingRealizedVol(dailySpyReturns, index, 20),
      spyDownsideVol10: computeDownsideVol(dailySpyReturns, index, 10),
      spyDownsideVol20: computeDownsideVol(dailySpyReturns, index, 20),
      spyIntradayReturn: spyOpen !== null && spyOpen > 0 && spyClose !== null ? (spyClose / spyOpen) - 1 : null,
      spyRangePct: spyOpen !== null && spyOpen > 0 && spyHigh !== null && spyLow !== null ? (spyHigh - spyLow) / spyOpen : null,
      spyGapFromPrevClose: spyOpen !== null && index > 0 && dailySpyCloses[index - 1] !== null && dailySpyCloses[index - 1] > 0
        ? (spyOpen / dailySpyCloses[index - 1]) - 1
        : null,
      vixChange1d: computeRollingDelta(dailyVixValues, index, 1),
      vixChange3d: computeRollingDelta(dailyVixValues, index, 3),
      vix9dChange1d: computeRollingDelta(dailyVix9dValues, index, 1),
      vix1dChange1d: computeRollingDelta(dailyVix1dValues, index, 1),
      vix3mChange1d: computeRollingDelta(dailyVix3mValues, index, 1),
    };
    featureRow.ts9d30 = safeRatio(featureRow.vix9d, featureRow.vix);
    featureRow.ts30d90d = safeRatio(featureRow.vix, featureRow.vix3m);
    featureRow.ts1d9d = safeRatio(featureRow.vix1d, featureRow.vix9d);
    featureRow.vix1dOverVix = safeRatio(featureRow.vix1d, featureRow.vix);
    featureRow.spyMaGap5 = featureRow.spyMa5 !== null && spyClose !== null && featureRow.spyMa5 !== 0 ? (spyClose / featureRow.spyMa5) - 1 : null;
    featureRow.spyMaGap10 = featureRow.spyMa10 !== null && spyClose !== null && featureRow.spyMa10 !== 0 ? (spyClose / featureRow.spyMa10) - 1 : null;
    featureRow.spyMaGap20 = featureRow.spyMa20 !== null && spyClose !== null && featureRow.spyMa20 !== 0 ? (spyClose / featureRow.spyMa20) - 1 : null;
    featureRow.spyCloseLocation = (
      spyClose !== null
      && spyHigh !== null
      && spyLow !== null
      && spyHigh > spyLow
    ) ? (spyClose - spyLow) / (spyHigh - spyLow) : null;
    featureRow.ts9d30Delta1 = index > 0 && featureRow.ts9d30 !== null
      ? featureRow.ts9d30 - safeRatio(dailyVix9dValues[index - 1], dailyVixValues[index - 1])
      : null;
    featureRow.ts30d90dDelta1 = index > 0 && featureRow.ts30d90d !== null
      ? featureRow.ts30d90d - safeRatio(dailyVixValues[index - 1], dailyVix3mValues[index - 1])
      : null;
    featureRow.ts1d9dDelta1 = index > 0 && featureRow.ts1d9d !== null
      ? featureRow.ts1d9d - safeRatio(dailyVix1dValues[index - 1], dailyVix9dValues[index - 1])
      : null;
    featureRow.vixRiskPremium10 = featureRow.spyRealizedVol10 !== null ? featureRow.vix - (featureRow.spyRealizedVol10 * 100) : null;
    featureRow.vixRiskPremium20 = featureRow.spyRealizedVol20 !== null ? featureRow.vix - (featureRow.spyRealizedVol20 * 100) : null;
    const classification = classifyRegime(featureRow, thresholdConfig.day, thresholdConfig.exposures);
    return {
      ...featureRow,
      regime: classification.regime,
      exposure: classification.exposure,
      reasons: classification.reasons,
    };
  });
}

function buildMinuteFeatures(alignedMinuteRows = [], thresholdConfig, dailyFeatures = []) {
  const vixHistory = [];
  const currentDayVixHistory = [];
  let lastDay = null;
  const dailyClosesBeforeMinute = new Map();

  dailyFeatures.forEach((row) => {
    dailyClosesBeforeMinute.set(row.tradeDateUtc, row);
  });

  return alignedMinuteRows.map((row, index) => {
    const vix = toNumber(row.prices.VIX);
    if (lastDay !== row.tradeDateUtc) {
      currentDayVixHistory.length = 0;
      lastDay = row.tradeDateUtc;
    }
    const priorDailyRows = dailyFeatures
      .filter((dailyRow) => dailyRow.tradeDateUtc < row.tradeDateUtc)
      .slice(-252)
      .map((dailyRow) => dailyRow.vix)
      .filter((value) => value !== null);
    const causalPercentileWindow = priorDailyRows.concat(currentDayVixHistory);
    const featureRow = {
      resolution: 'minute',
      timestamp: row.minuteUtc,
      tradeDateUtc: row.tradeDateUtc,
      spxClose: toNumber(row.prices.SPX),
      spyClose: toNumber(row.prices.SPY),
      spxlClose: toNumber(row.prices.SPXL),
      spxsClose: toNumber(row.prices.SPXS),
      vix,
      vix9d: toNumber(row.prices.VIX9D),
      vix1d: toNumber(row.prices.VIX1D),
      vix3m: toNumber(row.prices.VIX3M),
      delta30: vix !== null && vixHistory.length >= 30 ? vix - vixHistory[vixHistory.length - 30] : null,
      delta60: vix !== null && vixHistory.length >= 60 ? vix - vixHistory[vixHistory.length - 60] : null,
      vixPctRank: causalPercentileWindow.filter((value) => toNumber(value) !== null).length >= MIN_PERCENTILE_WINDOW
        ? percentileRank(causalPercentileWindow, vix)
        : null,
    };
    featureRow.ts9d30 = safeRatio(featureRow.vix9d, featureRow.vix);
    featureRow.ts30d90d = safeRatio(featureRow.vix, featureRow.vix3m);
    featureRow.ts1d9d = safeRatio(featureRow.vix1d, featureRow.vix9d);
    featureRow.vix1dOverVix = safeRatio(featureRow.vix1d, featureRow.vix);
    const classification = classifyRegime(featureRow, thresholdConfig.minute, thresholdConfig.exposures);
    if (vix !== null) {
      vixHistory.push(vix);
      currentDayVixHistory.push(vix);
    }
    return {
      ...featureRow,
      regime: classification.regime,
      exposure: classification.exposure,
      reasons: classification.reasons,
      minuteIndex: index,
    };
  });
}

function applyDailyThresholdConfig(dailyFeatures = [], thresholdConfig) {
  return dailyFeatures.map((featureRow) => {
    const classification = classifyRegime(featureRow, thresholdConfig.day, thresholdConfig.exposures);
    return {
      ...featureRow,
      regime: classification.regime,
      exposure: classification.exposure,
      reasons: classification.reasons,
    };
  });
}

function exposureToWeights(exposure) {
  const e = toNumber(exposure) ?? 0;
  const weights = { spy: 0, spxl: 0, spxs: 0, cash: 0 };
  if (e >= 1) {
    weights.spxl = Math.min(1, Math.max(0, (e - 1) / 2));
    weights.spy = Math.max(0, 1 - weights.spxl);
  } else if (e >= 0) {
    weights.spy = e;
    weights.cash = 1 - e;
  } else {
    weights.spxs = Math.min(1, Math.max(0, (-e) / 3));
    weights.cash = 1 - weights.spxs;
  }
  return weights;
}

function computeReturn(currentPrice, nextPrice) {
  const current = toNumber(currentPrice);
  const next = toNumber(nextPrice);
  if (current === null || next === null || current <= 0) return null;
  return (next / current) - 1;
}

function computePortfolioPath(featureRows = [], options = {}) {
  const transactionCostBps = toNumber(options.transactionCostBps) ?? 0;
  const periodsPerYear = Number.isFinite(options.periodsPerYear) ? options.periodsPerYear : 252;
  const signalTiming = String(options.signalTiming || 'bar_close').trim();
  const executionTiming = String(options.executionTiming || 'same_close_for_next_bar_return').trim();
  const observations = [];
  let equityCurve = 1;
  let benchmarkCurve = 1;
  let priorWeights = exposureToWeights(0);
  let maxEquity = equityCurve;
  let maxDrawdown = 0;
  let maxDrawdownDuration = 0;
  let currentDrawdownDuration = 0;
  let wins = 0;
  let total = 0;
  let turnoverTotal = 0;
  const regimeCounts = Object.fromEntries(REGIME_ORDER.map((regime) => [regime, 0]));
  let whipsawCount = 0;

  for (let index = 0; index < featureRows.length - 1; index += 1) {
    const current = featureRows[index];
    const next = featureRows[index + 1];
    regimeCounts[current.regime] = (regimeCounts[current.regime] || 0) + 1;
    const targetWeights = exposureToWeights(current.exposure);
    const turnover = (
      Math.abs(targetWeights.spy - priorWeights.spy)
      + Math.abs(targetWeights.spxl - priorWeights.spxl)
      + Math.abs(targetWeights.spxs - priorWeights.spxs)
      + Math.abs(targetWeights.cash - priorWeights.cash)
    ) / 2;
    turnoverTotal += turnover;
    const transactionCost = turnover * (transactionCostBps / 10000);
    const legSpy = computeReturn(current.spyClose, next.spyClose);
    const legSpxl = computeReturn(current.spxlClose, next.spxlClose);
    const legSpxs = computeReturn(current.spxsClose, next.spxsClose);
    const benchmarkRet = computeReturn(current.spyClose, next.spyClose);
    const portfolioRetRaw = (
      (targetWeights.spy * (legSpy ?? 0))
      + (targetWeights.spxl * (legSpxl ?? 0))
      + (targetWeights.spxs * (legSpxs ?? 0))
    );
    const portfolioRet = portfolioRetRaw - transactionCost;

    equityCurve *= (1 + portfolioRet);
    benchmarkCurve *= (1 + (benchmarkRet ?? 0));
    maxEquity = Math.max(maxEquity, equityCurve);
    const drawdown = maxEquity > 0 ? (equityCurve / maxEquity) - 1 : 0;
    if (drawdown < 0) {
      currentDrawdownDuration += 1;
      maxDrawdownDuration = Math.max(maxDrawdownDuration, currentDrawdownDuration);
      maxDrawdown = Math.min(maxDrawdown, drawdown);
    } else {
      currentDrawdownDuration = 0;
    }
    if (portfolioRet > 0) wins += 1;
    total += 1;

    if (index > 0 && featureRows[index - 1].regime !== current.regime && next.regime === featureRows[index - 1].regime) {
      whipsawCount += 1;
    }

    observations.push({
      timestamp: current.timestamp,
      signalTimestamp: current.timestamp,
      signalTradeDateUtc: current.tradeDateUtc,
      tradeDateUtc: current.tradeDateUtc,
      holdingPeriodStartTimestamp: current.timestamp,
      holdingPeriodStartTradeDateUtc: current.tradeDateUtc,
      holdingPeriodEndTimestamp: next.timestamp,
      holdingPeriodEndTradeDateUtc: next.tradeDateUtc,
      executionTiming,
      signalTiming,
      regime: current.regime,
      exposure: current.exposure,
      weights: targetWeights,
      portfolioReturn: portfolioRet,
      benchmarkReturn: benchmarkRet,
      equityCurve,
      benchmarkCurve,
      turnover,
      reasons: current.reasons,
    });
    priorWeights = targetWeights;
  }

  const returns = observations.map((row) => row.portfolioReturn).filter((value) => value !== null);
  const benchmarkReturns = observations.map((row) => row.benchmarkReturn).filter((value) => value !== null);
  const averageReturn = returns.length ? returns.reduce((acc, value) => acc + value, 0) / returns.length : 0;
  const downsideReturns = returns.filter((value) => value < 0);
  const variance = returns.length
    ? returns.reduce((acc, value) => acc + ((value - averageReturn) ** 2), 0) / returns.length
    : 0;
  const downsideVariance = downsideReturns.length
    ? downsideReturns.reduce((acc, value) => acc + (value ** 2), 0) / downsideReturns.length
    : 0;
  const annualizedVol = Math.sqrt(Math.max(0, variance)) * Math.sqrt(periodsPerYear);
  const annualizedDownsideVol = Math.sqrt(Math.max(0, downsideVariance)) * Math.sqrt(periodsPerYear);
  const years = observations.length / periodsPerYear;
  const cagr = years > 0 ? (equityCurve ** (1 / years)) - 1 : 0;
  const benchmarkYears = benchmarkReturns.length / periodsPerYear;
  const benchmarkCagr = benchmarkYears > 0 ? (benchmarkCurve ** (1 / benchmarkYears)) - 1 : 0;
  const sharpe = annualizedVol > 0 ? ((averageReturn * periodsPerYear) / annualizedVol) : null;
  const sortino = annualizedDownsideVol > 0 ? ((averageReturn * periodsPerYear) / annualizedDownsideVol) : null;
  const calmar = maxDrawdown < 0 ? cagr / Math.abs(maxDrawdown) : null;

  return {
    observations,
    summary: {
      executionConvention: {
        signalTiming,
        executionTiming,
        returnWindow: 'current_bar_close_to_next_bar_close',
        interpretation: 'Regime is computed on the current bar close and weights are applied to the following bar return window.',
      },
      periods: observations.length,
      periodsPerYear,
      years,
      endingEquity: equityCurve,
      benchmarkEndingEquity: benchmarkCurve,
      cagr,
      benchmarkCagr,
      annualizedVol,
      sharpe,
      sortino,
      maxDrawdown,
      calmar,
      maxDrawdownDuration,
      turnover: turnoverTotal,
      hitRate: total > 0 ? wins / total : null,
      regimeOccupancy: regimeCounts,
      whipsawCount,
    },
  };
}

function computeDailyNextOpenToClosePath(dailyFeatureRows = [], dailyEntryRows = [], options = {}) {
  const transactionCostBps = toNumber(options.transactionCostBps) ?? 0;
  const periodsPerYear = Number.isFinite(options.periodsPerYear) ? options.periodsPerYear : 252;
  const signalTiming = String(options.signalTiming || 'day_close').trim();
  const executionTiming = String(options.executionTiming || 'next_day_first_minute_to_close').trim();
  const entryByDay = new Map((dailyEntryRows || []).map((row) => [row.tradeDateUtc, row.closeBySymbol || {}]));
  const observations = [];
  let equityCurve = 1;
  let benchmarkCurve = 1;
  let priorWeights = exposureToWeights(0);
  let maxEquity = equityCurve;
  let maxDrawdown = 0;
  let maxDrawdownDuration = 0;
  let currentDrawdownDuration = 0;
  let wins = 0;
  let total = 0;
  let turnoverTotal = 0;
  const regimeCounts = Object.fromEntries(REGIME_ORDER.map((regime) => [regime, 0]));
  let whipsawCount = 0;

  for (let index = 0; index < dailyFeatureRows.length - 1; index += 1) {
    const current = dailyFeatureRows[index];
    const next = dailyFeatureRows[index + 1];
    const nextEntry = entryByDay.get(next.tradeDateUtc);
    if (!nextEntry) continue;

    const spyEntry = toNumber(nextEntry.SPY);
    const spxlEntry = toNumber(nextEntry.SPXL);
    const spxsEntry = toNumber(nextEntry.SPXS);
    if (spyEntry === null || spxlEntry === null || spxsEntry === null) continue;

    regimeCounts[current.regime] = (regimeCounts[current.regime] || 0) + 1;
    const targetWeights = exposureToWeights(current.exposure);
    const turnover = (
      Math.abs(targetWeights.spy - priorWeights.spy)
      + Math.abs(targetWeights.spxl - priorWeights.spxl)
      + Math.abs(targetWeights.spxs - priorWeights.spxs)
      + Math.abs(targetWeights.cash - priorWeights.cash)
    ) / 2;
    turnoverTotal += turnover;
    const transactionCost = turnover * (transactionCostBps / 10000);

    const legSpy = computeReturn(spyEntry, next.spyClose);
    const legSpxl = computeReturn(spxlEntry, next.spxlClose);
    const legSpxs = computeReturn(spxsEntry, next.spxsClose);
    const benchmarkRet = computeReturn(spyEntry, next.spyClose);
    const portfolioRetRaw = (
      (targetWeights.spy * (legSpy ?? 0))
      + (targetWeights.spxl * (legSpxl ?? 0))
      + (targetWeights.spxs * (legSpxs ?? 0))
    );
    const portfolioRet = portfolioRetRaw - transactionCost;

    equityCurve *= (1 + portfolioRet);
    benchmarkCurve *= (1 + (benchmarkRet ?? 0));
    maxEquity = Math.max(maxEquity, equityCurve);
    const drawdown = maxEquity > 0 ? (equityCurve / maxEquity) - 1 : 0;
    if (drawdown < 0) {
      currentDrawdownDuration += 1;
      maxDrawdownDuration = Math.max(maxDrawdownDuration, currentDrawdownDuration);
      maxDrawdown = Math.min(maxDrawdown, drawdown);
    } else {
      currentDrawdownDuration = 0;
    }
    if (portfolioRet > 0) wins += 1;
    total += 1;

    if (index > 0 && dailyFeatureRows[index - 1].regime !== current.regime && next.regime === dailyFeatureRows[index - 1].regime) {
      whipsawCount += 1;
    }

    observations.push({
      timestamp: current.timestamp,
      signalTimestamp: current.timestamp,
      signalTradeDateUtc: current.tradeDateUtc,
      tradeDateUtc: next.tradeDateUtc,
      holdingPeriodStartTradeDateUtc: next.tradeDateUtc,
      holdingPeriodEndTradeDateUtc: next.tradeDateUtc,
      signalTiming,
      executionTiming,
      regime: current.regime,
      exposure: current.exposure,
      weights: targetWeights,
      portfolioReturn: portfolioRet,
      benchmarkReturn: benchmarkRet,
      equityCurve,
      benchmarkCurve,
      turnover,
      reasons: current.reasons,
      entryPrices: {
        spy: spyEntry,
        spxl: spxlEntry,
        spxs: spxsEntry,
      },
      exitPrices: {
        spy: next.spyClose,
        spxl: next.spxlClose,
        spxs: next.spxsClose,
      },
    });
    priorWeights = targetWeights;
  }

  const returns = observations.map((row) => row.portfolioReturn).filter((value) => value !== null);
  const benchmarkReturns = observations.map((row) => row.benchmarkReturn).filter((value) => value !== null);
  const averageReturn = returns.length ? returns.reduce((acc, value) => acc + value, 0) / returns.length : 0;
  const downsideReturns = returns.filter((value) => value < 0);
  const variance = returns.length
    ? returns.reduce((acc, value) => acc + ((value - averageReturn) ** 2), 0) / returns.length
    : 0;
  const downsideVariance = downsideReturns.length
    ? downsideReturns.reduce((acc, value) => acc + (value ** 2), 0) / downsideReturns.length
    : 0;
  const annualizedVol = Math.sqrt(Math.max(0, variance)) * Math.sqrt(periodsPerYear);
  const annualizedDownsideVol = Math.sqrt(Math.max(0, downsideVariance)) * Math.sqrt(periodsPerYear);
  const years = observations.length / periodsPerYear;
  const cagr = years > 0 ? (equityCurve ** (1 / years)) - 1 : 0;
  const benchmarkYears = benchmarkReturns.length / periodsPerYear;
  const benchmarkCagr = benchmarkYears > 0 ? (benchmarkCurve ** (1 / benchmarkYears)) - 1 : 0;
  const sharpe = annualizedVol > 0 ? ((averageReturn * periodsPerYear) / annualizedVol) : null;
  const sortino = annualizedDownsideVol > 0 ? ((averageReturn * periodsPerYear) / annualizedDownsideVol) : null;
  const calmar = maxDrawdown < 0 ? cagr / Math.abs(maxDrawdown) : null;

  return {
    observations,
    summary: {
      executionConvention: {
        signalTiming,
        executionTiming,
        returnWindow: 'next_day_first_minute_to_same_day_close',
        interpretation: 'Regime is computed on day X close, execution occurs on the first minute of day X+1, and weights are held until day X+1 close.',
      },
      periods: observations.length,
      periodsPerYear,
      years,
      endingEquity: equityCurve,
      benchmarkEndingEquity: benchmarkCurve,
      cagr,
      benchmarkCagr,
      annualizedVol,
      sharpe,
      sortino,
      maxDrawdown,
      calmar,
      maxDrawdownDuration,
      turnover: turnoverTotal,
      hitRate: total > 0 ? wins / total : null,
      regimeOccupancy: regimeCounts,
      whipsawCount,
    },
  };
}

module.exports = {
  DEFAULT_REQUIRED_SYMBOLS,
  TRADING_MINUTES_PER_DAY,
  readThresholdConfig,
  computeCoverageReport,
  buildMinuteAlignment,
  buildDailyCloses,
  buildDailyFirstMinuteCloses,
  buildDailyFeatures,
  applyDailyThresholdConfig,
  buildMinuteFeatures,
  exposureToWeights,
  computePortfolioPath,
  computeDailyNextOpenToClosePath,
  classifyRegime,
  percentileRank,
  computeRollingDelta,
  safeRatio,
};
