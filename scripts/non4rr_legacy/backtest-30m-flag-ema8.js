#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const {
  resolveFlowReadBackend,
  buildArtifactPath,
  queryRowsSync,
} = require('../../src/storage/clickhouse');

const DEFAULT_FROM_ISO = '2025-09-01';
const DEFAULT_EMA_PERIOD = 8;
const DEFAULT_MORNING_END = '11:30';
const DEFAULT_LUNCH_START = '11:30';
const DEFAULT_LUNCH_END = '14:00';
const DEFAULT_MIN_MORNING_MOVE_PCT = 1.5;
const DEFAULT_MIN_PULLBACK_PCT = 0.4;
const DEFAULT_TOUCH_TOLERANCE_PCT = 0.15;
const DEFAULT_MIN_RISK_PCT = 0.08;
const DEFAULT_CHUNK_SIZE = 300;
const DEFAULT_R_TARGET = 9;
const DEFAULT_SYMBOLS_PATH = path.join(process.cwd(), 'config', 'top200-universe.json');
const DEFAULT_EXIT_MODEL = 'close_or_stop';
const MINUTE_MS = 60 * 1000;
const BAR_MS = 30 * MINUTE_MS;

const EXIT_MODELS = new Set([
  'close_or_stop',
  'hod_or_stop',
  'scaleout_1r_2r_be',
]);

const ETF_SYMBOLS = new Set([
  'SPY', 'QQQ', 'IWM', 'DIA',
  'XLF', 'XLK', 'XLE', 'XLI', 'XLY', 'XLC', 'XLP', 'XLV', 'XLB', 'XLU', 'XLRE',
  'SMH', 'SOXX', 'ARKK', 'TLT', 'GLD', 'SLV', 'USO', 'UNG', 'HYG', 'LQD',
  'VXX', 'UVXY', 'BITO',
]);

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      out[key] = '1';
      continue;
    }
    out[key] = next;
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

function resolveExitModel(value, fallback = DEFAULT_EXIT_MODEL) {
  const raw = String(value || '').trim().toLowerCase();
  if (EXIT_MODELS.has(raw)) return raw;
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

function hhmmToMinute(value, fallback) {
  const raw = String(value || '').trim();
  const match = raw.match(/^(\d{2}):(\d{2})$/);
  if (!match) return fallback;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return fallback;
  if (hour < 0 || hour > 23 || minute < 0 || minute > 59) return fallback;
  return (hour * 60) + minute;
}

function parseDateIso(value, fallback) {
  const raw = String(value || '').trim();
  if (!raw) return fallback;
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : fallback;
}

function ensureDir(filePath) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
}

function escapeSqlString(value) {
  return String(value).replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

function buildSymbolInClause(symbols) {
  if (!Array.isArray(symbols) || symbols.length === 0) return "''";
  return symbols.map((symbol) => `'${escapeSqlString(symbol)}'`).join(',');
}

function buildPairInClause(pairs) {
  if (!Array.isArray(pairs) || pairs.length === 0) return "('1900-01-01','__NONE__')";
  return pairs
    .map(({ dayEt, symbol }) => `('${escapeSqlString(dayEt)}','${escapeSqlString(symbol)}')`)
    .join(',');
}

function parseUtcToMs(value) {
  if (value === null || value === undefined) return Number.NaN;
  const iso = `${String(value).trim().replace(' ', 'T')}Z`;
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? ms : Number.NaN;
}

function parseMinuteOfDayFromEtTimestamp(value) {
  const raw = String(value || '');
  const match = raw.match(/\s(\d{2}):(\d{2}):\d{2}$/);
  if (!match) return null;
  return (Number(match[1]) * 60) + Number(match[2]);
}

function median(values) {
  if (!Array.isArray(values) || values.length === 0) return null;
  const sorted = values
    .filter((value) => Number.isFinite(value))
    .slice()
    .sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const mid = Math.floor(sorted.length / 2);
  if ((sorted.length % 2) === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

function quantile(values, p) {
  if (!Array.isArray(values) || values.length === 0) return null;
  const sorted = values
    .filter((value) => Number.isFinite(value))
    .slice()
    .sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const q = Math.max(0, Math.min(1, p));
  const idx = (sorted.length - 1) * q;
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return sorted[lo];
  const weight = idx - lo;
  return (sorted[lo] * (1 - weight)) + (sorted[hi] * weight);
}

function avg(values) {
  const finite = values.filter((value) => Number.isFinite(value));
  if (finite.length === 0) return null;
  const total = finite.reduce((sum, value) => sum + value, 0);
  return total / finite.length;
}

function sum(values) {
  const finite = values.filter((value) => Number.isFinite(value));
  if (finite.length === 0) return null;
  return finite.reduce((acc, value) => acc + value, 0);
}

function summarizeTrades(trades, rTarget) {
  const count = trades.length;
  if (count === 0) {
    return {
      trades: 0,
      stopHitRate: null,
      reach1RRate: null,
      reach2RRate: null,
      reach3RRate: null,
      reach5RRate: null,
      reach9RRate: null,
      priorHodHitRate: null,
      avgMaxR: null,
      medianMaxR: null,
      p90MaxR: null,
      avgCloseOrStopR: null,
      avgTargetPriorHodR: null,
      avgMorningMovePct: null,
      avgPullbackPct: null,
      maxRBest: null,
      worstCloseOrStopR: null,
      bestCloseOrStopR: null,
      medianCloseOrStopR: null,
      totalCloseOrStopR: null,
      positiveCloseOrStopRate: null,
    };
  }

  const maxRs = trades.map((trade) => trade.maxR);
  const closeOrStopRs = trades.map((trade) => trade.closeOrStopR);
  const targetR = trades.map((trade) => trade.targetPriorHodR);
  const morningMoves = trades.map((trade) => trade.morningMovePct);
  const pullbacks = trades.map((trade) => trade.pullbackPct);
  const stopHits = trades.filter((trade) => trade.stopHit).length;
  const reach = (threshold) => trades.filter((trade) => trade.maxR >= threshold).length / count;

  return {
    trades: count,
    stopHitRate: stopHits / count,
    reach1RRate: reach(1),
    reach2RRate: reach(2),
    reach3RRate: reach(3),
    reach5RRate: reach(5),
    reach9RRate: reach(rTarget),
    priorHodHitRate: trades.filter((trade) => trade.priorHodHit).length / count,
    avgMaxR: avg(maxRs),
    medianMaxR: median(maxRs),
    p90MaxR: quantile(maxRs, 0.9),
    avgCloseOrStopR: avg(closeOrStopRs),
    avgTargetPriorHodR: avg(targetR),
    avgMorningMovePct: avg(morningMoves),
    avgPullbackPct: avg(pullbacks),
    maxRBest: quantile(maxRs, 1),
    worstCloseOrStopR: quantile(closeOrStopRs, 0),
    bestCloseOrStopR: quantile(closeOrStopRs, 1),
    medianCloseOrStopR: median(closeOrStopRs),
    totalCloseOrStopR: sum(closeOrStopRs),
    positiveCloseOrStopRate: trades.filter((trade) => trade.closeOrStopR > 0).length / count,
  };
}

function summarizeTradesByExitField(trades, rTarget, exitField) {
  const base = summarizeTrades(trades, rTarget);
  const count = trades.length;
  if (count === 0) {
    return {
      ...base,
      exitField,
      avgExitR: null,
      medianExitR: null,
      totalExitR: null,
      positiveExitRate: null,
      bestExitR: null,
      worstExitR: null,
    };
  }

  const exitValues = trades
    .map((trade) => Number(trade[exitField]))
    .filter((value) => Number.isFinite(value));
  return {
    ...base,
    exitField,
    avgExitR: avg(exitValues),
    medianExitR: median(exitValues),
    totalExitR: sum(exitValues),
    positiveExitRate: exitValues.length > 0
      ? (exitValues.filter((value) => value > 0).length / exitValues.length)
      : null,
    bestExitR: quantile(exitValues, 1),
    worstExitR: quantile(exitValues, 0),
  };
}

function normalizeSymbols(rawSymbols = []) {
  const normalized = rawSymbols
    .map((value) => String(value || '').trim().toUpperCase())
    .filter(Boolean);
  return Array.from(new Set(normalized));
}

function parseSymbolsCsv(rawValue) {
  const raw = String(rawValue || '').trim();
  if (!raw) return [];
  return normalizeSymbols(raw.split(','));
}

function loadConfiguredSymbols(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(`symbols_file_not_array:${filePath}`);
  }
  return normalizeSymbols(parsed);
}

function summarizeBySymbol(trades, rTarget, exitField) {
  const buildWinLossStats = (exitValues = []) => {
    const wins = exitValues.filter((value) => value > 0);
    const losses = exitValues.filter((value) => value < 0);
    const breakeven = exitValues.filter((value) => value === 0);
    return {
      winningTrades: wins.length,
      losingTrades: losses.length,
      breakevenTrades: breakeven.length,
      avgWinR: wins.length > 0 ? avg(wins) : null,
      avgLossR: losses.length > 0 ? avg(losses) : null,
      winRate: exitValues.length > 0 ? (wins.length / exitValues.length) : null,
      lossRate: exitValues.length > 0 ? (losses.length / exitValues.length) : null,
      breakevenRate: exitValues.length > 0 ? (breakeven.length / exitValues.length) : null,
    };
  };

  const bySymbol = new Map();
  trades.forEach((trade) => {
    if (!bySymbol.has(trade.symbol)) bySymbol.set(trade.symbol, []);
    bySymbol.get(trade.symbol).push(trade);
  });

  const rows = Array.from(bySymbol.entries()).map(([symbol, symbolTrades]) => {
    const summary = summarizeTradesByExitField(symbolTrades, rTarget, exitField);
    const segmentSet = new Set(symbolTrades.map((trade) => trade.segment));
    const segment = segmentSet.size === 1 ? symbolTrades[0].segment : 'mixed';
    const exitValues = symbolTrades
      .map((trade) => Number(trade[exitField]))
      .filter((value) => Number.isFinite(value));
    const winLossStats = buildWinLossStats(exitValues);
    return {
      symbol,
      segment,
      ...summary,
      ...winLossStats,
    };
  });

  rows.sort((a, b) => {
    const aScore = Number.isFinite(a.avgExitR) ? a.avgExitR : -Infinity;
    const bScore = Number.isFinite(b.avgExitR) ? b.avgExitR : -Infinity;
    if (aScore !== bScore) return bScore - aScore;
    return a.symbol.localeCompare(b.symbol);
  });

  return rows;
}

function queryDateCoverage() {
  const rows = queryRowsSync(`
    SELECT
      toString(min(trade_date_utc)) AS min_day,
      toString(max(trade_date_utc)) AS max_day
    FROM options.stock_ohlc_minute_raw
  `);
  return rows[0] || { min_day: null, max_day: null };
}

function queryAvailableSymbols(fromIso, toIso, universeSymbols) {
  const symbolInClause = buildSymbolInClause(universeSymbols);
  const rows = queryRowsSync(`
    SELECT symbol
    FROM options.stock_ohlc_minute_raw
    WHERE trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
      AND symbol IN (${symbolInClause})
    GROUP BY symbol
    ORDER BY symbol ASC
  `, { fromIso, toIso });
  return rows
    .map((row) => String(row.symbol || '').trim().toUpperCase())
    .filter(Boolean);
}

function query30mBars(fromIso, toIso, symbols) {
  if (symbols.length === 0) return [];
  const symbolInClause = buildSymbolInClause(symbols);
  return queryRowsSync(`
    SELECT
      s.symbol AS symbol,
      toString(toDate(s.minute_bucket_utc)) AS trade_day_et,
      toString(toStartOfInterval(s.minute_bucket_utc, INTERVAL 30 minute)) AS bar_start_et,
      toString(toStartOfInterval(s.minute_bucket_utc, INTERVAL 30 minute)) AS bar_start_utc,
      argMin(ifNull(s.open, s.close), s.minute_bucket_utc) AS open,
      max(ifNull(s.high, s.close)) AS high,
      min(ifNull(s.low, s.close)) AS low,
      argMax(s.close, s.minute_bucket_utc) AS close
    FROM options.stock_ohlc_minute_raw AS s
    WHERE s.trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
      AND s.symbol IN (${symbolInClause})
      AND ((toHour(s.minute_bucket_utc) * 60) + toMinute(s.minute_bucket_utc)) BETWEEN 570 AND 959
    GROUP BY symbol, trade_day_et, bar_start_et, bar_start_utc
    ORDER BY symbol ASC, bar_start_utc ASC
  `, { fromIso, toIso });
}

function attachEmaAndNormalizeBars(rawBars, emaPeriod) {
  const alpha = 2 / (emaPeriod + 1);
  const bySymbol = new Map();

  rawBars.forEach((row) => {
    const symbol = String(row.symbol || '').trim().toUpperCase();
    const dayEt = String(row.trade_day_et || '').trim();
    const barStartEt = String(row.bar_start_et || '').trim();
    const barStartUtc = String(row.bar_start_utc || '').trim();
    const open = Number(row.open);
    const high = Number(row.high);
    const low = Number(row.low);
    const close = Number(row.close);
    const barStartUtcMs = parseUtcToMs(barStartUtc);
    const minuteOfDay = parseMinuteOfDayFromEtTimestamp(barStartEt);
    if (!symbol || !dayEt || !barStartEt || !barStartUtc || !Number.isFinite(barStartUtcMs)) return;
    if (!Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) return;
    if (minuteOfDay === null) return;

    if (!bySymbol.has(symbol)) bySymbol.set(symbol, []);
    bySymbol.get(symbol).push({
      symbol,
      dayEt,
      barStartEt,
      barStartUtc,
      barStartUtcMs,
      minuteOfDay,
      open,
      high,
      low,
      close,
      ema8: null,
    });
  });

  const out = [];
  bySymbol.forEach((bars, symbol) => {
    bars.sort((a, b) => a.barStartUtcMs - b.barStartUtcMs);
    let ema = null;
    bars.forEach((bar) => {
      ema = ema === null ? bar.close : (alpha * bar.close) + ((1 - alpha) * ema);
      bar.ema8 = ema;
      out.push(bar);
    });
  });

  out.sort((a, b) => {
    if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
    return a.barStartUtcMs - b.barStartUtcMs;
  });
  return out;
}

function detectSetups({
  bars,
  morningEndMinute,
  lunchStartMinute,
  lunchEndMinute,
  minMorningMovePct,
  minPullbackPct,
  touchTolerancePct,
}) {
  const diagnostics = {
    symbolDays: 0,
    symbolDaysWithMorningBars: 0,
    symbolDaysPassMorningMove: 0,
    lunchBarsScanned: 0,
    failEma: 0,
    failBelowPriorHigh: 0,
    failTouch: 0,
    failCloseAboveEma: 0,
    failBullBody: 0,
    failPullbackDepth: 0,
    selected: 0,
  };
  const bySymbolDay = new Map();
  bars.forEach((bar) => {
    const key = `${bar.symbol}\t${bar.dayEt}`;
    if (!bySymbolDay.has(key)) bySymbolDay.set(key, []);
    bySymbolDay.get(key).push(bar);
  });

  const setups = [];
  bySymbolDay.forEach((dayBars, key) => {
    diagnostics.symbolDays += 1;
    dayBars.sort((a, b) => a.barStartUtcMs - b.barStartUtcMs);
    if (dayBars.length < 6) return;

    const dayOpen = dayBars[0].open;
    if (!Number.isFinite(dayOpen) || dayOpen <= 0) return;

    const morningBars = dayBars.filter((bar) => bar.minuteOfDay < morningEndMinute);
    if (morningBars.length === 0) return;
    diagnostics.symbolDaysWithMorningBars += 1;

    const morningHigh = morningBars.reduce((maxHigh, bar) => Math.max(maxHigh, bar.high), -Infinity);
    if (!Number.isFinite(morningHigh) || morningHigh <= 0) return;
    const morningMovePct = ((morningHigh / dayOpen) - 1) * 100;
    if (morningMovePct < minMorningMovePct) return;
    diagnostics.symbolDaysPassMorningMove += 1;

    let priorHigh = dayOpen;
    let selected = null;
    for (let i = 0; i < dayBars.length; i += 1) {
      const bar = dayBars[i];
      if (i > 0) priorHigh = Math.max(priorHigh, dayBars[i - 1].high);

      if (bar.minuteOfDay < lunchStartMinute || bar.minuteOfDay > lunchEndMinute) continue;
      diagnostics.lunchBarsScanned += 1;
      if (!Number.isFinite(bar.ema8) || bar.ema8 <= 0) {
        diagnostics.failEma += 1;
        continue;
      }
      if (bar.high >= priorHigh) {
        diagnostics.failBelowPriorHigh += 1;
        continue;
      }
      const touchThreshold = bar.ema8 * (1 + (touchTolerancePct / 100));
      if (bar.low > touchThreshold) {
        diagnostics.failTouch += 1;
        continue;
      }
      if (bar.close <= bar.ema8) {
        diagnostics.failCloseAboveEma += 1;
        continue;
      }
      if (bar.close <= bar.open) {
        diagnostics.failBullBody += 1;
        continue;
      }
      const pullbackPct = ((priorHigh - bar.low) / priorHigh) * 100;
      if (!Number.isFinite(pullbackPct) || pullbackPct < minPullbackPct) {
        diagnostics.failPullbackDepth += 1;
        continue;
      }
      const entryTsMs = bar.barStartUtcMs + BAR_MS;
      selected = {
        symbol: bar.symbol,
        dayEt: bar.dayEt,
        setupBarStartEt: bar.barStartEt,
        setupBarStartUtc: bar.barStartUtc,
        setupBarStartUtcMs: bar.barStartUtcMs,
        entryTsMs,
        stopPrice: bar.ema8,
        morningMovePct,
        pullbackPct,
        priorHigh,
        setupBar: bar,
      };
      break;
    }

    if (selected) {
      const [symbol, dayEt] = key.split('\t');
      setups.push({
        ...selected,
        symbol,
        dayEt,
      });
      diagnostics.selected += 1;
    }
  });

  setups.sort((a, b) => {
    if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
    return a.entryTsMs - b.entryTsMs;
  });
  return { setups, diagnostics };
}

function queryMinuteRowsForSetups({
  fromIso,
  toIso,
  setups,
  chunkSize,
}) {
  const allRows = [];
  if (setups.length === 0) return allRows;

  const uniquePairs = Array.from(new Map(
    setups.map((setup) => [`${setup.dayEt}\t${setup.symbol}`, { dayEt: setup.dayEt, symbol: setup.symbol }]),
  ).values());

  for (let offset = 0; offset < uniquePairs.length; offset += chunkSize) {
    const chunk = uniquePairs.slice(offset, offset + chunkSize);
    const pairInClause = buildPairInClause(chunk);
    const symbolInClause = buildSymbolInClause(Array.from(new Set(chunk.map((row) => row.symbol))));
    const rows = queryRowsSync(`
      SELECT
        s.symbol AS symbol,
        toString(toDate(s.minute_bucket_utc)) AS trade_day_et,
        toString(s.minute_bucket_utc) AS minute_utc,
        ifNull(s.open, s.close) AS open,
        ifNull(s.high, s.close) AS high,
        ifNull(s.low, s.close) AS low,
        s.close AS close
      FROM options.stock_ohlc_minute_raw AS s
      WHERE s.trade_date_utc BETWEEN toDate({fromIso:String}) AND toDate({toIso:String})
        AND s.symbol IN (${symbolInClause})
        AND tuple(toString(toDate(s.minute_bucket_utc)), symbol) IN (${pairInClause})
        AND ((toHour(s.minute_bucket_utc) * 60) + toMinute(s.minute_bucket_utc)) BETWEEN 570 AND 959
      ORDER BY symbol ASC, trade_day_et ASC, s.minute_bucket_utc ASC
    `, { fromIso, toIso });
    rows.forEach((row) => allRows.push(row));
  }

  return allRows;
}

function buildMinuteMap(rawRows) {
  const minuteMap = new Map();
  rawRows.forEach((row) => {
    const symbol = String(row.symbol || '').trim().toUpperCase();
    const dayEt = String(row.trade_day_et || '').trim();
    const tsMs = parseUtcToMs(String(row.minute_utc || '').trim());
    const open = Number(row.open);
    const high = Number(row.high);
    const low = Number(row.low);
    const close = Number(row.close);
    if (!symbol || !dayEt || !Number.isFinite(tsMs)) return;
    if (!Number.isFinite(open) || !Number.isFinite(high) || !Number.isFinite(low) || !Number.isFinite(close)) return;
    const key = `${symbol}\t${dayEt}`;
    if (!minuteMap.has(key)) minuteMap.set(key, []);
    minuteMap.get(key).push({
      tsMs,
      open,
      high,
      low,
      close,
    });
  });

  minuteMap.forEach((rows) => rows.sort((a, b) => a.tsMs - b.tsMs));
  return minuteMap;
}

function computeScaleout1r2rBeR({
  closeR,
  stopHit,
  stopHitTsMs,
  firstHitRMs,
}) {
  const hit1 = Number(firstHitRMs[1]);
  const hit2 = Number(firstHitRMs[2]);
  const hasHit1 = Number.isFinite(hit1);
  const hasHit2 = Number.isFinite(hit2);
  const hasStop = stopHit && Number.isFinite(stopHitTsMs);

  if (!hasHit1) {
    return stopHit ? -1 : closeR;
  }

  let realized = 0.5; // 50% off at +1R
  let runnerWeight = 0.5;
  const stopAfterHit1 = hasStop && stopHitTsMs >= hit1;

  if (hasHit2 && (!hasStop || hit2 < stopHitTsMs)) {
    realized += 0.5; // 25% off at +2R
    runnerWeight = 0.25;
    if (hasStop && stopHitTsMs >= hit2) {
      return realized; // runner stopped at breakeven
    }
    return realized + (runnerWeight * Math.max(closeR, 0));
  }

  if (stopAfterHit1) {
    return realized; // remainder exited at breakeven
  }
  return realized + (runnerWeight * Math.max(closeR, 0));
}

function evaluateTrades({
  setups,
  minuteMap,
  minRiskPct,
  rTarget,
}) {
  const evaluated = [];
  const skipped = {
    noMinuteRows: 0,
    noEntryMinute: 0,
    invalidRisk: 0,
  };

  setups.forEach((setup) => {
    const key = `${setup.symbol}\t${setup.dayEt}`;
    const rows = minuteMap.get(key);
    if (!rows || rows.length === 0) {
      skipped.noMinuteRows += 1;
      return;
    }

    const entryIdx = rows.findIndex((row) => row.tsMs >= setup.entryTsMs);
    if (entryIdx < 0) {
      skipped.noEntryMinute += 1;
      return;
    }

    const entryBar = rows[entryIdx];
    const entryPrice = entryBar.open;
    const stopPrice = setup.stopPrice;
    if (!Number.isFinite(entryPrice) || !Number.isFinite(stopPrice) || entryPrice <= stopPrice) {
      skipped.invalidRisk += 1;
      return;
    }

    const risk = entryPrice - stopPrice;
    const riskPct = (risk / entryPrice) * 100;
    if (!Number.isFinite(riskPct) || riskPct < minRiskPct) {
      skipped.invalidRisk += 1;
      return;
    }

    let stopHit = false;
    let stopHitTsMs = null;
    let maxHigh = entryPrice;
    let priorHodHit = false;
    const firstHitR = {};
    const firstHitRMs = {};

    for (let i = entryIdx; i < rows.length; i += 1) {
      const row = rows[i];
      if (row.low <= stopPrice) {
        stopHit = true;
        stopHitTsMs = row.tsMs;
        break;
      }

      if (row.high > maxHigh) {
        maxHigh = row.high;
      }
      if (!priorHodHit && row.high >= setup.priorHigh) {
        priorHodHit = true;
      }

      for (let r = 1; r <= rTarget; r += 1) {
        if (firstHitR[r]) continue;
        const threshold = entryPrice + (r * risk);
        if (row.high >= threshold) {
          firstHitR[r] = new Date(row.tsMs).toISOString();
          firstHitRMs[r] = row.tsMs;
        }
      }
    }

    const maxR = (maxHigh - entryPrice) / risk;
    const lastClose = rows[rows.length - 1].close;
    const closeR = (lastClose - entryPrice) / risk;
    const closeOrStopR = stopHit ? -1 : closeR;
    const targetPriorHodR = (setup.priorHigh - entryPrice) / risk;
    const hodOrStopR = priorHodHit ? targetPriorHodR : (stopHit ? -1 : closeR);
    const scaleout1r2rBeR = computeScaleout1r2rBeR({
      closeR,
      stopHit,
      stopHitTsMs,
      firstHitRMs,
    });

    evaluated.push({
      symbol: setup.symbol,
      dayEt: setup.dayEt,
      segment: ETF_SYMBOLS.has(setup.symbol) ? 'etf' : 'stock',
      setupBarStartEt: setup.setupBarStartEt,
      entryTsUtc: new Date(setup.entryTsMs).toISOString(),
      stopPrice,
      entryPrice,
      risk,
      riskPct,
      morningMovePct: setup.morningMovePct,
      pullbackPct: setup.pullbackPct,
      priorHigh: setup.priorHigh,
      targetPriorHodR,
      priorHodHit,
      stopHit,
      stopHitTsUtc: stopHitTsMs ? new Date(stopHitTsMs).toISOString() : null,
      stopHitTsMs,
      maxR,
      closeR,
      closeOrStopR,
      hodOrStopR,
      scaleout1r2rBeR,
      reached9R: maxR >= rTarget,
      firstHitR,
      firstHitRMs,
    });
  });

  return { evaluated, skipped };
}

function asCompactTradeView(trade) {
  return {
    symbol: trade.symbol,
    dayEt: trade.dayEt,
    segment: trade.segment,
    setupBarStartEt: trade.setupBarStartEt,
    entryTsUtc: trade.entryTsUtc,
    entryPrice: Number(trade.entryPrice.toFixed(4)),
    stopPrice: Number(trade.stopPrice.toFixed(4)),
    riskPct: Number(trade.riskPct.toFixed(4)),
    morningMovePct: Number(trade.morningMovePct.toFixed(3)),
    pullbackPct: Number(trade.pullbackPct.toFixed(3)),
    targetPriorHodR: Number(trade.targetPriorHodR.toFixed(3)),
    priorHodHit: trade.priorHodHit,
    stopHit: trade.stopHit,
    maxR: Number(trade.maxR.toFixed(3)),
    closeOrStopR: Number(trade.closeOrStopR.toFixed(3)),
    hodOrStopR: Number(trade.hodOrStopR.toFixed(3)),
    scaleout1r2rBeR: Number(trade.scaleout1r2rBeR.toFixed(3)),
    reached9R: trade.reached9R,
  };
}

function run() {
  const args = parseArgs(process.argv);
  const startedAt = nowIso();

  const backend = resolveFlowReadBackend(process.env);
  if (backend !== 'clickhouse') {
    throw new Error(`clickhouse_backend_required:${backend}`);
  }

  const dateCoverage = queryDateCoverage();
  const fromIso = parseDateIso(
    args.from || process.env.FLAG30_FROM || DEFAULT_FROM_ISO,
    DEFAULT_FROM_ISO,
  );
  const fallbackTo = parseDateIso(dateCoverage.max_day, null);
  const toIso = parseDateIso(
    args.to || process.env.FLAG30_TO || fallbackTo || new Date().toISOString().slice(0, 10),
    fallbackTo || new Date().toISOString().slice(0, 10),
  );

  const morningEndMinute = hhmmToMinute(
    args.morningEndEt || process.env.FLAG30_MORNING_END_ET || DEFAULT_MORNING_END,
    hhmmToMinute(DEFAULT_MORNING_END, 690),
  );
  const lunchStartMinute = hhmmToMinute(
    args.lunchStartEt || process.env.FLAG30_LUNCH_START_ET || DEFAULT_LUNCH_START,
    hhmmToMinute(DEFAULT_LUNCH_START, 690),
  );
  const lunchEndMinute = hhmmToMinute(
    args.lunchEndEt || process.env.FLAG30_LUNCH_END_ET || DEFAULT_LUNCH_END,
    hhmmToMinute(DEFAULT_LUNCH_END, 840),
  );
  const minMorningMovePct = parseNumber(
    args.minMorningMovePct || process.env.FLAG30_MIN_MORNING_MOVE_PCT,
    DEFAULT_MIN_MORNING_MOVE_PCT,
  );
  const minPullbackPct = parseNumber(
    args.minPullbackPct || process.env.FLAG30_MIN_PULLBACK_PCT,
    DEFAULT_MIN_PULLBACK_PCT,
  );
  const touchTolerancePct = parseNumber(
    args.touchTolerancePct || process.env.FLAG30_TOUCH_TOLERANCE_PCT,
    DEFAULT_TOUCH_TOLERANCE_PCT,
  );
  const minRiskPct = parseNumber(
    args.minRiskPct || process.env.FLAG30_MIN_RISK_PCT,
    DEFAULT_MIN_RISK_PCT,
  );
  const emaPeriod = Math.max(2, Math.trunc(parseNumber(
    args.emaPeriod || process.env.FLAG30_EMA_PERIOD,
    DEFAULT_EMA_PERIOD,
  )));
  const chunkSize = Math.max(50, Math.trunc(parseNumber(
    args.chunkSize || process.env.FLAG30_QUERY_CHUNK_SIZE,
    DEFAULT_CHUNK_SIZE,
  )));
  const rTarget = Math.max(1, Math.trunc(parseNumber(
    args.rTarget || process.env.FLAG30_R_TARGET,
    DEFAULT_R_TARGET,
  )));
  const exitModel = resolveExitModel(
    args.exitModel || process.env.FLAG30_EXIT_MODEL || DEFAULT_EXIT_MODEL,
    DEFAULT_EXIT_MODEL,
  );
  const exitFieldByModel = {
    close_or_stop: 'closeOrStopR',
    hod_or_stop: 'hodOrStopR',
    scaleout_1r_2r_be: 'scaleout1r2rBeR',
  };
  const selectedExitField = exitFieldByModel[exitModel] || 'closeOrStopR';
  const maxSymbols = Math.max(0, Math.trunc(parseNumber(
    args.maxSymbols || process.env.FLAG30_MAX_SYMBOLS,
    0,
  )));
  const excludeEtfs = parseBooleanLike(
    args.excludeEtfs || process.env.FLAG30_EXCLUDE_ETFS,
    false,
  );
  const includeTrades = parseBooleanLike(
    args.includeTrades || process.env.FLAG30_INCLUDE_TRADES,
    false,
  );

  const inlineSymbols = parseSymbolsCsv(args.symbols || process.env.FLAG30_SYMBOLS);
  const symbolsPath = inlineSymbols.length > 0
    ? 'inline:--symbols'
    : path.resolve(args.symbolsPath || process.env.FLAG30_SYMBOLS_PATH || DEFAULT_SYMBOLS_PATH);
  const configuredSymbols = inlineSymbols.length > 0
    ? inlineSymbols
    : loadConfiguredSymbols(symbolsPath);
  let availableSymbols = queryAvailableSymbols(fromIso, toIso, configuredSymbols);
  if (excludeEtfs) {
    availableSymbols = availableSymbols.filter((symbol) => !ETF_SYMBOLS.has(symbol));
  }
  if (maxSymbols > 0) {
    availableSymbols = availableSymbols.slice(0, maxSymbols);
  }
  const bars = attachEmaAndNormalizeBars(query30mBars(fromIso, toIso, availableSymbols), emaPeriod);
  const {
    setups,
    diagnostics: setupDiagnostics,
  } = detectSetups({
    bars,
    morningEndMinute,
    lunchStartMinute,
    lunchEndMinute,
    minMorningMovePct,
    minPullbackPct,
    touchTolerancePct,
  });

  const minuteRows = queryMinuteRowsForSetups({
    fromIso,
    toIso,
    setups,
    chunkSize,
  });
  const minuteMap = buildMinuteMap(minuteRows);

  const { evaluated, skipped } = evaluateTrades({
    setups,
    minuteMap,
    minRiskPct,
    rTarget,
  });

  const etfTrades = evaluated.filter((trade) => trade.segment === 'etf');
  const stockTrades = evaluated.filter((trade) => trade.segment === 'stock');
  const summaryByExitModel = {};
  Object.entries(exitFieldByModel).forEach(([model, exitField]) => {
    summaryByExitModel[model] = {
      all: summarizeTradesByExitField(evaluated, rTarget, exitField),
      stocks: summarizeTradesByExitField(stockTrades, rTarget, exitField),
      etfs: summarizeTradesByExitField(etfTrades, rTarget, exitField),
    };
  });
  const symbolPerformance = summarizeBySymbol(evaluated, rTarget, selectedExitField);

  const avgoMarch11 = evaluated
    .filter((trade) => trade.symbol === 'AVGO' && trade.dayEt === '2026-03-11')
    .map(asCompactTradeView);
  const avgoAll = evaluated
    .filter((trade) => trade.symbol === 'AVGO')
    .sort((a, b) => b.maxR - a.maxR);

  const topByMaxR = evaluated
    .slice()
    .sort((a, b) => b.maxR - a.maxR)
    .slice(0, 20)
    .map(asCompactTradeView);

  const topBySelectedExitR = evaluated
    .slice()
    .sort((a, b) => b[selectedExitField] - a[selectedExitField])
    .slice(0, 20)
    .map(asCompactTradeView);

  const report = {
    generatedAt: nowIso(),
    startedAt,
    readBackend: backend,
    artifactPath: buildArtifactPath(process.env),
    config: {
      fromIso,
      toIso,
      symbolsPath,
      availableSymbols: availableSymbols.length,
      requestedSymbols: configuredSymbols.length,
      maxSymbolsApplied: maxSymbols,
      excludeEtfs,
      includeTrades,
      emaPeriod,
      morningEndEt: args.morningEndEt || process.env.FLAG30_MORNING_END_ET || DEFAULT_MORNING_END,
      lunchStartEt: args.lunchStartEt || process.env.FLAG30_LUNCH_START_ET || DEFAULT_LUNCH_START,
      lunchEndEt: args.lunchEndEt || process.env.FLAG30_LUNCH_END_ET || DEFAULT_LUNCH_END,
      minMorningMovePct,
      minPullbackPct,
      touchTolerancePct,
      minRiskPct,
      rTarget,
      exitModel,
      exitField: selectedExitField,
      chunkSize,
      conservativeFillRule: 'if stop and target are both touched in same minute, stop is assumed first',
    },
    dataCoverage: dateCoverage,
    pipelineCounts: {
      bars: bars.length,
      setupsDetected: setups.length,
      minuteRowsFetched: minuteRows.length,
      tradesEvaluated: evaluated.length,
      skipped,
      setupDiagnostics,
    },
    summary: summaryByExitModel[exitModel],
    summaryByExitModel,
    symbolPerformance,
    focusChecks: {
      avgo_2026_03_11: avgoMarch11,
      avgo_trade_count: avgoAll.length,
      avgo_top_by_maxR: avgoAll.slice(0, 20).map(asCompactTradeView),
    },
    topTradesByMaxR: topByMaxR,
    topTradesBySelectedExitR: topBySelectedExitR,
  };

  if (includeTrades) {
    report.trades = evaluated
      .map((trade) => ({
        symbol: trade.symbol,
        dayEt: trade.dayEt,
        entryTsUtc: trade.entryTsUtc,
        setupBarStartEt: trade.setupBarStartEt,
        segment: trade.segment,
        riskPct: trade.riskPct,
        morningMovePct: trade.morningMovePct,
        pullbackPct: trade.pullbackPct,
        targetPriorHodR: trade.targetPriorHodR,
        priorHodHit: trade.priorHodHit,
        stopHit: trade.stopHit,
        maxR: trade.maxR,
        closeOrStopR: trade.closeOrStopR,
        hodOrStopR: trade.hodOrStopR,
        scaleout1r2rBeR: trade.scaleout1r2rBeR,
        selectedExitR: trade[selectedExitField],
      }))
      .sort((a, b) => {
        if (a.symbol !== b.symbol) return a.symbol.localeCompare(b.symbol);
        return Date.parse(a.entryTsUtc) - Date.parse(b.entryTsUtc);
      });
  }

  const suffix = nowIso().replace(/[:.]/g, '').replace(/-/g, '');
  const outputPath = path.resolve(
    args.outputPath
      || process.env.FLAG30_OUTPUT_PATH
      || path.join(process.cwd(), 'artifacts', 'reports', `flag30-ema8-backtest-${fromIso}-${toIso}-${suffix}.json`),
  );
  ensureDir(outputPath);
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  const latestPath = path.resolve(
    process.cwd(),
    'artifacts',
    'reports',
    'flag30-ema8-backtest-latest.json',
  );
  ensureDir(latestPath);
  fs.writeFileSync(latestPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log(JSON.stringify({
    outputPath,
    latestPath,
    summary: report.summary,
    pipelineCounts: report.pipelineCounts,
    focusChecks: report.focusChecks,
  }, null, 2));
}

try {
  run();
} catch (error) {
  console.error(error?.stack || error?.message || String(error));
  process.exitCode = 1;
}
