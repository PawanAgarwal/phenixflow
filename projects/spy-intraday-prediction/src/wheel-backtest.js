const fs = require('node:fs');
const { spawn } = require('node:child_process');
const readline = require('node:readline');

const { resolveDatasetSource } = require('./config');
const { readGzipCsv, toNumber } = require('./csv');
const { openCalendarDays } = require('./coverage');
const { parseOpraTicker, daysBetween } = require('./opra');
const { nsToMinuteMs, minuteMsToIso, getEtParts, isRegularSessionMinute } = require('./time');

const DEFAULT_INITIAL_CAPITAL = 1_000_000;
const CHECKPOINT_SCHEMA_VERSION = 'wheel-backtest-checkpoint.v1';

const DEFAULT_STRATEGY_CONFIGS = Object.freeze([
  {
    id: 'cash_put_weekly_5otm',
    mode: 'cash_put',
    label: 'Cash-secured put, 5-10 DTE, 5% OTM target',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.95,
  },
  {
    id: 'cash_put_weekly_10otm',
    mode: 'cash_put',
    label: 'Cash-secured put, 5-10 DTE, 10% OTM target',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
  },
  {
    id: 'wheel_weekly_5otm_put_5otm_call',
    mode: 'wheel',
    label: 'Wheel, 5-10 DTE, 5% OTM put, 5% OTM covered call',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.95,
    callTargetMoneyness: 1.05,
  },
  {
    id: 'wheel_weekly_10otm_put_5otm_call',
    mode: 'wheel',
    label: 'Wheel, 5-10 DTE, 10% OTM put, 5% OTM covered call',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    callTargetMoneyness: 1.05,
  },
  {
    id: 'cash_put_monthly_10otm',
    mode: 'cash_put',
    label: 'Cash-secured put, 25-45 DTE, 10% OTM target',
    minDte: 25,
    maxDte: 45,
    putTargetMoneyness: 0.90,
  },
  {
    id: 'wheel_monthly_10otm_put_5otm_call',
    mode: 'wheel',
    label: 'Wheel, 25-45 DTE, 10% OTM put, 5% OTM covered call',
    minDte: 25,
    maxDte: 45,
    putTargetMoneyness: 0.90,
    callTargetMoneyness: 1.05,
  },
]);

const DEFAULT_EXECUTION = Object.freeze({
  entryMinuteEt: 600,
  entryWindowMinutes: 30,
  minPremium: 0.10,
  minVolume: 1,
  minTransactions: 1,
  premiumHaircutPct: 0.05,
  exitPremiumMarkupPct: 0.05,
  commissionPerContract: 0.65,
  stockSlippageBps: 2,
  maxContractsPerSymbol: 1,
  maxPositionPct: 0.04,
  maxPortfolioUtilization: 0.60,
  maxOpenShortOptions: 25,
});

const EXPERIMENT_STRATEGY_CONFIGS = Object.freeze([
  ...DEFAULT_STRATEGY_CONFIGS,
  {
    id: 'cash_put_weekly_10otm_yield25',
    mode: 'cash_put',
    label: 'Cash put, weekly 10% OTM, annualized premium >=25%',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    maxPutMoneyness: 0.92,
    minAnnualizedPremiumYield: 0.25,
    scoreMode: 'annualized_premium',
  },
  {
    id: 'cash_put_weekly_10otm_iv40_yield25',
    mode: 'cash_put',
    label: 'Cash put, weekly 10% OTM, IV >=40%, annualized premium >=25%',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    maxPutMoneyness: 0.92,
    minImpliedVol: 0.40,
    minAnnualizedPremiumYield: 0.25,
    scoreMode: 'annualized_premium',
  },
  {
    id: 'cash_put_weekly_10otm_ivrv125',
    mode: 'cash_put',
    label: 'Cash put, weekly 10% OTM, IV/RV >=1.25',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    maxPutMoneyness: 0.92,
    minStockHistoryDays: 20,
    minIvToRealizedVol: 1.25,
    scoreMode: 'iv_rv',
  },
  {
    id: 'cash_put_weekly_10otm_ivrank50',
    mode: 'cash_put',
    label: 'Cash put, weekly 10% OTM, rolling IV rank >=50%',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    maxPutMoneyness: 0.92,
    minIvRank: 0.50,
    minIvHistoryObservations: 10,
    scoreMode: 'iv_rank',
  },
  {
    id: 'cash_put_weekly_10otm_delta10_25',
    mode: 'cash_put',
    label: 'Cash put, weekly, put delta 0.10-0.25',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    minPutDeltaAbs: 0.10,
    maxPutDeltaAbs: 0.25,
    targetDeltaAbs: 0.16,
    scoreMode: 'delta_target',
  },
  {
    id: 'cash_put_weekly_10otm_trend_ivrv',
    mode: 'cash_put',
    label: 'Cash put, weekly 10% OTM, uptrend and IV/RV >=1.10',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    maxPutMoneyness: 0.92,
    minStockHistoryDays: 20,
    requireAboveSma20: true,
    minPriorReturn20: 0,
    minIvToRealizedVol: 1.10,
    scoreMode: 'iv_rv',
  },
  {
    id: 'wheel_weekly_10otm_yield25_profit50',
    mode: 'wheel',
    label: 'Wheel weekly 10% OTM, annualized premium >=25%, close at 50% profit',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    callTargetMoneyness: 1.05,
    maxPutMoneyness: 0.92,
    minAnnualizedPremiumYield: 0.25,
    profitTakePct: 0.50,
    scoreMode: 'annualized_premium',
  },
  {
    id: 'wheel_weekly_10otm_trend_ivrv_profit50',
    mode: 'wheel',
    label: 'Wheel weekly 10% OTM, uptrend IV/RV, close at 50% profit',
    minDte: 5,
    maxDte: 10,
    putTargetMoneyness: 0.90,
    callTargetMoneyness: 1.05,
    maxPutMoneyness: 0.92,
    minStockHistoryDays: 20,
    requireAboveSma20: true,
    minPriorReturn20: 0,
    minIvToRealizedVol: 1.10,
    profitTakePct: 0.50,
    scoreMode: 'iv_rv',
  },
  {
    id: 'cash_put_monthly_10otm_yield15',
    mode: 'cash_put',
    label: 'Cash put, monthly 10% OTM, annualized premium >=15%',
    minDte: 25,
    maxDte: 45,
    putTargetMoneyness: 0.90,
    maxPutMoneyness: 0.92,
    minAnnualizedPremiumYield: 0.15,
    scoreMode: 'annualized_premium',
  },
  {
    id: 'cash_put_monthly_10otm_ivrv125',
    mode: 'cash_put',
    label: 'Cash put, monthly 10% OTM, IV/RV >=1.25',
    minDte: 25,
    maxDte: 45,
    putTargetMoneyness: 0.90,
    maxPutMoneyness: 0.92,
    minStockHistoryDays: 20,
    minIvToRealizedVol: 1.25,
    scoreMode: 'iv_rv',
  },
  {
    id: 'wheel_monthly_10otm_yield15_profit50',
    mode: 'wheel',
    label: 'Wheel monthly 10% OTM, annualized premium >=15%, close at 50% profit',
    minDte: 25,
    maxDte: 45,
    putTargetMoneyness: 0.90,
    callTargetMoneyness: 1.05,
    maxPutMoneyness: 0.92,
    minAnnualizedPremiumYield: 0.15,
    profitTakePct: 0.50,
    scoreMode: 'annualized_premium',
  },
  {
    id: 'wheel_monthly_10otm_trend_ivrv_profit50',
    mode: 'wheel',
    label: 'Wheel monthly 10% OTM, uptrend IV/RV, close at 50% profit',
    minDte: 25,
    maxDte: 45,
    putTargetMoneyness: 0.90,
    callTargetMoneyness: 1.05,
    maxPutMoneyness: 0.92,
    minStockHistoryDays: 20,
    requireAboveSma20: true,
    minPriorReturn20: 0,
    minIvToRealizedVol: 1.10,
    profitTakePct: 0.50,
    scoreMode: 'iv_rv',
  },
  {
    id: 'wheel_monthly_10otm_profit50_stop2x',
    mode: 'wheel',
    label: 'Wheel monthly 10% OTM, close at 50% profit or 2x loss',
    minDte: 25,
    maxDte: 45,
    putTargetMoneyness: 0.90,
    callTargetMoneyness: 1.05,
    profitTakePct: 0.50,
    stopLossMultiple: 2.0,
  },
]);

function round(value, digits = 6) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function normalCdf(value) {
  const sign = value < 0 ? -1 : 1;
  const x = Math.abs(value) / Math.sqrt(2);
  const t = 1 / (1 + (0.3275911 * x));
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
  return 0.5 * (1 + sign * y);
}

function blackScholesPrice({ right, spot, strike, years, volatility, riskFreeRate = 0.04 }) {
  if (!(spot > 0) || !(strike > 0) || !(years > 0) || !(volatility > 0)) return null;
  const sqrtT = Math.sqrt(years);
  const d1 = (Math.log(spot / strike) + ((riskFreeRate + (0.5 * volatility * volatility)) * years)) / (volatility * sqrtT);
  const d2 = d1 - (volatility * sqrtT);
  if (right === 'CALL') return (spot * normalCdf(d1)) - (strike * Math.exp(-riskFreeRate * years) * normalCdf(d2));
  return (strike * Math.exp(-riskFreeRate * years) * normalCdf(-d2)) - (spot * normalCdf(-d1));
}

function blackScholesDelta({ right, spot, strike, years, volatility, riskFreeRate = 0.04 }) {
  if (!(spot > 0) || !(strike > 0) || !(years > 0) || !(volatility > 0)) return null;
  const sqrtT = Math.sqrt(years);
  const d1 = (Math.log(spot / strike) + ((riskFreeRate + (0.5 * volatility * volatility)) * years)) / (volatility * sqrtT);
  return right === 'CALL' ? normalCdf(d1) : normalCdf(d1) - 1;
}

function impliedVolatility({ right, spot, strike, years, price, riskFreeRate = 0.04 }) {
  if (!(price > 0) || !(spot > 0) || !(strike > 0) || !(years > 0)) return null;
  const intrinsic = intrinsicValue({ right, strike }, spot);
  if (price < intrinsic * 0.99) return null;
  let low = 0.01;
  let high = 5.0;
  for (let index = 0; index < 50; index += 1) {
    const mid = (low + high) / 2;
    const model = blackScholesPrice({ right, spot, strike, years, volatility: mid, riskFreeRate });
    if (!Number.isFinite(model)) return null;
    if (model > price) high = mid;
    else low = mid;
  }
  return (low + high) / 2;
}

function createMarketHistory() {
  return new Map();
}

function updateMarketHistory(history, stockDay, symbols, maxDays = 80) {
  symbols.forEach((symbol) => {
    const close = stockDay.eodClose.get(symbol);
    if (!(close > 0)) return;
    const rows = history.get(symbol) || [];
    rows.push(close);
    if (rows.length > maxDays) rows.shift();
    history.set(symbol, rows);
  });
}

function realizedVolatilityFromCloses(closes, lookback = 20) {
  if (!closes || closes.length < lookback + 1) return null;
  const slice = closes.slice(-(lookback + 1));
  const returns = [];
  for (let index = 1; index < slice.length; index += 1) {
    if (slice[index - 1] > 0 && slice[index] > 0) returns.push(Math.log(slice[index] / slice[index - 1]));
  }
  if (returns.length < lookback) return null;
  const mean = returns.reduce((sum, value) => sum + value, 0) / returns.length;
  const variance = returns.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / returns.length;
  return Math.sqrt(variance) * Math.sqrt(252);
}

function stockFeatures(history, symbol) {
  const closes = history.get(symbol) || [];
  const lastClose = closes[closes.length - 1] ?? null;
  const last20 = closes.slice(-20);
  const sma20 = last20.length ? last20.reduce((sum, value) => sum + value, 0) / last20.length : null;
  const prior20 = closes.length >= 21 && closes[closes.length - 21] > 0 && lastClose > 0
    ? (lastClose / closes[closes.length - 21]) - 1
    : null;
  return {
    historyDays: closes.length,
    lastClose,
    sma20,
    priceVsSma20: lastClose > 0 && sma20 > 0 ? (lastClose / sma20) - 1 : null,
    priorReturn20: prior20,
    realizedVol20: realizedVolatilityFromCloses(closes, 20),
  };
}

function ivRankFromHistory(historyValues, impliedVol) {
  const values = (historyValues || []).filter((value) => Number.isFinite(value) && value > 0);
  if (!values.length || !(impliedVol > 0)) return null;
  const min = Math.min(...values);
  const max = Math.max(...values);
  if (max === min) return impliedVol >= max ? 1 : 0;
  return Math.max(0, Math.min(1, (impliedVol - min) / (max - min)));
}

function candidateHistoryKey(symbol, right) {
  return `${symbol}|${right}`;
}

function normalizeSymbols(symbols) {
  return [...new Set((symbols || [])
    .map((symbol) => String(symbol || '').trim().toUpperCase())
    .filter(Boolean)
    .filter((symbol) => !symbol.includes(':'))
    .filter((symbol) => !['SPX', 'SPXW', 'VIX', 'VIXW', 'RUT', 'RUTW', 'XSP'].includes(symbol)))]
    .sort();
}

function mergeStrategyConfig(config, execution = {}) {
  return {
    ...DEFAULT_EXECUTION,
    ...config,
    ...execution,
  };
}

function makeStrategyState(config, initialCapital) {
  return {
    id: config.id,
    config,
    cash: initialCapital,
    openShorts: [],
    shares: new Map(),
    costBasis: new Map(),
    lastClose: new Map(),
    candidateHistory: new Map(),
    trades: [],
    daily: [],
    totals: {
      premiumCollected: 0,
      commissionPaid: 0,
      buybackCost: 0,
      putAssignments: 0,
      putAssignmentLiquidations: 0,
      callAssignments: 0,
      expiredWorthless: 0,
      profitTakes: 0,
      stopLosses: 0,
      skippedEntries: 0,
    },
  };
}

function createStockDay() {
  return {
    bySymbol: new Map(),
    rowsBySymbol: new Map(),
    eodClose: new Map(),
  };
}

function stockMinuteClose(stockDay, symbol, minuteMs) {
  return stockDay.bySymbol.get(symbol)?.get(minuteMs) ?? null;
}

function stockEodClose(stockDay, state, symbol) {
  const close = stockDay.eodClose.get(symbol);
  if (Number.isFinite(close) && close > 0) return close;
  return state?.lastClose?.get(symbol) ?? null;
}

function duckdbString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function parquetSql(filePath) {
  return `COPY (
    SELECT
      ticker,
      volume,
      open,
      close,
      high,
      low,
      window_start,
      COALESCE(CAST(transactions AS VARCHAR), '0') AS transactions
    FROM read_parquet(${duckdbString(filePath)})
  ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`;
}

function splitCsvLine(line) {
  return String(line || '').split(',');
}

async function streamParquetRows(filePath, onRow) {
  const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', parquetSql(filePath)], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const stderr = [];
  child.stderr.on('data', (chunk) => stderr.push(String(chunk)));
  const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  let headers = null;
  for await (const line of reader) {
    if (!line) continue;
    if (!headers) {
      headers = splitCsvLine(line);
      continue;
    }
    const values = splitCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });
    await onRow(row);
  }
  const code = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', resolve);
  });
  if (code !== 0) {
    throw new Error(`duckdb_parquet_read_failed:${filePath}:${stderr.join('').trim() || code}`);
  }
}

async function readStockDay(config, dayIso, symbols) {
  const source = resolveDatasetSource(config, 'stockBars', dayIso);
  const stockDay = createStockDay();
  if (source.format === 'missing') return { stockDay, filePath: source.filePath, available: false };

  const selected = new Set(symbols.map((symbol) => symbol.toUpperCase()));
  function onRow(row) {
    const symbol = String(row.ticker || '').toUpperCase();
    if (!selected.has(symbol)) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null || !isRegularSessionMinute(minuteMs, config.session)) return;
    const close = toNumber(row.close);
    if (!(close > 0)) return;
    if (!stockDay.bySymbol.has(symbol)) stockDay.bySymbol.set(symbol, new Map());
    if (!stockDay.rowsBySymbol.has(symbol)) stockDay.rowsBySymbol.set(symbol, []);
    const bar = {
      symbol,
      minuteMs,
      minuteUtc: minuteMsToIso(minuteMs),
      minuteOfDayEt: getEtParts(minuteMs).minuteOfDayEt,
      open: toNumber(row.open),
      high: toNumber(row.high),
      low: toNumber(row.low),
      close,
      volume: toNumber(row.volume) || 0,
    };
    stockDay.bySymbol.get(symbol).set(minuteMs, close);
    stockDay.rowsBySymbol.get(symbol).push(bar);
    stockDay.eodClose.set(symbol, close);
  }

  if (source.format === 'parquet') await streamParquetRows(source.filePath, onRow);
  else await readGzipCsv(source.filePath, onRow);

  stockDay.rowsBySymbol.forEach((rows) => {
    rows.sort((left, right) => left.minuteMs - right.minuteMs);
  });
  return { stockDay, filePath: source.filePath, available: true, sourceFormat: source.format };
}

function intrinsicValue(option, underlyingClose) {
  if (!(underlyingClose > 0)) return 0;
  if (option.right === 'PUT') return Math.max(0, option.strike - underlyingClose);
  return Math.max(0, underlyingClose - option.strike);
}

function openShortOptionCount(state) {
  return state.openShorts.reduce((sum, option) => sum + option.contracts, 0);
}

function reservedPutCollateral(state) {
  return state.openShorts
    .filter((option) => option.right === 'PUT')
    .reduce((sum, option) => sum + (option.strike * 100 * option.contracts), 0);
}

function computeEquity(state, stockDay) {
  let stockValue = 0;
  state.shares.forEach((shares, symbol) => {
    const close = stockEodClose(stockDay, state, symbol);
    if (Number.isFinite(close)) stockValue += shares * close;
  });

  let optionLiability = 0;
  state.openShorts.forEach((option) => {
    const close = stockEodClose(stockDay, state, option.symbol);
    const mark = Math.max(option.markPrice || 0, intrinsicValue(option, close));
    optionLiability += mark * 100 * option.contracts;
  });

  return {
    equity: state.cash + stockValue - optionLiability,
    stockValue,
    optionLiability,
    reservedCollateral: reservedPutCollateral(state),
  };
}

function utilizationAfterPut(state, nextCollateral) {
  return reservedPutCollateral(state) + nextCollateral;
}

function sharesFor(state, symbol) {
  return state.shares.get(symbol) || 0;
}

function hasOpenOptionForSymbol(state, symbol) {
  return state.openShorts.some((option) => option.symbol === symbol);
}

function coveredCallContractsAvailable(state, symbol, maxContracts) {
  const shares = sharesFor(state, symbol);
  const openCalls = state.openShorts
    .filter((option) => option.symbol === symbol && option.right === 'CALL')
    .reduce((sum, option) => sum + option.contracts, 0);
  return Math.max(0, Math.min(maxContracts, Math.floor(shares / 100) - openCalls));
}

function stockPassesConfigFilters(cfg, features) {
  if (features.historyDays < (cfg.minStockHistoryDays || 0)) return false;
  if (cfg.requireAboveSma20 && !(features.priceVsSma20 > 0)) return false;
  if (Number.isFinite(cfg.minPriorReturn20) && !(features.priorReturn20 >= cfg.minPriorReturn20)) return false;
  if (Number.isFinite(cfg.maxPriorReturn20) && !(features.priorReturn20 <= cfg.maxPriorReturn20)) return false;
  if (Number.isFinite(cfg.maxRealizedVol20) && !(features.realizedVol20 <= cfg.maxRealizedVol20)) return false;
  if (Number.isFinite(cfg.minRealizedVol20) && !(features.realizedVol20 >= cfg.minRealizedVol20)) return false;
  return true;
}

function buildDemandsForDay(states, stockDay, symbols, marketHistory) {
  const byRoot = new Map();
  states.forEach((state) => {
    const cfg = state.config;
    symbols.forEach((symbol) => {
      if (!stockDay.eodClose.has(symbol)) return;
      if (hasOpenOptionForSymbol(state, symbol)) return;
      const features = stockFeatures(marketHistory, symbol);
      if (!stockPassesConfigFilters(cfg, features)) return;

      let demand = null;
      if (cfg.mode === 'wheel' && sharesFor(state, symbol) >= 100) {
        const contractsAvailable = coveredCallContractsAvailable(state, symbol, cfg.maxContractsPerSymbol);
        if (contractsAvailable <= 0) return;
        demand = {
          state,
          symbol,
          right: 'CALL',
          targetMoneyness: cfg.callTargetMoneyness || 1.05,
          minStrike: state.costBasis.get(symbol) || 0,
          maxContracts: contractsAvailable,
          features,
        };
      } else {
        demand = {
          state,
          symbol,
          right: 'PUT',
          targetMoneyness: cfg.putTargetMoneyness || 0.95,
          minStrike: 0,
          maxContracts: cfg.maxContractsPerSymbol,
          features,
        };
      }

      const list = byRoot.get(symbol) || [];
      list.push(demand);
      byRoot.set(symbol, list);
    });
  });
  return byRoot;
}

function candidateKey(demand) {
  return `${demand.state.id}|${demand.symbol}`;
}

function isCandidateBetter(candidate, current) {
  if (!current) return true;
  if (candidate.score !== current.score) return candidate.score < current.score;
  if (candidate.premiumYield !== current.premiumYield) return candidate.premiumYield > current.premiumYield;
  if (candidate.volume !== current.volume) return candidate.volume > current.volume;
  if (candidate.minuteMs !== current.minuteMs) return candidate.minuteMs < current.minuteMs;
  return candidate.ticker.localeCompare(current.ticker) < 0;
}

function addCandidateRef(refsByTicker, candidate) {
  const list = refsByTicker.get(candidate.ticker) || [];
  list.push(candidate);
  refsByTicker.set(candidate.ticker, list);
}

function updateCandidateMark(candidate, minuteMs, close) {
  if (!(close > 0) || minuteMs < candidate.minuteMs) return;
  if (!candidate.dayLastMinuteMs || minuteMs >= candidate.dayLastMinuteMs) {
    candidate.dayLastMinuteMs = minuteMs;
    candidate.dayLastPrice = close;
  }
}

function putCandidateAllowed(parsed, underlyingClose, cfg) {
  const maxMoneyness = Number.isFinite(cfg.maxPutMoneyness) ? cfg.maxPutMoneyness : 0.995;
  const minMoneyness = Number.isFinite(cfg.minPutMoneyness) ? cfg.minPutMoneyness : 0;
  const moneyness = parsed.strike / underlyingClose;
  return moneyness <= maxMoneyness && moneyness >= minMoneyness;
}

function callCandidateAllowed(parsed, underlyingClose, minStrike, cfg) {
  const minMoneyness = Number.isFinite(cfg.minCallMoneyness) ? cfg.minCallMoneyness : 1.005;
  const maxMoneyness = Number.isFinite(cfg.maxCallMoneyness) ? cfg.maxCallMoneyness : Infinity;
  const moneyness = parsed.strike / underlyingClose;
  return moneyness >= minMoneyness && moneyness <= maxMoneyness && parsed.strike >= minStrike;
}

function passesCandidateFilters(cfg, candidate) {
  if (Number.isFinite(cfg.minAnnualizedPremiumYield) && !(candidate.annualizedPremiumYield >= cfg.minAnnualizedPremiumYield)) return false;
  if (Number.isFinite(cfg.maxAnnualizedPremiumYield) && !(candidate.annualizedPremiumYield <= cfg.maxAnnualizedPremiumYield)) return false;
  if (Number.isFinite(cfg.minImpliedVol) && !(candidate.impliedVol >= cfg.minImpliedVol)) return false;
  if (Number.isFinite(cfg.maxImpliedVol) && !(candidate.impliedVol <= cfg.maxImpliedVol)) return false;
  if (Number.isFinite(cfg.minIvRank) && !(candidate.ivRank >= cfg.minIvRank)) return false;
  if (Number.isFinite(cfg.minIvToRealizedVol) && !(candidate.ivToRealizedVol >= cfg.minIvToRealizedVol)) return false;
  if (candidate.right === 'PUT') {
    if (Number.isFinite(cfg.minPutDeltaAbs) && !(candidate.deltaAbs >= cfg.minPutDeltaAbs)) return false;
    if (Number.isFinite(cfg.maxPutDeltaAbs) && !(candidate.deltaAbs <= cfg.maxPutDeltaAbs)) return false;
  }
  if (candidate.right === 'CALL') {
    if (Number.isFinite(cfg.minCallDeltaAbs) && !(candidate.deltaAbs >= cfg.minCallDeltaAbs)) return false;
    if (Number.isFinite(cfg.maxCallDeltaAbs) && !(candidate.deltaAbs <= cfg.maxCallDeltaAbs)) return false;
  }
  return true;
}

function candidateScore(cfg, metrics) {
  const moneynessError = metrics.moneynessError;
  const liquidityBoost = Math.log1p(metrics.volume + metrics.transactions) / 10_000;
  if (cfg.scoreMode === 'annualized_premium') return -metrics.annualizedPremiumYield + (moneynessError / 10) - liquidityBoost;
  if (cfg.scoreMode === 'iv_rank') return -(metrics.ivRank ?? -1) + (moneynessError / 10) - liquidityBoost;
  if (cfg.scoreMode === 'iv_rv') return -(metrics.ivToRealizedVol ?? -1) + (moneynessError / 10) - liquidityBoost;
  if (cfg.scoreMode === 'delta_target') return Math.abs((metrics.deltaAbs ?? 0) - (cfg.targetDeltaAbs ?? 0.16)) - liquidityBoost;
  return moneynessError - liquidityBoost;
}

function maybeUpdateCandidate({
  bestCandidates,
  refsByTicker,
  demand,
  parsed,
  row,
  dayIso,
  minuteMs,
  minuteOfDayEt,
  underlyingClose,
}) {
  const cfg = demand.state.config;
  if (parsed.right !== demand.right) return;
  const dte = daysBetween(dayIso, parsed.expiration);
  if (!Number.isFinite(dte) || dte < cfg.minDte || dte > cfg.maxDte) return;
  if (minuteOfDayEt < cfg.entryMinuteEt || minuteOfDayEt >= cfg.entryMinuteEt + cfg.entryWindowMinutes) return;

  const close = toNumber(row.close);
  const volume = toNumber(row.volume) || 0;
  const transactions = toNumber(row.transactions) || 0;
  if (!(close >= cfg.minPremium) || volume < cfg.minVolume || transactions < cfg.minTransactions) return;
  if (!(underlyingClose > 0)) return;
  if (parsed.right === 'PUT' && !putCandidateAllowed(parsed, underlyingClose, cfg)) return;
  if (parsed.right === 'CALL' && !callCandidateAllowed(parsed, underlyingClose, demand.minStrike, cfg)) return;

  const targetStrike = underlyingClose * demand.targetMoneyness;
  const moneynessError = Math.abs((parsed.strike / underlyingClose) - demand.targetMoneyness);
  const premiumYield = close / parsed.strike;
  const entryPrice = close * (1 - cfg.premiumHaircutPct);
  const years = Math.max(dte, 1) / 365;
  const impliedVol = impliedVolatility({
    right: parsed.right,
    spot: underlyingClose,
    strike: parsed.strike,
    years,
    price: close,
  });
  const delta = impliedVol
    ? blackScholesDelta({
      right: parsed.right,
      spot: underlyingClose,
      strike: parsed.strike,
      years,
      volatility: impliedVol,
    })
    : null;
  const history = demand.state.candidateHistory.get(candidateHistoryKey(demand.symbol, parsed.right)) || [];
  const minHistory = cfg.minIvHistoryObservations || 0;
  const ivRank = history.length >= minHistory ? ivRankFromHistory(history, impliedVol) : null;
  const annualizedPremiumYield = (entryPrice / parsed.strike) * (365 / Math.max(dte, 1));
  const ivToRealizedVol = demand.features.realizedVol20 > 0 && impliedVol > 0
    ? impliedVol / demand.features.realizedVol20
    : null;
  const score = candidateScore(cfg, {
    moneynessError,
    volume,
    transactions,
    annualizedPremiumYield,
    ivRank,
    ivToRealizedVol,
    deltaAbs: Math.abs(delta ?? 0),
  });
  const candidate = {
    strategyId: demand.state.id,
    symbol: demand.symbol,
    right: parsed.right,
    ticker: parsed.ticker,
    strike: parsed.strike,
    expiration: parsed.expiration,
    dte,
    minuteMs,
    minuteUtc: minuteMsToIso(minuteMs),
    minuteOfDayEt,
    underlyingClose,
    targetStrike,
    close,
    entryPrice,
    volume,
    transactions,
    score,
    premiumYield,
    annualizedPremiumYield,
    impliedVol,
    delta,
    deltaAbs: Math.abs(delta ?? 0),
    ivRank,
    ivHistoryObservations: history.length,
    realizedVol20: demand.features.realizedVol20,
    ivToRealizedVol,
    priceVsSma20: demand.features.priceVsSma20,
    priorReturn20: demand.features.priorReturn20,
    maxContracts: demand.maxContracts,
    dayLastMinuteMs: minuteMs,
    dayLastPrice: close,
  };
  if (!passesCandidateFilters(cfg, candidate)) return;

  const key = candidateKey(demand);
  if (isCandidateBetter(candidate, bestCandidates.get(key))) {
    bestCandidates.set(key, candidate);
    addCandidateRef(refsByTicker, candidate);
  }
}

async function scanOptionDay({
  config,
  dayIso,
  stockDay,
  states,
  symbols,
  marketHistory,
}) {
  const source = resolveDatasetSource(config, 'optionBars', dayIso);
  const openTickers = new Set(states.flatMap((state) => state.openShorts.map((option) => option.ticker)));
  const demandByRoot = buildDemandsForDay(states, stockDay, symbols, marketHistory);
  const bestCandidates = new Map();
  const refsByTicker = new Map();
  const marks = new Map();

  if (source.format === 'missing') {
    return {
      filePath: source.filePath,
      available: false,
      candidates: bestCandidates,
      marks,
      providerSparse: false,
    };
  }

  let selectedRowCount = 0;
  function onRow(row) {
    const parsed = parseOpraTicker(row.ticker);
    if (!parsed) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null || !isRegularSessionMinute(minuteMs, config.session)) return;
    const close = toNumber(row.close);
    if (!(close > 0)) return;

    if (openTickers.has(parsed.ticker)) {
      const current = marks.get(parsed.ticker);
      if (!current || minuteMs >= current.minuteMs) {
        marks.set(parsed.ticker, {
          ticker: parsed.ticker,
          minuteMs,
          minuteUtc: minuteMsToIso(minuteMs),
          close,
        });
      }
    }

    const refs = refsByTicker.get(parsed.ticker);
    if (refs) refs.forEach((candidate) => updateCandidateMark(candidate, minuteMs, close));

    const demands = demandByRoot.get(parsed.root);
    if (!demands) return;
    selectedRowCount += 1;
    const minuteOfDayEt = getEtParts(minuteMs).minuteOfDayEt;
    demands.forEach((demand) => {
      const underlyingClose = stockMinuteClose(stockDay, demand.symbol, minuteMs);
      maybeUpdateCandidate({
        bestCandidates,
        refsByTicker,
        demand,
        parsed,
        row,
        dayIso,
        minuteMs,
        minuteOfDayEt,
        underlyingClose,
      });
    });
  }

  if (source.format === 'parquet') await streamParquetRows(source.filePath, onRow);
  else await readGzipCsv(source.filePath, onRow);

  return {
    filePath: source.filePath,
    available: true,
    candidates: bestCandidates,
    marks,
    providerSparse: selectedRowCount === 0,
  };
}

function updateLastCloses(state, stockDay) {
  stockDay.eodClose.forEach((close, symbol) => {
    if (Number.isFinite(close) && close > 0) state.lastClose.set(symbol, close);
  });
}

function updateOpenOptionMarks(state, marks, stockDay) {
  state.openShorts.forEach((option) => {
    const mark = marks.get(option.ticker);
    if (mark?.close > 0) {
      option.markPrice = mark.close;
      option.markMinuteUtc = mark.minuteUtc;
      option.markSource = 'option_1m_last';
      return;
    }
    const underlyingClose = stockEodClose(stockDay, state, option.symbol);
    const fallback = Math.max(intrinsicValue(option, underlyingClose), option.markPrice || 0);
    option.markPrice = fallback;
    option.markMinuteUtc = null;
    option.markSource = 'intrinsic_or_prior_mark';
  });
}

function recordCandidateObservations(state, candidates, maxObservations = 80) {
  [...candidates.values()]
    .filter((candidate) => candidate.strategyId === state.id && candidate.impliedVol > 0)
    .forEach((candidate) => {
      const key = candidateHistoryKey(candidate.symbol, candidate.right);
      const values = state.candidateHistory.get(key) || [];
      values.push(candidate.impliedVol);
      if (values.length > maxObservations) values.shift();
      state.candidateHistory.set(key, values);
    });
}

function manageOpenShorts(state, dayIso) {
  const cfg = state.config;
  const remaining = [];
  const events = [];
  state.openShorts.forEach((option) => {
    if (option.expiration <= dayIso) {
      remaining.push(option);
      return;
    }
    if (!(option.markPrice > 0) || !(option.entryPrice > 0)) {
      remaining.push(option);
      return;
    }

    let reason = null;
    if (Number.isFinite(cfg.profitTakePct) && option.markPrice <= option.entryPrice * (1 - cfg.profitTakePct)) {
      reason = 'profit_take';
    } else if (Number.isFinite(cfg.stopLossMultiple) && option.markPrice >= option.entryPrice * cfg.stopLossMultiple) {
      reason = 'stop_loss';
    }

    if (!reason) {
      remaining.push(option);
      return;
    }

    const contracts = option.contracts;
    const exitPrice = option.markPrice * (1 + cfg.exitPremiumMarkupPct);
    const buybackCost = exitPrice * 100 * contracts;
    const commission = cfg.commissionPerContract * contracts;
    state.cash -= buybackCost + commission;
    state.totals.buybackCost += buybackCost;
    state.totals.commissionPaid += commission;
    if (reason === 'profit_take') state.totals.profitTakes += contracts;
    if (reason === 'stop_loss') state.totals.stopLosses += contracts;
    const event = {
      type: 'buy_to_close',
      reason,
      date: dayIso,
      symbol: option.symbol,
      ticker: option.ticker,
      right: option.right,
      strike: option.strike,
      expiration: option.expiration,
      contracts,
      entryPrice: option.entryPrice,
      markPrice: option.markPrice,
      exitPrice,
      buybackCost,
      commission,
      grossPnl: ((option.entryPrice - exitPrice) * 100 * contracts) - commission,
    };
    state.trades.push(event);
    events.push(event);
  });
  state.openShorts = remaining;
  return events;
}

function addShares(state, symbol, shares, price) {
  const currentShares = state.shares.get(symbol) || 0;
  const currentBasis = state.costBasis.get(symbol) || 0;
  const nextShares = currentShares + shares;
  const nextBasis = nextShares > 0
    ? ((currentShares * currentBasis) + (shares * price)) / nextShares
    : 0;
  if (nextShares > 0) {
    state.shares.set(symbol, nextShares);
    state.costBasis.set(symbol, nextBasis);
  } else {
    state.shares.delete(symbol);
    state.costBasis.delete(symbol);
  }
}

function removeShares(state, symbol, shares) {
  const currentShares = state.shares.get(symbol) || 0;
  const nextShares = Math.max(0, currentShares - shares);
  if (nextShares > 0) {
    state.shares.set(symbol, nextShares);
  } else {
    state.shares.delete(symbol);
    state.costBasis.delete(symbol);
  }
}

function settleExpirations(state, dayIso, stockDay) {
  const cfg = state.config;
  const remaining = [];
  const events = [];
  state.openShorts.forEach((option) => {
    if (option.expiration > dayIso) {
      remaining.push(option);
      return;
    }

    const underlyingClose = stockEodClose(stockDay, state, option.symbol);
    const contracts = option.contracts;
    const shareCount = contracts * 100;
    const intrinsic = intrinsicValue(option, underlyingClose);
    const event = {
      type: 'expire',
      date: dayIso,
      symbol: option.symbol,
      ticker: option.ticker,
      right: option.right,
      strike: option.strike,
      expiration: option.expiration,
      contracts,
      underlyingClose,
      intrinsic,
    };

    if (option.right === 'PUT' && underlyingClose < option.strike) {
      if (cfg.mode === 'wheel') {
        state.cash -= option.strike * shareCount;
        addShares(state, option.symbol, shareCount, option.strike);
        state.totals.putAssignments += contracts;
        event.type = 'put_assigned';
      } else {
        const liquidationPrice = underlyingClose * (1 - (cfg.stockSlippageBps / 10_000));
        state.cash -= option.strike * shareCount;
        state.cash += liquidationPrice * shareCount;
        state.totals.putAssignmentLiquidations += contracts;
        event.type = 'put_assigned_liquidated';
        event.liquidationPrice = liquidationPrice;
      }
    } else if (option.right === 'CALL' && underlyingClose > option.strike) {
      state.cash += option.strike * shareCount;
      removeShares(state, option.symbol, shareCount);
      state.totals.callAssignments += contracts;
      event.type = 'call_assigned';
    } else {
      state.totals.expiredWorthless += contracts;
      event.type = 'expired_worthless';
    }
    state.trades.push(event);
    events.push(event);
  });
  state.openShorts = remaining;
  return events;
}

function optionEntrySort(left, right) {
  if (left.score !== right.score) return left.score - right.score;
  if (left.premiumYield !== right.premiumYield) return right.premiumYield - left.premiumYield;
  return left.symbol.localeCompare(right.symbol);
}

function openEntries(state, candidates, stockDay, dayIso) {
  const cfg = state.config;
  const entries = [];
  const byState = [...candidates.values()]
    .filter((candidate) => candidate.strategyId === state.id)
    .sort(optionEntrySort);

  byState.forEach((candidate) => {
    if (hasOpenOptionForSymbol(state, candidate.symbol)) return;
    if (openShortOptionCount(state) >= cfg.maxOpenShortOptions) {
      state.totals.skippedEntries += 1;
      return;
    }

    const equitySnapshot = computeEquity(state, stockDay);
    const equity = equitySnapshot.equity;
    const collateralPerContract = candidate.strike * 100;
    let contracts = Math.min(candidate.maxContracts, cfg.maxContractsPerSymbol);

    if (candidate.right === 'PUT') {
      const freeCash = state.cash - reservedPutCollateral(state);
      const positionContractLimit = Math.floor((equity * cfg.maxPositionPct) / collateralPerContract);
      const cashContractLimit = Math.floor(freeCash / collateralPerContract);
      const utilizationContractLimit = Math.floor(
        Math.max(0, (equity * cfg.maxPortfolioUtilization) - reservedPutCollateral(state)) / collateralPerContract,
      );
      contracts = Math.min(contracts, positionContractLimit, cashContractLimit, utilizationContractLimit);
      if (contracts <= 0 || utilizationAfterPut(state, collateralPerContract * contracts) > equity * cfg.maxPortfolioUtilization) {
        state.totals.skippedEntries += 1;
        return;
      }
    } else {
      contracts = Math.min(contracts, coveredCallContractsAvailable(state, candidate.symbol, cfg.maxContractsPerSymbol));
      if (contracts <= 0) {
        state.totals.skippedEntries += 1;
        return;
      }
    }

    const grossPremium = candidate.entryPrice * 100 * contracts;
    const commission = cfg.commissionPerContract * contracts;
    state.cash += grossPremium - commission;
    state.totals.premiumCollected += grossPremium;
    state.totals.commissionPaid += commission;

    const option = {
      symbol: candidate.symbol,
      ticker: candidate.ticker,
      right: candidate.right,
      strike: candidate.strike,
      expiration: candidate.expiration,
      contracts,
      entryDate: dayIso,
      entryMinuteUtc: candidate.minuteUtc,
      entryMinuteOfDayEt: candidate.minuteOfDayEt,
      rawEntryPrice: candidate.close,
      entryPrice: candidate.entryPrice,
      markPrice: candidate.dayLastPrice || candidate.close,
      markMinuteUtc: candidate.dayLastMinuteMs ? minuteMsToIso(candidate.dayLastMinuteMs) : candidate.minuteUtc,
      markSource: 'same_day_candidate_last',
      underlyingEntryClose: candidate.underlyingClose,
      dte: candidate.dte,
      impliedVol: candidate.impliedVol,
      delta: candidate.delta,
      annualizedPremiumYield: candidate.annualizedPremiumYield,
      ivRank: candidate.ivRank,
      ivToRealizedVol: candidate.ivToRealizedVol,
    };
    state.openShorts.push(option);

    const event = {
      type: candidate.right === 'PUT' ? 'sell_put' : 'sell_call',
      date: dayIso,
      symbol: candidate.symbol,
      ticker: candidate.ticker,
      right: candidate.right,
      strike: candidate.strike,
      expiration: candidate.expiration,
      contracts,
      dte: candidate.dte,
      minuteUtc: candidate.minuteUtc,
      underlyingEntryClose: candidate.underlyingClose,
      rawEntryPrice: candidate.close,
      entryPrice: candidate.entryPrice,
      grossPremium,
      commission,
      impliedVol: candidate.impliedVol,
      delta: candidate.delta,
      annualizedPremiumYield: candidate.annualizedPremiumYield,
      ivRank: candidate.ivRank,
      ivHistoryObservations: candidate.ivHistoryObservations,
      realizedVol20: candidate.realizedVol20,
      ivToRealizedVol: candidate.ivToRealizedVol,
      priceVsSma20: candidate.priceVsSma20,
      priorReturn20: candidate.priorReturn20,
    };
    state.trades.push(event);
    entries.push(event);
  });

  return entries;
}

function recordDaily(state, dayIso, stockDay, entries, expirations, closures = []) {
  const previous = state.daily[state.daily.length - 1];
  const previousEquity = previous?.equity ?? state.config.initialCapital;
  const snapshot = computeEquity(state, stockDay);
  const peak = Math.max(previous?.peakEquity ?? state.config.initialCapital, snapshot.equity);
  const drawdown = peak > 0 ? (snapshot.equity / peak) - 1 : 0;
  const row = {
    date: dayIso,
    cash: round(state.cash, 2),
    stockValue: round(snapshot.stockValue, 2),
    optionLiability: round(snapshot.optionLiability, 2),
    reservedCollateral: round(snapshot.reservedCollateral, 2),
    equity: round(snapshot.equity, 2),
    dailyReturn: previousEquity > 0 ? round((snapshot.equity / previousEquity) - 1, 8) : 0,
    peakEquity: round(peak, 2),
    drawdown: round(drawdown, 8),
    openShorts: state.openShorts.length,
    openPutContracts: state.openShorts.filter((option) => option.right === 'PUT').reduce((sum, option) => sum + option.contracts, 0),
    openCallContracts: state.openShorts.filter((option) => option.right === 'CALL').reduce((sum, option) => sum + option.contracts, 0),
    shareSymbols: [...state.shares.entries()].filter(([, shares]) => shares > 0).length,
    entries: entries.length,
    closures: closures.length,
    expirations: expirations.length,
  };
  state.daily.push(row);
  return row;
}

function maxDrawdownFromSeries(values) {
  let peak = values[0] || 1;
  let maxDrawdown = 0;
  values.forEach((value) => {
    peak = Math.max(peak, value);
    if (peak > 0) maxDrawdown = Math.min(maxDrawdown, (value / peak) - 1);
  });
  return maxDrawdown;
}

function summarizeMonthly(daily) {
  const byMonth = new Map();
  daily.forEach((row) => {
    const month = row.date.slice(0, 7);
    const list = byMonth.get(month) || [];
    list.push(row);
    byMonth.set(month, list);
  });
  return [...byMonth.entries()].map(([month, rows]) => {
    let equity = 1;
    rows.forEach((row) => {
      equity *= (1 + (row.dailyReturn || 0));
    });
    return {
      month,
      days: rows.length,
      return: round(equity - 1, 6),
      maxDrawdown: round(maxDrawdownFromSeries(rows.map((row) => row.equity)), 6),
    };
  });
}

function summarizeBenchmark(points) {
  const clean = points.filter((point) => point.close > 0);
  if (clean.length < 2) return { observations: clean.length, totalReturn: null, maxDrawdown: null };
  const first = clean[0].close;
  const equity = clean.map((point) => point.close / first);
  return {
    observations: clean.length,
    startDate: clean[0].date,
    endDate: clean[clean.length - 1].date,
    startClose: round(first, 4),
    endClose: round(clean[clean.length - 1].close, 4),
    totalReturn: round((clean[clean.length - 1].close / first) - 1, 6),
    maxDrawdown: round(maxDrawdownFromSeries(equity), 6),
  };
}

function summarizeStrategy(state) {
  const daily = state.daily;
  const initialCapital = state.config.initialCapital;
  const finalEquity = daily[daily.length - 1]?.equity ?? initialCapital;
  const dailyReturns = daily.map((row) => row.dailyReturn || 0);
  const positiveDays = dailyReturns.filter((value) => value > 0).length;
  const negativeDays = dailyReturns.filter((value) => value < 0).length;
  const sellTrades = state.trades.filter((trade) => trade.type === 'sell_put' || trade.type === 'sell_call');
  const buyToCloseTrades = state.trades.filter((trade) => trade.type === 'buy_to_close');
  const expirations = state.trades.filter((trade) => (
    trade.type === 'expired_worthless'
    || trade.type === 'put_assigned'
    || trade.type === 'put_assigned_liquidated'
    || trade.type === 'call_assigned'
  ));

  return {
    id: state.id,
    label: state.config.label,
    mode: state.config.mode,
    dteRange: [state.config.minDte, state.config.maxDte],
    putTargetMoneyness: state.config.putTargetMoneyness,
    callTargetMoneyness: state.config.callTargetMoneyness || null,
    initialCapital,
    finalEquity: round(finalEquity, 2),
    totalReturn: round((finalEquity / initialCapital) - 1, 6),
    maxDrawdown: round(maxDrawdownFromSeries(daily.map((row) => row.equity)), 6),
    dailyWinRate: daily.length ? round(positiveDays / daily.length, 4) : 0,
    negativeDayShare: daily.length ? round(negativeDays / daily.length, 4) : 0,
    sellTradeCount: sellTrades.length,
    expirationEventCount: expirations.length,
    buyToCloseCount: buyToCloseTrades.length,
    premiumCollected: round(state.totals.premiumCollected, 2),
    buybackCost: round(state.totals.buybackCost, 2),
    commissionPaid: round(state.totals.commissionPaid, 2),
    putAssignments: state.totals.putAssignments,
    putAssignmentLiquidations: state.totals.putAssignmentLiquidations,
    callAssignments: state.totals.callAssignments,
    expiredWorthless: state.totals.expiredWorthless,
    profitTakes: state.totals.profitTakes,
    stopLosses: state.totals.stopLosses,
    skippedEntries: state.totals.skippedEntries,
    endingOpenOptions: state.openShorts.length,
    endingShareSymbols: [...state.shares.entries()].filter(([, shares]) => shares > 0).length,
    averageOpenOptions: daily.length
      ? round(daily.reduce((sum, row) => sum + row.openShorts, 0) / daily.length, 3)
      : 0,
    averageReservedCollateral: daily.length
      ? round(daily.reduce((sum, row) => sum + row.reservedCollateral, 0) / daily.length, 2)
      : 0,
    monthly: summarizeMonthly(daily),
  };
}

function makeBenchmarkTrackers() {
  return {
    SPY: [],
    QQQ: [],
  };
}

function updateBenchmarks(benchmarks, dayIso, stockDay) {
  Object.keys(benchmarks).forEach((symbol) => {
    const close = stockDay.eodClose.get(symbol);
    if (close > 0) benchmarks[symbol].push({ date: dayIso, close });
  });
}

function createCoverage() {
  return {
    attemptedOpenDays: 0,
    attemptedMissing: [],
    providerSparse: [],
    processedDays: 0,
  };
}

function serializeMap(map) {
  return [...(map || new Map()).entries()];
}

function deserializeMap(entries) {
  return new Map(Array.isArray(entries) ? entries : []);
}

function clonePlain(value, fallback) {
  if (value === undefined) return fallback;
  return JSON.parse(JSON.stringify(value));
}

function sameJson(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function serializeStrategyState(state) {
  return {
    id: state.id,
    config: state.config,
    cash: state.cash,
    openShorts: clonePlain(state.openShorts, []),
    shares: serializeMap(state.shares),
    costBasis: serializeMap(state.costBasis),
    lastClose: serializeMap(state.lastClose),
    candidateHistory: serializeMap(state.candidateHistory),
    trades: clonePlain(state.trades, []),
    daily: clonePlain(state.daily, []),
    totals: clonePlain(state.totals, {}),
  };
}

function restoreStrategyState(serialized, config, initialCapital) {
  const state = makeStrategyState(config, initialCapital);
  state.cash = Number.isFinite(serialized.cash) ? serialized.cash : state.cash;
  state.openShorts = clonePlain(serialized.openShorts, []);
  state.shares = deserializeMap(serialized.shares);
  state.costBasis = deserializeMap(serialized.costBasis);
  state.lastClose = deserializeMap(serialized.lastClose);
  state.candidateHistory = deserializeMap(serialized.candidateHistory);
  state.trades = clonePlain(serialized.trades, []);
  state.daily = clonePlain(serialized.daily, []);
  state.totals = { ...state.totals, ...clonePlain(serialized.totals, {}) };
  return state;
}

function serializeCheckpoint({ startDate, endDate, symbols, initialCapital, execution, states, marketHistory, benchmarks, coverage }) {
  return {
    schemaVersion: CHECKPOINT_SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    symbols,
    initialCapital,
    execution,
    strategyIds: states.map((state) => state.id),
    states: states.map(serializeStrategyState),
    marketHistory: serializeMap(marketHistory),
    benchmarks: clonePlain(benchmarks, {}),
    coverage: clonePlain(coverage, createCoverage()),
  };
}

function restoreCheckpointContext({ checkpoint, strategies, symbols, initialCapital, execution }) {
  if (!checkpoint || checkpoint.schemaVersion !== CHECKPOINT_SCHEMA_VERSION) {
    throw new Error('missing_or_unsupported_wheel_checkpoint');
  }
  const strategyIds = strategies.map((strategy) => strategy.id);
  if (!sameJson(checkpoint.symbols || [], symbols)) {
    throw new Error('wheel_checkpoint_symbols_mismatch');
  }
  if (!sameJson(checkpoint.strategyIds || [], strategyIds)) {
    throw new Error('wheel_checkpoint_strategy_ids_mismatch');
  }
  if (!sameJson(checkpoint.execution || {}, execution)) {
    throw new Error('wheel_checkpoint_execution_mismatch');
  }
  if (checkpoint.initialCapital !== initialCapital) {
    throw new Error('wheel_checkpoint_initial_capital_mismatch');
  }
  const statesById = new Map((checkpoint.states || []).map((state) => [state.id, state]));
  const states = strategies.map((strategy) => {
    const serialized = statesById.get(strategy.id);
    if (!serialized) throw new Error(`checkpoint_missing_strategy:${strategy.id}`);
    return restoreStrategyState(serialized, strategy, initialCapital);
  });
  return {
    startDate: checkpoint.startDate,
    endDate: checkpoint.endDate,
    states,
    marketHistory: deserializeMap(checkpoint.marketHistory),
    benchmarks: clonePlain(checkpoint.benchmarks, makeBenchmarkTrackers()),
    coverage: { ...createCoverage(), ...clonePlain(checkpoint.coverage, {}) },
  };
}

async function runWheelBacktest({
  config,
  startDate,
  endDate,
  symbols,
  strategyConfigs = DEFAULT_STRATEGY_CONFIGS,
  initialCapital = DEFAULT_INITIAL_CAPITAL,
  execution = {},
  onProgress = null,
  resumeFromReport = null,
}) {
  const cleanSymbols = normalizeSymbols(symbols);
  const allStockSymbols = normalizeSymbols([...cleanSymbols, 'SPY', 'QQQ']);
  const reportStartDate = resumeFromReport?.checkpoint?.startDate || startDate;
  const calendarDays = openCalendarDays(config.roots.calendar, reportStartDate, endDate);
  const resolvedExecution = {
    ...DEFAULT_EXECUTION,
    ...execution,
  };
  const strategies = strategyConfigs.map((strategyConfig) => mergeStrategyConfig(strategyConfig, {
    ...execution,
    initialCapital,
  }));
  const resumeContext = resumeFromReport
    ? restoreCheckpointContext({
      checkpoint: resumeFromReport.checkpoint,
      strategies,
      symbols: cleanSymbols,
      initialCapital,
      execution: resolvedExecution,
    })
    : null;
  if (resumeContext && resumeContext.endDate >= endDate) {
    return {
      ...resumeFromReport,
      generatedAt: new Date().toISOString(),
      checkpoint: serializeCheckpoint({
        startDate: resumeContext.startDate,
        endDate: resumeContext.endDate,
        symbols: cleanSymbols,
        initialCapital,
        execution: resolvedExecution,
        states: resumeContext.states,
        marketHistory: resumeContext.marketHistory,
        benchmarks: resumeContext.benchmarks,
        coverage: resumeContext.coverage,
      }),
    };
  }
  const states = resumeContext?.states || strategies.map((strategy) => makeStrategyState(strategy, initialCapital));
  const coverage = resumeContext?.coverage || createCoverage();
  const benchmarks = resumeContext?.benchmarks || makeBenchmarkTrackers();
  const marketHistory = resumeContext?.marketHistory || createMarketHistory();
  const processDays = resumeContext
    ? calendarDays.filter((dayIso) => dayIso > resumeContext.endDate)
    : calendarDays;

  for (const dayIso of processDays) {
    coverage.attemptedOpenDays += 1;
    const startedAt = Date.now();
    const stockResult = await readStockDay(config, dayIso, allStockSymbols);
    if (!stockResult.available) {
      coverage.attemptedMissing.push({ date: dayIso, dataset: 'stock_quotes_1m', path: stockResult.filePath });
      continue;
    }

    states.forEach((state) => updateLastCloses(state, stockResult.stockDay));
    updateBenchmarks(benchmarks, dayIso, stockResult.stockDay);

    const optionResult = await scanOptionDay({
      config,
      dayIso,
      stockDay: stockResult.stockDay,
      states,
      symbols: cleanSymbols,
      marketHistory,
    });

    if (!optionResult.available) {
      coverage.attemptedMissing.push({ date: dayIso, dataset: 'option_quotes_1m', path: optionResult.filePath });
    } else if (optionResult.providerSparse) {
      coverage.providerSparse.push({ date: dayIso, dataset: 'option_quotes_1m' });
    }

    states.forEach((state) => {
      updateOpenOptionMarks(state, optionResult.marks, stockResult.stockDay);
      const closures = manageOpenShorts(state, dayIso);
      const entries = optionResult.available
        ? openEntries(state, optionResult.candidates, stockResult.stockDay, dayIso)
        : [];
      if (optionResult.available) recordCandidateObservations(state, optionResult.candidates);
      updateOpenOptionMarks(state, optionResult.marks, stockResult.stockDay);
      const expirations = settleExpirations(state, dayIso, stockResult.stockDay);
      updateOpenOptionMarks(state, optionResult.marks, stockResult.stockDay);
      recordDaily(state, dayIso, stockResult.stockDay, entries, expirations, closures);
    });
    updateMarketHistory(marketHistory, stockResult.stockDay, allStockSymbols);

    coverage.processedDays += 1;
    if (onProgress) {
      onProgress({
        dayIso,
        processedDays: coverage.processedDays,
        totalDays: calendarDays.length,
        elapsedMs: Date.now() - startedAt,
      });
    }
  }

  const summaries = states.map(summarizeStrategy)
    .sort((left, right) => {
      const leftCalmar = Math.abs(left.maxDrawdown) > 0 ? left.totalReturn / Math.abs(left.maxDrawdown) : left.totalReturn;
      const rightCalmar = Math.abs(right.maxDrawdown) > 0 ? right.totalReturn / Math.abs(right.maxDrawdown) : right.totalReturn;
      return rightCalmar - leftCalmar;
    });

  return {
    generatedAt: new Date().toISOString(),
    provider: config.dataPolicy?.provider || 'Massive',
    dataPolicy: {
      historicalCutoffDate: config.dataPolicy?.historicalCutoffDate,
      intradayProvisionalDate: config.dataPolicy?.intradayProvisionalDate,
      note: 'Uses Massive stock_quotes_1m and option_quotes_1m flat-file CSVs only.',
    },
    startDate: reportStartDate,
    endDate,
    calendarDayCount: calendarDays.length,
    symbols: cleanSymbols,
    initialCapital,
    execution: resolvedExecution,
    assumptions: [
      'Option entries use Massive OPRA 1-minute aggregate close inside the entry window, with a premium haircut applied to short-option proceeds.',
      'Open short options are marked daily from the last available 1-minute option mark; missing marks fall back to max(intrinsic value, prior mark).',
      'Implied volatility and delta filters are Black-Scholes estimates from Massive minute aggregate option prices, not provider Greeks.',
      'Trend and realized-volatility filters use prior daily closes only.',
      'Entries may expire after the report end date; those positions remain open and are marked through the final processed day.',
      'Historical Massive CSV flat files are preferred; live Massive parquet is used only when the historical file is not available for a requested day.',
      'Assignment is modeled at expiration only; early assignment, dividends, borrow constraints, taxes, and margin interest are not modeled.',
      'Cash-put variants liquidate assigned shares at expiration close; wheel variants keep assigned shares and sell covered calls when eligible.',
      'The universe is a liquid local proxy unless a complete holdings file is explicitly supplied.',
    ],
    coverage,
    benchmarks: Object.fromEntries(Object.entries(benchmarks).map(([symbol, points]) => [symbol, summarizeBenchmark(points)])),
    strategies: summaries,
    dailyByStrategy: Object.fromEntries(states.map((state) => [state.id, state.daily])),
    tradesByStrategy: Object.fromEntries(states.map((state) => [state.id, state.trades])),
    checkpoint: serializeCheckpoint({
      startDate: reportStartDate,
      endDate,
      symbols: cleanSymbols,
      initialCapital,
      execution: resolvedExecution,
      states,
      marketHistory,
      benchmarks,
      coverage,
    }),
  };
}

module.exports = {
  DEFAULT_INITIAL_CAPITAL,
  DEFAULT_STRATEGY_CONFIGS,
  EXPERIMENT_STRATEGY_CONFIGS,
  DEFAULT_EXECUTION,
  blackScholesPrice,
  blackScholesDelta,
  impliedVolatility,
  normalizeSymbols,
  mergeStrategyConfig,
  intrinsicValue,
  makeStrategyState,
  computeEquity,
  summarizeStrategy,
  runWheelBacktest,
};
