#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const readline = require('node:readline');
const zlib = require('node:zlib');

const { availableDates } = require('../src/calendar');
const {
  artifactPath,
  datasetCsvPath,
  ensureDir,
  loadConfig,
} = require('../src/config');
const { buildDailyContextByDate, readStockMinutesForDay, safeReturn } = require('../src/data');
const { parseOpraTicker, daysBetween } = require('../src/opra');
const { getEtParts, nsToMs } = require('../src/time');

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
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

function round(value, digits = 4) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : null;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function stdev(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(values.reduce((sum, value) => sum + ((value - avg) ** 2), 0) / (values.length - 1));
}

function sharpe(values) {
  const sd = stdev(values);
  return sd ? (mean(values) / sd) * Math.sqrt(252) : null;
}

function maxDrawdown(values) {
  let equity = 0;
  let peak = 0;
  let drawdown = 0;
  for (const value of values) {
    equity += value;
    peak = Math.max(peak, equity);
    drawdown = Math.min(drawdown, equity - peak);
  }
  return drawdown;
}

function monthKey(dayIso) {
  return dayIso.slice(0, 7);
}

function rootBounds(root) {
  const normalized = String(root || '').toUpperCase();
  const last = normalized[normalized.length - 1];
  const next = `${normalized.slice(0, -1)}${String.fromCharCode(last.charCodeAt(0) + 1)}`;
  return {
    start: `O:${normalized}`,
    stop: `O:${next}`,
  };
}

async function readOptionMinuteBarsForRoot(config, dayIso, root, settings = {}) {
  const filePath = datasetCsvPath(config, 'optionBars', dayIso);
  const byTicker = new Map();
  const byMinute = new Map();
  if (!fs.existsSync(filePath)) return { byTicker, byMinute, rows: 0 };

  const maxDte = settings.maxDteUniverse ?? 14;
  const { start, stop } = rootBounds(root);
  const fileStream = fs.createReadStream(filePath);
  const gunzip = zlib.createGunzip();
  const reader = readline.createInterface({
    input: fileStream.pipe(gunzip),
    crlfDelay: Infinity,
  });

  let isHeader = true;
  let started = false;
  let rows = 0;
  for await (const line of reader) {
    if (!line) continue;
    if (isHeader) {
      isHeader = false;
      continue;
    }
    if (!started && line < start) continue;
    started = true;
    if (line >= stop) {
      reader.close();
      fileStream.destroy();
      gunzip.destroy();
      break;
    }
    if (!line.startsWith(start)) continue;
    const values = line.split(',');
    const ticker = values[0];
    const parsed = parseOpraTicker(ticker);
    if (!parsed || parsed.root !== root) continue;
    const dte = daysBetween(dayIso, parsed.expiration);
    if (dte === null || dte < 0 || dte > maxDte) continue;
    const volume = Number(values[1]);
    const open = Number(values[2]);
    const close = Number(values[3]);
    const high = Number(values[4]);
    const low = Number(values[5]);
    const minuteMs = nsToMs(values[6]);
    const transactions = Number(values[7]);
    if (!Number.isFinite(minuteMs) || !(open > 0) || !(close > 0) || !(high > 0) || !(low > 0)) continue;
    const row = {
      ticker,
      root,
      right: parsed.right,
      strike: parsed.strike,
      expiration: parsed.expiration,
      dte,
      minuteMs,
      open,
      high,
      low,
      close,
      volume: Number.isFinite(volume) ? volume : 0,
      transactions: Number.isFinite(transactions) ? transactions : 0,
    };
    rows += 1;
    if (!byTicker.has(ticker)) byTicker.set(ticker, []);
    byTicker.get(ticker).push(row);
    const bucket = byMinute.get(minuteMs) || { CALL: [], PUT: [] };
    bucket[row.right].push(row);
    byMinute.set(minuteMs, bucket);
  }

  byTicker.forEach((list) => list.sort((left, right) => left.minuteMs - right.minuteMs));
  byMinute.forEach((bucket) => {
    bucket.CALL.sort((left, right) => left.dte - right.dte || Math.abs(left.strike) - Math.abs(right.strike));
    bucket.PUT.sort((left, right) => left.dte - right.dte || Math.abs(left.strike) - Math.abs(right.strike));
  });
  return { byTicker, byMinute, rows };
}

function indexStockMinutes(stockMinutes) {
  const out = new Map();
  stockMinutes.forEach((rows, symbol) => {
    const byMinute = new Map();
    rows.forEach((row) => byMinute.set(row.minuteMs, row));
    out.set(symbol, byMinute);
  });
  return out;
}

function buildStockSignals(config, dayIso, stockMinutes, dailyContext) {
  const bySymbol = indexStockMinutes(stockMinutes);
  const tslaRows = stockMinutes.get('TSLA') || [];
  if (!tslaRows.length) return [];
  const first15 = tslaRows.filter((row) => {
    const et = getEtParts(row.minuteMs);
    return et.minuteOfDayEt >= config.session.regularOpenMinuteEt
      && et.minuteOfDayEt < config.session.regularOpenMinuteEt + 15;
  });
  const first30 = tslaRows.filter((row) => {
    const et = getEtParts(row.minuteMs);
    return et.minuteOfDayEt >= config.session.regularOpenMinuteEt
      && et.minuteOfDayEt < config.session.regularOpenMinuteEt + 30;
  });
  const orb15High = first15.length ? Math.max(...first15.map((row) => row.high)) : null;
  const orb15Low = first15.length ? Math.min(...first15.map((row) => row.low)) : null;
  const orb30High = first30.length ? Math.max(...first30.map((row) => row.high)) : null;
  const orb30Low = first30.length ? Math.min(...first30.map((row) => row.low)) : null;
  const orb15Close = first15[first15.length - 1]?.close ?? null;
  const orb30Close = first30[first30.length - 1]?.close ?? null;
  const sessionOpen = tslaRows[0]?.open ?? null;
  const tslaDaily = dailyContext?.TSLA || {};
  const qqqDaily = dailyContext?.QQQ || {};
  const spyDaily = dailyContext?.SPY || {};
  const dailyTrendUpCount = (tslaDaily.trendUp || 0) + (qqqDaily.trendUp || 0) + (spyDaily.trendUp || 0);
  const dailyTrendDownCount = (tslaDaily.trendDown || 0) + (qqqDaily.trendDown || 0) + (spyDaily.trendDown || 0);

  let cumulativeDollarVolume = 0;
  let cumulativeVolume = 0;
  let dayHighSoFar = -Infinity;
  let dayLowSoFar = Infinity;
  const enriched = [];

  return tslaRows.map((tsla, index) => {
    const qqq = bySymbol.get('QQQ')?.get(tsla.minuteMs);
    const spy = bySymbol.get('SPY')?.get(tsla.minuteMs);
    const tsll = bySymbol.get('TSLL')?.get(tsla.minuteMs);
    const et = getEtParts(tsla.minuteMs);
    const previous = enriched[index - 1];
    const typicalPrice = (tsla.high + tsla.low + tsla.close) / 3;
    cumulativeDollarVolume += typicalPrice * (tsla.volume || 0);
    cumulativeVolume += tsla.volume || 0;
    dayHighSoFar = Math.max(dayHighSoFar, tsla.high);
    dayLowSoFar = Math.min(dayLowSoFar, tsla.low);
    const tslaVwap = cumulativeVolume > 0 ? cumulativeDollarVolume / cumulativeVolume : tsla.close;
    const prev30 = tslaRows[index - 30];
    const prev60 = tslaRows[index - 60];
    const prev10 = tslaRows[index - 10];
    const qqqRows = stockMinutes.get('QQQ') || [];
    const qqqIndex = qqqRows.findIndex((row) => row.minuteMs === tsla.minuteMs);
    const qqqPrev30 = qqqIndex >= 30 ? qqqRows[qqqIndex - 30] : null;
    const qqqPrev60 = qqqIndex >= 60 ? qqqRows[qqqIndex - 60] : null;
    const signal = {
      dayIso,
      minuteMs: tsla.minuteMs,
      minuteOfDayEt: et.minuteOfDayEt,
      minutesFromOpen: et.minuteOfDayEt - config.session.regularOpenMinuteEt,
      minutesToClose: config.session.regularCloseMinuteEt - et.minuteOfDayEt - 1,
      tsla,
      qqq,
      spy,
      tsll,
      tslaRet1: tsla.ret1 || 0,
      tslaRet5: tsla.ret5 || 0,
      tslaRet10: safeReturn(tsla.close, prev10?.close),
      tslaRet15: tsla.ret15 || 0,
      tslaRet30: safeReturn(tsla.close, prev30?.close),
      tslaRet60: safeReturn(tsla.close, prev60?.close),
      qqqRet1: qqq?.ret1 || 0,
      qqqRet5: qqq?.ret5 || 0,
      qqqRet15: qqq?.ret15 || 0,
      qqqRet30: safeReturn(qqq?.close, qqqPrev30?.close),
      qqqRet60: safeReturn(qqq?.close, qqqPrev60?.close),
      spyRet5: spy?.ret5 || 0,
      tsllRet1: tsll?.ret1 || 0,
      tsllRet5: tsll?.ret5 || 0,
      tsllRet15: tsll?.ret15 || 0,
      tslaVwap,
      tslaVwapDistance: safeReturn(tsla.close, tslaVwap),
      previousTslaVwapDistance: previous?.tslaVwapDistance || 0,
      dayHighSoFar,
      dayLowSoFar,
      dayHighPullbackPct: dayHighSoFar > 0 ? (dayHighSoFar - tsla.close) / tsla.close : 0,
      dayLowBouncePct: dayLowSoFar > 0 ? (tsla.close - dayLowSoFar) / dayLowSoFar : 0,
      rangeSoFarPct: dayLowSoFar > 0 ? (dayHighSoFar - dayLowSoFar) / dayLowSoFar : 0,
      orb15High,
      orb15Low,
      orb15Return: safeReturn(orb15Close, first15[0]?.open),
      orb30High,
      orb30Low,
      orb30Return: safeReturn(orb30Close, first30[0]?.open),
      sessionOpen,
      dailyContextReady: tslaDaily.ready && qqqDaily.ready ? 1 : 0,
      dailyTslaTrendUp: tslaDaily.trendUp || 0,
      dailyTslaTrendDown: tslaDaily.trendDown || 0,
      dailyQqqTrendUp: qqqDaily.trendUp || 0,
      dailyQqqTrendDown: qqqDaily.trendDown || 0,
      dailyMacroTrendUp: dailyTrendUpCount >= 2 ? 1 : 0,
      dailyMacroTrendDown: dailyTrendDownCount >= 2 ? 1 : 0,
      dailyTslaGapPct: safeReturn(sessionOpen, tslaDaily.prevClose),
      dailyTslaGapAtr: tslaDaily.atr14 ? (sessionOpen - tslaDaily.prevClose) / tslaDaily.atr14 : 0,
      dailyTslaFromPrevCloseAtr: tslaDaily.atr14 ? (tsla.close - tslaDaily.prevClose) / tslaDaily.atr14 : 0,
      dailyTslaAtrPct: tslaDaily.atrPct14 || 0,
    };
    enriched.push(signal);
    return {
      ...signal,
    };
  });
}

function directionForSignal(signal, candidate) {
  return candidate.direction(signal);
}

function selectContract(signal, direction, optionByMinute, selector) {
  const bucket = optionByMinute.get(signal.minuteMs);
  if (!bucket) return null;
  const rows = bucket[direction] || [];
  const spot = signal.tsla?.close;
  if (!(spot > 0)) return null;
  const filtered = rows.filter((row) => (
    row.dte >= selector.minDte
    && row.dte <= selector.maxDte
    && Math.abs((row.strike / spot) - 1) <= selector.maxMoneyness
    && row.close >= selector.minPremium
    && row.close <= selector.maxPremium
    && row.volume >= selector.minVolume1m
    && row.transactions >= selector.minTransactions1m
  ));
  if (!filtered.length) return null;
  const targetMoneyness = direction === 'CALL'
    ? (selector.callTargetMoneyness ?? selector.targetMoneyness ?? 0)
    : (selector.putTargetMoneyness ?? selector.targetMoneyness ?? 0);
  filtered.sort((left, right) => {
    const dteDiff = left.dte - right.dte;
    if (dteDiff) return dteDiff;
    const leftMny = Math.abs(((left.strike / spot) - 1) - targetMoneyness);
    const rightMny = Math.abs(((right.strike / spot) - 1) - targetMoneyness);
    if (leftMny !== rightMny) return leftMny - rightMny;
    return right.volume - left.volume;
  });
  return filtered[0];
}

function barAtOrAfter(rows, minuteMs) {
  if (!rows?.length) return null;
  let lo = 0;
  let hi = rows.length - 1;
  let ans = rows.length;
  while (lo <= hi) {
    const mid = Math.floor((lo + hi) / 2);
    if (rows[mid].minuteMs >= minuteMs) {
      ans = mid;
      hi = mid - 1;
    } else {
      lo = mid + 1;
    }
  }
  return ans < rows.length ? { index: ans, row: rows[ans] } : null;
}

function imbalance(numerator, denominator) {
  return denominator > 0 ? numerator / denominator : 0;
}

function buildOptionFlowByMinute(optionByMinute) {
  const out = new Map();
  const rolling = [];
  let callPremium5 = 0;
  let putPremium5 = 0;
  let callVolume5 = 0;
  let putVolume5 = 0;
  const minutes = [...optionByMinute.keys()].sort((left, right) => left - right);
  for (const minuteMs of minutes) {
    const bucket = optionByMinute.get(minuteMs) || {};
    let callPremium1 = 0;
    let putPremium1 = 0;
    let callVolume1 = 0;
    let putVolume1 = 0;
    (bucket.CALL || []).forEach((row) => {
      if (row.dte > 7) return;
      callPremium1 += (row.volume || 0) * row.close * 100;
      callVolume1 += row.volume || 0;
    });
    (bucket.PUT || []).forEach((row) => {
      if (row.dte > 7) return;
      putPremium1 += (row.volume || 0) * row.close * 100;
      putVolume1 += row.volume || 0;
    });
    rolling.push({
      minuteMs,
      callPremium1,
      putPremium1,
      callVolume1,
      putVolume1,
    });
    callPremium5 += callPremium1;
    putPremium5 += putPremium1;
    callVolume5 += callVolume1;
    putVolume5 += putVolume1;
    while (rolling.length && minuteMs - rolling[0].minuteMs >= 5 * 60_000) {
      const stale = rolling.shift();
      callPremium5 -= stale.callPremium1;
      putPremium5 -= stale.putPremium1;
      callVolume5 -= stale.callVolume1;
      putVolume5 -= stale.putVolume1;
    }
    const premium1 = callPremium1 + putPremium1;
    const premium5 = callPremium5 + putPremium5;
    const volume5 = callVolume5 + putVolume5;
    out.set(minuteMs, {
      optionCallPremium1: callPremium1,
      optionPutPremium1: putPremium1,
      optionPremium1: premium1,
      optionPremiumImbalance1: imbalance(callPremium1 - putPremium1, premium1),
      optionCallPremium5: callPremium5,
      optionPutPremium5: putPremium5,
      optionPremium5: premium5,
      optionPremiumImbalance5: imbalance(callPremium5 - putPremium5, premium5),
      optionCallVolume5: callVolume5,
      optionPutVolume5: putVolume5,
      optionVolume5: volume5,
      optionVolumeImbalance5: imbalance(callVolume5 - putVolume5, volume5),
    });
  }
  return out;
}

function addOptionFlowToSignals(signals, optionByMinute) {
  const flowByMinute = buildOptionFlowByMinute(optionByMinute);
  return signals.map((signal) => ({
    optionCallPremium1: 0,
    optionPutPremium1: 0,
    optionPremium1: 0,
    optionPremiumImbalance1: 0,
    optionCallPremium5: 0,
    optionPutPremium5: 0,
    optionPremium5: 0,
    optionPremiumImbalance5: 0,
    optionCallVolume5: 0,
    optionPutVolume5: 0,
    optionVolume5: 0,
    optionVolumeImbalance5: 0,
    ...signal,
    ...(flowByMinute.get(signal.minuteMs) || {}),
  }));
}

function simulateOptionTrade({ signal, contract, byTicker, exit, costPctPerSide }) {
  const rows = byTicker.get(contract.ticker);
  const entryHit = barAtOrAfter(rows, signal.minuteMs + 60_000);
  if (!entryHit || entryHit.row.minuteMs !== signal.minuteMs + 60_000) return null;
  const entry = entryHit.row.open;
  if (!(entry > 0)) return null;
  const target = entry * (1 + exit.targetPct);
  const stop = entry * (1 - exit.stopPct);
  let activeStop = stop;
  let highWatermark = entry;
  const lastIndex = Math.min(rows.length - 1, entryHit.index + exit.maxHoldMinutes - 1);
  let exitPrice = rows[lastIndex]?.close;
  let exitReason = 'timeout';
  let exitMinuteMs = rows[lastIndex]?.minuteMs;

  for (let index = entryHit.index; index <= lastIndex; index += 1) {
    const row = rows[index];
    highWatermark = Math.max(highWatermark, row.high);
    if (exit.breakevenAfterPct && highWatermark >= entry * (1 + exit.breakevenAfterPct)) {
      activeStop = Math.max(activeStop, entry * (1 + (exit.breakevenOffsetPct || 0)));
    }
    if (exit.trailAfterPct && exit.trailingStopPct && highWatermark >= entry * (1 + exit.trailAfterPct)) {
      activeStop = Math.max(activeStop, highWatermark * (1 - exit.trailingStopPct));
    }
    if (row.low <= activeStop && row.high >= target) {
      exitPrice = activeStop;
      exitReason = 'stop_same_bar';
      exitMinuteMs = row.minuteMs;
      break;
    }
    if (row.low <= activeStop) {
      exitPrice = activeStop;
      exitReason = 'stop';
      exitMinuteMs = row.minuteMs;
      break;
    }
    if (row.high >= target) {
      exitPrice = target;
      exitReason = 'target';
      exitMinuteMs = row.minuteMs;
      break;
    }
    if (exit.staleLossAfterMinutes && index - entryHit.index + 1 >= exit.staleLossAfterMinutes) {
      const openPnl = (row.close / entry) - 1;
      if (openPnl <= exit.staleLossPct) {
        exitPrice = row.close;
        exitReason = 'stale_loss';
        exitMinuteMs = row.minuteMs;
        break;
      }
    }
    if (exit.profitLockAfterMinutes && index - entryHit.index + 1 >= exit.profitLockAfterMinutes) {
      const openPnl = (row.close / entry) - 1;
      if (openPnl >= exit.profitLockPct) {
        exitPrice = row.close;
        exitReason = 'profit_lock';
        exitMinuteMs = row.minuteMs;
        break;
      }
    }
    if (exit.trailMarketWeakAfterMinutes && index - entryHit.index + 1 >= exit.trailMarketWeakAfterMinutes) {
      const openPnl = (row.close / entry) - 1;
      if (openPnl > 0 && exit.marketWeak(signal)) {
        exitPrice = row.close;
        exitReason = 'market_weak_lock';
        exitMinuteMs = row.minuteMs;
        break;
      }
    }
  }
  if (!(exitPrice > 0)) return null;
  const adjustedEntry = entry * (1 + costPctPerSide);
  const adjustedExit = exitPrice * (1 - costPctPerSide);
  const grossDollars = (exitPrice - entry) * 100;
  const netDollars = (adjustedExit - adjustedEntry) * 100;
  return {
    date: signal.dayIso,
    month: monthKey(signal.dayIso),
    signalMinuteMs: signal.minuteMs,
    entryMinuteMs: entryHit.row.minuteMs,
    exitMinuteMs,
    direction: contract.right,
    ticker: contract.ticker,
    dte: contract.dte,
    strike: contract.strike,
    spot: signal.tsla.close,
    entry,
    exit: exitPrice,
    grossDollars,
    netDollars,
    netReturn: adjustedEntry ? netDollars / (adjustedEntry * 100) : 0,
    entryCapital: adjustedEntry * 100,
    exitReason,
  };
}

function buildCandidateGrid(signals, selectors, exits, cooldownMinutes = 5) {
  const out = [];
  signals.forEach((signal) => {
    selectors.forEach((selector) => {
      exits.forEach((exit) => {
        out.push({
          id: `${signal.id}__${selector.id}__${exit.id}`,
          signal,
          selector,
          exit,
          cooldownMinutes,
        });
      });
    });
  });
  return out;
}

function focusedCandidates() {
  const selectors = [
    {
      id: 'dte0_7_itm_very_liq',
      minDte: 0,
      maxDte: 7,
      maxMoneyness: 0.07,
      callTargetMoneyness: -0.03,
      putTargetMoneyness: 0.03,
      minPremium: 1.5,
      maxPremium: 24,
      minVolume1m: 35,
      minTransactions1m: 10,
    },
    {
      id: 'dte1_7_atm_very_liq',
      minDte: 1,
      maxDte: 7,
      maxMoneyness: 0.03,
      minPremium: 1.5,
      maxPremium: 18,
      minVolume1m: 35,
      minTransactions1m: 10,
    },
    {
      id: 'dte3_14_itm_hi_prem',
      minDte: 3,
      maxDte: 14,
      maxMoneyness: 0.09,
      callTargetMoneyness: -0.04,
      putTargetMoneyness: 0.04,
      minPremium: 3,
      maxPremium: 35,
      minVolume1m: 15,
      minTransactions1m: 5,
    },
    {
      id: 'dte7_30_itm_hi_prem',
      minDte: 7,
      maxDte: 30,
      maxMoneyness: 0.11,
      callTargetMoneyness: -0.05,
      putTargetMoneyness: 0.05,
      minPremium: 5,
      maxPremium: 55,
      minVolume1m: 8,
      minTransactions1m: 3,
    },
  ];
  const exits = [
    {
      id: 't25_s10_h15_stale',
      targetPct: 0.25,
      stopPct: 0.10,
      maxHoldMinutes: 15,
      staleLossAfterMinutes: 5,
      staleLossPct: -0.025,
    },
    {
      id: 't30_s14_h20_be',
      targetPct: 0.30,
      stopPct: 0.14,
      maxHoldMinutes: 20,
      breakevenAfterPct: 0.16,
      breakevenOffsetPct: 0.02,
    },
    {
      id: 't35_s16_h25_trail',
      targetPct: 0.35,
      stopPct: 0.16,
      maxHoldMinutes: 25,
      trailAfterPct: 0.20,
      trailingStopPct: 0.12,
    },
    {
      id: 't45_s22_h35_trail',
      targetPct: 0.45,
      stopPct: 0.22,
      maxHoldMinutes: 35,
      trailAfterPct: 0.25,
      trailingStopPct: 0.16,
      staleLossAfterMinutes: 10,
      staleLossPct: -0.04,
    },
    {
      id: 't60_s28_h45_be_trail',
      targetPct: 0.60,
      stopPct: 0.28,
      maxHoldMinutes: 45,
      breakevenAfterPct: 0.24,
      breakevenOffsetPct: 0.03,
      trailAfterPct: 0.35,
      trailingStopPct: 0.20,
    },
  ];
  const signals = [
    {
      id: 'daily_up_vwap_pullback_call',
      direction(signal) {
        if (
          signal.dailyContextReady
          && signal.dailyTslaTrendUp
          && signal.dailyQqqTrendUp
          && Math.abs(signal.dailyTslaGapAtr) <= 1.2
          && signal.minutesFromOpen >= 30
          && signal.minutesToClose >= 30
          && signal.tslaVwapDistance >= 0
          && signal.tslaRet60 >= 0.005
          && signal.qqqRet15 >= -0.0005
          && signal.tslaRet1 <= -0.0012
          && signal.tslaRet5 > -0.004
          && signal.dayHighPullbackPct >= 0.002
        ) return 'CALL';
        return null;
      },
    },
    {
      id: 'daily_down_vwap_pullback_put',
      direction(signal) {
        if (
          signal.dailyContextReady
          && signal.dailyTslaTrendDown
          && signal.dailyQqqTrendDown
          && Math.abs(signal.dailyTslaGapAtr) <= 1.2
          && signal.minutesFromOpen >= 30
          && signal.minutesToClose >= 30
          && signal.tslaVwapDistance <= 0
          && signal.tslaRet60 <= -0.005
          && signal.qqqRet15 <= 0.0005
          && signal.tslaRet1 >= 0.0012
          && signal.tslaRet5 < 0.004
          && signal.dayLowBouncePct >= 0.002
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'qqq_stable_tsla_flush_call',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendDown === 0
          && signal.qqqRet5 >= -0.0008
          && signal.qqqRet15 >= -0.0015
          && signal.tslaRet5 <= -0.0045
          && signal.tslaRet1 <= -0.001
          && signal.tslaVwapDistance >= -0.0075
          && Math.abs(signal.dailyTslaFromPrevCloseAtr) <= 1.6
        ) return 'CALL';
        return null;
      },
    },
    {
      id: 'qqq_stable_tsla_pop_put',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendUp === 0
          && signal.qqqRet5 <= 0.0008
          && signal.qqqRet15 <= 0.0015
          && signal.tslaRet5 >= 0.0045
          && signal.tslaRet1 >= 0.001
          && signal.tslaVwapDistance <= 0.0075
          && Math.abs(signal.dailyTslaFromPrevCloseAtr) <= 1.6
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'orb30_drive_pullback',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 35
          && signal.minutesToClose >= 30
          && signal.orb30Return >= 0.006
          && signal.tsla.close >= signal.orb30High
          && signal.tslaRet1 <= -0.001
          && signal.tslaRet15 >= 0.003
          && signal.qqqRet15 >= 0
          && signal.tslaVwapDistance >= -0.002
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 35
          && signal.minutesToClose >= 30
          && signal.orb30Return <= -0.006
          && signal.tsla.close <= signal.orb30Low
          && signal.tslaRet1 >= 0.001
          && signal.tslaRet15 <= -0.003
          && signal.qqqRet15 <= 0
          && signal.tslaVwapDistance <= 0.002
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'failed_orb30_reclaim',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 45
          && signal.minutesToClose >= 30
          && signal.orb30Low
          && signal.tsla.low <= signal.orb30Low * 0.998
          && signal.tsla.close >= signal.orb30Low * 1.002
          && signal.qqqRet5 >= 0
          && signal.dailyMacroTrendDown === 0
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 45
          && signal.minutesToClose >= 30
          && signal.orb30High
          && signal.tsla.high >= signal.orb30High * 1.002
          && signal.tsla.close <= signal.orb30High * 0.998
          && signal.qqqRet5 <= 0
          && signal.dailyMacroTrendUp === 0
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'filtered_trend_momentum',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendUp
          && signal.tslaVwapDistance >= 0.002
          && signal.tslaRet15 >= 0.008
          && signal.qqqRet15 >= 0.001
          && signal.tslaRet1 >= -0.0005
          && signal.tslaRet1 <= 0.0035
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendDown
          && signal.tslaVwapDistance <= -0.002
          && signal.tslaRet15 <= -0.008
          && signal.qqqRet15 <= -0.001
          && signal.tslaRet1 <= 0.0005
          && signal.tslaRet1 >= -0.0035
        ) return 'PUT';
        return null;
      },
    },
  ];
  return buildCandidateGrid(signals, selectors, exits, 8);
}

function flowCandidates() {
  const selectors = [
    {
      id: 'dte0_7_itm_very_liq',
      minDte: 0,
      maxDte: 7,
      maxMoneyness: 0.07,
      callTargetMoneyness: -0.03,
      putTargetMoneyness: 0.03,
      minPremium: 1.5,
      maxPremium: 24,
      minVolume1m: 35,
      minTransactions1m: 10,
    },
    {
      id: 'dte3_14_itm_hi_prem',
      minDte: 3,
      maxDte: 14,
      maxMoneyness: 0.09,
      callTargetMoneyness: -0.04,
      putTargetMoneyness: 0.04,
      minPremium: 3,
      maxPremium: 35,
      minVolume1m: 15,
      minTransactions1m: 5,
    },
    {
      id: 'dte7_30_itm_hi_prem',
      minDte: 7,
      maxDte: 30,
      maxMoneyness: 0.11,
      callTargetMoneyness: -0.05,
      putTargetMoneyness: 0.05,
      minPremium: 5,
      maxPremium: 55,
      minVolume1m: 8,
      minTransactions1m: 3,
    },
  ];
  const exits = [
    {
      id: 't30_s14_h20_be',
      targetPct: 0.30,
      stopPct: 0.14,
      maxHoldMinutes: 20,
      breakevenAfterPct: 0.16,
      breakevenOffsetPct: 0.02,
    },
    {
      id: 't45_s22_h35_trail',
      targetPct: 0.45,
      stopPct: 0.22,
      maxHoldMinutes: 35,
      trailAfterPct: 0.25,
      trailingStopPct: 0.16,
      staleLossAfterMinutes: 10,
      staleLossPct: -0.04,
    },
    {
      id: 't60_s28_h45_be_trail',
      targetPct: 0.60,
      stopPct: 0.28,
      maxHoldMinutes: 45,
      breakevenAfterPct: 0.24,
      breakevenOffsetPct: 0.03,
      trailAfterPct: 0.35,
      trailingStopPct: 0.20,
    },
  ];
  const enoughFlow = (signal) => signal.optionPremium5 >= 250_000;
  const signals = [
    {
      id: 'flow_confirmed_trend_momentum',
      direction(signal) {
        if (
          enoughFlow(signal)
          && signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendUp
          && signal.tslaVwapDistance >= 0.002
          && signal.tslaRet15 >= 0.008
          && signal.qqqRet15 >= 0.001
          && signal.optionPremiumImbalance5 >= 0.12
          && signal.optionPremiumImbalance1 >= -0.10
        ) return 'CALL';
        if (
          enoughFlow(signal)
          && signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendDown
          && signal.tslaVwapDistance <= -0.002
          && signal.tslaRet15 <= -0.008
          && signal.qqqRet15 <= -0.001
          && signal.optionPremiumImbalance5 <= -0.12
          && signal.optionPremiumImbalance1 <= 0.10
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'flow_confirmed_vwap_pullback',
      direction(signal) {
        if (
          enoughFlow(signal)
          && signal.dailyContextReady
          && signal.dailyTslaTrendUp
          && signal.dailyQqqTrendUp
          && signal.minutesFromOpen >= 30
          && signal.minutesToClose >= 30
          && signal.tslaVwapDistance >= 0
          && signal.tslaRet60 >= 0.005
          && signal.tslaRet1 <= -0.0012
          && signal.tslaRet5 > -0.004
          && signal.optionPremiumImbalance5 >= 0
        ) return 'CALL';
        if (
          enoughFlow(signal)
          && signal.dailyContextReady
          && signal.dailyTslaTrendDown
          && signal.dailyQqqTrendDown
          && signal.minutesFromOpen >= 30
          && signal.minutesToClose >= 30
          && signal.tslaVwapDistance <= 0
          && signal.tslaRet60 <= -0.005
          && signal.tslaRet1 >= 0.0012
          && signal.tslaRet5 < 0.004
          && signal.optionPremiumImbalance5 <= 0
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'flow_divergence_fade',
      direction(signal) {
        if (
          enoughFlow(signal)
          && signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendDown === 0
          && signal.tslaRet5 <= -0.0045
          && signal.qqqRet5 >= -0.0008
          && signal.optionPremiumImbalance5 >= -0.05
          && signal.tslaVwapDistance >= -0.0075
        ) return 'CALL';
        if (
          enoughFlow(signal)
          && signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 25
          && signal.dailyMacroTrendUp === 0
          && signal.tslaRet5 >= 0.0045
          && signal.qqqRet5 <= 0.0008
          && signal.optionPremiumImbalance5 <= 0.05
          && signal.tslaVwapDistance <= 0.0075
        ) return 'PUT';
        return null;
      },
    },
  ];
  return buildCandidateGrid(signals, selectors, exits, 8);
}

function defaultCandidates(candidateSet = 'default') {
  if (candidateSet === 'focused') return focusedCandidates();
  if (candidateSet === 'flow') return flowCandidates();
  const selectors = [
    {
      id: 'dte0_7_atm_liq',
      minDte: 0,
      maxDte: 7,
      maxMoneyness: 0.035,
      minPremium: 0.75,
      maxPremium: 12,
      minVolume1m: 10,
      minTransactions1m: 3,
    },
    {
      id: 'dte0_14_atm_liq',
      minDte: 0,
      maxDte: 14,
      maxMoneyness: 0.04,
      minPremium: 0.75,
      maxPremium: 15,
      minVolume1m: 10,
      minTransactions1m: 3,
    },
    {
      id: 'dte0_7_atm_very_liq',
      minDte: 0,
      maxDte: 7,
      maxMoneyness: 0.03,
      minPremium: 1,
      maxPremium: 10,
      minVolume1m: 25,
      minTransactions1m: 8,
    },
    {
      id: 'dte0_7_itm_liq',
      minDte: 0,
      maxDte: 7,
      maxMoneyness: 0.06,
      callTargetMoneyness: -0.02,
      putTargetMoneyness: 0.02,
      minPremium: 1.25,
      maxPremium: 18,
      minVolume1m: 10,
      minTransactions1m: 3,
    },
    {
      id: 'dte1_14_atm_liq',
      minDte: 1,
      maxDte: 14,
      maxMoneyness: 0.04,
      minPremium: 0.75,
      maxPremium: 15,
      minVolume1m: 10,
      minTransactions1m: 3,
    },
  ];
  const exits = [
    { id: 't8_s6_h5', targetPct: 0.08, stopPct: 0.06, maxHoldMinutes: 5 },
    { id: 't12_s8_h8', targetPct: 0.12, stopPct: 0.08, maxHoldMinutes: 8 },
    { id: 't16_s10_h10', targetPct: 0.16, stopPct: 0.10, maxHoldMinutes: 10 },
    { id: 't12_s8_h8_lock', targetPct: 0.12, stopPct: 0.08, maxHoldMinutes: 8, profitLockAfterMinutes: 3, profitLockPct: 0.06 },
    { id: 't20_s12_h15', targetPct: 0.20, stopPct: 0.12, maxHoldMinutes: 15 },
    { id: 't25_s15_h20', targetPct: 0.25, stopPct: 0.15, maxHoldMinutes: 20 },
    { id: 't20_s12_h15_lock', targetPct: 0.20, stopPct: 0.12, maxHoldMinutes: 15, profitLockAfterMinutes: 5, profitLockPct: 0.10 },
  ];
  const signals = [
    {
      id: 'tsla_qqq_momentum',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 10
          && signal.minutesToClose >= 15
          && signal.tslaRet5 >= 0.004
          && signal.qqqRet5 >= 0
          && signal.tsllRet5 >= 0.006
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 10
          && signal.minutesToClose >= 15
          && signal.tslaRet5 <= -0.004
          && signal.qqqRet5 <= 0
          && signal.tsllRet5 <= -0.006
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'tsla_pullback_continuation',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 10
          && signal.minutesToClose >= 15
          && signal.tslaRet15 >= 0.004
          && signal.qqqRet5 >= -0.0005
          && signal.tslaRet1 <= -0.001
          && signal.tsllRet1 <= 0
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 10
          && signal.minutesToClose >= 15
          && signal.tslaRet15 <= -0.004
          && signal.qqqRet5 <= 0.0005
          && signal.tslaRet1 >= 0.001
          && signal.tsllRet1 >= 0
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'tsla_pullback_continuation_strict',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 15
          && signal.minutesToClose >= 20
          && signal.tslaRet15 >= 0.006
          && signal.tsllRet15 >= 0.009
          && signal.qqqRet5 >= 0
          && signal.tslaRet1 <= -0.0012
          && signal.tslaRet5 >= -0.001
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 15
          && signal.minutesToClose >= 20
          && signal.tslaRet15 <= -0.006
          && signal.tsllRet15 <= -0.009
          && signal.qqqRet5 <= 0
          && signal.tslaRet1 >= 0.0012
          && signal.tslaRet5 <= 0.001
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'tsla_call_pullback_only',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 15
          && signal.minutesToClose >= 20
          && signal.tslaRet30 >= 0.006
          && signal.tsllRet15 >= 0.006
          && signal.qqqRet15 >= -0.0005
          && signal.tslaRet1 <= -0.001
          && signal.tslaRet5 > -0.003
        ) return 'CALL';
        return null;
      },
    },
    {
      id: 'tsla_put_pullback_only',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 15
          && signal.minutesToClose >= 20
          && signal.tslaRet30 <= -0.006
          && signal.tsllRet15 <= -0.006
          && signal.qqqRet15 <= 0.0005
          && signal.tslaRet1 >= 0.001
          && signal.tslaRet5 < 0.003
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'orb15_breakout',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 15
          && signal.orb15High
          && signal.tsla.close > signal.orb15High
          && signal.qqqRet5 >= 0
          && signal.tsllRet5 >= 0
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 20
          && signal.minutesToClose >= 15
          && signal.orb15Low
          && signal.tsla.close < signal.orb15Low
          && signal.qqqRet5 <= 0
          && signal.tsllRet5 <= 0
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'tsll_fast_momentum',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 5
          && signal.minutesToClose >= 15
          && signal.tsllRet1 >= 0.004
          && signal.tslaRet1 >= 0.0015
          && signal.qqqRet1 >= 0
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 5
          && signal.minutesToClose >= 15
          && signal.tsllRet1 <= -0.004
          && signal.tslaRet1 <= -0.0015
          && signal.qqqRet1 <= 0
        ) return 'PUT';
        return null;
      },
    },
    {
      id: 'qqq_confirmed_reversal',
      direction(signal) {
        if (
          signal.minutesFromOpen >= 15
          && signal.minutesToClose >= 15
          && signal.tslaRet30 >= 0.003
          && signal.tslaRet5 <= -0.002
          && signal.qqqRet15 >= 0
        ) return 'CALL';
        if (
          signal.minutesFromOpen >= 15
          && signal.minutesToClose >= 15
          && signal.tslaRet30 <= -0.003
          && signal.tslaRet5 >= 0.002
          && signal.qqqRet15 <= 0
        ) return 'PUT';
        return null;
      },
    },
  ];

  return buildCandidateGrid(signals, selectors, exits, 5);
}

function createAgg(candidate) {
  return {
    id: candidate.id,
    signalId: candidate.signal.id,
    selectorId: candidate.selector.id,
    exitId: candidate.exit.id,
    trades: [],
    daily: new Map(),
  };
}

function updateAgg(agg, trade) {
  agg.trades.push(trade);
  const day = agg.daily.get(trade.date) || {
    date: trade.date,
    month: trade.month,
    trades: 0,
    netDollars: 0,
    grossDollars: 0,
    entryCapital: 0,
    wins: 0,
  };
  day.trades += 1;
  day.netDollars += trade.netDollars;
  day.grossDollars += trade.grossDollars;
  day.entryCapital += trade.entryCapital;
  day.wins += trade.netDollars > 0 ? 1 : 0;
  agg.daily.set(trade.date, day);
}

function summarizeDaily(days, allDates) {
  const byDate = new Map(days.map((day) => [day.date, day]));
  const aligned = allDates.map((date) => byDate.get(date) || {
    date,
    month: monthKey(date),
    trades: 0,
    netDollars: 0,
    grossDollars: 0,
    entryCapital: 0,
    wins: 0,
  });
  const pnl = aligned.map((day) => day.netDollars);
  const totalNet = pnl.reduce((sum, value) => sum + value, 0);
  const totalGross = aligned.reduce((sum, day) => sum + day.grossDollars, 0);
  const totalCapital = aligned.reduce((sum, day) => sum + day.entryCapital, 0);
  const totalTrades = aligned.reduce((sum, day) => sum + day.trades, 0);
  const totalWins = aligned.reduce((sum, day) => sum + day.wins, 0);
  return {
    days: aligned.length,
    tradedDays: aligned.filter((day) => day.trades > 0).length,
    positiveDays: aligned.filter((day) => day.netDollars > 0).length,
    trades: totalTrades,
    winRate: totalTrades ? round(totalWins / totalTrades, 6) : 0,
    netDollars: round(totalNet, 2),
    grossDollars: round(totalGross, 2),
    pnlPerContract: round(totalNet, 2),
    returnOnPremiumTurnover: totalCapital ? round(totalNet / totalCapital, 6) : 0,
    maxDrawdownDollars: round(maxDrawdown(pnl), 2),
    sharpeDailyPnl: round(sharpe(pnl), 3),
    avgDailyPnl: round(mean(pnl), 4),
    dailyPnlStdev: round(stdev(pnl), 4),
  };
}

function summarizeAgg(agg, dates) {
  const allDays = [...agg.daily.values()].sort((left, right) => left.date.localeCompare(right.date));
  const months = new Map();
  allDays.forEach((day) => {
    if (!months.has(day.month)) months.set(day.month, []);
    months.get(day.month).push(day);
  });
  return {
    id: agg.id,
    signalId: agg.signalId,
    selectorId: agg.selectorId,
    exitId: agg.exitId,
    overall: summarizeDaily(allDays, dates),
    train2025: summarizeDaily(allDays.filter((day) => day.date < '2026-01-01'), dates.filter((date) => date < '2026-01-01')),
    test2026Ytd: summarizeDaily(allDays.filter((day) => day.date >= '2026-01-01'), dates.filter((date) => date >= '2026-01-01')),
    months: [...months.entries()].sort().map(([month, days]) => ({
      month,
      ...summarizeDaily(days, dates.filter((date) => monthKey(date) === month)),
    })),
  };
}

function renderMarkdown(payload) {
  const lines = [];
  lines.push('# TSLA Option Scalping Ideas');
  lines.push('');
  lines.push(`Window: ${payload.startDate} to ${payload.endDate}`);
  lines.push(`Underlying option root: ${payload.root}`);
  lines.push(`Candidate set: ${payload.candidateSet}`);
  lines.push(`Cost model: ${(payload.costPctPerSide * 100).toFixed(2)}% per side on option premium.`);
  lines.push('');
  lines.push('| Rank | Candidate | Trades | Net $/contract | 2026 YTD $ | Turnover return | Max DD $ | Sharpe |');
  lines.push('| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |');
  payload.top.forEach((item, index) => {
    lines.push(`| ${index + 1} | ${item.id} | ${item.overall.trades} | ${item.overall.netDollars.toFixed(2)} | ${item.test2026Ytd.netDollars.toFixed(2)} | ${(item.overall.returnOnPremiumTurnover * 100).toFixed(2)}% | ${item.overall.maxDrawdownDollars.toFixed(2)} | ${item.overall.sharpeDailyPnl?.toFixed(3) ?? ''} |`);
  });
  lines.push('');
  lines.push('## Notes');
  lines.push('');
  lines.push('- Entry uses the next 1-minute option bar open after a completed stock signal minute.');
  lines.push('- Stop/target uses option minute high/low; same-minute target+stop is counted as stop first.');
  lines.push('- This is not a bid/ask/NBBO fill simulation.');
  lines.push('');
  return `${lines.join('\n')}\n`;
}

async function main() {
  const args = parseArgs();
  const config = loadConfig();
  const startDate = args.start || '2025-01-02';
  const endDate = args.end || '2026-05-08';
  const root = String(args.root || 'TSLA').toUpperCase();
  const costPctPerSide = Number(args.costPctPerSide ?? 0.01);
  const candidateSet = String(args.candidateSet || args.set || 'default');
  const candidates = defaultCandidates(candidateSet);
  const aggs = new Map(candidates.map((candidate) => [candidate.id, createAgg(candidate)]));
  const dates = availableDates(config, startDate, endDate, ['stockBars', 'optionBars']);
  const maxDteUniverse = Math.max(14, ...candidates.map((candidate) => candidate.selector.maxDte || 0));
  const { dailyContextByDate } = ['focused', 'flow'].includes(candidateSet)
    ? await buildDailyContextByDate(config, dates, { symbols: ['SPY', 'QQQ', 'TSLA', 'TSLL'] })
    : { dailyContextByDate: new Map() };
  const startedAt = Date.now();
  console.log(`[option-scalp] dates=${dates.length} candidates=${candidates.length} root=${root} set=${candidateSet} maxDte=${maxDteUniverse}`);

  for (let dateIndex = 0; dateIndex < dates.length; dateIndex += 1) {
    const dayIso = dates[dateIndex];
    const [stockMinutes, optionData] = await Promise.all([
      readStockMinutesForDay(config, dayIso, ['SPY', 'QQQ', 'TSLA', 'TSLL']),
      readOptionMinuteBarsForRoot(config, dayIso, root, { maxDteUniverse }),
    ]);
    const stockSignals = buildStockSignals(config, dayIso, stockMinutes, dailyContextByDate.get(dayIso));
    const signals = candidateSet === 'flow'
      ? addOptionFlowToSignals(stockSignals, optionData.byMinute)
      : stockSignals;
    const lastEntryByCandidate = new Map();
    for (const signal of signals) {
      for (const candidate of candidates) {
        if (signal.minuteMs <= (lastEntryByCandidate.get(candidate.id) || -Infinity)) continue;
        const direction = directionForSignal(signal, candidate.signal);
        if (!direction) continue;
        const contract = selectContract(signal, direction, optionData.byMinute, candidate.selector);
        if (!contract) continue;
        const trade = simulateOptionTrade({
          signal,
          contract,
          byTicker: optionData.byTicker,
          exit: candidate.exit,
          costPctPerSide,
        });
        if (!trade) continue;
        updateAgg(aggs.get(candidate.id), trade);
        lastEntryByCandidate.set(candidate.id, signal.minuteMs + (candidate.cooldownMinutes * 60_000));
      }
    }
    const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`[option-scalp] ${dateIndex + 1}/${dates.length} ${dayIso} optionRows=${optionData.rows} elapsed=${elapsed}s`);
  }

  const results = [...aggs.values()].map((agg) => summarizeAgg(agg, dates));
  const top = results
    .filter((result) => result.overall.trades >= 50)
    .sort((left, right) => {
      const testDiff = right.test2026Ytd.netDollars - left.test2026Ytd.netDollars;
      if (Math.abs(testDiff) > 1e-9) return testDiff;
      return right.overall.netDollars - left.overall.netDollars;
    })
    .slice(0, 25);
  const payload = {
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    root,
    candidateSet,
    maxDteUniverse,
    costPctPerSide,
    assumptions: {
      provider: 'Massive local option_quotes_1m plus stock_quotes_1m.',
      entry: 'Signal at completed stock minute; option entry at next minute open.',
      exit: 'Minute-bar target/stop with same-minute stop-first assumption.',
      caveat: 'No true bid/ask or queue simulation; use as directional option-scaping research only.',
    },
    top,
    results,
  };
  const setSuffix = candidateSet === 'default' ? '' : `-${candidateSet}`;
  const costSuffix = costPctPerSide === 0.01 ? '' : `-cost${String(costPctPerSide * 100).replace('.', 'p')}pct`;
  const suffix = `${root.toLowerCase()}${setSuffix}-${startDate}-${endDate}${costSuffix}`;
  const outJson = artifactPath(`option-scalping-ideas-${suffix}.json`);
  const outMd = artifactPath(`option-scalping-ideas-${suffix}.md`);
  ensureDir(path.dirname(outJson));
  fs.writeFileSync(outJson, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(outMd, renderMarkdown(payload));
  console.log(JSON.stringify({
    outJson,
    outMd,
    top: top.slice(0, 8).map((item) => ({
      id: item.id,
      trades: item.overall.trades,
      netDollars: item.overall.netDollars,
      test2026YtdDollars: item.test2026Ytd.netDollars,
      sharpeDailyPnl: item.overall.sharpeDailyPnl,
      maxDrawdownDollars: item.overall.maxDrawdownDollars,
    })),
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
