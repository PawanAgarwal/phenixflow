#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '..');
const POSTS_DIR = path.join(ROOT, 'posts');
const OUTPUT_DIR = path.join(ROOT, 'analysis', 'backtests', 'script-rules-feb-march');
const YAHOO_SYMBOL = '%5EGSPC';
const ET_TIME_ZONE = 'America/New_York';
const USER_AGENT = 'Mozilla/5.0';
const MONTH_PATTERN = /^2026-(02|03)-/;
const YAHOO_INTERVALS = ['1m', '2m', '5m'];

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function writeText(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, value, 'utf8');
}

function normalizeWhitespace(value = '') {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function formatNumber(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '';
  return numeric.toFixed(digits);
}

function formatInTimeZone(timestampMs, options = {}) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: ET_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    ...options,
  }).formatToParts(new Date(timestampMs));
  return Object.fromEntries(parts.map((part) => [part.type, part.value]));
}

function getTimeZoneOffsetMs(timestampMs, timeZone = ET_TIME_ZONE) {
  const parts = formatInTimeZone(timestampMs, { timeZone });
  const asUtcMs = Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
    Number(parts.second),
  );
  return asUtcMs - timestampMs;
}

function zonedLocalTimestampToUtcIso(localTimestamp = '', timeZone = ET_TIME_ZONE) {
  const match = String(localTimestamp || '').trim().match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})$/,
  );
  if (!match) return '';
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);

  let utcMs = Date.UTC(year, month - 1, day, hour, minute, second);
  for (let index = 0; index < 3; index += 1) {
    const offsetMs = getTimeZoneOffsetMs(utcMs, timeZone);
    const nextUtcMs = Date.UTC(year, month - 1, day, hour, minute, second) - offsetMs;
    if (nextUtcMs === utcMs) break;
    utcMs = nextUtcMs;
  }
  return new Date(utcMs).toISOString();
}

function makeSessionBoundary(dateIso, localTime) {
  return zonedLocalTimestampToUtcIso(`${dateIso}T${localTime}:00`, ET_TIME_ZONE);
}

function formatEtTime(iso = '') {
  if (!iso) return '';
  const parts = formatInTimeZone(Date.parse(iso));
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute} ET`;
}

function sessionBarsOnly(bars = []) {
  return bars.filter((bar) => bar.etTime >= '09:30' && bar.etTime <= '16:00');
}

async function fetchYahooBarsForDate(dateIso) {
  const period1 = Date.parse(makeSessionBoundary(dateIso, '00:00')) / 1000;
  const nextDate = new Date(`${dateIso}T00:00:00Z`);
  nextDate.setUTCDate(nextDate.getUTCDate() + 1);
  const nextIso = nextDate.toISOString().slice(0, 10);
  const period2 = Date.parse(makeSessionBoundary(nextIso, '00:00')) / 1000;
  let lastError = null;

  for (const interval of YAHOO_INTERVALS) {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${YAHOO_SYMBOL}?interval=${interval}&period1=${Math.floor(period1)}&period2=${Math.floor(period2)}&includePrePost=true&events=div%2Csplits`;
    const response = await fetch(url, {
      headers: {
        'User-Agent': USER_AGENT,
        Accept: 'application/json',
      },
    });
    if (!response.ok) {
      lastError = new Error(`Yahoo fetch failed for ${dateIso} at ${interval}: ${response.status}`);
      if (response.status === 422) continue;
      throw lastError;
    }
    const payload = await response.json();
    const result = payload?.chart?.result?.[0];
    const timestamps = result?.timestamp || [];
    const quote = result?.indicators?.quote?.[0] || {};
    const bars = [];
    for (let index = 0; index < timestamps.length; index += 1) {
      const ts = Number(timestamps[index]) * 1000;
      const open = Number(quote.open?.[index]);
      const high = Number(quote.high?.[index]);
      const low = Number(quote.low?.[index]);
      const close = Number(quote.close?.[index]);
      if (![open, high, low, close].every(Number.isFinite)) continue;
      if (![open, high, low, close].every((value) => value > 0)) continue;
      const parts = formatInTimeZone(ts);
      const etDate = `${parts.year}-${parts.month}-${parts.day}`;
      const etTime = `${parts.hour}:${parts.minute}`;
      if (etDate !== dateIso) continue;
      bars.push({
        ts: new Date(ts).toISOString(),
        etDate,
        etTime,
        open,
        high,
        low,
        close,
      });
    }
    return {
      interval,
      bars: sessionBarsOnly(bars),
    };
  }

  throw lastError || new Error(`Yahoo fetch failed for ${dateIso}`);
}

function parseScriptBlock(content = '') {
  const match = content.match(/SCRIPT INPUTS=== SPX closed at ([0-9.]+) ===\n\n([\s\S]*?)(?:\n\n=== ES closed at|$)/);
  if (!match) return null;
  const close = Number(match[1]);
  const tokens = match[2]
    .split(',')
    .map((token) => normalizeWhitespace(token))
    .filter((token) => token.length > 0);
  const numeric = (value) => {
    const cleaned = String(value || '').replace(/%/g, '').trim();
    const result = Number(cleaned);
    return Number.isFinite(result) ? result : null;
  };
  if (tokens.length < 18) return null;
  return {
    close,
    upper4: numeric(tokens[1]),
    upper3: numeric(tokens[2]),
    upper2: numeric(tokens[4]),
    upperRisk: numeric(tokens[6]),
    upper1: numeric(tokens[8]),
    lower1: numeric(tokens[10]),
    lowerRisk: numeric(tokens[12]),
    lower2: numeric(tokens[14]),
    lower3: numeric(tokens[15]),
    lower4: numeric(tokens[17]),
  };
}

function parseDateFromDirName(dirName = '') {
  const match = String(dirName).match(/^(\d{4}-\d{2}-\d{2})_/);
  return match ? match[1] : '';
}

function readScriptInputDays() {
  const entries = fs.readdirSync(POSTS_DIR, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory() && MONTH_PATTERN.test(entry.name))
    .map((entry) => {
      const contentPath = path.join(POSTS_DIR, entry.name, 'content.txt');
      if (!fs.existsSync(contentPath)) return null;
      const content = fs.readFileSync(contentPath, 'utf8');
      const levels = parseScriptBlock(content);
      if (!levels) return null;
      return {
        sourceDir: entry.name,
        sourcePath: contentPath,
        targetDate: parseDateFromDirName(entry.name),
        month: parseDateFromDirName(entry.name).slice(0, 7),
        ...levels,
      };
    })
    .filter(Boolean)
    .sort((left, right) => String(left.targetDate).localeCompare(String(right.targetDate)));
}

function sigmaEstimate(day) {
  return (Math.abs(day.upper1 - day.close) + Math.abs(day.close - day.lower1)) / 2;
}

function firstIndex(bars, predicate) {
  for (let index = 0; index < bars.length; index += 1) {
    if (predicate(bars[index], index)) return index;
  }
  return -1;
}

function tradePathMetrics(bars, entryIndex, exitIndex, direction, entryPrice) {
  let mfe = 0;
  let mae = 0;
  const start = Math.max(entryIndex + 1, 0);
  const end = Math.max(exitIndex, entryIndex);
  for (let index = start; index <= end && index < bars.length; index += 1) {
    const bar = bars[index];
    if (direction === 'long') {
      mfe = Math.max(mfe, bar.high - entryPrice);
      mae = Math.max(mae, entryPrice - bar.low);
    } else {
      mfe = Math.max(mfe, entryPrice - bar.low);
      mae = Math.max(mae, bar.high - entryPrice);
    }
  }
  return { mfePoints: mfe, maePoints: mae };
}

function finalizeTrade(base, bars, entryIndex, exitIndex, exitPrice, exitReason) {
  const direction = base.side === 'long' ? 'long' : 'short';
  const pnlPoints = direction === 'long'
    ? exitPrice - base.entryPrice
    : base.entryPrice - exitPrice;
  const metrics = tradePathMetrics(bars, entryIndex, exitIndex, direction, base.entryPrice);
  const exitBar = bars[exitIndex] || bars[bars.length - 1];
  return {
    ...base,
    exitTs: exitBar?.ts || '',
    exitEt: exitBar?.etTime || '',
    exitPrice,
    exitReason,
    stopTriggered: exitReason === 'stop',
    pnlPoints,
    pnlR: base.riskPoints > 0 ? pnlPoints / base.riskPoints : null,
    ...metrics,
  };
}

function simulateAfterEntry(base, bars, entryIndex) {
  const stop = base.stopPrice;
  const target = base.targetPrice;
  for (let index = entryIndex + 1; index < bars.length; index += 1) {
    const bar = bars[index];
    if (base.side === 'long') {
      const hitStop = Number.isFinite(stop) && bar.low <= stop;
      const hitTarget = Number.isFinite(target) && bar.high >= target;
      if (hitStop && hitTarget) return finalizeTrade(base, bars, entryIndex, index, stop, 'stop');
      if (hitStop) return finalizeTrade(base, bars, entryIndex, index, stop, 'stop');
      if (hitTarget) return finalizeTrade(base, bars, entryIndex, index, target, 'target');
    } else {
      const hitStop = Number.isFinite(stop) && bar.high >= stop;
      const hitTarget = Number.isFinite(target) && bar.low <= target;
      if (hitStop && hitTarget) return finalizeTrade(base, bars, entryIndex, index, stop, 'stop');
      if (hitStop) return finalizeTrade(base, bars, entryIndex, index, stop, 'stop');
      if (hitTarget) return finalizeTrade(base, bars, entryIndex, index, target, 'target');
    }
  }
  const lastIndex = bars.length - 1;
  const lastBar = bars[lastIndex];
  return finalizeTrade(base, bars, entryIndex, lastIndex, lastBar?.close ?? base.entryPrice, 'close');
}

function buildBaseTrade(day, strategy, side, entryBar, targetPrice, stopPrice, rationale) {
  const riskPoints = side === 'long'
    ? entryBar.close - stopPrice
    : stopPrice - entryBar.close;
  return {
    targetDate: day.targetDate,
    month: day.month,
    sourceDir: day.sourceDir,
    sourcePath: day.sourcePath,
    dataInterval: day.dataInterval,
    strategy,
    side,
    rationale,
    scriptClose: day.close,
    sigma: sigmaEstimate(day),
    entryTs: entryBar.ts,
    entryEt: entryBar.etTime,
    entryPrice: entryBar.close,
    targetPrice,
    stopPrice,
    riskPoints,
  };
}

function riskFadeTrade(day, bars, side) {
  const isUpper = side === 'short';
  const touchLevel = isUpper ? day.upperRisk : day.lowerRisk;
  const innerLevel = isUpper ? day.upper1 : day.lower1;
  const stopLevel = isUpper ? day.upper2 : day.lower2;
  const targetLevel = day.close;

  const touchIndex = firstIndex(bars, (bar) => (isUpper ? bar.high >= touchLevel : bar.low <= touchLevel));
  if (touchIndex < 0) return null;
  const confirmIndex = firstIndex(
    bars.slice(touchIndex).map((bar, idx) => ({ bar, idx: idx + touchIndex })),
    ({ bar }) => (isUpper ? bar.close <= innerLevel : bar.close >= innerLevel),
  );
  if (confirmIndex < 0) return null;
  const entryIndex = touchIndex + confirmIndex;
  const entryBar = bars[entryIndex];
  if (isUpper && entryBar.close <= targetLevel) return null;
  if (!isUpper && entryBar.close >= targetLevel) return null;
  const base = buildBaseTrade(
    day,
    'risk_fade',
    isUpper ? 'short' : 'long',
    entryBar,
    targetLevel,
    stopLevel,
    isUpper
      ? 'Touched upper risk band and reclaimed inside +1 sigma.'
      : 'Touched lower risk band and reclaimed inside -1 sigma.',
  );
  if (!(base.riskPoints > 0)) return null;
  return simulateAfterEntry(base, bars, entryIndex);
}

function breakout2SigmaTrade(day, bars, side) {
  const isUpper = side === 'long';
  const band2 = isUpper ? day.upper2 : day.lower2;
  const band3 = isUpper ? day.upper3 : day.lower3;
  const riskBand = isUpper ? day.upperRisk : day.lowerRisk;

  let entryIndex = -1;
  for (let index = 1; index < bars.length; index += 1) {
    if (isUpper) {
      if (bars[index - 1].close >= band2 && bars[index].close >= band2) {
        entryIndex = index;
        break;
      }
    } else if (bars[index - 1].close <= band2 && bars[index].close <= band2) {
      entryIndex = index;
      break;
    }
  }
  if (entryIndex < 0) return null;
  const entryBar = bars[entryIndex];
  if (isUpper && entryBar.close >= band3) return null;
  if (!isUpper && entryBar.close <= band3) return null;
  const base = buildBaseTrade(
    day,
    'two_sigma_breakout',
    isUpper ? 'long' : 'short',
    entryBar,
    band3,
    riskBand,
    isUpper
      ? 'Two consecutive closes above +2 sigma.'
      : 'Two consecutive closes below -2 sigma.',
  );
  if (!(base.riskPoints > 0)) return null;
  return simulateAfterEntry(base, bars, entryIndex);
}

function exhaustion3SigmaTrade(day, bars, side) {
  const isUpper = side === 'short';
  const band3 = isUpper ? day.upper3 : day.lower3;
  const band2 = isUpper ? day.upper2 : day.lower2;
  const band4 = isUpper ? day.upper4 : day.lower4;

  const touchIndex = firstIndex(bars, (bar) => (isUpper ? bar.high >= band3 : bar.low <= band3));
  if (touchIndex < 0) return null;
  const confirmIndex = firstIndex(
    bars.slice(touchIndex).map((bar, idx) => ({ bar, idx: idx + touchIndex })),
    ({ bar }) => (isUpper ? bar.close <= band3 : bar.close >= band3),
  );
  if (confirmIndex < 0) return null;
  const entryIndex = touchIndex + confirmIndex;
  const entryBar = bars[entryIndex];
  if (isUpper && entryBar.close <= band2) return null;
  if (!isUpper && entryBar.close >= band2) return null;
  const base = buildBaseTrade(
    day,
    'three_sigma_exhaustion',
    isUpper ? 'short' : 'long',
    entryBar,
    band2,
    band4,
    isUpper
      ? 'Touched +3 sigma and closed back inside the tail zone.'
      : 'Touched -3 sigma and closed back inside the tail zone.',
  );
  if (!(base.riskPoints > 0)) return null;
  return simulateAfterEntry(base, bars, entryIndex);
}

function summarizeTrades(trades = []) {
  const totalTrades = trades.length;
  const pnl = trades.map((trade) => trade.pnlPoints);
  const grossProfit = pnl.filter((value) => value > 0).reduce((sum, value) => sum + value, 0);
  const grossLoss = pnl.filter((value) => value < 0).reduce((sum, value) => sum + value, 0);
  const netProfit = grossProfit + grossLoss;
  const wins = pnl.filter((value) => value > 0).length;
  const losses = pnl.filter((value) => value < 0).length;
  const stopsTriggered = trades.filter((trade) => trade.stopTriggered).length;
  const profitFactor = grossLoss < 0 ? grossProfit / Math.abs(grossLoss) : null;

  let running = 0;
  let peak = 0;
  let maxDrawdown = 0;
  for (const trade of trades) {
    running += trade.pnlPoints;
    peak = Math.max(peak, running);
    maxDrawdown = Math.max(maxDrawdown, peak - running);
  }

  return {
    trades: totalTrades,
    wins,
    losses,
    winRate: totalTrades ? wins / totalTrades : 0,
    grossProfit,
    grossLoss,
    netProfit,
    avgTrade: totalTrades ? netProfit / totalTrades : 0,
    avgWinner: wins ? grossProfit / wins : 0,
    avgLoser: losses ? grossLoss / losses : 0,
    stopsTriggered,
    profitFactor,
    maxDrawdown,
  };
}

function groupBy(trades, keyFn) {
  const map = new Map();
  for (const trade of trades) {
    const key = keyFn(trade);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(trade);
  }
  return Array.from(map.entries()).map(([key, values]) => [key, values]);
}

function renderSummaryRows(pairs) {
  return pairs
    .map(([name, trades]) => [name, summarizeTrades(trades)])
    .sort((left, right) => String(left[0]).localeCompare(String(right[0])));
}

function renderMetricsRow(name, summary) {
  return `| ${name} | ${summary.trades} | ${summary.wins} | ${summary.losses} | ${formatNumber(summary.winRate * 100, 1)}% | ${formatNumber(summary.grossProfit)} | ${formatNumber(summary.grossLoss)} | ${formatNumber(summary.netProfit)} | ${formatNumber(summary.maxDrawdown)} | ${summary.stopsTriggered} | ${summary.profitFactor === null ? '' : formatNumber(summary.profitFactor, 2)} |`;
}

function renderTradeRow(trade) {
  return `| ${trade.targetDate} | ${trade.dataInterval} | ${trade.strategy} | ${trade.side} | ${trade.entryEt} | ${formatNumber(trade.entryPrice)} | ${formatNumber(trade.stopPrice)} | ${formatNumber(trade.targetPrice)} | ${trade.exitEt} | ${formatNumber(trade.exitPrice)} | ${trade.exitReason} | ${formatNumber(trade.pnlPoints)} | ${formatNumber(trade.maePoints)} | ${formatNumber(trade.mfePoints)} | [${trade.sourceDir}](${trade.sourcePath}) |`;
}

function renderReport(report) {
  const lines = [];
  lines.push('# Script Rules Feb-March Backtest');
  lines.push('');
  lines.push('This backtest uses only the raw SPX `SCRIPT INPUTS` ladder and the three deterministic rule families derived from it.');
  lines.push('');
  lines.push('Rules used:');
  lines.push('- `risk_fade`: touch the risk band, reclaim inside `1 sigma`, target prior close, stop at `2 sigma`.');
  lines.push('- `two_sigma_breakout`: two consecutive closes beyond `2 sigma`, target `3 sigma`, stop at risk band.');
  lines.push('- `three_sigma_exhaustion`: touch `3 sigma`, close back inside the tail zone, target `2 sigma`, stop at `4 sigma`.');
  lines.push('- Intrabar ambiguity is handled conservatively: if stop and target could both hit in the same 1-minute bar, the trade is counted as stopped.');
  lines.push('');
  lines.push('## Coverage');
  lines.push('');
  lines.push(`- Script-input days tested: ${report.days}`);
  lines.push(`- Date range: ${report.firstDate} to ${report.lastDate}`);
  lines.push(`- Gross trades triggered: ${report.overall.trades}`);
  lines.push(`- Yahoo interval usage: ${report.intervalUsage.map(([name, count]) => `${name}=${count}`).join(', ')}`);
  lines.push('');
  lines.push('## Overall');
  lines.push('');
  lines.push('| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  lines.push(renderMetricsRow('All trades', report.overall));
  lines.push('');
  lines.push('## By Strategy');
  lines.push('');
  lines.push('| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const [name, summary] of report.byStrategy) {
    lines.push(renderMetricsRow(name, summary));
  }
  lines.push('');
  lines.push('## By Month');
  lines.push('');
  lines.push('| Bucket | Trades | Wins | Losses | Win rate | Gross profit | Gross loss | Net profit | Max drawdown | Stops | Profit factor |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const [name, summary] of report.byMonth) {
    lines.push(renderMetricsRow(name, summary));
  }
  lines.push('');
  lines.push('## Stops Triggered');
  lines.push('');
  lines.push('| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Exit ET | Exit | PnL | Source |');
  lines.push('| --- | --- | --- | --- | --- | ---: | ---: | --- | ---: | ---: | --- |');
  for (const trade of report.trades.filter((trade) => trade.stopTriggered)) {
    lines.push(`| ${trade.targetDate} | ${trade.dataInterval} | ${trade.strategy} | ${trade.side} | ${trade.entryEt} | ${formatNumber(trade.entryPrice)} | ${formatNumber(trade.stopPrice)} | ${trade.exitEt} | ${formatNumber(trade.exitPrice)} | ${formatNumber(trade.pnlPoints)} | [${trade.sourceDir}](${trade.sourcePath}) |`);
  }
  lines.push('');
  lines.push('## All Trades');
  lines.push('');
  lines.push('| Date | Data | Strategy | Side | Entry ET | Entry | Stop | Target | Exit ET | Exit | Exit reason | PnL | MAE | MFE | Source |');
  lines.push('| --- | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | --- | ---: | ---: | ---: | --- |');
  for (const trade of report.trades) {
    lines.push(renderTradeRow(trade));
  }
  return `${lines.join('\n')}\n`;
}

async function main() {
  const days = readScriptInputDays();
  const trades = [];
  const dayDiagnostics = [];

  for (const day of days) {
    const fetched = await fetchYahooBarsForDate(day.targetDate);
    const bars = fetched.bars;
    if (bars.length === 0) continue;
    day.dataInterval = fetched.interval;
    const dayTrades = [
      riskFadeTrade(day, bars, 'short'),
      riskFadeTrade(day, bars, 'long'),
      breakout2SigmaTrade(day, bars, 'long'),
      breakout2SigmaTrade(day, bars, 'short'),
      exhaustion3SigmaTrade(day, bars, 'short'),
      exhaustion3SigmaTrade(day, bars, 'long'),
    ].filter(Boolean);

    trades.push(...dayTrades);
    dayDiagnostics.push({
      targetDate: day.targetDate,
      month: day.month,
      sourceDir: day.sourceDir,
      sourcePath: day.sourcePath,
      dataInterval: fetched.interval,
      scriptClose: day.close,
      sigma: sigmaEstimate(day),
      upper1: day.upper1,
      upperRisk: day.upperRisk,
      upper2: day.upper2,
      upper3: day.upper3,
      upper4: day.upper4,
      lower1: day.lower1,
      lowerRisk: day.lowerRisk,
      lower2: day.lower2,
      lower3: day.lower3,
      lower4: day.lower4,
      sessionOpen: bars[0]?.open ?? null,
      sessionHigh: Math.max(...bars.map((bar) => bar.high)),
      sessionLow: Math.min(...bars.map((bar) => bar.low)),
      sessionClose: bars[bars.length - 1]?.close ?? null,
      tradeCount: dayTrades.length,
    });
  }

  trades.sort((left, right) => String(left.exitTs).localeCompare(String(right.exitTs)) || String(left.entryTs).localeCompare(String(right.entryTs)));

  const overall = summarizeTrades(trades);
  const byStrategy = renderSummaryRows(groupBy(trades, (trade) => trade.strategy));
  const byMonth = renderSummaryRows(groupBy(trades, (trade) => trade.month));
  const intervalUsage = Array.from(
    dayDiagnostics.reduce((map, row) => {
      map.set(row.dataInterval, (map.get(row.dataInterval) || 0) + 1);
      return map;
    }, new Map()).entries(),
  ).sort((left, right) => String(left[0]).localeCompare(String(right[0])));
  const report = {
    generatedAt: new Date().toISOString(),
    days: dayDiagnostics.length,
    firstDate: dayDiagnostics[0]?.targetDate || '',
    lastDate: dayDiagnostics[dayDiagnostics.length - 1]?.targetDate || '',
    overall,
    byStrategy,
    byMonth,
    intervalUsage,
    trades,
    dayDiagnostics,
  };

  ensureDir(OUTPUT_DIR);
  writeJson(path.join(OUTPUT_DIR, 'report.json'), report);
  writeJson(path.join(OUTPUT_DIR, 'trades.json'), trades);
  writeJson(path.join(OUTPUT_DIR, 'day-diagnostics.json'), dayDiagnostics);
  writeText(path.join(OUTPUT_DIR, 'README.md'), renderReport(report));

  console.log(JSON.stringify({
    outputDir: OUTPUT_DIR,
    days: report.days,
    overall,
    byStrategy,
    byMonth,
    trades: trades.length,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
