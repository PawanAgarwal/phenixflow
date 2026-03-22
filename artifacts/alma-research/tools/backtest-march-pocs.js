#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '..');
const OUTPUT_DIR = path.join(ROOT, 'analysis', 'backtests', 'march-2026-poc');
const LEDGER_PATH = path.join(ROOT, 'analysis', 'predictions', 'json', 'daily-ledger.json');
const YAHOO_SYMBOL = '%5EGSPC';
const ET_TIME_ZONE = 'America/New_York';
const START_DATE = '2026-03-01';
const END_DATE = '2026-03-13';
const USER_AGENT = 'Mozilla/5.0';

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
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

function isBusinessDay(date) {
  const day = date.getUTCDay();
  return day >= 1 && day <= 5;
}

function toUtcDate(dateIso) {
  return new Date(`${dateIso}T00:00:00Z`);
}

function listBusinessDates(startIso, endIso) {
  const start = toUtcDate(startIso);
  const end = toUtcDate(endIso);
  const dates = [];
  const cursor = new Date(start);
  while (cursor <= end) {
    if (isBusinessDay(cursor)) dates.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
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

function extractZone(text = '') {
  const match = String(text || '').match(/(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?/);
  if (!match) return null;
  const first = Number(match[1]);
  const second = match[2] ? Number(match[2]) : first;
  return {
    low: Math.min(first, second),
    high: Math.max(first, second),
  };
}

function roundBucket(value, bucket = 5) {
  return Math.round(Number(value) / bucket) * bucket;
}

function parseSignal(row) {
  if (row.instrument !== 'SPX') return null;
  if (!row.targetDate || row.targetDate < START_DATE || row.targetDate > END_DATE) return null;

  const expected = normalizeWhitespace(row.expected || '');
  const breakoutLong = expected.match(/Bullish continuation: above the upside pivot at (\d+(?:\.\d+)?), expect extension toward (\d+(?:\.\d+)?)/i);
  if (breakoutLong) {
    return {
      family: 'breakout_long',
      direction: 'long',
      trigger: Number(breakoutLong[1]),
      target: Number(breakoutLong[2]),
      primaryLevel: Number(breakoutLong[1]),
      row,
    };
  }

  const breakoutShort = expected.match(/Bearish continuation: if the downside pivot at (\d+(?:\.\d+)?) gives way, expect a move toward (\d+(?:\.\d+)?)/i);
  if (breakoutShort) {
    return {
      family: 'breakout_short',
      direction: 'short',
      trigger: Number(breakoutShort[1]),
      target: Number(breakoutShort[2]),
      primaryLevel: Number(breakoutShort[1]),
      row,
    };
  }

  if (/support\/stall zone|support\/stabilization expected|bullish reversal \/ bounce setup/i.test(expected)) {
    const zone = extractZone(expected) || extractZone(row.prediction || '');
    if (zone) {
      return {
        family: 'support_bounce_long',
        direction: 'long',
        zone,
        primaryLevel: zone.high,
        row,
      };
    }
  }

  if (/Upside reversion zone|Rejection\/reversion expected/i.test(expected)) {
    const zone = extractZone(expected) || extractZone(row.prediction || '');
    if (zone) {
      return {
        family: 'rejection_short',
        direction: 'short',
        zone,
        primaryLevel: zone.low,
        row,
      };
    }
  }

  if (/Centroid pivot|Mean-reversion pivot|Magnet \/ pin zone/i.test(expected)) {
    const zone = extractZone(expected) || extractZone(row.prediction || '');
    if (zone) {
      return {
        family: 'centroid_mean_reversion',
        direction: 'both',
        centroid: zone.high,
        primaryLevel: zone.high,
        row,
      };
    }
  }

  return null;
}

function consolidateSignals(signals = []) {
  const grouped = new Map();
  for (const signal of signals) {
    const key = [
      signal.row.targetDate,
      signal.family,
      roundBucket(signal.primaryLevel, 5),
      signal.direction,
    ].join('::');
    const existing = grouped.get(key);
    if (!existing || String(signal.row.madeAt) < String(existing.row.madeAt)) {
      grouped.set(key, signal);
    }
  }
  return Array.from(grouped.values()).sort((left, right) => {
    return String(left.row.targetDate).localeCompare(String(right.row.targetDate))
      || String(left.row.madeAt).localeCompare(String(right.row.madeAt))
      || String(left.family).localeCompare(String(right.family));
  });
}

async function fetchYahooBarsForDate(dateIso) {
  const period1 = Date.parse(makeSessionBoundary(dateIso, '00:00')) / 1000;
  const nextDate = new Date(`${dateIso}T00:00:00Z`);
  nextDate.setUTCDate(nextDate.getUTCDate() + 1);
  const nextIso = nextDate.toISOString().slice(0, 10);
  const period2 = Date.parse(makeSessionBoundary(nextIso, '00:00')) / 1000;
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${YAHOO_SYMBOL}?interval=1m&period1=${Math.floor(period1)}&period2=${Math.floor(period2)}&includePrePost=true&events=div%2Csplits`;
  const response = await fetch(url, {
    headers: {
      'User-Agent': USER_AGENT,
      Accept: 'application/json',
    },
  });
  if (!response.ok) {
    throw new Error(`Yahoo fetch failed for ${dateIso}: ${response.status}`);
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
    const parts = formatInTimeZone(ts);
    const etDate = `${parts.year}-${parts.month}-${parts.day}`;
    const etTime = `${parts.hour}:${parts.minute}`;
    if (etDate !== dateIso) continue;
    if (etTime < '09:30' || etTime > '16:00') continue;
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
  return bars;
}

function barIsAfter(bar, startIso) {
  return String(bar.ts) >= String(startIso || '');
}

function firstTouchAfter(bars, startIso, predicate) {
  return bars.find((bar) => barIsAfter(bar, startIso) && predicate(bar)) || null;
}

function resolveStartIso(row) {
  const sessionOpen = makeSessionBoundary(row.targetDate, '09:30');
  return row.madeAt && String(row.madeAt) > sessionOpen ? row.madeAt : sessionOpen;
}

function exitAtEndOfDay(entryBar, bars, direction) {
  const lastBar = bars[bars.length - 1];
  return {
    exitReason: 'close',
    exitBar: lastBar,
    exitPrice: lastBar?.close ?? entryBar.close,
    pnlPoints: direction === 'long'
      ? (lastBar?.close ?? entryBar.close) - entryBar.entryPrice
      : entryBar.entryPrice - (lastBar?.close ?? entryBar.close),
  };
}

function walkAfterEntry(entryIndex, bars, resolver) {
  for (let index = entryIndex; index < bars.length; index += 1) {
    const decision = resolver(bars[index], index);
    if (decision) return decision;
  }
  return null;
}

function simulateBreakoutLong(signal, bars) {
  const startIso = resolveStartIso(signal.row);
  const entryBar = firstTouchAfter(bars, startIso, (bar) => bar.high >= signal.trigger);
  if (!entryBar) return null;
  const entryIndex = bars.findIndex((bar) => bar.ts === entryBar.ts);
  const entryPrice = signal.trigger;
  const stopDistance = Math.max((signal.target - signal.trigger) * 0.5, 6);
  const stop = signal.trigger - stopDistance;
  const exit = walkAfterEntry(entryIndex, bars, (bar) => {
    if (bar.low <= stop) {
      return {
        exitReason: 'stop',
        exitBar: bar,
        exitPrice: stop,
        pnlPoints: stop - entryPrice,
      };
    }
    if (bar.high >= signal.target) {
      return {
        exitReason: 'target',
        exitBar: bar,
        exitPrice: signal.target,
        pnlPoints: signal.target - entryPrice,
      };
    }
    return null;
  }) || exitAtEndOfDay({ entryPrice }, bars, 'long');
  return {
    family: signal.family,
    targetDate: signal.row.targetDate,
    sourceTitle: signal.row.sourceTitle,
    madeAt: signal.row.madeAt,
    entryTs: entryBar.ts,
    entryPrice,
    stop,
    target: signal.target,
    ...exit,
  };
}

function simulateBreakoutShort(signal, bars) {
  const startIso = resolveStartIso(signal.row);
  const entryBar = firstTouchAfter(bars, startIso, (bar) => bar.low <= signal.trigger);
  if (!entryBar) return null;
  const entryIndex = bars.findIndex((bar) => bar.ts === entryBar.ts);
  const entryPrice = signal.trigger;
  const stopDistance = Math.max((signal.trigger - signal.target) * 0.5, 6);
  const stop = signal.trigger + stopDistance;
  const exit = walkAfterEntry(entryIndex, bars, (bar) => {
    if (bar.high >= stop) {
      return {
        exitReason: 'stop',
        exitBar: bar,
        exitPrice: stop,
        pnlPoints: entryPrice - stop,
      };
    }
    if (bar.low <= signal.target) {
      return {
        exitReason: 'target',
        exitBar: bar,
        exitPrice: signal.target,
        pnlPoints: entryPrice - signal.target,
      };
    }
    return null;
  }) || exitAtEndOfDay({ entryPrice }, bars, 'short');
  return {
    family: signal.family,
    targetDate: signal.row.targetDate,
    sourceTitle: signal.row.sourceTitle,
    madeAt: signal.row.madeAt,
    entryTs: entryBar.ts,
    entryPrice,
    stop,
    target: signal.target,
    ...exit,
  };
}

function simulateSupportBounceLong(signal, bars) {
  const startIso = resolveStartIso(signal.row);
  const zone = signal.zone;
  const entryBar = firstTouchAfter(bars, startIso, (bar) => bar.low <= zone.high && bar.high >= zone.low);
  if (!entryBar) return null;
  const entryIndex = bars.findIndex((bar) => bar.ts === entryBar.ts);
  const entryPrice = zone.high;
  const width = Math.max(zone.high - zone.low, 6);
  const stop = zone.low - width * 0.5;
  const target = entryPrice + width;
  const exit = walkAfterEntry(entryIndex, bars, (bar) => {
    if (bar.low <= stop) {
      return {
        exitReason: 'stop',
        exitBar: bar,
        exitPrice: stop,
        pnlPoints: stop - entryPrice,
      };
    }
    if (bar.high >= target) {
      return {
        exitReason: 'target',
        exitBar: bar,
        exitPrice: target,
        pnlPoints: target - entryPrice,
      };
    }
    return null;
  }) || exitAtEndOfDay({ entryPrice }, bars, 'long');
  return {
    family: signal.family,
    targetDate: signal.row.targetDate,
    sourceTitle: signal.row.sourceTitle,
    madeAt: signal.row.madeAt,
    entryTs: entryBar.ts,
    entryPrice,
    stop,
    target,
    zone,
    ...exit,
  };
}

function simulateRejectionShort(signal, bars) {
  const startIso = resolveStartIso(signal.row);
  const zone = signal.zone;
  const entryBar = firstTouchAfter(bars, startIso, (bar) => bar.high >= zone.low && bar.low <= zone.high);
  if (!entryBar) return null;
  const entryIndex = bars.findIndex((bar) => bar.ts === entryBar.ts);
  const entryPrice = zone.low;
  const width = Math.max(zone.high - zone.low, 6);
  const stop = zone.high + width * 0.5;
  const target = entryPrice - width;
  const exit = walkAfterEntry(entryIndex, bars, (bar) => {
    if (bar.high >= stop) {
      return {
        exitReason: 'stop',
        exitBar: bar,
        exitPrice: stop,
        pnlPoints: entryPrice - stop,
      };
    }
    if (bar.low <= target) {
      return {
        exitReason: 'target',
        exitBar: bar,
        exitPrice: target,
        pnlPoints: entryPrice - target,
      };
    }
    return null;
  }) || exitAtEndOfDay({ entryPrice }, bars, 'short');
  return {
    family: signal.family,
    targetDate: signal.row.targetDate,
    sourceTitle: signal.row.sourceTitle,
    madeAt: signal.row.madeAt,
    entryTs: entryBar.ts,
    entryPrice,
    stop,
    target,
    zone,
    ...exit,
  };
}

function simulateSignal(signal, bars) {
  if (signal.family === 'breakout_long') return simulateBreakoutLong(signal, bars);
  if (signal.family === 'breakout_short') return simulateBreakoutShort(signal, bars);
  if (signal.family === 'support_bounce_long') return simulateSupportBounceLong(signal, bars);
  if (signal.family === 'rejection_short') return simulateRejectionShort(signal, bars);
  return null;
}

function summarizeTrades(trades = []) {
  const pnl = trades.map((trade) => Number(trade.pnlPoints || 0));
  const wins = pnl.filter((value) => value > 0).length;
  const losses = pnl.filter((value) => value < 0).length;
  return {
    trades: trades.length,
    wins,
    losses,
    winRate: trades.length ? wins / trades.length : 0,
    avgPnlPoints: pnl.length ? pnl.reduce((sum, value) => sum + value, 0) / pnl.length : 0,
    totalPnlPoints: pnl.reduce((sum, value) => sum + value, 0),
  };
}

function renderReport(report) {
  const lines = [];
  lines.push('# March 2026 Alma Backtest POC');
  lines.push('');
  lines.push(`Backtest window: ${START_DATE} to ${END_DATE} using Yahoo Finance \`^GSPC\` 1-minute bars.`);
  lines.push('');
  lines.push('## Why This Is A POC');
  lines.push('');
  lines.push('- It only uses SPX-tagged March rows whose `expected` text can be converted into deterministic triggers.');
  lines.push('- It intentionally skips broad macro, cross-asset, and ambiguous commentary.');
  lines.push('- Entry/exit rules are simple placeholders designed to reveal which signal families look testable at all.');
  lines.push('');
  lines.push('## Signal Funnel');
  lines.push('');
  lines.push(`- March rows reviewed: ${report.totalMarchRows}`);
  lines.push(`- March SPX rows reviewed: ${report.totalMarchSpxRows}`);
  lines.push(`- Parseable raw signals: ${report.rawSignals}`);
  lines.push(`- Consolidated signals: ${report.consolidatedSignals}`);
  lines.push('');
  lines.push('| Family | Raw signals | Consolidated | Triggered trades | Win rate | Avg pnl (pts) | Total pnl (pts) |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const family of report.families) {
    lines.push(`| ${family.family} | ${family.rawSignals} | ${family.consolidatedSignals} | ${family.summary.trades} | ${formatNumber(family.summary.winRate * 100, 1)}% | ${formatNumber(family.summary.avgPnlPoints)} | ${formatNumber(family.summary.totalPnlPoints)} |`);
  }
  lines.push('');
  lines.push('## Initial Read');
  lines.push('');
  lines.push('- Breakout-style rows are the cleanest first target because they already contain trigger + direction + target.');
  lines.push('- Support and rejection rows are testable, but target/invalidation still depend on a trade-construction choice rather than Alma explicitly stating both levels every time.');
  lines.push('- Centroid rows are promising, but they need a separate mean-reversion policy before they can be judged fairly.');
  lines.push('');
  lines.push('## Recommended Backtest Design');
  lines.push('');
  lines.push('1. Convert each prediction into a trade schema before simulation.');
  lines.push('2. Separate tradeable price-action rows from non-price rows.');
  lines.push('3. Backtest families independently before combining them.');
  lines.push('4. Use same-day intraday exits first, then add multi-day versions later.');
  lines.push('');
  lines.push('Proposed schema fields:');
  lines.push('- `family`: breakout, support_reversion, rejection_reversion, centroid_mean_reversion, vol_regime, macro');
  lines.push('- `direction`: long, short, both, none');
  lines.push('- `trigger_type`: cross_above, cross_below, first_touch_zone, recapture_centroid, none');
  lines.push('- `trigger_levels`: one or more numeric levels');
  lines.push('- `target_type`: fixed_level, centroid, 1R, close, multi_day_window');
  lines.push('- `target_levels`: one or more numeric levels');
  lines.push('- `stop_type`: fixed_level, opposite_zone_edge, 0.5R_buffer, time_stop');
  lines.push('- `tradeable_confidence`: high, medium, low');
  lines.push('');
  lines.push('## What To Build Next');
  lines.push('');
  lines.push('- Use an LLM classification pass to map each March row into that schema, but only after a deterministic pre-filter keeps the obviously tradeable rows.');
  lines.push('- Start the real backtest with only `breakout_long`, `breakout_short`, `support_bounce_long`, and `rejection_short`.');
  lines.push('- Keep centroid/heatmap strategies as a second module once we decide a fair entry rule.');
  lines.push('- Swap Yahoo bars for ClickHouse SPX minute data later without changing the schema or trade simulator.');
  lines.push('');
  lines.push('## Sample Trades');
  lines.push('');
  lines.push('| Date | Family | Entry (ET) | Entry | Exit reason | Exit | Pnl pts | Source |');
  lines.push('| --- | --- | --- | ---: | --- | ---: | ---: | --- |');
  for (const trade of report.sampleTrades) {
    lines.push(`| ${trade.targetDate} | ${trade.family} | ${trade.entryEt} | ${formatNumber(trade.entryPrice)} | ${trade.exitReason} | ${formatNumber(trade.exitPrice)} | ${formatNumber(trade.pnlPoints)} | ${trade.sourceTitle} |`);
  }
  lines.push('');
  return `${lines.join('\n')}\n`;
}

async function main() {
  const rows = readJson(LEDGER_PATH);
  const marchRows = rows.filter((row) => String(row.targetDate || '').startsWith('2026-03'));
  const marchSpxRows = marchRows.filter((row) => row.instrument === 'SPX' && row.targetDate >= START_DATE && row.targetDate <= END_DATE);

  const rawSignals = marchSpxRows.map(parseSignal).filter(Boolean);
  const consolidated = consolidateSignals(rawSignals);
  const barsByDate = {};
  for (const dateIso of listBusinessDates(START_DATE, END_DATE)) {
    barsByDate[dateIso] = await fetchYahooBarsForDate(dateIso);
  }

  const trades = [];
  for (const signal of consolidated) {
    const bars = barsByDate[signal.row.targetDate] || [];
    if (bars.length === 0) continue;
    const trade = simulateSignal(signal, bars);
    if (trade) trades.push(trade);
  }

  const families = [];
  for (const family of Array.from(new Set(rawSignals.map((signal) => signal.family))).sort()) {
    const familyRaw = rawSignals.filter((signal) => signal.family === family);
    const familyConsolidated = consolidated.filter((signal) => signal.family === family);
    const familyTrades = trades.filter((trade) => trade.family === family);
    families.push({
      family,
      rawSignals: familyRaw.length,
      consolidatedSignals: familyConsolidated.length,
      summary: summarizeTrades(familyTrades),
    });
  }

  const report = {
    generatedAt: new Date().toISOString(),
    totalMarchRows: marchRows.length,
    totalMarchSpxRows: marchSpxRows.length,
    rawSignals: rawSignals.length,
    consolidatedSignals: consolidated.length,
    families,
    trades,
    sampleTrades: trades.slice(0, 15).map((trade) => ({
      ...trade,
      entryEt: formatInTimeZone(Date.parse(trade.entryTs)).hour + ':' + formatInTimeZone(Date.parse(trade.entryTs)).minute,
    })),
  };

  ensureDir(OUTPUT_DIR);
  writeJson(path.join(OUTPUT_DIR, 'report.json'), report);
  writeJson(path.join(OUTPUT_DIR, 'trades.json'), trades);
  writeText(path.join(OUTPUT_DIR, 'README.md'), renderReport(report));

  process.stdout.write(`${JSON.stringify({
    outputDir: OUTPUT_DIR,
    rawSignals: report.rawSignals,
    consolidatedSignals: report.consolidatedSignals,
    trades: trades.length,
    families: families.map((family) => ({
      family: family.family,
      trades: family.summary.trades,
      totalPnlPoints: Number(formatNumber(family.summary.totalPnlPoints)),
    })),
  }, null, 2)}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error.message || error}\n`);
  process.exitCode = 1;
});
