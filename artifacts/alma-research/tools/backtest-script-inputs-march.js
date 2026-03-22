#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '..');
const POSTS_DIR = path.join(ROOT, 'posts');
const OUTPUT_DIR = path.join(ROOT, 'analysis', 'backtests', 'script-inputs-march');
const YAHOO_SYMBOL = '%5EGSPC';
const ET_TIME_ZONE = 'America/New_York';
const USER_AGENT = 'Mozilla/5.0';

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

function formatNumber(value, digits = 2) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '';
  return numeric.toFixed(digits);
}

function normalizeWhitespace(value = '') {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
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

function sessionBarsOnly(bars = []) {
  return bars.filter((bar) => bar.etTime >= '09:30' && bar.etTime <= '16:00');
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
  return sessionBarsOnly(bars);
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
  if (tokens.length < 16) return null;
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
    rawTokens: tokens,
  };
}

function parseDateFromDirName(dirName = '') {
  const match = String(dirName).match(/^(\d{4}-\d{2}-\d{2})_/);
  return match ? match[1] : '';
}

function readMarchScriptInputs() {
  const entries = fs.readdirSync(POSTS_DIR, { withFileTypes: true });
  return entries
    .filter((entry) => entry.isDirectory() && /^2026-03-/.test(entry.name))
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
        ...levels,
      };
    })
    .filter(Boolean)
    .sort((left, right) => String(left.targetDate).localeCompare(String(right.targetDate)));
}

function summarizeContainment(day, bars) {
  const high = Math.max(...bars.map((bar) => bar.high));
  const low = Math.min(...bars.map((bar) => bar.low));
  const close = bars[bars.length - 1]?.close ?? null;
  return {
    open: bars[0]?.open ?? null,
    high,
    low,
    close,
    closeWithin1: close !== null && close <= day.upper1 && close >= day.lower1,
    closeWithin2: close !== null && close <= day.upper2 && close >= day.lower2,
    closeWithin3: close !== null && close <= day.upper3 && close >= day.lower3,
    closeWithin4: close !== null && close <= day.upper4 && close >= day.lower4,
    rangeWithin1: high <= day.upper1 && low >= day.lower1,
    rangeWithin2: high <= day.upper2 && low >= day.lower2,
    rangeWithin3: high <= day.upper3 && low >= day.lower3,
    rangeWithin4: high <= day.upper4 && low >= day.lower4,
  };
}

function findFirstTouchIndex(bars, side, level) {
  if (!Number.isFinite(level)) return -1;
  for (let index = 0; index < bars.length; index += 1) {
    const bar = bars[index];
    if (side === 'upper' && bar.high >= level) return index;
    if (side === 'lower' && bar.low <= level) return index;
  }
  return -1;
}

function resolveBandOutcome(bars, startIndex, side, innerLevel, outerLevel) {
  if (startIndex < 0) return 'not_touched';
  for (let index = startIndex + 1; index < bars.length; index += 1) {
    const bar = bars[index];
    if (side === 'upper') {
      if (Number.isFinite(outerLevel) && bar.high >= outerLevel) return 'continue_outer_first';
      if (Number.isFinite(innerLevel) && bar.low <= innerLevel) return 'revert_inner_first';
    } else {
      if (Number.isFinite(outerLevel) && bar.low <= outerLevel) return 'continue_outer_first';
      if (Number.isFinite(innerLevel) && bar.high >= innerLevel) return 'revert_inner_first';
    }
  }
  return 'neither_before_close';
}

function buildEventOutcomes(day, bars) {
  const upperRiskTouch = findFirstTouchIndex(bars, 'upper', day.upperRisk);
  const lowerRiskTouch = findFirstTouchIndex(bars, 'lower', day.lowerRisk);
  const upper2Touch = findFirstTouchIndex(bars, 'upper', day.upper2);
  const lower2Touch = findFirstTouchIndex(bars, 'lower', day.lower2);
  const upper3Touch = findFirstTouchIndex(bars, 'upper', day.upper3);
  const lower3Touch = findFirstTouchIndex(bars, 'lower', day.lower3);

  return {
    upperRisk: resolveBandOutcome(bars, upperRiskTouch, 'upper', day.upper1, day.upper2),
    lowerRisk: resolveBandOutcome(bars, lowerRiskTouch, 'lower', day.lower1, day.lower2),
    upper2: resolveBandOutcome(bars, upper2Touch, 'upper', day.upper1, day.upper3),
    lower2: resolveBandOutcome(bars, lower2Touch, 'lower', day.lower1, day.lower3),
    upper3: resolveBandOutcome(bars, upper3Touch, 'upper', day.upper2, day.upper4),
    lower3: resolveBandOutcome(bars, lower3Touch, 'lower', day.lower2, day.lower4),
  };
}

function aggregateCounts(rows, key) {
  const result = {};
  for (const row of rows) {
    const value = row[key];
    result[value] = (result[value] || 0) + 1;
  }
  return result;
}

function sigmaEstimate(day) {
  const upper = Math.abs(day.upper1 - day.close);
  const lower = Math.abs(day.close - day.lower1);
  return (upper + lower) / 2;
}

function renderOutcomeRate(label, counts = {}) {
  const total = Object.values(counts).reduce((sum, value) => sum + value, 0);
  const revert = counts.revert_inner_first || 0;
  const cont = counts.continue_outer_first || 0;
  const neither = counts.neither_before_close || 0;
  const untouched = counts.not_touched || 0;
  return `| ${label} | ${total} | ${revert} | ${cont} | ${neither} | ${untouched} |`;
}

function renderReport(report) {
  const lines = [];
  lines.push('# Script Inputs March POC');
  lines.push('');
  lines.push('This POC tests the raw SPX `SCRIPT INPUTS` ladder only, without Alma commentary.');
  lines.push('');
  lines.push('## Interpretation Used');
  lines.push('');
  lines.push('- The March SPX ladders behave like a symmetric standardized move grid around the prior close.');
  lines.push('- Empirically, the rows line up very closely with approximately `1 sigma`, `1.35 sigma` (`risk`), `2 sigma`, `3 sigma`, and `4 sigma` bands.');
  lines.push('- That makes the raw block look more like a probability / expected-move ladder than a direct directional signal.');
  lines.push('- So the first fair test is not "buy or sell immediately," but whether the ladder predicts containment, exhaustion, or band-to-band continuation.');
  lines.push('');
  lines.push('## March Sample');
  lines.push('');
  lines.push(`- Script-input days tested: ${report.days}`);
  lines.push(`- Date range: ${report.firstDate} to ${report.lastDate}`);
  lines.push(`- Avg estimated sigma from the 68.2 bands: ${formatNumber(report.avgSigma)} SPX points`);
  lines.push('');
  lines.push('## Containment');
  lines.push('');
  lines.push('| Metric | Count | Rate |');
  lines.push('| --- | ---: | ---: |');
  lines.push(`| Close inside +/-1 sigma | ${report.containment.closeWithin1} | ${formatNumber((report.containment.closeWithin1 / report.days) * 100, 1)}% |`);
  lines.push(`| Close inside +/-2 sigma | ${report.containment.closeWithin2} | ${formatNumber((report.containment.closeWithin2 / report.days) * 100, 1)}% |`);
  lines.push(`| Close inside +/-3 sigma | ${report.containment.closeWithin3} | ${formatNumber((report.containment.closeWithin3 / report.days) * 100, 1)}% |`);
  lines.push(`| Full session range inside +/-1 sigma | ${report.containment.rangeWithin1} | ${formatNumber((report.containment.rangeWithin1 / report.days) * 100, 1)}% |`);
  lines.push(`| Full session range inside +/-2 sigma | ${report.containment.rangeWithin2} | ${formatNumber((report.containment.rangeWithin2 / report.days) * 100, 1)}% |`);
  lines.push(`| Full session range inside +/-3 sigma | ${report.containment.rangeWithin3} | ${formatNumber((report.containment.rangeWithin3 / report.days) * 100, 1)}% |`);
  lines.push(`| Full session range inside +/-4 sigma | ${report.containment.rangeWithin4} | ${formatNumber((report.containment.rangeWithin4 / report.days) * 100, 1)}% |`);
  lines.push('');
  lines.push('## Touch Outcomes');
  lines.push('');
  lines.push('The table below asks whether touching a band led to reversion toward the inner rung first, or continuation to the next outer rung first.');
  lines.push('');
  lines.push('| Event | Total | Revert first | Continue first | Neither by close | Not touched |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: |');
  lines.push(renderOutcomeRate('Upper risk -> upper1 vs upper2', report.eventCounts.upperRisk));
  lines.push(renderOutcomeRate('Lower risk -> lower1 vs lower2', report.eventCounts.lowerRisk));
  lines.push(renderOutcomeRate('Upper 2 sigma -> upper1 vs upper3', report.eventCounts.upper2));
  lines.push(renderOutcomeRate('Lower 2 sigma -> lower1 vs lower3', report.eventCounts.lower2));
  lines.push(renderOutcomeRate('Upper 3 sigma -> upper2 vs upper4', report.eventCounts.upper3));
  lines.push(renderOutcomeRate('Lower 3 sigma -> lower2 vs lower4', report.eventCounts.lower3));
  lines.push('');
  lines.push('## Initial Read');
  lines.push('');
  lines.push('- The raw script ladder looks usable as a regime and level framework, even without commentary.');
  lines.push('- Its first likely use is as a band model: expected body, stress zone, and tail zone.');
  lines.push('- The most practical trade tests are band-touch reactions and confirmed band-break continuations.');
  lines.push('- The ladder alone still does not tell us direction. Direction likely has to come from price path, speed commentary, or another filter.');
  lines.push('');
  lines.push('## Recommended Script-Only Backtest Design');
  lines.push('');
  lines.push('1. Treat the raw ladder as the state space for the day, not the full signal.');
  lines.push('2. Test mean reversion at `risk` and `2 sigma` touches.');
  lines.push('3. Test continuation only after a confirmed break of `2 sigma`, using the next outer rung as target.');
  lines.push('4. Treat `3 sigma` and `4 sigma` as exhaustion / tail zones and test fade setups separately.');
  lines.push('5. Record path features too: first band touched, deepest band reached, and final close band.');
  lines.push('');
  lines.push('## Example: 2026-03-13');
  lines.push('');
  lines.push(`- Close: ${formatNumber(report.example.scriptClose)}`);
  lines.push(`- +/-1 sigma: ${formatNumber(report.example.lower1)} to ${formatNumber(report.example.upper1)}`);
  lines.push(`- Risk levels: ${formatNumber(report.example.lowerRisk)} to ${formatNumber(report.example.upperRisk)}`);
  lines.push(`- +/-2 sigma: ${formatNumber(report.example.lower2)} to ${formatNumber(report.example.upper2)}`);
  lines.push(`- +/-3 sigma: ${formatNumber(report.example.lower3)} to ${formatNumber(report.example.upper3)}`);
  lines.push(`- +/-4 sigma: ${formatNumber(report.example.lower4)} to ${formatNumber(report.example.upper4)}`);
  lines.push('');
  lines.push('That is the cleanest way to backtest the raw script input itself before mixing in Alma commentary.');
  lines.push('');
  lines.push('## Daily Rows');
  lines.push('');
  lines.push('| Date | Close | Sigma | Session high | Session low | Session close | Deepest upper band touched | Deepest lower band touched |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |');
  for (const row of report.dailyRows) {
    lines.push(`| ${row.targetDate} | ${formatNumber(row.scriptClose)} | ${formatNumber(row.sigma)} | ${formatNumber(row.sessionHigh)} | ${formatNumber(row.sessionLow)} | ${formatNumber(row.sessionClose)} | ${row.deepestUpperBand} | ${row.deepestLowerBand} |`);
  }
  return `${lines.join('\n')}\n`;
}

function deepestBandTouched(day, sessionHigh, sessionLow) {
  const upper = sessionHigh >= day.upper4 ? 'upper4'
    : sessionHigh >= day.upper3 ? 'upper3'
    : sessionHigh >= day.upper2 ? 'upper2'
    : sessionHigh >= day.upperRisk ? 'upperRisk'
    : sessionHigh >= day.upper1 ? 'upper1'
    : 'none';
  const lower = sessionLow <= day.lower4 ? 'lower4'
    : sessionLow <= day.lower3 ? 'lower3'
    : sessionLow <= day.lower2 ? 'lower2'
    : sessionLow <= day.lowerRisk ? 'lowerRisk'
    : sessionLow <= day.lower1 ? 'lower1'
    : 'none';
  return { upper, lower };
}

async function main() {
  const days = readMarchScriptInputs();
  const dailyRows = [];

  for (const day of days) {
    const bars = await fetchYahooBarsForDate(day.targetDate);
    if (bars.length === 0) continue;
    const containment = summarizeContainment(day, bars);
    const outcomes = buildEventOutcomes(day, bars);
    const deepest = deepestBandTouched(day, containment.high, containment.low);
    dailyRows.push({
      targetDate: day.targetDate,
      sourceDir: day.sourceDir,
      sourcePath: day.sourcePath,
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
      sessionOpen: containment.open,
      sessionHigh: containment.high,
      sessionLow: containment.low,
      sessionClose: containment.close,
      deepestUpperBand: deepest.upper,
      deepestLowerBand: deepest.lower,
      containment,
      outcomes,
    });
  }

  const containmentTotals = dailyRows.reduce((acc, row) => {
    for (const key of Object.keys(row.containment)) {
      if (typeof row.containment[key] === 'boolean') acc[key] = (acc[key] || 0) + (row.containment[key] ? 1 : 0);
    }
    return acc;
  }, {});

  const eventCounts = {};
  for (const eventName of ['upperRisk', 'lowerRisk', 'upper2', 'lower2', 'upper3', 'lower3']) {
    eventCounts[eventName] = aggregateCounts(dailyRows.map((row) => ({ [eventName]: row.outcomes[eventName] })), eventName);
  }

  const avgSigma = dailyRows.reduce((sum, row) => sum + row.sigma, 0) / Math.max(dailyRows.length, 1);
  const report = {
    generatedAt: new Date().toISOString(),
    days: dailyRows.length,
    firstDate: dailyRows[0]?.targetDate || '',
    lastDate: dailyRows[dailyRows.length - 1]?.targetDate || '',
    avgSigma,
    containment: containmentTotals,
    eventCounts,
    dailyRows,
    example: dailyRows.find((row) => row.targetDate === '2026-03-13') || dailyRows[dailyRows.length - 1] || null,
  };

  ensureDir(OUTPUT_DIR);
  writeJson(path.join(OUTPUT_DIR, 'report.json'), report);
  writeJson(path.join(OUTPUT_DIR, 'daily-rows.json'), dailyRows);
  writeText(path.join(OUTPUT_DIR, 'README.md'), renderReport(report));

  console.log(JSON.stringify({
    outputDir: OUTPUT_DIR,
    days: report.days,
    avgSigma: Number(formatNumber(report.avgSigma)),
    containment: report.containment,
    eventCounts: report.eventCounts,
  }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
