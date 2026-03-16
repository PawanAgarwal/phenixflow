#!/usr/bin/env node

const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { createAlmaLlmExtractor } = require('./alma-llm-extractor');

const ROOT = path.resolve(__dirname, '..');
const WORKSPACE_ROOT = path.resolve(ROOT, '..', '..');
const OUTPUT_SUFFIX = String(process.env.ALMA_PREDICTION_OUTPUT_SUFFIX || '')
  .trim()
  .replace(/[^a-z0-9._-]+/gi, '-')
  .replace(/^-+|-+$/g, '');
const DEFAULT_OUTPUT_ROOT = path.join(ROOT, 'analysis', 'predictions');
const OUTPUT_ROOT = OUTPUT_SUFFIX ? `${DEFAULT_OUTPUT_ROOT}-${OUTPUT_SUFFIX}` : DEFAULT_OUTPUT_ROOT;
const MANIFEST_PATH = path.join(ROOT, 'manifest.json');
const PT_TIME_ZONE = 'America/Los_Angeles';
const ET_TIME_ZONE = 'America/New_York';
const STATE_VERSION = 1;
const CHAT_MODE = ['disabled', 'on', 'chat-only'].includes(String(process.env.ALMA_CHAT_MODE || '').trim().toLowerCase())
  ? String(process.env.ALMA_CHAT_MODE || '').trim().toLowerCase()
  : 'on';
const TARGET_MONTH_FILTERS = String(process.env.ALMA_PREDICTION_TARGET_MONTHS || '')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const THETA_ENV_PATH = path.join(WORKSPACE_ROOT, '.env.mon79.local');
const THETA_STOCK_HISTORY_PATH = '/v3/stock/history/ohlc';
const YAHOO_CHART_BASE_URL = 'https://query1.finance.yahoo.com/v8/finance/chart/';
const INCREMENTAL_ENABLED = /^(1|true|yes)$/i.test(String(process.env.ALMA_PREDICTION_INCREMENTAL || '').trim());
const INCREMENTAL_OVERLAP_HOURS = Math.max(0, Number(process.env.ALMA_PREDICTION_INCREMENTAL_OVERLAP_HOURS || 120) || 120);
const MERGE_CHAT_ONLY_INTO_MAIN = /^(1|true|yes)$/i.test(String(process.env.ALMA_PREDICTION_MERGE_CHAT_ONLY_INTO_MAIN || '').trim());
const YAHOO_HEADERS = {
  'User-Agent': 'Mozilla/5.0',
  Accept: 'application/json,text/plain,*/*',
  'Accept-Language': 'en-US,en;q=0.9',
  Referer: 'https://finance.yahoo.com/',
};

const TITLE_SKIP_PATTERNS = [
  /guide to reading/i,
  /what is volatility/i,
  /education/i,
  /python quant course/i,
  /performance review/i,
  /review of one years performance/i,
  /set free/i,
  /optionsdepth heatmap overview/i,
];

const PREDICTION_KEYWORDS = [
  'expect',
  'expected',
  'likely',
  'most likely',
  'market is betting',
  'betting on',
  'centroid',
  'pivot',
  'target',
  'pin',
  'rangebound',
  'stabiliz',
  'rebound',
  'break out',
  'breakout',
  'break down',
  'breakdown',
  'momentum',
  'support',
  'resistance',
  'vol selling',
  'volatility',
  'upside',
  'downside',
  'bull',
  'bear',
  'confirm',
  'false breakout',
  'trap',
  'magnet',
  'signal',
  'suppression',
  'melt-up',
  'melt up',
  'snapback',
  'bounce',
  'correction',
  'constructive',
  'drift',
  'overextended',
  'whipsaw',
];

const LEVEL_KEYWORDS = [
  'pivot',
  'target',
  'centroid',
  'pin',
  'zero vanna',
  'vanna flip',
  'charm flip',
  'speed flip',
  'margin of error',
  'risk level',
  'support',
  'resistance',
  'confirmation',
  'line in the sand',
  'magnet',
  'sticky',
  'rangebound',
];

const RETROSPECTIVE_PATTERNS = [
  /yesterday/i,
  /last week/i,
  /as weekly post predicted/i,
  /came true/i,
  /it happened/i,
  /i wrote that/i,
  /i said that/i,
  /was expected/i,
];

const WEEKDAY_NAMES = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const WEEKDAY_LABELS = WEEKDAY_NAMES.slice(1, 6);
const MONTH_NAMES = [
  'january',
  'february',
  'march',
  'april',
  'may',
  'june',
  'july',
  'august',
  'september',
  'october',
  'november',
  'december',
];

const INSTRUMENT_ALIASES = {
  SPX: 'SPX',
  ES: 'ES',
  SPY: 'SPY',
  NDX: 'NDX',
  NQ: 'NQ',
  QQQ: 'QQQ',
  VIX: 'VIX',
  IWM: 'IWM',
  RUT: 'RUT',
  NVDA: 'NVDA',
  AMZN: 'AMZN',
  AAPL: 'AAPL',
  GOOGL: 'GOOGL',
  GOOG: 'GOOG',
  META: 'META',
  TSLA: 'TSLA',
  TSM: 'TSM',
  AMD: 'AMD',
  GC: 'GC',
  SI: 'SI',
  TLT: 'TLT',
  ZB: 'ZB',
  CL: 'CL',
};

const INSTRUMENT_FAMILY = {
  SPX: 'SPX_complex',
  ES: 'SPX_complex',
  SPY: 'SPX_complex',
  SPX_or_ES: 'SPX_complex',
  NDX: 'NDX_complex',
  NQ: 'NDX_complex',
  QQQ: 'NDX_complex',
  NDX_or_NQ: 'NDX_complex',
  VIX: 'VIX',
  IWM: 'RUT_complex',
  RUT: 'RUT_complex',
  GC: 'metals',
  SI: 'metals',
  TLT: 'rates',
  ZB: 'rates',
  CL: 'energy',
};

const INSTRUMENT_REFERENCE_MAP = {
  AAPL: { provider: 'theta', lookupSymbol: 'AAPL', quality: 'exact' },
  AMD: { provider: 'theta', lookupSymbol: 'AMD', quality: 'exact' },
  AMZN: { provider: 'theta', lookupSymbol: 'AMZN', quality: 'exact' },
  CL: { provider: 'yahoo', lookupSymbol: 'CL=F', interval: '60m', range: '730d', quality: 'exact' },
  ES: { provider: 'yahoo', lookupSymbol: 'ES=F', interval: '60m', range: '730d', quality: 'exact' },
  GC: { provider: 'yahoo', lookupSymbol: 'GC=F', interval: '60m', range: '730d', quality: 'exact' },
  GOOG: { provider: 'theta', lookupSymbol: 'GOOG', quality: 'exact' },
  GOOGL: { provider: 'theta', lookupSymbol: 'GOOGL', quality: 'exact' },
  IWM: { provider: 'theta', lookupSymbol: 'IWM', quality: 'exact' },
  META: { provider: 'theta', lookupSymbol: 'META', quality: 'exact' },
  NDX: { provider: 'yahoo', lookupSymbol: '^NDX', interval: '60m', range: '730d', quality: 'exact' },
  NQ: { provider: 'yahoo', lookupSymbol: 'NQ=F', interval: '60m', range: '730d', quality: 'exact' },
  NVDA: { provider: 'theta', lookupSymbol: 'NVDA', quality: 'exact' },
  QQQ: { provider: 'theta', lookupSymbol: 'QQQ', quality: 'exact' },
  RUT: { provider: 'yahoo', lookupSymbol: '^RUT', interval: '60m', range: '730d', quality: 'exact' },
  SI: { provider: 'yahoo', lookupSymbol: 'SI=F', interval: '60m', range: '730d', quality: 'exact' },
  SPX: { provider: 'yahoo', lookupSymbol: '^GSPC', interval: '60m', range: '730d', quality: 'exact' },
  SPY: { provider: 'theta', lookupSymbol: 'SPY', quality: 'exact' },
  TLT: { provider: 'theta', lookupSymbol: 'TLT', quality: 'exact' },
  TSLA: { provider: 'theta', lookupSymbol: 'TSLA', quality: 'exact' },
  TSM: { provider: 'theta', lookupSymbol: 'TSM', quality: 'exact' },
  VIX: { provider: 'yahoo', lookupSymbol: '^VIX', interval: '60m', range: '730d', quality: 'exact' },
  ZB: { provider: 'yahoo', lookupSymbol: 'ZB=F', interval: '60m', range: '730d', quality: 'exact' },
};

const AMBIGUOUS_INSTRUMENT_REFERENCE_MAP = {
  NDX_or_NQ: { candidates: ['NDX', 'NQ'], quality: 'family_inferred' },
  SPX_or_ES: { candidates: ['SPX', 'ES'], quality: 'family_inferred' },
};

const INSTRUMENT_REGEX = new RegExp(
  `\\b(${Object.keys(INSTRUMENT_ALIASES).sort((a, b) => b.length - a.length).map((value) => escapeRegExp(value)).join('|')})\\b`,
  'gi',
);
const llmExtractor = createAlmaLlmExtractor({
  workspaceRoot: WORKSPACE_ROOT,
  outputRoot: OUTPUT_ROOT,
  cacheRoot: path.join(DEFAULT_OUTPUT_ROOT, 'llm-cache'),
});

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function readJsonIfExists(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function readText(relPath) {
  return fs.readFileSync(path.join(ROOT, relPath), 'utf8');
}

function stableHash(value) {
  return crypto.createHash('sha1').update(String(value || '')).digest('hex');
}

function dailyDirForRoot(outputRoot = OUTPUT_ROOT) {
  return path.join(outputRoot, 'daily');
}

function jsonDirForRoot(outputRoot = OUTPUT_ROOT) {
  return path.join(outputRoot, 'json');
}

function statePathForRoot(outputRoot = OUTPUT_ROOT) {
  return path.join(outputRoot, 'build-state.json');
}

function normalizeWhitespace(value = '') {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function parseEnvLine(rawLine = '') {
  const line = String(rawLine || '').trim();
  if (!line || line.startsWith('#')) return null;
  const match = line.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
  if (!match) return null;
  let value = match[2].trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith('\'') && value.endsWith('\''))) {
    value = value.slice(1, -1);
  }
  value = value
    .replace(/\\n/g, '\n')
    .replace(/\\"/g, '"')
    .replace(/\\'/g, '\'');
  return { key: match[1], value };
}

function loadEnvFile(filePath, baseEnv = process.env) {
  const env = { ...baseEnv };
  if (!filePath || !fs.existsSync(filePath)) return env;
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const parsed = parseEnvLine(line);
    if (!parsed) continue;
    if (!env[parsed.key]) env[parsed.key] = parsed.value;
  }
  return env;
}

function escapeRegExp(value = '') {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function compactText(text = '', maxLength = 220) {
  const clean = normalizeWhitespace(text).replace(/\n+/g, ' ');
  if (clean.length <= maxLength) return clean;
  return `${clean.slice(0, maxLength - 1).trimEnd()}…`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const PROGRESS_LOGS_ENABLED = /^(1|true|yes)$/i.test(String(process.env.ALMA_PREDICTION_PROGRESS || '').trim());

function logProgress(message = '') {
  if (!PROGRESS_LOGS_ENABLED) return;
  process.stderr.write(`[alma-predictions] ${message}\n`);
}

function escapePipe(text = '') {
  return String(text || '')
    .replace(/\r?\n+/g, ' ')
    .replace(/[ \t]{2,}/g, ' ')
    .replace(/\|/g, '\\|')
    .trim();
}

function formatTimestampPt(timestamp = '') {
  if (!timestamp) return '';
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return String(timestamp);
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: PT_TIME_ZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day} ${byType.hour}:${byType.minute}:${byType.second} PT`;
}

function formatTimestampInTimeZone(timestamp = '', timeZone = PT_TIME_ZONE, suffix = '') {
  if (!timestamp) return '';
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return String(timestamp);
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day} ${byType.hour}:${byType.minute}:${byType.second}${suffix ? ` ${suffix}` : ''}`;
}

function formatDateIsoInTimeZone(timestamp = '', timeZone = ET_TIME_ZONE) {
  if (!timestamp) return '';
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return '';
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

function getTimeZoneOffsetMs(timestampMs, timeZone = ET_TIME_ZONE) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(new Date(timestampMs));
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  const asUtcMs = Date.UTC(
    Number(byType.year),
    Number(byType.month) - 1,
    Number(byType.day),
    Number(byType.hour),
    Number(byType.minute),
    Number(byType.second),
  );
  return asUtcMs - timestampMs;
}

function zonedLocalTimestampToUtcIso(localTimestamp = '', timeZone = ET_TIME_ZONE) {
  const match = String(localTimestamp || '').trim().match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?$/,
  );
  if (!match) return '';
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const millisecond = Number(String(match[7] || '0').padEnd(3, '0'));

  let utcMs = Date.UTC(year, month - 1, day, hour, minute, second, millisecond);
  for (let index = 0; index < 3; index += 1) {
    const offsetMs = getTimeZoneOffsetMs(utcMs, timeZone);
    const nextUtcMs = Date.UTC(year, month - 1, day, hour, minute, second, millisecond) - offsetMs;
    if (nextUtcMs === utcMs) break;
    utcMs = nextUtcMs;
  }
  return new Date(utcMs).toISOString();
}

function formatPriceValue(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return '';
  if (Math.abs(numeric) >= 1000) return numeric.toFixed(2);
  if (Math.abs(numeric) >= 100) return numeric.toFixed(2);
  return numeric.toFixed(4);
}

function countMatches(haystack = '', needles = []) {
  const lower = String(haystack || '').toLowerCase();
  return needles.reduce((count, needle) => count + (lower.includes(String(needle).toLowerCase()) ? 1 : 0), 0);
}

function extractLevels(text = '') {
  const matches = String(text || '').match(/\b\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?\b/g) || [];
  return Array.from(new Set(matches));
}

function extractNumericLevels(text = '') {
  const output = [];
  for (const token of extractLevels(text)) {
    for (const rawPart of String(token).split('/')) {
      const numeric = Number.parseFloat(String(rawPart).trim());
      if (Number.isFinite(numeric)) output.push(numeric);
    }
  }
  return Array.from(new Set(output.map((value) => value.toFixed(6)))).map((value) => Number.parseFloat(value));
}

function uniqueValues(values = []) {
  return Array.from(new Set((values || []).filter(Boolean)));
}

function maxIso(values = []) {
  return (values || [])
    .filter(Boolean)
    .sort((left, right) => String(right).localeCompare(String(left)))[0] || '';
}

function shiftIsoByHours(isoValue = '', hoursDelta = 0) {
  const baseMs = Date.parse(isoValue || '');
  if (!Number.isFinite(baseMs)) return '';
  return new Date(baseMs + (Number(hoursDelta) || 0) * 60 * 60 * 1000).toISOString();
}

function shouldKeepTargetMonth(dateIso = '', monthFilters = TARGET_MONTH_FILTERS) {
  if (!Array.isArray(monthFilters) || monthFilters.length === 0) return true;
  return monthFilters.some((month) => String(dateIso || '').startsWith(month));
}

function weekRangeIntersectsTargetMonths(targetWeek = '', monthFilters = TARGET_MONTH_FILTERS) {
  if (!Array.isArray(monthFilters) || monthFilters.length === 0) return true;
  const match = String(targetWeek || '').match(/^(\d{4}-\d{2}-\d{2}) to (\d{4}-\d{2}-\d{2})$/);
  if (!match) return false;
  const start = new Date(`${match[1]}T00:00:00Z`);
  const end = new Date(`${match[2]}T00:00:00Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return false;
  const cursor = new Date(start);
  while (cursor <= end) {
    const dateIso = cursor.toISOString().slice(0, 10);
    if (shouldKeepTargetMonth(dateIso, monthFilters)) return true;
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return false;
}

function instrumentFamily(instrument = '') {
  return INSTRUMENT_FAMILY[instrument] || '';
}

function findInstrumentMentions(text = '') {
  const mentions = [];
  const value = String(text || '');
  INSTRUMENT_REGEX.lastIndex = 0;
  let match;
  while ((match = INSTRUMENT_REGEX.exec(value)) !== null) {
    const canonical = INSTRUMENT_ALIASES[String(match[1] || '').toUpperCase()];
    if (!canonical) continue;
    mentions.push({
      instrument: canonical,
      start: match.index,
      end: match.index + match[0].length,
      text: match[0],
    });
  }
  return mentions;
}

function inferInstrumentFromLevels(text = '', contextText = '') {
  const predictionLevels = extractLevels(text).map((value) => Number.parseFloat(String(value).split('/')[0]));
  const numericLevels = predictionLevels.filter((value) => Number.isFinite(value));
  const contextMentions = uniqueValues(findInstrumentMentions(contextText).map((mention) => mention.instrument));
  const contextFamilies = uniqueValues(contextMentions.map((instrument) => instrumentFamily(instrument)));
  const looksLikeLevelFragment = /\b(supportive|sticky|pin|centroid|upward correction|downside zone|upside zone|band|range)\b/i.test(text);

  if (contextMentions.length === 1 && (numericLevels.length > 0 || looksLikeLevelFragment)) return contextMentions[0];

  if (contextFamilies.length === 1 && numericLevels.length > 0) {
    const family = contextFamilies[0];
    if (family === 'SPX_complex' && numericLevels.some((value) => value >= 3000 && value <= 9000)) return 'SPX_or_ES';
    if (family === 'NDX_complex' && numericLevels.some((value) => value >= 10000 && value <= 30000)) return 'NDX_or_NQ';
  }

  if (numericLevels.some((value) => value >= 10000 && value <= 30000)) return 'NDX_or_NQ';
  if (numericLevels.some((value) => value >= 3000 && value <= 9000)) return 'SPX_or_ES';
  return '';
}

function splitPredictionTextByInstrument(predictionText = '', contextText = '') {
  const rawText = normalizeWhitespace(predictionText);
  if (!rawText) return [];

  const parts = splitIntoPredictionUnits(rawText)
    .map((part) => normalizeWhitespace(part))
    .filter(Boolean);
  const workingParts = parts.length > 0 ? parts : [rawText];
  const partMentions = workingParts.map((part) => uniqueValues(findInstrumentMentions(part).map((mention) => mention.instrument)));
  const explicitInstruments = uniqueValues(partMentions.flat());

  if (explicitInstruments.length === 0) {
    const inferred = inferInstrumentFromLevels(rawText, contextText);
    return [{
      instrument: inferred,
      instrumentFamily: instrumentFamily(inferred),
      text: rawText,
    }];
  }

  if (explicitInstruments.length === 1) {
    const instrument = explicitInstruments[0];
    return [{
      instrument,
      instrumentFamily: instrumentFamily(instrument),
      text: rawText,
    }];
  }

  const grouped = new Map(explicitInstruments.map((instrument) => [instrument, []]));
  let lastInstrument = '';
  for (let index = 0; index < workingParts.length; index += 1) {
    const part = workingParts[index];
    const instruments = partMentions[index];
    if (instruments.length === 1) {
      grouped.get(instruments[0]).push(part);
      lastInstrument = instruments[0];
      continue;
    }
    if (instruments.length > 1) {
      for (const instrument of instruments) {
        grouped.get(instrument).push(part);
      }
      lastInstrument = instruments[instruments.length - 1];
      continue;
    }
    if (lastInstrument) {
      grouped.get(lastInstrument).push(part);
    }
  }

  const scoped = [];
  for (const instrument of explicitInstruments) {
    const scopedText = normalizeWhitespace(grouped.get(instrument).join(' '));
    if (!scopedText) continue;
    scoped.push({
      instrument,
      instrumentFamily: instrumentFamily(instrument),
      text: scopedText,
    });
  }
  return scoped.length > 0 ? scoped : explicitInstruments.map((instrument) => ({
    instrument,
    instrumentFamily: instrumentFamily(instrument),
    text: rawText,
  }));
}

function splitPredictionUnitByInstrument(unit) {
  const scopedTexts = splitPredictionTextByInstrument(unit.rawPredictionText || unit.predictionText, unit.contextText || '');
  if (scopedTexts.length === 0) {
    return [{
      ...unit,
      instrument: '',
      instrumentFamily: '',
    }];
  }
  return scopedTexts.map((scoped) => {
    const { condition } = parseConditionExpected(scoped.text);
    return {
      ...unit,
      rawPredictionText: normalizeWhitespace(scoped.text),
      predictionText: derivePredictionSnippet(scoped.text, unit.contextText || ''),
      condition,
      expected: deriveActionableExpected(scoped.text, unit.contextText || '', unit.basis),
      instrument: scoped.instrument,
      instrumentFamily: scoped.instrumentFamily,
    };
  });
}

function zoneFromLevels(levels = []) {
  const unique = Array.from(new Set((levels || []).filter(Boolean)));
  if (unique.length === 0) return '';
  if (unique.length === 1) return unique[0];
  return `${unique[0]}-${unique[1]}`;
}

function needsContextualSnippet(text = '') {
  const value = normalizeWhitespace(text);
  if (!value) return false;
  if (/^(if it|if that|otherwise|this zone|that zone|this tells me|so,|but |while |watch )/i.test(value)) return true;
  if (/pivot is at|target is|expect|expected|market is betting|confirmed at|above \d|below \d/i.test(value)) return false;
  return value.length < 90;
}

function derivePredictionSnippet(predictionText = '', contextText = '') {
  const prediction = normalizeWhitespace(predictionText);
  const context = normalizeWhitespace(contextText);
  if (!context || context === prediction) return prediction;
  if (!needsContextualSnippet(prediction)) return prediction;

  const parts = splitIntoPredictionUnits(context)
    .map((part) => normalizeWhitespace(part))
    .filter(Boolean);
  const index = parts.findIndex((part) => part === prediction || part.includes(prediction) || prediction.includes(part));
  if (index < 0) return compactText(prediction, 340);

  const selected = [parts[index]];
  let totalLength = selected[0].length;

  const shouldIncludeNeighbor = (value) => {
    if (!value) return false;
    if (value.length > 220) return false;
    if (hasMarketContext(value)) return true;
    if (scorePredictionText(value) >= 3) return true;
    return /^(if|as long as|unless|watch|otherwise|but|so|while|this zone|that zone|the pivot|the target|low probability|stabilization)/i.test(value);
  };

  const tryAdd = (value, side) => {
    if (!shouldIncludeNeighbor(value)) return;
    if (totalLength + value.length + 1 > 340) return;
    if (side === 'before') {
      selected.unshift(value);
    } else {
      selected.push(value);
    }
    totalLength += value.length + 1;
  };

  tryAdd(parts[index - 1], 'before');
  tryAdd(parts[index + 1], 'after');
  tryAdd(parts[index + 2], 'after');

  return compactText(selected.join(' '), 340);
}

function biasLabel(text = '') {
  const lower = normalizeWhitespace(text).toLowerCase();
  if (!lower) return 'directional';
  if (/vol crush|cool down|vol down|suppressed|suppress|manage iv spikes|compression/.test(lower)) return 'vol_compression';
  if (/vol up|iv move up|amplified|puts catch bid|downside hedges are bid|kurtosis|accelerate the move to the downside|synthetic circuit breaker/.test(lower)) return 'vol_expansion';
  if (/rangebound|sticky|pin|stabiliz|slow down/.test(lower)) return 'range_or_stall';
  if (/bounce|rebound|snapback|upward correction|overextended|bull|supportive|support/.test(lower)) return 'bullish_reversal';
  if (/bear|risk off|downside|reject|reversion|resistance|false breakout|break down|breakdown/.test(lower)) return 'bearish_or_rejection';
  return 'directional';
}

function deriveActionableExpected(predictionText = '', contextText = '', basis = 'commentary') {
  const prediction = normalizeWhitespace(predictionText);
  const context = normalizeWhitespace(contextText);
  const combined = normalizeWhitespace(`${prediction} ${context}`.trim());
  const predictionLower = prediction.toLowerCase();
  const lower = combined.toLowerCase();
  const predictionLevels = extractLevels(prediction);
  const contextLevels = extractLevels(context);
  const levels = predictionLevels.length > 0 ? predictionLevels : contextLevels;
  const primaryZone = zoneFromLevels(levels);

  const watchCorrection = combined.match(/Watch\s+(\d{3,5}(?:\.\d+)?)\s+for\b.*?\b(\d{3,5}(?:\.\d+)?)\s+to confirm upward correction/i);
  const otherwiseTarget = combined.match(/Otherwise,?\s*target is\s+(\d{3,5}(?:\.\d+)?)/i);
  if (watchCorrection) {
    const pivot = watchCorrection[1];
    const confirm = watchCorrection[2];
    const failTarget = otherwiseTarget ? otherwiseTarget[1] : '';
    return `Bullish reversal setup: buy only if ${confirm} confirms after ${pivot} acts as pivot${failTarget ? `; otherwise expect downside continuation toward ${failTarget}` : ''}.`;
  }

  const pivotTarget = prediction.match(/(downside|upside)\s+pivot\s+is\s+at\s+(\d{3,5}(?:\.\d+)?).*?target\s+is\s+(\d{3,5}(?:\.\d+)?)/i)
    || combined.match(/(downside|upside)\s+pivot\s+is\s+at\s+(\d{3,5}(?:\.\d+)?).*?target\s+is\s+(\d{3,5}(?:\.\d+)?)/i);
  if (pivotTarget) {
    const direction = pivotTarget[1].toLowerCase();
    const pivot = pivotTarget[2];
    const target = pivotTarget[3];
    if (direction === 'downside') {
      if (/supportive|sticky|slow down|stabiliz|brake|hold|pin/i.test(lower)) {
        return `Downside support/stall zone: expect selling to slow or stabilize in the ${pivot}-${target} area.`;
      }
      return `Bearish continuation: if the downside pivot at ${pivot} gives way, expect a move toward ${target}.`;
    }
    if (/reversion|reject|rejected|resistance/i.test(lower)) {
      return `Upside reversion zone: expect rejection or mean reversion on a test of ${pivot}-${target}.`;
    }
    if (/sticky|pin/i.test(lower)) {
      return `Upside test zone: expect sticky trade or pinning in the ${pivot}-${target} area.`;
    }
    return `Bullish continuation: above the upside pivot at ${pivot}, expect extension toward ${target}.`;
  }

  const centroidReversion = prediction.match(/centroid(?:\s+is|\s+at| is at)?\s+(\d{3,5}(?:\.\d+)?)/i)
    || combined.match(/centroid(?:\s+is|\s+at| is at)?\s+(\d{3,5}(?:\.\d+)?)/i);
  if (centroidReversion) {
    const centroid = centroidReversion[1];
    if (/reversion|mean reversion|high probability reversion|very strong pivot|pin/i.test(lower)) {
      return `Mean-reversion pivot: expect price to gravitate back toward ${centroid}${/pm|afternoon/i.test(lower) ? ' later in the session' : ''}.`;
    }
    if (/supportive|sticky|magnet/i.test(predictionLower) || /supportive|sticky|magnet/i.test(lower)) {
      return `Magnet / pin zone: expect price to cluster near centroid ${centroid}.`;
    }
    return `Centroid pivot: use ${centroid} as the main intraday balance / reaction level.`;
  }

  const bandHold = combined.match(/(\d{3,5}(?:\.\d+)?\/\d+(?:\.\d+)?)\s+band.*?(slow down|supportive|stabiliz)/i);
  if (bandHold) {
    return `Support/stabilization zone: expect price to slow or base in the ${bandHold[1]} area.`;
  }

  const pinMatch = combined.match(/(?:pin|pinning)\s+(?:near\s+)?(\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?)/i);
  if (pinMatch) {
    return `Pinning expectation: price is likely to gravitate toward ${pinMatch[1]}.`;
  }

  if (/vol crush|cool down|vol down|suppressed|suppress|manage iv spikes|pricing vol crush/i.test(lower)) {
    return `Volatility compression: expect IV/vol to cool down${predictionLevels.length > 0 ? ` around ${primaryZone}` : ''}.`;
  }

  if (/vol up|iv move up|amplified|puts catch bid|downside hedges are bid|kurtosis|accelerate the move to the downside|synthetic circuit breaker/i.test(lower)) {
    return `Volatility expansion: expect stronger IV response${/downside|puts catch bid|accelerate the move to the downside/i.test(lower) ? ' with downside pressure' : ''}${predictionLevels.length > 0 ? ` around ${primaryZone}` : ''}.`;
  }

  if (/overextended|bounce|rebound|snapback|upward correction/i.test(lower)) {
    return `Bullish reversal / bounce setup${predictionLevels.length > 0 ? ` around ${primaryZone}` : ''}.`;
  }

  if (/supportive|slow down|stabiliz|hold/i.test(lower) && predictionLevels.length > 0 && primaryZone) {
    return `Support/stabilization expected near ${primaryZone}.`;
  }

  if (/reject|reversion|revert|resistance/i.test(lower) && predictionLevels.length > 0 && primaryZone) {
    return `Rejection/reversion expected near ${primaryZone}.`;
  }

  if (/rangebound|sticky/i.test(lower) && predictionLevels.length > 0 && primaryZone) {
    return `Rangebound / sticky trade expected near ${primaryZone}.`;
  }

  if (/spot\/vol correlation|risk-on|upside momentum|bull market narrative|longer spot\/vol correlation/i.test(lower)) {
    return `Bullish tape expectation${predictionLevels.length > 0 ? ` above ${primaryZone}` : ''}.`;
  }

  if (/bullish bias|bullishness|trend up|go further up|more bullish/i.test(lower)) {
    return `Bullish bias with upside continuation expected${predictionLevels.length > 0 ? ` above ${primaryZone}` : ''}.`;
  }

  const breakAbove = combined.match(/break above\s+(\d{3,5}(?:\.\d+)?)/i);
  if (breakAbove) {
    return `Upside breakout only above ${breakAbove[1]}.`;
  }

  if (/wait for the next week|wouldn't take any today|won’t take any longer-term options position|high risk \(wide, volatile momentums\)|until the dust settles/i.test(lower)) {
    return 'Stand aside / no-trade setup until volatility settles or the next week begins.';
  }

  if (/accelerate to the downside very fast|confident short|downside of|correction next week|bearish headlines/i.test(lower)) {
    return `Bearish continuation / downside acceleration expected${predictionLevels.length > 0 ? ` below ${primaryZone}` : ''}.`;
  }

  if (/cautious into wednesday|high risk|wide, volatile/i.test(lower)) {
    return 'High-volatility / caution setup rather than a clean directional trend.';
  }

  if (/risk off|downside|bear/i.test(lower) && predictionLevels.length > 0 && primaryZone) {
    return `Bearish pressure expected toward ${primaryZone}.`;
  }

  const noExpectMatch = combined.match(/(?:don['’]t expect|do not expect)\s+(.+?)(?:[.!?]|$)/i);
  if (noExpectMatch) {
    return `Not expected: ${normalizeWhitespace(noExpectMatch[1])}.`;
  }

  const expectMatch = combined.match(/(?:i expect|market expects|they expect|we expect|will likely see|likely gonna see|likely gonna add|my guess is that)\s+(.+?)(?:[.!?]|$)/i);
  if (expectMatch) {
    return `Expected scenario: ${normalizeWhitespace(expectMatch[1])}.`;
  }

  const bias = biasLabel(combined);
  if (basis === 'script_levels' && primaryZone) {
    return `Level-driven ${bias.replace(/_/g, ' ')} call centered on ${primaryZone}.`;
  }
  return `${bias.replace(/_/g, ' ')} call${primaryZone ? ` centered on ${primaryZone}` : ''}.`;
}

function titleLooksEducational(title = '') {
  return TITLE_SKIP_PATTERNS.some((pattern) => pattern.test(title));
}

function isBusinessDay(date) {
  const day = date.getUTCDay();
  return day >= 1 && day <= 5;
}

function toUtcDate(dateIso) {
  return new Date(`${dateIso}T00:00:00Z`);
}

function addDays(dateIso, days) {
  const date = toUtcDate(dateIso);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

function nextBusinessDay(dateIso, offset = 1) {
  const date = toUtcDate(dateIso);
  let remaining = offset;
  while (remaining > 0) {
    date.setUTCDate(date.getUTCDate() + 1);
    if (isBusinessDay(date)) remaining -= 1;
  }
  return date.toISOString().slice(0, 10);
}

function previousBusinessDay(dateIso, offset = 1) {
  const date = toUtcDate(dateIso);
  let remaining = offset;
  while (remaining > 0) {
    date.setUTCDate(date.getUTCDate() - 1);
    if (isBusinessDay(date)) remaining -= 1;
  }
  return date.toISOString().slice(0, 10);
}

function weekStartIso(dateIso) {
  const date = toUtcDate(dateIso);
  const day = date.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  date.setUTCDate(date.getUTCDate() + diff);
  return date.toISOString().slice(0, 10);
}

function weekEndIso(dateIso) {
  return addDays(weekStartIso(dateIso), 4);
}

function weekdayName(dateIso) {
  const date = toUtcDate(dateIso);
  return WEEKDAY_NAMES[date.getUTCDay()];
}

function listBusinessDates(startIso, endIso) {
  if (!startIso || !endIso) return [];
  const output = [];
  const cursor = toUtcDate(startIso);
  const end = toUtcDate(endIso);
  while (cursor <= end) {
    if (isBusinessDay(cursor)) {
      output.push(cursor.toISOString().slice(0, 10));
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return output;
}

function nextWeekRange(dateIso) {
  const currentWeekStart = weekStartIso(dateIso);
  const nextWeekStart = addDays(currentWeekStart, 7);
  return {
    weekStart: nextWeekStart,
    weekEnd: addDays(nextWeekStart, 4),
  };
}

function parseChatMessages(text = '') {
  const messages = [];
  const lines = String(text || '').split(/\r?\n/);
  let current = null;
  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    const header = line.match(/^(\d{4}-\d{2}-\d{2}T[^|]+)\s+\|\s+(.+)$/);
    if (header) {
      if (current) {
        current.text = normalizeWhitespace(current.text);
        messages.push(current);
      }
      current = {
        timestamp: header[1],
        author: header[2],
        text: '',
      };
      continue;
    }
    if (!current) continue;
    if (!line.trim()) continue;
    current.text += `${current.text ? '\n' : ''}${line.trim()}`;
  }
  if (current) {
    current.text = normalizeWhitespace(current.text);
    messages.push(current);
  }
  return messages;
}

function looksLikeRawScriptBlock(text = '') {
  const value = String(text || '');
  return /^===\s*[A-Z]{2,5}\s+closed at/m.test(value)
    || /99\.73%,/m.test(value)
    || /SCRIPT INPUTS/i.test(value);
}

function looksLikeNoise(text = '') {
  const value = normalizeWhitespace(text);
  if (!value) return true;
  if (/^https?:\/\//i.test(value)) return true;
  if (/\.pdf\b/i.test(value)) return true;
  if (/discount|promo code|leaderboard|post is out/i.test(value)) return true;
  if (/python quant course/i.test(value)) return true;
  if (/below i will dissect the positioning/i.test(value)) return true;
  if (/take notes|build a plan|discuss/i.test(value)) return true;
  if (/\bi won't be able to be here\b|\bi'll be here more\b|\btoday as well, i did a lot of different things\b/i.test(value) && !hasMarketContext(value)) return true;
  if (looksLikeRawScriptBlock(value)) return true;
  return false;
}

function looksLikeChatAdminOrSocial(text = '', contextText = '') {
  const combined = normalizeWhitespace(`${text}\n${contextText}`);
  if (!combined) return true;
  if (/\bi(?:'m| am)? mailing with the support\b|\bsupport ticket\b|\bsubstack support\b|\bcontact(?:ing)? support\b/i.test(combined)) return true;
  if (/\bread my\b.+\bpost\b|\bi shared my\b.+\bpost\b|\bi wrote about this in\b.+\bpost\b/i.test(combined)) return true;
  if (/\banti[- ]persian\b|\bhate mail\b|\bmail me\b/i.test(combined) && !hasMarketContext(combined)) return true;
  if (/\btwitter\b|\bx algo\b|\bfollowers\b|\bmy account\b|\bshadowban/i.test(combined) && !hasMarketContext(combined)) return true;
  if (/^\s*(thanks|thank you|gm|good morning|good night|hello|hi)\b/i.test(combined) && !hasMarketContext(combined)) return true;
  return false;
}

function addPredictionBreaks(text = '') {
  let value = normalizeWhitespace(text);
  const markers = [
    'Downside pivot is at',
    'Upside pivot is at',
    'Downside target is',
    'Upside target is',
    'The target is',
    'Target is',
    'The centroid is at',
    'Centroid is at',
    'The key takeaway is',
    'The first local magnet is',
    'Pain is up',
    'Sentiment projection',
    'If spot',
    'If upside',
    'If downside',
    'If both SPX',
    'If both ES',
    'If volatility',
    'As long as',
    'Watch ',
    'Monday:',
    'Tuesday:',
    'Wednesday:',
    'Thursday:',
    'Friday:',
  ];
  for (const marker of markers) {
    const regex = new RegExp(`([^.!?\\n])\\s*(${escapeRegExp(marker)})`, 'g');
    value = value.replace(regex, '$1\n$2');
  }
  value = value.replace(/([0-9])\s+(Very supportive zone|Very sticky zone|This is a significant zone|This is also very sticky|This is a very fragile exposure)/g, '$1\n$2');
  value = value.replace(/([a-z0-9)])\s+(If\s)/g, '$1\n$2');
  value = value.replace(/([a-z0-9)])\s+(As long as\s)/g, '$1\n$2');
  return value;
}

function splitIntoPredictionUnits(text = '') {
  const marked = addPredictionBreaks(text);
  const parts = marked
    .split(/\n+/)
    .map((part) => normalizeWhitespace(part))
    .filter(Boolean)
    .flatMap((part) => part.split(/(?<=[.!?])\s+(?=[A-Z0-9(])/))
    .map((part) => normalizeWhitespace(part))
    .filter(Boolean);
  return parts;
}

function scorePredictionText(text = '') {
  const value = normalizeWhitespace(text);
  if (!value) return 0;
  if (looksLikeNoise(value)) return -10;
  let score = countMatches(value, PREDICTION_KEYWORDS) * 2;
  const numberMatches = value.match(/\b\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?\b/g) || [];
  if (numberMatches.length > 0) score += 1;
  if (/^(if|as long as|unless|watch|below|above)\b/i.test(value)) score += 2;
  if (/next week|coming week|tomorrow|monday:|tuesday:|wednesday:|thursday:|friday:/i.test(value)) score += 2;
  if (RETROSPECTIVE_PATTERNS.some((pattern) => pattern.test(value)) && !/tomorrow|next week|upcoming|into thursday|into friday/i.test(value)) {
    score -= 4;
  }
  if (value.length < 18 || value.length > 500) score -= 2;
  return score;
}

function hasMarketContext(text = '') {
  return /\b(spx|spy|es|qqq|ndx|nq|vix|market|spot|vol|iv|rv|gamma|vanna|charm|speed|centroid|pivot|target|support|resistance|pin)\b/i.test(text);
}

function detectBasis(text = '', sectionKind = 'commentary') {
  const numberMatches = text.match(/\b\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?\b/g) || [];
  const levelScore = countMatches(text, LEVEL_KEYWORDS) + (numberMatches.length >= 2 ? 1 : 0) + (sectionKind === 'script_levels' ? 2 : 0);
  return levelScore >= 2 ? 'script_levels' : 'commentary';
}

function parseConditionExpected(text = '') {
  let value = normalizeWhitespace(text);
  let condition = '';
  let expected = value;

  const weekdayMatch = value.match(/^(Monday|Tuesday|Wednesday|Thursday|Friday):\s*(.+)$/i);
  if (weekdayMatch) {
    value = weekdayMatch[2];
    expected = value;
  }

  let match = value.match(/^((?:If|As long as|Unless)\b.+?),(.+)$/i);
  if (!match) {
    match = value.match(/^((?:Below|Above)\b.+?),(.+)$/i);
  }
  if (match) {
    condition = normalizeWhitespace(match[1]);
    expected = normalizeWhitespace(match[2]);
    return { condition, expected };
  }

  match = value.match(/^(Watch\b.+?for\b.+?)(?:,|\.)(.+)$/i);
  if (match) {
    condition = normalizeWhitespace(match[1]);
    expected = normalizeWhitespace(match[2]);
  }

  return { condition, expected };
}

function buildSources(manifest) {
  const sources = [];
  for (const post of manifest.posts || []) {
    const text = readText(post.files.text);
    sources.push({
      sourceType: 'post',
      id: `post-${post.id}`,
      title: post.title,
      timestamp: post.postDate,
      date: String(post.postDate || '').slice(0, 10),
      sourcePath: post.files.text,
      text,
      sourceHash: stableHash(JSON.stringify(['post', post.id, post.title, post.postDate, text])),
    });
  }
  for (const chat of manifest.chats || []) {
    const timestamp = chat.chatDate || chat.createdAt || chat.updatedAt || '';
    const text = readText(chat.files.text);
    sources.push({
      sourceType: 'chat',
      id: `chat-${chat.id}`,
      title: chat.title || chat.body || chat.folderName,
      timestamp,
      date: String(timestamp).slice(0, 10),
      sourcePath: chat.files.text,
      text,
      sourceHash: stableHash(JSON.stringify(['chat', chat.id, chat.title || chat.body || chat.folderName, timestamp, text])),
    });
  }
  return sources.sort((a, b) => String(a.timestamp).localeCompare(String(b.timestamp)));
}

function parseWeekRange(title = '', postDateIso = '') {
  const lower = String(title || '').toLowerCase();
  const year = Number(String(postDateIso || '').slice(0, 4));
  if (!Number.isFinite(year) || year < 2000) return null;

  const toMonthIndex = (raw) => {
    const key = String(raw || '').toLowerCase().slice(0, 3);
    return MONTH_NAMES.findIndex((name) => name.startsWith(key));
  };

  let startDay;
  let endDay;
  let startMonthIndex;
  let endMonthIndex;

  let match = lower.match(/\((\d{1,2})\s*-\s*(\d{1,2})\/([a-z]+)\)/);
  if (match) {
    startDay = Number(match[1]);
    endDay = Number(match[2]);
    startMonthIndex = toMonthIndex(match[3]);
    endMonthIndex = startMonthIndex;
  }

  if (!match) {
    match = lower.match(/\((\d{1,2})\/([a-z]+)\s*-\s*(\d{1,2})\/([a-z]+)\)/);
    if (match) {
      startDay = Number(match[1]);
      startMonthIndex = toMonthIndex(match[2]);
      endDay = Number(match[3]);
      endMonthIndex = toMonthIndex(match[4]);
    }
  }

  if (!match) {
    match = lower.match(/\(([a-z]+)\s*(\d{1,2})\s*-\s*(\d{1,2})\)/);
    if (match) {
      startMonthIndex = toMonthIndex(match[1]);
      endMonthIndex = startMonthIndex;
      startDay = Number(match[2]);
      endDay = Number(match[3]);
    }
  }

  if (!match) {
    match = lower.match(/\((\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)\s*-\s*(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)\)/);
    if (match) {
      startDay = Number(match[1]);
      startMonthIndex = toMonthIndex(match[2]);
      endDay = Number(match[3]);
      endMonthIndex = toMonthIndex(match[4]);
    }
  }

  if (![startDay, endDay, startMonthIndex, endMonthIndex].every(Number.isFinite)) return null;

  const endYear = endMonthIndex < startMonthIndex ? year + 1 : year;
  const startDate = new Date(Date.UTC(year, startMonthIndex, startDay));
  const endDate = new Date(Date.UTC(endYear, endMonthIndex, endDay));
  while (!isBusinessDay(startDate)) {
    startDate.setUTCDate(startDate.getUTCDate() + 1);
  }
  while (!isBusinessDay(endDate)) {
    endDate.setUTCDate(endDate.getUTCDate() - 1);
  }

  return {
    weekStart: startDate.toISOString().slice(0, 10),
    weekEnd: endDate.toISOString().slice(0, 10),
  };
}

function parseWeeklyDayBlocks(text = '', weekRange = null) {
  if (!weekRange) return [];
  const blocks = [];
  const paragraphs = String(text || '')
    .split(/\n{2,}/)
    .map((paragraph) => addPredictionBreaks(paragraph))
    .map((paragraph) => normalizeWhitespace(paragraph))
    .filter(Boolean);

  for (const paragraph of paragraphs) {
    const weekdayMatches = paragraph.match(/\b(Monday|Tuesday|Wednesday|Thursday|Friday):/gi) || [];
    const startsWithWeekday = /^(Monday|Tuesday|Wednesday|Thursday|Friday):/i.test(paragraph);
    if (!startsWithWeekday && weekdayMatches.length < 2) continue;

    const regex = /(Monday|Tuesday|Wednesday|Thursday|Friday):\s*([\s\S]*?)(?=(?:Monday|Tuesday|Wednesday|Thursday|Friday):|$)/gi;
    let match;
    while ((match = regex.exec(paragraph)) !== null) {
      const dayName = match[1];
      const body = normalizeWhitespace(match[2]);
      if (!body) continue;
      const targetDate = listBusinessDates(weekRange.weekStart, weekRange.weekEnd)
        .find((dateIso) => WEEKDAY_NAMES[toUtcDate(dateIso).getUTCDay()].toLowerCase() === dayName.toLowerCase());
      if (!targetDate) continue;
      blocks.push({
        dayName,
        targetDate,
        text: body,
      });
    }
  }
  return blocks;
}

function parsePostParagraphs(text = '') {
  const rawParagraphs = String(text || '')
    .split(/\n{2,}/)
    .map((paragraph) => normalizeWhitespace(paragraph))
    .filter(Boolean);

  const paragraphs = [];
  let sectionKind = 'commentary';

  for (const paragraph of rawParagraphs) {
    if (!paragraph) continue;
    if (looksLikeNoise(paragraph)) continue;
    if (/Educational posts/i.test(paragraph)) break;
    if (/Weekly post:$/i.test(paragraph)) continue;
    if (/OptionsDepth Heatmap/i.test(paragraph)) {
      sectionKind = 'commentary';
      continue;
    }
    if (/SCRIPT INPUTS/i.test(paragraph) || looksLikeRawScriptBlock(paragraph)) {
      sectionKind = 'script_inputs';
      continue;
    }
    if (/INTRADAY POST Coding|WEEKLY POSITIONING/i.test(paragraph)) {
      sectionKind = 'script_levels';
      continue;
    }
    if (/Let’s look at the daily positioning|Let’s look at, how the market positions itself for the week|Let’s check how the OpEx positioning evolved/i.test(paragraph)) {
      continue;
    }
    if (/Sentiment projection/i.test(paragraph)) {
      const remainder = normalizeWhitespace(paragraph.replace(/Sentiment projection/i, ''));
      if (remainder) {
        paragraphs.push({ sectionKind: 'commentary', text: remainder });
      }
      sectionKind = 'commentary';
      continue;
    }
    paragraphs.push({ sectionKind, text: paragraph });
  }

  return paragraphs;
}

function buildPredictionUnit(base) {
  const predictionSnippet = derivePredictionSnippet(base.predictionText, base.contextText);
  const { condition } = parseConditionExpected(base.predictionText);
  const expected = deriveActionableExpected(base.predictionText, base.contextText, base.basis);
  return {
    predictionText: predictionSnippet,
    rawPredictionText: normalizeWhitespace(base.predictionText),
    contextText: normalizeWhitespace(base.contextText),
    condition,
    expected,
    basis: base.basis,
    sectionKind: base.sectionKind || '',
    origin: base.origin,
    madeAt: base.madeAt,
    sourceType: base.sourceType,
    sourceTitle: base.sourceTitle,
    sourcePath: base.sourcePath,
  };
}

function resolveReferenceDescriptor(instrument = '') {
  return INSTRUMENT_REFERENCE_MAP[instrument] || null;
}

function buildYahooChartUrl(lookupSymbol, interval = '60m', range = '730d') {
  if (!lookupSymbol) return '';
  const url = new URL(`${YAHOO_CHART_BASE_URL}${encodeURIComponent(lookupSymbol)}`);
  url.searchParams.set('interval', interval);
  url.searchParams.set('range', range);
  url.searchParams.set('includePrePost', 'true');
  url.searchParams.set('events', 'div,splits');
  return url.toString();
}

function buildThetaStockHistoryUrl(thetaSymbol, dayIso, env) {
  const baseUrl = String(env.THETADATA_BASE_URL || '').trim().replace(/\/$/, '');
  if (!baseUrl || !thetaSymbol || !dayIso) return '';
  const url = new URL(`${baseUrl}${THETA_STOCK_HISTORY_PATH}`);
  url.searchParams.set('symbol', thetaSymbol);
  url.searchParams.set('date', dayIso.replace(/-/g, ''));
  url.searchParams.set('interval', '1m');
  url.searchParams.set('format', 'json');
  return url.toString();
}

function parseThetaColumnarRows(parsed) {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return [];
  const entries = Object.entries(parsed).filter(([, value]) => Array.isArray(value));
  if (!entries.length) return [];
  const rowCount = entries[0][1].length;
  if (!entries.every(([, values]) => values.length === rowCount)) return [];
  return Array.from({ length: rowCount }, (_unused, index) => {
    const row = {};
    for (const [key, values] of entries) {
      row[key] = values[index];
    }
    return row;
  });
}

async function fetchThetaDayRows(thetaSymbol, dayIso, thetaEnv) {
  const endpoint = buildThetaStockHistoryUrl(thetaSymbol, dayIso, thetaEnv);
  if (!endpoint) return [];

  const timeoutMs = Number(thetaEnv.THETADATA_DOWNLOAD_TIMEOUT_MS || 20000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(endpoint, { signal: controller.signal });
    if (!response.ok) {
      return [];
    }
    const parsed = JSON.parse(await response.text());
    const rows = parseThetaColumnarRows(parsed);
    return rows
      .map((row) => {
        const tsRaw = String(row.timestamp || '').trim();
        const tsUtc = /[zZ]|[+-]\d{2}:\d{2}$/.test(tsRaw)
          ? new Date(tsRaw).toISOString()
          : zonedLocalTimestampToUtcIso(tsRaw, ET_TIME_ZONE);
        const value = Number(row.close);
        if (!tsUtc || !Number.isFinite(value) || value === 0) return null;
        return {
          timestampUtc: tsUtc,
          value,
        };
      })
      .filter(Boolean);
  } catch {
    return [];
  } finally {
    clearTimeout(timer);
  }
}

async function fetchYahooHistoryRows(lookupSymbol, interval = '60m', range = '730d') {
  const endpoint = buildYahooChartUrl(lookupSymbol, interval, range);
  if (!endpoint) return [];

  const timeoutMs = 20000;
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(endpoint, {
        headers: YAHOO_HEADERS,
        signal: controller.signal,
      });
      if (!response.ok) {
        if (response.status === 429 || response.status >= 500) {
          await sleep(1000 * (attempt + 1));
          continue;
        }
        return [];
      }
      const parsed = JSON.parse(await response.text());
      const result = parsed?.chart?.result?.[0];
      const timestamps = Array.isArray(result?.timestamp) ? result.timestamp : [];
      const closes = result?.indicators?.quote?.[0]?.close;
      if (!timestamps.length || !Array.isArray(closes) || closes.length !== timestamps.length) return [];
      return timestamps
        .map((timestamp, index) => {
          const timestampMs = Number(timestamp) * 1000;
          const value = Number(closes[index]);
          if (!Number.isFinite(timestampMs) || !Number.isFinite(value) || value === 0) return null;
          return {
            timestampUtc: new Date(timestampMs).toISOString(),
            value,
          };
        })
        .filter(Boolean);
    } catch {
      if (attempt === 2) return [];
      await sleep(1000 * (attempt + 1));
    } finally {
      clearTimeout(timer);
    }
  }
  return [];
}

function pickClosestReferenceRow(rows = [], targetTimestamp = '') {
  const targetMs = Date.parse(targetTimestamp);
  if (!Number.isFinite(targetMs) || !Array.isArray(rows) || rows.length === 0) return null;
  let best = null;
  for (const row of rows) {
    const rowMs = Date.parse(row.timestampUtc);
    if (!Number.isFinite(rowMs) || !Number.isFinite(Number(row.value))) continue;
    const lagMinutes = Math.round(Math.abs(rowMs - targetMs) / 60000);
    if (!best || lagMinutes < best.lagMinutes || (lagMinutes === best.lagMinutes && rowMs < Date.parse(best.timestampUtc))) {
      best = {
        timestampUtc: row.timestampUtc,
        value: Number(row.value),
        lagMinutes,
      };
    }
  }
  return best;
}

async function loadReferenceRows(descriptor, targetTimestamp, caches = {}) {
  if (!descriptor || !targetTimestamp) return [];

  if (descriptor.provider === 'theta') {
    if (!caches.thetaEnv) {
      caches.thetaEnv = loadEnvFile(THETA_ENV_PATH, process.env);
    }
    const thetaEnv = caches.thetaEnv;
    if (!thetaEnv.THETADATA_BASE_URL) return [];
    if (!caches.dayRows) caches.dayRows = new Map();

    const targetEtDate = formatDateIsoInTimeZone(targetTimestamp, ET_TIME_ZONE);
    const candidateDays = uniqueValues([
      targetEtDate,
      previousBusinessDay(targetEtDate),
      nextBusinessDay(targetEtDate),
    ]).filter(Boolean);

    const rows = [];
    for (const dayIso of candidateDays) {
      const cacheKey = `${descriptor.lookupSymbol}::${dayIso}`;
      let dayRows = caches.dayRows.get(cacheKey);
      if (!dayRows) {
        dayRows = await fetchThetaDayRows(descriptor.lookupSymbol, dayIso, thetaEnv);
        caches.dayRows.set(cacheKey, dayRows);
      }
      if (Array.isArray(dayRows) && dayRows.length > 0) {
        rows.push(...dayRows);
      }
    }
    return rows;
  }

  if (descriptor.provider === 'yahoo') {
    if (!caches.yahooRows) caches.yahooRows = new Map();
    const cacheKey = `${descriptor.lookupSymbol}::${descriptor.interval || '60m'}::${descriptor.range || '730d'}`;
    let historyRows = caches.yahooRows.get(cacheKey);
    if (!historyRows) {
      historyRows = await fetchYahooHistoryRows(
        descriptor.lookupSymbol,
        descriptor.interval || '60m',
        descriptor.range || '730d',
      );
      caches.yahooRows.set(cacheKey, historyRows);
    }
    return Array.isArray(historyRows) ? historyRows : [];
  }

  return [];
}

async function inferChatInstrumentFromRow(row, caches = {}) {
  if (!row || row.sourceType !== 'chat' || row.instrument || !row.madeAt) return null;
  const text = normalizeWhitespace(`${row.prediction || ''} ${row.condition || ''} ${row.contextText || ''}`);
  if (!text) return null;

  if (/\bSPX\b|\bspot\b|\bindex\b/i.test(text) && !/\bES\b|\bfutures?\b/i.test(text)) {
    return {
      instrument: 'SPX',
      descriptor: resolveReferenceDescriptor('SPX'),
      quality: 'chat_inferred',
    };
  }
  if (/\bES\b|\bfutures?\b/i.test(text) && !/\bSPX\b/i.test(text)) {
    return {
      instrument: 'ES',
      descriptor: resolveReferenceDescriptor('ES'),
      quality: 'chat_inferred',
    };
  }
  if (/\bgold\b/i.test(text)) {
    return {
      instrument: 'GC',
      descriptor: resolveReferenceDescriptor('GC'),
      quality: 'chat_inferred',
    };
  }
  if (/\bcrude\b|\boil\b|\bbrent\b/i.test(text)) {
    return {
      instrument: 'CL',
      descriptor: resolveReferenceDescriptor('CL'),
      quality: 'chat_inferred',
    };
  }

  const explicitLevels = extractNumericLevels(text).filter((value) => value >= 1000);
  const shorthandNumbers = explicitLevels.length === 0 ? extractChatShorthandNumbers(text) : [];
  const candidateInstruments = ['SPX', 'ES'];
  let best = null;

  for (const candidateInstrument of candidateInstruments) {
    const descriptor = resolveReferenceDescriptor(candidateInstrument);
    if (!descriptor) continue;
    const rows = await loadReferenceRows(descriptor, row.madeAt, caches);
    const nearest = pickClosestReferenceRow(rows, row.madeAt);
    if (!nearest) continue;
    const candidateLevels = explicitLevels.length > 0
      ? explicitLevels
      : expandChatShorthandAgainstAnchor(nearest.value, shorthandNumbers);
    const score = candidateLevels.length > 0
      ? scoreReferenceDistance(nearest.value, candidateLevels)
      : candidateInstrument === 'SPX' ? 0 : 5;
    if (!best || score < best.score) {
      best = {
        instrument: candidateInstrument,
        descriptor,
        quality: candidateLevels.length > 0 ? 'chat_inferred' : 'chat_inferred_default',
        reference: nearest,
        score,
      };
    }
  }

  if (!best) {
    return {
      instrument: 'SPX',
      descriptor: resolveReferenceDescriptor('SPX'),
      quality: 'chat_inferred_default',
    };
  }

  return best;
}

function scoreReferenceDistance(referenceValue, levels = []) {
  if (!Number.isFinite(referenceValue) || !Array.isArray(levels) || levels.length === 0) {
    return Number.POSITIVE_INFINITY;
  }
  return Math.min(...levels.map((level) => Math.abs(Number(level) - referenceValue)));
}

async function resolveReferenceDescriptorForRow(row, caches = {}) {
  const chatInferred = await inferChatInstrumentFromRow(row, caches);
  if (chatInferred?.descriptor) {
    return {
      instrument: chatInferred.instrument,
      descriptor: chatInferred.descriptor,
      quality: chatInferred.quality,
      reference: chatInferred.reference,
    };
  }

  const exactDescriptor = resolveReferenceDescriptor(row.instrument);
  if (exactDescriptor) {
    return {
      instrument: row.instrument,
      descriptor: exactDescriptor,
      quality: exactDescriptor.quality,
    };
  }

  const ambiguous = AMBIGUOUS_INSTRUMENT_REFERENCE_MAP[row.instrument];
  if (!ambiguous || !row.madeAt) {
    return {
      instrument: row.instrument || '',
      descriptor: null,
      quality: '',
    };
  }

  const levels = extractNumericLevels(`${row.prediction || ''} ${row.condition || ''}`);
  if (levels.length === 0) {
    return {
      instrument: row.instrument,
      descriptor: null,
      quality: ambiguous.quality,
    };
  }

  let bestCandidate = null;
  for (const candidateInstrument of ambiguous.candidates) {
    const descriptor = resolveReferenceDescriptor(candidateInstrument);
    if (!descriptor) continue;
    const rows = await loadReferenceRows(descriptor, row.madeAt, caches);
    const nearest = pickClosestReferenceRow(rows, row.madeAt);
    if (!nearest) continue;
    const levelDistance = scoreReferenceDistance(nearest.value, levels);
    if (
      !bestCandidate
      || levelDistance < bestCandidate.levelDistance
      || (levelDistance === bestCandidate.levelDistance && nearest.lagMinutes < bestCandidate.reference.lagMinutes)
    ) {
      bestCandidate = {
        instrument: candidateInstrument,
        descriptor,
        reference: nearest,
        levelDistance,
      };
    }
  }

  if (!bestCandidate) {
    return {
      instrument: row.instrument,
      descriptor: null,
      quality: ambiguous.quality,
    };
  }

  return {
    instrument: bestCandidate.instrument,
    descriptor: bestCandidate.descriptor,
    quality: ambiguous.quality,
    reference: bestCandidate.reference,
  };
}

async function resolveReferenceForRow(row, caches = {}) {
  const resolved = await resolveReferenceDescriptorForRow(row, caches);
  const descriptor = resolved.descriptor;
  if (!descriptor || !row.madeAt) {
    return {
      instrument: resolved.instrument || row.instrument || '',
      referenceSymbol: '',
      referenceSource: '',
      referenceQuality: resolved.quality || '',
      referenceValue: '',
      referenceAt: '',
      referenceLagMinutes: '',
    };
  }

  const allRows = await loadReferenceRows(descriptor, row.madeAt, caches);
  const best = resolved.reference || pickClosestReferenceRow(allRows, row.madeAt);
  if (!best) {
    return {
      instrument: resolved.instrument || row.instrument || '',
      referenceSymbol: descriptor.lookupSymbol,
      referenceSource: descriptor.provider === 'theta' ? 'thetadata_stock_history_1m' : `yahoo_chart_${descriptor.interval || '60m'}`,
      referenceQuality: resolved.quality || descriptor.quality,
      referenceValue: '',
      referenceAt: '',
      referenceLagMinutes: '',
    };
  }

  return {
    instrument: resolved.instrument || row.instrument || '',
    referenceSymbol: descriptor.lookupSymbol,
    referenceSource: descriptor.provider === 'theta' ? 'thetadata_stock_history_1m' : `yahoo_chart_${descriptor.interval || '60m'}`,
    referenceQuality: resolved.quality || descriptor.quality,
    referenceValue: formatPriceValue(best.value),
    referenceAt: best.timestampUtc,
    referenceLagMinutes: String(best.lagMinutes),
  };
}

async function augmentRowsWithReferenceValues(rows = []) {
  const caches = {
    thetaEnv: null,
    dayRows: new Map(),
    yahooRows: new Map(),
  };
  const output = [];
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const reference = await resolveReferenceForRow(row, caches);
    output.push({
      ...row,
      instrument: reference.instrument || row.instrument || '',
      instrumentFamily: instrumentFamily(reference.instrument || row.instrument || '') || row.instrumentFamily || '',
      referenceSymbol: reference.referenceSymbol,
      referenceSource: reference.referenceSource,
      referenceQuality: reference.referenceQuality,
      referenceValue: reference.referenceValue,
      referenceAt: reference.referenceAt,
      referenceLagMinutes: reference.referenceLagMinutes,
    });
    if ((index + 1) % 250 === 0 || index === rows.length - 1) {
      logProgress(`reference rows ${index + 1}/${rows.length}`);
    }
  }
  return output;
}

function keepRowPerInstrumentScope(row = {}) {
  if (!row || typeof row !== 'object') return false;
  if (row.sourceType === 'chat') return true;
  if (row.sectionKind !== 'script_levels') return true;
  return row.instrument === 'SPX';
}

function extractPredictionUnitsFromParagraph(paragraphText = '', meta = {}) {
  if (!paragraphText || looksLikeNoise(paragraphText)) return [];
  const units = [];
  const paragraphHasMarketContext = hasMarketContext(paragraphText);
  for (const candidate of splitIntoPredictionUnits(paragraphText)) {
    let score = scorePredictionText(candidate);
    if (paragraphHasMarketContext) score += 1;
    if (/^otherwise\b/i.test(candidate)) score += 2;
    if (score < (meta.sourceType === 'chat' ? 3 : 4)) continue;
    if (!hasMarketContext(candidate) && !paragraphHasMarketContext && score < 6) continue;
    const basis = detectBasis(candidate, meta.sectionKind);
    const baseUnit = buildPredictionUnit({
      predictionText: candidate,
      contextText: paragraphText,
      basis,
      sectionKind: meta.sectionKind,
      origin: meta.origin,
      madeAt: meta.madeAt,
      sourceType: meta.sourceType,
      sourceTitle: meta.sourceTitle,
      sourcePath: meta.sourcePath,
    });
    units.push(...splitPredictionUnitByInstrument(baseUnit));
  }
  return units;
}

function resolveNamedWeekday(baseDateIso, weekdayLabel, forceNextWeek = false) {
  const targetWeekday = WEEKDAY_NAMES.findIndex((name) => name.toLowerCase() === String(weekdayLabel || '').toLowerCase());
  if (targetWeekday < 0) return null;
  const base = toUtcDate(baseDateIso);
  const currentWeekday = base.getUTCDay();
  let diff = targetWeekday - currentWeekday;
  if (forceNextWeek) {
    if (diff <= 0) diff += 7;
  } else if (diff < 0) {
    diff += 7;
  }
  const target = new Date(base);
  target.setUTCDate(target.getUTCDate() + diff);
  if (!isBusinessDay(target)) return null;
  return target.toISOString().slice(0, 10);
}

function inferTargetDatesFromText(text = '', madeDate = '') {
  const lower = normalizeWhitespace(text).toLowerCase();
  if (!madeDate) return [];

  if (/\bnext week\b|\bcoming week\b|\bfollowing week\b/.test(lower)) {
    const range = nextWeekRange(madeDate);
    return listBusinessDates(range.weekStart, range.weekEnd);
  }

  if (/\bthis week\b/.test(lower)) {
    return listBusinessDates(madeDate, weekEndIso(madeDate));
  }

  if (/\btomorrow\b/.test(lower)) {
    return [nextBusinessDay(madeDate)];
  }

  const weekdayHits = WEEKDAY_LABELS.filter((label) => new RegExp(`\\b${label.toLowerCase()}\\b`, 'i').test(lower));
  if (weekdayHits.length > 0) {
    const forceNextWeek = /\bnext\s+(monday|tuesday|wednesday|thursday|friday)\b/i.test(lower);
    return weekdayHits
      .map((label) => resolveNamedWeekday(madeDate, label, forceNextWeek))
      .filter(Boolean);
  }

  return [madeDate];
}

function expandUnitToDailyRows(unit, targetDates = []) {
  return targetDates.map((targetDate) => ({
    id: `${targetDate}-${unit.madeAt}-${unit.sourcePath}-${unit.predictionText}`.toLowerCase(),
    targetDate,
    madeAt: unit.madeAt,
    sourceType: unit.sourceType,
    sectionKind: unit.sectionKind || '',
    sourceTitle: unit.sourceTitle,
    sourcePath: unit.sourcePath,
    instrument: unit.instrument || '',
    instrumentFamily: unit.instrumentFamily || '',
    referenceSymbol: '',
    referenceSource: '',
    referenceQuality: '',
    referenceValue: '',
    referenceAt: '',
    referenceLagMinutes: '',
    origin: unit.origin,
    basis: unit.basis,
    prediction: unit.predictionText,
    condition: unit.condition,
    expected: unit.expected,
    proxyActualValue: '',
    aligned: '',
  }));
}

function summarizeTargetDates(targetDates = []) {
  const uniqueDates = Array.from(new Set((targetDates || []).filter(Boolean))).sort();
  if (uniqueDates.length === 0) {
    return {
      targetWeek: '',
      targetScope: '',
      targetDates: [],
    };
  }

  const firstWeekStart = weekStartIso(uniqueDates[0]);
  const sameWeek = uniqueDates.every((dateIso) => weekStartIso(dateIso) === firstWeekStart);
  const targetWeek = sameWeek
    ? `${firstWeekStart} to ${weekEndIso(firstWeekStart)}`
    : `${uniqueDates[0]} to ${uniqueDates[uniqueDates.length - 1]}`;

  const businessWeekDates = sameWeek ? listBusinessDates(firstWeekStart, weekEndIso(firstWeekStart)) : [];
  let targetScope = '';
  if (sameWeek && uniqueDates.length === businessWeekDates.length && uniqueDates.every((dateIso, index) => dateIso === businessWeekDates[index])) {
    targetScope = 'whole_week';
  } else if (uniqueDates.length === 1) {
    targetScope = `${weekdayName(uniqueDates[0])} (${uniqueDates[0]})`;
  } else {
    targetScope = uniqueDates.map((dateIso) => `${weekdayName(dateIso)} (${dateIso})`).join(', ');
  }

  return {
    targetWeek,
    targetScope,
    targetDates: uniqueDates,
  };
}

function buildWeeklyRow(unit, targetDates = [], originOverride = null) {
  const summary = summarizeTargetDates(targetDates);
  return {
    targetWeek: summary.targetWeek,
    targetScope: summary.targetScope,
    targetDates: summary.targetDates,
    madeAt: unit.madeAt,
    sourceType: unit.sourceType,
    sectionKind: unit.sectionKind || '',
    sourceTitle: unit.sourceTitle,
    sourcePath: unit.sourcePath,
    instrument: unit.instrument || '',
    instrumentFamily: unit.instrumentFamily || '',
    referenceSymbol: '',
    referenceSource: '',
    referenceQuality: '',
    referenceValue: '',
    referenceAt: '',
    referenceLagMinutes: '',
    origin: originOverride || unit.origin,
    basis: unit.basis,
    prediction: unit.predictionText,
    condition: unit.condition,
    expected: unit.expected,
  };
}

function isChatLlmCandidate(text = '') {
  const value = normalizeWhitespace(text);
  if (!value || looksLikeNoise(value) || looksLikeChatAdminOrSocial(value)) return false;
  const hasLevels = /\b\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?\b/.test(value);
  return hasMarketContext(value) && (scorePredictionText(value) >= 2 || hasLevels);
}

function buildChatHeuristicCandidates(messages = [], source = {}) {
  const candidates = [];
  for (const message of messages) {
    if (!isChatLlmCandidate(message.text)) continue;
    const heuristicUnits = extractPredictionUnitsFromParagraph(message.text, {
      sectionKind: 'commentary',
      sourceType: 'chat',
      sourceTitle: source.title,
      sourcePath: source.sourcePath,
      madeAt: message.timestamp,
      origin: 'chat_prediction',
    });
    const candidateText = uniqueValues(heuristicUnits.map((unit) => normalizeWhitespace(unit.predictionText)).filter(Boolean)).join('\n');
    if (!candidateText) continue;
    candidates.push({
      ...message,
      text: candidateText,
    });
  }
  return candidates;
}

function keepMeaningfulChatUnit(unit) {
  const prediction = normalizeWhitespace(unit?.predictionText || unit?.rawPredictionText || '');
  const context = normalizeWhitespace(unit?.contextText || '');
  if (!prediction) return false;
  if (looksLikeChatAdminOrSocial(prediction, context)) return false;
  if (!hasMarketContext(`${prediction}\n${context}`) && !/\b\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?\b/.test(prediction)) return false;
  return scorePredictionText(prediction) >= 1 || /\b\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?\b/.test(prediction);
}

function extractChatShorthandNumbers(text = '') {
  const matches = [];
  const value = String(text || '');
  const regex = /(^|[^\d:])(\d{2})(?:\s*(?:\/|-|to)\s*(\d{2}))?(?=$|[^\d])/gi;
  let match;
  while ((match = regex.exec(value)) !== null) {
    const start = match.index + String(match[1] || '').length;
    const preceding = value.slice(Math.max(0, start - 2), start);
    const trailing = value.slice(regex.lastIndex, regex.lastIndex + 4);
    if (preceding.includes(':') || /^:/.test(trailing) || /^(am|pm)\b/i.test(trailing)) continue;
    const first = Number(match[2]);
    const second = match[3] ? Number(match[3]) : NaN;
    if (Number.isFinite(first)) matches.push(first);
    if (Number.isFinite(second)) matches.push(second);
  }
  return Array.from(new Set(matches));
}

function expandChatShorthandAgainstAnchor(anchorValue, shorthandNumbers = []) {
  const anchor = Number(anchorValue);
  if (!Number.isFinite(anchor) || !Array.isArray(shorthandNumbers) || shorthandNumbers.length === 0) return [];
  const anchorHundreds = Math.floor(anchor / 100) * 100;
  const expanded = [];
  for (const number of shorthandNumbers) {
    for (const base of [anchorHundreds - 100, anchorHundreds, anchorHundreds + 100]) {
      const candidate = base + number;
      if (candidate > 0) expanded.push(candidate);
    }
  }
  return Array.from(new Set(expanded))
    .sort((left, right) => Math.abs(left - anchor) - Math.abs(right - anchor))
    .slice(0, shorthandNumbers.length || 1);
}

async function buildFromWeeklySource(source) {
  const dailyRows = [];
  const weeklyRows = [];
  const weekRange = parseWeekRange(source.title, source.timestamp);
  if (!weekRange) return { dailyRows, weeklyRows };

  const daySpecificBlocks = parseWeeklyDayBlocks(source.text, weekRange);
  for (const block of daySpecificBlocks) {
    let units = [];
    if (llmExtractor.enabled) {
      units = await llmExtractor.extractTextBlockCommentaryPredictions(
        source,
        block.text,
        source.timestamp,
        'weekly_day_specific',
      );
    }
    if (units.length === 0) {
      units = extractPredictionUnitsFromParagraph(block.text, {
        sectionKind: 'commentary',
        sourceType: source.sourceType,
        sourceTitle: source.title,
        sourcePath: source.sourcePath,
        madeAt: source.timestamp,
        origin: 'weekly_day_specific',
      });
    }
    for (const unit of units) {
      dailyRows.push(...expandUnitToDailyRows(unit, [block.targetDate]));
      weeklyRows.push(buildWeeklyRow(unit, [block.targetDate], 'weekly_day_specific'));
    }
  }

  const weekDates = listBusinessDates(weekRange.weekStart, weekRange.weekEnd);
  const paragraphs = parsePostParagraphs(source.text)
    .filter((paragraph) => !/(Monday|Tuesday|Wednesday|Thursday|Friday):/i.test(paragraph.text));
  const commentaryParagraphs = paragraphs
    .filter((paragraph) => paragraph.sectionKind === 'commentary')
    .map((paragraph) => paragraph.text);

  let commentaryUnits = [];
  if (llmExtractor.enabled) {
    commentaryUnits = await llmExtractor.extractPostCommentaryPredictions(source, commentaryParagraphs);
    commentaryUnits = commentaryUnits.map((unit) => ({ ...unit, origin: 'weekly_generic_carry' }));
  }
  if (commentaryUnits.length === 0) {
    for (const paragraph of paragraphs.filter((value) => value.sectionKind === 'commentary')) {
      commentaryUnits.push(...extractPredictionUnitsFromParagraph(paragraph.text, {
        sectionKind: paragraph.sectionKind,
        sourceType: source.sourceType,
        sourceTitle: source.title,
        sourcePath: source.sourcePath,
        madeAt: source.timestamp,
        origin: 'weekly_generic_carry',
      }));
    }
  }

  const scriptUnits = [];
  for (const paragraph of paragraphs.filter((value) => value.sectionKind !== 'commentary')) {
    scriptUnits.push(...extractPredictionUnitsFromParagraph(paragraph.text, {
      sectionKind: paragraph.sectionKind,
      sourceType: source.sourceType,
      sourceTitle: source.title,
      sourcePath: source.sourcePath,
      madeAt: source.timestamp,
      origin: 'weekly_generic_carry',
    }));
  }

  const heatmapUnits = llmExtractor.enabled
    ? await llmExtractor.extractHeatmapPredictions(source)
    : [];

  for (const unit of commentaryUnits.concat(scriptUnits, heatmapUnits)) {
    dailyRows.push(...expandUnitToDailyRows(unit, weekDates));
    weeklyRows.push(buildWeeklyRow(unit, weekDates, unit.origin || 'weekly_generic_carry'));
  }

  return { dailyRows, weeklyRows };
}

async function buildFromPostSource(source) {
  const dailyRows = [];
  const weeklyRows = [];

  const isWeekly = /weekly post/i.test(source.title);
  if (isWeekly) return buildFromWeeklySource(source);

  const paragraphs = parsePostParagraphs(source.text);
  const commentaryParagraphs = paragraphs
    .filter((paragraph) => paragraph.sectionKind === 'commentary')
    .map((paragraph) => paragraph.text);

  let commentaryUnits = [];
  if (llmExtractor.enabled) {
    commentaryUnits = await llmExtractor.extractPostCommentaryPredictions(source, commentaryParagraphs);
    commentaryUnits = commentaryUnits.map((unit) => ({ ...unit, origin: 'post_prediction' }));
  }
  if (commentaryUnits.length === 0) {
    for (const paragraph of paragraphs.filter((value) => value.sectionKind === 'commentary')) {
      commentaryUnits.push(...extractPredictionUnitsFromParagraph(paragraph.text, {
        sectionKind: paragraph.sectionKind,
        sourceType: source.sourceType,
        sourceTitle: source.title,
        sourcePath: source.sourcePath,
        madeAt: source.timestamp,
        origin: 'post_prediction',
      }));
    }
  }

  const scriptUnits = [];
  for (const paragraph of paragraphs.filter((value) => value.sectionKind !== 'commentary')) {
    scriptUnits.push(...extractPredictionUnitsFromParagraph(paragraph.text, {
      sectionKind: paragraph.sectionKind,
      sourceType: source.sourceType,
      sourceTitle: source.title,
      sourcePath: source.sourcePath,
      madeAt: source.timestamp,
      origin: 'post_prediction',
    }));
  }

  const heatmapUnits = llmExtractor.enabled
    ? await llmExtractor.extractHeatmapPredictions(source)
    : [];

  for (const unit of commentaryUnits.concat(scriptUnits, heatmapUnits)) {
    const targetDates = inferTargetDatesFromText(unit.predictionText, source.date);
    const origin = targetDates.length > 1 || targetDates[0] !== source.date
      ? 'post_forward_prediction'
      : 'post_same_day_prediction';
    const adjustedUnit = { ...unit, origin };
    dailyRows.push(...expandUnitToDailyRows(adjustedUnit, targetDates));
    if (targetDates.length > 1 || (targetDates.length === 1 && targetDates[0] !== source.date)) {
      weeklyRows.push(buildWeeklyRow(adjustedUnit, targetDates, 'post_week_duration_prediction'));
    }
  }

  return { dailyRows, weeklyRows };
}

async function buildFromChatSource(source) {
  const dailyRows = [];
  const weeklyRows = [];
  const messages = parseChatMessages(source.text)
    .filter((message) => /alma/i.test(message.author))
    .filter((message) => !looksLikeNoise(message.text))
    .filter((message) => !looksLikeChatAdminOrSocial(message.text));
  const heuristicCandidateMessages = buildChatHeuristicCandidates(messages, source);

  let units = [];
  if (llmExtractor.enabled && heuristicCandidateMessages.length > 0) {
    units = await llmExtractor.extractChatPredictions(source, heuristicCandidateMessages);
  }
  if (units.length === 0) {
    for (const message of heuristicCandidateMessages) {
      units.push(...extractPredictionUnitsFromParagraph(message.text, {
        sectionKind: 'commentary',
        sourceType: source.sourceType,
        sourceTitle: source.title,
        sourcePath: source.sourcePath,
        madeAt: message.timestamp,
        origin: 'chat_prediction',
      }));
    }
  }
  units = units.filter(keepMeaningfulChatUnit);

  for (const unit of units) {
    const madeDate = String(unit.madeAt || '').slice(0, 10);
    const targetDates = inferTargetDatesFromText(unit.predictionText, madeDate);
    const origin = targetDates.length > 1 || targetDates[0] !== madeDate
      ? 'chat_forward_prediction'
      : 'chat_same_day_prediction';
    const adjustedUnit = { ...unit, origin };
    dailyRows.push(...expandUnitToDailyRows(adjustedUnit, targetDates));
    if (targetDates.length > 1 || (targetDates.length === 1 && targetDates[0] !== madeDate)) {
      weeklyRows.push(buildWeeklyRow(adjustedUnit, targetDates, 'chat_week_duration_prediction'));
    }
  }

  return { dailyRows, weeklyRows };
}

function dedupeRows(rows = []) {
  const seen = new Set();
  const output = [];
  for (const row of rows) {
    const key = [
      row.targetDate || row.targetSpan || '',
      row.targetWeek || '',
      row.targetScope || '',
      row.madeAt || '',
      row.sourcePath || '',
      row.origin || '',
      row.basis || '',
      row.instrument || '',
      row.instrumentFamily || '',
      row.referenceSymbol || '',
      row.referenceValue || '',
      row.referenceAt || '',
      row.prediction || '',
    ].join('::').toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    output.push(row);
  }
  return output;
}

function compareRows(a, b) {
  return String(a.targetDate || a.targetWeek || a.targetSpan || '').localeCompare(String(b.targetDate || b.targetWeek || b.targetSpan || ''))
    || String(a.targetScope || '').localeCompare(String(b.targetScope || ''))
    || String(a.madeAt || '').localeCompare(String(b.madeAt || ''))
    || String(a.sourceTitle || '').localeCompare(String(b.sourceTitle || ''))
    || String(a.prediction || '').localeCompare(String(b.prediction || ''));
}

function groupDailyByMonth(rows = []) {
  const grouped = new Map();
  for (const row of rows.sort(compareRows)) {
    const monthKey = String(row.targetDate).slice(0, 7);
    if (!grouped.has(monthKey)) grouped.set(monthKey, []);
    grouped.get(monthKey).push(row);
  }
  return grouped;
}

function renderDailyMonthTable(monthKey, rows = []) {
  const lines = [];
  lines.push(`# Alma Daily Prediction Capture ${monthKey}`);
  lines.push('');
  lines.push('One row = one captured prediction aimed at a specific target date. Rows are sorted by target date, then by when the prediction was made.');
  lines.push('');
  lines.push('| Target date | Made at (PT) | Instrument | Market family | Ref symbol | Ref at (PT) | Ref value | Ref quality | Source | Origin | Basis | Prediction snippet | Condition / trigger | Expected result | Proxy actual value | Aligned? |');
  lines.push('| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |');
  for (const row of rows) {
    const sourceLink = `[${escapePipe(row.sourceTitle)}](${path.join(ROOT, row.sourcePath)})`;
    lines.push(
      `| ${row.targetDate} | ${formatTimestampPt(row.madeAt)} | ${row.instrument || ''} | ${row.instrumentFamily || ''} | ${row.referenceSymbol || ''} | ${row.referenceAt ? formatTimestampPt(row.referenceAt) : ''} | ${row.referenceValue || ''} | ${row.referenceQuality || ''} | ${sourceLink} | ${row.origin} | ${row.basis} | ${escapePipe(compactText(row.prediction, 320))} | ${escapePipe(compactText(row.condition, 170))} | ${escapePipe(compactText(row.expected, 220))} | ${row.proxyActualValue} | ${row.aligned} |`,
    );
  }
  lines.push('');
  return lines.join('\n');
}

function renderWeeklyTable(rows = []) {
  const lines = [];
  lines.push('# Alma Weekly Prediction Capture');
  lines.push('');
  lines.push('One row = one weekly-duration prediction. These can come from weekly posts, or from posts/chats that clearly point at the coming week or a future weekday inside that week.');
  lines.push('');
  lines.push('| Target week | Target scope | Made at (PT) | Instrument | Market family | Ref symbol | Ref at (PT) | Ref value | Ref quality | Source | Source type | Origin | Basis | Prediction snippet | Condition / trigger | Expected result |');
  lines.push('| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |');
  for (const row of rows.sort(compareRows)) {
    const sourceLink = `[${escapePipe(row.sourceTitle)}](${path.join(ROOT, row.sourcePath)})`;
    lines.push(
      `| ${row.targetWeek} | ${escapePipe(row.targetScope || '')} | ${formatTimestampPt(row.madeAt)} | ${row.instrument || ''} | ${row.instrumentFamily || ''} | ${row.referenceSymbol || ''} | ${row.referenceAt ? formatTimestampPt(row.referenceAt) : ''} | ${row.referenceValue || ''} | ${row.referenceQuality || ''} | ${sourceLink} | ${row.sourceType} | ${row.origin} | ${row.basis} | ${escapePipe(compactText(row.prediction, 320))} | ${escapePipe(compactText(row.condition, 170))} | ${escapePipe(compactText(row.expected, 220))} |`,
    );
  }
  lines.push('');
  return lines.join('\n');
}

function renderMethodology(dailyRows = [], weeklyRows = []) {
  const heatmapDailyCount = dailyRows.filter((row) => row.basis === 'optiondepth_heatmap').length;
  return `# Methodology

## Goal

This pass is focused on capturing Alma predictions cleanly before evaluating them.

## What changed

- One row now means one prediction, not one source item.
- The main daily ledgers are keyed by the **target date** of the prediction.
- Each row also records **when the prediction was made** in PT.
- Each row now also carries an instrument tag when the text supports it.
- Each row now also tries to capture the closest available underlying value at the prediction timestamp.
- Script-section rows are intentionally narrowed to SPX only; ES, NQ, VIX, and stock-specific script levels are excluded.
- Commentary extraction uses cached OpenAI structured extraction first. Chat extraction is stricter: Alma-authored messages are heuristic-filtered first, then only those candidate snippets are passed to the LLM, with the deterministic parser as fallback when LLM extraction returns nothing.
- Runtime chat mode is configurable with \`ALMA_CHAT_MODE=disabled|on|chat-only\`.
- Incremental rebuilds are configurable with \`ALMA_PREDICTION_INCREMENTAL=1\`; they reuse stored source hashes and rebuild only new or changed Alma sources plus the recent overlap window.
- A \`chat-only\` rebuild can optionally replace stale chat-derived rows in the main ledger with \`ALMA_PREDICTION_MERGE_CHAT_ONLY_INTO_MAIN=1\`.
- OptionsDepth heatmap images are also extracted and converted into separate \`optiondepth_heatmap\` prediction rows when a post includes that image block.
- Commentary-driven calls, level/script-driven calls, and heatmap-driven calls are separated with a \`basis\` field.
- \`Expected result\` is now a normalized action statement so we can backtest it more deterministically later.
- \`Proxy actual value\` and \`Aligned?\` are intentionally left blank for now.
- The weekly ledger now captures all week-duration predictions, not just weekly-post rows.

## Daily capture rules

- Weekly notes can generate many daily rows:
  - \`weekly_day_specific\` when Alma gives explicit day lines like \`Tuesday:\` or \`Friday:\`
  - \`weekly_generic_carry\` when a broader weekly comment is applied to every trading day of that week
- Daily posts generate one row per prediction sentence or clause that looks forecast-like.
- For commentary, the extractor asks the model for one row per distinct forward-looking claim, including pivot/centroid/reversion style commentary that is easy to miss with simple regex parsing.
- For chats, Alma-authored messages are reduced by heuristics first and only those candidate prediction snippets are sent to the model.
- When a post contains an OptionsDepth heatmap, the image plus nearby text are processed as a separate prediction source.
- Chats use Alma-authored messages only, and can generate:
  - \`chat_same_day_prediction\`
  - \`chat_forward_prediction\`

## Weekly capture rules

- Weekly rows are created from:
  - weekly posts with generic week-long calls
  - weekly posts with day-specific calls like \`Thursday:\`
  - non-weekly posts that clearly point to the coming week or a future weekday
  - chats where Alma makes a forward-looking weekly or weekday call
- Each weekly row records:
  - \`targetWeek\`
  - \`targetScope\` such as \`whole_week\` or \`Thursday (2026-02-19)\`
  - \`madeAt\`
  - \`origin\`
  - \`basis\`

## Basis rules

- \`commentary\`: broader narrative forecasts, sentiment calls, regime calls, and directional/volatility commentary
- \`script_levels\`: pivot/target/centroid/pin/confirmation style calls that are clearly tied to her level framework
- \`optiondepth_heatmap\`: image-derived support, pin, reversion, and rejection zones extracted from the heatmap plus nearby context
- Chat rows are kept even when they mention level-style calls, because they reflect Alma's direct chat commentary rather than the standalone script tables
- \`instrument\`: exact symbol when explicit in the text; family labels such as \`SPX_or_ES\` are resolved down to one market when the prediction levels fit one candidate better than the other
- \`instrumentFamily\`: broader grouping such as \`SPX_complex\` or \`NDX_complex\`
- \`referenceSymbol\`: the actual ThetaData or Yahoo Finance symbol used to fetch the nearest available price, such as \`SPY\`, \`^GSPC\`, or \`ES=F\`
- \`referenceValue\` / \`referenceAt\`: the closest available market value near the prediction-made timestamp
- \`referenceQuality\`: \`exact\` for direct symbol lookups and \`family_inferred\` when an ambiguous family tag was resolved using the prediction levels themselves
- \`expected\`: a heuristic action summary derived from the quote plus nearby paragraph context, intended to be easier to evaluate than the raw quote alone

## Current output size

- Daily prediction rows: ${dailyRows.length}
- Weekly prediction rows: ${weeklyRows.length}
- Daily heatmap rows: ${heatmapDailyCount}

## Next step

The next pass should focus on evaluation logic only after we review whether the captured predictions themselves look right.
`;
}

function renderIndex(dailyRows = [], weeklyRows = [], groupedDaily = new Map(), outputRoot = OUTPUT_ROOT) {
  const commentaryCount = dailyRows.filter((row) => row.basis === 'commentary').length;
  const scriptCount = dailyRows.filter((row) => row.basis === 'script_levels').length;
  const heatmapCount = dailyRows.filter((row) => row.basis === 'optiondepth_heatmap').length;
  const dailyDir = dailyDirForRoot(outputRoot);
  const lines = [];
  lines.push('# Alma Prediction Capture');
  lines.push('');
  lines.push('This pass is capture-first: one row per prediction, with timestamps and basis, and no evaluation filled in yet.');
  lines.push('Commentary and chats are extracted with cached OpenAI structured output first, script tables stay rule-based, and OptionsDepth heatmaps are captured as a separate image-derived basis.');
  lines.push('');
  lines.push('## What is here');
  lines.push('');
  lines.push(`- [weekly-predictions.md](${path.join(outputRoot, 'weekly-predictions.md')}) is the weekly prediction capture view.`);
  lines.push(`- [methodology.md](${path.join(outputRoot, 'methodology.md')}) explains the capture rules.`);
  lines.push(`- [build-state.json](${statePathForRoot(outputRoot)}) records the latest incremental checkpoint and source hashes.`);
  for (const monthKey of groupedDaily.keys()) {
    lines.push(`- [daily/${monthKey}.md](${path.join(dailyDir, `${monthKey}.md`)}) is the daily prediction table for ${monthKey}.`);
  }
  lines.push('');
  lines.push('## Snapshot');
  lines.push('');
  lines.push(`- Daily prediction rows: ${dailyRows.length}`);
  lines.push(`- Weekly prediction rows: ${weeklyRows.length}`);
  lines.push(`- Commentary rows: ${commentaryCount}`);
  lines.push(`- Script-level rows: ${scriptCount}`);
  lines.push(`- Heatmap rows: ${heatmapCount}`);
  lines.push('');
  lines.push('## Notes');
  lines.push('');
  lines.push('- Multiple predictions on the same target date are kept as separate rows.');
  lines.push('- Rows are ordered by target date, then prediction-made time, earliest first.');
  lines.push('- Displayed prediction timestamps are shown in PT.');
  lines.push('- Instrument tagging now tries to keep one market per row when the text supports it.');
  lines.push('- When an instrument is tagged, the ledger also captures the closest available ThetaData or Yahoo reference value and the actual symbol used for that lookup.');
  lines.push('- Script-table rows are intentionally narrowed to SPX only; Alma commentary rows and Alma chat rows still keep all markets she explicitly talks about.');
  lines.push('- Weekly rows now include weekly-post predictions and week-duration predictions coming from posts or chats.');
  lines.push('- Weekly and vague forward comments are expanded into daily rows for the relevant future days.');
  lines.push('- Incremental builds reuse source hashes and only rebuild new or changed Alma sources plus the recent overlap window.');
  lines.push('- \`Proxy actual value\` and \`Aligned?\` are left blank on purpose for the next evaluation pass.');
  lines.push('');
  return `${lines.join('\n')}\n`;
}

function listExistingMonthFiles(dailyDir) {
  if (!fs.existsSync(dailyDir)) return [];
  return fs.readdirSync(dailyDir).filter((name) => name.endsWith('.md'));
}

function writeOutputs(dailyRows, weeklyRows, outputRoot = OUTPUT_ROOT) {
  const dailyDir = dailyDirForRoot(outputRoot);
  const jsonDir = jsonDirForRoot(outputRoot);
  ensureDir(outputRoot);
  ensureDir(dailyDir);
  ensureDir(jsonDir);

  const groupedDaily = groupDailyByMonth(dailyRows);

  fs.writeFileSync(path.join(outputRoot, 'README.md'), renderIndex(dailyRows, weeklyRows, groupedDaily, outputRoot));
  fs.writeFileSync(path.join(outputRoot, 'methodology.md'), renderMethodology(dailyRows, weeklyRows));
  fs.writeFileSync(path.join(outputRoot, 'weekly-predictions.md'), renderWeeklyTable(weeklyRows));

  const expectedMonthFiles = new Set(Array.from(groupedDaily.keys()).map((monthKey) => `${monthKey}.md`));
  for (const existingFile of listExistingMonthFiles(dailyDir)) {
    if (!expectedMonthFiles.has(existingFile)) {
      fs.rmSync(path.join(dailyDir, existingFile), { force: true });
    }
  }

  for (const [monthKey, rows] of groupedDaily.entries()) {
    fs.writeFileSync(path.join(dailyDir, `${monthKey}.md`), renderDailyMonthTable(monthKey, rows));
  }

  fs.writeFileSync(path.join(jsonDir, 'daily-ledger.json'), JSON.stringify(dailyRows, null, 2));
  fs.writeFileSync(path.join(jsonDir, 'weekly-ledger.json'), JSON.stringify(weeklyRows, null, 2));
}

function buildConfigSnapshot(sourceFilter = '') {
  return {
    chatMode: CHAT_MODE,
    llmEnabled: llmExtractor.enabled,
    outputRoot: OUTPUT_ROOT,
    outputSuffix: OUTPUT_SUFFIX || '',
    sourceFilter: normalizeWhitespace(sourceFilter),
    targetMonths: TARGET_MONTH_FILTERS.slice(),
  };
}

function comparableBuildConfig(config = {}) {
  return {
    chatMode: config.chatMode || '',
    llmEnabled: Boolean(config.llmEnabled),
    outputRoot: config.outputRoot || '',
    outputSuffix: config.outputSuffix || '',
    sourceFilter: normalizeWhitespace(config.sourceFilter || ''),
    targetMonths: Array.isArray(config.targetMonths) ? config.targetMonths.slice() : [],
  };
}

function buildSourceStateEntry(source) {
  return {
    id: source.id,
    sourceType: source.sourceType,
    sourcePath: source.sourcePath,
    timestamp: source.timestamp,
    title: source.title,
    sourceHash: source.sourceHash,
  };
}

function buildOutputState({ manifest, activeSources, dailyRows, weeklyRows, sourceFilter = '' }) {
  const allRows = dailyRows.concat(weeklyRows);
  return {
    stateVersion: STATE_VERSION,
    builtAt: new Date().toISOString(),
    buildConfig: buildConfigSnapshot(sourceFilter),
    sync: {
      latestPostDate: manifest?.sync?.latestPostDate || '',
      latestChatDate: manifest?.sync?.latestChatDate || '',
      latestSourceTimestamp: maxIso(activeSources.map((source) => source.timestamp)),
      latestPredictionMadeAt: maxIso(allRows.map((row) => row.madeAt)),
      latestHeatmapMadeAt: maxIso(allRows.filter((row) => row.basis === 'optiondepth_heatmap').map((row) => row.madeAt)),
    },
    counts: {
      sourceCount: activeSources.length,
      dailyRows: dailyRows.length,
      weeklyRows: weeklyRows.length,
      commentaryRows: dailyRows.filter((row) => row.basis === 'commentary').length,
      scriptRows: dailyRows.filter((row) => row.basis === 'script_levels').length,
      heatmapRows: dailyRows.filter((row) => row.basis === 'optiondepth_heatmap').length,
    },
    sources: activeSources.map(buildSourceStateEntry),
  };
}

function writeBuildState(state, outputRoot = OUTPUT_ROOT) {
  fs.writeFileSync(statePathForRoot(outputRoot), JSON.stringify(state, null, 2));
}

function loadExistingRows(outputRoot = OUTPUT_ROOT) {
  const jsonDir = jsonDirForRoot(outputRoot);
  return {
    dailyRows: readJsonIfExists(path.join(jsonDir, 'daily-ledger.json'), []),
    weeklyRows: readJsonIfExists(path.join(jsonDir, 'weekly-ledger.json'), []),
  };
}

function canReuseIncrementalState(existingState, sourceFilter = '') {
  if (!existingState || existingState.stateVersion !== STATE_VERSION) return false;
  const existingConfig = JSON.stringify(comparableBuildConfig(existingState.buildConfig || {}));
  const currentConfig = JSON.stringify(comparableBuildConfig(buildConfigSnapshot(sourceFilter)));
  return existingConfig === currentConfig;
}

function buildIncrementalPlan(activeSources = [], existingState = null) {
  const previousSources = new Map((existingState?.sources || []).map((source) => [source.id, source]));
  const latestSourceTimestamp = existingState?.sync?.latestSourceTimestamp || '';
  const overlapCutoffIso = latestSourceTimestamp
    ? shiftIsoByHours(latestSourceTimestamp, -INCREMENTAL_OVERLAP_HOURS)
    : '';
  const changedSources = activeSources.filter((source) => {
    const previous = previousSources.get(source.id);
    if (!previous) return true;
    if (previous.sourceHash !== source.sourceHash) return true;
    if (previous.timestamp !== source.timestamp) return true;
    if (previous.sourcePath !== source.sourcePath) return true;
    if (overlapCutoffIso && String(source.timestamp || '') >= overlapCutoffIso) return true;
    return false;
  });
  return {
    overlapCutoffIso,
    changedSources,
    changedSourcePaths: new Set(changedSources.map((source) => source.sourcePath)),
    activeSourcePaths: new Set(activeSources.map((source) => source.sourcePath)),
  };
}

function filterRowsForIncrementalBase(rows = [], plan) {
  return (rows || []).filter((row) => {
    if (!plan.activeSourcePaths.has(row.sourcePath)) return false;
    if (plan.changedSourcePaths.has(row.sourcePath)) return false;
    return true;
  });
}

function shouldReplaceChatRowInMain(row, currentChatSourcePaths = new Set(), monthFilters = TARGET_MONTH_FILTERS, sourceFilter = '') {
  if (row.sourceType !== 'chat') return false;
  if (Array.isArray(monthFilters) && monthFilters.length > 0) {
    if (row.targetDate && shouldKeepTargetMonth(row.targetDate, monthFilters)) return true;
    if (row.targetWeek && weekRangeIntersectsTargetMonths(row.targetWeek, monthFilters)) return true;
  }
  if (sourceFilter) {
    const haystack = `${row.sourceTitle || ''} ${row.sourcePath || ''}`.toLowerCase();
    return haystack.includes(sourceFilter.toLowerCase());
  }
  if (currentChatSourcePaths.size > 0) return currentChatSourcePaths.has(row.sourcePath);
  return true;
}

function mergeChatOnlyRowsIntoMainOutput({ manifest, chatDailyRows, chatWeeklyRows, currentChatSources, sourceFilter = '' }) {
  const currentChatSourcePaths = new Set(currentChatSources.map((source) => source.sourcePath));
  const mainOutput = loadExistingRows(DEFAULT_OUTPUT_ROOT);
  const mergedDailyRows = dedupeRows(
    mainOutput.dailyRows
      .filter((row) => !shouldReplaceChatRowInMain(row, currentChatSourcePaths, TARGET_MONTH_FILTERS, sourceFilter))
      .concat(chatDailyRows),
  ).sort(compareRows);
  const mergedWeeklyRows = dedupeRows(
    mainOutput.weeklyRows
      .filter((row) => !shouldReplaceChatRowInMain(row, currentChatSourcePaths, TARGET_MONTH_FILTERS, sourceFilter))
      .concat(chatWeeklyRows),
  ).sort(compareRows);

  writeOutputs(mergedDailyRows, mergedWeeklyRows, DEFAULT_OUTPUT_ROOT);
  const mainSources = buildSources(manifest)
    .filter((source) => !titleLooksEducational(source.title))
    .filter((source) => source.sourceType === 'post' || source.sourceType === 'chat');
  const mergedState = buildOutputState({
    manifest,
    activeSources: mainSources,
    dailyRows: mergedDailyRows,
    weeklyRows: mergedWeeklyRows,
    sourceFilter: '',
  });
  mergedState.buildConfig = {
    ...buildConfigSnapshot(''),
    chatMode: 'on',
    targetMonths: [],
    outputRoot: DEFAULT_OUTPUT_ROOT,
    outputSuffix: '',
  };
  mergedState.mergedFromChatOnlyAt = new Date().toISOString();
  writeBuildState(mergedState, DEFAULT_OUTPUT_ROOT);
  return {
    dailyRows: mergedDailyRows.length,
    weeklyRows: mergedWeeklyRows.length,
  };
}

async function mapWithConcurrency(items = [], concurrency = 1, worker) {
  const limit = Math.max(1, Number(concurrency) || 1);
  const results = new Array(items.length);
  let nextIndex = 0;

  async function runWorker() {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await worker(items[index], index);
    }
  }

  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, () => runWorker()));
  return results;
}

async function main() {
  const manifest = readJson(MANIFEST_PATH);
  const sourceFilter = normalizeWhitespace(process.env.ALMA_PREDICTION_SOURCE_FILTER || '');
  let sources = buildSources(manifest)
    .filter((source) => {
      if (!sourceFilter) return true;
      const haystack = `${source.title} ${source.sourcePath} ${source.timestamp} ${source.date}`.toLowerCase();
      return haystack.includes(sourceFilter.toLowerCase());
    });
  if (CHAT_MODE === 'disabled') {
    sources = sources.filter((source) => source.sourceType !== 'chat');
  } else if (CHAT_MODE === 'chat-only') {
    sources = sources.filter((source) => source.sourceType === 'chat');
  }
  const activeSources = sources.filter((source) => !titleLooksEducational(source.title));
  const existingState = readJsonIfExists(statePathForRoot(OUTPUT_ROOT), null);
  const existingRows = loadExistingRows(OUTPUT_ROOT);
  const incrementalReusable = INCREMENTAL_ENABLED && canReuseIncrementalState(existingState, sourceFilter);
  const incrementalPlan = incrementalReusable ? buildIncrementalPlan(activeSources, existingState) : null;
  const sourcesToProcess = incrementalPlan ? incrementalPlan.changedSources : activeSources;
  let baseDailyRows = incrementalPlan ? filterRowsForIncrementalBase(existingRows.dailyRows, incrementalPlan) : [];
  let baseWeeklyRows = incrementalPlan ? filterRowsForIncrementalBase(existingRows.weeklyRows, incrementalPlan) : [];
  const sourceConcurrency = Number(process.env.ALMA_PREDICTION_SOURCE_CONCURRENCY || (llmExtractor.enabled ? 3 : 1));
  let extractedCount = 0;
  logProgress(
    incrementalPlan
      ? `starting incremental extraction for ${sourcesToProcess.length}/${activeSources.length} sources with concurrency ${sourceConcurrency} overlap=${incrementalPlan.overlapCutoffIso || 'none'}`
      : `starting extraction for ${activeSources.length} sources with concurrency ${sourceConcurrency}`,
  );
  const extractedSources = await mapWithConcurrency(sourcesToProcess, sourceConcurrency, async (source) => {
    if (source.sourceType === 'post') {
      const result = await buildFromPostSource(source);
      extractedCount += 1;
      if (extractedCount % 25 === 0 || extractedCount === sourcesToProcess.length) {
        logProgress(`source extraction ${extractedCount}/${sourcesToProcess.length}`);
      }
      return result;
    }
    const result = await buildFromChatSource(source);
    extractedCount += 1;
    if (extractedCount % 25 === 0 || extractedCount === sourcesToProcess.length) {
      logProgress(`source extraction ${extractedCount}/${sourcesToProcess.length}`);
    }
    return result;
  });

  let extractedDailyRows = [];
  let extractedWeeklyRows = [];
  for (const extracted of extractedSources) {
    extractedDailyRows = extractedDailyRows.concat(extracted.dailyRows);
    extractedWeeklyRows = extractedWeeklyRows.concat(extracted.weeklyRows);
  }

  extractedDailyRows = dedupeRows(extractedDailyRows).sort(compareRows);
  extractedWeeklyRows = dedupeRows(extractedWeeklyRows).sort(compareRows);
  logProgress(`extracted ${extractedDailyRows.length} daily rows and ${extractedWeeklyRows.length} weekly rows before references`);
  extractedDailyRows = await augmentRowsWithReferenceValues(extractedDailyRows);
  extractedWeeklyRows = await augmentRowsWithReferenceValues(extractedWeeklyRows);
  extractedDailyRows = extractedDailyRows.filter(keepRowPerInstrumentScope).sort(compareRows);
  extractedWeeklyRows = extractedWeeklyRows.filter(keepRowPerInstrumentScope).sort(compareRows);
  let dailyRows = dedupeRows(baseDailyRows.concat(extractedDailyRows)).sort(compareRows);
  let weeklyRows = dedupeRows(baseWeeklyRows.concat(extractedWeeklyRows)).sort(compareRows);
  dailyRows = dailyRows.filter((row) => shouldKeepTargetMonth(row.targetDate, TARGET_MONTH_FILTERS)).sort(compareRows);
  weeklyRows = weeklyRows.filter((row) => weekRangeIntersectsTargetMonths(row.targetWeek, TARGET_MONTH_FILTERS)).sort(compareRows);
  writeOutputs(dailyRows, weeklyRows, OUTPUT_ROOT);
  const outputState = buildOutputState({
    manifest,
    activeSources,
    dailyRows,
    weeklyRows,
    sourceFilter,
  });
  writeBuildState(outputState, OUTPUT_ROOT);
  let mergedMainSummary = null;
  if (CHAT_MODE === 'chat-only' && MERGE_CHAT_ONLY_INTO_MAIN) {
    mergedMainSummary = mergeChatOnlyRowsIntoMainOutput({
      manifest,
      chatDailyRows: dailyRows,
      chatWeeklyRows: weeklyRows,
      currentChatSources: sourcesToProcess,
      sourceFilter,
    });
  }
  logProgress(`wrote outputs with ${dailyRows.length} daily rows and ${weeklyRows.length} weekly rows`);

  process.stdout.write(
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        outputRoot: OUTPUT_ROOT,
        incremental: incrementalReusable,
        processedSources: sourcesToProcess.length,
        totalActiveSources: activeSources.length,
        overlapCutoffIso: incrementalPlan?.overlapCutoffIso || '',
        dailyRows: dailyRows.length,
        weeklyRows: weeklyRows.length,
        mergedMain: mergedMainSummary,
      },
      null,
      2,
    ),
  );
  process.stdout.write('\n');
}

main().catch((error) => {
  process.stderr.write(`${error.message || error}\n`);
  process.exitCode = 1;
});
