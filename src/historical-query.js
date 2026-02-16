const {
  parseChipList,
  getRequiredMetricsForChips,
} = require('./historical-filter-definitions');
const { normalizeRight } = require('./historical-formulas');

function parseNumber(rawValue) {
  if (rawValue === undefined || rawValue === null || rawValue === '') return undefined;
  const parsed = Number(rawValue);
  return Number.isFinite(parsed) ? parsed : undefined;
}

function parseSentiment(rawValue) {
  if (typeof rawValue !== 'string') return undefined;
  const normalized = rawValue.trim().toLowerCase();
  if (normalized === 'bullish' || normalized === 'bearish' || normalized === 'neutral') return normalized;
  return undefined;
}

function parseHistoricalFilters(rawQuery = {}) {
  const chips = parseChipList(rawQuery.chips);
  const right = normalizeRight(rawQuery.right) || undefined;

  return {
    chips,
    right,
    sentiment: parseSentiment(rawQuery.sentiment),
    minValue: parseNumber(rawQuery.minValue),
    maxValue: parseNumber(rawQuery.maxValue),
    minSize: parseNumber(rawQuery.minSize),
    maxSize: parseNumber(rawQuery.maxSize),
    minDte: parseNumber(rawQuery.minDte),
    maxDte: parseNumber(rawQuery.maxDte),
    minOtmPct: parseNumber(rawQuery.minOtmPct),
    maxOtmPct: parseNumber(rawQuery.maxOtmPct),
    minVolOi: parseNumber(rawQuery.minVolOi),
    minRepeat3m: parseNumber(rawQuery.minRepeat3m),
    minSigScore: parseNumber(rawQuery.minSigScore),
    maxSigScore: parseNumber(rawQuery.maxSigScore),
  };
}

function getRequiredMetricsForQuery(filters) {
  const required = new Set(getRequiredMetricsForChips(filters.chips));

  if (filters.minValue !== undefined || filters.maxValue !== undefined) required.add('value');
  if (filters.minSize !== undefined || filters.maxSize !== undefined) required.add('size');
  if (filters.minDte !== undefined || filters.maxDte !== undefined) required.add('dte');
  if (filters.minOtmPct !== undefined || filters.maxOtmPct !== undefined) required.add('otmPct');
  if (filters.minVolOi !== undefined) required.add('volOiRatio');
  if (filters.minRepeat3m !== undefined) required.add('repeat3m');
  if (filters.minSigScore !== undefined || filters.maxSigScore !== undefined) required.add('sigScore');
  if (filters.sentiment !== undefined) required.add('sentiment');
  if (filters.right !== undefined) required.add('execution');

  return Array.from(required);
}

function includesChip(chips, targetChip) {
  if (!Array.isArray(chips)) return false;
  return chips.includes(targetChip);
}

function passesRange(value, minValue, maxValue) {
  if (minValue !== undefined && (value === null || value === undefined || value < minValue)) return false;
  if (maxValue !== undefined && (value === null || value === undefined || value > maxValue)) return false;
  return true;
}

function applyHistoricalFilters(rows, filters) {
  return rows.filter((row) => {
    if (filters.right && row.right !== filters.right) return false;
    if (filters.sentiment && row.sentiment !== filters.sentiment) return false;

    if (!passesRange(row.value, filters.minValue, filters.maxValue)) return false;
    if (!passesRange(row.size, filters.minSize, filters.maxSize)) return false;
    if (!passesRange(row.dte, filters.minDte, filters.maxDte)) return false;
    if (!passesRange(row.otmPct, filters.minOtmPct, filters.maxOtmPct)) return false;
    if (!passesRange(row.sigScore, filters.minSigScore, filters.maxSigScore)) return false;

    if (filters.minVolOi !== undefined && (row.volOiRatio === null || row.volOiRatio === undefined || row.volOiRatio < filters.minVolOi)) {
      return false;
    }

    if (filters.minRepeat3m !== undefined && (row.repeat3m === null || row.repeat3m === undefined || row.repeat3m < filters.minRepeat3m)) {
      return false;
    }

    if (filters.chips.length) {
      const allMatch = filters.chips.every((chipId) => includesChip(row.chips, chipId));
      if (!allMatch) return false;
    }

    return true;
  });
}

module.exports = {
  parseHistoricalFilters,
  getRequiredMetricsForQuery,
  applyHistoricalFilters,
};
