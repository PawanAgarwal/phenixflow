const fs = require('node:fs');

const DEFAULT_INSTRUMENT_TYPE = 'option_root';
const INDEX_SPOT_ONLY_INSTRUMENT_TYPE = 'index_spot_only';

function normalizeSymbol(rawValue) {
  if (typeof rawValue !== 'string' || !rawValue.trim()) return null;
  return rawValue.trim().toUpperCase();
}

function normalizeInstrumentType(rawValue, fallback = DEFAULT_INSTRUMENT_TYPE) {
  const raw = String(rawValue || '').trim().toLowerCase();
  if (!raw) return fallback;
  if (
    raw === INDEX_SPOT_ONLY_INSTRUMENT_TYPE
    || raw === 'index_spot'
    || raw === 'index'
    || raw === 'spot_only_index'
    || raw === 'spot_index_only'
  ) {
    return INDEX_SPOT_ONLY_INSTRUMENT_TYPE;
  }
  return DEFAULT_INSTRUMENT_TYPE;
}

function hasOptionsForInstrumentType(instrumentType) {
  return normalizeInstrumentType(instrumentType) !== INDEX_SPOT_ONLY_INSTRUMENT_TYPE;
}

function normalizeUniverseEntry(rawEntry, options = {}) {
  const defaultInstrumentType = normalizeInstrumentType(
    options.defaultInstrumentType,
    DEFAULT_INSTRUMENT_TYPE,
  );

  if (typeof rawEntry === 'string') {
    const symbol = normalizeSymbol(rawEntry);
    if (!symbol) return null;
    return {
      symbol,
      instrumentType: defaultInstrumentType,
      hasOptions: hasOptionsForInstrumentType(defaultInstrumentType),
    };
  }

  if (!rawEntry || typeof rawEntry !== 'object') return null;

  const symbol = normalizeSymbol(rawEntry.symbol || rawEntry.root || rawEntry.ticker);
  if (!symbol) return null;

  const explicitHasOptions = rawEntry.hasOptions;
  const instrumentType = explicitHasOptions === false
    ? INDEX_SPOT_ONLY_INSTRUMENT_TYPE
    : normalizeInstrumentType(rawEntry.instrumentType, defaultInstrumentType);

  return {
    symbol,
    instrumentType,
    hasOptions: explicitHasOptions === undefined
      ? hasOptionsForInstrumentType(instrumentType)
      : Boolean(explicitHasOptions),
  };
}

function readUniverseEntries(filePath, options = {}) {
  const raw = fs.readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(raw);
  if (!Array.isArray(parsed)) {
    throw new Error(`invalid_symbol_file:${filePath}`);
  }

  const entries = [];
  const seen = new Set();
  parsed.forEach((rawEntry) => {
    const entry = normalizeUniverseEntry(rawEntry, options);
    if (!entry || seen.has(entry.symbol)) return;
    seen.add(entry.symbol);
    entries.push(entry);
  });
  return entries;
}

module.exports = {
  DEFAULT_INSTRUMENT_TYPE,
  INDEX_SPOT_ONLY_INSTRUMENT_TYPE,
  normalizeSymbol,
  normalizeInstrumentType,
  hasOptionsForInstrumentType,
  normalizeUniverseEntry,
  readUniverseEntries,
};
