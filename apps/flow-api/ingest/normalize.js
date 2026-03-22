const crypto = require('node:crypto');
const { normalizeRight } = require('../historical-formulas');

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function toInteger(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : null;
}

function pickField(row, keys) {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== null && row[key] !== '') {
      return row[key];
    }
  }
  return null;
}

function toIsoTimestamp(value) {
  if (value === null || value === undefined || value === '') return null;

  if (typeof value === 'number') {
    const millis = value > 1e12 ? value : value * 1000;
    const date = new Date(millis);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
  }

  const raw = String(value).trim();
  if (!raw) return null;

  const numeric = Number(raw);
  if (Number.isFinite(numeric)) {
    const millis = numeric > 1e12 ? numeric : numeric * 1000;
    const date = new Date(millis);
    if (!Number.isNaN(date.getTime())) return date.toISOString();
  }

  const hasZone = /[zZ]|[+-]\d\d:\d\d$/.test(raw);
  const parsed = new Date(hasZone ? raw : `${raw}Z`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString();
}

function buildTradeId(row) {
  return crypto
    .createHash('sha1')
    .update([
      row.symbol,
      row.expiration,
      row.strike,
      row.optionRight,
      row.tradeTsUtc,
      row.price,
      row.size,
      row.conditionCode || '',
      row.exchange || '',
    ].join('|'))
    .digest('hex');
}

function normalizeIngestRow(rawRow, { fallbackSymbol = null, watermark = null } = {}) {
  const symbol = String(pickField(rawRow, ['symbol', 'root']) || fallbackSymbol || '').trim().toUpperCase();
  const expiration = pickField(rawRow, ['expiration', 'exp', 'expiry', 'expiration_date']);
  const strike = toNumber(pickField(rawRow, ['strike', 'strike_price']));
  const optionRight = normalizeRight(pickField(rawRow, ['right', 'option_right', 'side'])) || null;
  const price = toNumber(pickField(rawRow, ['price', 'trade_price', 'last']));
  const size = toInteger(pickField(rawRow, ['size', 'trade_size', 'quantity', 'qty']));

  if (!symbol || !expiration || strike === null || !optionRight || price === null || size === null) {
    return null;
  }

  const tradeTsUtc = toIsoTimestamp(pickField(rawRow, ['trade_timestamp', 'trade_ts', 'timestamp', 'time']));
  if (!tradeTsUtc) return null;

  const normalized = {
    tradeId: null,
    tradeTsUtc,
    tradeTsEt: tradeTsUtc,
    symbol,
    expiration: String(expiration),
    strike,
    optionRight,
    price,
    size,
    bid: toNumber(pickField(rawRow, ['bid', 'bid_price'])),
    ask: toNumber(pickField(rawRow, ['ask', 'ask_price'])),
    conditionCode: pickField(rawRow, ['condition_code', 'condition', 'sale_condition']),
    exchange: pickField(rawRow, ['exchange', 'exch']),
    rawPayloadJson: JSON.stringify(rawRow),
    watermark: watermark === undefined || watermark === null ? null : String(watermark),
  };

  normalized.tradeId = buildTradeId(normalized);
  normalized.conditionCode = normalized.conditionCode === null ? null : String(normalized.conditionCode);
  normalized.exchange = normalized.exchange === null ? null : String(normalized.exchange);

  return normalized;
}

function normalizeIngestRows(rawRows = [], options = {}) {
  const out = [];
  rawRows.forEach((rawRow) => {
    if (!rawRow || typeof rawRow !== 'object') return;
    const normalized = normalizeIngestRow(rawRow, options);
    if (normalized) out.push(normalized);
  });
  return out;
}

module.exports = {
  normalizeIngestRow,
  normalizeIngestRows,
  toIsoTimestamp,
};
