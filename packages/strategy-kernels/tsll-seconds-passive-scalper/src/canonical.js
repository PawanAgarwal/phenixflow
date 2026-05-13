const crypto = require('node:crypto');

function normalizeNumber(value) {
  if (!Number.isFinite(value)) return value;
  const rounded = Number(value.toFixed(10));
  return Object.is(rounded, -0) ? 0 : rounded;
}

function canonicalize(value) {
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (typeof value === 'number') return normalizeNumber(value);
  if (typeof value !== 'object') return value;
  if (Array.isArray(value)) {
    return value
      .map(canonicalize)
      .filter((item) => item !== undefined);
  }
  return Object.keys(value)
    .sort()
    .reduce((out, key) => {
      const next = canonicalize(value[key]);
      if (next !== undefined) out[key] = next;
      return out;
    }, {});
}

function canonicalStringify(value) {
  return JSON.stringify(canonicalize(value));
}

function sha256Text(value) {
  return crypto.createHash('sha256').update(String(value)).digest('hex');
}

function sha256Canonical(value) {
  return sha256Text(canonicalStringify(value));
}

function jsonlFromRecords(records) {
  return `${(records || []).map((record) => canonicalStringify(record)).join('\n')}\n`;
}

function sha256Jsonl(records) {
  return sha256Text(jsonlFromRecords(records));
}

function clone(value) {
  return value === undefined ? undefined : JSON.parse(JSON.stringify(value));
}

function withContentHashId(value, idField) {
  const next = clone(value);
  delete next[idField];
  return {
    ...value,
    [idField]: sha256Canonical(next),
  };
}

module.exports = {
  canonicalize,
  canonicalStringify,
  clone,
  jsonlFromRecords,
  sha256Canonical,
  sha256Jsonl,
  sha256Text,
  withContentHashId,
};
