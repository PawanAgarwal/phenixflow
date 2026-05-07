function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    if (key.startsWith('no-')) {
      out[key.slice(3)] = false;
      continue;
    }
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

function asBoolean(value, defaultValue = false) {
  if (value === undefined) return defaultValue;
  if (typeof value === 'boolean') return value;
  return ['1', 'true', 'yes', 'y'].includes(String(value).trim().toLowerCase());
}

function asNumber(value, defaultValue) {
  if (value === undefined) return defaultValue;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : defaultValue;
}

module.exports = {
  parseArgs,
  asBoolean,
  asNumber,
};
