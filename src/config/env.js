const path = require('node:path');
const fs = require('node:fs');

function resolveDbPath(env = process.env) {
  const configured = (env.PHENIX_DB_PATH || '').trim();
  if (configured) return path.resolve(configured);
  return path.resolve(__dirname, '..', '..', 'data', 'phenixflow.sqlite');
}

function resolveThetaBaseUrl(env = process.env) {
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  if (!baseUrl) return null;
  return baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
}

function resolveThetaIngestPath(env = process.env) {
  const configured = (env.THETADATA_INGEST_PATH || '').trim();
  if (!configured) return '/v3/option/stream/trade_quote';
  return configured.startsWith('/') ? configured : `/${configured}`;
}

function resolveIngestPollIntervalMs(env = process.env) {
  const parsed = Number(env.INGEST_POLL_INTERVAL_MS);
  if (!Number.isFinite(parsed)) return 1000;
  return Math.max(250, Math.trunc(parsed));
}

function resolveIngestSymbol(env = process.env) {
  const symbol = (env.INGEST_SYMBOL || '').trim();
  return symbol ? symbol.toUpperCase() : null;
}

function parseCsvSymbols(rawValue) {
  return String(rawValue || '')
    .split(',')
    .map((token) => token.trim().toUpperCase())
    .filter(Boolean);
}

function readUniverseFile(filePath) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .map((symbol) => String(symbol || '').trim().toUpperCase())
      .filter(Boolean);
  } catch {
    return [];
  }
}

function resolveIngestSymbols(env = process.env) {
  const csv = parseCsvSymbols(env.INGEST_SYMBOLS);
  if (csv.length > 0) return csv;

  const single = resolveIngestSymbol(env);
  if (single) return [single];

  const configuredFile = (env.INGEST_UNIVERSE_FILE || '').trim();
  const universePath = configuredFile
    ? path.resolve(configuredFile)
    : path.resolve(__dirname, '..', '..', 'config', 'top200-universe.json');
  const fromFile = readUniverseFile(universePath);
  if (fromFile.length > 0) return fromFile;

  return [];
}

module.exports = {
  resolveDbPath,
  resolveThetaBaseUrl,
  resolveThetaIngestPath,
  resolveIngestPollIntervalMs,
  resolveIngestSymbol,
  resolveIngestSymbols,
};
