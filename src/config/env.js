const path = require('node:path');

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

module.exports = {
  resolveDbPath,
  resolveThetaBaseUrl,
  resolveThetaIngestPath,
  resolveIngestPollIntervalMs,
  resolveIngestSymbol,
};
