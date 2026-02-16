const path = require('node:path');
const Database = require('better-sqlite3');

const SERVICE_VERSION = '0.1.0';

function resolveDbPath(env = process.env) {
  const configuredPath = env.PHENIX_DB_PATH || path.resolve(__dirname, '..', 'data', 'phenixflow.sqlite');
  return path.resolve(configuredPath);
}

function resolveThetaHealthUrl(env = process.env) {
  const baseUrl = (env.THETADATA_BASE_URL || '').trim();
  if (!baseUrl) return null;

  const healthPath = (env.THETADATA_HEALTH_PATH || '/').trim() || '/';
  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = healthPath.startsWith('/') ? healthPath : `/${healthPath}`;
  return `${normalizedBase}${normalizedPath}`;
}

function parseBacklogMax(env = process.env) {
  const parsed = Number(env.PHENIX_ENRICHMENT_BACKLOG_MAX);
  if (!Number.isFinite(parsed) || parsed < 0) return 0;
  return Math.trunc(parsed);
}

function parseTimeoutMs(env = process.env) {
  const parsed = Number(env.THETADATA_READY_TIMEOUT_MS);
  if (!Number.isFinite(parsed) || parsed <= 0) return 3000;
  return Math.trunc(parsed);
}

function hasRequiredTables(db) {
  const required = new Set(['option_trades', 'option_trade_enriched']);
  const rows = db.prepare(`
    SELECT name
    FROM sqlite_master
    WHERE type = 'table'
      AND name IN ('option_trades', 'option_trade_enriched')
  `).all();

  rows.forEach((row) => required.delete(row.name));
  return required.size === 0;
}

function getEnrichmentBacklog(db) {
  const row = db.prepare(`
    SELECT COUNT(1) AS backlog
    FROM option_trades t
    LEFT JOIN option_trade_enriched e ON e.trade_id = t.trade_id
    WHERE e.trade_id IS NULL
  `).get();

  return Number(row?.backlog || 0);
}

function checkDb(env = process.env) {
  const dbPath = resolveDbPath(env);
  let db;

  try {
    db = new Database(dbPath, { fileMustExist: true, readonly: true });
    db.pragma('query_only = ON');

    if (!hasRequiredTables(db)) {
      return {
        ok: false,
        reason: 'db_schema_missing',
        dbPath,
      };
    }

    return {
      ok: true,
      reason: null,
      dbPath,
    };
  } catch (error) {
    return {
      ok: false,
      reason: 'db_unavailable',
      dbPath,
      error: error.message,
    };
  } finally {
    if (db) db.close();
  }
}

async function checkTheta(env = process.env) {
  const url = resolveThetaHealthUrl(env);
  if (!url) {
    return {
      ok: false,
      reason: 'thetadata_not_configured',
      url: null,
    };
  }

  const timeoutMs = parseTimeoutMs(env);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(url, {
      method: 'GET',
      signal: controller.signal,
    });

    if (!response.ok) {
      return {
        ok: false,
        reason: 'thetadata_unreachable',
        url,
        status: response.status,
      };
    }

    return {
      ok: true,
      reason: null,
      url,
      status: response.status,
    };
  } catch (error) {
    return {
      ok: false,
      reason: 'thetadata_unreachable',
      url,
      error: error?.name === 'AbortError' ? `timeout:${timeoutMs}ms` : error.message,
    };
  } finally {
    clearTimeout(timeout);
  }
}

function checkEnrichmentBacklog(env = process.env) {
  const dbPath = resolveDbPath(env);
  const backlogMax = parseBacklogMax(env);
  let db;

  try {
    db = new Database(dbPath, { fileMustExist: true, readonly: true });
    db.pragma('query_only = ON');

    if (!hasRequiredTables(db)) {
      return {
        ok: false,
        reason: 'db_schema_missing',
        backlog: null,
        backlogMax,
      };
    }

    const backlog = getEnrichmentBacklog(db);
    return {
      ok: backlog <= backlogMax,
      reason: backlog <= backlogMax ? null : 'enrichment_backlog_high',
      backlog,
      backlogMax,
    };
  } catch (error) {
    return {
      ok: false,
      reason: 'db_unavailable',
      backlog: null,
      backlogMax,
      error: error.message,
    };
  } finally {
    if (db) db.close();
  }
}

function mapReason(dbCheck, thetaCheck, backlogCheck) {
  if (!dbCheck.ok) return dbCheck.reason || 'db_unavailable';
  if (!thetaCheck.ok) return thetaCheck.reason || 'thetadata_unreachable';
  if (!backlogCheck.ok) return backlogCheck.reason || 'enrichment_backlog_high';
  return null;
}

async function getReadiness(env = process.env) {
  const dbCheck = checkDb(env);
  const thetaCheck = await checkTheta(env);
  const backlogCheck = checkEnrichmentBacklog(env);
  const ready = dbCheck.ok && thetaCheck.ok && backlogCheck.ok;

  const checks = {
    db: dbCheck.ok ? 'ok' : 'fail',
    thetadata: thetaCheck.ok ? 'ok' : 'fail',
    enrichmentBacklog: backlogCheck.ok ? 'ok' : 'fail',
  };

  if (ready) {
    return {
      statusCode: 200,
      body: {
        status: 'ready',
        checks,
        version: SERVICE_VERSION,
      },
    };
  }

  return {
    statusCode: 503,
    body: {
      status: 'not_ready',
      checks,
      reason: mapReason(dbCheck, thetaCheck, backlogCheck),
    },
  };
}

module.exports = {
  getReadiness,
  __private: {
    resolveDbPath,
    resolveThetaHealthUrl,
    parseBacklogMax,
    parseTimeoutMs,
    hasRequiredTables,
    getEnrichmentBacklog,
    checkDb,
    checkTheta,
    checkEnrichmentBacklog,
  },
};

