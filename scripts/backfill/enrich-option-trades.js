#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const Database = require('better-sqlite3');

const {
  __private: {
    ensureSchema,
    resolveDbPath,
    getDayCache,
    getMetricCacheMap,
    ensureEnrichedForDay,
    METRIC_NAMES,
    DAY_CACHE_STATUS_FULL,
  },
} = require('../../src/historical-flow');
const { getThresholds } = require('../../src/historical-filter-definitions');

const DB_PATH = process.env.PHENIX_DB_PATH || resolveDbPath(process.env);
const OUTPUT_PATH = process.env.ENRICH_REPORT_PATH
  || path.resolve(process.cwd(), 'artifacts', 'reports', 'enrich-option-trades.json');

const FORCE = process.env.ENRICH_FORCE === '1' || process.env.ENRICH_FORCE === 'true';

function getAllCachedDays(db) {
  return db.prepare(`
    SELECT symbol, trade_date_utc AS dayIso, row_count AS rowCount
    FROM option_trade_day_cache
    WHERE cache_status = 'full'
    ORDER BY symbol, trade_date_utc
  `).all();
}

function isFullyEnriched(metricCacheMap) {
  for (const metricName of METRIC_NAMES) {
    const entry = metricCacheMap[metricName];
    if (!entry || entry.cacheStatus !== DAY_CACHE_STATUS_FULL) return false;
  }
  return true;
}

async function run() {
  console.log(`DB: ${DB_PATH}`);
  console.log(`Force recompute: ${FORCE}`);

  if (!fs.existsSync(DB_PATH)) {
    console.error(`Database not found at ${DB_PATH}`);
    process.exitCode = 1;
    return;
  }

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 10000');
  ensureSchema(db);

  const thresholds = getThresholds(process.env);
  const cachedDays = getAllCachedDays(db);

  if (cachedDays.length === 0) {
    console.log('No cached days found in option_trade_day_cache. Nothing to enrich.');
    db.close();
    return;
  }

  console.log(`Found ${cachedDays.length} symbol+day combos with full raw data`);

  const report = {
    startedAt: new Date().toISOString(),
    dbPath: DB_PATH,
    force: FORCE,
    totalJobs: cachedDays.length,
    skippedJobs: 0,
    enrichedJobs: 0,
    failedJobs: 0,
    totalRowsEnriched: 0,
    failures: [],
  };

  for (let i = 0; i < cachedDays.length; i++) {
    const { symbol, dayIso, rowCount } = cachedDays[i];
    const prefix = `[${i + 1}/${cachedDays.length}] ${symbol} ${dayIso}`;

    if (!FORCE) {
      const metricCacheMap = getMetricCacheMap(db, { symbol, dayIso });
      if (isFullyEnriched(metricCacheMap)) {
        report.skippedJobs += 1;
        console.log(`${prefix} SKIP already enriched (${rowCount} raw rows)`);
        continue;
      }
    }

    try {
      const result = await ensureEnrichedForDay({
        db,
        symbol,
        dayIso,
        forceRecompute: true,
        thresholds,
        env: process.env,
      });

      report.enrichedJobs += 1;
      report.totalRowsEnriched += result.rowCount;
      console.log(`${prefix} OK enriched:${result.rowCount} (raw:${rowCount})`);
    } catch (error) {
      report.failedJobs += 1;
      report.failures.push({ symbol, dayIso, error: error.message });
      console.error(`${prefix} FAIL ${error.message}`);
    }
  }

  db.close();

  report.completedAt = new Date().toISOString();

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log('\n--- SUMMARY ---');
  console.log(JSON.stringify({
    totalJobs: report.totalJobs,
    skippedJobs: report.skippedJobs,
    enrichedJobs: report.enrichedJobs,
    failedJobs: report.failedJobs,
    totalRowsEnriched: report.totalRowsEnriched,
    reportPath: OUTPUT_PATH,
  }, null, 2));

  if (report.failedJobs > 0) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
