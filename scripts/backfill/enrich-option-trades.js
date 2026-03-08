#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const { resolveFlowWriteBackend, buildArtifactPath } = require('../../src/storage/clickhouse');

const {
  __private: {
    getClickHouseMetricCacheMap,
    listClickHouseCachedDays,
    materializeHistoricalDayInClickHouse,
    METRIC_NAMES,
    DAY_CACHE_STATUS_FULL,
  },
} = require('../../src/historical-flow');
const { getThresholds } = require('../../src/historical-filter-definitions');

const OUTPUT_PATH = process.env.ENRICH_REPORT_PATH
  || path.resolve(process.cwd(), 'artifacts', 'reports', 'enrich-option-trades.json');

const FORCE = process.env.ENRICH_FORCE === '1' || process.env.ENRICH_FORCE === 'true';
const RETRY_ATTEMPTS = Math.max(1, Number(process.env.ENRICH_LOCK_RETRIES || 12));
const RETRY_DELAY_MS = Math.max(100, Number(process.env.ENRICH_LOCK_RETRY_DELAY_MS || 2000));
const RETRY_MAX_DELAY_MS = Math.max(
  RETRY_DELAY_MS,
  Number(process.env.ENRICH_LOCK_RETRY_MAX_DELAY_MS || 20000),
);
const WORKER_TOTAL = Math.max(1, Math.trunc(Number(process.env.ENRICH_WORKER_TOTAL || 1)));
const WORKER_INDEX = Math.max(0, Math.trunc(Number(process.env.ENRICH_WORKER_INDEX || 0)));
const JOB_LIMIT = Math.max(0, Math.trunc(Number(process.env.ENRICH_JOB_LIMIT || 0)));

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetriableError(error) {
  const message = String(error?.message || '').toLowerCase();
  return (
    message.includes('timed out')
    || message.includes('timeout')
    || message.includes('connect')
    || message.includes('socket')
    || message.includes('fetch failed')
    || message.includes('thetadata_request_failed:429')
    || message.includes('too many simultaneous queries')
    || message.includes('resource temporarily unavailable')
    || message.includes('mutations are processing')
  );
}

function hashJobKey(symbol, dayIso) {
  const input = `${symbol}|${dayIso}`;
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function filterJobsForWorker(rows, workerTotal, workerIndex) {
  if (workerTotal <= 1) return rows;
  return rows.filter(({ symbol, dayIso }) => (hashJobKey(symbol, dayIso) % workerTotal) === workerIndex);
}

function isFullyEnriched(metricCacheMap) {
  for (const metricName of METRIC_NAMES) {
    const entry = metricCacheMap[metricName];
    if (!entry || entry.cacheStatus !== DAY_CACHE_STATUS_FULL) return false;
  }
  return true;
}

async function run() {
  const writeBackend = resolveFlowWriteBackend(process.env);
  console.log(`Write backend: ${writeBackend}`);
  console.log(`DB: ${buildArtifactPath(process.env)}`);
  console.log(`Force recompute: ${FORCE}`);
  console.log(`Retry attempts: ${RETRY_ATTEMPTS} (base ${RETRY_DELAY_MS}ms, max ${RETRY_MAX_DELAY_MS}ms)`);
  console.log(`Worker shard: ${WORKER_INDEX + 1}/${WORKER_TOTAL}`);

  if (writeBackend !== 'clickhouse') {
    console.error(`ClickHouse backend required, got: ${writeBackend}`);
    process.exitCode = 1;
    return;
  }

  if (WORKER_INDEX >= WORKER_TOTAL) {
    console.error(`Invalid worker shard: index=${WORKER_INDEX} total=${WORKER_TOTAL}`);
    process.exitCode = 1;
    return;
  }

  const thresholds = getThresholds(process.env);
  const allCachedDays = listClickHouseCachedDays(process.env);
  const shardedDays = filterJobsForWorker(allCachedDays, WORKER_TOTAL, WORKER_INDEX);
  const cachedDays = JOB_LIMIT > 0 ? shardedDays.slice(0, JOB_LIMIT) : shardedDays;

  if (cachedDays.length === 0) {
    console.log('No cached days found for this worker shard. Nothing to enrich.');
    return;
  }

  console.log(
    `Found ${cachedDays.length} symbol+day combos for this worker shard `
    + `(of ${allCachedDays.length} total${JOB_LIMIT > 0 ? `, limited to ${JOB_LIMIT}` : ''})`,
  );

  const report = {
    startedAt: new Date().toISOString(),
    dbPath: buildArtifactPath(process.env),
    writeBackend,
    force: FORCE,
    workerIndex: WORKER_INDEX,
    workerTotal: WORKER_TOTAL,
    jobLimit: JOB_LIMIT,
    totalJobs: cachedDays.length,
    skippedJobs: 0,
    pendingRawJobs: 0,
    enrichedJobs: 0,
    retryJobs: 0,
    retryCount: 0,
    failedJobs: 0,
    totalRowsEnriched: 0,
    failures: [],
  };

  for (let i = 0; i < cachedDays.length; i++) {
    const { symbol, dayIso, rowCount } = cachedDays[i];
    const prefix = `[${i + 1}/${cachedDays.length}] ${symbol} ${dayIso}`;

    if (!FORCE) {
      const metricCacheMap = getClickHouseMetricCacheMap({ symbol, dayIso, env: process.env });
      if (isFullyEnriched(metricCacheMap)) {
        report.skippedJobs += 1;
        console.log(`${prefix} SKIP already enriched (${rowCount} raw rows)`);
        continue;
      }
    }

    try {
      let result = null;
      let retriesUsed = 0;
      for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt += 1) {
        try {
          result = await materializeHistoricalDayInClickHouse({
            symbol,
            dayIso,
            thresholds,
            env: process.env,
            mode: 'enrich',
          });
          break;
        } catch (error) {
          if (!isRetriableError(error) || attempt >= RETRY_ATTEMPTS) {
            throw error;
          }
          retriesUsed += 1;
          const delayMs = Math.min(
            RETRY_MAX_DELAY_MS,
            RETRY_DELAY_MS * (2 ** (attempt - 1)),
          );
          console.log(`${prefix} RETRY ${attempt}/${RETRY_ATTEMPTS - 1} wait:${delayMs}ms`);
          await sleep(delayMs);
        }
      }

      if (!result) {
        throw new Error('enrichment_no_result');
      }

      const enrichment = result.enrichment || {};
      if (enrichment.reason === 'raw_not_ready') {
        report.pendingRawJobs += 1;
        console.log(`${prefix} WAIT raw_not_ready`);
        continue;
      }

      report.enrichedJobs += 1;
      report.totalRowsEnriched += Number(enrichment.rowCount || 0);
      if (retriesUsed > 0) {
        report.retryJobs += 1;
        report.retryCount += retriesUsed;
      }
      console.log(`${prefix} OK enriched:${Number(enrichment.rowCount || 0)} (raw:${rowCount}) retries:${retriesUsed}`);
    } catch (error) {
      report.failedJobs += 1;
      report.failures.push({ symbol, dayIso, error: error.message });
      console.error(`${prefix} FAIL ${error.message}`);
    }
  }

  report.completedAt = new Date().toISOString();

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log('\n--- SUMMARY ---');
  console.log(JSON.stringify({
    totalJobs: report.totalJobs,
    skippedJobs: report.skippedJobs,
    pendingRawJobs: report.pendingRawJobs,
    enrichedJobs: report.enrichedJobs,
    retryJobs: report.retryJobs,
    retryCount: report.retryCount,
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
