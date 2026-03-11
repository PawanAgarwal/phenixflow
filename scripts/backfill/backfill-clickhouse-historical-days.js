#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { resolveFlowWriteBackend, buildArtifactPath } = require('../../src/storage/clickhouse');
const {
  __private: {
    getClickHouseDayCache,
    getClickHouseMetricCacheMap,
    materializeHistoricalDayInClickHouse,
    METRIC_NAMES,
    DAY_CACHE_STATUS_FULL,
  },
} = require('../../src/historical-flow');
const { getThresholds } = require('../../src/historical-filter-definitions');

const VALID_MODES = new Set(['full', 'download', 'enrich']);
const MODE = String(process.env.BACKFILL_MODE || 'full').trim().toLowerCase();
const OUTPUT_PATH = process.env.BACKFILL_REPORT_PATH
  || path.resolve(process.cwd(), 'artifacts', 'reports', 'backfill-clickhouse-historical-days.json');
const FORCE = process.env.BACKFILL_FORCE === '1' || process.env.BACKFILL_FORCE === 'true';
const RETRY_ATTEMPTS = Math.max(1, Math.trunc(Number(process.env.BACKFILL_RETRY_ATTEMPTS || 3)));
const RETRY_DELAY_MS = Math.max(100, Math.trunc(Number(process.env.BACKFILL_RETRY_DELAY_MS || 2000)));
const RETRY_MAX_DELAY_MS = Math.max(RETRY_DELAY_MS, Math.trunc(Number(process.env.BACKFILL_RETRY_MAX_DELAY_MS || 20000)));
const WORKER_TOTAL = Math.max(1, Math.trunc(Number(process.env.BACKFILL_WORKER_TOTAL || 1)));
const WORKER_INDEX = Math.max(0, Math.trunc(Number(process.env.BACKFILL_WORKER_INDEX || 0)));
const JOB_LIMIT = Math.max(0, Math.trunc(Number(process.env.BACKFILL_JOB_LIMIT || 0)));
const SHARD_STRATEGY = String(process.env.BACKFILL_SHARD_STRATEGY || 'balanced').trim().toLowerCase();
const LOOP_UNTIL_READY = process.env.BACKFILL_LOOP_UNTIL_READY === '1'
  || process.env.BACKFILL_LOOP_UNTIL_READY === 'true';
const LOOP_SLEEP_MS = Math.max(250, Math.trunc(Number(process.env.BACKFILL_LOOP_SLEEP_MS || 5000)));
const LOOP_MAX_PASSES = Math.max(1, Math.trunc(Number(process.env.BACKFILL_LOOP_MAX_PASSES || 200)));
const DOWNLOAD_DONE_FLAG = String(process.env.BACKFILL_DOWNLOAD_DONE_FLAG || '').trim();
const INCLUDE_JOB_DETAILS = process.env.BACKFILL_REPORT_INCLUDE_JOBS === '1'
  || process.env.BACKFILL_REPORT_INCLUDE_JOBS === 'true';

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function resolveDefaultInputPath() {
  const reportsDir = path.resolve(process.cwd(), 'artifacts', 'reports');
  if (!fs.existsSync(reportsDir)) return null;

  const matches = fs.readdirSync(reportsDir)
    .filter((entry) => /^last-week-missing-symbol-days-\d{8}T\d{4}\.tsv$/.test(entry))
    .sort()
    .reverse();

  if (matches.length === 0) return null;
  return path.join(reportsDir, matches[0]);
}

function resolveInputPath() {
  const configured = String(process.env.BACKFILL_SYMBOL_DAY_LIST_PATH || '').trim();
  if (configured) return path.resolve(configured);
  return resolveDefaultInputPath();
}

function parseJobs(filePath) {
  return fs.readFileSync(filePath, 'utf8')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [dayIso, symbol] = line.split(/\t+/);
      return {
        dayIso: String(dayIso || '').trim(),
        symbol: String(symbol || '').trim().toUpperCase(),
      };
    })
    .filter((row) => /^\d{4}-\d{2}-\d{2}$/.test(row.dayIso) && /^[A-Z.\-]+$/.test(row.symbol));
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

const HEAVY_SYMBOL_WEIGHTS = new Map([
  ['SPY', 40],
  ['QQQ', 36],
  ['TSLA', 28],
  ['NVDA', 24],
  ['AAPL', 22],
  ['META', 20],
  ['AMD', 18],
  ['AMZN', 16],
  ['MSFT', 16],
  ['COIN', 14],
  ['PLTR', 14],
  ['MU', 12],
  ['NFLX', 12],
  ['GS', 10],
  ['ORCL', 10],
  ['CRM', 10],
  ['BAC', 8],
  ['XOM', 8],
  ['CMG', 8],
]);

function estimateJobWeight(job, originalIndex, totalRows) {
  const heavyWeight = HEAVY_SYMBOL_WEIGHTS.get(job.symbol) || 0;
  const decileSize = Math.max(1, Math.ceil(totalRows / 10));
  const rankWeight = Math.max(1, Math.ceil((totalRows - originalIndex) / decileSize));
  return heavyWeight + rankWeight;
}

function balancedShardJobs(rows, workerTotal, workerIndex) {
  const indexed = rows.map((job, index) => ({
    job,
    index,
    weight: estimateJobWeight(job, index, rows.length),
  }));
  indexed.sort((left, right) => {
    if (right.weight !== left.weight) return right.weight - left.weight;
    return left.index - right.index;
  });

  const buckets = Array.from({ length: workerTotal }, () => ({ load: 0, jobs: [] }));
  indexed.forEach((entry) => {
    let target = 0;
    for (let idx = 1; idx < buckets.length; idx += 1) {
      if (buckets[idx].load < buckets[target].load) {
        target = idx;
      }
    }
    buckets[target].jobs.push(entry);
    buckets[target].load += entry.weight;
  });

  const selected = buckets[workerIndex]?.jobs || [];
  selected.sort((left, right) => left.index - right.index);
  return selected.map(({ job }) => job);
}

function filterJobsForWorker(rows, workerTotal, workerIndex) {
  if (workerTotal <= 1) return rows;
  if (SHARD_STRATEGY === 'hash') {
    return rows.filter(({ symbol, dayIso }) => (hashJobKey(symbol, dayIso) % workerTotal) === workerIndex);
  }
  if (SHARD_STRATEGY !== 'balanced') {
    return rows.filter(({ symbol, dayIso }) => (hashJobKey(symbol, dayIso) % workerTotal) === workerIndex);
  }
  return balancedShardJobs(rows, workerTotal, workerIndex);
}

function isFullyEnriched(metricCacheMap = {}) {
  return METRIC_NAMES.every((metricName) => metricCacheMap[metricName]?.cacheStatus === DAY_CACHE_STATUS_FULL);
}

function isRetriableError(error) {
  const message = String(error?.message || '').toLowerCase();
  const theta5xx = /thetadata_request_failed:5\d\d/.test(message) || /http_5\d\d/.test(message);
  return (
    message.includes('fetch failed')
    || theta5xx
    || message.includes('thetadata_request_failed:429')
    || message.includes('thetadata_request_timeout:')
    || message.includes('timed out')
    || message.includes('timeout')
    || message.includes('connect')
    || message.includes('socket')
    || message.includes('mutations are processing')
    || message.includes('too many simultaneous queries')
    || message.includes('resource temporarily unavailable')
  );
}

function shouldSkipForMode(mode, force, dayCache, metricCacheMap) {
  if (force) return false;
  const dayFull = dayCache?.cacheStatus === DAY_CACHE_STATUS_FULL;
  const metricsFull = isFullyEnriched(metricCacheMap);
  if (mode === 'download') return dayFull && metricsFull;
  if (mode === 'enrich') return metricsFull;
  return dayFull && metricsFull;
}

function appendJobDetail(report, payload) {
  if (Array.isArray(report?.jobs)) {
    report.jobs.push(payload);
  }
}

async function processJob({
  symbol,
  dayIso,
  mode,
  thresholds,
  report,
  jobIndex,
  jobCount,
}) {
  const prefix = `[${jobIndex + 1}/${jobCount}] ${symbol} ${dayIso}`;
  const dayCache = getClickHouseDayCache({ symbol, dayIso, env: process.env });
  const metricCacheMap = getClickHouseMetricCacheMap({ symbol, dayIso, env: process.env });

  if (mode === 'enrich' && dayCache?.cacheStatus !== DAY_CACHE_STATUS_FULL) {
    report.pendingJobs += 1;
    appendJobDetail(report, {
      symbol,
      dayIso,
      status: 'pending_raw',
      cacheStatus: dayCache?.cacheStatus || null,
      rowCount: Number(dayCache?.rowCount || 0),
    });
    console.log(`${prefix} WAIT raw_not_ready`);
    return { pending: true };
  }

  if (shouldSkipForMode(mode, FORCE, dayCache, metricCacheMap)) {
    report.skippedJobs += 1;
    appendJobDetail(report, {
      symbol,
      dayIso,
      status: 'skipped',
      cacheStatus: dayCache?.cacheStatus || null,
      rowCount: Number(dayCache?.rowCount || 0),
    });
    console.log(`${prefix} SKIP already_${mode === 'download' ? 'downloaded' : 'full'}`);
    return { pending: false };
  }

  let result = null;
  let attemptsUsed = 0;
  for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt += 1) {
    try {
      result = await materializeHistoricalDayInClickHouse({
        symbol,
        dayIso,
        thresholds,
        env: process.env,
        mode,
        forceRecompute: FORCE,
      });
      if (result?.db) {
        result.db.close();
      }
      break;
    } catch (error) {
      if (attempt >= RETRY_ATTEMPTS || !isRetriableError(error)) {
        throw error;
      }
      attemptsUsed += 1;
      const delayMs = Math.min(RETRY_MAX_DELAY_MS, RETRY_DELAY_MS * (2 ** (attempt - 1)));
      console.log(`${prefix} RETRY ${attempt}/${RETRY_ATTEMPTS - 1} wait:${delayMs}ms error:${error.message}`);
      await sleep(delayMs);
    }
  }

  if (!result) {
    throw new Error('materialize_no_result');
  }

  const sync = result.sync || {};
  const enrichment = result.enrichment || {};
  const rawHydration = result.rawHydration || {};
  const coverage = rawHydration?.coverage && typeof rawHydration.coverage === 'object'
    ? rawHydration.coverage
    : null;
  const isNoData = sync.reason === 'no_data'
    && Number(enrichment.rowCount || 0) === 0
    && Number(rawHydration.tradeRows || 0) === 0;

  if (mode === 'enrich' && enrichment.reason === 'raw_not_ready') {
    report.pendingJobs += 1;
    appendJobDetail(report, {
      symbol,
      dayIso,
      status: 'pending_raw',
      attemptsUsed,
      sync,
      enrichment,
    });
    console.log(`${prefix} WAIT raw_not_ready`);
    return { pending: true };
  }

  report.completedJobs += 1;
  report.totalFetchedRows += Number(sync.fetchedRows || 0);
  report.totalEnrichedRows += Number(enrichment.rowCount || 0);
  report.totalRawTradeRows += Number(rawHydration.tradeRows || 0);
  report.totalRawStockRows += Number(rawHydration.stockRows || 0);
  report.totalRawOiRows += Number(rawHydration.oiRows || 0);
  report.totalRawQuoteRows += Number(rawHydration.quoteRows || 0);
  report.totalRawGreeksRows += Number(rawHydration.greeksRows || 0);
  if (isNoData) {
    report.noDataJobs += 1;
  }
  if (attemptsUsed > 0) {
    report.retriedJobs += 1;
    report.retryCount += attemptsUsed;
  }

  appendJobDetail(report, {
    symbol,
    dayIso,
    status: isNoData ? 'no_data' : 'completed',
    attemptsUsed,
    sync,
    enrichment: {
      rowCount: Number(enrichment.rowCount || 0),
      ruleVersion: enrichment.ruleVersion || null,
      reason: enrichment.reason || null,
    },
    rawHydration: {
      tradeRows: Number(rawHydration.tradeRows || 0),
      stockRows: Number(rawHydration.stockRows || 0),
      oiRows: Number(rawHydration.oiRows || 0),
      quoteRows: Number(rawHydration.quoteRows || 0),
      greeksRows: Number(rawHydration.greeksRows || 0),
    },
    coverage: coverage || null,
  });

  const coverageSummary = coverage && !coverage.error
    ? (
      ` slots(exp_pad:${coverage.expectedPaddedSlots ?? coverage.expectedSlots ?? 'na'}`
      + ` exp_core:${coverage.expectedCoreSlots ?? 'na'}`
      + ` stock:${coverage.stockSlots ?? 'na'}`
      + ` quote:${coverage.quoteSlots ?? 'na'}`
      + ` trade:${coverage.tradeSlots ?? 'na'}`
      + ` enrich:${coverage.enrichSlots ?? 'na'})`
      + ` missing(stock_vs_pad:${coverage.missingStockSlots ?? 'na'}`
      + ` quote_vs_core:${coverage.missingQuoteCoreSlots ?? coverage.missingQuoteSlots ?? 'na'}`
      + ` trade_vs_core:${coverage.missingTradeCoreSlots ?? coverage.missingTradeSlots ?? 'na'}`
      + ` trade_no_data:${coverage.tradeNoDataCoreSlots ?? coverage.tradeNoDataSlots ?? 'na'}`
      + ` trade_incomplete:${coverage.tradeIncompleteCoreSlots ?? coverage.tradeIncompleteSlots ?? 'na'}`
      + ` enrich_vs_trade:${coverage.missingEnrichVsTradeSlots ?? 'na'})`
      + ` coverage_pct(stock:${coverage.stockCoveragePct ?? 'na'}`
      + ` quote:${coverage.quoteCoveragePct ?? 'na'}`
      + ` trade:${coverage.tradeCoveragePct ?? 'na'})`
    )
    : coverage?.error
      ? ` coverage_error:${coverage.error}`
      : '';

  console.log(
    `${prefix} OK mode:${mode} status:${isNoData ? 'no_data' : 'completed'} `
    + `fetched:${Number(sync.fetchedRows || 0)} enriched:${Number(enrichment.rowCount || 0)} `
    + `quotes:${Number(rawHydration.quoteRows || 0)} retries:${attemptsUsed}${coverageSummary}`,
  );
  return { pending: false };
}

async function run() {
  if (!VALID_MODES.has(MODE)) {
    throw new Error(`invalid_backfill_mode:${MODE}`);
  }

  const writeBackend = resolveFlowWriteBackend(process.env);
  const inputPath = resolveInputPath();

  console.log(`Write backend: ${writeBackend}`);
  console.log(`Artifact path: ${buildArtifactPath(process.env)}`);
  console.log(`Mode: ${MODE}`);
  console.log(`Force recompute: ${FORCE}`);
  console.log(`Retry attempts: ${RETRY_ATTEMPTS} (base ${RETRY_DELAY_MS}ms, max ${RETRY_MAX_DELAY_MS}ms)`);
  console.log(`Worker shard: ${WORKER_INDEX + 1}/${WORKER_TOTAL}`);
  console.log(`Report job details: ${INCLUDE_JOB_DETAILS}`);
  if (MODE === 'enrich') {
    console.log(`Loop until ready: ${LOOP_UNTIL_READY} (sleep ${LOOP_SLEEP_MS}ms, max passes ${LOOP_MAX_PASSES})`);
    if (DOWNLOAD_DONE_FLAG) {
      console.log(`Download done flag: ${DOWNLOAD_DONE_FLAG}`);
    }
  }

  if (writeBackend !== 'clickhouse') {
    throw new Error(`clickhouse_write_backend_required:${writeBackend}`);
  }

  if (WORKER_INDEX >= WORKER_TOTAL) {
    throw new Error(`invalid_worker_shard:index=${WORKER_INDEX}:total=${WORKER_TOTAL}`);
  }

  if (!inputPath || !fs.existsSync(inputPath)) {
    throw new Error(`missing_symbol_day_list:${inputPath || 'not_found'}`);
  }

  const allJobs = parseJobs(inputPath);
  const shardedJobs = filterJobsForWorker(allJobs, WORKER_TOTAL, WORKER_INDEX);
  const jobs = JOB_LIMIT > 0 ? shardedJobs.slice(0, JOB_LIMIT) : shardedJobs;
  const thresholds = getThresholds(process.env);

  console.log(`Input: ${inputPath}`);
  console.log(`Jobs for this worker: ${jobs.length} (of ${allJobs.length} total)`);

  const report = {
    startedAt: new Date().toISOString(),
    dbPath: buildArtifactPath(process.env),
    inputPath,
    writeBackend,
    mode: MODE,
    force: FORCE,
    workerIndex: WORKER_INDEX,
    workerTotal: WORKER_TOTAL,
    jobLimit: JOB_LIMIT,
    totalJobs: jobs.length,
    loopUntilReady: LOOP_UNTIL_READY,
    loopSleepMs: LOOP_SLEEP_MS,
    loopMaxPasses: LOOP_MAX_PASSES,
    passes: 0,
    skippedJobs: 0,
    completedJobs: 0,
    pendingJobs: 0,
    noDataJobs: 0,
    retriedJobs: 0,
    retryCount: 0,
    failedJobs: 0,
    totalFetchedRows: 0,
    totalEnrichedRows: 0,
    totalRawTradeRows: 0,
    totalRawStockRows: 0,
    totalRawOiRows: 0,
    totalRawQuoteRows: 0,
    totalRawGreeksRows: 0,
    failures: [],
    jobs: INCLUDE_JOB_DETAILS ? [] : undefined,
  };

  let pass = 0;
  let pendingSet = jobs.slice();
  let lastPendingCount = Number.POSITIVE_INFINITY;
  let loopExitReason = 'completed';

  while (pendingSet.length > 0) {
    pass += 1;
    report.passes = pass;
    const nextPending = [];
    let progressed = false;

    for (let i = 0; i < pendingSet.length; i += 1) {
      const { symbol, dayIso } = pendingSet[i];
      try {
        const result = await processJob({
          symbol,
          dayIso,
          mode: MODE,
          thresholds,
          report,
          jobIndex: i,
          jobCount: pendingSet.length,
        });
        if (result.pending) {
          nextPending.push({ symbol, dayIso });
        } else {
          progressed = true;
        }
      } catch (error) {
        report.failedJobs += 1;
        report.failures.push({ symbol, dayIso, error: error.message });
        appendJobDetail(report, {
          symbol,
          dayIso,
          status: 'failed',
          error: error.message,
        });
        console.error(`[${i + 1}/${pendingSet.length}] ${symbol} ${dayIso} FAIL ${error.message}`);
      }
    }

    pendingSet = nextPending;
    if (MODE !== 'enrich' || !LOOP_UNTIL_READY || pendingSet.length === 0) {
      if (pendingSet.length > 0 && MODE === 'enrich' && !LOOP_UNTIL_READY) {
        loopExitReason = 'pending_without_loop';
      }
      break;
    }

    const downloadFinished = DOWNLOAD_DONE_FLAG ? fs.existsSync(DOWNLOAD_DONE_FLAG) : false;
    if (pass >= LOOP_MAX_PASSES) {
      loopExitReason = 'max_passes_reached';
      break;
    }
    if (!progressed && downloadFinished && pendingSet.length >= lastPendingCount) {
      loopExitReason = 'no_progress_after_download';
      break;
    }

    lastPendingCount = pendingSet.length;
    console.log(`Pass ${pass} complete. Pending raw jobs: ${pendingSet.length}. Sleeping ${LOOP_SLEEP_MS}ms...`);
    await sleep(LOOP_SLEEP_MS);
  }

  if (pendingSet.length > 0) {
    report.pendingJobs += pendingSet.length;
    pendingSet.forEach((job) => {
      appendJobDetail(report, {
        symbol: job.symbol,
        dayIso: job.dayIso,
        status: 'pending_raw',
        error: 'not_ready_before_exit',
      });
    });
  }
  report.loopExitReason = loopExitReason;
  report.completedAt = new Date().toISOString();

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(report, null, 2)}\n`, 'utf8');

  console.log('\n--- SUMMARY ---');
  console.log(JSON.stringify({
    mode: report.mode,
    passes: report.passes,
    totalJobs: report.totalJobs,
    skippedJobs: report.skippedJobs,
    completedJobs: report.completedJobs,
    pendingJobs: report.pendingJobs,
    noDataJobs: report.noDataJobs,
    retriedJobs: report.retriedJobs,
    retryCount: report.retryCount,
    failedJobs: report.failedJobs,
    totalFetchedRows: report.totalFetchedRows,
    totalEnrichedRows: report.totalEnrichedRows,
    totalRawTradeRows: report.totalRawTradeRows,
    totalRawStockRows: report.totalRawStockRows,
    totalRawOiRows: report.totalRawOiRows,
    totalRawQuoteRows: report.totalRawQuoteRows,
    totalRawGreeksRows: report.totalRawGreeksRows,
    loopExitReason: report.loopExitReason,
    reportPath: OUTPUT_PATH,
  }, null, 2));

  if (report.failedJobs > 0 || (MODE === 'enrich' && report.pendingJobs > 0)) {
    process.exitCode = 1;
  }
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
