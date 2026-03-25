#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const {
  DEFAULT_INDEX_GREEKS_SYMBOLS,
  DEFAULT_SYMBOL_FILE,
  buildJobs,
  buildRunId,
  calculateGreeksToParquet,
  downloadIndexGreeksToParquet,
  downloadQuotesToParquet,
  downloadStockToParquet,
  ensureRunLayout,
  getQuotePartitionDir,
  loadStockPartition,
  parseIndexGreeksSymbols,
  probeQuotePartition,
  resolveRunRoot,
  writeJsonFile,
} = require('./common');
const {
  COMPUTE_ROLE,
  DOWNLOAD_ROLE,
  claimNextTask,
  collectRunState,
  completeTask,
  ensureJobStates,
  failTask,
  normalizeStageMaxAttempts,
  readRunStopRequest,
  requestRunStop,
  roleShouldContinue,
  sleep,
  waitForJobStatesReady,
  writeJobsReady,
} = require('./task-state');

function parseCsv(rawValue) {
  return String(rawValue || '')
    .split(',')
    .map((token) => token.trim())
    .filter(Boolean);
}

function resolveWorkerRole() {
  const raw = String(process.env.PARQUET_WORKER_ROLE || DOWNLOAD_ROLE).trim().toLowerCase();
  if (raw === COMPUTE_ROLE) return COMPUTE_ROLE;
  return DOWNLOAD_ROLE;
}

function resolveWorkerIndex(role) {
  const roleKey = role === COMPUTE_ROLE ? 'PARQUET_COMPUTE_WORKER_INDEX' : 'PARQUET_DOWNLOAD_WORKER_INDEX';
  const fallback = Math.trunc(Number(process.env.PARQUET_WORKER_INDEX || 0));
  const parsed = Math.trunc(Number(process.env[roleKey] || fallback));
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function resolveWorkerTotal(role) {
  const roleKey = role === COMPUTE_ROLE ? 'PARQUET_COMPUTE_WORKER_TOTAL' : 'PARQUET_DOWNLOAD_WORKER_TOTAL';
  const parsed = Math.trunc(Number(process.env[roleKey] || 1));
  return Number.isFinite(parsed) && parsed >= 1 ? parsed : 1;
}

function workerReportPath(reportsRoot, role, workerIndex) {
  return path.join(reportsRoot, `${role}-worker-${workerIndex}.json`);
}

function buildWorkerReport({
  runId,
  runRoot,
  role,
  workerIndex,
  workerTotal,
  startedAt,
  currentTask = null,
  counters = {},
  completedAt = null,
  lastError = null,
}) {
  return {
    runId,
    runRoot,
    role,
    workerIndex,
    workerTotal,
    pid: process.pid,
    startedAt,
    updatedAt: new Date().toISOString(),
    completedAt,
    currentTask,
    lastError,
    counters: {
      tasksClaimed: Number(counters.tasksClaimed || 0),
      tasksCompleted: Number(counters.tasksCompleted || 0),
      tasksFailed: Number(counters.tasksFailed || 0),
      idleLoops: Number(counters.idleLoops || 0),
      stockRows: Number(counters.stockRows || 0),
      quoteRows: Number(counters.quoteRows || 0),
      rawGreekRows: Number(counters.rawGreekRows || 0),
      finalGreekRows: Number(counters.finalGreekRows || 0),
      stockMs: Number(counters.stockMs || 0),
      quoteMs: Number(counters.quoteMs || 0),
      rawGreekMs: Number(counters.rawGreekMs || 0),
      calcGreekMs: Number(counters.calcGreekMs || 0),
    },
  };
}

function persistWorkerReport(reportPath, payload) {
  writeJsonFile(reportPath, payload);
}

async function loadOrCreateManifest({
  runId,
  runRoot,
  workerIndex,
  startDate,
  endDate,
  symbolFile,
  symbolLimit,
  extraSymbols,
  indexGreeksSymbols,
}) {
  const layout = ensureRunLayout(runRoot);
  const manifestPath = path.join(layout.manifestsRoot, 'run.json');
  if (fs.existsSync(manifestPath)) {
    return JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
  }

  if (workerIndex === 0) {
    const built = await buildJobs({
      startDate,
      endDate,
      symbolFile,
      symbolLimit,
      extraSymbols,
      env: process.env,
    });
    const manifest = {
      runId,
      runRoot,
      createdAt: new Date().toISOString(),
      startDate,
      endDate,
      symbolFile,
      symbolLimit,
      extraSymbols,
      symbols: built.symbols,
      openDays: built.openDays,
      jobCount: built.jobs.length,
      indexGreeksSymbols: Array.from(indexGreeksSymbols),
    };
    writeJsonFile(manifestPath, manifest);
    return manifest;
  }

  for (let attempt = 0; attempt < 120; attempt += 1) {
    if (fs.existsSync(manifestPath)) {
      return JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    }
    await sleep(500);
  }

  const built = await buildJobs({
    startDate,
    endDate,
    symbolFile,
    symbolLimit,
    extraSymbols,
    env: process.env,
  });
  const manifest = {
    runId,
    runRoot,
    createdAt: new Date().toISOString(),
    startDate,
    endDate,
    symbolFile,
    symbolLimit,
    extraSymbols,
    symbols: built.symbols,
    openDays: built.openDays,
    jobCount: built.jobs.length,
    indexGreeksSymbols: Array.from(indexGreeksSymbols),
  };
  writeJsonFile(manifestPath, manifest);
  return manifest;
}

function manifestJobs(manifest) {
  const symbols = Array.isArray(manifest?.symbols) ? manifest.symbols : [];
  const openDays = Array.isArray(manifest?.openDays) ? manifest.openDays : [];
  const jobs = [];
  openDays.forEach((dayIso) => {
    symbols.forEach((symbol) => {
      jobs.push({ symbol, dayIso });
    });
  });
  return jobs;
}

async function runClaimedTask(claim, { runId, runRoot }) {
  const { symbol, dayIso, greekMode, stages } = claim.job;
  if (claim.stageName === 'stock') {
    const startedAtMs = Date.now();
    const result = await downloadStockToParquet({
      runRoot,
      symbol,
      dayIso,
      env: process.env,
    });
    return {
      elapsedMs: Date.now() - startedAtMs,
      rowCount: result.rowCount,
      meta: {
        filePath: result.filePath,
      },
      counterPatch: {
        stockRows: result.rowCount,
        stockMs: Date.now() - startedAtMs,
      },
    };
  }

  if (claim.stageName === 'quotes') {
    const startedAtMs = Date.now();
    const result = await downloadQuotesToParquet({
      runRoot,
      symbol,
      dayIso,
      env: process.env,
    });
    const elapsedMs = Date.now() - startedAtMs;
    return {
      elapsedMs,
      rowCount: result.rowCount,
      meta: {
        expirations: result.expirations,
        expirationCount: result.expirations.length,
        filePath: result.filePath,
      },
      counterPatch: {
        quoteRows: result.rowCount,
        quoteMs: elapsedMs,
      },
    };
  }

  if (claim.stageName === 'greeks' && greekMode === 'raw') {
    const quoteInfo = Array.isArray(stages?.quotes?.meta?.expirations)
      ? { expirations: stages.quotes.meta.expirations }
      : await probeQuotePartition(getQuotePartitionDir(runRoot, symbol, dayIso));
    const expirations = Array.isArray(quoteInfo?.expirations) ? quoteInfo.expirations : [];
    if (expirations.length === 0) {
      throw new Error(`missing_quote_expirations:${symbol}:${dayIso}`);
    }
    const startedAtMs = Date.now();
    const result = await downloadIndexGreeksToParquet({
      runRoot,
      symbol,
      dayIso,
      expirations,
      runId,
      env: process.env,
    });
    const elapsedMs = Date.now() - startedAtMs;
    return {
      elapsedMs,
      rowCount: result.rawRowsWritten,
      meta: {
        expirations,
        rawPath: result.rawPath,
        finalPath: result.finalPath,
      },
      counterPatch: {
        rawGreekRows: result.rawRowsWritten,
        finalGreekRows: result.rawRowsWritten,
        rawGreekMs: elapsedMs,
      },
    };
  }

  if (claim.stageName === 'greeks' && greekMode === 'calculated') {
    const stockPartitionDir = path.join(runRoot, 'datasets', 'raw', 'stock_ohlc_minute', `symbol=${symbol}`, `trade_date_utc=${dayIso}`);
    const stock = await loadStockPartition(stockPartitionDir);
    if (!stock) {
      throw new Error(`missing_stock_parquet:${symbol}:${dayIso}`);
    }
    const startedAtMs = Date.now();
    const result = await calculateGreeksToParquet({
      runRoot,
      symbol,
      dayIso,
      stockByMinute: stock.stockByMinute,
      runId,
      env: process.env,
    });
    const elapsedMs = Date.now() - startedAtMs;
    return {
      elapsedMs,
      rowCount: result.writtenRows,
      meta: {
        finalPath: result.finalPath,
      },
      counterPatch: {
        finalGreekRows: result.writtenRows,
        calcGreekMs: elapsedMs,
      },
    };
  }

  throw new Error(`unsupported_task:${claim.stageName}:${greekMode}`);
}

async function main() {
  const role = resolveWorkerRole();
  const workerIndex = resolveWorkerIndex(role);
  const workerTotal = resolveWorkerTotal(role);
  if (workerIndex >= workerTotal) {
    throw new Error(`invalid_worker_index:${workerIndex}/${workerTotal}:${role}`);
  }

  const runId = String(process.env.PARQUET_RUN_ID || '').trim() || buildRunId('parquet-benchmark');
  const runRoot = resolveRunRoot(runId, process.env);
  const layout = ensureRunLayout(runRoot);
  const startDate = String(process.env.START_DATE || '2025-01-02').trim();
  const endDate = String(process.env.END_DATE || '2025-01-08').trim();
  const symbolFile = path.resolve(process.env.SYMBOL_FILE || DEFAULT_SYMBOL_FILE);
  const symbolLimit = Math.max(1, Math.trunc(Number(process.env.SYMBOL_LIMIT || 100)));
  const extraSymbols = parseCsv(
    Object.prototype.hasOwnProperty.call(process.env, 'EXTRA_SYMBOLS')
      ? process.env.EXTRA_SYMBOLS
      : DEFAULT_INDEX_GREEKS_SYMBOLS.join(','),
  );
  const indexGreeksSymbols = parseIndexGreeksSymbols(process.env);
  const stageMaxAttempts = normalizeStageMaxAttempts(process.env);
  const workerId = `${role}-${workerIndex}`;

  const manifest = await loadOrCreateManifest({
    runId,
    runRoot,
    workerIndex,
    startDate,
    endDate,
    symbolFile,
    symbolLimit,
    extraSymbols,
    indexGreeksSymbols,
  });
  const jobs = manifestJobs(manifest);
  const shouldInitializeStates = role === DOWNLOAD_ROLE && workerIndex === 0;
  if (shouldInitializeStates) {
    await ensureJobStates({
      runId,
      runRoot,
      jobs,
      indexGreeksSymbols,
    });
    writeJobsReady(runRoot, { jobCount: jobs.length, initializedBy: workerId });
  } else {
    await waitForJobStatesReady(runRoot);
  }

  const reportPath = workerReportPath(layout.reportsRoot, role, workerIndex);
  const counters = {};
  const startedAt = new Date().toISOString();
  let currentTask = null;
  let lastError = null;
  let stopLogged = false;

  const writeReport = (completedAt = null) => {
    persistWorkerReport(reportPath, buildWorkerReport({
      runId,
      runRoot,
      role,
      workerIndex,
      workerTotal,
      startedAt,
      currentTask,
      counters,
      completedAt,
      lastError,
    }));
  };

  const requestStop = (reason) => {
    requestRunStop(runRoot, `${reason}:${workerId}`);
  };

  process.on('SIGINT', () => requestStop('sigint'));
  process.on('SIGTERM', () => requestStop('sigterm'));

  writeReport();

  while (true) {
    const stopRequested = readRunStopRequest(runRoot);
    if (stopRequested && !stopLogged) {
      stopLogged = true;
      console.log('[PARQUET_STOP_REQUESTED]', JSON.stringify({
        role,
        workerIndex,
        reason: stopRequested.reason || 'requested',
      }));
    }

    const claim = await claimNextTask({
      runRoot,
      role,
      workerId,
      maxAttempts: stageMaxAttempts,
    });

    if (!claim) {
      const runState = collectRunState(runRoot, { maxAttempts: stageMaxAttempts });
      counters.idleLoops = Number(counters.idleLoops || 0) + 1;
      currentTask = null;
      writeReport();
      if (!roleShouldContinue(runState, role)) break;
      await sleep(1000);
      continue;
    }

    counters.tasksClaimed = Number(counters.tasksClaimed || 0) + 1;
    currentTask = {
      symbol: claim.job.symbol,
      dayIso: claim.job.dayIso,
      greekMode: claim.job.greekMode,
      stageName: claim.stageName,
      claimedAt: new Date().toISOString(),
    };
    writeReport();
    console.log('[PARQUET_TASK_START]', JSON.stringify({
      role,
      workerIndex,
      symbol: claim.job.symbol,
      dayIso: claim.job.dayIso,
      greekMode: claim.job.greekMode,
      stageName: claim.stageName,
    }));

    try {
      const result = await runClaimedTask(claim, { runId, runRoot });
      await completeTask(claim, {
        rowCount: result.rowCount,
        elapsedMs: result.elapsedMs,
        meta: result.meta,
      });
      counters.tasksCompleted = Number(counters.tasksCompleted || 0) + 1;
      Object.entries(result.counterPatch || {}).forEach(([key, value]) => {
        counters[key] = Number(counters[key] || 0) + Number(value || 0);
      });
      console.log('[PARQUET_TASK_DONE]', JSON.stringify({
        role,
        workerIndex,
        symbol: claim.job.symbol,
        dayIso: claim.job.dayIso,
        stageName: claim.stageName,
        rowCount: result.rowCount,
        elapsedMs: result.elapsedMs,
      }));
      lastError = null;
    } catch (error) {
      counters.tasksFailed = Number(counters.tasksFailed || 0) + 1;
      lastError = String(error?.stack || error?.message || error);
      await failTask(claim, error);
      console.error('[PARQUET_TASK_FAILED]', JSON.stringify({
        role,
        workerIndex,
        symbol: claim.job.symbol,
        dayIso: claim.job.dayIso,
        stageName: claim.stageName,
        error: lastError.split('\n')[0],
      }));
    }

    currentTask = null;
    writeReport();
  }

  writeReport(new Date().toISOString());
  const runState = collectRunState(runRoot, { maxAttempts: stageMaxAttempts });
  console.log(JSON.stringify({
    runId,
    runRoot,
    role,
    workerIndex,
    workerTotal,
    stopRequested: Boolean(runState.stopRequested),
    totalJobs: runState.totalJobs,
    completeJobs: runState.completeJobs,
    failedJobs: runState.failedJobs,
    downloadReady: runState.downloadReady,
    computeReady: runState.computeReady,
  }, null, 2));
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});
