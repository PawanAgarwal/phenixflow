#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const {
  DEFAULT_INDEX_GREEKS_SYMBOLS,
  DEFAULT_SYMBOL_FILE,
  buildJobs,
  buildRunId,
  calculateGreeksToParquet,
  collectThetaConnectionSlotUsage,
  downloadQuoteRequestToParquet,
  downloadQuoteRequestToSpool,
  downloadRawGreeksRequestToParquet,
  downloadStockToParquet,
  downloadTradeRequestToParquet,
  ensureRunLayout,
  getQuotePartitionDir,
  loadStockPartition,
  parseQuoteRequestSpoolToParquet,
  parseIndexGreeksSymbols,
  probeQuotePartition,
  resolveRunRoot,
  writeJsonFile,
} = require('./common');
const {
  COMPUTE_ROLE,
  DOWNLOAD_ROLE,
  claimNextDownloadRequest,
  claimNextTask,
  collectRunState,
  completeDownloadRequest,
  completeTask,
  ensureJobStates,
  ensureRequestStates,
  failDownloadRequest,
  failTask,
  normalizeStageMaxAttempts,
  pruneCompleteRequestStatesForCompletedStages,
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
      tradeRows: Number(counters.tradeRows || 0),
      quoteRows: Number(counters.quoteRows || 0),
      rawGreekRows: Number(counters.rawGreekRows || 0),
      finalGreekRows: Number(counters.finalGreekRows || 0),
      stockMs: Number(counters.stockMs || 0),
      tradeMs: Number(counters.tradeMs || 0),
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
  role,
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

  if (role === DOWNLOAD_ROLE && workerIndex === 0) {
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

function buildThetaDownloadSnapshot() {
  const usage = collectThetaConnectionSlotUsage(process.env);
  return {
    thetaSlotsInUse: usage.slotsInUse,
    thetaSlotCapacity: usage.slotCapacity,
    thetaActiveRequests: usage.thetaActiveRequests,
    thetaQueuedRequests: usage.thetaQueuedRequests,
    thetaInUseByKind: usage.inUseByKind,
  };
}

async function runClaimedTask(claim, { runId, runRoot }) {
  if (claim.request) {
    const request = claim.request;
    if (request.kind === 'stock') {
      const startedAtMs = Date.now();
      const result = await downloadStockToParquet({
        runRoot,
        symbol: request.symbol,
        dayIso: request.dayIso,
        env: process.env,
      });
      const elapsedMs = Date.now() - startedAtMs;
      return {
        elapsedMs,
        rowCount: result.rowCount,
        meta: {
          filePath: result.filePath,
          partitionDir: path.dirname(result.filePath),
        },
        counterPatch: {
          stockRows: result.rowCount,
          stockMs: elapsedMs,
        },
      };
    }

    if (request.kind === 'trades') {
      const startedAtMs = Date.now();
      const result = await downloadTradeRequestToParquet({
        runRoot,
        symbol: request.symbol,
        dayIso: request.dayIso,
        partIndex: request.partIndex,
        window: request.window,
        env: process.env,
      });
      const elapsedMs = Date.now() - startedAtMs;
      return {
        elapsedMs,
        rowCount: result.rowCount,
        meta: {
          filePath: result.filePath,
          partitionDir: result.partitionDir,
          window: request.window,
          partIndex: request.partIndex,
          endpoint: result.endpoint,
        },
        counterPatch: {
          tradeRows: result.rowCount,
          tradeMs: elapsedMs,
        },
      };
    }

    if (request.kind === 'quote') {
      const startedAtMs = Date.now();
      const result = await downloadQuoteRequestToParquet({
        runRoot,
        symbol: request.symbol,
        dayIso: request.dayIso,
        partIndex: request.partIndex,
        window: request.window,
        env: process.env,
      });
      const elapsedMs = Date.now() - startedAtMs;
      return {
        elapsedMs,
        rowCount: result.rowCount,
        meta: {
          filePath: result.filePath,
          partitionDir: result.partitionDir,
          expirations: result.expirations,
          expirationCount: result.expirations.length,
          window: request.window,
          partIndex: request.partIndex,
        },
        counterPatch: {
          quoteRows: result.rowCount,
          quoteMs: elapsedMs,
        },
      };
    }

    if (request.kind === 'quote_fetch') {
      const startedAtMs = Date.now();
      const result = await downloadQuoteRequestToSpool({
        runRoot,
        symbol: request.symbol,
        dayIso: request.dayIso,
        partIndex: request.partIndex,
        window: request.window,
        env: process.env,
      });
      const elapsedMs = Date.now() - startedAtMs;
      return {
        elapsedMs,
        rowCount: result.rowCount,
        meta: {
          outputPath: result.spoolPath,
          partitionDir: path.dirname(result.spoolPath),
          endpoint: result.endpoint,
          spoolBytes: result.spoolBytes,
          spoolElapsedMs: result.spoolElapsedMs,
          window: request.window,
          partIndex: request.partIndex,
        },
        counterPatch: {
          quoteRows: result.rowCount,
          quoteMs: elapsedMs,
        },
      };
    }

    if (request.kind === 'quote_parse') {
      const startedAtMs = Date.now();
      const result = await parseQuoteRequestSpoolToParquet({
        runRoot,
        symbol: request.symbol,
        dayIso: request.dayIso,
        partIndex: request.partIndex,
        window: request.window,
        env: process.env,
      });
      const elapsedMs = Date.now() - startedAtMs;
      return {
        elapsedMs,
        rowCount: result.rowCount,
        meta: {
          filePath: result.filePath,
          partitionDir: result.partitionDir,
          expirations: result.expirations,
          expirationCount: result.expirations.length,
          window: request.window,
          partIndex: request.partIndex,
          sourcePath: result.spoolPath,
          parseElapsedMs: result.parseElapsedMs,
        },
        counterPatch: {
          quoteRows: result.rowCount,
          quoteMs: elapsedMs,
        },
      };
    }

    if (request.kind === 'raw_greeks') {
      const startedAtMs = Date.now();
      const result = await downloadRawGreeksRequestToParquet({
        runRoot,
        symbol: request.symbol,
        dayIso: request.dayIso,
        expirations: request.expirations,
        window: request.window,
        partIndex: request.partIndex,
        runId,
        env: process.env,
      });
      const elapsedMs = Date.now() - startedAtMs;
      return {
        elapsedMs,
        rowCount: result.rawRowsWritten,
        meta: {
          expirations: request.expirations,
          window: request.window,
          partIndex: request.partIndex,
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

    throw new Error(`unsupported_request_kind:${request.kind}`);
  }

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
    role,
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
    await ensureRequestStates({
      runRoot,
      jobs,
      env: process.env,
    });
    const pruned = pruneCompleteRequestStatesForCompletedStages(runRoot);
    writeJobsReady(runRoot, {
      jobCount: jobs.length,
      initializedBy: workerId,
      readyToken: String(process.env.PARQUET_READY_TOKEN || '').trim() || null,
      prunedCompletedRequests: pruned.removed,
    });
  } else {
    await waitForJobStatesReady(runRoot, {
      readyToken: String(process.env.PARQUET_READY_TOKEN || '').trim() || null,
    });
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

    const claim = role === DOWNLOAD_ROLE
      ? await claimNextDownloadRequest({
        runRoot,
        workerId,
        maxAttempts: stageMaxAttempts,
        env: process.env,
      })
      : await claimNextTask({
        runRoot,
        role,
        workerId,
        maxAttempts: stageMaxAttempts,
        env: process.env,
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
    currentTask = claim.request
      ? {
        requestId: claim.request.requestId,
        symbol: claim.request.symbol,
        dayIso: claim.request.dayIso,
        greekMode: claim.request.greekMode,
        stageName: claim.request.stageName,
        requestKind: claim.request.kind,
        partIndex: claim.request.partIndex,
        window: claim.request.window || null,
        claimedAt: new Date().toISOString(),
      }
      : {
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
      symbol: claim.request ? claim.request.symbol : claim.job.symbol,
      dayIso: claim.request ? claim.request.dayIso : claim.job.dayIso,
      greekMode: claim.request ? claim.request.greekMode : claim.job.greekMode,
      stageName: claim.request ? claim.request.stageName : claim.stageName,
      requestKind: claim.request ? claim.request.kind : null,
      partIndex: claim.request ? claim.request.partIndex : null,
      ...(claim.request ? buildThetaDownloadSnapshot() : {}),
    }));

    try {
      const result = await runClaimedTask(claim, { runId, runRoot });
      if (claim.request) {
        await completeDownloadRequest(claim, {
          rowCount: result.rowCount,
          elapsedMs: result.elapsedMs,
          meta: result.meta,
          env: process.env,
        });
      } else {
        await completeTask(claim, {
          rowCount: result.rowCount,
          elapsedMs: result.elapsedMs,
          meta: result.meta,
        });
      }
      counters.tasksCompleted = Number(counters.tasksCompleted || 0) + 1;
      Object.entries(result.counterPatch || {}).forEach(([key, value]) => {
        counters[key] = Number(counters[key] || 0) + Number(value || 0);
      });
      console.log('[PARQUET_TASK_DONE]', JSON.stringify({
        role,
        workerIndex,
        symbol: claim.request ? claim.request.symbol : claim.job.symbol,
        dayIso: claim.request ? claim.request.dayIso : claim.job.dayIso,
        stageName: claim.request ? claim.request.stageName : claim.stageName,
        requestKind: claim.request ? claim.request.kind : null,
        partIndex: claim.request ? claim.request.partIndex : null,
        rowCount: result.rowCount,
        elapsedMs: result.elapsedMs,
        ...(claim.request ? buildThetaDownloadSnapshot() : {}),
      }));
      lastError = null;
    } catch (error) {
      counters.tasksFailed = Number(counters.tasksFailed || 0) + 1;
      lastError = String(error?.stack || error?.message || error);
      if (claim.request) {
        await failDownloadRequest(claim, error, {
          env: process.env,
        });
      } else {
        await failTask(claim, error);
      }
      console.error('[PARQUET_TASK_FAILED]', JSON.stringify({
        role,
        workerIndex,
        symbol: claim.request ? claim.request.symbol : claim.job.symbol,
        dayIso: claim.request ? claim.request.dayIso : claim.job.dayIso,
        stageName: claim.request ? claim.request.stageName : claim.stageName,
        requestKind: claim.request ? claim.request.kind : null,
        partIndex: claim.request ? claim.request.partIndex : null,
        error: lastError.split('\n')[0],
        ...(claim.request ? buildThetaDownloadSnapshot() : {}),
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
