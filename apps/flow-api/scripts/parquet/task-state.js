const fs = require('node:fs');
const path = require('node:path');

const {
  ensureRunLayout,
  fetchCalendarSessionWindow,
  collectThetaConnectionSlotUsage,
  getFinalGreeksPartitionDir,
  getPartitionPartPath,
  getQuotePartitionDir,
  getQuoteSpoolPath,
  getQuoteSpoolPartitionDir,
  getRawGreeksPartitionDir,
  getStockPartitionDir,
  getTradeQuotePartitionDir,
  getTradePartitionDir,
  listParquetPartFiles,
  parseQuoteRequestSpoolToParquet,
  parseIndexGreeksSymbols,
  probeQuotePartition,
  probeRowPartition,
  probeStockPartition,
  resolveQuoteRequestPlan,
  resolveRawGreeksRequestPlan,
  resolveTradeRequestPlan,
  writePartitionSuccessMarker,
} = require('./common');

const DOWNLOAD_ROLE = 'download';
const COMPUTE_ROLE = 'compute';
const REQUEST_KIND_STOCK = 'stock';
const REQUEST_KIND_TRADES = 'trades';
const REQUEST_KIND_QUOTE = 'quote';
const REQUEST_KIND_QUOTE_FETCH = 'quote_fetch';
const REQUEST_KIND_QUOTE_PARSE = 'quote_parse';
const REQUEST_KIND_RAW_GREEKS = 'raw_greeks';
const DEFAULT_STAGE_MAX_ATTEMPTS = 3;
const DEFAULT_THETA_ACTIVE_TARGET = 8;
const DEFAULT_THETA_QUEUED_TARGET = 16;
const DEFAULT_THETA_PER_JOB_LIMIT = 1;
const DEFAULT_THETA_PER_JOB_BURST_LIMIT = 2;
const THETA_RETRY_WINDOW_MS = 5 * 60 * 1000;

function nowIso() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Math.trunc(ms || 0))));
}

function ensureDir(target) {
  fs.mkdirSync(target, { recursive: true });
}

function readJsonFile(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function writeJsonAtomic(filePath, payload) {
  ensureDir(path.dirname(filePath));
  const tempPath = `${filePath}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2, 8)}.tmp`;
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  fs.renameSync(tempPath, filePath);
}

function parseNumberEnv(name, fallback, env = process.env) {
  const parsed = Number(env[name]);
  if (Number.isFinite(parsed)) return parsed;
  return fallback;
}

function normalizeStageMaxAttempts(env = process.env) {
  const parsed = Number(env.PARQUET_STAGE_MAX_ATTEMPTS);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(100, Math.trunc(parsed)));
  }
  return DEFAULT_STAGE_MAX_ATTEMPTS;
}

function parseThetaActiveTarget(env = process.env) {
  return Math.max(1, Math.min(16, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_ACTIVE_TARGET',
    parseNumberEnv('PARQUET_THETA_MAX_CONCURRENT_CONNECTIONS', DEFAULT_THETA_ACTIVE_TARGET, env),
    env,
  ))));
}

function parseThetaQueuedTarget(env = process.env) {
  return Math.max(0, Math.min(32, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_QUEUED_TARGET',
    DEFAULT_THETA_QUEUED_TARGET,
    env,
  ))));
}

function parseThetaPerJobLimit(env = process.env) {
  return Math.max(1, Math.min(8, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_PER_JOB_LIMIT',
    DEFAULT_THETA_PER_JOB_LIMIT,
    env,
  ))));
}

function parseThetaPerJobBurstLimit(env = process.env) {
  return Math.max(parseThetaPerJobLimit(env), Math.min(8, Math.trunc(parseNumberEnv(
    'PARQUET_THETA_PER_JOB_BURST_LIMIT',
    DEFAULT_THETA_PER_JOB_BURST_LIMIT,
    env,
  ))));
}

function parseComputeWorkerTotal(env = process.env) {
  return Math.max(1, Math.trunc(parseNumberEnv('PARQUET_COMPUTE_WORKERS', 1, env)));
}

function parseQuoteUseSpool(env = process.env) {
  const raw = String(env.PARQUET_QUOTE_USE_SPOOL || '').trim().toLowerCase();
  if (!raw) return false;
  return raw === '1' || raw === 'true' || raw === 'yes';
}

function parseHeavyDownloadWorkers(env = process.env) {
  const configured = Math.max(0, Math.trunc(parseNumberEnv('PARQUET_HEAVY_DOWNLOAD_WORKERS', 0, env)));
  const total = Math.max(1, Math.trunc(parseNumberEnv('PARQUET_DOWNLOAD_WORKER_TOTAL', 1, env)));
  return Math.min(configured, total);
}

function parseHeavyDownloadSymbols(env = process.env) {
  const raw = String(env.PARQUET_HEAVY_DOWNLOAD_SYMBOLS || '').trim();
  if (raw) {
    return new Set(raw.split(',').map((token) => String(token || '').trim().toUpperCase()).filter(Boolean));
  }
  return parseIndexGreeksSymbols(env);
}

function resolveDownloadWorkerLane(env = process.env) {
  const heavyWorkers = parseHeavyDownloadWorkers(env);
  const totalWorkers = Math.max(1, Math.trunc(parseNumberEnv('PARQUET_DOWNLOAD_WORKER_TOTAL', 1, env)));
  const workerIndex = Math.max(0, Math.trunc(parseNumberEnv('PARQUET_DOWNLOAD_WORKER_INDEX', 0, env)));
  if (heavyWorkers <= 0 || heavyWorkers >= totalWorkers) return 'mixed';
  return workerIndex < heavyWorkers ? 'heavy' : 'regular';
}

function requestMatchesDownloadWorkerLane(requestState, heavySymbols, lane) {
  if (lane === 'mixed') return true;
  const symbol = String(requestState?.symbol || '').trim().toUpperCase();
  const isHeavy = heavySymbols.has(symbol);
  return lane === 'heavy' ? isHeavy : !isHeavy;
}

function isThetaDownloadRequestKind(kind) {
  return kind === REQUEST_KIND_STOCK
    || kind === REQUEST_KIND_TRADES
    || kind === REQUEST_KIND_QUOTE
    || kind === REQUEST_KIND_QUOTE_FETCH
    || kind === REQUEST_KIND_RAW_GREEKS;
}

function isComputeRequestKind(kind) {
  return kind === REQUEST_KIND_QUOTE_PARSE;
}

function jobId(symbol, dayIso) {
  return `${String(symbol).trim().toUpperCase()}__${String(dayIso).trim()}`;
}

function buildRequestId(symbol, dayIso, kind, partIndex = 0) {
  return `${jobId(symbol, dayIso)}__${kind}__${String(Math.max(0, Math.trunc(partIndex || 0))).padStart(4, '0')}`;
}

function getStatePaths(runRoot) {
  const layout = ensureRunLayout(runRoot);
  return {
    ...layout,
    stopPath: path.join(layout.controlRoot, 'stop-requested.json'),
    readyPath: path.join(layout.controlRoot, 'jobs-ready.json'),
    requestStateRoot: path.join(layout.stateRoot, 'requests'),
    sessionCacheRoot: path.join(layout.stateRoot, 'sessions'),
    schedulerLockPath: path.join(layout.controlRoot, 'theta-request-scheduler.lock'),
  };
}

function getJobStatePath(runRoot, symbol, dayIso) {
  return path.join(getStatePaths(runRoot).jobStateRoot, `${jobId(symbol, dayIso)}.json`);
}

function getRequestStatePath(runRoot, requestId) {
  return path.join(getStatePaths(runRoot).requestStateRoot, `${requestId}.json`);
}

function getSessionWindowCachePath(runRoot, dayIso) {
  return path.join(getStatePaths(runRoot).sessionCacheRoot, `${String(dayIso).trim()}.json`);
}

function getRequestStatePathForParts(runRoot, {
  symbol,
  dayIso,
  kind,
  partIndex = 0,
}) {
  return getRequestStatePath(runRoot, buildRequestId(symbol, dayIso, kind, partIndex));
}

function getTaskLockPath(runRoot, symbol, dayIso, stageName) {
  return path.join(getStatePaths(runRoot).lockRoot, `${jobId(symbol, dayIso)}__${stageName}.lock`);
}

function getJobUpdateLockPath(runRoot, symbol, dayIso) {
  return path.join(getStatePaths(runRoot).lockRoot, `${jobId(symbol, dayIso)}__state.lock`);
}

function listJobStateFiles(runRoot) {
  const { jobStateRoot } = getStatePaths(runRoot);
  if (!fs.existsSync(jobStateRoot)) return [];
  return fs.readdirSync(jobStateRoot)
    .filter((name) => name.endsWith('.json'))
    .sort()
    .map((name) => path.join(jobStateRoot, name));
}

function listRequestStateFiles(runRoot) {
  const { requestStateRoot } = getStatePaths(runRoot);
  if (!fs.existsSync(requestStateRoot)) return [];
  return fs.readdirSync(requestStateRoot)
    .filter((name) => name.endsWith('.json'))
    .sort()
    .map((name) => path.join(requestStateRoot, name));
}

function partitionHasData(partitionDir) {
  if (!fs.existsSync(partitionDir)) return false;
  try {
    return fs.readdirSync(partitionDir).some((name) => name.endsWith('.parquet'));
  } catch {
    return false;
  }
}

function isPidAlive(pid) {
  const normalizedPid = Math.trunc(Number(pid) || 0);
  if (normalizedPid <= 0) return false;
  try {
    process.kill(normalizedPid, 0);
    return true;
  } catch (error) {
    return error?.code === 'EPERM';
  }
}

function cleanupStaleLock(lockPath) {
  const metadata = readJsonFile(lockPath);
  if (metadata?.pid && isPidAlive(metadata.pid)) return false;
  fs.rmSync(lockPath, { recursive: true, force: true });
  return true;
}

function buildStageState(name, mode = null, controlMode = 'task') {
  return {
    name,
    mode,
    controlMode,
    status: 'pending',
    attempts: 0,
    claimedBy: null,
    claimedAt: null,
    startedAt: null,
    completedAt: null,
    updatedAt: nowIso(),
    error: null,
    rowCount: 0,
    elapsedMs: 0,
    meta: {},
  };
}

function buildInitialJobState({
  runId,
  runRoot,
  symbol,
  dayIso,
  greekMode,
}) {
  return {
    runId,
    runRoot,
    symbol,
    dayIso,
    jobId: jobId(symbol, dayIso),
    greekMode,
    createdAt: nowIso(),
    updatedAt: nowIso(),
    status: 'pending',
    stages: {
      stock: buildStageState('stock', null, 'requests'),
      trades: buildStageState('trades', null, 'requests'),
      quotes: buildStageState('quotes', null, 'requests'),
      greeks: buildStageState('greeks', greekMode, greekMode === 'raw' ? 'requests' : 'task'),
    },
  };
}

function refreshJobStatus(jobState) {
  const greeksStage = jobState?.stages?.greeks;
  const stockStage = jobState?.stages?.stock;
  const tradeStage = jobState?.stages?.trades;
  const quoteStage = jobState?.stages?.quotes;
  if (stockStage?.status === 'complete' && tradeStage?.status === 'complete' && quoteStage?.status === 'complete' && greeksStage?.status === 'complete') {
    jobState.status = 'complete';
  } else if ([stockStage, tradeStage, quoteStage, greeksStage].some((stage) => stage?.status === 'running')) {
    jobState.status = 'running';
  } else if ([stockStage, tradeStage, quoteStage, greeksStage].some((stage) => stage?.status === 'failed')) {
    jobState.status = 'failed';
  } else {
    jobState.status = 'pending';
  }
  jobState.updatedAt = nowIso();
  return jobState;
}

async function inferExistingStageState(jobState) {
  const { runRoot, symbol, dayIso, greekMode } = jobState;
  const stockPartitionDir = getStockPartitionDir(runRoot, symbol, dayIso);
  const stockMarker = readJsonFile(path.join(stockPartitionDir, '_SUCCESS.json'));
  if ((Number(stockMarker?.partCount || 0) > 0) || partitionHasData(stockPartitionDir)) {
    jobState.stages.stock = {
      ...jobState.stages.stock,
      controlMode: 'requests',
      status: 'complete',
      startedAt: jobState.stages.stock.startedAt || stockMarker?.completedAt || jobState.createdAt,
      completedAt: stockMarker?.completedAt || nowIso(),
      updatedAt: nowIso(),
      rowCount: Number(stockMarker?.rowCount || jobState.stages.stock.rowCount || 0),
      meta: {
        ...(jobState.stages.stock.meta || {}),
        partCount: stockMarker?.partCount || null,
        legacyFastPath: !stockMarker,
      },
    };
  }

  const tradePartitionDir = getTradePartitionDir(runRoot, symbol, dayIso);
  const tradeQuotePartitionDir = getTradeQuotePartitionDir(runRoot, symbol, dayIso);
  const tradeMarker = readJsonFile(path.join(tradePartitionDir, '_SUCCESS.json'));
  const tradeQuoteMarker = readJsonFile(path.join(tradeQuotePartitionDir, '_SUCCESS.json'));
  const hasTradeData = (Number(tradeMarker?.partCount || 0) > 0) || partitionHasData(tradePartitionDir);
  const hasTradeQuoteData = (Number(tradeQuoteMarker?.partCount || 0) > 0) || partitionHasData(tradeQuotePartitionDir);
  if (hasTradeData && hasTradeQuoteData) {
    const tradeProbe = await probeRowPartition(tradePartitionDir, 'trades');
    const tradeQuoteProbe = await probeRowPartition(tradeQuotePartitionDir, 'trade_quotes');
    jobState.stages.trades = {
      ...jobState.stages.trades,
      controlMode: 'requests',
      status: 'complete',
      startedAt: jobState.stages.trades.startedAt || tradeMarker?.completedAt || jobState.createdAt,
      completedAt: tradeQuoteMarker?.completedAt || tradeMarker?.completedAt || tradeQuoteProbe?.marker?.completedAt || tradeProbe?.marker?.completedAt || nowIso(),
      updatedAt: nowIso(),
      rowCount: Number(tradeProbe?.rowCount || tradeMarker?.rowCount || jobState.stages.trades.rowCount || 0),
      meta: {
        ...(jobState.stages.trades.meta || {}),
        partCount: tradeMarker?.partCount || tradeProbe?.marker?.partCount || null,
        tradeQuotePartCount: tradeQuoteMarker?.partCount || tradeQuoteProbe?.marker?.partCount || null,
        tradeQuoteRowCount: Number(tradeQuoteProbe?.rowCount || tradeQuoteMarker?.rowCount || 0),
        legacyFastPath: !tradeMarker || !tradeQuoteMarker,
      },
    };
  } else if (jobState.stages.trades.controlMode === 'requests' && jobState.stages.trades.status === 'complete') {
    jobState.stages.trades = {
      ...jobState.stages.trades,
      status: 'pending',
      completedAt: null,
      updatedAt: nowIso(),
      meta: {
        ...(jobState.stages.trades.meta || {}),
        missingTradeRowsOutput: !hasTradeData,
        missingTradeQuoteOutput: !hasTradeQuoteData,
      },
    };
  }

  const quotePartitionDir = getQuotePartitionDir(runRoot, symbol, dayIso);
  const quoteMarker = readJsonFile(path.join(quotePartitionDir, '_SUCCESS.json'));
  if ((Number(quoteMarker?.partCount || 0) > 0) || partitionHasData(quotePartitionDir)) {
    jobState.stages.quotes = {
      ...jobState.stages.quotes,
      controlMode: 'requests',
      status: 'complete',
      startedAt: jobState.stages.quotes.startedAt || quoteMarker?.completedAt || jobState.createdAt,
      completedAt: quoteMarker?.completedAt || nowIso(),
      updatedAt: nowIso(),
      rowCount: Number(quoteMarker?.rowCount || jobState.stages.quotes.rowCount || 0),
      meta: {
        ...(jobState.stages.quotes.meta || {}),
        expirations: Array.isArray(quoteMarker?.expirations) ? quoteMarker.expirations : null,
        expirationCount: Number(quoteMarker?.expirationCount || 0),
        partCount: quoteMarker?.partCount || null,
        legacyFastPath: !quoteMarker,
      },
    };
  }

  if (greekMode === 'raw') {
    const rawPartitionDir = getRawGreeksPartitionDir(runRoot, symbol, dayIso);
    const rawMarker = readJsonFile(path.join(rawPartitionDir, '_SUCCESS.json'));
    const finalPartitionDir = getFinalGreeksPartitionDir(runRoot, symbol, dayIso);
    const finalMarker = readJsonFile(path.join(finalPartitionDir, '_SUCCESS.json'));
    if (((Number(rawMarker?.partCount || 0) > 0) || partitionHasData(rawPartitionDir))
      && ((Number(finalMarker?.partCount || 0) > 0) || partitionHasData(finalPartitionDir))) {
      jobState.stages.greeks = {
        ...jobState.stages.greeks,
        controlMode: 'requests',
        status: 'complete',
        startedAt: jobState.stages.greeks.startedAt || rawMarker?.completedAt || finalMarker?.completedAt || jobState.createdAt,
        completedAt: finalMarker?.completedAt || rawMarker?.completedAt || nowIso(),
        updatedAt: nowIso(),
        rowCount: Number(rawMarker?.rowCount || finalMarker?.rowCount || jobState.stages.greeks.rowCount || 0),
        meta: {
          ...(jobState.stages.greeks.meta || {}),
          rawRowCount: Number(rawMarker?.rowCount || 0),
          finalRowCount: Number(finalMarker?.rowCount || 0),
          partCount: finalMarker?.partCount || rawMarker?.partCount || null,
          legacyFastPath: !rawMarker || !finalMarker,
        },
      };
    }
  } else {
    const finalPartitionDir = getFinalGreeksPartitionDir(runRoot, symbol, dayIso);
    const finalMarker = readJsonFile(path.join(finalPartitionDir, '_SUCCESS.json'));
    if ((Number(finalMarker?.partCount || 0) > 0) || partitionHasData(finalPartitionDir)) {
      jobState.stages.greeks = {
        ...jobState.stages.greeks,
        controlMode: 'task',
        status: 'complete',
        startedAt: jobState.stages.greeks.startedAt || finalMarker?.completedAt || jobState.createdAt,
        completedAt: finalMarker?.completedAt || nowIso(),
        updatedAt: nowIso(),
        rowCount: Number(finalMarker?.rowCount || jobState.stages.greeks.rowCount || 0),
        meta: {
          ...(jobState.stages.greeks.meta || {}),
          finalRowCount: Number(finalMarker?.rowCount || 0),
          partCount: finalMarker?.partCount || null,
          legacyFastPath: !finalMarker,
        },
      };
    }
  }
  return refreshJobStatus(jobState);
}

async function ensureJobStates({
  runId,
  runRoot,
  jobs,
  indexGreeksSymbols,
}) {
  const { jobStateRoot, lockRoot, controlRoot } = getStatePaths(runRoot);
  ensureDir(jobStateRoot);
  ensureDir(lockRoot);
  ensureDir(controlRoot);
  for (const job of jobs) {
    const greekMode = indexGreeksSymbols.has(job.symbol) ? 'raw' : 'calculated';
    const filePath = getJobStatePath(runRoot, job.symbol, job.dayIso);
    let state = readJsonFile(filePath);
    if (!state) {
      state = buildInitialJobState({
        runId,
        runRoot,
        symbol: job.symbol,
        dayIso: job.dayIso,
        greekMode,
      });
    } else {
      state.runId = state.runId || runId;
      state.runRoot = runRoot;
      state.greekMode = greekMode;
      state.jobId = state.jobId || jobId(job.symbol, job.dayIso);
      state.stages = state.stages || {};
      state.stages.stock = {
        ...buildStageState('stock', null, 'requests'),
        ...(state.stages.stock || {}),
        controlMode: 'requests',
      };
      state.stages.trades = {
        ...buildStageState('trades', null, 'requests'),
        ...(state.stages.trades || {}),
        controlMode: 'requests',
      };
      state.stages.quotes = {
        ...buildStageState('quotes', null, 'requests'),
        ...(state.stages.quotes || {}),
        controlMode: 'requests',
      };
      state.stages.greeks = {
        ...buildStageState('greeks', greekMode, greekMode === 'raw' ? 'requests' : 'task'),
        ...(state.stages.greeks || {}),
        mode: greekMode,
        controlMode: greekMode === 'raw' ? 'requests' : 'task',
      };
    }
    reconcileStaleRunningStages(state);
    state = await inferExistingStageState(state);
    cleanupNonRunningStageLocks(state);
    writeJsonAtomic(filePath, state);
  }
}

function writeJobsReady(runRoot, payload = {}) {
  writeJsonAtomic(getStatePaths(runRoot).readyPath, {
    readyAt: nowIso(),
    ...payload,
  });
}

function readJobsReady(runRoot) {
  return readJsonFile(getStatePaths(runRoot).readyPath);
}

async function waitForJobStatesReady(runRoot, {
  timeoutMs = 300000,
  pollMs = 500,
  readyToken = process.env.PARQUET_READY_TOKEN || null,
} = {}) {
  const startedAt = Date.now();
  while (true) {
    const ready = readJobsReady(runRoot);
    if (ready && (!readyToken || ready.readyToken === readyToken)) return ready;
    if ((Date.now() - startedAt) >= timeoutMs) {
      throw new Error(`job_state_ready_timeout:${timeoutMs}:${readyToken || 'any'}`);
    }
    await sleep(pollMs);
  }
}

function clearRunStopRequest(runRoot) {
  const { stopPath } = getStatePaths(runRoot);
  fs.rmSync(stopPath, { recursive: true, force: true });
}

function requestRunStop(runRoot, reason = 'requested') {
  const { stopPath } = getStatePaths(runRoot);
  writeJsonAtomic(stopPath, {
    reason,
    requestedAt: nowIso(),
    pid: process.pid,
  });
}

function readRunStopRequest(runRoot) {
  return readJsonFile(getStatePaths(runRoot).stopPath);
}

function stageCanClaim(stage, maxAttempts) {
  if (!stage || stage.controlMode !== 'task') return false;
  if (stage.status === 'complete') return false;
  if (stage.status === 'running') return false;
  const attempts = Math.max(0, Math.trunc(Number(stage.attempts || 0)));
  return attempts < maxAttempts;
}

function computeComputeCandidate(state, maxAttempts) {
  if (state.greekMode !== 'calculated') return null;
  if (state.stages.stock.status !== 'complete' || state.stages.quotes.status !== 'complete') return null;
  if (!stageCanClaim(state.stages.greeks, maxAttempts)) return null;
  return { stageName: 'greeks', priority: 10 };
}

function getCandidateForRole(state, role, maxAttempts) {
  if (role === COMPUTE_ROLE) return computeComputeCandidate(state, maxAttempts);
  return null;
}

function acquireTaskLock(lockPath, metadata) {
  try {
    fs.writeFileSync(lockPath, `${JSON.stringify(metadata, null, 2)}\n`, {
      encoding: 'utf8',
      flag: 'wx',
    });
    return true;
  } catch (error) {
    if (error?.code === 'EEXIST') {
      cleanupStaleLock(lockPath);
      return false;
    }
    throw error;
  }
}

async function withJobStateLock(runRoot, symbol, dayIso, fn) {
  const lockPath = getJobUpdateLockPath(runRoot, symbol, dayIso);
  const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  while (true) {
    const locked = acquireTaskLock(lockPath, {
      pid: process.pid,
      token,
      symbol,
      dayIso,
      kind: 'job_state',
      claimedAt: nowIso(),
    });
    if (locked) break;
    await sleep(50);
  }
  try {
    return await fn();
  } finally {
    const metadata = readJsonFile(lockPath);
    if (!metadata?.token || metadata.token === token) {
      fs.rmSync(lockPath, { recursive: true, force: true });
    }
  }
}

async function withSchedulerLock(runRoot, fn) {
  const lockPath = getStatePaths(runRoot).schedulerLockPath;
  const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  while (true) {
    const locked = acquireTaskLock(lockPath, {
      pid: process.pid,
      token,
      kind: 'theta_request_scheduler',
      claimedAt: nowIso(),
    });
    if (locked) break;
    await sleep(50);
  }
  try {
    return await fn();
  } finally {
    const metadata = readJsonFile(lockPath);
    if (!metadata?.token || metadata.token === token) {
      fs.rmSync(lockPath, { recursive: true, force: true });
    }
  }
}

function clearStageClaim(stage) {
  stage.claimedBy = null;
  stage.claimedAt = null;
  return stage;
}

function stageLockIsLive(runRoot, symbol, dayIso, stageName) {
  const lockPath = getTaskLockPath(runRoot, symbol, dayIso, stageName);
  const metadata = readJsonFile(lockPath);
  if (!metadata?.pid) return false;
  if (!isPidAlive(metadata.pid)) {
    fs.rmSync(lockPath, { recursive: true, force: true });
    return false;
  }
  return true;
}

function reconcileStaleRunningStages(state) {
  let changed = false;
  for (const stageName of ['stock', 'quotes', 'greeks']) {
    const stage = state?.stages?.[stageName];
    if (!stage || stage.controlMode !== 'task' || stage.status !== 'running') continue;
    if (stageLockIsLive(state.runRoot, state.symbol, state.dayIso, stageName)) continue;
    stage.status = 'pending';
    stage.updatedAt = nowIso();
    stage.error = null;
    stage.completedAt = null;
    clearStageClaim(stage);
    changed = true;
  }
  if (changed) refreshJobStatus(state);
  return changed;
}

function cleanupNonRunningStageLocks(state) {
  let cleaned = false;
  for (const stageName of ['stock', 'quotes', 'greeks']) {
    const stage = state?.stages?.[stageName];
    if (!stage || stage.controlMode !== 'task' || stage.status === 'running') continue;
    const lockPath = getTaskLockPath(state.runRoot, state.symbol, state.dayIso, stageName);
    if (fs.existsSync(lockPath)) {
      cleaned = cleanupStaleLock(lockPath) || cleaned;
    }
  }
  return cleaned;
}

function getStageNameForRequestKind(kind) {
  if (kind === REQUEST_KIND_STOCK) return 'stock';
  if (kind === REQUEST_KIND_TRADES) return 'trades';
  if (kind === REQUEST_KIND_QUOTE || kind === REQUEST_KIND_QUOTE_FETCH || kind === REQUEST_KIND_QUOTE_PARSE) return 'quotes';
  if (kind === REQUEST_KIND_RAW_GREEKS) return 'greeks';
  throw new Error(`unknown_request_kind:${kind}`);
}

function buildInitialRequestState({
  runId,
  runRoot,
  job,
  kind,
  partIndex = 0,
  window = null,
  expirations = [],
  partitionDir = null,
  outputPath = null,
  secondaryPartitionDir = null,
  secondaryOutputPath = null,
  schedulerPriority = 50,
  estimatedCost = 1,
  meta = {},
}) {
  const requestId = buildRequestId(job.symbol, job.dayIso, kind, partIndex);
  return {
    runId,
    runRoot,
    requestId,
    jobId: job.jobId,
    fairnessKey: job.jobId,
    symbol: job.symbol,
    dayIso: job.dayIso,
    greekMode: job.greekMode,
    kind,
    stageName: getStageNameForRequestKind(kind),
    partIndex,
    window: window || null,
    expirations: Array.isArray(expirations) ? expirations.slice() : [],
    partitionDir,
    outputPath,
    secondaryPartitionDir,
    secondaryOutputPath,
    schedulerPriority: Math.max(0, Math.trunc(Number(schedulerPriority) || 0)),
    estimatedCost: Math.max(1, Math.trunc(Number(estimatedCost) || 1)),
    status: 'pending',
    attempts: 0,
    claimToken: null,
    claimedBy: null,
    claimedPid: null,
    claimedAt: null,
    startedAt: null,
    completedAt: null,
    updatedAt: nowIso(),
    error: null,
    rowCount: 0,
    elapsedMs: 0,
    meta: { ...meta },
  };
}

function requestHasOutput(request) {
  if (
    request.kind === REQUEST_KIND_STOCK
    || request.kind === REQUEST_KIND_QUOTE
    || request.kind === REQUEST_KIND_QUOTE_FETCH
    || request.kind === REQUEST_KIND_QUOTE_PARSE
  ) {
    return Boolean(request.outputPath && fs.existsSync(request.outputPath));
  }
  if (request.kind === REQUEST_KIND_TRADES) {
    const hasPrimary = Boolean(request.outputPath && fs.existsSync(request.outputPath));
    const hasSecondary = request.secondaryOutputPath
      ? Boolean(fs.existsSync(request.secondaryOutputPath))
      : true;
    return hasPrimary && hasSecondary;
  }
  if (request.kind === REQUEST_KIND_RAW_GREEKS) {
    return Boolean(
      request.outputPath && fs.existsSync(request.outputPath)
      && request.secondaryOutputPath && fs.existsSync(request.secondaryOutputPath),
    );
  }
  return false;
}

function mergePlannedRequestState(existing, planned, stageState) {
  const stageRecoveredComplete = stageState?.status === 'complete';
  const outputRecoveredComplete = requestHasOutput(planned);
  const recoveredComplete = stageRecoveredComplete || outputRecoveredComplete;
  if (!existing) {
    return {
      ...planned,
      status: recoveredComplete ? 'complete' : 'pending',
      completedAt: recoveredComplete ? (stageState?.completedAt || nowIso()) : null,
      updatedAt: nowIso(),
      rowCount: recoveredComplete && planned.kind === REQUEST_KIND_STOCK
        ? Number(stageState?.rowCount || 0)
        : 0,
      meta: {
        ...planned.meta,
        recoveredFromOutputs: recoveredComplete && !stageRecoveredComplete,
        recoveredFromStage: recoveredComplete && stageRecoveredComplete,
      },
    };
  }
  const resetForMissingOutput = existing.status === 'complete' && !stageRecoveredComplete && !outputRecoveredComplete;
  return {
    ...existing,
    runId: planned.runId,
    runRoot: planned.runRoot,
    requestId: planned.requestId,
    jobId: planned.jobId,
    fairnessKey: planned.fairnessKey,
    symbol: planned.symbol,
    dayIso: planned.dayIso,
    greekMode: planned.greekMode,
    kind: planned.kind,
    stageName: planned.stageName,
    partIndex: planned.partIndex,
    window: planned.window,
    expirations: planned.expirations,
    partitionDir: planned.partitionDir,
    outputPath: planned.outputPath,
    secondaryPartitionDir: planned.secondaryPartitionDir,
    secondaryOutputPath: planned.secondaryOutputPath,
    schedulerPriority: planned.schedulerPriority,
    estimatedCost: planned.estimatedCost,
    status: resetForMissingOutput ? 'pending' : existing.status,
    claimToken: resetForMissingOutput ? null : existing.claimToken,
    claimedBy: resetForMissingOutput ? null : existing.claimedBy,
    claimedPid: resetForMissingOutput ? null : existing.claimedPid,
    claimedAt: resetForMissingOutput ? null : existing.claimedAt,
    startedAt: resetForMissingOutput ? null : existing.startedAt,
    completedAt: resetForMissingOutput ? null : existing.completedAt,
    updatedAt: nowIso(),
    error: resetForMissingOutput ? null : existing.error,
    rowCount: resetForMissingOutput ? 0 : existing.rowCount,
    elapsedMs: resetForMissingOutput ? 0 : existing.elapsedMs,
    meta: {
      ...(existing.meta || {}),
      ...(planned.meta || {}),
      resetForMissingOutput,
    },
  };
}

async function loadSessionWindowForDay(sessionCache, dayIso, env = process.env, {
  runRoot = null,
} = {}) {
  if (sessionCache.has(dayIso)) return sessionCache.get(dayIso);
  if (runRoot) {
    const cached = readJsonFile(getSessionWindowCachePath(runRoot, dayIso));
    if (cached && Object.prototype.hasOwnProperty.call(cached, 'sessionWindow')) {
      sessionCache.set(dayIso, cached.sessionWindow);
      return cached.sessionWindow;
    }
  }
  const loaded = await fetchCalendarSessionWindow(dayIso, env).catch(() => null);
  sessionCache.set(dayIso, loaded);
  if (runRoot) {
    writeJsonAtomic(getSessionWindowCachePath(runRoot, dayIso), {
      dayIso,
      fetchedAt: nowIso(),
      sessionWindow: loaded,
    });
  }
  return loaded;
}

function listRequestStatesForJob(runRoot, targetJobId) {
  return listRequestStateFiles(runRoot)
    .filter((filePath) => path.basename(filePath).startsWith(`${targetJobId}__`))
    .map((filePath) => ({ filePath, state: readJsonFile(filePath) }))
    .filter((entry) => entry.state);
}

function getRequestEntriesForJobKind(runRoot, targetJobId, kind) {
  return listRequestStatesForJob(runRoot, targetJobId)
    .filter((entry) => entry.state.kind === kind);
}

async function ensureStockRequestForJobState(state) {
  if (state.stages.stock.controlMode !== 'requests') return;
  const planned = buildInitialRequestState({
    runId: state.runId,
    runRoot: state.runRoot,
    job: state,
    kind: REQUEST_KIND_STOCK,
    partIndex: 0,
    partitionDir: getStockPartitionDir(state.runRoot, state.symbol, state.dayIso),
    outputPath: path.join(getStockPartitionDir(state.runRoot, state.symbol, state.dayIso), 'part-000.parquet'),
    schedulerPriority: state.greekMode === 'calculated' ? 15 : 25,
    estimatedCost: 1,
  });
  const filePath = getRequestStatePath(state.runRoot, planned.requestId);
  const existing = readJsonFile(filePath);
  writeJsonAtomic(filePath, mergePlannedRequestState(existing, planned, state.stages.stock));
}

async function ensureTradeRequestsForJobState(state, {
  sessionCache,
  env = process.env,
}) {
  if (state.stages.trades.controlMode !== 'requests') return;
  const sessionWindow = await loadSessionWindowForDay(sessionCache, state.dayIso, env, {
    runRoot: state.runRoot,
  });
  const plan = resolveTradeRequestPlan({
    runRoot: state.runRoot,
    symbol: state.symbol,
    dayIso: state.dayIso,
    sessionWindow,
    env,
  });
  for (const request of plan.requests) {
    const planned = buildInitialRequestState({
      runId: state.runId,
      runRoot: state.runRoot,
      job: state,
      kind: REQUEST_KIND_TRADES,
      partIndex: request.partIndex,
      window: request.window,
      partitionDir: getTradePartitionDir(state.runRoot, state.symbol, state.dayIso),
      outputPath: request.filePath,
      secondaryPartitionDir: getTradeQuotePartitionDir(state.runRoot, state.symbol, state.dayIso),
      secondaryOutputPath: getPartitionPartPath(getTradeQuotePartitionDir(state.runRoot, state.symbol, state.dayIso), request.partIndex),
      schedulerPriority: request.schedulerPriority,
      estimatedCost: request.estimatedCost,
    });
    const filePath = getRequestStatePath(state.runRoot, planned.requestId);
    const existing = readJsonFile(filePath);
    writeJsonAtomic(filePath, mergePlannedRequestState(existing, planned, state.stages.trades));
  }
}

async function ensureQuoteRequestsForJobState(state, {
  sessionCache,
  env = process.env,
}) {
  if (state.stages.quotes.controlMode !== 'requests') return;
  const existingRequestEntries = listRequestStatesForJob(state.runRoot, state.jobId);
  const hasLegacyQuoteRequests = existingRequestEntries.some((entry) => entry.state.kind === REQUEST_KIND_QUOTE);
  const hasSplitQuoteRequests = existingRequestEntries.some((entry) => (
    entry.state.kind === REQUEST_KIND_QUOTE_FETCH || entry.state.kind === REQUEST_KIND_QUOTE_PARSE
  ));
  const useSpoolMode = hasSplitQuoteRequests || (parseQuoteUseSpool(env) && !hasLegacyQuoteRequests);
  const sessionWindow = await loadSessionWindowForDay(sessionCache, state.dayIso, env, {
    runRoot: state.runRoot,
  });
  const plan = resolveQuoteRequestPlan({
    runRoot: state.runRoot,
    symbol: state.symbol,
    dayIso: state.dayIso,
    sessionWindow,
    env,
  });
  for (const request of plan.requests) {
    if (useSpoolMode) {
      const fetchPlanned = buildInitialRequestState({
        runId: state.runId,
        runRoot: state.runRoot,
        job: state,
        kind: REQUEST_KIND_QUOTE_FETCH,
        partIndex: request.partIndex,
        window: request.window,
        partitionDir: getQuoteSpoolPartitionDir(state.runRoot, state.symbol, state.dayIso),
        outputPath: getQuoteSpoolPath(state.runRoot, state.symbol, state.dayIso, request.partIndex),
        schedulerPriority: request.schedulerPriority,
        estimatedCost: request.estimatedCost,
      });
      const fetchPath = getRequestStatePath(state.runRoot, fetchPlanned.requestId);
      const existingFetch = readJsonFile(fetchPath);
      writeJsonAtomic(fetchPath, mergePlannedRequestState(existingFetch, fetchPlanned, state.stages.quotes));

      const parsePlanned = buildInitialRequestState({
        runId: state.runId,
        runRoot: state.runRoot,
        job: state,
        kind: REQUEST_KIND_QUOTE_PARSE,
        partIndex: request.partIndex,
        window: request.window,
        partitionDir: getQuotePartitionDir(state.runRoot, state.symbol, state.dayIso),
        outputPath: request.filePath,
        schedulerPriority: request.schedulerPriority,
        estimatedCost: request.estimatedCost,
        meta: {
          sourceRequestId: fetchPlanned.requestId,
          sourcePath: fetchPlanned.outputPath,
        },
      });
      const parsePath = getRequestStatePath(state.runRoot, parsePlanned.requestId);
      const existingParse = readJsonFile(parsePath);
      writeJsonAtomic(parsePath, mergePlannedRequestState(existingParse, parsePlanned, state.stages.quotes));
      continue;
    }

    const planned = buildInitialRequestState({
      runId: state.runId,
      runRoot: state.runRoot,
      job: state,
      kind: REQUEST_KIND_QUOTE,
      partIndex: request.partIndex,
      window: request.window,
      partitionDir: getQuotePartitionDir(state.runRoot, state.symbol, state.dayIso),
      outputPath: request.filePath,
      schedulerPriority: request.schedulerPriority,
      estimatedCost: request.estimatedCost,
    });
    const filePath = getRequestStatePath(state.runRoot, planned.requestId);
    const existing = readJsonFile(filePath);
    writeJsonAtomic(filePath, mergePlannedRequestState(existing, planned, state.stages.quotes));
  }
}

async function ensureRawGreeksRequestsForJobState(state, {
  sessionCache,
  env = process.env,
}) {
  if (state.greekMode !== 'raw' || state.stages.greeks.controlMode !== 'requests') return false;
  if (state.stages.greeks.status === 'complete') return false;
  if (state.stages.quotes.status !== 'complete') return false;
  let expirations = Array.isArray(state?.stages?.quotes?.meta?.expirations)
    ? state.stages.quotes.meta.expirations
    : [];
  if (!expirations.length) {
    const quotePartition = await probeQuotePartition(getQuotePartitionDir(state.runRoot, state.symbol, state.dayIso));
    expirations = Array.isArray(quotePartition?.expirations) ? quotePartition.expirations : [];
  }
  if (!expirations.length) return false;
  const sessionWindow = await loadSessionWindowForDay(sessionCache, state.dayIso, env, {
    runRoot: state.runRoot,
  });
  const plan = resolveRawGreeksRequestPlan({
    runRoot: state.runRoot,
    symbol: state.symbol,
    dayIso: state.dayIso,
    expirations,
    sessionWindow,
    env,
  });
  let created = false;
  for (const request of plan.requests) {
    const planned = buildInitialRequestState({
      runId: state.runId,
      runRoot: state.runRoot,
      job: state,
      kind: REQUEST_KIND_RAW_GREEKS,
      partIndex: request.partIndex,
      window: request.window,
      expirations: request.expirations,
      partitionDir: getRawGreeksPartitionDir(state.runRoot, state.symbol, state.dayIso),
      outputPath: request.rawPath,
      secondaryPartitionDir: getFinalGreeksPartitionDir(state.runRoot, state.symbol, state.dayIso),
      secondaryOutputPath: request.finalPath,
      schedulerPriority: request.schedulerPriority,
      estimatedCost: request.estimatedCost,
      meta: {
        taskMode: request.taskMode,
        mode: plan.mode,
      },
    });
    const filePath = getRequestStatePath(state.runRoot, planned.requestId);
    const existing = readJsonFile(filePath);
    if (!existing) created = true;
    writeJsonAtomic(filePath, mergePlannedRequestState(existing, planned, state.stages.greeks));
  }
  return created;
}

async function ensureRequestStates({
  runRoot,
  jobs,
  env = process.env,
}) {
  const { requestStateRoot } = getStatePaths(runRoot);
  ensureDir(requestStateRoot);
  const sessionCache = new Map();
  const uniqueDays = Array.from(new Set(
    jobs
      .map((job) => String(job?.dayIso || '').trim())
      .filter(Boolean),
  )).sort();
  for (const dayIso of uniqueDays) {
    await loadSessionWindowForDay(sessionCache, dayIso, env, { runRoot });
  }
  for (const job of jobs) {
    const state = readJsonFile(getJobStatePath(runRoot, job.symbol, job.dayIso));
    if (!state) continue;
    await ensureStockRequestForJobState(state);
    await ensureTradeRequestsForJobState(state, { sessionCache, env });
    await ensureQuoteRequestsForJobState(state, { sessionCache, env });
    await ensureRawGreeksRequestsForJobState(state, { sessionCache, env });
  }
}

function requestKindToStageName(kind) {
  if (kind === REQUEST_KIND_STOCK) return 'stock';
  if (kind === REQUEST_KIND_TRADES) return 'trades';
  if (
    kind === REQUEST_KIND_QUOTE
    || kind === REQUEST_KIND_QUOTE_FETCH
    || kind === REQUEST_KIND_QUOTE_PARSE
  ) {
    return 'quotes';
  }
  if (kind === REQUEST_KIND_RAW_GREEKS) return 'greeks';
  return null;
}

function pruneCompleteRequestStatesForCompletedStages(runRoot) {
  const requestFiles = listRequestStateFiles(runRoot);
  if (requestFiles.length === 0) return { removed: 0 };
  const jobStatesById = new Map();
  for (const filePath of listJobStateFiles(runRoot)) {
    const state = readJsonFile(filePath);
    if (!state) continue;
    jobStatesById.set(state.jobId, state);
  }
  let removed = 0;
  for (const filePath of requestFiles) {
    const requestState = readJsonFile(filePath);
    if (!requestState || requestState.status !== 'complete') continue;
    const stageName = requestKindToStageName(requestState.kind);
    if (!stageName) continue;
    const jobState = jobStatesById.get(requestState.jobId);
    if (!jobState) continue;
    const stage = jobState?.stages?.[stageName];
    if (stage?.status !== 'complete') continue;
    fs.rmSync(filePath, { force: true });
    removed += 1;
  }
  return { removed };
}

function reconcileStaleRunningRequestState(requestState) {
  if (!requestState || requestState.status !== 'running') return false;
  if (requestState.claimedPid && isPidAlive(requestState.claimedPid)) return false;
  requestState.status = 'pending';
  requestState.updatedAt = nowIso();
  requestState.claimToken = null;
  requestState.claimedBy = null;
  requestState.claimedPid = null;
  requestState.claimedAt = null;
  requestState.error = null;
  requestState.completedAt = null;
  return true;
}

function getThetaCooldownStatePath(runRoot) {
  return path.join(path.dirname(path.dirname(runRoot)), 'theta-rate-limit-state.json');
}

function readThetaCooldownState(runRoot) {
  const filePath = getThetaCooldownStatePath(runRoot);
  if (!fs.existsSync(filePath)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    const untilMs = Number(parsed?.untilMs || 0);
    if (!Number.isFinite(untilMs) || untilMs <= 0) return null;
    if (untilMs <= Date.now()) return null;
    return {
      filePath,
      untilMs,
      reason: parsed?.reason || null,
      updatedAt: parsed?.updatedAt || null,
    };
  } catch {
    return null;
  }
}

function isRetryableThetaMessage(message) {
  const normalized = String(message || '');
  if (!normalized) return false;
  if (/thetadata_request_failed:429/.test(normalized)) return true;
  if (/thetadata_request_timeout:/.test(normalized)) return true;
  if (/thetadata_request_idle_timeout:/.test(normalized)) return true;
  if (/fetch failed/i.test(normalized)) return true;
  if (/ECONNRESET|EPIPE|socket hang up|UND_ERR|ETIMEDOUT|ECONNREFUSED/i.test(normalized)) return true;
  return false;
}

function isRetryableBurstThetaMessage(message) {
  const normalized = String(message || '');
  if (!normalized) return false;
  if (/thetadata_request_failed:429/.test(normalized)) return true;
  if (/thetadata_request_idle_timeout:/.test(normalized)) return true;
  if (/ECONNRESET|socket hang up|UND_ERR|ETIMEDOUT|fetch failed/i.test(normalized)) return true;
  return false;
}

function buildRequestMapByJob(jobStates) {
  return new Map(jobStates.map((state) => [state.jobId, state]));
}

function computeDownloadRequestPriority(requestState, jobState, {
  computeReadyCount,
  computeWorkers,
}) {
  const starvingCompute = computeReadyCount < (2 * Math.max(1, computeWorkers));
  let dynamicPriority = 10;
  if (starvingCompute && jobState?.greekMode === 'calculated') {
    if (
      (requestState.kind === REQUEST_KIND_QUOTE || requestState.kind === REQUEST_KIND_QUOTE_FETCH)
      && jobState?.stages?.stock?.status === 'complete'
    ) {
      dynamicPriority = 0;
    } else if (requestState.kind === REQUEST_KIND_STOCK) {
      dynamicPriority = 1;
    } else if (requestState.kind === REQUEST_KIND_TRADES) {
      dynamicPriority = 2;
    } else if (requestState.kind === REQUEST_KIND_RAW_GREEKS) {
      dynamicPriority = 3;
    }
  } else if (requestState.kind === REQUEST_KIND_QUOTE || requestState.kind === REQUEST_KIND_QUOTE_FETCH) {
    dynamicPriority = 3;
  } else if (requestState.kind === REQUEST_KIND_TRADES) {
    dynamicPriority = 4;
  } else if (requestState.kind === REQUEST_KIND_RAW_GREEKS) {
    dynamicPriority = 5;
  } else if (requestState.kind === REQUEST_KIND_STOCK) {
    dynamicPriority = 8;
  }
  return {
    starvingCompute,
    dynamicPriority,
    estimatedCost: Math.max(1, Math.trunc(Number(requestState.estimatedCost || 1))),
    schedulerPriority: Math.max(0, Math.trunc(Number(requestState.schedulerPriority || 0))),
  };
}

function buildDownloadSchedulerSnapshot(runRoot, jobStates, requestStates, {
  maxAttempts = normalizeStageMaxAttempts(process.env),
  env = process.env,
} = {}) {
  const activeTarget = parseThetaActiveTarget(env);
  const configuredQueuedTarget = parseThetaQueuedTarget(env);
  const perJobLimit = parseThetaPerJobLimit(env);
  const perJobBurstLimit = parseThetaPerJobBurstLimit(env);
  const computeWorkers = parseComputeWorkerTotal(env);
  const cooldown = readThetaCooldownState(runRoot);
  const nowMs = Date.now();
  const thetaRequestStates = requestStates.filter((request) => isThetaDownloadRequestKind(request.kind));
  const recent429Count = thetaRequestStates.filter((request) => {
    const failureAt = Date.parse(request?.meta?.lastFailureAt || '');
    return Number.isFinite(failureAt)
      && (nowMs - failureAt) <= THETA_RETRY_WINDOW_MS
      && /thetadata_request_failed:429/.test(String(request?.meta?.lastFailureError || ''));
  }).length;
  const recentRetryCount = thetaRequestStates.filter((request) => {
    const failureAt = Date.parse(request?.meta?.lastFailureAt || '');
    return Number.isFinite(failureAt)
      && (nowMs - failureAt) <= THETA_RETRY_WINDOW_MS
      && isRetryableThetaMessage(request?.meta?.lastFailureError || '');
  }).length;
  const burstRetryCount = thetaRequestStates.filter((request) => {
    const failureAt = Date.parse(request?.meta?.lastFailureAt || '');
    return Number.isFinite(failureAt)
      && (nowMs - failureAt) <= THETA_RETRY_WINDOW_MS
      && isRetryableBurstThetaMessage(request?.meta?.lastFailureError || '');
  }).length;
  const effectiveQueuedTarget = (cooldown || recent429Count > 0 || burstRetryCount >= 2) ? 0 : configuredQueuedTarget;
  const outstandingTarget = activeTarget + effectiveQueuedTarget;
  const runningRequests = thetaRequestStates.filter((request) => request.status === 'running');
  const runningByJob = new Map();
  runningRequests.forEach((request) => {
    runningByJob.set(request.fairnessKey, Number(runningByJob.get(request.fairnessKey) || 0) + 1);
  });
  const readyRequests = thetaRequestStates.filter((request) => {
    if (request.status === 'complete' || request.status === 'running') return false;
    return Number(request.attempts || 0) < maxAttempts;
  });
  const readyDistinctJobs = new Set(readyRequests.map((request) => request.fairnessKey));
  const burstAllowed = readyDistinctJobs.size < activeTarget;
  const computeReadyCount = jobStates.reduce((count, state) => {
    return count + (computeComputeCandidate(state, maxAttempts) ? 1 : 0);
  }, 0);
  const requestMapByJob = buildRequestMapByJob(jobStates);
  const claimableRequests = [];
  let fairnessBlocked = 0;
  for (const requestState of readyRequests) {
    const runningForJob = Number(runningByJob.get(requestState.fairnessKey) || 0);
    const allowedLimit = burstAllowed ? perJobBurstLimit : perJobLimit;
    if (runningForJob >= allowedLimit) {
      fairnessBlocked += 1;
      continue;
    }
    const jobState = requestMapByJob.get(requestState.jobId) || null;
    const priority = computeDownloadRequestPriority(requestState, jobState, {
      computeReadyCount,
      computeWorkers,
    });
    claimableRequests.push({
      requestState,
      priority,
      jobState,
    });
  }
  claimableRequests.sort((left, right) => {
    if (left.priority.dynamicPriority !== right.priority.dynamicPriority) {
      return left.priority.dynamicPriority - right.priority.dynamicPriority;
    }
    const preferLargerWindows = (
      left.requestState.kind !== REQUEST_KIND_STOCK
      || right.requestState.kind !== REQUEST_KIND_STOCK
    );
    if (left.priority.estimatedCost !== right.priority.estimatedCost) {
      return preferLargerWindows
        ? (right.priority.estimatedCost - left.priority.estimatedCost)
        : (left.priority.estimatedCost - right.priority.estimatedCost);
    }
    if (left.priority.schedulerPriority !== right.priority.schedulerPriority) {
      return left.priority.schedulerPriority - right.priority.schedulerPriority;
    }
    if (left.requestState.schedulerPriority !== right.requestState.schedulerPriority) {
      return left.requestState.schedulerPriority - right.requestState.schedulerPriority;
    }
    if (left.requestState.estimatedCost !== right.requestState.estimatedCost) {
      return preferLargerWindows
        ? (right.requestState.estimatedCost - left.requestState.estimatedCost)
        : (left.requestState.estimatedCost - right.requestState.estimatedCost);
    }
    return left.requestState.requestId.localeCompare(right.requestState.requestId);
  });
  const runningRequestsByKind = {
    stock: runningRequests.filter((request) => request.kind === REQUEST_KIND_STOCK).length,
    trades: runningRequests.filter((request) => request.kind === REQUEST_KIND_TRADES).length,
    quote: runningRequests.filter((request) => (
      request.kind === REQUEST_KIND_QUOTE || request.kind === REQUEST_KIND_QUOTE_FETCH
    )).length,
    raw_greeks: runningRequests.filter((request) => request.kind === REQUEST_KIND_RAW_GREEKS).length,
  };
  const readyRequestsByKind = {
    stock: readyRequests.filter((request) => request.kind === REQUEST_KIND_STOCK).length,
    trades: readyRequests.filter((request) => request.kind === REQUEST_KIND_TRADES).length,
    quote: readyRequests.filter((request) => (
      request.kind === REQUEST_KIND_QUOTE || request.kind === REQUEST_KIND_QUOTE_FETCH
    )).length,
    raw_greeks: readyRequests.filter((request) => request.kind === REQUEST_KIND_RAW_GREEKS).length,
  };
  const thetaActiveRequests = Math.min(activeTarget, runningRequests.length);
  const thetaQueuedRequests = Math.max(0, runningRequests.length - activeTarget);
  const requestCountsByJob = new Map();
  thetaRequestStates
    .filter((request) => request.status !== 'complete')
    .forEach((request) => {
      requestCountsByJob.set(request.fairnessKey, Number(requestCountsByJob.get(request.fairnessKey) || 0) + 1);
    });
  const fairnessCapForPotential = burstAllowed ? perJobBurstLimit : perJobLimit;
  const thetaMaxPossibleRequestsNow = Array.from(requestCountsByJob.values()).reduce((sum, count) => {
    return sum + Math.min(fairnessCapForPotential, Math.max(0, Math.trunc(Number(count || 0))));
  }, 0);
  const thetaFullSaturationPossible = thetaMaxPossibleRequestsNow >= activeTarget;
  let thetaPotentialDegradedReason = null;
  const hasBacklog = readyRequests.length > 0 || runningRequests.length > 0;
  if (hasBacklog && thetaActiveRequests < activeTarget && thetaFullSaturationPossible) {
    if (cooldown) {
      thetaPotentialDegradedReason = 'cooldown_backoff';
    } else if (recentRetryCount > 0 && effectiveQueuedTarget === 0) {
      thetaPotentialDegradedReason = 'repeated_theta_failures';
    } else if (readyRequests.length === 0) {
      thetaPotentialDegradedReason = 'no_ready_requests';
    } else if (fairnessBlocked > 0 && claimableRequests.length === 0) {
      thetaPotentialDegradedReason = 'per_job_fairness_throttling';
    } else if (runningRequests.length === 0 && claimableRequests.length > 0) {
      thetaPotentialDegradedReason = 'lock_contention_or_claim_stall';
    } else {
      thetaPotentialDegradedReason = 'request_planning_gap';
    }
  }
  return {
    activeTarget,
    configuredQueuedTarget,
    effectiveQueuedTarget,
    outstandingTarget,
    thetaActiveRequests,
    thetaQueuedRequests,
    thetaDistinctJobsInFlight: new Set(runningRequests.map((request) => request.fairnessKey)).size,
    thetaUtilizationPct: Math.max(0, Math.min(100, Math.round((thetaActiveRequests / Math.max(1, activeTarget)) * 100))),
    thetaRecent429Count: recent429Count,
    thetaRecentRetryCount: recentRetryCount,
    thetaCooldownActive: Boolean(cooldown),
    thetaFullSaturationPossible,
    thetaMaxPossibleRequestsNow,
    thetaPotentialDegradedReason,
    readyRequestsByKind,
    runningRequestsByKind,
    readyRequests,
    runningRequests,
    claimableRequests,
    fairnessBlocked,
    computeReadyCount,
  };
}

async function summarizeRequestControlledStage(state, stageName, requestEntries, env = process.env) {
  const stage = state.stages[stageName];
  const requests = requestEntries.map((entry) => entry.state);
  if (requests.length === 0) {
    return {
      ...stage,
      controlMode: 'requests',
      attempts: 0,
      claimedBy: null,
      claimedAt: null,
      meta: {
        ...(stage.meta || {}),
        requestCount: 0,
        completeRequests: 0,
        runningRequests: 0,
        failedRequests: 0,
        pendingRequests: 0,
      },
    };
  }
  const completeRequests = requests.filter((request) => request.status === 'complete');
  const runningRequests = requests.filter((request) => request.status === 'running');
  const failedRequests = requests.filter((request) => request.status === 'failed');
  const pendingRequests = requests.filter((request) => request.status === 'pending');
  const attempts = requests.reduce((sum, request) => sum + Number(request.attempts || 0), 0);
  const rowCountSourceRequests = stageName === 'quotes'
    ? requests.filter((request) => request.kind === REQUEST_KIND_QUOTE || request.kind === REQUEST_KIND_QUOTE_PARSE)
    : requests;
  const rowCount = rowCountSourceRequests.reduce((sum, request) => sum + Number(request.rowCount || 0), 0);
  const elapsedMs = requests.reduce((sum, request) => sum + Number(request.elapsedMs || 0), 0);
  const startedAt = requests
    .map((request) => request.startedAt)
    .filter(Boolean)
    .sort()[0] || stage.startedAt || null;
  const completedAt = completeRequests.length === requests.length
    ? completeRequests.map((request) => request.completedAt).filter(Boolean).sort().slice(-1)[0] || nowIso()
    : null;
  const meta = {
    ...(stage.meta || {}),
    requestCount: requests.length,
    completeRequests: completeRequests.length,
    runningRequests: runningRequests.length,
    failedRequests: failedRequests.length,
    pendingRequests: pendingRequests.length,
  };

  if (stageName === 'stock') {
    const partitionDir = getStockPartitionDir(state.runRoot, state.symbol, state.dayIso);
    const partCount = listParquetPartFiles(partitionDir).length;
    meta.partCount = partCount;
    if (completeRequests.length === requests.length) {
      writePartitionSuccessMarker(partitionDir, {
        stage: 'stock',
        rowCount,
        partCount,
      });
    }
  } else if (stageName === 'trades') {
    const partitionDir = getTradePartitionDir(state.runRoot, state.symbol, state.dayIso);
    const tradeQuotePartitionDir = getTradeQuotePartitionDir(state.runRoot, state.symbol, state.dayIso);
    const partCount = listParquetPartFiles(partitionDir).length;
    const tradeQuotePartCount = listParquetPartFiles(tradeQuotePartitionDir).length;
    const tradeQuoteRowCount = requests.reduce((sum, request) => sum + Number(request?.meta?.tradeQuoteRowCount || 0), 0);
    meta.partCount = partCount;
    meta.tradeQuotePartCount = tradeQuotePartCount;
    meta.tradeQuoteRowCount = tradeQuoteRowCount;
    if (completeRequests.length === requests.length) {
      writePartitionSuccessMarker(partitionDir, {
        stage: 'trades',
        rowCount,
        partCount,
      });
      writePartitionSuccessMarker(tradeQuotePartitionDir, {
        stage: 'trade_quotes',
        rowCount: tradeQuoteRowCount,
        partCount: tradeQuotePartCount,
      });
    }
  } else if (stageName === 'quotes') {
    const partitionDir = getQuotePartitionDir(state.runRoot, state.symbol, state.dayIso);
    const expirations = new Set();
    rowCountSourceRequests.forEach((request) => {
      (request.meta?.expirations || request.expirations || []).forEach((expiration) => expirations.add(expiration));
    });
    if (completeRequests.length === requests.length && expirations.size === 0) {
      const probe = await probeQuotePartition(partitionDir);
      (probe?.expirations || []).forEach((expiration) => expirations.add(expiration));
    }
    const partCount = listParquetPartFiles(partitionDir).length;
    meta.partCount = partCount;
    meta.expirations = Array.from(expirations).sort();
    meta.expirationCount = meta.expirations.length;
    meta.fetchRequestCount = requests.filter((request) => request.kind === REQUEST_KIND_QUOTE_FETCH).length;
    meta.parseRequestCount = requests.filter((request) => request.kind === REQUEST_KIND_QUOTE_PARSE).length;
    if (completeRequests.length === requests.length) {
      writePartitionSuccessMarker(partitionDir, {
        stage: 'quotes',
        rowCount,
        expirationCount: meta.expirationCount,
        expirations: meta.expirations,
        partCount,
      });
    }
  } else if (stageName === 'greeks' && state.greekMode === 'raw') {
    const rawPartitionDir = getRawGreeksPartitionDir(state.runRoot, state.symbol, state.dayIso);
    const finalPartitionDir = getFinalGreeksPartitionDir(state.runRoot, state.symbol, state.dayIso);
    const rawPartCount = listParquetPartFiles(rawPartitionDir).length;
    const finalPartCount = listParquetPartFiles(finalPartitionDir).length;
    meta.partCount = finalPartCount;
    meta.rawPartCount = rawPartCount;
    meta.finalPartCount = finalPartCount;
    meta.finalRowCount = rowCount;
    meta.rawRowCount = rowCount;
    if (completeRequests.length === requests.length) {
      writePartitionSuccessMarker(rawPartitionDir, {
        stage: 'raw_greeks',
        rowCount,
        partCount: rawPartCount,
      });
      writePartitionSuccessMarker(finalPartitionDir, {
        stage: 'final_greeks_raw',
        rowCount,
        partCount: finalPartCount,
      });
    }
  }

  let status = 'pending';
  if (completeRequests.length === requests.length) {
    status = 'complete';
  } else if (runningRequests.length > 0) {
    status = 'running';
  } else if (failedRequests.length > 0) {
    status = 'failed';
  }

  return {
    ...stage,
    controlMode: 'requests',
    status,
    attempts,
    claimedBy: runningRequests.length === 1 ? runningRequests[0].claimedBy : null,
    claimedAt: runningRequests.length === 1 ? runningRequests[0].claimedAt : null,
    startedAt,
    completedAt,
    updatedAt: nowIso(),
    error: failedRequests[0]?.error || null,
    rowCount,
    elapsedMs,
    meta,
  };
}

async function syncRequestControlledStagesForJob(runRoot, symbol, dayIso, {
  maxAttempts = normalizeStageMaxAttempts(process.env),
  env = process.env,
} = {}) {
  return withJobStateLock(runRoot, symbol, dayIso, async () => {
    const statePath = getJobStatePath(runRoot, symbol, dayIso);
    const state = readJsonFile(statePath);
    if (!state) return null;
    const requestEntries = listRequestStatesForJob(runRoot, state.jobId);
    if (state.stages.stock.controlMode === 'requests') {
      state.stages.stock = await summarizeRequestControlledStage(
        state,
        'stock',
        requestEntries.filter((entry) => entry.state.kind === REQUEST_KIND_STOCK),
        env,
      );
    }
    if (state.stages.trades.controlMode === 'requests') {
      state.stages.trades = await summarizeRequestControlledStage(
        state,
        'trades',
        requestEntries.filter((entry) => entry.state.kind === REQUEST_KIND_TRADES),
        env,
      );
    }
    if (state.stages.quotes.controlMode === 'requests') {
      state.stages.quotes = await summarizeRequestControlledStage(
        state,
        'quotes',
        requestEntries.filter((entry) => (
          entry.state.kind === REQUEST_KIND_QUOTE
          || entry.state.kind === REQUEST_KIND_QUOTE_FETCH
          || entry.state.kind === REQUEST_KIND_QUOTE_PARSE
        )),
        env,
      );
    }
    if (state.greekMode === 'raw' && state.stages.greeks.controlMode === 'requests') {
      state.stages.greeks = await summarizeRequestControlledStage(
        state,
        'greeks',
        requestEntries.filter((entry) => entry.state.kind === REQUEST_KIND_RAW_GREEKS),
        env,
      );
    } else if (state.greekMode === 'calculated') {
      const candidate = getCandidateForRole(state, COMPUTE_ROLE, maxAttempts);
      if (state.stages.greeks.controlMode === 'task' && state.stages.greeks.status !== 'complete' && !candidate) {
        state.stages.greeks.status = state.stages.greeks.status === 'running' ? 'running' : state.stages.greeks.status;
      }
    }
    refreshJobStatus(state);
    writeJsonAtomic(statePath, state);
    return state;
  });
}

function isQuoteParseRequestReady(requestState, requestStatesById) {
  if (requestState.kind !== REQUEST_KIND_QUOTE_PARSE) return false;
  const sourceRequestId = requestState?.meta?.sourceRequestId || null;
  if (!sourceRequestId) {
    return Boolean(requestState?.meta?.sourcePath && fs.existsSync(requestState.meta.sourcePath));
  }
  const sourceState = requestStatesById.get(sourceRequestId);
  if (!sourceState) {
    return Boolean(requestState?.meta?.sourcePath && fs.existsSync(requestState.meta.sourcePath));
  }
  if (sourceState.status === 'complete') return true;
  return false;
}

async function claimNextComputeRequest({
  runRoot,
  workerId,
  maxAttempts = normalizeStageMaxAttempts(process.env),
  env = process.env,
} = {}) {
  if (readRunStopRequest(runRoot)) return null;
  return withSchedulerLock(runRoot, async () => {
    const requestFiles = listRequestStateFiles(runRoot);
    const requestStates = [];
    for (const filePath of requestFiles) {
      const state = readJsonFile(filePath);
      if (!state) continue;
      if (reconcileStaleRunningRequestState(state)) {
        writeJsonAtomic(filePath, state);
      }
      requestStates.push(state);
    }
    const requestStatesById = new Map(requestStates.map((state) => [state.requestId, state]));
    const candidates = requestStates
      .filter((state) => isComputeRequestKind(state.kind))
      .filter((state) => state.status !== 'complete' && state.status !== 'running')
      .filter((state) => Number(state.attempts || 0) < maxAttempts)
      .filter((state) => isQuoteParseRequestReady(state, requestStatesById))
      .sort((left, right) => {
        const leftCost = Math.max(1, Math.trunc(Number(left.estimatedCost || 1)));
        const rightCost = Math.max(1, Math.trunc(Number(right.estimatedCost || 1)));
        if (leftCost !== rightCost) return leftCost - rightCost;
        const leftPriority = Math.max(0, Math.trunc(Number(left.schedulerPriority || 0)));
        const rightPriority = Math.max(0, Math.trunc(Number(right.schedulerPriority || 0)));
        if (leftPriority !== rightPriority) return leftPriority - rightPriority;
        return left.requestId.localeCompare(right.requestId);
      });
    const next = candidates[0];
    if (!next) return null;
    const requestPath = getRequestStatePath(runRoot, next.requestId);
    const latest = readJsonFile(requestPath);
    if (!latest || latest.status === 'complete' || latest.status === 'running' || Number(latest.attempts || 0) >= maxAttempts) {
      return null;
    }
    if (!isQuoteParseRequestReady(latest, requestStatesById)) {
      return null;
    }
    const claimedAt = nowIso();
    const claimToken = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    latest.status = 'running';
    latest.attempts = Math.max(0, Math.trunc(Number(latest.attempts || 0))) + 1;
    latest.claimToken = claimToken;
    latest.claimedBy = workerId;
    latest.claimedPid = process.pid;
    latest.claimedAt = claimedAt;
    latest.startedAt = latest.startedAt || claimedAt;
    latest.updatedAt = claimedAt;
    latest.error = null;
    writeJsonAtomic(requestPath, latest);
    await syncRequestControlledStagesForJob(runRoot, latest.symbol, latest.dayIso, {
      maxAttempts,
      env,
    });
    return {
      requestPath,
      token: claimToken,
      request: latest,
    };
  });
}

async function claimNextTask({
  runRoot,
  role,
  workerId,
  maxAttempts = normalizeStageMaxAttempts(process.env),
  env = process.env,
}) {
  if (role === COMPUTE_ROLE) {
    const computeRequestClaim = await claimNextComputeRequest({
      runRoot,
      workerId,
      maxAttempts,
      env,
    });
    if (computeRequestClaim) return computeRequestClaim;
  }
  if (readRunStopRequest(runRoot)) return null;
  const files = listJobStateFiles(runRoot);
  for (const filePath of files) {
    const current = readJsonFile(filePath);
    if (!current) continue;
    if (reconcileStaleRunningStages(current)) {
      writeJsonAtomic(filePath, current);
    }
    cleanupNonRunningStageLocks(current);
    const candidate = getCandidateForRole(current, role, maxAttempts);
    if (!candidate) continue;
    const lockPath = getTaskLockPath(runRoot, current.symbol, current.dayIso, candidate.stageName);
    const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    const claimedAt = nowIso();
    const locked = acquireTaskLock(lockPath, {
      pid: process.pid,
      role,
      workerId,
      stageName: candidate.stageName,
      symbol: current.symbol,
      dayIso: current.dayIso,
      token,
      claimedAt,
    });
    if (!locked) continue;
    const latest = await withJobStateLock(runRoot, current.symbol, current.dayIso, async () => {
      const fresh = readJsonFile(filePath);
      const refreshedCandidate = fresh ? getCandidateForRole(fresh, role, maxAttempts) : null;
      if (!fresh || !refreshedCandidate || refreshedCandidate.stageName !== candidate.stageName) {
        return null;
      }
      const stage = fresh.stages[candidate.stageName];
      stage.status = 'running';
      stage.attempts = Math.max(0, Math.trunc(Number(stage.attempts || 0))) + 1;
      stage.claimedBy = workerId;
      stage.claimedAt = claimedAt;
      stage.startedAt = stage.startedAt || claimedAt;
      stage.updatedAt = claimedAt;
      stage.error = null;
      fresh.status = 'running';
      fresh.updatedAt = claimedAt;
      writeJsonAtomic(filePath, fresh);
      return fresh;
    });
    if (!latest) {
      fs.rmSync(lockPath, { recursive: true, force: true });
      continue;
    }
    return {
      token,
      lockPath,
      statePath: filePath,
      stageName: candidate.stageName,
      job: latest,
    };
  }
  return null;
}

async function claimNextDownloadRequest({
  runRoot,
  workerId,
  maxAttempts = normalizeStageMaxAttempts(process.env),
  env = process.env,
}) {
  if (readRunStopRequest(runRoot)) return null;
  return withSchedulerLock(runRoot, async () => {
    const requestFiles = listRequestStateFiles(runRoot);
    for (const filePath of requestFiles) {
      const state = readJsonFile(filePath);
      if (!state) continue;
      if (reconcileStaleRunningRequestState(state)) {
        writeJsonAtomic(filePath, state);
      }
    }
    const jobStates = [];
    for (const filePath of listJobStateFiles(runRoot)) {
      const state = readJsonFile(filePath);
      if (!state) continue;
      if (reconcileStaleRunningStages(state)) {
        writeJsonAtomic(filePath, state);
      }
      cleanupNonRunningStageLocks(state);
      jobStates.push(state);
    }
    const requestStates = listRequestStateFiles(runRoot)
      .map((filePath) => readJsonFile(filePath))
      .filter(Boolean);
    const snapshot = buildDownloadSchedulerSnapshot(runRoot, jobStates, requestStates, {
      maxAttempts,
      env,
    });
    if (snapshot.runningRequests.length >= snapshot.outstandingTarget) {
      return null;
    }
    const workerLane = resolveDownloadWorkerLane(env);
    const heavySymbols = parseHeavyDownloadSymbols(env);
    const laneCandidates = snapshot.claimableRequests.filter((entry) => (
      requestMatchesDownloadWorkerLane(entry.requestState, heavySymbols, workerLane)
    ));
    const next = laneCandidates[0] || snapshot.claimableRequests[0];
    if (!next) return null;
    const requestPath = getRequestStatePath(runRoot, next.requestState.requestId);
    const latest = readJsonFile(requestPath);
    if (!latest || latest.status === 'complete' || latest.status === 'running' || Number(latest.attempts || 0) >= maxAttempts) {
      return null;
    }
    const claimedAt = nowIso();
    const claimToken = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
    latest.status = 'running';
    latest.attempts = Math.max(0, Math.trunc(Number(latest.attempts || 0))) + 1;
    latest.claimToken = claimToken;
    latest.claimedBy = workerId;
    latest.claimedPid = process.pid;
    latest.claimedAt = claimedAt;
    latest.startedAt = latest.startedAt || claimedAt;
    latest.updatedAt = claimedAt;
    latest.error = null;
    writeJsonAtomic(requestPath, latest);
    await syncRequestControlledStagesForJob(runRoot, latest.symbol, latest.dayIso, {
      maxAttempts,
      env,
    });
    return {
      requestPath,
      token: claimToken,
      request: latest,
    };
  });
}

async function completeTask(claim, {
  rowCount = null,
  elapsedMs = 0,
  meta = {},
} = {}) {
  const lockMetadata = readJsonFile(claim.lockPath);
  if (!lockMetadata?.token || lockMetadata.token !== claim.token) return;
  await withJobStateLock(claim.job.runRoot, claim.job.symbol, claim.job.dayIso, async () => {
    const state = readJsonFile(claim.statePath);
    if (!state) return;
    const stage = state.stages[claim.stageName];
    if (stage.status !== 'running' || stage.claimedBy !== lockMetadata.workerId) return;
    stage.status = 'complete';
    stage.completedAt = nowIso();
    stage.updatedAt = stage.completedAt;
    stage.error = null;
    stage.elapsedMs = Math.max(0, Math.trunc(Number(elapsedMs || 0)));
    if (rowCount !== null && rowCount !== undefined) stage.rowCount = Number(rowCount) || 0;
    stage.meta = { ...(stage.meta || {}), ...meta };
    clearStageClaim(stage);
    refreshJobStatus(state);
    writeJsonAtomic(claim.statePath, state);
  });
  fs.rmSync(claim.lockPath, { recursive: true, force: true });
}

async function completeDownloadRequest(claim, {
  rowCount = null,
  elapsedMs = 0,
  meta = {},
  env = process.env,
} = {}) {
  const current = readJsonFile(claim.requestPath);
  if (!current?.claimToken || current.claimToken !== claim.token) return null;
  current.status = 'complete';
  current.completedAt = nowIso();
  current.updatedAt = current.completedAt;
  current.claimToken = null;
  current.claimedBy = null;
  current.claimedPid = null;
  current.claimedAt = null;
  current.error = null;
  current.rowCount = rowCount !== null && rowCount !== undefined ? Number(rowCount) || 0 : Number(current.rowCount || 0);
  current.elapsedMs = Math.max(0, Math.trunc(Number(elapsedMs || 0)));
  current.meta = {
    ...(current.meta || {}),
    ...meta,
    lastSuccessAt: current.completedAt,
  };
  writeJsonAtomic(claim.requestPath, current);
  let state = await syncRequestControlledStagesForJob(current.runRoot, current.symbol, current.dayIso, {
    maxAttempts: normalizeStageMaxAttempts(env),
    env,
  });
  if (
    (current.kind === REQUEST_KIND_QUOTE || current.kind === REQUEST_KIND_QUOTE_PARSE)
    && state?.greekMode === 'raw'
    && state?.stages?.quotes?.status === 'complete'
  ) {
    const sessionCache = new Map();
    await ensureRawGreeksRequestsForJobState(state, { sessionCache, env });
    state = await syncRequestControlledStagesForJob(current.runRoot, current.symbol, current.dayIso, {
      maxAttempts: normalizeStageMaxAttempts(env),
      env,
    });
  }
  return state;
}

async function failTask(claim, error, {
  elapsedMs = 0,
  meta = {},
} = {}) {
  const lockMetadata = readJsonFile(claim.lockPath);
  if (!lockMetadata?.token || lockMetadata.token !== claim.token) return;
  await withJobStateLock(claim.job.runRoot, claim.job.symbol, claim.job.dayIso, async () => {
    const state = readJsonFile(claim.statePath);
    if (!state) return;
    const stage = state.stages[claim.stageName];
    if (stage.status !== 'running' || stage.claimedBy !== lockMetadata.workerId) return;
    stage.status = 'failed';
    stage.completedAt = null;
    stage.updatedAt = nowIso();
    stage.error = String(error?.stack || error?.message || error);
    stage.elapsedMs = Math.max(0, Math.trunc(Number(elapsedMs || 0)));
    stage.meta = { ...(stage.meta || {}), ...meta };
    clearStageClaim(stage);
    refreshJobStatus(state);
    writeJsonAtomic(claim.statePath, state);
  });
  fs.rmSync(claim.lockPath, { recursive: true, force: true });
}

async function failDownloadRequest(claim, error, {
  elapsedMs = 0,
  meta = {},
  env = process.env,
} = {}) {
  const current = readJsonFile(claim.requestPath);
  if (!current?.claimToken || current.claimToken !== claim.token) return null;
  current.status = 'failed';
  current.completedAt = null;
  current.updatedAt = nowIso();
  current.claimToken = null;
  current.claimedBy = null;
  current.claimedPid = null;
  current.claimedAt = null;
  current.error = String(error?.stack || error?.message || error);
  current.elapsedMs = Math.max(0, Math.trunc(Number(elapsedMs || 0)));
  current.meta = {
    ...(current.meta || {}),
    ...meta,
    lastFailureAt: current.updatedAt,
    lastFailureError: current.error,
  };
  writeJsonAtomic(claim.requestPath, current);
  return syncRequestControlledStagesForJob(current.runRoot, current.symbol, current.dayIso, {
    maxAttempts: normalizeStageMaxAttempts(env),
    env,
  });
}

function collectRunState(runRoot, {
  maxAttempts = normalizeStageMaxAttempts(process.env),
  env = process.env,
} = {}) {
  const stopRequested = Boolean(readRunStopRequest(runRoot));
  const aggregate = {
    runRoot,
    checkedAt: nowIso(),
    stopRequested,
    totalJobs: 0,
    completeJobs: 0,
    failedJobs: 0,
    runningJobs: 0,
    downloadReady: 0,
    computeReady: 0,
    downloadRunning: 0,
    computeRunning: 0,
    remainingDownloadJobs: 0,
    remainingComputeJobs: 0,
    thetaActiveTarget: parseThetaActiveTarget(env),
    thetaConfiguredQueuedTarget: parseThetaQueuedTarget(env),
    thetaEffectiveQueuedTarget: parseThetaQueuedTarget(env),
    thetaActiveRequests: 0,
    thetaQueuedRequests: 0,
    thetaOutstandingTarget: parseThetaActiveTarget(env) + parseThetaQueuedTarget(env),
    thetaUtilizationPct: 0,
    thetaDistinctJobsInFlight: 0,
    thetaRecent429Count: 0,
    thetaRecentRetryCount: 0,
    thetaCooldownActive: false,
    thetaFullSaturationPossible: false,
    thetaMaxPossibleRequestsNow: 0,
    thetaPotentialDegradedReason: null,
    thetaConnectionSlotsInUse: 0,
    thetaNetworkActiveRequests: 0,
    thetaNetworkQueuedRequests: 0,
    thetaConnectionsByKind: {},
    readyRequestsByKind: { stock: 0, trades: 0, quote: 0, raw_greeks: 0 },
    runningRequestsByKind: { stock: 0, trades: 0, quote: 0, raw_greeks: 0 },
  };
  const jobStates = [];
  for (const filePath of listJobStateFiles(runRoot)) {
    const state = readJsonFile(filePath);
    if (!state) continue;
    if (reconcileStaleRunningStages(state)) {
      writeJsonAtomic(filePath, state);
    }
    cleanupNonRunningStageLocks(state);
    jobStates.push(state);
    aggregate.totalJobs += 1;
    if (state.status === 'complete') aggregate.completeJobs += 1;
    if (state.status === 'failed') aggregate.failedJobs += 1;
    if (state.status === 'running') aggregate.runningJobs += 1;
    if (computeComputeCandidate(state, maxAttempts)) aggregate.computeReady += 1;
    if (state.greekMode === 'calculated' && state.stages.greeks.status === 'running') {
      aggregate.computeRunning += 1;
    }
    const hasRemainingDownload = state?.stages?.stock?.status !== 'complete'
      || state?.stages?.trades?.status !== 'complete'
      || state?.stages?.quotes?.status !== 'complete'
      || (state.greekMode === 'raw' && state?.stages?.greeks?.status !== 'complete');
    if (hasRemainingDownload) aggregate.remainingDownloadJobs += 1;
    if (state.greekMode === 'calculated' && state.stages.greeks.status !== 'complete') {
      aggregate.remainingComputeJobs += 1;
    }
  }

  const requestStates = [];
  for (const filePath of listRequestStateFiles(runRoot)) {
    const state = readJsonFile(filePath);
    if (!state) continue;
    if (reconcileStaleRunningRequestState(state)) {
      writeJsonAtomic(filePath, state);
    }
    requestStates.push(state);
  }
  const snapshot = buildDownloadSchedulerSnapshot(runRoot, jobStates, requestStates, {
    maxAttempts,
    env,
  });
  const requestStatesById = new Map(requestStates.map((state) => [state.requestId, state]));
  const readyComputeRequests = requestStates.filter((state) => (
    isComputeRequestKind(state.kind)
    && state.status !== 'complete'
    && state.status !== 'running'
    && Number(state.attempts || 0) < maxAttempts
    && isQuoteParseRequestReady(state, requestStatesById)
  ));
  const runningComputeRequests = requestStates.filter((state) => (
    isComputeRequestKind(state.kind)
    && state.status === 'running'
  ));
  aggregate.downloadReady = snapshot.readyRequests.length;
  aggregate.downloadRunning = snapshot.runningRequests.length;
  aggregate.computeReady += readyComputeRequests.length;
  aggregate.computeRunning += runningComputeRequests.length;
  aggregate.thetaActiveTarget = snapshot.activeTarget;
  aggregate.thetaConfiguredQueuedTarget = snapshot.configuredQueuedTarget;
  aggregate.thetaEffectiveQueuedTarget = snapshot.effectiveQueuedTarget;
  aggregate.thetaActiveRequests = snapshot.thetaActiveRequests;
  aggregate.thetaQueuedRequests = snapshot.thetaQueuedRequests;
  aggregate.thetaOutstandingTarget = snapshot.outstandingTarget;
  aggregate.thetaUtilizationPct = snapshot.thetaUtilizationPct;
  aggregate.thetaDistinctJobsInFlight = snapshot.thetaDistinctJobsInFlight;
  aggregate.thetaRecent429Count = snapshot.thetaRecent429Count;
  aggregate.thetaRecentRetryCount = snapshot.thetaRecentRetryCount;
  aggregate.thetaCooldownActive = snapshot.thetaCooldownActive;
  aggregate.thetaFullSaturationPossible = snapshot.thetaFullSaturationPossible;
  aggregate.thetaMaxPossibleRequestsNow = snapshot.thetaMaxPossibleRequestsNow;
  aggregate.thetaPotentialDegradedReason = snapshot.thetaPotentialDegradedReason;
  aggregate.readyRequestsByKind = snapshot.readyRequestsByKind;
  aggregate.runningRequestsByKind = snapshot.runningRequestsByKind;
  const thetaSlotUsage = collectThetaConnectionSlotUsage(env);
  aggregate.thetaConnectionSlotsInUse = thetaSlotUsage.slotsInUse;
  aggregate.thetaNetworkActiveRequests = thetaSlotUsage.thetaActiveRequests;
  aggregate.thetaNetworkQueuedRequests = thetaSlotUsage.thetaQueuedRequests;
  aggregate.thetaConnectionsByKind = thetaSlotUsage.inUseByKind;
  return aggregate;
}

function roleShouldContinue(runState, role) {
  if (role === DOWNLOAD_ROLE) {
    if (runState.stopRequested) {
      return runState.downloadRunning > 0;
    }
    return runState.downloadReady > 0 || runState.downloadRunning > 0;
  }
  if (role === COMPUTE_ROLE) {
    if (runState.stopRequested) {
      return runState.computeRunning > 0;
    }
    return runState.computeReady > 0
      || runState.computeRunning > 0
      || (runState.remainingComputeJobs > 0 && (runState.downloadReady > 0 || runState.downloadRunning > 0));
  }
  return false;
}

module.exports = {
  COMPUTE_ROLE,
  DOWNLOAD_ROLE,
  REQUEST_KIND_QUOTE,
  REQUEST_KIND_RAW_GREEKS,
  REQUEST_KIND_STOCK,
  REQUEST_KIND_TRADES,
  claimNextDownloadRequest,
  claimNextTask,
  clearRunStopRequest,
  collectRunState,
  completeDownloadRequest,
  completeTask,
  ensureJobStates,
  ensureRequestStates,
  failDownloadRequest,
  failTask,
  getJobStatePath,
  getRequestStatePath,
  getStatePaths,
  listJobStateFiles,
  listRequestStateFiles,
  normalizeStageMaxAttempts,
  pruneCompleteRequestStatesForCompletedStages,
  readJsonFile,
  readRunStopRequest,
  requestRunStop,
  roleShouldContinue,
  sleep,
  waitForJobStatesReady,
  writeJobsReady,
  writeJsonAtomic,
};
