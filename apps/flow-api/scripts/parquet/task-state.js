const fs = require('node:fs');
const path = require('node:path');

const {
  ensureRunLayout,
  getFinalGreeksPartitionDir,
  getQuotePartitionDir,
  getRawGreeksPartitionDir,
  getStockPartitionDir,
  probeQuotePartition,
  probeRowPartition,
  probeStockPartition,
} = require('./common');

const DOWNLOAD_ROLE = 'download';
const COMPUTE_ROLE = 'compute';
const DEFAULT_STAGE_MAX_ATTEMPTS = 3;

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

function normalizeStageMaxAttempts(env = process.env) {
  const parsed = Number(env.PARQUET_STAGE_MAX_ATTEMPTS);
  if (Number.isFinite(parsed) && parsed >= 1) {
    return Math.max(1, Math.min(100, Math.trunc(parsed)));
  }
  return DEFAULT_STAGE_MAX_ATTEMPTS;
}

function jobId(symbol, dayIso) {
  return `${String(symbol).trim().toUpperCase()}__${String(dayIso).trim()}`;
}

function getStatePaths(runRoot) {
  const layout = ensureRunLayout(runRoot);
  return {
    ...layout,
    stopPath: path.join(layout.controlRoot, 'stop-requested.json'),
    readyPath: path.join(layout.controlRoot, 'jobs-ready.json'),
  };
}

function getJobStatePath(runRoot, symbol, dayIso) {
  return path.join(getStatePaths(runRoot).jobStateRoot, `${jobId(symbol, dayIso)}.json`);
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

function buildStageState(name, mode = null) {
  return {
    name,
    mode,
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
      stock: buildStageState('stock'),
      quotes: buildStageState('quotes'),
      greeks: buildStageState('greeks', greekMode),
    },
  };
}

function refreshJobStatus(jobState) {
  const greeksStage = jobState?.stages?.greeks;
  const stockStage = jobState?.stages?.stock;
  const quoteStage = jobState?.stages?.quotes;
  if (greeksStage?.status === 'complete') {
    jobState.status = 'complete';
  } else if ([stockStage, quoteStage, greeksStage].some((stage) => stage?.status === 'running')) {
    jobState.status = 'running';
  } else if ([stockStage, quoteStage, greeksStage].some((stage) => stage?.status === 'failed')) {
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
      status: 'complete',
      startedAt: jobState.stages.stock.startedAt || stockMarker?.completedAt || jobState.createdAt,
      completedAt: stockMarker?.completedAt || nowIso(),
      updatedAt: nowIso(),
      rowCount: Number(stockMarker?.rowCount || jobState.stages.stock.rowCount || 0),
      meta: {
        partCount: stockMarker?.partCount || null,
        legacyFastPath: !stockMarker,
      },
    };
  }

  const quotePartitionDir = getQuotePartitionDir(runRoot, symbol, dayIso);
  const quoteMarker = readJsonFile(path.join(quotePartitionDir, '_SUCCESS.json'));
  if ((Number(quoteMarker?.partCount || 0) > 0) || partitionHasData(quotePartitionDir)) {
    jobState.stages.quotes = {
      ...jobState.stages.quotes,
      status: 'complete',
      startedAt: jobState.stages.quotes.startedAt || quoteMarker?.completedAt || jobState.createdAt,
      completedAt: quoteMarker?.completedAt || nowIso(),
      updatedAt: nowIso(),
      rowCount: Number(quoteMarker?.rowCount || jobState.stages.quotes.rowCount || 0),
      meta: {
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
        status: 'complete',
        startedAt: jobState.stages.greeks.startedAt || rawMarker?.completedAt || finalMarker?.completedAt || jobState.createdAt,
        completedAt: finalMarker?.completedAt || rawMarker?.completedAt || nowIso(),
        updatedAt: nowIso(),
        rowCount: Number(rawMarker?.rowCount || finalMarker?.rowCount || jobState.stages.greeks.rowCount || 0),
        meta: {
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
        status: 'complete',
        startedAt: jobState.stages.greeks.startedAt || finalMarker?.completedAt || jobState.createdAt,
        completedAt: finalMarker?.completedAt || nowIso(),
        updatedAt: nowIso(),
        rowCount: Number(finalMarker?.rowCount || jobState.stages.greeks.rowCount || 0),
        meta: {
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
      state.stages.stock = state.stages.stock || buildStageState('stock');
      state.stages.quotes = state.stages.quotes || buildStageState('quotes');
      state.stages.greeks = state.stages.greeks || buildStageState('greeks', greekMode);
      state.stages.greeks.mode = greekMode;
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
} = {}) {
  const startedAt = Date.now();
  while (true) {
    const ready = readJobsReady(runRoot);
    if (ready) return ready;
    if ((Date.now() - startedAt) >= timeoutMs) {
      throw new Error(`job_state_ready_timeout:${timeoutMs}`);
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
  if (!stage) return false;
  if (stage.status === 'complete') return false;
  if (stage.status === 'running') return false;
  const attempts = Math.max(0, Math.trunc(Number(stage.attempts || 0)));
  return attempts < maxAttempts;
}

function computeDownloadCandidate(state, maxAttempts) {
  const stockReady = stageCanClaim(state.stages.stock, maxAttempts);
  const quotesReady = stageCanClaim(state.stages.quotes, maxAttempts);
  const greeksReady = state.greekMode === 'raw'
    && state.stages.quotes.status === 'complete'
    && stageCanClaim(state.stages.greeks, maxAttempts);

  const candidates = [];
  if (greeksReady) candidates.push({ stageName: 'greeks', priority: 10 });
  if (quotesReady) candidates.push({ stageName: 'quotes', priority: state.stages.stock.status === 'complete' ? 20 : 30 });
  if (stockReady) candidates.push({ stageName: 'stock', priority: state.stages.quotes.status === 'complete' ? 20 : 40 });
  if (candidates.length === 0) return null;
  candidates.sort((left, right) => left.priority - right.priority);
  return candidates[0];
}

function computeComputeCandidate(state, maxAttempts) {
  if (state.greekMode !== 'calculated') return null;
  if (state.stages.stock.status !== 'complete' || state.stages.quotes.status !== 'complete') return null;
  if (!stageCanClaim(state.stages.greeks, maxAttempts)) return null;
  return { stageName: 'greeks', priority: 10 };
}

function getCandidateForRole(state, role, maxAttempts) {
  if (role === DOWNLOAD_ROLE) return computeDownloadCandidate(state, maxAttempts);
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

async function claimNextTask({
  runRoot,
  role,
  workerId,
  maxAttempts = normalizeStageMaxAttempts(process.env),
}) {
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
    if (!stage || stage.status !== 'running') continue;
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
    if (!stage || stage.status === 'running') continue;
    const lockPath = getTaskLockPath(state.runRoot, state.symbol, state.dayIso, stageName);
    if (fs.existsSync(lockPath)) {
      fs.rmSync(lockPath, { recursive: true, force: true });
      cleaned = true;
    }
  }
  return cleaned;
}

async function completeTask(claim, {
  rowCount = null,
  elapsedMs = 0,
  meta = {},
} = {}) {
  await withJobStateLock(claim.job.runRoot, claim.job.symbol, claim.job.dayIso, async () => {
    const state = readJsonFile(claim.statePath);
    if (!state) return;
    const stage = state.stages[claim.stageName];
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

async function failTask(claim, error, {
  elapsedMs = 0,
  meta = {},
} = {}) {
  await withJobStateLock(claim.job.runRoot, claim.job.symbol, claim.job.dayIso, async () => {
    const state = readJsonFile(claim.statePath);
    if (!state) return;
    const stage = state.stages[claim.stageName];
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

function collectRunState(runRoot, {
  maxAttempts = normalizeStageMaxAttempts(process.env),
} = {}) {
  const files = listJobStateFiles(runRoot);
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
  };
  for (const filePath of files) {
    const state = readJsonFile(filePath);
    if (!state) continue;
    if (reconcileStaleRunningStages(state)) {
      writeJsonAtomic(filePath, state);
    }
    cleanupNonRunningStageLocks(state);
    aggregate.totalJobs += 1;
    if (state.status === 'complete') aggregate.completeJobs += 1;
    if (state.status === 'failed') aggregate.failedJobs += 1;
    if (state.status === 'running') aggregate.runningJobs += 1;
    const downloadCandidate = getCandidateForRole(state, DOWNLOAD_ROLE, maxAttempts);
    const computeCandidate = getCandidateForRole(state, COMPUTE_ROLE, maxAttempts);
    if (downloadCandidate) aggregate.downloadReady += 1;
    if (computeCandidate) aggregate.computeReady += 1;
    if ([state.stages.stock.status, state.stages.quotes.status].includes('running')
      || (state.greekMode === 'raw' && state.stages.greeks.status === 'running')) {
      aggregate.downloadRunning += 1;
    }
    if (state.greekMode === 'calculated' && state.stages.greeks.status === 'running') {
      aggregate.computeRunning += 1;
    }
    const hasRemainingDownload = state.stages.stock.status !== 'complete'
      || state.stages.quotes.status !== 'complete'
      || (state.greekMode === 'raw' && state.stages.greeks.status !== 'complete');
    if (hasRemainingDownload) aggregate.remainingDownloadJobs += 1;
    if (state.greekMode === 'calculated' && state.stages.greeks.status !== 'complete') {
      aggregate.remainingComputeJobs += 1;
    }
  }
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
  claimNextTask,
  clearRunStopRequest,
  collectRunState,
  completeTask,
  ensureJobStates,
  failTask,
  getJobStatePath,
  getStatePaths,
  listJobStateFiles,
  normalizeStageMaxAttempts,
  readJsonFile,
  readRunStopRequest,
  requestRunStop,
  roleShouldContinue,
  waitForJobStatesReady,
  writeJobsReady,
  sleep,
  writeJsonAtomic,
};
