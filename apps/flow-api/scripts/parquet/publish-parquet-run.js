#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const {
  resolvePublishedCurrentRoot,
  resolvePublishedDatasetsRoot,
  resolvePublishedReleasesRoot,
} = require('./common.js');

const SPECIAL_NYSE_CLOSED_DAYS = new Set([
  '2025-01-09',
]);

function parseArgs(argv) {
  const args = {
    runRoot: null,
    force: false,
  };
  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--force') {
      args.force = true;
      continue;
    }
    if (!args.runRoot) {
      args.runRoot = path.resolve(token);
      continue;
    }
    throw new Error(`unknown_argument:${token}`);
  }
  if (!args.runRoot) {
    throw new Error('usage: publish-parquet-run.js <runRoot> [--force]');
  }
  return args;
}

function ensureDir(target) {
  fs.mkdirSync(target, { recursive: true });
}

function nowIso() {
  return new Date().toISOString();
}

function readJsonFile(filePath) {
  if (!fs.existsSync(filePath)) return null;
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function writeJsonAtomic(filePath, payload) {
  ensureDir(path.dirname(filePath));
  const tempPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  fs.renameSync(tempPath, filePath);
}

function monthKeyForDayIso(dayIso) {
  return String(dayIso || '').slice(0, 7);
}

function uniqueSorted(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).filter(Boolean))).sort();
}

function formatIsoDateUtc(date) {
  return date.toISOString().slice(0, 10);
}

function makeUtcDate(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day));
}

function addUtcDays(date, days) {
  return new Date(date.getTime() + (days * 86400000));
}

function observedHolidayIso(year, month, day) {
  const base = makeUtcDate(year, month, day);
  const weekday = base.getUTCDay();
  if (weekday === 6) return formatIsoDateUtc(addUtcDays(base, -1));
  if (weekday === 0) return formatIsoDateUtc(addUtcDays(base, 1));
  return formatIsoDateUtc(base);
}

function nthWeekdayOfMonthIso(year, month, weekday, ordinal) {
  const first = makeUtcDate(year, month, 1);
  const firstWeekday = first.getUTCDay();
  const offset = (weekday - firstWeekday + 7) % 7;
  const day = 1 + offset + ((ordinal - 1) * 7);
  return formatIsoDateUtc(makeUtcDate(year, month, day));
}

function lastWeekdayOfMonthIso(year, month, weekday) {
  const last = makeUtcDate(year, month + 1, 0);
  const lastWeekday = last.getUTCDay();
  const offset = (lastWeekday - weekday + 7) % 7;
  return formatIsoDateUtc(addUtcDays(last, -offset));
}

function easterSundayUtc(year) {
  const a = year % 19;
  const b = Math.trunc(year / 100);
  const c = year % 100;
  const d = Math.trunc(b / 4);
  const e = b % 4;
  const f = Math.trunc((b + 8) / 25);
  const g = Math.trunc((b - f + 1) / 3);
  const h = ((19 * a) + b - d - g + 15) % 30;
  const i = Math.trunc(c / 4);
  const k = c % 4;
  const l = (32 + (2 * e) + (2 * i) - h - k) % 7;
  const m = Math.trunc((a + (11 * h) + (22 * l)) / 451);
  const month = Math.trunc((h + l - (7 * m) + 114) / 31);
  const day = ((h + l - (7 * m) + 114) % 31) + 1;
  return makeUtcDate(year, month, day);
}

function nyseClosedDaysForYear(year) {
  const closed = new Set([
    observedHolidayIso(year, 1, 1),
    nthWeekdayOfMonthIso(year, 1, 1, 3),
    nthWeekdayOfMonthIso(year, 2, 1, 3),
    formatIsoDateUtc(addUtcDays(easterSundayUtc(year), -2)),
    lastWeekdayOfMonthIso(year, 5, 1),
    observedHolidayIso(year, 6, 19),
    observedHolidayIso(year, 7, 4),
    nthWeekdayOfMonthIso(year, 9, 1, 1),
    nthWeekdayOfMonthIso(year, 11, 4, 4),
    observedHolidayIso(year, 12, 25),
  ]);
  const nextNewYearsObserved = observedHolidayIso(year + 1, 1, 1);
  if (String(nextNewYearsObserved).startsWith(`${year}-`)) {
    closed.add(nextNewYearsObserved);
  }
  for (const dayIso of SPECIAL_NYSE_CLOSED_DAYS) {
    if (dayIso.startsWith(`${year}-`)) {
      closed.add(dayIso);
    }
  }
  return closed;
}

function listNyseOpenDaysInRangeLocal(startIso, endIso) {
  const start = new Date(`${startIso}T00:00:00Z`);
  const end = new Date(`${endIso}T00:00:00Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || start > end) {
    throw new Error(`invalid_date_range:${startIso}:${endIso}`);
  }
  const closedByYear = new Map();
  const openDays = [];
  for (let cursor = start; cursor <= end; cursor = addUtcDays(cursor, 1)) {
    const weekday = cursor.getUTCDay();
    if (weekday === 0 || weekday === 6) continue;
    const year = cursor.getUTCFullYear();
    if (!closedByYear.has(year)) {
      closedByYear.set(year, nyseClosedDaysForYear(year));
    }
    const dayIso = formatIsoDateUtc(cursor);
    if (closedByYear.get(year).has(dayIso)) continue;
    openDays.push(dayIso);
  }
  return openDays;
}

function monthBounds(monthKey) {
  const [yearToken, monthToken] = String(monthKey || '').split('-');
  const year = Number(yearToken);
  const month = Number(monthToken);
  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) {
    throw new Error(`invalid_month_key:${monthKey}`);
  }
  const lastDay = new Date(Date.UTC(year, month, 0)).toISOString().slice(0, 10);
  return {
    startDate: `${yearToken}-${monthToken}-01`,
    endDate: lastDay,
  };
}

function copyDirRecursive(sourceDir, targetDir) {
  if (!fs.existsSync(sourceDir)) {
    throw new Error(`missing_source_dir:${sourceDir}`);
  }
  ensureDir(path.dirname(targetDir));
  fs.cpSync(sourceDir, targetDir, {
    recursive: true,
    dereference: true,
    force: true,
    errorOnExist: false,
    preserveTimestamps: true,
  });
}

function relativeDatasetDir(kind, monthKey, symbol, dayIso) {
  return path.join(kind, `month=${monthKey}`, `symbol=${symbol}`, `trade_date_utc=${dayIso}`);
}

function collectJobStateFiles(runRoot) {
  const jobStateRoot = path.join(runRoot, 'state', 'jobs');
  if (!fs.existsSync(jobStateRoot)) return [];
  return fs.readdirSync(jobStateRoot)
    .filter((name) => name.endsWith('.json'))
    .sort()
    .map((name) => path.join(jobStateRoot, name));
}

async function validatePublishableMonths(runManifest, env) {
  const openDays = uniqueSorted(runManifest?.openDays);
  const grouped = new Map();
  for (const dayIso of openDays) {
    const monthKey = monthKeyForDayIso(dayIso);
    if (!monthKey) continue;
    const bucket = grouped.get(monthKey) || [];
    bucket.push(dayIso);
    grouped.set(monthKey, bucket);
  }
  const publishableMonths = [];
  const skippedMonths = [];
  const monthDetails = new Map();
  for (const [monthKey, monthOpenDays] of grouped.entries()) {
    const bounds = monthBounds(monthKey);
    const expectedOpenDays = listNyseOpenDaysInRangeLocal(bounds.startDate, bounds.endDate, env);
    const actual = monthOpenDays.slice().sort();
    const expected = expectedOpenDays.slice().sort();
    const isFullCoverage = actual.length === expected.length
      && actual.every((dayIso, index) => dayIso === expected[index]);
    const detail = {
      month: monthKey,
      startDate: bounds.startDate,
      endDate: bounds.endDate,
      actualOpenDays: actual,
      expectedOpenDays: expected,
    };
    monthDetails.set(monthKey, detail);
    if (isFullCoverage) {
      publishableMonths.push(monthKey);
    } else {
      skippedMonths.push({
        month: monthKey,
        reason: 'partial_month_coverage',
        expectedOpenDays: expected.length,
        actualOpenDays: actual.length,
      });
    }
  }
  publishableMonths.sort();
  skippedMonths.sort((left, right) => left.month.localeCompare(right.month));
  return { publishableMonths, skippedMonths, monthDetails };
}

function buildRunSummaryFallback(runRoot, runManifest, jobStates) {
  const totalJobs = jobStates.length;
  const completedJobs = jobStates.filter((job) => job?.status === 'complete').length;
  const failedJobs = jobStates.filter((job) => job?.status === 'failed').length;
  const runningJobs = jobStates.filter((job) => job?.status === 'running').length;
  const startedAt = runManifest?.createdAt || null;
  const generatedAt = nowIso();
  const startedMs = startedAt ? Date.parse(startedAt) : Number.NaN;
  const generatedMs = Date.parse(generatedAt);
  return {
    runRoot,
    generatedAt,
    startedAt,
    durationMs: Number.isFinite(startedMs) ? Math.max(0, generatedMs - startedMs) : null,
    totalJobs,
    completedJobs,
    failedJobs,
    runningJobs,
  };
}

function resolveRunSummary(runRoot, runManifest, runSummary, jobStates) {
  if (runSummary) return runSummary;
  return buildRunSummaryFallback(runRoot, runManifest, jobStates);
}

function assertCompleteRun(runRoot, runManifest, runSummary, jobStates) {
  if (!runManifest) throw new Error(`missing_run_manifest:${runRoot}`);
  if (Number(runSummary.failedJobs || 0) !== 0) {
    throw new Error(`run_has_failed_jobs:${runSummary.failedJobs}`);
  }
  if (Number(runSummary.completedJobs || 0) !== Number(runSummary.totalJobs || 0)) {
    throw new Error(`run_not_complete:${runSummary.completedJobs}/${runSummary.totalJobs}`);
  }
  const incomplete = jobStates.filter((job) => job?.status !== 'complete');
  if (incomplete.length > 0) {
    throw new Error(`job_states_not_complete:${incomplete.length}`);
  }
}

function copyPublishedPartitions({
  runRoot,
  stagingReleaseRoot,
  jobStates,
  publishableMonths,
}) {
  const publishable = new Set(publishableMonths);
  const datasetMonthCounts = new Map();
  for (const job of jobStates) {
    const monthKey = monthKeyForDayIso(job.dayIso);
    if (!publishable.has(monthKey)) continue;
    const symbol = job.symbol;
    const dayIso = job.dayIso;

    const partitionCopies = [
      {
        enabled: job?.stages?.stock?.status === 'complete',
        sourceDir: path.join(runRoot, 'datasets', 'raw', 'stock_ohlc_minute', `symbol=${symbol}`, `trade_date_utc=${dayIso}`),
        targetDir: path.join(stagingReleaseRoot, relativeDatasetDir(path.join('raw', 'stock_ohlc_minute'), monthKey, symbol, dayIso)),
        datasetKey: `raw/stock_ohlc_minute/${monthKey}`,
      },
      {
        enabled: job?.stages?.quotes?.status === 'complete',
        sourceDir: path.join(runRoot, 'datasets', 'raw', 'option_quote_minute', `symbol=${symbol}`, `trade_date_utc=${dayIso}`),
        targetDir: path.join(stagingReleaseRoot, relativeDatasetDir(path.join('raw', 'option_quote_minute'), monthKey, symbol, dayIso)),
        datasetKey: `raw/option_quote_minute/${monthKey}`,
      },
      {
        enabled: job?.stages?.trades?.status === 'complete',
        sourceDir: path.join(runRoot, 'datasets', 'raw', 'option_trades', `symbol=${symbol}`, `trade_date_utc=${dayIso}`),
        targetDir: path.join(stagingReleaseRoot, relativeDatasetDir(path.join('raw', 'option_trades'), monthKey, symbol, dayIso)),
        datasetKey: `raw/option_trades/${monthKey}`,
      },
      {
        enabled: job?.stages?.trades?.status === 'complete',
        sourceDir: path.join(runRoot, 'datasets', 'raw', 'option_trade_quote', `symbol=${symbol}`, `trade_date_utc=${dayIso}`),
        targetDir: path.join(stagingReleaseRoot, relativeDatasetDir(path.join('raw', 'option_trade_quote'), monthKey, symbol, dayIso)),
        datasetKey: `raw/option_trade_quote/${monthKey}`,
      },
      {
        enabled: job.greekMode === 'raw' && job?.stages?.greeks?.status === 'complete',
        sourceDir: path.join(runRoot, 'datasets', 'raw', 'option_greeks_minute', `symbol=${symbol}`, `trade_date_utc=${dayIso}`),
        targetDir: path.join(stagingReleaseRoot, relativeDatasetDir(path.join('raw', 'option_greeks_minute'), monthKey, symbol, dayIso)),
        datasetKey: `raw/option_greeks_minute/${monthKey}`,
      },
      {
        enabled: job?.stages?.greeks?.status === 'complete',
        sourceDir: path.join(runRoot, 'datasets', 'derived', 'option_greeks_minute', `symbol=${symbol}`, `trade_date_utc=${dayIso}`),
        targetDir: path.join(stagingReleaseRoot, relativeDatasetDir(path.join('derived', 'option_greeks_minute'), monthKey, symbol, dayIso)),
        datasetKey: `derived/option_greeks_minute/${monthKey}`,
      },
    ];

    for (const copy of partitionCopies) {
      if (!copy.enabled) continue;
      copyDirRecursive(copy.sourceDir, copy.targetDir);
      datasetMonthCounts.set(copy.datasetKey, Number(datasetMonthCounts.get(copy.datasetKey) || 0) + 1);
    }
  }
  return Object.fromEntries(Array.from(datasetMonthCounts.entries()).sort());
}

function partitionCountsByMonth(datasetMonthCounts, monthKey) {
  const counts = {};
  for (const [datasetKey, count] of Object.entries(datasetMonthCounts || {})) {
    const suffix = `/${monthKey}`;
    if (!datasetKey.endsWith(suffix)) continue;
    counts[datasetKey.slice(0, -suffix.length)] = Number(count || 0);
  }
  return counts;
}

function swapCurrentMonthSymlink(currentPath, targetPath) {
  ensureDir(path.dirname(currentPath));
  const tempPath = `${currentPath}.tmp-${process.pid}-${Date.now()}`;
  const backupPath = `${currentPath}.bak-${process.pid}-${Date.now()}`;
  const relativeTarget = path.relative(path.dirname(currentPath), targetPath);
  fs.symlinkSync(relativeTarget, tempPath, 'dir');
  if (fs.existsSync(currentPath)) {
    fs.renameSync(currentPath, backupPath);
  }
  fs.renameSync(tempPath, currentPath);
  if (fs.existsSync(backupPath)) {
    fs.rmSync(backupPath, { recursive: true, force: true });
  }
}

async function publishRun(runRoot, { force = false, env = process.env } = {}) {
  const runManifest = readJsonFile(path.join(runRoot, 'manifests', 'run.json'));
  const summaryPath = path.join(runRoot, 'reports', 'summary.json');
  const persistedRunSummary = readJsonFile(summaryPath);
  const jobStates = collectJobStateFiles(runRoot).map((filePath) => readJsonFile(filePath)).filter(Boolean);
  const jobDays = uniqueSorted(jobStates.map((job) => job?.dayIso));
  if ((!Array.isArray(runManifest?.openDays) || runManifest.openDays.length === 0) && jobDays.length > 0) {
    runManifest.openDays = jobDays;
  }
  const runSummary = resolveRunSummary(runRoot, runManifest, persistedRunSummary, jobStates);
  assertCompleteRun(runRoot, runManifest, runSummary, jobStates);

  const { publishableMonths, skippedMonths, monthDetails } = await validatePublishableMonths(runManifest, env);
  if (publishableMonths.length === 0) {
    const skipped = {
      published: false,
      reason: 'no_full_months',
      runRoot,
      runId: runManifest?.runId || path.basename(runRoot),
      skippedMonths,
    };
    console.log(JSON.stringify(skipped, null, 2));
    return skipped;
  }

  const datasetsRoot = resolvePublishedDatasetsRoot(env);
  const currentRoot = resolvePublishedCurrentRoot(env);
  const releasesRoot = resolvePublishedReleasesRoot(env);
  const releaseId = runManifest.runId || path.basename(runRoot);
  const stagingReleaseRoot = path.join(datasetsRoot, '.staging', releaseId);
  const finalReleaseRoot = path.join(releasesRoot, releaseId);
  const releaseManifestPath = path.join(finalReleaseRoot, '_release.json');
  const monthManifestRoot = path.join(datasetsRoot, '_manifests');
  let releaseWasReused = false;

  if (fs.existsSync(finalReleaseRoot)) {
    if (!force) {
      const existing = readJsonFile(releaseManifestPath);
      if (existing?.sourceRunRoot === runRoot) {
        releaseWasReused = true;
      } else {
        throw new Error(`release_already_exists:${finalReleaseRoot}`);
      }
    } else {
      fs.rmSync(finalReleaseRoot, { recursive: true, force: true });
    }
  }

  let datasetMonthCounts;
  let releaseManifest;
  if (releaseWasReused) {
    releaseManifest = readJsonFile(releaseManifestPath) || {};
    datasetMonthCounts = releaseManifest.datasetMonthCounts || {};
  } else {
    fs.rmSync(stagingReleaseRoot, { recursive: true, force: true });
    ensureDir(path.dirname(stagingReleaseRoot));

    datasetMonthCounts = copyPublishedPartitions({
      runRoot,
      stagingReleaseRoot,
      jobStates,
      publishableMonths,
    });

    releaseManifest = {
      publishedAt: nowIso(),
      releaseId,
      sourceRunId: runManifest.runId || releaseId,
      sourceRunRoot: runRoot,
      datasetsRoot,
      currentRoot,
      publishableMonths,
      skippedMonths,
      datasetMonthCounts,
      totalJobs: Number(runSummary.totalJobs || 0),
      completedJobs: Number(runSummary.completedJobs || 0),
      failedJobs: Number(runSummary.failedJobs || 0),
      summaryPath: fs.existsSync(summaryPath) ? summaryPath : null,
      monthDetails: Object.fromEntries(Array.from(monthDetails.entries()).sort(([left], [right]) => left.localeCompare(right))),
    };
    writeJsonAtomic(path.join(stagingReleaseRoot, '_release.json'), releaseManifest);

    ensureDir(path.dirname(finalReleaseRoot));
    fs.renameSync(stagingReleaseRoot, finalReleaseRoot);
  }

  const publishedDatasets = [
    'raw/stock_ohlc_minute',
    'raw/option_quote_minute',
    'raw/option_trades',
    'raw/option_trade_quote',
    'raw/option_greeks_minute',
    'derived/option_greeks_minute',
  ];
  for (const monthKey of publishableMonths) {
    for (const datasetRelative of publishedDatasets) {
      const targetPath = path.join(finalReleaseRoot, datasetRelative, `month=${monthKey}`);
      if (!fs.existsSync(targetPath)) continue;
      const currentPath = path.join(currentRoot, datasetRelative, `month=${monthKey}`);
      swapCurrentMonthSymlink(currentPath, targetPath);
    }
    const detail = monthDetails.get(monthKey) || {};
    writeJsonAtomic(path.join(monthManifestRoot, `month=${monthKey}.json`), {
      month: monthKey,
      releaseId,
      releaseRoot: finalReleaseRoot,
      currentReleaseId: releaseId,
      currentReleaseRoot: finalReleaseRoot,
      publishedAt: releaseManifest.publishedAt || nowIso(),
      sourceRunId: releaseManifest.sourceRunId || releaseId,
      sourceRunRoot: runRoot,
      jobCount: jobStates.filter((job) => monthKeyForDayIso(job.dayIso) === monthKey).length,
      openDays: detail.actualOpenDays || [],
      expectedOpenDays: detail.expectedOpenDays || [],
      datasets: partitionCountsByMonth(datasetMonthCounts, monthKey),
    });
  }

  const result = {
    published: true,
    reusedRelease: false,
    runRoot,
    runId: releaseId,
    releaseRoot: finalReleaseRoot,
    currentRoot,
    months: publishableMonths,
    skippedMonths,
    datasetMonthCounts,
  };
  console.log(JSON.stringify(result, null, 2));
  return result;
}

async function main() {
  const args = parseArgs(process.argv);
  await publishRun(args.runRoot, {
    force: args.force,
    env: process.env,
  });
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error?.stack || error?.message || error);
    process.exitCode = 1;
  });
}

module.exports = {
  publishRun,
};
