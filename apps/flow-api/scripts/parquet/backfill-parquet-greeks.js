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
  parseIndexGreeksSymbols,
  resolveRunRoot,
  shardJobsBalanced,
  writeJsonFile,
} = require('./common');

function parseCsv(rawValue) {
  return String(rawValue || '')
    .split(',')
    .map((token) => token.trim())
    .filter(Boolean);
}

function jobKey(symbol, dayIso) {
  return `${symbol}::${dayIso}`;
}

function loadExistingWorkerReports(reportsRoot) {
  if (!fs.existsSync(reportsRoot)) return [];
  return fs.readdirSync(reportsRoot)
    .filter((name) => /^worker-\d+\.json$/.test(name))
    .sort()
    .map((name) => {
      const reportPath = path.join(reportsRoot, name);
      try {
        return JSON.parse(fs.readFileSync(reportPath, 'utf8'));
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}

function buildWorkerReport({
  runId,
  runRoot,
  workerIndex,
  workerTotal,
  totalJobs,
  startedAt,
  jobsByKey,
}) {
  const jobs = Array.from(jobsByKey.values()).sort((left, right) => {
    if (left.dayIso !== right.dayIso) return String(left.dayIso).localeCompare(String(right.dayIso));
    return String(left.symbol).localeCompare(String(right.symbol));
  });
  const report = {
    runId,
    runRoot,
    workerIndex,
    workerTotal,
    startedAt,
    completedAt: null,
    totalJobs,
    completedJobs: 0,
    failedJobs: 0,
    totalStockRows: 0,
    totalQuoteRows: 0,
    totalRawGreekRows: 0,
    totalFinalGreekRows: 0,
    totalIndexGreekJobs: 0,
    totalCalculatedGreekJobs: 0,
    stockMs: 0,
    quoteMs: 0,
    indexGreekMs: 0,
    calcGreekMs: 0,
    jobs,
  };
  jobs.forEach((job) => {
    if (job.status === 'complete') report.completedJobs += 1;
    if (job.status === 'failed') report.failedJobs += 1;
    report.totalStockRows += Number(job.stockRows || 0);
    report.totalQuoteRows += Number(job.quoteRows || 0);
    report.totalRawGreekRows += Number(job.rawGreekRows || 0);
    report.totalFinalGreekRows += Number(job.finalGreekRows || 0);
    if (job.greekMode === 'raw' && job.status === 'complete') report.totalIndexGreekJobs += 1;
    if (job.greekMode === 'calculated' && job.status === 'complete') report.totalCalculatedGreekJobs += 1;
    report.stockMs += Number(job.stockMs || 0);
    report.quoteMs += Number(job.quoteMs || 0);
    report.indexGreekMs += Number(job.indexGreekMs || 0);
    report.calcGreekMs += Number(job.calcGreekMs || 0);
  });
  return report;
}

async function main() {
  const runId = String(process.env.PARQUET_RUN_ID || '').trim() || buildRunId('parquet-benchmark');
  const runRoot = resolveRunRoot(runId, process.env);
  const layout = ensureRunLayout(runRoot);
  const workerTotal = Math.max(1, Math.trunc(Number(process.env.PARQUET_WORKER_TOTAL || 1)));
  const workerIndex = Math.max(0, Math.trunc(Number(process.env.PARQUET_WORKER_INDEX || 0)));
  if (workerIndex >= workerTotal) {
    throw new Error(`invalid_worker_index:${workerIndex}/${workerTotal}`);
  }

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

  const manifestPath = path.join(layout.manifestsRoot, 'run.json');
  let built;
  if (!fs.existsSync(manifestPath) || workerIndex === 0) {
    built = await buildJobs({
      startDate,
      endDate,
      symbolFile,
      symbolLimit,
      extraSymbols,
      env: process.env,
    });
    writeJsonFile(manifestPath, {
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
    });
  } else {
    built = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));
    built.jobs = [];
    const openDays = Array.isArray(built.openDays) ? built.openDays : [];
    const symbols = Array.isArray(built.symbols) ? built.symbols : [];
    openDays.forEach((dayIso) => {
      symbols.forEach((symbol) => {
        built.jobs.push({ symbol, dayIso });
      });
    });
    if (!built.jobs.length) {
      built = await buildJobs({
        startDate,
        endDate,
        symbolFile,
        symbolLimit,
        extraSymbols,
        env: process.env,
      });
      writeJsonFile(manifestPath, {
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
      });
    }
  }

  const workerJobs = shardJobsBalanced(built.jobs, workerTotal, workerIndex);
  const existingReports = loadExistingWorkerReports(layout.reportsRoot);
  const completedJobKeys = new Set();
  existingReports.forEach((report) => {
    (Array.isArray(report.jobs) ? report.jobs : []).forEach((job) => {
      if (job?.status === 'complete' && job.symbol && job.dayIso) {
        completedJobKeys.add(jobKey(job.symbol, job.dayIso));
      }
    });
  });

  const workerReportPath = path.join(layout.reportsRoot, `worker-${workerIndex}.json`);
  const existingWorkerReport = fs.existsSync(workerReportPath)
    ? JSON.parse(fs.readFileSync(workerReportPath, 'utf8'))
    : null;
  const jobsByKey = new Map();
  (Array.isArray(existingWorkerReport?.jobs) ? existingWorkerReport.jobs : []).forEach((job) => {
    if (job?.symbol && job?.dayIso) {
      jobsByKey.set(jobKey(job.symbol, job.dayIso), job);
    }
  });
  let report = buildWorkerReport({
    runId,
    runRoot,
    workerIndex,
    workerTotal,
    totalJobs: workerJobs.length,
    startedAt: existingWorkerReport?.startedAt || new Date().toISOString(),
    jobsByKey,
  });
  writeJsonFile(workerReportPath, report);

  for (let index = 0; index < workerJobs.length; index += 1) {
    const job = workerJobs[index];
    const key = jobKey(job.symbol, job.dayIso);
    if (completedJobKeys.has(key)) {
      console.log('[PARQUET_JOB_SKIP]', JSON.stringify({
        symbol: job.symbol,
        dayIso: job.dayIso,
        reason: 'already_complete',
      }));
      continue;
    }
    const startedAtMs = Date.now();
    const jobSummary = {
      symbol: job.symbol,
      dayIso: job.dayIso,
      jobIndex: index + 1,
      totalJobs: workerJobs.length,
      greekMode: indexGreeksSymbols.has(job.symbol) ? 'raw' : 'calculated',
      status: 'running',
      stockRows: 0,
      quoteRows: 0,
      rawGreekRows: 0,
      finalGreekRows: 0,
      stockMs: 0,
      quoteMs: 0,
      indexGreekMs: 0,
      calcGreekMs: 0,
      elapsedMs: 0,
      error: null,
    };
    console.log('[PARQUET_JOB_START]', JSON.stringify(jobSummary));
    try {
      const stockStartedAtMs = Date.now();
      const stockResult = await downloadStockToParquet({
        runRoot,
        symbol: job.symbol,
        dayIso: job.dayIso,
        env: process.env,
      });
      jobSummary.stockMs = Math.max(0, Date.now() - stockStartedAtMs);
      report.totalStockRows += stockResult.rowCount;
      jobSummary.stockRows = stockResult.rowCount;

      const quoteStartedAtMs = Date.now();
      const quoteResult = await downloadQuotesToParquet({
        runRoot,
        symbol: job.symbol,
        dayIso: job.dayIso,
        env: process.env,
      });
      jobSummary.quoteMs = Math.max(0, Date.now() - quoteStartedAtMs);
      report.totalQuoteRows += quoteResult.rowCount;
      jobSummary.quoteRows = quoteResult.rowCount;

      if (indexGreeksSymbols.has(job.symbol)) {
        const rawStartedAtMs = Date.now();
        const rawResult = await downloadIndexGreeksToParquet({
          runRoot,
          symbol: job.symbol,
          dayIso: job.dayIso,
          expirations: quoteResult.expirations,
          runId,
          env: process.env,
        });
        jobSummary.indexGreekMs = Math.max(0, Date.now() - rawStartedAtMs);
        report.totalRawGreekRows += rawResult.rawRowsWritten;
        report.totalFinalGreekRows += rawResult.rawRowsWritten;
        report.totalIndexGreekJobs += 1;
        jobSummary.rawGreekRows = rawResult.rawRowsWritten;
        jobSummary.finalGreekRows = rawResult.rawRowsWritten;
      } else {
        const calcStartedAtMs = Date.now();
        const calcResult = await calculateGreeksToParquet({
          runRoot,
          symbol: job.symbol,
          dayIso: job.dayIso,
          stockByMinute: stockResult.stockByMinute,
          runId,
          env: process.env,
        });
        jobSummary.calcGreekMs = Math.max(0, Date.now() - calcStartedAtMs);
        report.totalFinalGreekRows += calcResult.writtenRows;
        report.totalCalculatedGreekJobs += 1;
        jobSummary.finalGreekRows = calcResult.writtenRows;
      }

      jobSummary.status = 'complete';
      report.completedJobs += 1;
    } catch (error) {
      report.failedJobs += 1;
      jobSummary.status = 'failed';
      jobSummary.error = String(error?.stack || error?.message || error);
      console.error('[PARQUET_JOB_FAILED]', JSON.stringify({
        symbol: job.symbol,
        dayIso: job.dayIso,
        error: jobSummary.error.split('\n')[0],
      }));
    }
    jobSummary.elapsedMs = Math.max(0, Date.now() - startedAtMs);
    jobsByKey.set(key, jobSummary);
    report = buildWorkerReport({
      runId,
      runRoot,
      workerIndex,
      workerTotal,
      totalJobs: workerJobs.length,
      startedAt: report.startedAt,
      jobsByKey,
    });
    writeJsonFile(workerReportPath, report);
    console.log('[PARQUET_JOB_DONE]', JSON.stringify(jobSummary));
  }

  report = buildWorkerReport({
    runId,
    runRoot,
    workerIndex,
    workerTotal,
    totalJobs: workerJobs.length,
    startedAt: report.startedAt,
    jobsByKey,
  });
  report.completedAt = new Date().toISOString();
  writeJsonFile(workerReportPath, report);
  console.log(JSON.stringify({
    runId,
    runRoot,
    workerIndex,
    workerTotal,
    totalJobs: report.totalJobs,
    completedJobs: report.completedJobs,
    failedJobs: report.failedJobs,
    totalStockRows: report.totalStockRows,
    totalQuoteRows: report.totalQuoteRows,
    totalRawGreekRows: report.totalRawGreekRows,
    totalFinalGreekRows: report.totalFinalGreekRows,
  }, null, 2));
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});
