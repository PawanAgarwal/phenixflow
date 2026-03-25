#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Math.trunc(ms || 0))));
}

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function parseArgs(argv) {
  const args = { runRoot: null, watch: false, intervalSeconds: 600 };
  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--watch') {
      args.watch = true;
      continue;
    }
    if (token === '--interval-seconds') {
      args.intervalSeconds = Number(argv[index + 1] || 600) || 600;
      index += 1;
      continue;
    }
    if (!args.runRoot) args.runRoot = path.resolve(token);
  }
  if (!args.runRoot) {
    throw new Error('usage: monitor-parquet-run.js <runRoot> [--watch] [--interval-seconds N]');
  }
  return args;
}

function pidAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function collectStatus(runRoot) {
  const reportsRoot = path.join(runRoot, 'reports');
  const logsRoot = path.join(runRoot, 'logs');
  const summaryPath = path.join(reportsRoot, 'summary.json');
  const pidFile = path.join(runRoot, 'worker-pids.txt');
  const summary = readJson(summaryPath);
  let configuredWorkerCount = null;
  let configuredPids = [];
  if (fs.existsSync(pidFile)) {
    configuredPids = fs.readFileSync(pidFile, 'utf8')
      .split(/\s+/)
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0);
    configuredWorkerCount = configuredPids.length;
  }
  const workerFiles = fs.existsSync(reportsRoot)
    ? fs.readdirSync(reportsRoot).filter((name) => /^worker-\d+\.json$/.test(name)).sort()
    : [];
  const aggregate = {
    runRoot,
    checkedAt: new Date().toISOString(),
    workerReports: workerFiles.length,
    totalJobs: 0,
    completedJobs: 0,
    failedJobs: 0,
    activePids: [],
    lastReportUpdatedAt: null,
    hasSummary: Boolean(summary),
    summaryCompletedJobs: Number(summary?.completedJobs || 0),
    summaryFailedJobs: Number(summary?.failedJobs || 0),
  };

  const jobsByKey = new Map();
  workerFiles.forEach((name) => {
    const reportPath = path.join(reportsRoot, name);
    const report = readJson(reportPath);
    if (!report) return;
    (Array.isArray(report.jobs) ? report.jobs : []).forEach((job) => {
      if (!job?.symbol || !job?.dayIso) return;
      const key = `${job.symbol}::${job.dayIso}`;
      const existing = jobsByKey.get(key);
      if (!existing || existing.status !== 'complete' || job.status === 'complete') {
        jobsByKey.set(key, job);
      }
    });
    const stat = fs.statSync(reportPath);
    const mtime = stat.mtime.toISOString();
    if (!aggregate.lastReportUpdatedAt || mtime > aggregate.lastReportUpdatedAt) {
      aggregate.lastReportUpdatedAt = mtime;
    }
  });
  aggregate.totalJobs = jobsByKey.size;
  jobsByKey.forEach((job) => {
    if (job.status === 'complete') aggregate.completedJobs += 1;
    if (job.status === 'failed') aggregate.failedJobs += 1;
  });

  aggregate.activePids = configuredPids.filter(pidAlive);

  if (!aggregate.lastReportUpdatedAt && fs.existsSync(logsRoot)) {
    const logFiles = fs.readdirSync(logsRoot).filter((name) => /^worker-\d+\.log$/.test(name)).sort();
    logFiles.forEach((name) => {
      const stat = fs.statSync(path.join(logsRoot, name));
      const mtime = stat.mtime.toISOString();
      if (!aggregate.lastReportUpdatedAt || mtime > aggregate.lastReportUpdatedAt) {
        aggregate.lastReportUpdatedAt = mtime;
      }
    });
  }

  aggregate.state = aggregate.activePids.length > 0
    ? 'running'
    : (summary ? 'finished' : 'idle');
  return aggregate;
}

async function main() {
  const args = parseArgs(process.argv);
  do {
    const status = collectStatus(args.runRoot);
    console.log(JSON.stringify(status));
    if (!args.watch || status.state !== 'running') break;
    await sleep(args.intervalSeconds * 1000);
  } while (true);
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});
