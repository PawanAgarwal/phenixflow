#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { collectRunState, sleep } = require('./task-state');

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
  const aggregate = collectRunState(runRoot);
  aggregate.hasSummary = Boolean(summary);
  aggregate.summaryCompletedJobs = Number(summary?.completedJobs || 0);
  aggregate.summaryFailedJobs = Number(summary?.failedJobs || 0);
  aggregate.activePids = [];
  aggregate.workerReports = 0;
  aggregate.lastReportUpdatedAt = null;

  if (fs.existsSync(pidFile)) {
    aggregate.activePids = fs.readFileSync(pidFile, 'utf8')
      .split(/\s+/)
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0 && pidAlive(value));
  }

  const workerFiles = fs.existsSync(reportsRoot)
    ? fs.readdirSync(reportsRoot).filter((name) => /(download|compute)-worker-\d+\.json$/.test(name)).sort()
    : [];
  aggregate.workerReports = workerFiles.length;
  workerFiles.forEach((name) => {
    const stat = fs.statSync(path.join(reportsRoot, name));
    const mtime = stat.mtime.toISOString();
    if (!aggregate.lastReportUpdatedAt || mtime > aggregate.lastReportUpdatedAt) {
      aggregate.lastReportUpdatedAt = mtime;
    }
  });

  if (!aggregate.lastReportUpdatedAt && fs.existsSync(logsRoot)) {
    const logFiles = fs.readdirSync(logsRoot).filter((name) => /worker-\d+\.log$/.test(name)).sort();
    logFiles.forEach((name) => {
      const stat = fs.statSync(path.join(logsRoot, name));
      const mtime = stat.mtime.toISOString();
      if (!aggregate.lastReportUpdatedAt || mtime > aggregate.lastReportUpdatedAt) {
        aggregate.lastReportUpdatedAt = mtime;
      }
    });
  }

  if (aggregate.activePids.length > 0) {
    aggregate.state = 'running';
  } else if (aggregate.totalJobs > 0 && aggregate.completeJobs < aggregate.totalJobs) {
    aggregate.state = 'paused';
  } else if (summary) {
    aggregate.state = 'finished';
  } else {
    aggregate.state = 'idle';
  }
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
