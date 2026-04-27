#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const { collectRunState, sleep } = require('./task-state');

function readJson(filePath) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return null;
  }
}

function parseArgs(argv) {
  const args = { runRoot: null, watch: false, intervalSeconds: 600, sampleBandwidth: false };
  for (let index = 2; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === '--watch') {
      args.watch = true;
      continue;
    }
    if (token === '--sample-bandwidth') {
      args.sampleBandwidth = true;
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

function parseNettopCsv(output) {
  const lines = String(output || '').split('\n');
  const blocks = [];
  let current = [];
  lines.forEach((line) => {
    if (line.startsWith('time,')) {
      if (current.length > 0) blocks.push(current);
      current = [];
      return;
    }
    if (line.trim()) current.push(line);
  });
  if (current.length > 0) blocks.push(current);
  return blocks;
}

function sampleLoopbackBandwidth() {
  try {
    const output = execFileSync('nettop', [
      '-P',
      '-L', '4',
      '-s', '1',
      '-d',
      '-x',
      '-m', 'tcp',
      '-t', 'loopback',
    ], {
      encoding: 'utf8',
      timeout: 8000,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const blocks = parseNettopCsv(output).slice(1);
    if (blocks.length === 0) {
      return {
        loopbackNodeInAvgMBps: 0,
        loopbackNodeInMinMBps: 0,
        loopbackNodeInMaxMBps: 0,
        loopbackJavaOutAvgMBps: 0,
        loopbackNodeProcessCount: 0,
      };
    }
    const samples = blocks.map((block) => {
      let javaOut = 0;
      let nodeIn = 0;
      let nodeProcessCount = 0;
      block.forEach((line) => {
        const parts = line.split(',');
        if (parts.length < 6) return;
        const processName = parts[1] || '';
        const bytesIn = Number(parts[4] || 0);
        const bytesOut = Number(parts[5] || 0);
        if (processName.startsWith('java.')) javaOut += bytesOut;
        if (processName.startsWith('node.')) {
          nodeIn += bytesIn;
          nodeProcessCount += 1;
        }
      });
      return {
        javaOutMBps: javaOut / 1024 / 1024,
        nodeInMBps: nodeIn / 1024 / 1024,
        nodeProcessCount,
      };
    });
    const nodeValues = samples.map((sample) => sample.nodeInMBps);
    const javaValues = samples.map((sample) => sample.javaOutMBps);
    return {
      loopbackNodeInAvgMBps: Number((nodeValues.reduce((sum, value) => sum + value, 0) / nodeValues.length).toFixed(2)),
      loopbackNodeInMinMBps: Number(Math.min(...nodeValues).toFixed(2)),
      loopbackNodeInMaxMBps: Number(Math.max(...nodeValues).toFixed(2)),
      loopbackJavaOutAvgMBps: Number((javaValues.reduce((sum, value) => sum + value, 0) / javaValues.length).toFixed(2)),
      loopbackNodeProcessCount: Math.max(...samples.map((sample) => sample.nodeProcessCount)),
    };
  } catch (error) {
    return {
      loopbackBandwidthError: String(error?.message || error),
    };
  }
}

function parseTopCpuLine(output) {
  const text = String(output || '');
  const match = text.match(/CPU usage:\s*([\d.]+)% user,\s*([\d.]+)% sys,\s*([\d.]+)% idle/i);
  if (!match) return {};
  return {
    systemCpuUserPct: Number(match[1]),
    systemCpuSysPct: Number(match[2]),
    systemCpuIdlePct: Number(match[3]),
  };
}

function parseTopPhysMemLine(output) {
  const text = String(output || '');
  const match = text.match(/PhysMem:\s*([^()]+)\(([^,]+),\s*([^)]+)\),\s*([^.]+)\./i);
  if (!match) return {};
  return {
    systemPhysMemUsed: match[1].trim(),
    systemPhysMemWired: match[2].trim(),
    systemPhysMemCompressor: match[3].trim(),
    systemPhysMemUnused: match[4].trim(),
  };
}

function sampleSystemUsage() {
  try {
    const output = execFileSync('top', ['-l', '1'], {
      encoding: 'utf8',
      timeout: 8000,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    return {
      ...parseTopCpuLine(output),
      ...parseTopPhysMemLine(output),
    };
  } catch (error) {
    return {
      systemUsageError: String(error?.message || error),
    };
  }
}

function parsePsRows(output) {
  return String(output || '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const match = line.match(/^(\d+)\s+([\d.]+)\s+(\d+)\s+(.*)$/);
      if (!match) return null;
      return {
        pid: Number(match[1]),
        cpuPct: Number(match[2]),
        rssKb: Number(match[3]),
        command: match[4],
      };
    })
    .filter(Boolean);
}

function sampleProcessUsage(activePids = []) {
  const metrics = {
    workerNodeCount: 0,
    workerNodeCpuPctSum: 0,
    workerNodeRssMbSum: 0,
    thetaServerPid: null,
    thetaServerCpuPct: 0,
    thetaServerRssMb: 0,
  };

  try {
    const pids = Array.from(new Set((Array.isArray(activePids) ? activePids : [])
      .map((value) => Number(value))
      .filter((value) => Number.isInteger(value) && value > 0)));
    if (pids.length > 0) {
      const output = execFileSync('ps', ['-p', pids.join(','), '-o', 'pid=,%cpu=,rss=,command='], {
        encoding: 'utf8',
        timeout: 8000,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const rows = parsePsRows(output);
      metrics.workerNodeCount = rows.length;
      metrics.workerNodeCpuPctSum = Number(rows.reduce((sum, row) => sum + Number(row.cpuPct || 0), 0).toFixed(2));
      metrics.workerNodeRssMbSum = Number((rows.reduce((sum, row) => sum + Number(row.rssKb || 0), 0) / 1024).toFixed(2));
    }
  } catch (error) {
    metrics.workerUsageError = String(error?.message || error);
  }

  try {
    const pidOutput = execFileSync('lsof', ['-tiTCP:25503', '-sTCP:LISTEN'], {
      encoding: 'utf8',
      timeout: 8000,
      stdio: ['ignore', 'pipe', 'pipe'],
    }).trim();
    const thetaPid = Number(String(pidOutput).split(/\s+/).find(Boolean) || 0);
    if (Number.isInteger(thetaPid) && thetaPid > 0) {
      metrics.thetaServerPid = thetaPid;
      const output = execFileSync('ps', ['-p', String(thetaPid), '-o', 'pid=,%cpu=,rss=,command='], {
        encoding: 'utf8',
        timeout: 8000,
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      const row = parsePsRows(output)[0] || null;
      if (row) {
        metrics.thetaServerCpuPct = Number(Number(row.cpuPct || 0).toFixed(2));
        metrics.thetaServerRssMb = Number((Number(row.rssKb || 0) / 1024).toFixed(2));
        metrics.thetaServerCommand = row.command;
      }
    }
  } catch (error) {
    metrics.thetaServerUsageError = String(error?.message || error);
  }

  return metrics;
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

function getMonitorStatePath(runRoot) {
  return path.join(runRoot, 'state', 'control', 'theta-monitor-state.json');
}

function updateThetaMonitorState(runRoot, status) {
  const filePath = getMonitorStatePath(runRoot);
  const previous = readJson(filePath) || {};
  const backlogExists = Number(status.downloadReady || 0) > 0 || Number(status.downloadRunning || 0) > 0;
  const lowTheta = backlogExists
    && status.thetaFullSaturationPossible === true
    && Number(status.thetaActiveRequests || 0) < Number(status.thetaActiveTarget || 0);
  const streak = lowTheta
    ? Math.max(0, Number(previous.lowThetaActiveSampleStreak || 0)) + 1
    : 0;
  const lowBandwidth = backlogExists && Number(status.loopbackNodeInAvgMBps || 0) > 0
    && Number(status.loopbackNodeInAvgMBps || 0) < 1;
  const bandwidthStreak = lowBandwidth
    ? Math.max(0, Number(previous.lowBandwidthSampleStreak || 0)) + 1
    : 0;
  const next = {
    lowThetaActiveSampleStreak: streak,
    lowBandwidthSampleStreak: bandwidthStreak,
    lastCheckedAt: new Date().toISOString(),
    degradedReason: streak >= 3 ? (status.thetaPotentialDegradedReason || 'lock_contention_or_claim_stall') : null,
  };
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, `${JSON.stringify(next, null, 2)}\n`, 'utf8');
  status.thetaLowActiveSampleStreak = streak;
  status.lowBandwidthSampleStreak = bandwidthStreak;
  status.thetaDegradedReason = next.degradedReason;
  return status;
}

async function main() {
  const args = parseArgs(process.argv);
  do {
    const status = collectStatus(args.runRoot);
    if (args.sampleBandwidth) {
      Object.assign(status, sampleLoopbackBandwidth());
      Object.assign(status, sampleSystemUsage());
      Object.assign(status, sampleProcessUsage(status.activePids));
    }
    updateThetaMonitorState(args.runRoot, status);
    console.log(JSON.stringify(status));
    if (!args.watch || status.state !== 'running') break;
    await sleep(args.intervalSeconds * 1000);
  } while (true);
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});
