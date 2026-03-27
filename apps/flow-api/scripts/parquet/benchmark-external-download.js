#!/usr/bin/env node
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const execFileAsync = promisify(execFile);

const DEFAULT_TARGETS = [
  { name: 'hetzner_ash_1gb', url: 'https://ash-speed.hetzner.com/1GB.bin' },
  { name: 'ovh_proof_1gb', url: 'https://proof.ovh.net/files/1Gb.dat' },
];

function parseArgs(argv) {
  const out = {
    maxTimeSeconds: 180,
    targets: DEFAULT_TARGETS.slice(),
  };
  const customTargets = [];
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];
    if (token === '--max-time' && next) {
      out.maxTimeSeconds = Math.max(10, Math.trunc(Number(next) || 180));
      index += 1;
    } else if (token === '--url' && next) {
      const value = next.trim();
      customTargets.push({
        name: `custom_${customTargets.length + 1}`,
        url: value,
      });
      index += 1;
    }
  }
  if (customTargets.length > 0) {
    out.targets = customTargets;
  }
  return out;
}

function parseCurlMetrics(stdout) {
  return Object.fromEntries(
    String(stdout || '')
      .trim()
      .split(/\s+/)
      .map((pair) => pair.split('='))
      .filter((pair) => pair.length === 2),
  );
}

async function benchmarkOne(target, maxTimeSeconds) {
  try {
    const { stdout } = await execFileAsync('curl', [
      '-L',
      '-sS',
      '-o', '/dev/null',
      '-w', 'status=%{http_code} size=%{size_download} time=%{time_total} speed=%{speed_download}',
      '--max-time', String(maxTimeSeconds),
      target.url,
    ], { maxBuffer: 1024 * 1024 });
    const metrics = parseCurlMetrics(stdout);
    return {
      name: target.name,
      url: target.url,
      status: Number(metrics.status || 0),
      bytes: Number(metrics.size || 0),
      elapsedMs: Math.round(Number(metrics.time || 0) * 1000),
      bytesPerSecond: Number(metrics.speed || 0),
      timedOut: false,
    };
  } catch (error) {
    const metrics = parseCurlMetrics(error.stdout || error.output || '');
    return {
      name: target.name,
      url: target.url,
      status: Number(metrics.status || 0),
      bytes: Number(metrics.size || 0),
      elapsedMs: Math.round(Number(metrics.time || 0) * 1000),
      bytesPerSecond: Number(metrics.speed || 0),
      timedOut: error.code === 28,
      error: String(error.stderr || error.message || error).trim(),
    };
  }
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const results = [];
  for (const target of options.targets) {
    const result = await benchmarkOne(target, options.maxTimeSeconds);
    console.log(JSON.stringify(result));
    results.push(result);
  }
  console.log(JSON.stringify({
    type: 'summary',
    maxTimeSeconds: options.maxTimeSeconds,
    results,
  }, null, 2));
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});
