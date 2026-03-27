#!/usr/bin/env node
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');

const execFileAsync = promisify(execFile);

const common = require('./common');
const {
  __private: historicalPrivate,
} = require('../../historical-flow');

function parseArgs(argv) {
  const out = {
    symbol: 'AXP',
    dayIso: '2025-03-13',
    startTime: null,
    endTime: null,
    kind: 'quote',
    repeat: 1,
    baseUrl: process.env.THETADATA_BASE_URL || 'http://127.0.0.1:25503',
    skipCurl: false,
    skipParquet: false,
    skipSpool: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];
    if (token === '--symbol' && next) {
      out.symbol = next.trim().toUpperCase();
      index += 1;
    } else if (token === '--day' && next) {
      out.dayIso = next.trim();
      index += 1;
    } else if (token === '--start-time' && next) {
      out.startTime = next.trim();
      index += 1;
    } else if (token === '--end-time' && next) {
      out.endTime = next.trim();
      index += 1;
    } else if (token === '--kind' && next) {
      out.kind = next.trim().toLowerCase();
      index += 1;
    } else if (token === '--repeat' && next) {
      out.repeat = Math.max(1, Math.trunc(Number(next) || 1));
      index += 1;
    } else if (token === '--base-url' && next) {
      out.baseUrl = next.trim();
      index += 1;
    } else if (token === '--skip-curl') {
      out.skipCurl = true;
    } else if (token === '--skip-parquet') {
      out.skipParquet = true;
    } else if (token === '--skip-spool') {
      out.skipSpool = true;
    }
  }
  return out;
}

function yyyymmdd(dayIso) {
  return String(dayIso || '').replace(/-/g, '');
}

function windowOptions({ startTime, endTime }) {
  return {
    startTime: startTime || null,
    endTime: endTime || null,
  };
}

function buildEndpoint(kind, options, env) {
  if (kind === 'trade_quote') {
    return historicalPrivate.resolveThetaEndpoint(
      options.symbol,
      yyyymmdd(options.dayIso),
      env,
      windowOptions(options),
    );
  }
  return historicalPrivate.resolveThetaOptionQuoteEndpoint(
    options.symbol,
    options.dayIso,
    env,
    windowOptions(options),
  );
}

function toMbps(bytes, elapsedMs) {
  if (!Number.isFinite(bytes) || !Number.isFinite(elapsedMs) || elapsedMs <= 0) return null;
  return Number(((bytes / elapsedMs) / 1000).toFixed(2));
}

async function runCurl(url) {
  const { stdout } = await execFileAsync('curl', [
    '-sS',
    '-o', '/dev/null',
    '-w', 'status=%{http_code} size=%{size_download} time=%{time_total} speed=%{speed_download}',
    '--max-time', '600',
    url,
  ], { maxBuffer: 1024 * 1024 });
  const parts = Object.fromEntries(
    String(stdout || '')
      .trim()
      .split(/\s+/)
      .map((pair) => pair.split('='))
      .filter((pair) => pair.length === 2),
  );
  const bytes = Number(parts.size || 0);
  const elapsedMs = Number(parts.time || 0) * 1000;
  return {
    phase: 'curl',
    status: Number(parts.status || 0),
    bytes,
    elapsedMs: Math.round(elapsedMs),
    mbps: toMbps(bytes, elapsedMs),
  };
}

async function streamBytesOnly(url) {
  const startedAt = Date.now();
  const response = await fetch(url);
  let bytes = 0;
  const reader = response.body.getReader();
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) bytes += value.length;
  }
  const elapsedMs = Date.now() - startedAt;
  return {
    phase: 'bytes_only',
    status: response.status,
    bytes,
    elapsedMs,
    mbps: toMbps(bytes, elapsedMs),
  };
}

async function streamParseOnly(url) {
  const startedAt = Date.now();
  const response = await fetch(url);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let bytes = 0;
  let rows = 0;
  let buffer = '';
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) bytes += value.length;
    buffer += decoder.decode(value, { stream: true });
    let newlineIndex = buffer.indexOf('\n');
    while (newlineIndex >= 0) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line) {
        JSON.parse(line);
        rows += 1;
      }
      newlineIndex = buffer.indexOf('\n');
    }
  }
  buffer += decoder.decode();
  if (buffer.trim()) {
    JSON.parse(buffer.trim());
    rows += 1;
  }
  const elapsedMs = Date.now() - startedAt;
  return {
    phase: 'parse_only',
    status: response.status,
    bytes,
    rows,
    elapsedMs,
    mbps: toMbps(bytes, elapsedMs),
  };
}

async function streamNormalizeQuote(url, dayIso) {
  const startedAt = Date.now();
  const response = await fetch(url);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let bytes = 0;
  let parsedRows = 0;
  let normalizedRows = 0;
  let buffer = '';
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    if (value) bytes += value.length;
    buffer += decoder.decode(value, { stream: true });
    let newlineIndex = buffer.indexOf('\n');
    while (newlineIndex >= 0) {
      const line = buffer.slice(0, newlineIndex).trim();
      buffer = buffer.slice(newlineIndex + 1);
      if (line) {
        const parsed = JSON.parse(line);
        parsedRows += 1;
        if (common.__private.normalizeOptionQuoteRow(parsed, dayIso)) {
          normalizedRows += 1;
        }
      }
      newlineIndex = buffer.indexOf('\n');
    }
  }
  buffer += decoder.decode();
  if (buffer.trim()) {
    const parsed = JSON.parse(buffer.trim());
    parsedRows += 1;
    if (common.__private.normalizeOptionQuoteRow(parsed, dayIso)) {
      normalizedRows += 1;
    }
  }
  const elapsedMs = Date.now() - startedAt;
  return {
    phase: 'normalize_quote',
    status: response.status,
    bytes,
    rows: parsedRows,
    normalizedRows,
    elapsedMs,
    mbps: toMbps(bytes, elapsedMs),
  };
}

async function runParquetQuote(options, env) {
  const runRoot = path.join(os.tmpdir(), `phenixflow-theta-bench-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  try {
    const startedAt = Date.now();
    const result = await common.downloadQuoteRequestToParquet({
      runRoot,
      symbol: options.symbol,
      dayIso: options.dayIso,
      partIndex: 0,
      window: windowOptions(options),
      env: {
        ...env,
        PARQUET_RUN_ID: '',
        PHENIXFLOW_PARQUET_ROOT: path.join(os.tmpdir(), 'phenixflow-theta-bench-root'),
      },
    });
    const elapsedMs = Date.now() - startedAt;
    const parquetBytes = result.filePath && fs.existsSync(result.filePath)
      ? fs.statSync(result.filePath).size
      : 0;
    return {
      phase: 'parquet_quote',
      status: 200,
      rows: result.rowCount,
      expirationCount: result.expirations.length,
      elapsedMs,
      parquetBytes,
    };
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
}

async function runSpoolQuote(options, env) {
  const runRoot = path.join(os.tmpdir(), `phenixflow-theta-spool-bench-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`);
  const endpoint = buildEndpoint('quote', options, env);
  const spoolPath = common.getQuoteSpoolPath(runRoot, options.symbol, options.dayIso, 0);
  const filePath = common.getPartitionPartPath(common.getQuotePartitionDir(runRoot, options.symbol, options.dayIso), 0);
  try {
    const download = await common.__private.fetchNdjsonToFile(endpoint, {
      outputPath: spoolPath,
      env: {
        ...env,
        PARQUET_RUN_ID: '',
        PHENIXFLOW_PARQUET_ROOT: path.join(os.tmpdir(), 'phenixflow-theta-bench-root'),
      },
    });
    const spoolBytes = fs.existsSync(spoolPath) ? fs.statSync(spoolPath).size : 0;
    const parseStartedAt = Date.now();
    const parsed = await common.__private.parseQuoteSpoolToParquet({
      spoolPath,
      filePath,
      symbol: options.symbol,
      dayIso: options.dayIso,
      endpoint,
      env: {
        ...env,
        PARQUET_RUN_ID: '',
        PHENIXFLOW_PARQUET_ROOT: path.join(os.tmpdir(), 'phenixflow-theta-bench-root'),
      },
    });
    const parseElapsedMs = Date.now() - parseStartedAt;
    const parquetBytes = fs.existsSync(filePath) ? fs.statSync(filePath).size : 0;
    return [
      {
        phase: 'spool_download',
        status: download.response.status,
        bytes: Number(download.bytesDownloaded || spoolBytes || 0),
        rows: Number(download.rowCount || 0),
        elapsedMs: Number(download.durationMs || 0),
        mbps: toMbps(Number(download.bytesDownloaded || spoolBytes || 0), Number(download.durationMs || 0)),
      },
      {
        phase: 'spool_parse_quote',
        status: download.response.status,
        bytes: spoolBytes,
        rows: parsed.rowCount,
        expirationCount: parsed.expirations.length,
        elapsedMs: parseElapsedMs,
        mbps: toMbps(spoolBytes, parseElapsedMs),
        parquetBytes,
      },
      {
        phase: 'spool_quote_total',
        status: download.response.status,
        bytes: Number(download.bytesDownloaded || spoolBytes || 0),
        rows: parsed.rowCount,
        expirationCount: parsed.expirations.length,
        elapsedMs: Number(download.durationMs || 0) + parseElapsedMs,
        mbps: toMbps(Number(download.bytesDownloaded || spoolBytes || 0), Number(download.durationMs || 0) + parseElapsedMs),
        parquetBytes,
      },
    ];
  } finally {
    fs.rmSync(runRoot, { recursive: true, force: true });
  }
}

async function runOne(kind, options, env) {
  const url = buildEndpoint(kind, options, env);
  const phases = [];
  if (!options.skipCurl) {
    phases.push(await runCurl(url));
  }
  phases.push(await streamBytesOnly(url));
  phases.push(await streamParseOnly(url));
  if (kind === 'quote') {
    phases.push(await streamNormalizeQuote(url, options.dayIso));
    if (!options.skipParquet) {
      phases.push(await runParquetQuote(options, env));
    }
    if (!options.skipSpool) {
      phases.push(...await runSpoolQuote(options, env));
    }
  }
  return {
    kind,
    url,
    phases,
  };
}

function summarize(results) {
  const summary = {};
  for (const result of results) {
    const key = result.kind;
    summary[key] = summary[key] || {};
    for (const phase of result.phases) {
      const bucket = summary[key][phase.phase] || {
        runs: 0,
        elapsedMs: [],
        mbps: [],
      };
      bucket.runs += 1;
      if (Number.isFinite(phase.elapsedMs)) bucket.elapsedMs.push(phase.elapsedMs);
      if (Number.isFinite(phase.mbps)) bucket.mbps.push(phase.mbps);
      summary[key][phase.phase] = bucket;
    }
  }
  for (const phaseMap of Object.values(summary)) {
    for (const bucket of Object.values(phaseMap)) {
      const avg = (values) => values.length
        ? Number((values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(2))
        : null;
      bucket.avgElapsedMs = avg(bucket.elapsedMs);
      bucket.avgMbps = avg(bucket.mbps);
    }
  }
  return summary;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const env = {
    ...process.env,
    THETADATA_BASE_URL: options.baseUrl,
  };
  const kinds = options.kind === 'both'
    ? ['quote', 'trade_quote']
    : [options.kind];
  const results = [];
  for (let repeat = 0; repeat < options.repeat; repeat += 1) {
    for (const kind of kinds) {
      const result = await runOne(kind, options, env);
      result.repeat = repeat + 1;
      result.symbol = options.symbol;
      result.dayIso = options.dayIso;
      result.startTime = options.startTime;
      result.endTime = options.endTime;
      console.log(JSON.stringify(result));
      results.push(result);
    }
  }
  console.log(JSON.stringify({
    type: 'summary',
    symbol: options.symbol,
    dayIso: options.dayIso,
    startTime: options.startTime,
    endTime: options.endTime,
    repeat: options.repeat,
    summary: summarize(results),
  }, null, 2));
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});
