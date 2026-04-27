#!/usr/bin/env node
const crypto = require('node:crypto');
const fs = require('node:fs');
const https = require('node:https');
const os = require('node:os');
const path = require('node:path');

const {
  buildRunId,
  resolveParquetRoot,
  writeJsonFile,
} = require('./common');

const MASSIVE_S3_ENDPOINT = 'https://files.massive.com';
const MASSIVE_S3_HOST = 'files.massive.com';
const MASSIVE_S3_BUCKET = 'flatfiles';
const MASSIVE_S3_REGION = 'us-east-1';
const MASSIVE_S3_SERVICE = 's3';
const DEFAULT_DATASET = 'us_options_opra/minute_aggs_v1';
const DEFAULT_RETRY_ATTEMPTS = 6;

function parseArgs(argv) {
  const currentDate = new Date();
  const defaultMonth = resolveMostRecentCompletedApril(currentDate);
  const out = {
    dataset: DEFAULT_DATASET,
    month: defaultMonth,
    startDate: `${defaultMonth}-01`,
    endDate: lastDayOfMonthIso(defaultMonth),
    runId: buildRunId('massive-options-1m'),
    parquetRoot: resolveParquetRoot(process.env),
    concurrency: Math.max(4, Math.min(24, Math.trunc((os.cpus()?.length || 8) * 2))),
    retryAttempts: DEFAULT_RETRY_ATTEMPTS,
    overwrite: false,
    dryRun: false,
    includeWeekends: false,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    const next = argv[index + 1];
    if (token === '--month' && next) {
      out.month = normalizeMonth(next) || out.month;
      out.startDate = `${out.month}-01`;
      out.endDate = lastDayOfMonthIso(out.month);
      index += 1;
    } else if (token === '--start-date' && next) {
      out.startDate = normalizeIsoDate(next) || out.startDate;
      index += 1;
    } else if (token === '--end-date' && next) {
      out.endDate = normalizeIsoDate(next) || out.endDate;
      index += 1;
    } else if (token === '--dataset' && next) {
      out.dataset = String(next).trim().replace(/^\/+|\/+$/g, '') || out.dataset;
      index += 1;
    } else if (token === '--run-id' && next) {
      out.runId = String(next).trim() || out.runId;
      index += 1;
    } else if (token === '--parquet-root' && next) {
      out.parquetRoot = path.resolve(next);
      index += 1;
    } else if (token === '--concurrency' && next) {
      const parsed = Math.trunc(Number(next) || out.concurrency);
      out.concurrency = Math.max(1, Math.min(64, parsed));
      index += 1;
    } else if (token === '--retry-attempts' && next) {
      const parsed = Math.trunc(Number(next) || out.retryAttempts);
      out.retryAttempts = Math.max(1, Math.min(20, parsed));
      index += 1;
    } else if (token === '--overwrite') {
      out.overwrite = true;
    } else if (token === '--dry-run') {
      out.dryRun = true;
    } else if (token === '--include-weekends') {
      out.includeWeekends = true;
    }
  }

  if (out.startDate > out.endDate) {
    throw new Error(`invalid_date_range:${out.startDate}:${out.endDate}`);
  }
  return out;
}

function normalizeIsoDate(rawValue) {
  if (typeof rawValue !== 'string') return null;
  const trimmed = rawValue.trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(trimmed) ? trimmed : null;
}

function normalizeMonth(rawValue) {
  if (typeof rawValue !== 'string') return null;
  const trimmed = rawValue.trim();
  return /^\d{4}-\d{2}$/.test(trimmed) ? trimmed : null;
}

function lastDayOfMonthIso(monthIso) {
  const normalized = normalizeMonth(monthIso);
  if (!normalized) throw new Error(`invalid_month:${monthIso}`);
  const [year, month] = normalized.split('-').map((value) => Number(value));
  const last = new Date(Date.UTC(year, month, 0));
  return `${normalized}-${String(last.getUTCDate()).padStart(2, '0')}`;
}

function resolveMostRecentCompletedApril(currentDate) {
  const year = currentDate.getUTCFullYear();
  const month = currentDate.getUTCMonth() + 1;
  const targetYear = month >= 5 ? year : year - 1;
  return `${String(targetYear).padStart(4, '0')}-04`;
}

function listDatesInRange(startDate, endDate, { includeWeekends = false } = {}) {
  const out = [];
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  while (cursor <= end) {
    const dayIso = cursor.toISOString().slice(0, 10);
    const dayOfWeek = cursor.getUTCDay();
    if (includeWeekends || (dayOfWeek !== 0 && dayOfWeek !== 6)) {
      out.push(dayIso);
    }
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

function resolveCredentials(env = process.env) {
  const accessKey = String(
    env.MASSIVE_S3_ACCESS_KEY
    || env.MASSIVE_ACCESS_KEY
    || env.POLYGON_S3_ACCESS_KEY
    || '',
  ).trim();
  const secretKey = String(
    env.MASSIVE_S3_SECRET_KEY
    || env.MASSIVE_SECRET_KEY
    || env.POLYGON_S3_SECRET_KEY
    || '',
  ).trim();
  const apiKey = String(
    env.MASSIVE_API_KEY
    || env.POLYGON_API_KEY
    || '',
  ).trim();
  return {
    accessKey,
    secretKey,
    apiKey,
    ready: Boolean(accessKey && secretKey),
  };
}

function hmac(key, value, encoding) {
  return crypto.createHmac('sha256', key).update(value, 'utf8').digest(encoding);
}

function sha256(value, encoding = 'hex') {
  return crypto.createHash('sha256').update(value, 'utf8').digest(encoding);
}

function formatAmzDate(date = new Date()) {
  return date.toISOString().replace(/[:-]|\.\d{3}/g, '');
}

function buildAuthorizationHeaders({
  method,
  objectKey,
  credentials,
  now = new Date(),
}) {
  const amzDate = formatAmzDate(now);
  const dateStamp = amzDate.slice(0, 8);
  const canonicalUri = `/${MASSIVE_S3_BUCKET}/${objectKey}`;
  const payloadHash = 'UNSIGNED-PAYLOAD';
  const canonicalHeaders = [
    `host:${MASSIVE_S3_HOST}`,
    `x-amz-content-sha256:${payloadHash}`,
    `x-amz-date:${amzDate}`,
  ].join('\n');
  const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';
  const canonicalRequest = [
    method,
    canonicalUri,
    '',
    `${canonicalHeaders}\n`,
    signedHeaders,
    payloadHash,
  ].join('\n');
  const credentialScope = `${dateStamp}/${MASSIVE_S3_REGION}/${MASSIVE_S3_SERVICE}/aws4_request`;
  const stringToSign = [
    'AWS4-HMAC-SHA256',
    amzDate,
    credentialScope,
    sha256(canonicalRequest),
  ].join('\n');
  const kDate = hmac(`AWS4${credentials.secretKey}`, dateStamp);
  const kRegion = hmac(kDate, MASSIVE_S3_REGION);
  const kService = hmac(kRegion, MASSIVE_S3_SERVICE);
  const kSigning = hmac(kService, 'aws4_request');
  const signature = hmac(kSigning, stringToSign, 'hex');
  return {
    Authorization: `AWS4-HMAC-SHA256 Credential=${credentials.accessKey}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
    'x-amz-content-sha256': payloadHash,
    'x-amz-date': amzDate,
  };
}

function buildObjectKey(dataset, dayIso) {
  const normalizedDataset = String(dataset || '').trim().replace(/^\/+|\/+$/g, '');
  const [year, month] = dayIso.split('-');
  return `${normalizedDataset}/${year}/${month}/${dayIso}.csv.gz`;
}

function buildOutputPath(runRoot, dataset, dayIso) {
  const normalizedDataset = String(dataset || '').trim().replace(/^\/+|\/+$/g, '');
  const segments = normalizedDataset.split('/');
  const [year, month] = dayIso.split('-');
  return path.join(
    runRoot,
    'datasets',
    'external',
    'massive',
    ...segments,
    `year=${year}`,
    `month=${month}`,
    `trade_date=${dayIso}`,
    `${dayIso}.csv.gz`,
  );
}

function ensureDir(target) {
  fs.mkdirSync(target, { recursive: true });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, Math.trunc(ms || 0))));
}

function isRetryableStatus(statusCode) {
  return statusCode === 408 || statusCode === 425 || statusCode === 429 || (statusCode >= 500 && statusCode <= 599);
}

function isRetryableError(error) {
  const code = String(error?.code || '');
  return code === 'ECONNRESET' || code === 'ETIMEDOUT' || code === 'ECONNREFUSED' || code === 'EAI_AGAIN';
}

function computeRetryDelayMs(attempt) {
  const cappedAttempt = Math.max(0, Math.min(10, Math.trunc(attempt || 0)));
  const base = 1000 * (2 ** cappedAttempt);
  return Math.min(30000, base + Math.trunc(Math.random() * 500));
}

async function withRetry(taskFn, { retryAttempts, label }) {
  let lastError = null;
  for (let attempt = 0; attempt < retryAttempts; attempt += 1) {
    try {
      return await taskFn(attempt);
    } catch (error) {
      lastError = error;
      if (attempt >= retryAttempts - 1) break;
      if (!isRetryableError(error) && !isRetryableStatus(Number(error?.statusCode || 0))) {
        break;
      }
      const delayMs = computeRetryDelayMs(attempt);
      console.warn('[MASSIVE_DOWNLOAD_RETRY]', JSON.stringify({
        label,
        attempt: attempt + 1,
        retryAttempts,
        delayMs,
        statusCode: Number(error?.statusCode || 0) || null,
        error: String(error?.message || error),
      }));
      await sleep(delayMs);
    }
  }
  throw lastError;
}

async function downloadOne({
  dayIso,
  dataset,
  outputPath,
  credentials,
  retryAttempts,
  overwrite,
}) {
  if (!overwrite && fs.existsSync(outputPath)) {
    const stat = fs.statSync(outputPath);
    return {
      dayIso,
      dataset,
      outputPath,
      bytesDownloaded: stat.size,
      skipped: true,
    };
  }

  ensureDir(path.dirname(outputPath));
  const tempPath = `${outputPath}.tmp`;
  fs.rmSync(tempPath, { force: true });
  const objectKey = buildObjectKey(dataset, dayIso);
  const url = `${MASSIVE_S3_ENDPOINT}/${MASSIVE_S3_BUCKET}/${objectKey}`;

  return withRetry(async () => new Promise((resolve, reject) => {
    const headers = buildAuthorizationHeaders({
      method: 'GET',
      objectKey,
      credentials,
    });
    const startedAtMs = Date.now();
    const request = https.request(url, {
      method: 'GET',
      headers,
      timeout: 120000,
    }, (response) => {
      const statusCode = Number(response.statusCode || 0);
      if (statusCode !== 200) {
        let body = '';
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          body += chunk;
        });
        response.on('end', () => {
          const snippet = body.trim().replace(/\s+/g, ' ').slice(0, 240);
          const error = new Error(`massive_download_failed:${statusCode}:${dayIso}:${snippet || 'empty_body'}`);
          error.statusCode = statusCode;
          reject(error);
        });
        return;
      }
      const output = fs.createWriteStream(tempPath, { flags: 'w' });
      let bytesDownloaded = 0;
      response.on('data', (chunk) => {
        bytesDownloaded += chunk.length;
      });
      response.on('error', (error) => {
        output.destroy(error);
      });
      output.on('error', (error) => {
        fs.rmSync(tempPath, { force: true });
        reject(error);
      });
      output.on('finish', () => {
        output.close(() => {
          fs.renameSync(tempPath, outputPath);
          resolve({
            dayIso,
            dataset,
            outputPath,
            bytesDownloaded,
            elapsedMs: Date.now() - startedAtMs,
            skipped: false,
            url,
          });
        });
      });
      response.pipe(output);
    });
    request.on('timeout', () => {
      request.destroy(Object.assign(new Error(`massive_download_timeout:${dayIso}`), { code: 'ETIMEDOUT' }));
    });
    request.on('error', (error) => {
      fs.rmSync(tempPath, { force: true });
      reject(error);
    });
    request.end();
  }), {
    retryAttempts,
    label: `${dataset}:${dayIso}`,
  });
}

async function runTasksWithConcurrency(items, concurrency, taskFn) {
  const results = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (true) {
      const currentIndex = cursor;
      cursor += 1;
      if (currentIndex >= items.length) return;
      results[currentIndex] = await taskFn(items[currentIndex], currentIndex);
    }
  }
  const workers = Array.from({ length: Math.max(1, Math.min(concurrency, items.length || 1)) }, () => worker());
  await Promise.all(workers);
  return results;
}

async function main() {
  const options = parseArgs(process.argv.slice(2));
  const credentials = resolveCredentials(process.env);
  const runRoot = path.join(options.parquetRoot, 'runs', options.runId);
  const tradingDates = listDatesInRange(options.startDate, options.endDate, {
    includeWeekends: options.includeWeekends,
  });
  const plan = tradingDates.map((dayIso) => ({
    dayIso,
    dataset: options.dataset,
    objectKey: buildObjectKey(options.dataset, dayIso),
    outputPath: buildOutputPath(runRoot, options.dataset, dayIso),
  }));

  const manifest = {
    runId: options.runId,
    runRoot,
    parquetRoot: options.parquetRoot,
    dataset: options.dataset,
    startDate: options.startDate,
    endDate: options.endDate,
    concurrency: options.concurrency,
    retryAttempts: options.retryAttempts,
    dryRun: options.dryRun,
    credentialsReady: credentials.ready,
    apiKeyProvided: Boolean(credentials.apiKey),
    notes: !credentials.ready && credentials.apiKey
      ? ['Massive flat files require S3 access key plus secret key; REST API key alone is not sufficient for bulk S3 downloads.']
      : [],
    files: plan,
  };

  writeJsonFile(path.join(runRoot, 'manifests', 'massive-options-1m.json'), manifest);

  if (options.dryRun) {
    console.log(JSON.stringify({
      type: 'dry_run',
      runId: options.runId,
      runRoot,
      dataset: options.dataset,
      fileCount: plan.length,
      firstDate: plan[0]?.dayIso || null,
      lastDate: plan[plan.length - 1]?.dayIso || null,
      credentialsReady: credentials.ready,
      apiKeyProvided: Boolean(credentials.apiKey),
    }, null, 2));
    return;
  }

  if (!credentials.ready) {
    throw new Error('massive_s3_credentials_missing: set MASSIVE_S3_ACCESS_KEY and MASSIVE_S3_SECRET_KEY from the Massive dashboard flat-files section');
  }

  let completed = 0;
  let skipped = 0;
  let failed = 0;
  let totalBytes = 0;
  const startedAtMs = Date.now();
  const results = await runTasksWithConcurrency(plan, options.concurrency, async (entry) => {
    try {
      const result = await downloadOne({
        dayIso: entry.dayIso,
        dataset: entry.dataset,
        outputPath: entry.outputPath,
        credentials,
        retryAttempts: options.retryAttempts,
        overwrite: options.overwrite,
      });
      completed += 1;
      skipped += result.skipped ? 1 : 0;
      totalBytes += Number(result.bytesDownloaded || 0);
      console.log('[MASSIVE_DOWNLOAD_PROGRESS]', JSON.stringify({
        completed,
        total: plan.length,
        skipped,
        failed,
        dayIso: entry.dayIso,
        bytesDownloaded: result.bytesDownloaded,
        outputPath: result.outputPath,
      }));
      return { ok: true, ...result };
    } catch (error) {
      failed += 1;
      console.error('[MASSIVE_DOWNLOAD_FAILED]', JSON.stringify({
        completed,
        total: plan.length,
        skipped,
        failed,
        dayIso: entry.dayIso,
        error: String(error?.message || error),
      }));
      return {
        ok: false,
        dayIso: entry.dayIso,
        dataset: entry.dataset,
        outputPath: entry.outputPath,
        error: String(error?.message || error),
      };
    }
  });

  const summary = {
    type: 'summary',
    runId: options.runId,
    runRoot,
    dataset: options.dataset,
    startDate: options.startDate,
    endDate: options.endDate,
    concurrency: options.concurrency,
    retryAttempts: options.retryAttempts,
    completed,
    skipped,
    failed,
    totalFiles: plan.length,
    totalBytes,
    elapsedMs: Date.now() - startedAtMs,
  };

  writeJsonFile(path.join(runRoot, 'reports', 'massive-options-1m-summary.json'), {
    summary,
    results,
  });
  console.log(JSON.stringify(summary, null, 2));
  if (failed > 0) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error?.stack || error?.message || error);
  process.exitCode = 1;
});
