const fs = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');

function parseRetryDelays(raw) {
  if (!raw || typeof raw !== 'string') return [2000, 5000, 15000];
  const delays = raw
    .split(',')
    .map((item) => Number(item.trim()))
    .filter((value) => Number.isFinite(value) && value > 0);
  return delays.length ? delays : [2000, 5000, 15000];
}

function buildAuthHeaders(config) {
  const headers = {
    Accept: '*/*',
  };

  if (config.apiKey) {
    headers.Authorization = `Bearer ${config.apiKey}`;
  } else if (config.username && config.password) {
    const token = Buffer.from(`${config.username}:${config.password}`, 'utf8').toString('base64');
    headers.Authorization = `Basic ${token}`;
  }

  return headers;
}

function resolveUrl(baseUrl, pathOrUrl) {
  if (!pathOrUrl) {
    throw new Error('Missing URL/path value');
  }

  try {
    return new URL(pathOrUrl).toString();
  } catch {
    if (!baseUrl) {
      throw new Error(`Unable to resolve relative URL without THETADATA_BASE_URL: ${pathOrUrl}`);
    }
    return new URL(pathOrUrl, baseUrl).toString();
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function withTimeout(timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs);
  return {
    signal: controller.signal,
    clear: () => clearTimeout(timer),
  };
}

function isRetryableStatus(code) {
  return code === 408 || code === 429 || (code >= 500 && code <= 599);
}

async function fetchWithRetry(options) {
  const {
    fetchImpl = fetch,
    url,
    method = 'GET',
    headers,
    timeoutMs,
    retryDelaysMs,
    stepName,
    logs,
  } = options;

  let attempt = 0;
  let lastError = null;
  const maxAttempts = retryDelaysMs.length + 1;

  while (attempt < maxAttempts) {
    attempt += 1;
    const startedAt = Date.now();
    const timeout = withTimeout(timeoutMs);

    try {
      const response = await fetchImpl(url, { method, headers, signal: timeout.signal });
      const elapsedMs = Date.now() - startedAt;

      logs.push({
        step: stepName,
        attempt,
        url,
        elapsedMs,
        status: response.status,
      });

      if (!response.ok && isRetryableStatus(response.status) && attempt < maxAttempts) {
        await sleep(retryDelaysMs[attempt - 1]);
        continue;
      }

      const bodyBytes = Buffer.from(await response.arrayBuffer());
      return {
        ok: response.ok,
        status: response.status,
        headers: Object.fromEntries(response.headers.entries()),
        bodyBytes,
        attempts: attempt,
      };
    } catch (error) {
      const elapsedMs = Date.now() - startedAt;
      lastError = error;

      logs.push({
        step: stepName,
        attempt,
        url,
        elapsedMs,
        error: error && error.message ? error.message : String(error),
      });

      if (attempt < maxAttempts) {
        await sleep(retryDelaysMs[attempt - 1]);
        continue;
      }
    } finally {
      timeout.clear();
    }
  }

  throw lastError || new Error(`Request failed for step=${stepName}`);
}

function parseMaybeJson(buffer) {
  try {
    return JSON.parse(buffer.toString('utf8'));
  } catch {
    return null;
  }
}

function inferRowCount(jsonPayload) {
  if (Array.isArray(jsonPayload)) return jsonPayload.length;
  if (jsonPayload && Array.isArray(jsonPayload.data)) return jsonPayload.data.length;
  if (jsonPayload && Array.isArray(jsonPayload.rows)) return jsonPayload.rows.length;
  if (jsonPayload && Array.isArray(jsonPayload.results)) return jsonPayload.results.length;
  return null;
}

async function runThetaDataSmoke(config = {}) {
  const startedAt = new Date().toISOString();
  const retryDelaysMs = parseRetryDelays(config.retryDelaysMs || process.env.THETADATA_RETRY_DELAYS_MS);

  const resolved = {
    baseUrl: config.baseUrl || process.env.THETADATA_BASE_URL,
    entitlementPath: config.entitlementPath || process.env.THETADATA_ENTITLEMENT_PATH || '/v2/system/entitlements',
    downloadPath: config.downloadPath || process.env.THETADATA_DOWNLOAD_PATH,
    connectTimeoutMs: Number(config.connectTimeoutMs || process.env.THETADATA_CONNECT_TIMEOUT_MS || 8000),
    downloadTimeoutMs: Number(config.downloadTimeoutMs || process.env.THETADATA_DOWNLOAD_TIMEOUT_MS || 20000),
    outputDir: config.outputDir || process.env.THETADATA_OUTPUT_DIR || path.join('artifacts', 'mon-79'),
    username: config.username || process.env.THETADATA_USERNAME,
    password: config.password || process.env.THETADATA_PASSWORD,
    apiKey: config.apiKey || process.env.THETADATA_API_KEY,
  };

  if (!resolved.downloadPath) {
    throw new Error('Missing THETADATA_DOWNLOAD_PATH (or config.downloadPath) for real download smoke artifact');
  }

  const headers = buildAuthHeaders(resolved);
  const logs = [];

  const entitlementUrl = resolveUrl(resolved.baseUrl, resolved.entitlementPath);
  const downloadUrl = resolveUrl(resolved.baseUrl, resolved.downloadPath);

  const entitlement = await fetchWithRetry({
    url: entitlementUrl,
    method: 'GET',
    headers,
    timeoutMs: resolved.connectTimeoutMs,
    retryDelaysMs,
    stepName: 'entitlement_probe',
    logs,
  });

  if (!entitlement.ok) {
    const bodyPreview = entitlement.bodyBytes.toString('utf8').slice(0, 500);
    throw new Error(`Entitlement probe failed: status=${entitlement.status} body=${bodyPreview}`);
  }

  const download = await fetchWithRetry({
    url: downloadUrl,
    method: 'GET',
    headers,
    timeoutMs: resolved.downloadTimeoutMs,
    retryDelaysMs,
    stepName: 'download_artifact',
    logs,
  });

  if (!download.ok) {
    const bodyPreview = download.bodyBytes.toString('utf8').slice(0, 500);
    throw new Error(`Download step failed: status=${download.status} body=${bodyPreview}`);
  }

  const endedAt = new Date().toISOString();
  const timestamp = endedAt.replace(/[:.]/g, '-');

  await fs.mkdir(resolved.outputDir, { recursive: true });

  const contentType = download.headers['content-type'] || 'application/octet-stream';
  const ext = contentType.includes('json') ? 'json' : contentType.includes('csv') ? 'csv' : 'bin';
  const artifactPath = path.join(resolved.outputDir, `thetadata-download-${timestamp}.${ext}`);
  await fs.writeFile(artifactPath, download.bodyBytes);

  const sha256 = crypto.createHash('sha256').update(download.bodyBytes).digest('hex');
  const jsonPayload = parseMaybeJson(download.bodyBytes);
  const rowCount = inferRowCount(jsonPayload);

  const report = {
    status: 'ok',
    startedAt,
    endedAt,
    retryBackoff: {
      policy: 'max_attempts = 1 + retryDelays, delays applied between attempts',
      retryDelaysMs,
      retryableStatusCodes: [408, 429, '5xx'],
    },
    timeoutTaxonomy: {
      entitlementProbeTimeoutMs: resolved.connectTimeoutMs,
      artifactDownloadTimeoutMs: resolved.downloadTimeoutMs,
    },
    assertions: {
      entitlementOk: entitlement.ok,
      artifactDownloadOk: download.ok,
      artifactBytesGtZero: download.bodyBytes.byteLength > 0,
    },
    endpoints: {
      entitlementUrl,
      downloadUrl,
    },
    metrics: {
      entitlementStatus: entitlement.status,
      entitlementAttempts: entitlement.attempts,
      downloadStatus: download.status,
      downloadAttempts: download.attempts,
    },
    artifact: {
      path: artifactPath,
      bytes: download.bodyBytes.byteLength,
      sha256,
      contentType,
      rowCount,
    },
    logs,
  };

  const reportPath = path.join(resolved.outputDir, `thetadata-smoke-report-${timestamp}.json`);
  await fs.writeFile(reportPath, JSON.stringify(report, null, 2));

  return {
    ...report,
    reportPath,
  };
}

module.exports = {
  runThetaDataSmoke,
  parseRetryDelays,
  resolveUrl,
  inferRowCount,
  fetchWithRetry,
};
