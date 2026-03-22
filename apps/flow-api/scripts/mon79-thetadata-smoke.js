const fs = require('node:fs');
const path = require('node:path');

const REQUIRED_ENV_HINT = [
  'THETADATA_DOWNLOAD_PATH',
  'THETADATA_BASE_URL (if THETADATA_DOWNLOAD_PATH is relative)',
  'ThetaTerminal must be running with valid creds.txt',
];

function parseIntEnv(name, fallback) {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (Number.isNaN(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer (received: ${raw})`);
  }
  return parsed;
}

function resolveEndpoint(downloadPath, baseUrl) {
  if (!downloadPath) {
    return null;
  }

  if (downloadPath.startsWith('http://') || downloadPath.startsWith('https://')) {
    return downloadPath;
  }

  if (!baseUrl) {
    return null;
  }

  const normalizedBase = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
  const normalizedPath = downloadPath.startsWith('/') ? downloadPath : `/${downloadPath}`;
  return `${normalizedBase}${normalizedPath}`;
}

function failMissingEnv(extraLine) {
  const lines = [
    'MON-79 ThetaData smoke check is blocked: missing required environment configuration.',
    extraLine,
    'Required variables:',
    ...REQUIRED_ENV_HINT.map((item) => `- ${item}`),
    '',
    'Tip: set -a; source .env.mon79.local; set +a',
  ];

  throw new Error(lines.join('\n'));
}

async function main() {
  const downloadPath = (process.env.THETADATA_DOWNLOAD_PATH || '').trim();
  const baseUrl = (process.env.THETADATA_BASE_URL || '').trim();

  if (!downloadPath) {
    failMissingEnv('THETADATA_DOWNLOAD_PATH is empty.');
  }

  const endpoint = resolveEndpoint(downloadPath, baseUrl);
  if (!endpoint) {
    failMissingEnv('THETADATA_BASE_URL is required when THETADATA_DOWNLOAD_PATH is relative.');
  }

  const connectTimeoutMs = parseIntEnv('THETADATA_CONNECT_TIMEOUT_MS', 8000);
  const downloadTimeoutMs = parseIntEnv('THETADATA_DOWNLOAD_TIMEOUT_MS', 20000);
  const timeoutMs = connectTimeoutMs + downloadTimeoutMs;

  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  let response;
  try {
    response = await fetch(endpoint, {
      method: 'GET',
      signal: controller.signal,
    });
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`ThetaData smoke request timed out after ${timeoutMs}ms`);
    }
    throw new Error(`ThetaData smoke request failed: ${error.message}`);
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    const body = await response.text();
    const snippet = body.slice(0, 400);
    throw new Error(`ThetaData smoke request failed (${response.status} ${response.statusText}). Response: ${snippet}`);
  }

  const outputDir = process.env.THETADATA_OUTPUT_DIR || 'artifacts/mon-79';
  const resolvedOutputDir = path.resolve(process.cwd(), outputDir);
  fs.mkdirSync(resolvedOutputDir, { recursive: true });

  const timeLabel = new Date().toISOString().replace(/[:.]/g, '-');
  const artifactPath = path.join(resolvedOutputDir, `thetadata-smoke-${timeLabel}.bin`);
  const bytes = Buffer.from(await response.arrayBuffer());
  fs.writeFileSync(artifactPath, bytes);

  process.stdout.write(`MON-79 ThetaData smoke passed\n`);
  process.stdout.write(`Artifact: ${artifactPath}\n`);
  process.stdout.write(`Bytes: ${bytes.length}\n`);
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});
