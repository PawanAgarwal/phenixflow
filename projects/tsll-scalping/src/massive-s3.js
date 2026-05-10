const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const MASSIVE_S3_ENDPOINT = 'https://files.massive.com';
const MASSIVE_S3_HOST = 'files.massive.com';
const MASSIVE_S3_BUCKET = 'flatfiles';
const MASSIVE_S3_REGION = 'us-east-1';
const MASSIVE_S3_SERVICE = 's3';

function parseEnvFileLine(rawLine) {
  const line = String(rawLine || '').trim();
  if (!line || line.startsWith('#')) return null;
  const splitIndex = line.indexOf('=');
  if (splitIndex <= 0) return null;
  const key = line.slice(0, splitIndex).replace(/^export\s+/, '').trim();
  let value = line.slice(splitIndex + 1).trim();
  if (
    (value.startsWith('"') && value.endsWith('"'))
    || (value.startsWith("'") && value.endsWith("'"))
  ) {
    value = value.slice(1, -1);
  }
  return key ? { key, value } : null;
}

function loadDotEnvIfExists(envPath, env = process.env) {
  if (!envPath || !fs.existsSync(envPath)) return { loaded: false, keys: [] };
  const keys = [];
  fs.readFileSync(envPath, 'utf8').split(/\r?\n/).forEach((line) => {
    const parsed = parseEnvFileLine(line);
    if (!parsed) return;
    if (env[parsed.key] === undefined) env[parsed.key] = parsed.value;
    keys.push(parsed.key);
  });
  return { loaded: true, keys };
}

function loadMassiveEnv(env = process.env) {
  const envPaths = [
    path.resolve(__dirname, '..', '..', '..', '.env.local'),
    env.MASSIVE_ENV_FILE,
    path.join(os.homedir(), 'config', 'massive', '.env.local'),
  ].filter(Boolean).map((envPath) => path.resolve(envPath));
  return envPaths.map((envPath) => ({
    envPath,
    ...loadDotEnvIfExists(envPath, env),
  }));
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
  return {
    accessKey,
    secretKey,
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

function buildObjectUrl(objectKey) {
  return `${MASSIVE_S3_ENDPOINT}/${MASSIVE_S3_BUCKET}/${objectKey}`;
}

module.exports = {
  MASSIVE_S3_BUCKET,
  MASSIVE_S3_ENDPOINT,
  MASSIVE_S3_HOST,
  buildAuthorizationHeaders,
  buildObjectKey,
  buildObjectUrl,
  loadMassiveEnv,
  resolveCredentials,
};
