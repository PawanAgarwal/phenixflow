const { execFileSync } = require('node:child_process');

const READ_BACKENDS = new Set(['auto', 'clickhouse']);
const WRITE_BACKENDS = new Set(['auto', 'clickhouse']);
const DEFAULT_INSERT_CHUNK_SIZE = 5000;
const DEFAULT_INSERT_MAX_BYTES = 32 * 1024 * 1024;
const DEFAULT_QUERY_MAX_BUFFER_BYTES = 1024 * 1024 * 1024;

function resolveFlowReadBackend(env = process.env) {
  const configured = String(env.PHENIX_FLOW_READ_BACKEND || '').trim().toLowerCase();
  if (configured === 'sqlite') return 'clickhouse';
  if (READ_BACKENDS.has(configured)) return configured;
  return 'clickhouse';
}

function resolveFlowWriteBackend(env = process.env) {
  const configured = String(env.PHENIX_FLOW_WRITE_BACKEND || '').trim().toLowerCase();
  if (configured === 'sqlite') return 'clickhouse';
  if (WRITE_BACKENDS.has(configured)) return configured;
  return 'clickhouse';
}

function resolveClickHouseConfig(env = process.env) {
  return {
    host: String(env.CLICKHOUSE_HOST || '127.0.0.1').trim() || '127.0.0.1',
    port: String(env.CLICKHOUSE_PORT || '9000').trim() || '9000',
    user: String(env.CLICKHOUSE_USER || 'default').trim() || 'default',
    password: String(env.CLICKHOUSE_PASSWORD || ''),
    database: String(env.CLICKHOUSE_DATABASE || 'options').trim() || 'options',
    connectTimeoutSec: String(env.CLICKHOUSE_CONNECT_TIMEOUT_SEC || '10').trim() || '10',
    sendTimeoutSec: String(env.CLICKHOUSE_SEND_TIMEOUT_SEC || '60').trim() || '60',
    receiveTimeoutSec: String(env.CLICKHOUSE_RECEIVE_TIMEOUT_SEC || '60').trim() || '60',
  };
}

function buildArtifactPath(env = process.env) {
  const config = resolveClickHouseConfig(env);
  return `clickhouse://${config.host}:${config.port}/${config.database}`;
}

function parseJsonEachRow(raw = '') {
  return String(raw || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function buildClientArgs(query, params = {}, env = process.env, format = 'JSONEachRow') {
  const config = resolveClickHouseConfig(env);
  const args = [
    'client',
    '--host', config.host,
    '--port', String(config.port),
    '--user', config.user,
    '--connect_timeout', String(config.connectTimeoutSec),
    '--send_timeout', String(config.sendTimeoutSec),
    '--receive_timeout', String(config.receiveTimeoutSec),
    '--query', format ? `${query}\nFORMAT ${format}` : query,
  ];

  if (config.password) {
    args.push('--password', config.password);
  }

  Object.entries(params).forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    args.push(`--param_${key}=${String(value)}`);
  });

  return args;
}

function resolveQueryMaxBufferBytes(env = process.env) {
  const configured = Number(env.CLICKHOUSE_QUERY_MAX_BUFFER_BYTES);
  if (Number.isFinite(configured) && configured >= 1024 * 1024) {
    return Math.trunc(configured);
  }
  return DEFAULT_QUERY_MAX_BUFFER_BYTES;
}

function queryRowsSync(query, params = {}, env = process.env) {
  const output = execFileSync('clickhouse', buildClientArgs(query, params, env, 'JSONEachRow'), {
    encoding: 'utf8',
    maxBuffer: resolveQueryMaxBufferBytes(env),
  });
  return parseJsonEachRow(output);
}

function execQuerySync(query, params = {}, env = process.env) {
  return execFileSync('clickhouse', buildClientArgs(query, params, env, null), {
    encoding: 'utf8',
    maxBuffer: resolveQueryMaxBufferBytes(env),
  });
}

function sanitizeJsonRow(row = {}) {
  const sanitized = {};
  Object.entries(row).forEach(([key, value]) => {
    sanitized[key] = value === undefined ? null : value;
  });
  return sanitized;
}

function isIterableRows(rows) {
  return rows !== null
    && rows !== undefined
    && typeof rows[Symbol.iterator] === 'function';
}

function resolveInsertMaxBytes(options = {}, env = process.env) {
  const configured = Number(options.maxChunkBytes || env.CLICKHOUSE_INSERT_MAX_BYTES);
  if (Number.isFinite(configured) && configured >= 1024) {
    return Math.trunc(configured);
  }
  return DEFAULT_INSERT_MAX_BYTES;
}

function buildJsonInsertChunks(rows = [], options = {}, env = process.env) {
  if (!Array.isArray(rows) || rows.length === 0) return [];

  const chunkSize = Math.max(1, Math.trunc(Number(options.chunkSize || DEFAULT_INSERT_CHUNK_SIZE)));
  const maxChunkBytes = resolveInsertMaxBytes(options, env);
  const chunks = [];

  for (let offset = 0; offset < rows.length;) {
    const lines = [];
    let chunkBytes = 0;

    while (offset < rows.length && lines.length < chunkSize) {
      const line = `${JSON.stringify(sanitizeJsonRow(rows[offset]))}\n`;
      const lineBytes = Buffer.byteLength(line);
      if (lines.length > 0 && chunkBytes + lineBytes > maxChunkBytes) {
        break;
      }
      lines.push(line);
      chunkBytes += lineBytes;
      offset += 1;
    }

    chunks.push(lines.join(''));
  }

  return chunks;
}

function execInsertChunkSync(insertQuery, chunk, env = process.env) {
  execFileSync('clickhouse', buildClientArgs(`${insertQuery}
SETTINGS date_time_input_format='best_effort', input_format_parallel_parsing=1`, {}, env, 'JSONEachRow'), {
    encoding: 'utf8',
    input: chunk,
    maxBuffer: resolveQueryMaxBufferBytes(env),
  });
}

function insertJsonRowsSync(insertQuery, rows = [], env = process.env, options = {}) {
  if (!isIterableRows(rows)) return 0;
  if (Array.isArray(rows) && rows.length === 0) return 0;

  let inserted = 0;
  const chunkSize = Math.max(1, Math.trunc(Number(options.chunkSize || DEFAULT_INSERT_CHUNK_SIZE)));
  const maxChunkBytes = resolveInsertMaxBytes(options, env);
  let lines = [];
  let chunkBytes = 0;
  let chunkRowCount = 0;

  const flushChunk = () => {
    if (chunkRowCount === 0) return;
    execInsertChunkSync(insertQuery, lines.join(''), env);
    inserted += chunkRowCount;
    lines = [];
    chunkBytes = 0;
    chunkRowCount = 0;
  };

  for (const row of rows) {
    const line = `${JSON.stringify(sanitizeJsonRow(row))}\n`;
    const lineBytes = Buffer.byteLength(line);
    if (chunkRowCount > 0 && (chunkRowCount >= chunkSize || chunkBytes + lineBytes > maxChunkBytes)) {
      flushChunk();
    }
    lines.push(line);
    chunkBytes += lineBytes;
    chunkRowCount += 1;
  }

  flushChunk();

  return inserted;
}

module.exports = {
  resolveFlowReadBackend,
  resolveFlowWriteBackend,
  resolveClickHouseConfig,
  buildArtifactPath,
  queryRowsSync,
  execQuerySync,
  insertJsonRowsSync,
  __private: {
    parseJsonEachRow,
    buildClientArgs,
    resolveQueryMaxBufferBytes,
    sanitizeJsonRow,
    isIterableRows,
    buildJsonInsertChunks,
  },
};
