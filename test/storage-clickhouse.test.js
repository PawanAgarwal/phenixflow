const {
  resolveFlowReadBackend,
  resolveFlowWriteBackend,
  resolveClickHouseConfig,
  buildArtifactPath,
  __private,
} = require('../src/storage/clickhouse');

describe('ClickHouse storage helpers', () => {
  it('defaults flow reads to clickhouse and coerces sqlite to clickhouse', () => {
    expect(resolveFlowReadBackend({ NODE_ENV: 'test' })).toBe('clickhouse');
    expect(resolveFlowReadBackend({ NODE_ENV: 'production' })).toBe('clickhouse');
    expect(resolveFlowReadBackend({ PHENIX_FLOW_READ_BACKEND: 'clickhouse' })).toBe('clickhouse');
    expect(resolveFlowReadBackend({ PHENIX_FLOW_READ_BACKEND: 'sqlite' })).toBe('clickhouse');
  });

  it('defaults flow writes to clickhouse and coerces sqlite to clickhouse', () => {
    expect(resolveFlowWriteBackend({ NODE_ENV: 'test' })).toBe('clickhouse');
    expect(resolveFlowWriteBackend({ NODE_ENV: 'production' })).toBe('clickhouse');
    expect(resolveFlowWriteBackend({ PHENIX_FLOW_WRITE_BACKEND: 'clickhouse' })).toBe('clickhouse');
    expect(resolveFlowWriteBackend({ PHENIX_FLOW_WRITE_BACKEND: 'sqlite' })).toBe('clickhouse');
  });

  it('builds ClickHouse connection config and artifact path from env', () => {
    const env = {
      CLICKHOUSE_HOST: 'clickhouse.local',
      CLICKHOUSE_PORT: '9440',
      CLICKHOUSE_USER: 'analytics',
      CLICKHOUSE_PASSWORD: 'secret',
      CLICKHOUSE_DATABASE: 'options_prod',
      CLICKHOUSE_CONNECT_TIMEOUT_SEC: '15',
      CLICKHOUSE_SEND_TIMEOUT_SEC: '90',
      CLICKHOUSE_RECEIVE_TIMEOUT_SEC: '120',
    };

    expect(resolveClickHouseConfig(env)).toEqual({
      host: 'clickhouse.local',
      port: '9440',
      user: 'analytics',
      password: 'secret',
      database: 'options_prod',
      connectTimeoutSec: '15',
      sendTimeoutSec: '90',
      receiveTimeoutSec: '120',
    });
    expect(buildArtifactPath(env)).toBe('clickhouse://clickhouse.local:9440/options_prod');
  });

  it('uses a larger default query buffer and allows env override', () => {
    expect(__private.resolveQueryMaxBufferBytes({})).toBe(1024 * 1024 * 1024);
    expect(__private.resolveQueryMaxBufferBytes({
      CLICKHOUSE_QUERY_MAX_BUFFER_BYTES: String(512 * 1024 * 1024),
    })).toBe(512 * 1024 * 1024);
    expect(__private.resolveQueryMaxBufferBytes({
      CLICKHOUSE_QUERY_MAX_BUFFER_BYTES: '1024',
    })).toBe(1024 * 1024 * 1024);
  });

  it('parses JSONEachRow output into row objects', () => {
    expect(__private.parseJsonEachRow('{"id":"a"}\n{"id":"b","n":2}\n')).toEqual([
      { id: 'a' },
      { id: 'b', n: 2 },
    ]);
  });

  it('sanitizes undefined JSONEachRow fields to null', () => {
    expect(__private.sanitizeJsonRow({
      symbol: 'AAPL',
      bid: undefined,
      ask: 1.5,
    })).toEqual({
      symbol: 'AAPL',
      bid: null,
      ask: 1.5,
    });
  });

  it('splits inserts by byte size as well as row count', () => {
    const rows = [
      { id: 'a', payload: 'x'.repeat(2000) },
      { id: 'b', payload: 'x'.repeat(2000) },
      { id: 'c', payload: 'x'.repeat(2000) },
    ];
    const singleLineBytes = Buffer.byteLength(`${JSON.stringify(rows[0])}\n`);

    const chunks = __private.buildJsonInsertChunks(rows, {
      chunkSize: 10,
      maxChunkBytes: singleLineBytes,
    });

    expect(chunks).toHaveLength(3);
    expect(chunks.every((chunk) => Buffer.byteLength(chunk) <= singleLineBytes)).toBe(true);
  });
});
