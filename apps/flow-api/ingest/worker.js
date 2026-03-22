const Database = require('better-sqlite3');
const {
  resolveDbPath,
  resolveIngestPollIntervalMs,
  resolveIngestSymbol,
  resolveIngestSymbols,
} = require('../config/env');
const { ThetaDataClient } = require('../thetadata/client');
const { normalizeIngestRow } = require('./normalize');
const { ensureCheckpointSchema, getCheckpoint, setCheckpoint } = require('./checkpoint-store');

function ensureIngestSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS option_trades (
      trade_id TEXT PRIMARY KEY,
      trade_ts_utc TEXT NOT NULL,
      trade_ts_et TEXT NOT NULL,
      symbol TEXT NOT NULL,
      expiration TEXT NOT NULL,
      strike REAL NOT NULL,
      option_right TEXT NOT NULL CHECK (option_right IN ('CALL', 'PUT')),
      price REAL NOT NULL,
      size INTEGER NOT NULL,
      bid REAL,
      ask REAL,
      condition_code TEXT,
      exchange TEXT,
      raw_payload_json TEXT,
      watermark TEXT,
      ingested_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE INDEX IF NOT EXISTS idx_option_trades_symbol_ts
      ON option_trades(symbol, trade_ts_utc);

    CREATE TABLE IF NOT EXISTS ingest_dead_letter (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      stream_name TEXT NOT NULL,
      reason TEXT NOT NULL,
      raw_payload_json TEXT NOT NULL,
      created_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS ingest_worker_stats (
      stat_key TEXT PRIMARY KEY,
      stat_value INTEGER NOT NULL DEFAULT 0,
      updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
  `);

  ensureCheckpointSchema(db);
}

function upsertTrades(db, rows) {
  if (!rows.length) return 0;

  const upsert = db.prepare(`
    INSERT INTO option_trades (
      trade_id,
      trade_ts_utc,
      trade_ts_et,
      symbol,
      expiration,
      strike,
      option_right,
      price,
      size,
      bid,
      ask,
      condition_code,
      exchange,
      raw_payload_json,
      watermark
    ) VALUES (
      @tradeId,
      @tradeTsUtc,
      @tradeTsEt,
      @symbol,
      @expiration,
      @strike,
      @optionRight,
      @price,
      @size,
      @bid,
      @ask,
      @conditionCode,
      @exchange,
      @rawPayloadJson,
      @watermark
    )
    ON CONFLICT(trade_id) DO UPDATE SET
      bid = excluded.bid,
      ask = excluded.ask,
      condition_code = excluded.condition_code,
      exchange = excluded.exchange,
      raw_payload_json = excluded.raw_payload_json,
      watermark = excluded.watermark
  `);

  const txn = db.transaction((items) => {
    let writes = 0;
    items.forEach((row) => {
      writes += upsert.run(row).changes;
    });
    return writes;
  });

  return txn(rows);
}

function recordDeadLetters(db, streamName, deadLetters = []) {
  if (!deadLetters.length) return;

  const insertDeadLetter = db.prepare(`
    INSERT INTO ingest_dead_letter (
      stream_name,
      reason,
      raw_payload_json
    ) VALUES (
      @streamName,
      @reason,
      @rawPayloadJson
    )
  `);

  const txn = db.transaction((items) => {
    items.forEach((item) => insertDeadLetter.run({
      streamName,
      reason: item.reason,
      rawPayloadJson: JSON.stringify(item.rawRow || {}),
    }));
  });

  txn(deadLetters);
}

function bumpStat(db, key, delta) {
  if (!delta) return;
  db.prepare(`
    INSERT INTO ingest_worker_stats (
      stat_key,
      stat_value,
      updated_at_utc
    ) VALUES (
      @key,
      @delta,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(stat_key) DO UPDATE SET
      stat_value = ingest_worker_stats.stat_value + excluded.stat_value,
      updated_at_utc = excluded.updated_at_utc
  `).run({
    key,
    delta: Math.trunc(delta),
  });
}

function parseIntWithFallback(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : fallback;
}

function createIngestWorker(options = {}) {
  const env = options.env || process.env;
  const dbPath = options.dbPath || resolveDbPath(env);
  const streamName = options.streamName || 'thetadata-options';
  const pollIntervalMs = options.pollIntervalMs || resolveIngestPollIntervalMs(env);
  const configuredSymbols = options.symbols
    || (options.symbol ? [String(options.symbol).toUpperCase()] : resolveIngestSymbols(env));
  const fallbackSymbol = resolveIngestSymbol(env);
  const ingestSymbols = Array.isArray(configuredSymbols) && configuredSymbols.length
    ? configuredSymbols
    : (fallbackSymbol ? [fallbackSymbol] : []);
  const limit = options.limit;

  const client = options.client || new ThetaDataClient({ env, fetchImpl: options.fetchImpl });
  const databaseFactory = options.databaseFactory || ((targetPath) => new Database(targetPath));
  const setTimeoutFn = options.setTimeoutFn || setTimeout;
  const clearTimeoutFn = options.clearTimeoutFn || clearTimeout;
  const maxAttempts = options.maxAttempts || parseIntWithFallback(env.INGEST_FETCH_MAX_ATTEMPTS, 3);
  const retryBaseMs = options.retryBaseMs || parseIntWithFallback(env.INGEST_RETRY_BASE_MS, 200);
  const maxBufferedRows = options.maxBufferedRows || parseIntWithFallback(env.INGEST_MAX_BUFFER_ROWS, 5000);

  let timer = null;
  let running = false;

  const sleep = (ms) => new Promise((resolve) => {
    setTimeoutFn(resolve, Math.max(0, ms));
  });

  async function fetchWithRetry({ symbol, watermark }) {
    let attempt = 0;
    let lastError = null;
    while (attempt < maxAttempts) {
      attempt += 1;
      try {
        const batch = await client.fetchIngestBatch({ symbol, watermark, limit });
        return { batch, retryCount: attempt - 1 };
      } catch (error) {
        lastError = error;
        if (attempt >= maxAttempts) break;
        const jitter = Math.floor(Math.random() * retryBaseMs);
        const backoffMs = retryBaseMs * (2 ** (attempt - 1)) + jitter;
        await sleep(backoffMs);
      }
    }
    throw lastError || new Error('ingest_fetch_failed');
  }

  async function runOnce() {
    const db = databaseFactory(dbPath);
    try {
      ensureIngestSchema(db);

      const deadLetters = [];
      const normalizedRows = [];
      let fetchedRows = 0;
      let upsertedRows = 0;
      let totalRetryCount = 0;
      let firstWatermarkBefore = null;
      let lastWatermarkAfter = null;
      let endpoint = null;

      for (const symbol of ingestSymbols) {
        const checkpointKey = ingestSymbols.length === 1 ? streamName : `${streamName}:${symbol}`;
        const checkpoint = getCheckpoint(db, checkpointKey);
        if (firstWatermarkBefore === null) firstWatermarkBefore = checkpoint;

        const { batch, retryCount } = await fetchWithRetry({ symbol, watermark: checkpoint });
        totalRetryCount += retryCount;
        fetchedRows += batch.rows.length;
        endpoint = batch.endpoint;
        const effectiveWatermark = batch.watermark || checkpoint;

        batch.rows.forEach((rawRow) => {
          const normalized = normalizeIngestRow(rawRow, {
            fallbackSymbol: symbol,
            watermark: effectiveWatermark,
          });
          if (normalized) normalizedRows.push(normalized);
          else deadLetters.push({ reason: 'normalize_failed', rawRow });
        });

        if (batch.watermark !== undefined && batch.watermark !== null && batch.watermark !== '') {
          setCheckpoint(db, { streamName: checkpointKey, watermark: batch.watermark });
        }
        lastWatermarkAfter = batch.watermark || checkpoint || null;
      }

      let droppedRows = 0;
      let normalizedRowsToWrite = normalizedRows;
      if (normalizedRows.length > maxBufferedRows) {
        droppedRows = normalizedRows.length - maxBufferedRows;
        normalizedRowsToWrite = normalizedRows.slice(0, maxBufferedRows);
      }

      upsertedRows = upsertTrades(db, normalizedRowsToWrite);
      recordDeadLetters(db, streamName, deadLetters);
      bumpStat(db, 'ingest_events_total', fetchedRows);
      bumpStat(db, 'ingest_parse_failures_total', deadLetters.length);
      bumpStat(db, 'ingest_dropped_total', droppedRows);
      bumpStat(db, 'ingest_retries_total', totalRetryCount);

      return {
        fetchedRows,
        normalizedRows: normalizedRowsToWrite.length,
        upsertedRows,
        droppedRows,
        parseFailures: deadLetters.length,
        retryCount: totalRetryCount,
        watermarkBefore: firstWatermarkBefore,
        watermarkAfter: lastWatermarkAfter,
        endpoint,
      };
    } finally {
      db.close();
    }
  }

  async function tick() {
    if (!running) return;
    try {
      await runOnce();
    } finally {
      if (running) {
        timer = setTimeoutFn(tick, pollIntervalMs);
      }
    }
  }

  return {
    runOnce,
    start() {
      if (running) return;
      running = true;
      timer = setTimeoutFn(tick, 0);
    },
    stop() {
      running = false;
      if (timer) {
        clearTimeoutFn(timer);
        timer = null;
      }
    },
    getState() {
      return {
        running,
        dbPath,
        streamName,
        pollIntervalMs,
        symbols: ingestSymbols,
      };
    },
    __private: {
      ensureIngestSchema,
      upsertTrades,
      recordDeadLetters,
      bumpStat,
    },
  };
}

module.exports = {
  createIngestWorker,
  ensureIngestSchema,
  upsertTrades,
};
