const Database = require('better-sqlite3');
const {
  resolveDbPath,
  resolveIngestPollIntervalMs,
  resolveIngestSymbol,
} = require('../config/env');
const { ThetaDataClient } = require('../thetadata/client');
const { normalizeIngestRows } = require('./normalize');
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

function createIngestWorker(options = {}) {
  const env = options.env || process.env;
  const dbPath = options.dbPath || resolveDbPath(env);
  const streamName = options.streamName || 'thetadata-options';
  const pollIntervalMs = options.pollIntervalMs || resolveIngestPollIntervalMs(env);
  const ingestSymbol = options.symbol || resolveIngestSymbol(env);
  const limit = options.limit;

  const client = options.client || new ThetaDataClient({ env, fetchImpl: options.fetchImpl });
  const databaseFactory = options.databaseFactory || ((targetPath) => new Database(targetPath));
  const setTimeoutFn = options.setTimeoutFn || setTimeout;
  const clearTimeoutFn = options.clearTimeoutFn || clearTimeout;

  let timer = null;
  let running = false;

  async function runOnce() {
    const db = databaseFactory(dbPath);
    try {
      ensureIngestSchema(db);

      const checkpoint = getCheckpoint(db, streamName);
      const batch = await client.fetchIngestBatch({
        symbol: ingestSymbol,
        watermark: checkpoint,
        limit,
      });

      const effectiveWatermark = batch.watermark || checkpoint;
      const normalizedRows = normalizeIngestRows(batch.rows, {
        fallbackSymbol: ingestSymbol,
        watermark: effectiveWatermark,
      });

      const upsertedRows = upsertTrades(db, normalizedRows);

      if (batch.watermark !== undefined && batch.watermark !== null && batch.watermark !== '') {
        setCheckpoint(db, { streamName, watermark: batch.watermark });
      }

      return {
        fetchedRows: batch.rows.length,
        normalizedRows: normalizedRows.length,
        upsertedRows,
        watermarkBefore: checkpoint,
        watermarkAfter: batch.watermark || checkpoint || null,
        endpoint: batch.endpoint,
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
        symbol: ingestSymbol,
      };
    },
    __private: {
      ensureIngestSchema,
      upsertTrades,
    },
  };
}

module.exports = {
  createIngestWorker,
  ensureIngestSchema,
  upsertTrades,
};
