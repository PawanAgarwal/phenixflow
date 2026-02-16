function ensureCheckpointSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS ingest_checkpoints (
      stream_name TEXT PRIMARY KEY,
      watermark TEXT,
      updated_at_utc TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    )
  `);
}

function getCheckpoint(db, streamName) {
  ensureCheckpointSchema(db);
  const row = db.prepare(`
    SELECT watermark
    FROM ingest_checkpoints
    WHERE stream_name = @streamName
  `).get({ streamName });

  return row ? row.watermark : null;
}

function setCheckpoint(db, { streamName, watermark }) {
  ensureCheckpointSchema(db);
  db.prepare(`
    INSERT INTO ingest_checkpoints (
      stream_name,
      watermark,
      updated_at_utc
    ) VALUES (
      @streamName,
      @watermark,
      strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(stream_name) DO UPDATE SET
      watermark = excluded.watermark,
      updated_at_utc = excluded.updated_at_utc
  `).run({
    streamName,
    watermark: watermark === undefined || watermark === null ? null : String(watermark),
  });
}

module.exports = {
  ensureCheckpointSchema,
  getCheckpoint,
  setCheckpoint,
};
