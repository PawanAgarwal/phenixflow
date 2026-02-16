const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');
const { createIngestWorker } = require('../../src/ingest/worker');

function makeTempDb() {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'ingest-worker-'));
  const dbPath = path.join(tempDir, 'worker.sqlite');
  return {
    dbPath,
    cleanup() {
      fs.rmSync(tempDir, { recursive: true, force: true });
    },
  };
}

describe('ingest worker', () => {
  it('persists normalized trade rows and updates checkpoint', async () => {
    const temp = makeTempDb();
    const fetchCalls = [];

    const fakeClient = {
      async fetchIngestBatch({ watermark }) {
        fetchCalls.push(watermark);
        return {
          endpoint: 'http://thetadata.local/stream',
          rows: [
            {
              symbol: 'AAPL',
              trade_timestamp: '2026-02-13T14:35:00.000Z',
              expiration: '2026-02-20',
              strike: 210,
              right: 'CALL',
              price: 1.2,
              size: 5,
              bid: 1.19,
              ask: 1.21,
            },
          ],
          watermark: fetchCalls.length === 1 ? 'w1' : 'w2',
        };
      },
    };

    try {
      const worker = createIngestWorker({
        dbPath: temp.dbPath,
        symbol: 'AAPL',
        client: fakeClient,
      });

      const first = await worker.runOnce();
      expect(first).toMatchObject({
        fetchedRows: 1,
        normalizedRows: 1,
        upsertedRows: 1,
        watermarkBefore: null,
        watermarkAfter: 'w1',
      });

      const second = await worker.runOnce();
      expect(second).toMatchObject({
        fetchedRows: 1,
        normalizedRows: 1,
        watermarkBefore: 'w1',
        watermarkAfter: 'w2',
      });
      expect(fetchCalls).toEqual([null, 'w1']);

      const db = new Database(temp.dbPath, { readonly: true });
      const tradeCount = db.prepare('SELECT COUNT(*) AS c FROM option_trades').get().c;
      const checkpoint = db.prepare('SELECT watermark FROM ingest_checkpoints WHERE stream_name = ?').get('thetadata-options');
      db.close();

      expect(tradeCount).toBe(1);
      expect(checkpoint).toEqual({ watermark: 'w2' });
    } finally {
      temp.cleanup();
    }
  });
});
