const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');

const activeHarnesses = new Set();

function registerHarness(harness) {
  activeHarnesses.add(harness);
}

async function cleanupAllHarnesses() {
  const harnesses = [...activeHarnesses];
  activeHarnesses.clear();
  await Promise.all(harnesses.map((harness) => harness.close()));
}

async function createSqliteHarness(options = {}) {
  const prefix = options.prefix || 'phenixflow-test-db-';
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
  const dbPath = path.join(tempDir, 'test.sqlite');

  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  let isClosed = false;

  const harness = {
    dbPath,
    run(sql, params = []) {
      return db.prepare(sql).run(params);
    },
    get(sql, params = []) {
      return db.prepare(sql).get(params);
    },
    all(sql, params = []) {
      return db.prepare(sql).all(params);
    },
    exec(sql) {
      return db.exec(sql);
    },
    async close() {
      if (isClosed) {
        return;
      }

      isClosed = true;
      activeHarnesses.delete(harness);
      db.close();
      await fs.rm(tempDir, { recursive: true, force: true });
    },
  };

  registerHarness(harness);
  return harness;
}

module.exports = {
  createSqliteHarness,
  cleanupAllHarnesses,
};
