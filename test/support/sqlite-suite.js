const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');

function createIsolatedSqliteSuite(label = 'suite') {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), `phenixflow-${label}-`));
  const dbPath = path.join(tempDir, 'test.sqlite');
  let db;

  globalThis.beforeAll(() => {
    db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
  });

  globalThis.afterAll(() => {
    if (db) {
      db.close();
    }
    fs.rmSync(tempDir, { recursive: true, force: true });
  });

  return {
    get db() {
      if (!db) throw new Error('SQLite test DB is not initialized yet.');
      return db;
    },
    dbPath,
    tempDir,
  };
}

module.exports = { createIsolatedSqliteSuite };
