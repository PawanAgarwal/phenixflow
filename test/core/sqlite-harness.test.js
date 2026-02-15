const fs = require('node:fs');
const { createIsolatedSqliteSuite } = require('../support/sqlite-suite');

describe('core SQLite test harness (suite A)', () => {
  const suite = createIsolatedSqliteSuite('core-a');

  it('creates an isolated temporary sqlite file per suite', () => {
    expect(suite.dbPath).toContain('phenixflow-core-a-');
    expect(fs.existsSync(suite.dbPath)).toBe(true);
  });

  it('starts empty and can persist records inside the suite database', () => {
    suite.db.exec('CREATE TABLE flow_events (id TEXT PRIMARY KEY, symbol TEXT NOT NULL);');

    const insert = suite.db.prepare('INSERT INTO flow_events (id, symbol) VALUES (?, ?)');
    insert.run('evt_001', 'AAPL');

    const count = suite.db.prepare('SELECT COUNT(*) AS total FROM flow_events').get();
    expect(count.total).toBe(1);
  });
});

describe('core SQLite test harness (suite B)', () => {
  const suite = createIsolatedSqliteSuite('core-b');

  it('uses a different sqlite path from other suites', () => {
    expect(suite.dbPath).toContain('phenixflow-core-b-');
    expect(suite.dbPath).not.toContain('phenixflow-core-a-');
    expect(fs.existsSync(suite.dbPath)).toBe(true);
  });

  it('is independently initialized for this suite', () => {
    suite.db.exec('CREATE TABLE suite_check (value INTEGER NOT NULL);');
    suite.db.prepare('INSERT INTO suite_check (value) VALUES (?)').run(42);

    const row = suite.db.prepare('SELECT value FROM suite_check').get();
    expect(row.value).toBe(42);
  });
});
