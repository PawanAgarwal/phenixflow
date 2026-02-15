const fs = require('node:fs/promises');
const { createSqliteHarness } = require('./harness/sqliteHarness');

describe('sqlite harness', () => {
  it('creates isolated databases for each harness', async () => {
    const first = await createSqliteHarness({ prefix: 'pf-sqlite-a-' });
    const second = await createSqliteHarness({ prefix: 'pf-sqlite-b-' });

    first.exec('CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL);');
    second.exec('CREATE TABLE items(id INTEGER PRIMARY KEY, value TEXT NOT NULL);');

    first.run('INSERT INTO items(value) VALUES (?)', ['alpha']);
    second.run('INSERT INTO items(value) VALUES (?)', ['beta']);

    expect(first.get('SELECT COUNT(*) AS count FROM items').count).toBe(1);
    expect(second.get('SELECT COUNT(*) AS count FROM items').count).toBe(1);
    expect(first.get('SELECT value FROM items LIMIT 1').value).toBe('alpha');
    expect(second.get('SELECT value FROM items LIMIT 1').value).toBe('beta');

    const [firstPathExists, secondPathExists] = await Promise.all([
      fs.access(first.dbPath).then(() => true).catch(() => false),
      fs.access(second.dbPath).then(() => true).catch(() => false),
    ]);

    expect(firstPathExists).toBe(true);
    expect(secondPathExists).toBe(true);
  });
});
