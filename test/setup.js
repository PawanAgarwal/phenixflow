const { cleanupAllHarnesses } = require('./harness/sqliteHarness');

afterEach(async () => {
  await cleanupAllHarnesses();
});
