/** @type {import('vitest/config').UserConfig} */
module.exports = {
  test: {
    globals: true,
    environment: 'node',
    include: ['test/app.test.js', 'test/sqlite-harness.test.js'],
    setupFiles: ['test/setup.js'],
    clearMocks: true,
  },
};
