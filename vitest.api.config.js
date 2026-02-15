const { defineConfig } = require('vitest/config');

module.exports = defineConfig({
  test: {
    name: 'api',
    include: ['test/api/**/*.test.js'],
    globals: true,
  },
});
