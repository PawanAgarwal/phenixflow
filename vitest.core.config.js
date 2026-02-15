const { defineConfig } = require('vitest/config');

module.exports = defineConfig({
  test: {
    name: 'core',
    include: ['test/core/**/*.test.js', 'test/shadow/**/*.test.js'],
    globals: true,
  },
});
