const globals = require('globals');

module.exports = [
  {
    files: ['**/*.js'],
    ignores: ['node_modules/**', 'artifacts/**', 'output/**', 'projects/**/artifacts/**'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'commonjs',
      globals: {
        ...globals.node,
      },
    },
    rules: {
      'no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
    },
  },
  {
    files: ['test/**/*.js'],
    languageOptions: {
      globals: {
        ...globals.node,
      },
    },
  },
];
