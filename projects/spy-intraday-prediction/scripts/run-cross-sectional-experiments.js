#!/usr/bin/env node
process.argv.push('--strategy', 'cross_sectional');
require('./run-research-suite').main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
