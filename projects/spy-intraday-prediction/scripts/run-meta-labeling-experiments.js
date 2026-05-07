#!/usr/bin/env node
process.argv.push('--strategy', 'meta_labeling');
require('./run-research-suite').main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
