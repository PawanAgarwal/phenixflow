#!/usr/bin/env node

const { runThetaDataSmoke } = require('../src/thetadata/smoke');

async function main() {
  try {
    const report = await runThetaDataSmoke();
    // Keep stdout stable for PM/CI parsing.
    process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  } catch (error) {
    const message = error && error.message ? error.message : String(error);
    process.stderr.write(`MON-79 ThetaData smoke failed: ${message}\n`);
    process.exitCode = 1;
  }
}

main();
