#!/usr/bin/env node

const { syncOptionOiFromGov } = require('../../src/oi-gov');

async function main() {
  const source = process.argv[2] || process.env.GOV_OI_SOURCE || 'FINRA';
  const sourceUrl = process.argv[3] || process.env.GOV_OI_SOURCE_URL || null;
  const asOfDate = process.argv[4] || process.env.GOV_OI_AS_OF_DATE || null;

  const result = await syncOptionOiFromGov({
    source,
    sourceUrl,
    asOfDate,
    env: process.env,
  });

  console.log(JSON.stringify(result, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
