#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs } = require('../src/cli');
const { validatePhase2Signals } = require('../src/signal-validation');

function defaultReportPath() {
  return path.join(PROJECT_ROOT, 'artifacts', 'full-phase2-suite-2025-01-02-2026-04-27.json');
}

function compactVariant(variant) {
  return {
    variantKey: variant.variantKey,
    familyKey: variant.familyKey,
    promoteToPaper: variant.verdict.promoteToPaper,
    defaultTotalReturn: variant.summary.defaultTotalReturn,
    doubleCostTotalReturn: variant.summary.doubleCostTotalReturn,
    highCostTotalReturn: variant.summary.highCostTotalReturn,
    longOnlyTotalReturn: variant.summary.longOnlyTotalReturn,
    delayedOneMinuteTotalReturn: variant.summary.delayedOneMinuteTotalReturn,
    returnWithoutBestDayTotal: variant.summary.returnWithoutBestDayTotal,
    positiveMonths: variant.summary.positiveMonths,
    doubleCostPositiveMonths: variant.summary.doubleCostPositiveMonths,
    stricterThresholdTotalReturn: variant.verdict.stricterThresholdTotalReturn,
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const reportPath = path.resolve(args.report || defaultReportPath());
  const outputPath = path.resolve(args.output || path.join(PROJECT_ROOT, 'artifacts', 'phase2-signal-validation.json'));
  const result = await validatePhase2Signals({
    reportPath,
    predictionsPath: args.predictions ? path.resolve(args.predictions) : undefined,
    config,
  });
  ensureDir(path.dirname(outputPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath,
    candidateSignalVariantCount: result.candidateSignalVariantCount,
    candidateSignalFamilyCount: result.candidateSignalFamilyCount,
    promotedVariantCount: result.promotedVariantCount,
    topFamilies: result.families.slice(0, 8),
    topVariants: result.variants.slice(0, 10).map(compactVariant),
  }, null, 2));
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error.stack || error.message);
    process.exitCode = 1;
  });
}

module.exports = { main };
