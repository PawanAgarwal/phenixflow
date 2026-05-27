#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs, asNumber } = require('../src/cli');
const {
  formatStrictPreflightMarkdown,
  preflightStrictWheelBacktest,
  resolveStrictBacktestWindow,
} = require('../src/cross-sectional-wheel-strict');

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const window = resolveStrictBacktestWindow(config, {
    startDate: args['start-date'],
    endDate: args['end-date'],
  });

  const outputPath = path.resolve(
    args.output || path.join(
      PROJECT_ROOT,
      'artifacts',
      `cross-sectional-wheel-strict-preflight-${window.startDate}-${window.endDate}.json`,
    ),
  );
  const markdownPath = path.resolve(args.markdown || outputPath.replace(/\.json$/i, '.md'));

  const report = await preflightStrictWheelBacktest({
    config,
    startDate: window.startDate,
    endDate: window.endDate,
    membershipPath: args.membership || args['pit-membership'],
    optionBidAskPath: args['option-bid-ask'] || args['option-quotes'],
    ivSurfacePath: args['iv-surface'],
    dividendsPath: args.dividends,
    riskFreePath: args['risk-free'] || args.rates,
    modeledSpreadBps: asNumber(args['modeled-spread-bps'], null),
    modeledSlippageBps: asNumber(args['modeled-slippage-bps'], null),
    slippageBps: asNumber(args['slippage-bps'], null),
    commissionPerContract: asNumber(args.commission, null),
  });

  ensureDir(path.dirname(outputPath));
  ensureDir(path.dirname(markdownPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  fs.writeFileSync(markdownPath, formatStrictPreflightMarkdown(report), 'utf8');

  const summary = {
    status: report.status,
    outputPath,
    markdownPath,
    window: report.window,
    blockingErrors: report.errors.map((error) => error.code),
    warnings: report.warnings.map((warning) => warning.code),
  };
  const payload = `${JSON.stringify(summary, null, 2)}\n`;
  if (report.status === 'PASS') {
    process.stdout.write(payload);
    return;
  }

  process.stderr.write(payload);
  process.exitCode = 1;
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
