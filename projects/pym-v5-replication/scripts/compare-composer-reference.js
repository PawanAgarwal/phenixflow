#!/usr/bin/env node
const fs = require('node:fs');

const { loadConfig, artifactPath, ensureDir } = require('../src/config');
const { resolveEndDate } = require('../src/calendar');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--timing') out.timing = argv[++index];
    else if (arg === '--label') out.label = argv[++index];
    else if (arg === '--local-artifact') out.localArtifact = argv[++index];
  }
  return out;
}

async function fetchComposerReference(config, startDate, endDate) {
  const apiBase = config.source.composerApiBase.replace(/\/$/, '');
  const response = await fetch(`${apiBase}/api/v2/public/symphonies/${config.source.composerSymphonyId}/backtest`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      capital: config.execution.initialCapital,
      start_date: startDate,
      end_date: endDate,
      apply_reg_fee: true,
      apply_taf_fee: true,
      apply_subscription: 'none',
      slippage_percent: 0.0001,
      broker: 'ALPACA_WHITE_LABEL',
      backtest_version: 'v2',
    }),
  });
  if (!response.ok) throw new Error(`Composer reference backtest failed: ${response.status}`);
  return response.json();
}

function localArtifactPath(timing, startDate, endDate, label) {
  const labelPart = label ? `${label}-` : '';
  return artifactPath(`pym-v5-backtest-${labelPart}${timing}-${startDate}-${endDate}.json`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const config = loadConfig();
  const startDate = args.startDate || config.windows.backtestStartDate;
  const endDate = resolveEndDate(config, args.endDate || config.windows.endDate);
  const timing = args.timing || config.execution.timing;
  const localPath = args.localArtifact || localArtifactPath(timing, startDate, endDate, args.label);
  if (!fs.existsSync(localPath)) throw new Error(`Missing local backtest artifact: ${localPath}`);
  const local = JSON.parse(fs.readFileSync(localPath, 'utf8'));
  const composer = await fetchComposerReference(config, startDate, endDate);
  const initialCapital = config.execution.initialCapital;
  const report = {
    generatedAt: new Date().toISOString(),
    startDate,
    endDate,
    localMassiveOnly: {
      artifactPath: localPath,
      timing,
      totalReturn: local.summary.totalReturn,
      finalEquity: local.summary.finalEquity,
      maxDrawdown: local.summary.maxDrawdown,
      sharpe: local.summary.sharpe,
      spyBuyHoldReturn: local.summary.spyBuyHoldReturn,
      splitAdjustments: local.splitAdjustments,
    },
    composerReference: {
      note: 'External Composer API reference only; not used as local market data.',
      finalEquity: composer.last_market_days_value,
      totalReturn: (composer.last_market_days_value / initialCapital) - 1,
      costs: composer.costs,
      lastMarketDay: composer.last_market_day,
      lastMarketDaysHoldings: composer.last_market_days_holdings,
    },
    gap: {
      finalEquity: composer.last_market_days_value - local.summary.finalEquity,
      totalReturn: ((composer.last_market_days_value / initialCapital) - 1) - local.summary.totalReturn,
    },
    interpretation: [
      'The local path uses Massive raw minute bars converted to split-adjusted daily closes.',
      'Composer uses its own adjusted data and exact platform semantics.',
      'The local cache starts in January 2025, so January-October 2025 lacks pre-2025 warmup for long indicators.',
      'A large positive gap means the Massive-only replication does not currently verify the report headline for this window.',
    ],
  };
  const labelPart = args.label ? `${args.label}-` : '';
  const outPath = artifactPath(`pym-v5-composer-comparison-${labelPart}${timing}-${startDate}-${endDate}.json`);
  ensureDir(artifactPath());
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({
    startDate,
    endDate,
    timing,
    localReturnPct: (report.localMassiveOnly.totalReturn * 100).toFixed(2),
    composerReturnPct: (report.composerReference.totalReturn * 100).toFixed(2),
    gapPct: (report.gap.totalReturn * 100).toFixed(2),
  }, null, 2));
  console.log(`wrote ${outPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
