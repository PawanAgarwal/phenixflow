#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { PROJECT_ROOT, ensureDir, loadConfig } = require('../src/config');
const { parseArgs, asNumber } = require('../src/cli');
const {
  DEFAULT_EXECUTION,
  DEFAULT_INITIAL_CAPITAL,
  DEFAULT_STRATEGY_CONFIGS,
  EXPERIMENT_STRATEGY_CONFIGS,
  normalizeSymbols,
  runWheelBacktest,
} = require('../src/wheel-backtest');

const REPO_ROOT = path.resolve(PROJECT_ROOT, '..', '..');
const DEFAULT_UNIVERSE_PATH = path.join(REPO_ROOT, 'apps', 'flow-api', 'config', 'top100-universe.json');

function readJson(filePath) {
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

function loadSymbols(args) {
  if (args.symbols) return normalizeSymbols(String(args.symbols).split(','));
  const universePath = path.resolve(args.universe || DEFAULT_UNIVERSE_PATH);
  const raw = readJson(universePath);
  const symbols = Array.isArray(raw)
    ? raw
    : raw.symbols || raw.tickers || raw.universe || [];
  return normalizeSymbols(symbols);
}

function strategySuite(name) {
  if (name === 'expanded' || name === 'experiments') return EXPERIMENT_STRATEGY_CONFIGS;
  return DEFAULT_STRATEGY_CONFIGS;
}

function filterStrategies(strategyIds, suite = DEFAULT_STRATEGY_CONFIGS) {
  if (!strategyIds) return suite;
  const selected = new Set(String(strategyIds).split(',').map((id) => id.trim()).filter(Boolean));
  return suite.filter((strategy) => selected.has(strategy.id));
}

function pct(value) {
  return Number.isFinite(value) ? `${(value * 100).toFixed(2)}%` : 'n/a';
}

function markdownReport(report) {
  const lines = [];
  lines.push(`# Put Selling and Wheel Backtest`);
  lines.push('');
  lines.push(`Generated: ${report.generatedAt}`);
  lines.push(`Window: ${report.startDate} through ${report.endDate}`);
  lines.push(`Provider: ${report.provider} flat files only`);
  lines.push(`Universe size: ${report.symbols.length}`);
  lines.push(`Initial capital: $${report.initialCapital.toLocaleString('en-US')}`);
  lines.push('');
  lines.push(`## Strategy Framing`);
  lines.push('');
  lines.push('- Cash-secured put variants sell OTM puts and reserve full assignment notional.');
  lines.push('- Wheel variants sell cash-secured puts, hold assigned shares, then sell covered calls against those shares until called away.');
  lines.push('- This implementation uses expiration assignment only and daily mark-to-market equity from Massive 1-minute option marks.');
  lines.push('- The option minute aggregate close is not a bid/ask quote, so entry proceeds are haircut before commissions.');
  lines.push('');
  lines.push(`## Execution Assumptions`);
  lines.push('');
  lines.push(`- Entry window: ${Math.floor(report.execution.entryMinuteEt / 60)}:${String(report.execution.entryMinuteEt % 60).padStart(2, '0')} ET for ${report.execution.entryWindowMinutes} minutes.`);
  lines.push(`- Minimum option premium: $${report.execution.minPremium}.`);
  lines.push(`- Premium haircut: ${(report.execution.premiumHaircutPct * 100).toFixed(1)}%.`);
  lines.push(`- Commission: $${report.execution.commissionPerContract} per contract.`);
  lines.push(`- Max position: ${(report.execution.maxPositionPct * 100).toFixed(1)}% of equity per symbol.`);
  lines.push(`- Max put collateral utilization: ${(report.execution.maxPortfolioUtilization * 100).toFixed(1)}% of equity.`);
  lines.push('');
  lines.push(`## Coverage`);
  lines.push('');
  lines.push(`- Attempted open days: ${report.coverage.attemptedOpenDays}`);
  lines.push(`- Processed days: ${report.coverage.processedDays}`);
  lines.push(`- Attempted missing files: ${report.coverage.attemptedMissing.length}`);
  lines.push(`- Provider-sparse days: ${report.coverage.providerSparse.length}`);
  lines.push('');
  lines.push(`## Benchmarks`);
  lines.push('');
  lines.push('| Symbol | Return | Max DD | Observations |');
  lines.push('| --- | ---: | ---: | ---: |');
  Object.entries(report.benchmarks).forEach(([symbol, benchmark]) => {
    lines.push(`| ${symbol} | ${pct(benchmark.totalReturn)} | ${pct(benchmark.maxDrawdown)} | ${benchmark.observations} |`);
  });
  lines.push('');
  lines.push(`## Results`);
  lines.push('');
  lines.push('| Strategy | Return | Max DD | STO | BTC | Assignments | Premium | Buybacks | Ending open |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  report.strategies.forEach((strategy) => {
    const assignments = strategy.putAssignments + strategy.putAssignmentLiquidations + strategy.callAssignments;
    lines.push(`| ${strategy.id} | ${pct(strategy.totalReturn)} | ${pct(strategy.maxDrawdown)} | ${strategy.sellTradeCount} | ${strategy.buyToCloseCount || 0} | ${assignments} | $${strategy.premiumCollected.toLocaleString('en-US')} | $${(strategy.buybackCost || 0).toLocaleString('en-US')} | ${strategy.endingOpenOptions} |`);
  });
  lines.push('');
  lines.push(`## Monthly Returns`);
  report.strategies.forEach((strategy) => {
    lines.push('');
    lines.push(`### ${strategy.id}`);
    lines.push('');
    lines.push('| Month | Return | Max DD | Days |');
    lines.push('| --- | ---: | ---: | ---: |');
    strategy.monthly.forEach((month) => {
      lines.push(`| ${month.month} | ${pct(month.return)} | ${pct(month.maxDrawdown)} | ${month.days} |`);
    });
  });
  lines.push('');
  lines.push(`## Research Sources`);
  lines.push('');
  lines.push('- Options Industry Council: cash-secured put strategy description and collateral framing: https://www.optionseducation.org/strategies/all-strategies/cash-secured-put');
  lines.push('- Cboe Options Institute via Fidelity: cash-secured puts are generally for investors willing to buy the underlying at the strike: https://www.fidelity.com/learning-center/investment-products/options/options-strategy-guide/shortput-cashsecured');
  lines.push('- FINRA assignment overview: short option sellers can be assigned and must fulfill the contract: https://www.finra.org/investors/insights/trading-options-understanding-assignment');
  lines.push('- Schwab wheel overview: wheel cycles through cash-secured puts, assigned stock, and covered calls: https://www.schwab.com/learn/story/three-things-to-know-about-wheel-strategy');
  lines.push('');
  lines.push(`## Limitations`);
  lines.push('');
  report.assumptions.forEach((assumption) => lines.push(`- ${assumption}`));
  lines.push('- This is research infrastructure and historical simulation, not investment advice or production approval.');
  lines.push('');
  return `${lines.join('\n')}\n`;
}

function compactConsole(report) {
  return {
    outputWindow: `${report.startDate}:${report.endDate}`,
    symbols: report.symbols.length,
    coverage: report.coverage,
    benchmarks: report.benchmarks,
    topStrategies: report.strategies.slice(0, 6).map((strategy) => ({
      id: strategy.id,
      totalReturn: strategy.totalReturn,
      maxDrawdown: strategy.maxDrawdown,
      sellTradeCount: strategy.sellTradeCount,
      assignments: strategy.putAssignments + strategy.putAssignmentLiquidations + strategy.callAssignments,
      premiumCollected: strategy.premiumCollected,
    })),
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const startDate = args['start-date'] || config.windows.train.startDate;
  const endDate = args['end-date'] || config.dataPolicy.historicalCutoffDate;
  const limit = Math.max(1, Math.floor(asNumber(args.limit, 100)));
  const symbols = loadSymbols(args).slice(0, limit);
  const initialCapital = asNumber(args.capital, DEFAULT_INITIAL_CAPITAL);
  const strategyConfigs = filterStrategies(args.strategies, strategySuite(args.suite));
  const outputPath = path.resolve(
    args.output || path.join(PROJECT_ROOT, 'artifacts', `wheel-strategy-backtest-${startDate}-${endDate}.json`),
  );
  const markdownPath = path.resolve(
    args.markdown || outputPath.replace(/\.json$/i, '.md'),
  );
  const progress = args.progress !== false;

  const execution = {
    entryMinuteEt: Math.floor(asNumber(args['entry-minute-et'], DEFAULT_EXECUTION.entryMinuteEt)),
    entryWindowMinutes: Math.floor(asNumber(args['entry-window-minutes'], DEFAULT_EXECUTION.entryWindowMinutes)),
    minPremium: asNumber(args['min-premium'], DEFAULT_EXECUTION.minPremium),
    minVolume: asNumber(args['min-volume'], DEFAULT_EXECUTION.minVolume),
    minTransactions: asNumber(args['min-transactions'], DEFAULT_EXECUTION.minTransactions),
    premiumHaircutPct: asNumber(args['premium-haircut-pct'], DEFAULT_EXECUTION.premiumHaircutPct),
    commissionPerContract: asNumber(args['commission'], DEFAULT_EXECUTION.commissionPerContract),
    stockSlippageBps: asNumber(args['stock-slippage-bps'], DEFAULT_EXECUTION.stockSlippageBps),
    maxContractsPerSymbol: Math.floor(asNumber(args['max-contracts-per-symbol'], DEFAULT_EXECUTION.maxContractsPerSymbol)),
    maxPositionPct: asNumber(args['max-position-pct'], DEFAULT_EXECUTION.maxPositionPct),
    maxPortfolioUtilization: asNumber(args['max-utilization'], DEFAULT_EXECUTION.maxPortfolioUtilization),
    maxOpenShortOptions: Math.floor(asNumber(args['max-open-options'], DEFAULT_EXECUTION.maxOpenShortOptions)),
  };

  const report = await runWheelBacktest({
    config,
    startDate,
    endDate,
    symbols,
    strategyConfigs,
    initialCapital,
    execution,
    onProgress: progress
      ? ({ dayIso, processedDays, totalDays, elapsedMs }) => {
        console.error(`[wheel] ${processedDays}/${totalDays} ${dayIso} ${elapsedMs}ms`);
      }
      : null,
  });

  ensureDir(path.dirname(outputPath));
  ensureDir(path.dirname(markdownPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(report, null, 2)}\n`, 'utf8');
  fs.writeFileSync(markdownPath, markdownReport(report), 'utf8');
  console.log(JSON.stringify({
    outputPath,
    markdownPath,
    ...compactConsole(report),
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
