#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { artifactPath, ensureDir, loadConfig } = require('../src/config');
const { loadOpenDates } = require('../src/calendar');
const { readTargetTradesForDay } = require('../src/data');
const { simulatePassiveMarketMaking } = require('../src/passive-mm');
const { quotePathForDay, readFilteredQuotesForDay } = require('../src/quotes');

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    index += 1;
  }
  return out;
}

function asNumber(value, fallback) {
  if (value === undefined || value === true) return fallback;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function compactResult(result, includeTrades = false) {
  const out = {
    strategy: result.strategy,
    settings: result.settings,
    summary: result.summary,
  };
  if (includeTrades) out.trades = result.trades;
  return out;
}

function buildDefaultGrid({ costCentsPerSide, session }) {
  const common = {
    costCentsPerSide,
    regularOpenMinuteEt: session.regularOpenMinuteEt,
    regularCloseMinuteEt: session.regularCloseMinuteEt,
    minBidSize: 1,
    minAskSize: 1,
    minPrintSize: 1,
    noEntryFirstMinutes: 5,
    noEntryLastMinutes: 10,
    cooldownMs: 1000,
  };
  return [
    {
      ...common,
      name: 'touch_2c_fast',
      minSpreadCents: 2,
      maxSpreadCents: 8,
      minProfitCents: 1,
      stopCents: 3,
      maxHoldMs: 2000,
      latencyMs: 0,
    },
    {
      ...common,
      name: 'touch_3c_fast',
      minSpreadCents: 3,
      maxSpreadCents: 10,
      minProfitCents: 2,
      stopCents: 4,
      maxHoldMs: 2500,
      latencyMs: 0,
    },
    {
      ...common,
      name: 'touch_3c_hold5s',
      minSpreadCents: 3,
      maxSpreadCents: 12,
      minProfitCents: 2,
      stopCents: 5,
      maxHoldMs: 5000,
      latencyMs: 0,
    },
    {
      ...common,
      name: 'latency50_3c',
      minSpreadCents: 3,
      maxSpreadCents: 12,
      minProfitCents: 2,
      stopCents: 5,
      maxHoldMs: 5000,
      latencyMs: 50,
    },
    {
      ...common,
      name: 'latency100_4c',
      minSpreadCents: 4,
      maxSpreadCents: 14,
      minProfitCents: 2,
      stopCents: 6,
      maxHoldMs: 7000,
      latencyMs: 100,
    },
    {
      ...common,
      name: 'through0p5_3c',
      minSpreadCents: 3,
      maxSpreadCents: 12,
      minProfitCents: 2,
      stopCents: 5,
      maxHoldMs: 5000,
      requireBuyThroughCents: 0.5,
      requireSellThroughCents: 0.5,
      latencyMs: 50,
    },
    {
      ...common,
      name: 'through1_4c',
      minSpreadCents: 4,
      maxSpreadCents: 16,
      minProfitCents: 2,
      stopCents: 6,
      maxHoldMs: 8000,
      requireBuyThroughCents: 1,
      requireSellThroughCents: 1,
      latencyMs: 50,
    },
    {
      ...common,
      name: 'wide_5c_hold10s',
      minSpreadCents: 5,
      maxSpreadCents: 20,
      minProfitCents: 3,
      stopCents: 8,
      maxHoldMs: 10000,
      latencyMs: 100,
    },
  ];
}

function rankResults(results, minTrades) {
  return results
    .filter((result) => result.summary.trades >= minTrades)
    .sort((left, right) => {
      if (right.summary.avgNetCents !== left.summary.avgNetCents) {
        return right.summary.avgNetCents - left.summary.avgNetCents;
      }
      return right.summary.netCents - left.summary.netCents;
    });
}

function reportMarkdown({ symbol, startDate, endDate, dataset, quoteDays, missingDays, ranked, results, notes }) {
  const lines = [
    `# TSLL Passive Bid-to-Ask Market-Making ${startDate} to ${endDate}`,
    '',
    `Symbol: ${symbol}`,
    `Quote dataset: ${dataset}`,
    `Quote days loaded: ${quoteDays.length}`,
    `Missing quote days: ${missingDays.length ? missingDays.join(', ') : 'none'}`,
    '',
  ];
  if (notes.length) {
    lines.push('## Notes', '');
    notes.forEach((note) => lines.push(`- ${note}`));
    lines.push('');
  }
  if (!ranked.length) {
    lines.push('## Result', '');
    lines.push('No variant met the minimum trade-count threshold. If quote files are missing, run the quote filter first.');
    lines.push('');
  } else {
    lines.push('## Ranked Variants', '');
    lines.push('| rank | strategy | trades | avg net c/share | net c/share | win rate | max DD c | fill/order | exits |');
    lines.push('| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |');
    ranked.slice(0, 12).forEach((result, index) => {
      const s = result.summary;
      lines.push([
        `| ${index + 1}`,
        result.strategy,
        s.trades,
        s.avgNetCents,
        s.netCents,
        s.winRate,
        s.maxDrawdownCents,
        s.fillRatePerOrder,
        `target=${s.sellFills} stop=${s.stopExits} timeout=${s.timeoutExits} eod=${s.endOfDayExits} |`,
      ].join(' | '));
    });
    lines.push('');
  }
  lines.push('## All Variants', '');
  results.forEach((result) => {
    const s = result.summary;
    lines.push(`- ${result.strategy}: trades=${s.trades}, avg=${s.avgNetCents}c, net=${s.netCents}c, win=${s.winRate}, dd=${s.maxDrawdownCents}c, eligibleQuotes=${s.eligibleQuotes}, buyFills=${s.buyFills}`);
  });
  lines.push('');
  lines.push('Fill caveat: SIP quote/trade data can show that a quote was touched or traded through, but it cannot prove our passive order had queue priority. Treat touch-fill variants as optimistic and through/latency variants as stricter approximations.');
  return `${lines.join('\n')}\n`;
}

async function loadDay({ config, dayIso, symbol, quoteDir }) {
  const quoteFile = quoteDir
    ? path.join(path.resolve(quoteDir), `massive-stock-quotes-${symbol}-${dayIso}.csv.gz`)
    : quotePathForDay(symbol, dayIso);
  const [quoteResult, rawTrades] = await Promise.all([
    readFilteredQuotesForDay({ config, dayIso, symbol, filePath: quoteFile }),
    readTargetTradesForDay(config, dayIso, symbol),
  ]);
  return {
    dayIso,
    quoteFile,
    quoteResult,
    trades: rawTrades.map((trade) => ({ ...trade, dayIso })),
  };
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const symbol = String(args.symbol || config.target || 'TSLL').toUpperCase();
  const startDate = args['start-date'] || args.date || '2026-01-09';
  const endDate = args['end-date'] || startDate;
  const minTrades = asNumber(args['min-trades'], 10);
  const costCentsPerSide = asNumber(args['cost-cents-per-side'], config.execution?.costCentsPerSide ?? 0.5);
  const dates = loadOpenDates(config, startDate, endDate);
  const quoteDir = args['quote-dir'] || null;
  const dataset = config.research?.massiveStockTickQuoteDataset || 'us_stocks_sip/quotes_v1';
  const notes = [];
  const dayLoads = [];
  for (const dayIso of dates) {
    const loaded = await loadDay({ config, dayIso, symbol, quoteDir });
    dayLoads.push(loaded);
    console.error(`[tsll-passive-mm] ${dayIso} quotes=${loaded.quoteResult.quotes.length} trades=${loaded.trades.length} quoteFile=${loaded.quoteFile}`);
  }
  const missingDays = dayLoads.filter((day) => day.quoteResult.missing).map((day) => day.dayIso);
  const quoteDays = dayLoads.filter((day) => !day.quoteResult.missing && day.quoteResult.quotes.length).map((day) => day.dayIso);
  if (missingDays.length) notes.push(`Missing filtered quote files for ${missingDays.join(', ')}.`);
  const quotes = dayLoads.flatMap((day) => day.quoteResult.quotes);
  const trades = dayLoads.flatMap((day) => day.trades);
  const grid = buildDefaultGrid({ costCentsPerSide, session: config.session });
  const results = quotes.length ? grid.map((settings) => (
    compactResult(simulatePassiveMarketMaking({ quotes, trades, settings }), args['include-trades'] === true)
  )) : [];
  const ranked = rankResults(results, minTrades);
  const baseName = `tsll-passive-mm-${symbol}-${startDate}-${endDate}-cost${String(costCentsPerSide).replace('.', 'p')}`;
  const jsonPath = artifactPath(`${baseName}.json`);
  const mdPath = artifactPath(`${baseName}.md`);
  ensureDir(path.dirname(jsonPath));
  const payload = {
    symbol,
    startDate,
    endDate,
    dataset,
    generatedAt: new Date().toISOString(),
    dates,
    quoteDays,
    missingDays,
    quoteFiles: dayLoads.map((day) => ({
      dayIso: day.dayIso,
      filePath: day.quoteFile,
      missing: day.quoteResult.missing,
      rowsRead: day.quoteResult.rowsRead,
      quotes: day.quoteResult.quotes.length,
      trades: day.trades.length,
    })),
    ranked,
    results,
    notes,
  };
  fs.writeFileSync(jsonPath, `${JSON.stringify(payload, null, 2)}\n`);
  fs.writeFileSync(mdPath, reportMarkdown({
    symbol,
    startDate,
    endDate,
    dataset,
    quoteDays,
    missingDays,
    ranked,
    results,
    notes,
  }));
  console.error(`[tsll-passive-mm] wrote ${jsonPath}`);
  console.error(`[tsll-passive-mm] wrote ${mdPath}`);
  console.log(JSON.stringify({
    jsonPath,
    mdPath,
    quoteDays: quoteDays.length,
    missingDays,
    top: ranked.slice(0, 3),
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
