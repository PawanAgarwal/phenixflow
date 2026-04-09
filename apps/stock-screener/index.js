#!/usr/bin/env node
/**
 * 200-Week Moving Average Stock Screener
 *
 * Strategy: "If all you ever did was buy high-quality stocks on the 200-week
 * moving average, you would beat the S&P 500 by a large margin."
 *
 * Usage:
 *   node index.js                          # Screen all (SP500 + NASDAQ100)
 *   node index.js --index sp500            # S&P 500 only
 *   node index.js --index nasdaq100        # NASDAQ-100 only
 *   node index.js --symbols AAPL,MSFT,GOOG # Custom symbols
 *   node index.js --threshold 3            # Within 3% of 200w SMA (default 5%)
 *   node index.js --quality 5              # Min quality score (default 5)
 *   node index.js --no-quality             # Skip quality enrichment (faster)
 *   node index.js --json                   # JSON output
 */

import { getUniverse } from './universes.js';
import { screenStocks } from './screener.js';
import { enrichWithQuality } from './quality-filter.js';

async function main() {
  const args = parseArgs(process.argv.slice(2));

  const threshold = args.threshold || 5;
  const minQuality = args.quality !== undefined ? args.quality : 5;
  const skipQuality = args['no-quality'] || false;
  const jsonOutput = args.json || false;
  const indexName = args.index || 'all';

  // Determine ticker list
  let tickers;
  if (args.symbols) {
    tickers = args.symbols.split(',').map(s => s.trim().toUpperCase());
  } else {
    tickers = getUniverse(indexName);
  }

  console.log('='.repeat(70));
  console.log('  200-WEEK MOVING AVERAGE STOCK SCREENER');
  console.log('  "Buy high-quality stocks on the 200-week moving average"');
  console.log('='.repeat(70));
  console.log();
  console.log(`Universe:    ${args.symbols ? 'Custom' : indexName.toUpperCase()} (${tickers.length} stocks)`);
  console.log(`Threshold:   Within ${threshold}% of 200-week SMA`);
  console.log(`Quality min: ${skipQuality ? 'Disabled' : minQuality + '/13'}`);
  console.log(`Date:        ${new Date().toISOString().split('T')[0]}`);
  console.log();

  // Phase 1: Screen for stocks near 200-week SMA
  console.log('[Phase 1] Screening for stocks near 200-week SMA...');
  const phase1Results = await screenStocks(tickers, {
    maxDistancePct: threshold,
    concurrency: 5,
    onProgress: (done, total) => {
      process.stdout.write(`\r  Progress: ${done}/${total} stocks analyzed...`);
    },
  });
  console.log(`\n  Found ${phase1Results.length} stocks within ${threshold}% of 200w SMA\n`);

  if (phase1Results.length === 0) {
    console.log('No stocks found near the 200-week moving average.');
    console.log('This is normal during strong bull markets when most stocks');
    console.log('are trading well above their long-term averages.');
    console.log('\nTry increasing --threshold (e.g., --threshold 10) to widen the search.');
    return;
  }

  // Phase 2: Enrich with quality metrics
  let finalResults;
  if (skipQuality) {
    finalResults = phase1Results;
  } else {
    console.log('[Phase 2] Fetching quality metrics...');
    finalResults = await enrichWithQuality(phase1Results, {
      minQualityScore: minQuality,
      concurrency: 3,
      onProgress: (done, total) => {
        process.stdout.write(`\r  Progress: ${done}/${total} quality checks...`);
      },
    });
    console.log(`\n  ${finalResults.length} stocks pass quality filter (score >= ${minQuality})\n`);
  }

  if (finalResults.length === 0) {
    console.log('No stocks pass both the 200w SMA proximity and quality filters.');
    console.log('Try lowering --quality or increasing --threshold.');
    return;
  }

  // Output
  if (jsonOutput) {
    console.log(JSON.stringify(finalResults, null, 2));
  } else {
    printResults(finalResults, skipQuality);
    printSummary(finalResults, skipQuality);
  }
}

function printResults(results, skipQuality) {
  console.log('='.repeat(70));
  console.log('  RESULTS: Stocks Near 200-Week Moving Average');
  console.log('='.repeat(70));
  console.log();

  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    const position = r.distancePct >= 0 ? 'ABOVE' : 'BELOW';
    const arrow = r.distancePct >= 0 ? '+' : '';

    console.log(`  ${i + 1}. ${r.symbol}${r.shortName ? ` (${r.shortName})` : ''}`);
    console.log(`     Price: $${r.currentPrice}  |  200w SMA: $${r.sma200w}  |  ${arrow}${r.distancePct}% ${position}`);

    if (r.sma50w) {
      console.log(`     50w SMA: $${r.sma50w}  |  Trend: ${r.trend === 'above_50w' ? 'Bullish (above 50w)' : 'Bearish (below 50w)'}`);
    }

    console.log(`     52w High: $${r.high52w} (${r.fromHigh52Pct}%)  |  52w Low: $${r.low52w}`);

    if (r.convergingFromAbove) {
      console.log(`     ** CONVERGING FROM ABOVE ** (recently crossed below 200w SMA)`);
    }

    if (!skipQuality && r.qualityScore !== undefined) {
      console.log(`     Quality: ${r.qualityScore}/13  |  ${r.marketCapLabel}  |  Sector: ${r.sector}`);
      console.log(`     P/E: ${r.trailingPE || 'N/A'} (fwd: ${r.forwardPE || 'N/A'})  |  Margins: ${r.operatingMargins != null ? r.operatingMargins + '%' : 'N/A'}  |  ROE: ${r.returnOnEquity != null ? r.returnOnEquity + '%' : 'N/A'}`);
      console.log(`     Rev Growth: ${r.revenueGrowth != null ? r.revenueGrowth + '%' : 'N/A'}  |  D/E: ${r.debtToEquity || 'N/A'}  |  Div Yield: ${r.dividendYield != null ? r.dividendYield + '%' : 'N/A'}`);
      console.log(`     Flags: ${r.qualityFlags.join(', ')}`);
    }

    console.log();
  }
}

function printSummary(results, skipQuality) {
  console.log('='.repeat(70));
  console.log('  SUMMARY');
  console.log('='.repeat(70));
  console.log();

  // Categorize
  const onMA = results.filter(r => r.absDistancePct <= 2);
  const nearMA = results.filter(r => r.absDistancePct > 2 && r.absDistancePct <= 5);
  const approaching = results.filter(r => r.absDistancePct > 5);

  if (onMA.length > 0) {
    console.log(`  ON the 200w SMA (within 2%): ${onMA.map(r => r.symbol).join(', ')}`);
  }
  if (nearMA.length > 0) {
    console.log(`  NEAR the 200w SMA (2-5%):    ${nearMA.map(r => r.symbol).join(', ')}`);
  }
  if (approaching.length > 0) {
    console.log(`  APPROACHING (5%+):           ${approaching.map(r => r.symbol).join(', ')}`);
  }

  const below = results.filter(r => r.distancePct < 0);
  const above = results.filter(r => r.distancePct >= 0);
  console.log();
  console.log(`  Below 200w SMA: ${below.length} stocks ${below.length > 0 ? '(' + below.map(r => r.symbol).join(', ') + ')' : ''}`);
  console.log(`  Above 200w SMA: ${above.length} stocks ${above.length > 0 ? '(' + above.map(r => r.symbol).join(', ') + ')' : ''}`);

  if (!skipQuality) {
    // Top picks: highest quality + closest to MA
    const topPicks = results.slice(0, Math.min(5, results.length));
    console.log();
    console.log('  TOP PICKS (highest quality, closest to 200w SMA):');
    for (const r of topPicks) {
      const dir = r.distancePct >= 0 ? '+' : '';
      console.log(`    ${r.symbol.padEnd(6)} $${String(r.currentPrice).padEnd(10)} ${dir}${r.distancePct}% from 200w SMA  Quality: ${r.qualityScore}/13`);
    }
  }

  const converging = results.filter(r => r.convergingFromAbove);
  if (converging.length > 0) {
    console.log();
    console.log(`  MOMENTUM SHIFT (recently crossed below 200w SMA):`);
    console.log(`    ${converging.map(r => r.symbol).join(', ')}`);
  }

  console.log();
  console.log('-'.repeat(70));
  console.log('  Note: This is a screening tool, not investment advice.');
  console.log('  Always do your own due diligence before investing.');
  console.log('-'.repeat(70));
}

/**
 * Simple argument parser.
 */
function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    if (arg === '--no-quality') {
      args['no-quality'] = true;
    } else if (arg === '--json') {
      args.json = true;
    } else if (arg.startsWith('--')) {
      const key = arg.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith('--')) {
        args[key] = isNaN(Number(next)) ? next : Number(next);
        i++;
      } else {
        args[key] = true;
      }
    }
  }
  return args;
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
