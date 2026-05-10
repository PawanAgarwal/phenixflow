#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { ensureDir, PROJECT_ROOT } = require('../src/config');

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

function round(value, digits = 4) {
  return Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function groupTradesByDay(trades) {
  const byDay = new Map();
  trades.forEach((trade) => {
    const current = byDay.get(trade.tradeDate) || {
      date: trade.tradeDate,
      trades: 0,
      wins: 0,
      netCents: 0,
      grossCents: 0,
      buyCapitalPerShareUnit: 0,
      avgEntryNumerator: 0,
    };
    current.trades += 1;
    current.wins += trade.netCents > 0 ? 1 : 0;
    current.netCents += trade.netCents;
    current.grossCents += trade.grossCents;
    current.buyCapitalPerShareUnit += trade.entryPrice;
    current.avgEntryNumerator += trade.entryPrice;
    byDay.set(trade.tradeDate, current);
  });
  return [...byDay.values()].sort((left, right) => left.date.localeCompare(right.date)).map((day) => {
    const avgEntry = day.trades ? day.avgEntryNumerator / day.trades : 0;
    const pnlDollarsPerShareUnit = day.netCents / 100;
    return {
      date: day.date,
      trades: day.trades,
      winRate: day.trades ? round(day.wins / day.trades, 6) : 0,
      netCents: round(day.netCents, 4),
      grossCents: round(day.grossCents, 4),
      pnlDollarsPerShareUnit: round(pnlDollarsPerShareUnit, 6),
      pnlPer1000Shares: round(pnlDollarsPerShareUnit * 1000, 2),
      buyCapitalPer1000Shares: round(day.buyCapitalPerShareUnit * 1000, 2),
      avgEntry: round(avgEntry, 6),
      returnOnBuyTurnover: day.buyCapitalPerShareUnit ? round(pnlDollarsPerShareUnit / day.buyCapitalPerShareUnit, 8) : 0,
      returnOnRecycledCapital: avgEntry ? round(pnlDollarsPerShareUnit / avgEntry, 8) : 0,
    };
  });
}

function summarize(days, trades) {
  const totals = days.reduce((acc, day) => {
    acc.trades += day.trades;
    acc.netCents += day.netCents;
    acc.pnlPer1000Shares += day.pnlPer1000Shares;
    acc.buyCapitalPer1000Shares += day.buyCapitalPer1000Shares;
    acc.avgEntryNumerator += day.avgEntry * day.trades;
    acc.winningDays += day.netCents > 0 ? 1 : 0;
    return acc;
  }, {
    trades: 0,
    netCents: 0,
    pnlPer1000Shares: 0,
    buyCapitalPer1000Shares: 0,
    avgEntryNumerator: 0,
    winningDays: 0,
  });
  const wins = trades.filter((trade) => trade.netCents > 0).length;
  const avgEntry = totals.trades ? totals.avgEntryNumerator / totals.trades : 0;
  const pnlDollarsPerShareUnit = totals.netCents / 100;
  return {
    days: days.length,
    winningDays: totals.winningDays,
    trades: totals.trades,
    winRate: totals.trades ? round(wins / totals.trades, 6) : 0,
    netCents: round(totals.netCents, 4),
    avgNetCents: totals.trades ? round(totals.netCents / totals.trades, 6) : 0,
    pnlPer1000Shares: round(totals.pnlPer1000Shares, 2),
    buyCapitalPer1000Shares: round(totals.buyCapitalPer1000Shares, 2),
    avgEntry: round(avgEntry, 6),
    returnOnBuyTurnover: totals.buyCapitalPer1000Shares ? round(totals.pnlPer1000Shares / totals.buyCapitalPer1000Shares, 8) : 0,
    returnOnRecycledCapital: avgEntry ? round(pnlDollarsPerShareUnit / avgEntry, 8) : 0,
  };
}

function exportReport({ sourcePath, outputPath }) {
  const source = JSON.parse(fs.readFileSync(sourcePath, 'utf8'));
  const best = source.bestWithTrades;
  if (!best?.trades?.length) throw new Error(`missing_best_trades:${sourcePath}`);
  const days = groupTradesByDay(best.trades);
  const payload = {
    generatedAt: new Date().toISOString(),
    sourceArtifact: path.relative(path.resolve(PROJECT_ROOT, '..', '..'), sourcePath),
    project: 'tsll-scalping',
    provider: source.provider || 'Massive',
    symbol: 'TSLL',
    startDate: source.startDate,
    endDate: source.endDate,
    strategy: {
      id: 'tsll-seconds-passive-b3-t3-s5-h10',
      name: 'TSLL Seconds Passive Limit Scalper',
      description: 'Buy TSLL 3 cents below the prior completed 1-second close, target +3 cents, stop 5 cents, and exit after 10 seconds.',
      settings: best.settings,
    },
    assumptions: {
      explicitCostCentsPerSide: source.costCentsPerSide || 0,
      data: 'TSLL Massive tick trades converted to 1-second OHLCV bars plus 1-minute SPY/QQQ/TSLA market context.',
      caveat: 'Seconds bars show traded prices, not actual bid/ask queue priority; validate with quote data before live use.',
    },
    totals: summarize(days, best.trades),
    days,
  };
  ensureDir(path.dirname(outputPath));
  fs.writeFileSync(outputPath, `${JSON.stringify(payload, null, 2)}\n`);
  return payload;
}

function main() {
  const args = parseArgs();
  const sourcePath = path.resolve(args.source || path.join(PROJECT_ROOT, 'artifacts', 'tsll-seconds-passive-mm-fixed-feb2026-cost0.json'));
  const outputPath = path.resolve(args.output || path.join(PROJECT_ROOT, 'reports', 'tsll-seconds-passive-fixed-feb2026.json'));
  const payload = exportReport({ sourcePath, outputPath });
  console.log(JSON.stringify({
    outputPath,
    days: payload.totals.days,
    trades: payload.totals.trades,
    netCents: payload.totals.netCents,
    returnOnRecycledCapital: payload.totals.returnOnRecycledCapital,
  }, null, 2));
}

main();
