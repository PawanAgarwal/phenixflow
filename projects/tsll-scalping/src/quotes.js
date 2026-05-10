const fs = require('node:fs');

const { runtimePath } = require('./config');
const { readGzipCsv, toNumber } = require('./csv');
const { getEtParts, isRegularSessionMs, nsToMs } = require('./time');

function quotePathForDay(symbol, dayIso) {
  return runtimePath('tick-quotes', `massive-stock-quotes-${String(symbol).toUpperCase()}-${dayIso}.csv.gz`);
}

function firstDefined(row, keys) {
  for (const key of keys) {
    if (row[key] !== undefined && row[key] !== '') return row[key];
  }
  return undefined;
}

function parseQuoteRow(row) {
  const tsMs = nsToMs(firstDefined(row, [
    'sip_timestamp',
    'participant_timestamp',
    'trf_timestamp',
    'timestamp',
  ]));
  const bidPrice = toNumber(firstDefined(row, ['bid_price', 'bid', 'bp']));
  const askPrice = toNumber(firstDefined(row, ['ask_price', 'ask', 'ap']));
  const bidSize = toNumber(firstDefined(row, ['bid_size', 'bid_size_lots', 'bs'])) || 0;
  const askSize = toNumber(firstDefined(row, ['ask_size', 'ask_size_lots', 'as'])) || 0;
  return {
    symbol: String(firstDefined(row, ['ticker', 'symbol']) || '').toUpperCase(),
    tsMs,
    bidPrice,
    askPrice,
    bidSize,
    askSize,
    bidExchange: firstDefined(row, ['bid_exchange', 'bid_exchange_id']),
    askExchange: firstDefined(row, ['ask_exchange', 'ask_exchange_id']),
    conditions: firstDefined(row, ['conditions', 'indicators']) || '',
  };
}

function isUsableQuote(quote, session) {
  return Number.isFinite(quote.tsMs)
    && Number.isFinite(quote.bidPrice)
    && Number.isFinite(quote.askPrice)
    && quote.bidPrice > 0
    && quote.askPrice > quote.bidPrice
    && isRegularSessionMs(quote.tsMs, session);
}

async function readFilteredQuotesForDay({
  config,
  dayIso,
  symbol = config.target,
  filePath = quotePathForDay(symbol, dayIso),
}) {
  const quotes = [];
  if (!fs.existsSync(filePath)) return { filePath, quotes, missing: true, rowsRead: 0 };
  const wanted = String(symbol || '').toUpperCase();
  let rowsRead = 0;
  await readGzipCsv(filePath, (row) => {
    rowsRead += 1;
    const quote = parseQuoteRow(row);
    if (quote.symbol && quote.symbol !== wanted) return undefined;
    if (!isUsableQuote(quote, config.session)) return undefined;
    const parts = getEtParts(quote.tsMs);
    quotes.push({
      ...quote,
      dayIso,
      minuteOfDayEt: parts.minuteOfDayEt,
    });
    return undefined;
  });
  quotes.sort((left, right) => left.tsMs - right.tsMs);
  return { filePath, quotes, missing: false, rowsRead };
}

module.exports = {
  isUsableQuote,
  parseQuoteRow,
  quotePathForDay,
  readFilteredQuotesForDay,
};
