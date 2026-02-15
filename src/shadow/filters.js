function oldFilter(symbols) {
  return symbols.filter((s) => s.volume > 0).map((s) => s.ticker);
}

function newFilter(symbols) {
  return symbols
    .filter((s) => s.volume >= 100000)
    .filter((s) => s.price >= 5)
    .filter((s) => s.volatility <= 0.06)
    .map((s) => s.ticker);
}

module.exports = {
  oldFilter,
  newFilter,
};
