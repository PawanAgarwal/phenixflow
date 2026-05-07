const { openCalendarDays } = require('./coverage');

function assertHistoricalWindow(config, startDate, endDate, { allowProvisional = false } = {}) {
  const cutoff = config.dataPolicy.historicalCutoffDate;
  if (!allowProvisional && endDate > cutoff) {
    throw new Error(
      `historical_cutoff_exceeded:${endDate}:formal datasets stop at ${cutoff}; pass --allow-provisional for paper/live-style work`,
    );
  }
}

function datesForRange(config, startDate, endDate, settings = {}) {
  assertHistoricalWindow(config, startDate, endDate, settings);
  return openCalendarDays(config.roots.calendar, startDate, endDate);
}

function configuredWindows(config) {
  return [config.windows.train, ...config.windows.tests];
}

function rowsInWindow(rows, window) {
  return rows.filter((row) => row.tradeDate >= window.startDate && row.tradeDate <= window.endDate);
}

function splitRowsByConfig(rows, config) {
  return {
    train: rowsInWindow(rows, config.windows.train),
    sensitivityTrain: config.windows.sensitivityTrain ? rowsInWindow(rows, config.windows.sensitivityTrain) : [],
    tests: config.windows.tests.map((window) => ({
      window,
      rows: rowsInWindow(rows, window),
    })),
  };
}

module.exports = {
  assertHistoricalWindow,
  datesForRange,
  configuredWindows,
  rowsInWindow,
  splitRowsByConfig,
};
