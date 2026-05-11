const fs = require('node:fs');

const { resolveDatasetSource } = require('./config');
const { listDates } = require('./time');

function loadOpenDates(config, startDate, endDate) {
  if (config.roots.calendar && fs.existsSync(config.roots.calendar)) {
    const calendar = JSON.parse(fs.readFileSync(config.roots.calendar, 'utf8'));
    if (Array.isArray(calendar.days)) {
      return calendar.days
        .filter((day) => day.date >= startDate && day.date <= endDate && day.isOpen)
        .map((day) => day.date);
    }
  }
  return listDates(startDate, endDate).filter((dayIso) => {
    const day = new Date(`${dayIso}T00:00:00.000Z`).getUTCDay();
    return day !== 0 && day !== 6;
  });
}

function availableDates(config, startDate, endDate, requiredDatasets = ['stockTrades', 'stockBars']) {
  return loadOpenDates(config, startDate, endDate).filter((dayIso) => (
    requiredDatasets.every((datasetKey) => resolveDatasetSource(config, datasetKey, dayIso).format !== 'missing')
  ));
}

module.exports = {
  loadOpenDates,
  availableDates,
};
