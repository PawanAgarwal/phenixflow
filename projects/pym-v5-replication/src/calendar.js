const fs = require('node:fs');
const path = require('node:path');

function loadCalendar(calendarPath) {
  return JSON.parse(fs.readFileSync(calendarPath, 'utf8'));
}

function openCalendarDays(calendarPath, startDate, endDate) {
  const calendar = loadCalendar(calendarPath);
  return (calendar.days || [])
    .filter((day) => day.date >= startDate && day.date <= endDate && day.isOpen)
    .map((day) => ({
      date: day.date,
      closeTimeEt: day.equitiesCloseTimeEt || '16:00',
      expectedEquitiesMinutes: day.expectedEquitiesMinutes || 390,
    }));
}

function latestDatasetDate(root, datasetId) {
  const datasetRoot = path.join(root, datasetId);
  if (!fs.existsSync(datasetRoot)) return null;
  const dates = fs.readdirSync(datasetRoot)
    .filter((entry) => entry.startsWith('date='))
    .map((entry) => entry.slice('date='.length))
    .sort();
  return dates.at(-1) || null;
}

function latestDatasetDateAcrossRoots(roots, datasetId) {
  const dates = roots
    .filter(Boolean)
    .map((root) => latestDatasetDate(root, datasetId))
    .filter(Boolean)
    .sort();
  return dates.at(-1) || null;
}

function resolveEndDate(config, requestedEndDate = config.windows.endDate) {
  if (requestedEndDate && requestedEndDate !== 'auto') return requestedEndDate;
  const latest = latestDatasetDateAcrossRoots(
    [config.roots.historical, config.roots.liveParquet],
    config.datasets.stockBars,
  );
  if (!latest) throw new Error('No local Massive stock bar dates were found.');
  return latest;
}

function closeMinuteEt(day) {
  const [hour, minute] = String(day.closeTimeEt || '16:00').split(':').map(Number);
  return (hour * 60) + minute;
}

module.exports = {
  loadCalendar,
  openCalendarDays,
  latestDatasetDate,
  latestDatasetDateAcrossRoots,
  resolveEndDate,
  closeMinuteEt,
};
