const fs = require('node:fs');
const path = require('node:path');

function loadCalendar(calendarPath) {
  const raw = JSON.parse(fs.readFileSync(calendarPath, 'utf8'));
  return raw.days || [];
}

function openCalendarDays(calendarPath, startDate, endDate) {
  return loadCalendar(calendarPath)
    .filter((day) => day.date >= startDate && day.date <= endDate && day.isOpen)
    .map((day) => day.date)
    .sort();
}

function listDatasetDates(root, datasetId) {
  const datasetRoot = path.join(root, datasetId);
  if (!fs.existsSync(datasetRoot)) return [];
  return fs.readdirSync(datasetRoot, { withFileTypes: true })
    .filter((entry) => entry.isDirectory() && entry.name.startsWith('date='))
    .map((entry) => entry.name.slice('date='.length))
    .sort();
}

function listParquetFiles(root, datasetId, dayIso) {
  const dayRoot = path.join(root, datasetId, `date=${dayIso}`);
  if (!fs.existsSync(dayRoot)) return [];
  return fs.readdirSync(dayRoot)
    .filter((name) => name.endsWith('.parquet'))
    .map((name) => path.join(dayRoot, name))
    .sort();
}

function readSuccessManifest(root, datasetId, dayIso) {
  const successPath = path.join(root, datasetId, `date=${dayIso}`, '_SUCCESS.json');
  if (!fs.existsSync(successPath)) return null;
  return JSON.parse(fs.readFileSync(successPath, 'utf8'));
}

function coverageForDataset({ root, datasetId, calendarDays }) {
  const dates = listDatasetDates(root, datasetId);
  const startDate = calendarDays[0] || null;
  const endDate = calendarDays[calendarDays.length - 1] || null;
  const windowDates = startDate && endDate
    ? dates.filter((day) => day >= startDate && day <= endDate)
    : dates;
  const dateSet = new Set(windowDates);
  const missingOpenDays = calendarDays.filter((day) => !dateSet.has(day));
  const extraDays = windowDates.filter((day) => !calendarDays.includes(day));
  const successMissing = windowDates.filter((day) => !fs.existsSync(path.join(root, datasetId, `date=${day}`, '_SUCCESS.json')));
  return {
    datasetId,
    dateCount: dates.length,
    windowDateCount: windowDates.length,
    minDate: dates[0] || null,
    maxDate: dates[dates.length - 1] || null,
    missingOpenDayCount: missingOpenDays.length,
    missingOpenDays,
    extraDayCount: extraDays.length,
    extraDays,
    successMissingCount: successMissing.length,
    successMissing,
    ready: missingOpenDays.length === 0 && successMissing.length === 0,
  };
}

function liveParquetCoverage({ root, datasetIds, dayIso }) {
  return Object.fromEntries(datasetIds.map((datasetId) => {
    const files = listParquetFiles(root, datasetId, dayIso);
    return [datasetId, {
      available: files.length > 0,
      fileCount: files.length,
      files,
    }];
  }));
}

module.exports = {
  loadCalendar,
  openCalendarDays,
  listDatasetDates,
  listParquetFiles,
  readSuccessManifest,
  coverageForDataset,
  liveParquetCoverage,
};
