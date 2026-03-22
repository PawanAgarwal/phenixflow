const fs = require('node:fs');
const path = require('node:path');

function addDays(isoDate, offsetDays) {
  const date = new Date(`${isoDate}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + offsetDays);
  return date.toISOString().slice(0, 10);
}

function dateFromParts(year, month, day) {
  return new Date(Date.UTC(year, month - 1, day));
}

function thirdFriday(year, month) {
  const first = dateFromParts(year, month, 1);
  const firstDay = first.getUTCDay();
  const daysToFriday = (5 - firstDay + 7) % 7;
  const firstFriday = 1 + daysToFriday;
  const thirdFridayDay = firstFriday + 14;
  return dateFromParts(year, month, thirdFridayDay).toISOString().slice(0, 10);
}

function buildOpexDates(startDate, endDate) {
  const out = [];
  let cursor = new Date(`${startDate}T00:00:00.000Z`);
  cursor.setUTCDate(1);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  while (cursor <= end) {
    const year = cursor.getUTCFullYear();
    const month = cursor.getUTCMonth() + 1;
    const expiry = thirdFriday(year, month);
    if (expiry >= startDate && expiry <= endDate) out.push(expiry);
    cursor.setUTCMonth(cursor.getUTCMonth() + 1);
  }
  return out;
}

function loadJsonArray(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return [];
  const value = JSON.parse(fs.readFileSync(filePath, 'utf8'));
  return Array.isArray(value) ? value : [];
}

function loadEventCalendar(options = {}) {
  const startDate = options.startDate;
  const endDate = options.endDate;
  const fomcPath = options.fomcPath || path.join(__dirname, '..', 'config', 'fomc-dates.json');
  const blsPath = options.blsPath || path.join(__dirname, '..', 'config', 'bls-major-release-calendar.json');
  const earningsPath = options.earningsPath || path.join(__dirname, '..', 'config', 'major-earnings-days.json');

  const fomcDates = new Set(loadJsonArray(fomcPath));
  const earningsDates = new Set(loadJsonArray(earningsPath));
  const blsRows = loadJsonArray(blsPath);
  const macroByType = new Map();
  blsRows.forEach((row) => {
    const key = String(row.type || '').trim();
    const date = String(row.date || '').slice(0, 10);
    if (!key || !date) return;
    const current = macroByType.get(key) || new Set();
    current.add(date);
    macroByType.set(key, current);
  });

  const monthlyOpex = new Set(buildOpexDates(startDate, endDate));
  const quarterlyOpex = new Set(Array.from(monthlyOpex).filter((date) => {
    const month = Number(date.slice(5, 7));
    return [3, 6, 9, 12].includes(month);
  }));

  return {
    fomcDates,
    earningsDates,
    monthlyOpex,
    quarterlyOpex,
    macroByType,
  };
}

function hasDate(setLike, isoDate) {
  return Boolean(setLike && setLike.has && setLike.has(isoDate));
}

function annotateDailyEventFeatures(dailyFeatures = [], calendar = null) {
  if (!calendar) return dailyFeatures;
  return dailyFeatures.map((row) => {
    const date = row.tradeDateUtc;
    const isFomcDay = hasDate(calendar.fomcDates, date);
    const isMonthlyOpex = hasDate(calendar.monthlyOpex, date);
    const isQuarterlyOpex = hasDate(calendar.quarterlyOpex, date);
    const isCpiDay = hasDate(calendar.macroByType.get('CPI'), date);
    const isPpiDay = hasDate(calendar.macroByType.get('PPI'), date);
    const isNfpDay = hasDate(calendar.macroByType.get('NFP'), date);
    const isJoltsDay = hasDate(calendar.macroByType.get('JOLTS'), date);
    const isMajorEarningsDay = hasDate(calendar.earningsDates, date);
    const isPreFomcDay = hasDate(calendar.fomcDates, addDays(date, 1));
    const isPostFomcDay = hasDate(calendar.fomcDates, addDays(date, -1));
    const isPreMonthlyOpexDay = hasDate(calendar.monthlyOpex, addDays(date, 1));
    const isPostMonthlyOpexDay = hasDate(calendar.monthlyOpex, addDays(date, -1));
    const macroEventCount = [isCpiDay, isPpiDay, isNfpDay, isJoltsDay].filter(Boolean).length;
    const eventScore = (
      (isFomcDay ? 3 : 0)
      + (isQuarterlyOpex ? 2 : 0)
      + (isMonthlyOpex ? 1 : 0)
      + macroEventCount
      + (isMajorEarningsDay ? 1 : 0)
    );

    return {
      ...row,
      isFomcDay,
      isMonthlyOpex,
      isQuarterlyOpex,
      isCpiDay,
      isPpiDay,
      isNfpDay,
      isJoltsDay,
      isMajorEarningsDay,
      isMacroEventDay: macroEventCount > 0,
      isPreFomcDay,
      isPostFomcDay,
      isPreMonthlyOpexDay,
      isPostMonthlyOpexDay,
      eventScore,
      avoidSuggested: eventScore >= 2 || isPreFomcDay,
    };
  });
}

module.exports = {
  addDays,
  thirdFriday,
  buildOpexDates,
  loadEventCalendar,
  annotateDailyEventFeatures,
};
