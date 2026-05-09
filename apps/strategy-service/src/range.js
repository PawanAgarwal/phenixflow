function parseLimit(value, fallback = 500) {
  const parsed = Math.trunc(Number(value));
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(1, Math.min(10000, parsed));
}

function normalizeDate(value) {
  const text = String(value || '').trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? text : null;
}

function filterByRange(items, { start, end, dateKey = 'date' } = {}) {
  return items.filter((item) => {
    const date = item[dateKey];
    return (!start || date >= start) && (!end || date <= end);
  });
}

function lastOrNull(items) {
  return items.length ? items[items.length - 1] : null;
}

module.exports = {
  parseLimit,
  normalizeDate,
  filterByRange,
  lastOrNull,
};
