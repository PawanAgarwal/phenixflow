const ET_FORMATTER = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
});

function nsToMinuteMs(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const minuteNs = 60_000_000_000n;
  try {
    const minuteIndex = BigInt(raw) / minuteNs;
    return Number(minuteIndex * 60_000n);
  } catch {
    return null;
  }
}

function minuteMsToIso(minuteMs) {
  if (!Number.isFinite(minuteMs)) return null;
  return new Date(minuteMs).toISOString();
}

function getEtParts(minuteMs) {
  const parts = {};
  for (const part of ET_FORMATTER.formatToParts(new Date(minuteMs))) {
    if (part.type !== 'literal') parts[part.type] = part.value;
  }
  const hour = Number(parts.hour === '24' ? '00' : parts.hour);
  const minute = Number(parts.minute);
  return {
    dateEt: `${parts.year}-${parts.month}-${parts.day}`,
    hour,
    minute,
    minuteOfDayEt: (hour * 60) + minute,
  };
}

function isRegularSessionMinute(minuteMs, session) {
  const minuteOfDayEt = getEtParts(minuteMs).minuteOfDayEt;
  return minuteOfDayEt >= session.regularOpenMinuteEt && minuteOfDayEt < session.regularCloseMinuteEt;
}

function formatDateIso(date) {
  return date.toISOString().slice(0, 10);
}

function listCalendarDates(startDate, endDate) {
  const out = [];
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  while (cursor <= end) {
    out.push(formatDateIso(cursor));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

module.exports = {
  nsToMinuteMs,
  minuteMsToIso,
  getEtParts,
  isRegularSessionMinute,
  listCalendarDates,
};
