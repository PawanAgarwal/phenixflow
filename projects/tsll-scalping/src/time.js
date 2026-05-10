const ET_FORMATTER = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

function nsToMs(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return Number(BigInt(raw) / 1_000_000n);
  } catch {
    return null;
  }
}

function nsToMinuteMs(value) {
  const ms = nsToMs(value);
  return Number.isFinite(ms) ? Math.floor(ms / 60000) * 60000 : null;
}

function floorToBucketMs(ms, bucketSeconds) {
  const bucketMs = Math.max(1, Math.trunc(bucketSeconds || 1)) * 1000;
  return Math.floor(ms / bucketMs) * bucketMs;
}

function minuteMsToIso(minuteMs) {
  if (!Number.isFinite(minuteMs)) return null;
  return new Date(minuteMs).toISOString();
}

function getEtParts(ms) {
  const parts = {};
  for (const part of ET_FORMATTER.formatToParts(new Date(ms))) {
    if (part.type !== 'literal') parts[part.type] = part.value;
  }
  const hour = Number(parts.hour === '24' ? '00' : parts.hour);
  const minute = Number(parts.minute);
  const second = Number(parts.second || 0);
  return {
    dateEt: `${parts.year}-${parts.month}-${parts.day}`,
    hour,
    minute,
    second,
    minuteOfDayEt: (hour * 60) + minute,
    secondOfDayEt: (hour * 3600) + (minute * 60) + second,
  };
}

function isRegularSessionMs(ms, session) {
  const minuteOfDayEt = getEtParts(ms).minuteOfDayEt;
  return minuteOfDayEt >= session.regularOpenMinuteEt && minuteOfDayEt < session.regularCloseMinuteEt;
}

function etMinuteToUtcMs(dateIso, minuteOfDayEt) {
  const start = Date.parse(`${dateIso}T00:00:00.000Z`);
  const end = start + (36 * 60 * 60000);
  for (let ms = start; ms <= end; ms += 60000) {
    const parts = getEtParts(ms);
    if (parts.dateEt === dateIso && parts.minuteOfDayEt === minuteOfDayEt) return ms;
  }
  return null;
}

function listDates(startDate, endDate) {
  const out = [];
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  while (cursor <= end) {
    out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

module.exports = {
  nsToMs,
  nsToMinuteMs,
  floorToBucketMs,
  minuteMsToIso,
  getEtParts,
  isRegularSessionMs,
  etMinuteToUtcMs,
  listDates,
};
