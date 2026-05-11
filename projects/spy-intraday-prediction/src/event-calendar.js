const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_CALENDAR_PATH = path.resolve(__dirname, '..', 'config', 'event-calendar.json');

let cached = null;
function loadCalendar(p = DEFAULT_CALENDAR_PATH) {
  if (cached) return cached;
  cached = JSON.parse(fs.readFileSync(p, 'utf8'));
  return cached;
}

function buildEventIndex(calendar = loadCalendar()) {
  const idx = new Map(); // date → Set of event types
  for (const [kind, dates] of Object.entries(calendar)) {
    if (kind.startsWith('_')) continue;
    for (const d of dates) {
      if (!idx.has(d)) idx.set(d, new Set());
      idx.get(d).add(kind);
    }
  }
  return idx;
}

function tagDay(dayIso, idx = buildEventIndex()) {
  const events = idx.get(dayIso);
  return {
    date: dayIso,
    events: events ? Array.from(events) : [],
    is_event: Boolean(events && events.size > 0),
    is_fomc: Boolean(events && events.has('fomc')),
    is_cpi: Boolean(events && events.has('cpi')),
    is_ppi: Boolean(events && events.has('ppi')),
    is_nfp: Boolean(events && events.has('nfp')),
    is_opex: Boolean(events && events.has('opex')),
  };
}

module.exports = {
  loadCalendar,
  buildEventIndex,
  tagDay,
};
