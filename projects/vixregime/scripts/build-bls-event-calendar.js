#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const START_DATE = String(process.env.START_DATE || '2025-01-01').trim();
const END_DATE = String(process.env.END_DATE || '2026-12-31').trim();
const OUTPUT_PATH = path.resolve(
  process.env.OUTPUT_PATH
    || path.join(process.cwd(), 'vixregime', 'config', 'bls-major-release-calendar.json'),
);
const ICS_URL = 'https://www.bls.gov/schedule/news_release/bls.ics';

function parseIcsDate(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^\d{8}$/.test(raw)) return `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
  if (/^\d{8}T/.test(raw)) return `${raw.slice(0, 4)}-${raw.slice(4, 6)}-${raw.slice(6, 8)}`;
  return null;
}

function classifySummary(summary = '') {
  const value = summary.toLowerCase();
  if (value.includes('consumer price index')) return 'CPI';
  if (value.includes('producer price index')) return 'PPI';
  if (value.includes('employment situation')) return 'NFP';
  if (value.includes('job openings and labor turnover')) return 'JOLTS';
  return null;
}

async function run() {
  const response = await fetch(ICS_URL);
  if (!response.ok) throw new Error(`bls_fetch_failed:${response.status}`);
  const text = await response.text();
  const events = [];
  let current = null;

  text.split(/\r?\n/).forEach((line) => {
    if (line === 'BEGIN:VEVENT') {
      current = {};
      return;
    }
    if (line === 'END:VEVENT') {
      if (current) events.push(current);
      current = null;
      return;
    }
    if (!current) return;
    const separator = line.indexOf(':');
    if (separator === -1) return;
    const key = line.slice(0, separator).split(';')[0];
    const value = line.slice(separator + 1);
    current[key] = value;
  });

  const filtered = events
    .map((event) => {
      const date = parseIcsDate(event.DTSTART);
      const type = classifySummary(event.SUMMARY);
      if (!date || !type) return null;
      if (date < START_DATE || date > END_DATE) return null;
      return { date, type, summary: event.SUMMARY };
    })
    .filter(Boolean)
    .sort((left, right) => left.date.localeCompare(right.date) || left.type.localeCompare(right.type));

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, `${JSON.stringify(filtered, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({
    outputPath: OUTPUT_PATH,
    rowCount: filtered.length,
    startDate: filtered[0]?.date || null,
    endDate: filtered[filtered.length - 1]?.date || null,
    eventTypes: Array.from(new Set(filtered.map((row) => row.type))),
  }, null, 2));
}

run().catch((error) => {
  console.error(error.stack || String(error));
  process.exitCode = 1;
});
