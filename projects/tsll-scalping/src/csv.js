const fs = require('node:fs');
const readline = require('node:readline');
const zlib = require('node:zlib');

function parseCsvLine(line) {
  const out = [];
  let current = '';
  let inQuotes = false;
  const text = String(line || '');
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (char === '"') {
      if (inQuotes && text[index + 1] === '"') {
        current += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      out.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  out.push(current);
  return out;
}

async function readGzipCsv(filePath, onRow) {
  const stream = fs.createReadStream(filePath).pipe(zlib.createGunzip());
  const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let headers = null;
  let rowCount = 0;
  for await (const line of reader) {
    if (!line) continue;
    if (!headers) {
      headers = parseCsvLine(line);
      continue;
    }
    const values = parseCsvLine(line);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] ?? '';
    });
    rowCount += 1;
    await onRow(row, rowCount);
  }
  return rowCount;
}

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

module.exports = {
  parseCsvLine,
  readGzipCsv,
  toNumber,
};
