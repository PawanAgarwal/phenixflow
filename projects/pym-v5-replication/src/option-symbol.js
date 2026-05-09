function parseOpraTicker(ticker) {
  const normalized = String(ticker || '').trim().toUpperCase();
  const match = normalized.match(/^O:([A-Z0-9]+?)(\d{6})([CP])(\d{8})$/);
  if (!match) return null;
  const [, root, yymmdd, right, strikeRaw] = match;
  const year = 2000 + Number(yymmdd.slice(0, 2));
  const month = yymmdd.slice(2, 4);
  const day = yymmdd.slice(4, 6);
  return {
    ticker: normalized,
    root,
    expiration: `${year}-${month}-${day}`,
    right: right === 'C' ? 'CALL' : 'PUT',
    strike: Number(strikeRaw) / 1000,
  };
}

function opraRoot(ticker) {
  const normalized = String(ticker || '').trim().toUpperCase();
  if (!normalized.startsWith('O:')) return null;
  for (let index = 2; index < normalized.length; index += 1) {
    const code = normalized.charCodeAt(index);
    if (code >= 48 && code <= 57) return normalized.slice(2, index);
  }
  return null;
}

function daysBetween(startDate, endDate) {
  const start = Date.parse(`${startDate}T00:00:00.000Z`);
  const end = Date.parse(`${endDate}T00:00:00.000Z`);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  return Math.round((end - start) / 86_400_000);
}

module.exports = {
  parseOpraTicker,
  opraRoot,
  daysBetween,
};
