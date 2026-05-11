const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const { datasetCsvPath } = require('./config');
const { readGzipCsv, toNumber } = require('./csv');
const { parseOpraTicker } = require('./opra');
const { nsToMinuteMs, getEtParts } = require('./time');
const { solveImpliedVol, computeGreeks } = require('./greeks');

const MS_PER_YEAR = 365.25 * 24 * 60 * 60 * 1000;
const DEFAULT_RISK_FREE = 0.045;
const DEFAULT_DIV_YIELD = 0.013; // SPY
const REGULAR_OPEN_ET = 570; // 9:30 ET
const REGULAR_CLOSE_ET = 960; // 16:00 ET

// Cache for 16:00 ET-on-expiry-date ms.
const expirationCloseCache = new Map();

function expirationCloseMs(expirationIso) {
  if (expirationCloseCache.has(expirationIso)) return expirationCloseCache.get(expirationIso);
  // Try EDT (UTC-4 → 20:00Z) and EST (UTC-5 → 21:00Z); pick whichever maps to 16:00 ET on that date.
  const candidates = [
    Date.parse(`${expirationIso}T20:00:00.000Z`),
    Date.parse(`${expirationIso}T21:00:00.000Z`),
  ];
  let chosen = candidates[1];
  for (const ms of candidates) {
    const parts = getEtParts(ms);
    if (parts.dateEt === expirationIso && parts.hour === 16 && parts.minute === 0) {
      chosen = ms;
      break;
    }
  }
  expirationCloseCache.set(expirationIso, chosen);
  return chosen;
}

async function loadUnderlyingSeries(config, dayIso, symbol) {
  const dataset = symbol.startsWith('I:') ? config.datasets.indexBars : config.datasets.stockBars;
  const csvPath = datasetCsvPath({ ...config, datasets: config.datasets }, dataset, dayIso);
  if (!fs.existsSync(csvPath)) return new Map();
  const byMinute = new Map();
  await readGzipCsv(csvPath, async (row) => {
    if (row.ticker !== symbol) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null) return;
    const close = toNumber(row.close);
    if (close === null || !(close > 0)) return;
    byMinute.set(minuteMs, close);
  });
  return byMinute;
}

function forwardFillSpot(spotByMinute, minuteMs, maxLookbackMs = 5 * 60_000) {
  if (spotByMinute.has(minuteMs)) return spotByMinute.get(minuteMs);
  // Walk back at most maxLookbackMs in 1-minute steps.
  for (let step = 60_000; step <= maxLookbackMs; step += 60_000) {
    const v = spotByMinute.get(minuteMs - step);
    if (v !== undefined) return v;
  }
  return null;
}

function rootUnderlyingSymbol(root) {
  switch (root) {
    case 'SPY': return 'SPY';
    case 'SPX':
    case 'SPXW': return 'I:SPX';
    case 'VIX':
    case 'VIXW': return 'I:VIX';
    case 'QQQ': return 'QQQ';
    case 'IWM': return 'IWM';
    default: return root;
  }
}

async function buildGreeksForDay({
  config,
  dayIso,
  roots = ['SPY'],
  riskFree = DEFAULT_RISK_FREE,
  divYield = DEFAULT_DIV_YIELD,
  regularSessionOnly = true,
  outputPath,
  minPrice = 0.05,
  minVolume = 1,
}) {
  const optionPath = datasetCsvPath({ ...config, datasets: config.datasets }, config.datasets.optionBars, dayIso);
  if (!fs.existsSync(optionPath)) {
    throw new Error(`No option_quotes_1m for ${dayIso}: ${optionPath}`);
  }

  // Load underlyings for every requested root.
  const underlyingByRoot = new Map();
  const underlyingSymbols = new Set(roots.map(rootUnderlyingSymbol));
  for (const symbol of underlyingSymbols) {
    // eslint-disable-next-line no-await-in-loop
    const series = await loadUnderlyingSeries(config, dayIso, symbol);
    underlyingByRoot.set(symbol, series);
  }

  const rootSet = new Set(roots);
  const outDir = path.dirname(outputPath);
  fs.mkdirSync(outDir, { recursive: true });

  const gzipStream = zlib.createGzip();
  const fileStream = fs.createWriteStream(outputPath);
  gzipStream.pipe(fileStream);
  const fileDone = new Promise((resolve, reject) => {
    fileStream.on('finish', resolve);
    fileStream.on('error', reject);
    gzipStream.on('error', reject);
  });

  const stats = {
    rowsSeen: 0,
    rowsKept: 0,
    rowsSolved: 0,
    rowsNoIv: 0,
    rowsFilteredRoot: 0,
    rowsFilteredSession: 0,
    rowsNoSpot: 0,
    rowsLowLiquidity: 0,
  };

  function writeRow(obj) {
    if (!gzipStream.write(`${JSON.stringify(obj)}\n`)) {
      return new Promise((resolve) => gzipStream.once('drain', resolve));
    }
    return null;
  }

  await readGzipCsv(optionPath, async (row) => {
    stats.rowsSeen += 1;
    const parsed = parseOpraTicker(row.ticker);
    if (!parsed) return;
    if (!rootSet.has(parsed.root)) {
      stats.rowsFilteredRoot += 1;
      return;
    }
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null) return;
    const etParts = getEtParts(minuteMs);
    if (regularSessionOnly
      && (etParts.minuteOfDayEt < REGULAR_OPEN_ET || etParts.minuteOfDayEt >= REGULAR_CLOSE_ET)) {
      stats.rowsFilteredSession += 1;
      return;
    }
    const volume = toNumber(row.volume) || 0;
    const close = toNumber(row.close);
    if (close === null || close < minPrice || volume < minVolume) {
      stats.rowsLowLiquidity += 1;
      return;
    }
    const underlyingSymbol = rootUnderlyingSymbol(parsed.root);
    const spotSeries = underlyingByRoot.get(underlyingSymbol);
    const spot = spotSeries ? forwardFillSpot(spotSeries, minuteMs) : null;
    if (!(spot > 0)) {
      stats.rowsNoSpot += 1;
      return;
    }

    const expiryMs = expirationCloseMs(parsed.expiration);
    const T = Math.max(0, (expiryMs - minuteMs) / MS_PER_YEAR);
    const dteIntegerDays = Math.floor((expiryMs - minuteMs) / 86_400_000);

    let iv = null;
    let ivStatus = 'unsolved';
    let greeks = {
      delta: null, gamma: null, vega_annual: null, theta_annual: null,
      vanna: null, charm_annual: null, vomma: null,
    };

    if (T > 0.5 / (365.25 * 24 * 60)) {
      const solved = solveImpliedVol({
        targetPrice: close,
        spot,
        strike: parsed.strike,
        T,
        r: riskFree,
        q: divYield,
        right: parsed.right,
      });
      iv = solved.iv;
      ivStatus = solved.status;
      if (iv && iv > 0 && iv < 5) {
        greeks = computeGreeks({
          spot,
          strike: parsed.strike,
          T,
          sigma: iv,
          r: riskFree,
          q: divYield,
          right: parsed.right,
        });
        stats.rowsSolved += 1;
      } else {
        stats.rowsNoIv += 1;
      }
    } else {
      ivStatus = 'expired';
    }

    const moneynessPct = parsed.right === 'CALL'
      ? ((spot - parsed.strike) / spot) * 100
      : ((parsed.strike - spot) / spot) * 100;

    const outRow = {
      minute_ms: minuteMs,
      date_et: etParts.dateEt,
      minute_of_day_et: etParts.minuteOfDayEt,
      ticker: parsed.ticker,
      root: parsed.root,
      expiration: parsed.expiration,
      right: parsed.right,
      strike: parsed.strike,
      dte: dteIntegerDays,
      T,
      spot,
      price: close,
      volume,
      transactions: toNumber(row.transactions) || 0,
      iv,
      iv_status: ivStatus,
      delta: greeks.delta,
      gamma: greeks.gamma,
      vega_per_1pct: greeks.vega_annual !== null ? greeks.vega_annual / 100 : null,
      theta_per_day: greeks.theta_annual !== null ? greeks.theta_annual / 365.25 : null,
      vanna: greeks.vanna,
      charm_per_day: greeks.charm_annual !== null ? greeks.charm_annual / 365.25 : null,
      vomma: greeks.vomma,
      moneyness_pct: moneynessPct,
    };
    stats.rowsKept += 1;
    const back = writeRow(outRow);
    if (back) await back;
  });

  gzipStream.end();
  await fileDone;
  return stats;
}

function defaultOutputPath(projectRoot, root, dayIso) {
  return path.join(projectRoot, 'runtime', 'greeks-1m', root, `date=${dayIso}`, `${dayIso}.jsonl.gz`);
}

module.exports = {
  buildGreeksForDay,
  defaultOutputPath,
  rootUnderlyingSymbol,
  expirationCloseMs,
};
