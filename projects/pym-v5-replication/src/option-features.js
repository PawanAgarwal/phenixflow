const fs = require('node:fs');
const { spawn } = require('node:child_process');
const path = require('node:path');
const readline = require('node:readline');
const zlib = require('node:zlib');

const { closeMinuteEt } = require('./calendar');
const { ensureDir, runtimePath } = require('./config');
const { minuteEtFromNs } = require('./market-data');
const { daysBetween, opraRoot, parseOpraTicker } = require('./option-symbol');
const { collectTickers } = require('./symphony');

const OPTION_BAR_DATASET = 'option_quotes_1m';
const DEFAULT_EXTRA_OPTION_ROOTS = Object.freeze(['SPX', 'SPXW', 'VIX', 'VIXW']);
const DERIVED_METRICS = Object.freeze([
  'premiumImbalance',
  'volumeImbalance',
  'callPremiumShare',
  'putCallPremiumRatio',
  'nearDteVolumeShare',
  'zeroDteVolumeShare',
  'atmVolumeShare',
  'nearAtmPremiumShare',
  'shortDatedAtmFlowProxy',
  'totalPremiumLog',
]);

function optionBarsCsvPath(config, dayIso) {
  return path.join(
    config.roots.historical,
    config.datasets.optionBars || OPTION_BAR_DATASET,
    `date=${dayIso}`,
    `${dayIso}.csv.gz`,
  );
}

function optionBarsParquetPath(config, dayIso) {
  if (!config.roots.liveParquet) return null;
  return path.join(
    config.roots.liveParquet,
    config.datasets.optionBars || OPTION_BAR_DATASET,
    `date=${dayIso}`,
    `${dayIso}.live.parquet`,
  );
}

function resolveOptionBarsSource(config, dayIso) {
  const parquetPath = optionBarsParquetPath(config, dayIso);
  if (parquetPath && fs.existsSync(parquetPath)) {
    return {
      format: 'parquet',
      filePath: parquetPath,
      preferredFilePath: parquetPath,
    };
  }
  const csvPath = optionBarsCsvPath(config, dayIso);
  if (fs.existsSync(csvPath)) {
    return {
      format: 'csv.gz',
      filePath: csvPath,
      preferredFilePath: parquetPath || csvPath,
    };
  }
  return {
    format: 'missing',
    filePath: parquetPath || csvPath,
    preferredFilePath: parquetPath || csvPath,
    fallbackFilePath: csvPath,
  };
}

function latestOptionBarsDate(config) {
  const dataset = config.datasets.optionBars || OPTION_BAR_DATASET;
  const roots = [config.roots.historical, config.roots.liveParquet].filter(Boolean);
  const dates = roots.flatMap((root) => {
    const datasetRoot = path.join(root, dataset);
    if (!fs.existsSync(datasetRoot)) return [];
    return fs.readdirSync(datasetRoot)
      .filter((entry) => entry.startsWith('date='))
      .map((entry) => entry.slice('date='.length));
  });
  return dates.length ? dates.sort().at(-1) : null;
}

function safeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function safeRatio(numerator, denominator) {
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) return 0;
  return numerator / denominator;
}

function emptyRootFeature(root) {
  return {
    root,
    contractBars: 0,
    transactions: 0,
    totalVolume: 0,
    totalPremium: 0,
    callVolume: 0,
    putVolume: 0,
    callPremium: 0,
    putPremium: 0,
    nearDteVolume: 0,
    nearDtePremium: 0,
    zeroDteVolume: 0,
    zeroDtePremium: 0,
    oneDteVolume: 0,
    oneDtePremium: 0,
    twoToSevenDteVolume: 0,
    twoToSevenDtePremium: 0,
    eightToThirtyDteVolume: 0,
    eightToThirtyDtePremium: 0,
    atmVolume: 0,
    atmPremium: 0,
    nearAtmVolume: 0,
    nearAtmPremium: 0,
    nearAtmCallPremium: 0,
    nearAtmPutPremium: 0,
    otmCallVolume: 0,
    otmPutVolume: 0,
    itmCallVolume: 0,
    itmPutVolume: 0,
  };
}

function moneynessBucket(parsed, underlyingClose) {
  if (!parsed || !(underlyingClose > 0)) return null;
  const ratio = parsed.strike / underlyingClose;
  if (Math.abs(ratio - 1) <= 0.01) return 'atm';
  if (parsed.right === 'CALL') return ratio > 1 ? 'otm_call' : 'itm_call';
  return ratio < 1 ? 'otm_put' : 'itm_put';
}

function addDteBuckets(feature, dte, volume, premium) {
  if (!Number.isFinite(dte) || dte < 0) return;
  if (dte <= 7) {
    feature.nearDteVolume += volume;
    feature.nearDtePremium += premium;
  }
  if (dte === 0) {
    feature.zeroDteVolume += volume;
    feature.zeroDtePremium += premium;
  } else if (dte === 1) {
    feature.oneDteVolume += volume;
    feature.oneDtePremium += premium;
  } else if (dte >= 2 && dte <= 7) {
    feature.twoToSevenDteVolume += volume;
    feature.twoToSevenDtePremium += premium;
  } else if (dte >= 8 && dte <= 30) {
    feature.eightToThirtyDteVolume += volume;
    feature.eightToThirtyDtePremium += premium;
  }
}

function addMoneynessBuckets(feature, parsed, bucket, dte, volume, premium) {
  if (bucket === 'atm') {
    feature.atmVolume += volume;
    feature.atmPremium += premium;
    if (Number.isFinite(dte) && dte >= 0 && dte <= 7) {
      feature.nearAtmVolume += volume;
      feature.nearAtmPremium += premium;
      if (parsed.right === 'CALL') feature.nearAtmCallPremium += premium;
      else feature.nearAtmPutPremium += premium;
    }
  } else if (bucket === 'otm_call') {
    feature.otmCallVolume += volume;
  } else if (bucket === 'otm_put') {
    feature.otmPutVolume += volume;
  } else if (bucket === 'itm_call') {
    feature.itmCallVolume += volume;
  } else if (bucket === 'itm_put') {
    feature.itmPutVolume += volume;
  }
}

function optionBarPrice(values) {
  const open = safeNumber(values[2]);
  const close = safeNumber(values[3]);
  const high = safeNumber(values[4]);
  const low = safeNumber(values[5]);
  const finite = [open, high, low, close].filter((value) => Number.isFinite(value) && value > 0);
  if (!finite.length) return null;
  return finite.reduce((sum, value) => sum + value, 0) / finite.length;
}

function parseOptionBarLine(line) {
  const values = String(line || '').split(',');
  if (values.length < 8) return null;
  const ticker = values[0];
  return {
    ticker,
    volume: safeNumber(values[1]) || 0,
    price: optionBarPrice(values),
    windowStart: values[6],
    transactions: safeNumber(values[7]) || 0,
  };
}

function duckdbString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function optionParquetSql(filePath) {
  return `COPY (
    SELECT
      ticker,
      volume,
      open,
      close,
      high,
      low,
      window_start,
      COALESCE(CAST(transactions AS VARCHAR), '0') AS transactions
    FROM read_parquet(${duckdbString(filePath)})
  ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`;
}

async function streamParquetOptionBarLines(filePath, onLine) {
  const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', optionParquetSql(filePath)], {
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  const stderr = [];
  child.stderr.on('data', (chunk) => {
    stderr.push(String(chunk));
  });

  const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  let headerSeen = false;
  for await (const line of reader) {
    if (!headerSeen) {
      headerSeen = true;
      continue;
    }
    if (line) onLine(line);
  }

  const code = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', resolve);
  });
  if (code !== 0) {
    throw new Error(`duckdb_parquet_read_failed:${filePath}:${stderr.join('').trim() || code}`);
  }
}

function addOptionBarFeature(featuresByRoot, dayIso, row, underlyingCloses, selectedRoots = null) {
  const parsed = parseOpraTicker(row.ticker);
  if (selectedRoots && !selectedRoots.has(parsed?.root)) return false;
  if (!parsed || !Number.isFinite(row.price) || row.price <= 0 || row.volume <= 0) return false;
  let feature = featuresByRoot.get(parsed.root);
  if (!feature) {
    feature = emptyRootFeature(parsed.root);
    featuresByRoot.set(parsed.root, feature);
  }
  const premium = row.volume * row.price * 100;
  const dte = daysBetween(dayIso, parsed.expiration);
  const bucket = moneynessBucket(parsed, underlyingCloses.get(parsed.root));

  feature.contractBars += 1;
  feature.transactions += row.transactions || 0;
  feature.totalVolume += row.volume;
  feature.totalPremium += premium;
  if (parsed.right === 'CALL') {
    feature.callVolume += row.volume;
    feature.callPremium += premium;
  } else {
    feature.putVolume += row.volume;
    feature.putPremium += premium;
  }
  addDteBuckets(feature, dte, row.volume, premium);
  addMoneynessBuckets(feature, parsed, bucket, dte, row.volume, premium);
  return true;
}

function finalizeRootFeature(feature) {
  return {
    ...feature,
    premiumImbalance: safeRatio(feature.callPremium - feature.putPremium, feature.callPremium + feature.putPremium),
    volumeImbalance: safeRatio(feature.callVolume - feature.putVolume, feature.callVolume + feature.putVolume),
    callPremiumShare: safeRatio(feature.callPremium, feature.totalPremium),
    putCallPremiumRatio: safeRatio(feature.putPremium, feature.callPremium),
    nearDteVolumeShare: safeRatio(feature.nearDteVolume, feature.totalVolume),
    zeroDteVolumeShare: safeRatio(feature.zeroDteVolume, feature.totalVolume),
    atmVolumeShare: safeRatio(feature.atmVolume, feature.totalVolume),
    nearAtmPremiumShare: safeRatio(feature.nearAtmPremium, feature.totalPremium),
    nearAtmPremiumImbalance: safeRatio(
      feature.nearAtmCallPremium - feature.nearAtmPutPremium,
      feature.nearAtmCallPremium + feature.nearAtmPutPremium,
    ),
    shortDatedAtmFlowProxy: safeRatio(feature.nearAtmPremium, feature.totalPremium) * Math.log1p(feature.nearAtmVolume),
    totalPremiumLog: Math.log1p(feature.totalPremium),
  };
}

function defaultOptionRoots(score, market) {
  const roots = new Set([...collectTickers(score), 'SPY', 'QQQ']);
  DEFAULT_EXTRA_OPTION_ROOTS.forEach((root) => roots.add(root));
  (market?.tickers || []).forEach((ticker) => {
    if (roots.has(ticker)) roots.add(ticker);
  });
  return [...roots].sort();
}

function underlyingClosesForDate(market, date) {
  const dateIndex = market.dates.indexOf(date);
  const closes = new Map();
  if (dateIndex < 0) return closes;
  market.tickers.forEach((ticker) => {
    const close = market.closes.get(ticker)?.[dateIndex];
    if (Number.isFinite(close) && close > 0) closes.set(ticker, close);
  });
  return closes;
}

async function readOptionFeaturesForDay({ config, day, market, roots }) {
  const source = resolveOptionBarsSource(config, day.date);
  const selectedRoots = new Set(roots.map((root) => root.toUpperCase()));
  const closeMinute = closeMinuteEt(day);
  const featuresByRoot = new Map();
  const underlyingCloses = underlyingClosesForDate(market, day.date);
  let rowsRead = 0;
  let rowsMatchedRoot = 0;
  let rowsUsed = 0;

  if (source.format === 'missing') {
    return {
      date: day.date,
      filePath: source.filePath,
      sourceFormat: source.format,
      missingFile: true,
      rowsRead,
      rowsMatchedRoot,
      rowsUsed,
      roots: {},
    };
  }

  function processOptionBarLine(line) {
    rowsRead += 1;
    const comma = line.indexOf(',');
    if (comma < 0) return;
    const ticker = line.slice(0, comma);
    const root = opraRoot(ticker);
    if (!root || !selectedRoots.has(root)) return;
    rowsMatchedRoot += 1;
    const row = parseOptionBarLine(line);
    if (!row) return;
    const minuteEt = minuteEtFromNs(row.windowStart);
    if (minuteEt === null || minuteEt < 570 || minuteEt >= closeMinute) return;
    if (addOptionBarFeature(featuresByRoot, day.date, row, underlyingCloses, selectedRoots)) rowsUsed += 1;
  }

  if (source.format === 'parquet') {
    await streamParquetOptionBarLines(source.filePath, processOptionBarLine);
  } else {
    const stream = fs.createReadStream(source.filePath).pipe(zlib.createGunzip());
    const reader = readline.createInterface({ input: stream, crlfDelay: Infinity });
    let headerSeen = false;
    for await (const line of reader) {
      if (!headerSeen) {
        headerSeen = true;
        continue;
      }
      if (line) processOptionBarLine(line);
    }
  }

  const rootEntries = [...featuresByRoot.entries()]
    .map(([root, feature]) => [root, finalizeRootFeature(feature)])
    .sort(([left], [right]) => left.localeCompare(right));
  return {
    date: day.date,
    filePath: source.filePath,
    sourceFormat: source.format,
    missingFile: false,
    rowsRead,
    rowsMatchedRoot,
    rowsUsed,
    roots: Object.fromEntries(rootEntries),
  };
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function withRollingOptionStats(rows, { window = 20 } = {}) {
  const sorted = rows.slice().sort((left, right) => left.date.localeCompare(right.date));
  const roots = new Set();
  sorted.forEach((row) => Object.keys(row.roots || {}).forEach((root) => roots.add(root)));
  const history = new Map([...roots].map((root) => [root, []]));

  return sorted.map((row) => {
    const nextRoots = {};
    [...roots].forEach((root) => {
      const feature = row.roots?.[root] || finalizeRootFeature(emptyRootFeature(root));
      const prior = history.get(root).slice(-window);
      const rolling = {};
      DERIVED_METRICS.forEach((metric) => {
        const values = prior.map((item) => item[metric]).filter(Number.isFinite);
        const avg = mean(values);
        const sd = standardDeviation(values);
        rolling[`${metric}Mean${window}`] = values.length ? avg : null;
        rolling[`${metric}Z${window}`] = values.length >= 5 && sd > 0 ? (feature[metric] - avg) / sd : 0;
      });
      const priorFive = history.get(root).slice(-5);
      const priorCallPremium = mean(priorFive.map((item) => item.callPremium).filter(Number.isFinite));
      const priorPutPremium = mean(priorFive.map((item) => item.putPremium).filter(Number.isFinite));
      rolling.callPremiumMomentum5 = priorCallPremium > 0 ? (feature.callPremium / priorCallPremium) - 1 : 0;
      rolling.putPremiumMomentum5 = priorPutPremium > 0 ? (feature.putPremium / priorPutPremium) - 1 : 0;
      nextRoots[root] = { ...feature, rolling };
      history.get(root).push(feature);
    });
    return { ...row, roots: nextRoots };
  });
}

async function buildOptionFeatureFile({ config, market, score, days, startDate, endDate, roots, outputPath, onProgress }) {
  const selectedRoots = roots?.length ? roots.map((root) => root.toUpperCase()).sort() : defaultOptionRoots(score, market);
  const outPath = outputPath || runtimePath(`pym-v5-option-bar-features-${startDate}-${endDate}.jsonl`);
  const manifestPath = outPath.replace(/\.jsonl$/, '.manifest.json');
  ensureDir(path.dirname(outPath));
  const writer = fs.createWriteStream(outPath, { encoding: 'utf8' });
  const coverage = [];
  let processedDays = 0;
  let totalRowsRead = 0;
  let totalRowsUsed = 0;

  for (const day of days) {
    const result = await readOptionFeaturesForDay({ config, day, market, roots: selectedRoots });
    writer.write(`${JSON.stringify(result)}\n`);
    coverage.push({
      date: day.date,
      missingFile: result.missingFile,
      sourceFormat: result.sourceFormat,
      filePath: result.filePath,
      rowsRead: result.rowsRead,
      rowsMatchedRoot: result.rowsMatchedRoot,
      rowsUsed: result.rowsUsed,
      activeRoots: Object.keys(result.roots).length,
    });
    processedDays += 1;
    totalRowsRead += result.rowsRead;
    totalRowsUsed += result.rowsUsed;
    if (onProgress) onProgress({ day, processedDays, result });
  }

  await new Promise((resolve, reject) => {
    writer.end(resolve);
    writer.on('error', reject);
  });

  const manifest = {
    generatedAt: new Date().toISOString(),
    source: {
      provider: 'Massive',
      dataset: config.datasets.optionBars || OPTION_BAR_DATASET,
      sourcePreference: 'live parquet when available, historical csv.gz fallback',
      liveParquetRoot: config.roots.liveParquet || null,
      historicalRoot: config.roots.historical,
      note: 'Historical option Greeks/open interest were not present in the local Massive cache; gamma-style features are short-dated ATM option-flow proxies derived from aggregate option bars.',
    },
    startDate,
    endDate,
    outputPath: outPath,
    selectedRoots,
    processedDays,
    totalRowsRead,
    totalRowsUsed,
    missingFileDays: coverage.filter((item) => item.missingFile).map((item) => item.date),
    coverage,
  };
  fs.writeFileSync(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');
  return { outputPath: outPath, manifestPath, manifest };
}

function readOptionFeatureJsonl(filePath, { rollingWindow = 20 } = {}) {
  const rows = fs.readFileSync(filePath, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  return withRollingOptionStats(rows, { window: rollingWindow });
}

module.exports = {
  DERIVED_METRICS,
  DEFAULT_EXTRA_OPTION_ROOTS,
  addOptionBarFeature,
  buildOptionFeatureFile,
  defaultOptionRoots,
  emptyRootFeature,
  finalizeRootFeature,
  latestOptionBarsDate,
  moneynessBucket,
  optionBarsCsvPath,
  optionBarsParquetPath,
  parseOptionBarLine,
  readOptionFeatureJsonl,
  readOptionFeaturesForDay,
  resolveOptionBarsSource,
  safeRatio,
  withRollingOptionStats,
};
