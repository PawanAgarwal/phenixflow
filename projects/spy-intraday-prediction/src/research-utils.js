// Shared research utilities for the post-OCC/VIX backlog (S1, S2, S3, A1, A2, A3, B1, B2, B3).
//
// Provides:
//   - OCC P/C z-score loader (reuses pattern from occ-pc-contrarian)
//   - VIX tenor loader (reuses pattern from vix-term-structure)
//   - SPY minute-bar loader (with historical CSV + live parquet fallback via DuckDB)
//   - Helper to compute rolling z-scores
//   - PYM bias loader (in-memory recompute, same approach as build-pym-gated-artifacts)
//   - Standardized backtest harness for signal -> next-day SPY/SPXL/TQQQ trade
//   - Walk-forward report formatter
//
// All time conversions go through getEtParts so EDT/EST DST is handled automatically.

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');
const { spawn } = require('node:child_process');

const { PROJECT_ROOT, loadConfig, resolveDatasetSource } = require('./config');
const { loadStockBars, buildOccOverlay, defaultFeaturesPath } = require('./build-features-1m');
const { readGzipCsv, toNumber } = require('./csv');
const { getEtParts } = require('./time');
const { pymBias } = require('./pym-bias-strategy');

const DEFAULT_OCC_ROOT = '/Volumes/SEC4TB/massive-data/occ/option_open_interest_eod';

let cachedConfig = null;
function getConfig() {
  if (!cachedConfig) cachedConfig = loadConfig();
  return cachedConfig;
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

function duckdbString(value) { return `'${String(value).replace(/'/g, "''")}'`; }

async function streamParquetRows(filePath, columns, onRow) {
  const sql = `COPY (SELECT ${columns.join(', ')} FROM read_parquet(${duckdbString(filePath)})) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`;
  const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', sql], { stdio: ['ignore', 'pipe', 'pipe'] });
  const stderr = [];
  child.stderr.on('data', (chunk) => stderr.push(String(chunk)));
  const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
  let headers = null;
  for await (const line of reader) {
    if (!line) continue;
    if (!headers) { headers = String(line).split(','); continue; }
    const values = String(line).split(',');
    const row = {};
    headers.forEach((header, index) => { row[header] = values[index] ?? ''; });
    await onRow(row);
  }
  const code = await new Promise((resolve, reject) => { child.on('error', reject); child.on('close', resolve); });
  if (code !== 0) throw new Error(`duckdb_parquet_read_failed:${filePath}:${stderr.join('').trim() || code}`);
}

// ------------------------- Trading-day enumeration --------------------------

function stockTradingDaysInRange(startDate, endDate) {
  const config = getConfig();
  const dates = new Set();
  for (const root of [config.roots.historical, config.roots.liveParquet].filter(Boolean)) {
    const datasetRoot = path.join(root, config.datasets.stockBars);
    if (!fs.existsSync(datasetRoot)) continue;
    for (const entry of fs.readdirSync(datasetRoot)) {
      if (!entry.startsWith('date=')) continue;
      const day = entry.slice('date='.length);
      if (day >= startDate && day <= endDate && !isWeekend(day)) dates.add(day);
    }
  }
  return [...dates].sort();
}

// ----------------------------- OCC P/C signal -------------------------------

const occCache = new Map(); // key = `${start}|${end}` → Map<day, {ratio, z}>
async function loadOccZScoreSeries(startDate, endDate) {
  // Returns Map<day, { ratio, z }>. Use buildOccOverlay (rolling 20-day z).
  const cacheKey = `${startDate}|${endDate}`;
  if (occCache.has(cacheKey)) return occCache.get(cacheKey);
  const occRoot = process.env.OCC_EOD_ROOT || DEFAULT_OCC_ROOT;
  if (!fs.existsSync(occRoot)) { occCache.set(cacheKey, new Map()); return occCache.get(cacheKey); }
  const days = stockTradingDaysInRange(startDate, endDate);
  const overlay = await buildOccOverlay({ occRoot, dates: days });
  const out = new Map();
  for (const day of days) {
    const r = overlay.get(day);
    if (!r) continue;
    if (Number.isFinite(r.equity_pc_ratio) && Number.isFinite(r.equity_pc_ratio_z)) {
      out.set(day, { ratio: r.equity_pc_ratio, z: r.equity_pc_ratio_z });
    }
  }
  occCache.set(cacheKey, out);
  return out;
}

// Load OCC series once for the widest known range; sub-windows filter via filterMapByRange.
let occWidePromise = null;
async function loadOccZScoreSeriesWide() {
  if (!occWidePromise) {
    occWidePromise = loadOccZScoreSeries('2024-11-01', SENSITIVITY_WINDOWS.full.endDate);
  }
  return occWidePromise;
}

// --------------------------- VIX-tenor signal ------------------------------

const VIX_PARQUET_ROOT = '/Volumes/SEC4TB/massive-data/massive/indices_1m_parquet';
const VIX_SYMBOLS_PARQUET = ['VIX', 'VIX1D', 'VIX9D', 'VIX3M', 'VIX1Y', 'VVIX'];

// Returns Map<day, { 'I:VIX': close, ... }> for parquet-covered days.
async function bulkLoadVixFromParquet(startDate, endDate) {
  const out = new Map();
  if (!fs.existsSync(VIX_PARQUET_ROOT)) return out;
  const symbolList = VIX_SYMBOLS_PARQUET.map((s) => `'${s}'`).join(',');
  const glob = `${VIX_PARQUET_ROOT}/trade_date_utc=*/shard=*/part-*.parquet`;
  const sql = `SELECT trade_date_utc::VARCHAR AS day, symbol, ARG_MAX(close, minute_bucket_utc) AS close
               FROM read_parquet(${duckdbString(glob)}, hive_partitioning=true)
               WHERE symbol IN (${symbolList})
               AND trade_date_utc BETWEEN DATE ${duckdbString(startDate)} AND DATE ${duckdbString(endDate)}
               GROUP BY trade_date_utc, symbol`;
  await new Promise((resolve, reject) => {
    const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', `COPY (${sql}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`], { stdio: ['ignore', 'pipe', 'pipe'] });
    const stderr = [];
    child.stderr.on('data', (chunk) => stderr.push(String(chunk)));
    const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
    let isHeader = true;
    reader.on('line', (line) => {
      if (!line) return;
      if (isHeader) { isHeader = false; return; }
      const parts = String(line).split(',');
      const day = parts[0]; const symbol = parts[1]; const close = Number(parts[2]);
      if (!day || !symbol || !Number.isFinite(close)) return;
      if (!out.has(day)) out.set(day, {});
      out.get(day)[`I:${symbol}`] = close;
    });
    reader.on('close', () => resolve());
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) reject(new Error(`bulk_vix_parquet_failed:${stderr.join('').trim() || code}`));
    });
  });
  return out;
}

async function loadVixTenorsForDay(day) {
  const config = getConfig();
  const source = resolveDatasetSource(config, config.datasets.indexBars, day);
  if (source.format === 'missing') return null;
  const wanted = new Set(['I:VIX', 'I:VIX1D', 'I:VIX9D', 'I:VIX3M', 'I:VIX1Y', 'I:VVIX']);
  const latest = {};
  async function onRow(row) {
    const ticker = String(row.ticker || '').trim();
    if (!wanted.has(ticker)) return;
    const close = toNumber(row.close);
    if (!Number.isFinite(close)) return;
    const ws = toNumber(row.window_start);
    if (!latest[ticker] || (ws && ws > (latest[ticker].ws || 0))) latest[ticker] = { close, ws };
  }
  if (source.format === 'csv.gz') {
    await readGzipCsv(source.filePath, onRow);
  } else if (source.format === 'parquet') {
    await streamParquetRows(source.filePath, ['ticker', 'open', 'close', 'window_start'], onRow);
  }
  const out = {};
  for (const [t, v] of Object.entries(latest)) out[t] = v.close;
  return out;
}

function rollingZ(values, lookback) {
  const out = new Array(values.length).fill(null);
  for (let i = lookback; i < values.length; i += 1) {
    const window = values.slice(Math.max(0, i - lookback), i);
    const valid = window.filter((v) => Number.isFinite(v));
    if (valid.length < Math.min(5, lookback)) continue;
    const m = valid.reduce((a, b) => a + b, 0) / valid.length;
    const sd = Math.sqrt(valid.reduce((a, b) => a + ((b - m) ** 2), 0) / valid.length);
    if (sd === 0 || !Number.isFinite(values[i])) continue;
    out[i] = (values[i] - m) / sd;
  }
  return out;
}

const vixCache = new Map(); // key = `${start}|${end}|${lookback}`
async function loadVixTermZSeries(startDate, endDate, lookback = 20) {
  const cacheKey = `${startDate}|${endDate}|${lookback}`;
  if (vixCache.has(cacheKey)) return vixCache.get(cacheKey);
  // Returns Map<day, { vix1d, vix, vix3m, vvix, ratio_1d_3m, z_1d_3m, ratio_vix_3m, z_vix_3m, z_vvix }>
  const days = stockTradingDaysInRange(startDate, endDate);
  // Fast path: bulk read all parquet-covered days in one DuckDB query.
  const bulk = await bulkLoadVixFromParquet(startDate, endDate);
  const records = [];
  for (const day of days) {
    let v = bulk.get(day);
    if (!v) {
      // Fall back to per-day CSV.GZ scan for days outside parquet coverage.
      // eslint-disable-next-line no-await-in-loop
      v = await loadVixTenorsForDay(day);
    }
    if (!v) continue;
    records.push({
      day,
      vix1d: v['I:VIX1D'],
      vix: v['I:VIX'],
      vix3m: v['I:VIX3M'],
      vvix: v['I:VVIX'],
      ratio_1d_3m: (v['I:VIX1D'] && v['I:VIX3M']) ? v['I:VIX1D'] / v['I:VIX3M'] : null,
      ratio_vix_3m: (v['I:VIX'] && v['I:VIX3M']) ? v['I:VIX'] / v['I:VIX3M'] : null,
    });
  }
  const z_1d_3m = rollingZ(records.map((r) => r.ratio_1d_3m), lookback);
  const z_vix_3m = rollingZ(records.map((r) => r.ratio_vix_3m), lookback);
  const z_vvix = rollingZ(records.map((r) => r.vvix), lookback);
  const out = new Map();
  for (let i = 0; i < records.length; i += 1) {
    out.set(records[i].day, {
      ...records[i],
      z_1d_3m: z_1d_3m[i],
      z_vix_3m: z_vix_3m[i],
      z_vvix: z_vvix[i],
    });
  }
  vixCache.set(cacheKey, out);
  return out;
}

const vixWidePromises = new Map();
async function loadVixTermZSeriesWide(lookback = 20, endDate = SENSITIVITY_WINDOWS.full.endDate) {
  const cacheKey = `${lookback}|${endDate}`;
  if (!vixWidePromises.has(cacheKey)) {
    vixWidePromises.set(cacheKey, loadVixTermZSeries('2024-11-01', endDate, lookback));
  }
  return vixWidePromises.get(cacheKey);
}

function filterMapByRange(map, startDate, endDate) {
  const out = new Map();
  for (const [day, value] of map.entries()) {
    if (day >= startDate && day <= endDate) out.set(day, value);
  }
  return out;
}

// --------------------------- PYM bias signal ------------------------------

let cachedPymByDate = null;
async function loadPymBiasByDay() {
  if (cachedPymByDate) return cachedPymByDate;
  const PYM_ROOT = path.resolve(__dirname, '..', '..', 'pym-v5-replication');
  const pymConfig = require(path.join(PYM_ROOT, 'src', 'config')).loadConfig();
  const { readDailyBarsJsonl } = require(path.join(PYM_ROOT, 'src', 'backtest'));
  const { loadMassiveEnv } = require(path.join(PYM_ROOT, 'src', 'env'));
  const {
    buildDailyRebalanceReport,
    defaultScorePath,
    findLatestMassiveEodBarsPath,
  } = require(path.join(PYM_ROOT, 'src', 'rebalance-report'));
  loadMassiveEnv();
  const barsPath = process.env.PYM_V5_DAILY_BARS_PATH || findLatestMassiveEodBarsPath();
  const scorePath = process.env.PYM_V5_SCORE_PATH || defaultScorePath(pymConfig);
  if (!barsPath || !fs.existsSync(barsPath)) return new Map();
  if (!scorePath || !fs.existsSync(scorePath)) return new Map();
  const score = JSON.parse(fs.readFileSync(scorePath, 'utf8'));
  const market = readDailyBarsJsonl(barsPath);
  const report = buildDailyRebalanceReport({
    market, score,
    startDate: '2024-12-01', rsiMode: 'wilder',
    initialCapital: pymConfig.execution?.initialCapital ?? 10_000,
    transactionCostBps: pymConfig.execution?.transactionCostBps ?? 1,
    slippageBps: pymConfig.execution?.slippageBps ?? 1,
  });
  const byDate = new Map();
  for (const snap of report.snapshots || []) {
    if (!snap.nextDate) continue;
    const holdingsMap = {};
    for (const h of snap.holdings || []) {
      if (h && h.ticker && Number.isFinite(h.weight)) holdingsMap[h.ticker] = h.weight;
    }
    byDate.set(snap.nextDate, { bias: pymBias(holdingsMap), holdings: holdingsMap });
  }
  cachedPymByDate = byDate;
  return byDate;
}

// --------------------------- Daily bars (multi-ticker) -------------------

const PYM_DAILY_BARS_PATH = '/Users/pawanagarwal/github/phenixflow/projects/pym-v5-replication/runtime/pym-v5-massive-eod-adjusted-daily-bars-2024-01-01-2026-05-11.jsonl';

let dailyBarsAll = null; // Map<ticker, Map<date, {open, high, low, close, volume}>>
async function loadAllDailyBars() {
  if (dailyBarsAll) return dailyBarsAll;
  if (!fs.existsSync(PYM_DAILY_BARS_PATH)) { dailyBarsAll = new Map(); return dailyBarsAll; }
  const map = new Map();
  const reader = readline.createInterface({ input: fs.createReadStream(PYM_DAILY_BARS_PATH), crlfDelay: Infinity });
  for await (const line of reader) {
    if (!line) continue;
    let obj;
    try { obj = JSON.parse(line); } catch { continue; }
    const t = obj.ticker; const d = obj.date;
    if (!t || !d) continue;
    if (!map.has(t)) map.set(t, new Map());
    map.get(t).set(d, { open: obj.open, high: obj.high, low: obj.low, close: obj.close, volume: obj.volume });
  }
  dailyBarsAll = map;
  return map;
}

async function loadDailyBars(ticker, startDate, endDate) {
  const all = await loadAllDailyBars();
  const bars = all.get(ticker);
  if (bars) {
    const out = new Map();
    for (const [d, b] of bars.entries()) {
      if (d >= startDate && d <= endDate) out.set(d, b);
    }
    return out;
  }
  // Fallback: bulk DuckDB read from stock_quotes_1m for tickers outside PYM universe.
  return loadDailyBarsFromStockQuotes(ticker, startDate, endDate);
}

const stockQuotesCache = new Map(); // key = `${ticker}|${start}|${end}`
async function loadDailyBarsFromStockQuotes(ticker, startDate, endDate) {
  const cacheKey = `${ticker}|${startDate}|${endDate}`;
  if (stockQuotesCache.has(cacheKey)) return stockQuotesCache.get(cacheKey);
  const out = new Map();
  const root = '/Volumes/SEC4TB/massive-data/massive/stock_quotes_1m';
  if (!fs.existsSync(root)) { stockQuotesCache.set(cacheKey, out); return out; }
  const glob = `${root}/date=*/*.csv.gz`;
  const sql = `SELECT regexp_extract(filename, 'date=([0-9-]+)', 1) AS day,
                       ARG_MIN(open, window_start) AS open,
                       MAX(high) AS high,
                       MIN(low) AS low,
                       ARG_MAX(close, window_start) AS close,
                       SUM(volume) AS volume
                FROM read_csv_auto(${duckdbString(glob)}, filename=true)
                WHERE ticker = ${duckdbString(ticker)}
                GROUP BY day
                HAVING day >= ${duckdbString(startDate)} AND day <= ${duckdbString(endDate)}`;
  await new Promise((resolve, reject) => {
    const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', `COPY (${sql}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`], { stdio: ['ignore', 'pipe', 'pipe'] });
    const stderr = [];
    child.stderr.on('data', (chunk) => stderr.push(String(chunk)));
    const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
    let isHeader = true;
    reader.on('line', (line) => {
      if (!line) return;
      if (isHeader) { isHeader = false; return; }
      const [day, open, high, low, close, volume] = String(line).split(',').map((s, idx) => idx === 0 ? s : Number(s));
      if (!day) return;
      out.set(day, { open, high, low, close, volume });
    });
    reader.on('close', () => resolve());
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) reject(new Error(`bulk_daily_csv_failed:${stderr.join('').trim() || code}`));
    });
  });
  stockQuotesCache.set(cacheKey, out);
  return out;
}

// --------------------------- SPY minute bars ------------------------------

const minuteBarCache = new Map();

// Bulk-prefetch SPY 1m bars for a date range via DuckDB.  Caches results in
// minuteBarCache so subsequent loadSpyMinuteBars(day) calls are O(1).
let spyBulkPromise = null;
async function prefetchSpyMinuteBarsBulk(startDate = '2024-11-01', endDate = SENSITIVITY_WINDOWS.full.endDate) {
  if (spyBulkPromise) return spyBulkPromise;
  spyBulkPromise = (async () => {
    const root = '/Volumes/SEC4TB/massive-data/massive/stock_quotes_1m';
    if (!fs.existsSync(root)) return;
    const glob = `${root}/date=*/*.csv.gz`;
    const sql = `SELECT regexp_extract(filename, 'date=([0-9-]+)', 1) AS day,
                        window_start, open, high, low, close, volume
                 FROM read_csv_auto(${duckdbString(glob)}, filename=true)
                 WHERE ticker = 'SPY'
                 AND regexp_extract(filename, 'date=([0-9-]+)', 1) BETWEEN ${duckdbString(startDate)} AND ${duckdbString(endDate)}`;
    const byDay = new Map();
    await new Promise((resolve, reject) => {
      const child = spawn(process.env.DUCKDB_BIN || 'duckdb', ['-c', `COPY (${sql}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`], { stdio: ['ignore', 'pipe', 'pipe'] });
      const stderr = [];
      child.stderr.on('data', (chunk) => stderr.push(String(chunk)));
      const reader = readline.createInterface({ input: child.stdout, crlfDelay: Infinity });
      let isHeader = true;
      reader.on('line', (line) => {
        if (!line) return;
        if (isHeader) { isHeader = false; return; }
        const parts = String(line).split(',');
        const day = parts[0];
        const ws = Number(parts[1]);
        if (!day || !Number.isFinite(ws)) return;
        const minuteMs = Math.floor(ws / 1_000_000); // ns → ms
        const et = getEtParts(minuteMs);
        if (et.dateEt !== day) return;
        if (et.minuteOfDayEt < 570 || et.minuteOfDayEt >= 960) return;
        const row = {
          minuteMs,
          open: Number(parts[2]),
          high: Number(parts[3]),
          low: Number(parts[4]),
          close: Number(parts[5]),
          volume: Number(parts[6]),
          minute_of_day_et: et.minuteOfDayEt,
        };
        if (!byDay.has(day)) byDay.set(day, []);
        byDay.get(day).push(row);
      });
      reader.on('close', () => resolve());
      child.on('error', reject);
      child.on('close', (code) => {
        if (code !== 0) reject(new Error(`bulk_spy_failed:${stderr.join('').trim() || code}`));
      });
    });
    for (const [day, rows] of byDay.entries()) {
      rows.sort((a, b) => a.minuteMs - b.minuteMs);
      minuteBarCache.set(day, rows);
    }
  })();
  return spyBulkPromise;
}

async function loadSpyMinuteBars(day) {
  if (minuteBarCache.has(day)) return minuteBarCache.get(day);
  const universe = getConfig();
  const byMinute = await loadStockBars(universe, day, 'SPY');
  if (byMinute.size === 0) { minuteBarCache.set(day, null); return null; }
  const rows = [];
  for (const [minuteMs, bar] of byMinute.entries()) {
    const et = getEtParts(minuteMs);
    if (et.dateEt !== day) continue;
    if (et.minuteOfDayEt < 570 || et.minuteOfDayEt >= 960) continue;
    rows.push({ minuteMs, ...bar, minute_of_day_et: et.minuteOfDayEt });
  }
  rows.sort((a, b) => a.minuteMs - b.minuteMs);
  minuteBarCache.set(day, rows);
  return rows;
}

function getSpyBarAtMinute(rows, minuteEt) {
  return rows?.find((r) => r.minute_of_day_et === minuteEt) || null;
}

// --------------------------- Trade execution ------------------------------

async function executeTrade({ side, leverage, signalDay, entryDay, exitDay, entryMinuteEt, exitMinuteEt, costBpsRoundTrip = 2 }) {
  const entryRows = await loadSpyMinuteBars(entryDay);
  const exitRows = await loadSpyMinuteBars(exitDay);
  if (!entryRows || !exitRows) return null;
  const entryBar = getSpyBarAtMinute(entryRows, entryMinuteEt);
  const exitBar = getSpyBarAtMinute(exitRows, exitMinuteEt);
  if (!entryBar || !exitBar) return null;
  const entryPrice = entryMinuteEt === 575 ? entryBar.open : entryBar.close; // 9:35 open vs other close
  const exitPrice = exitBar.close;
  if (!Number.isFinite(entryPrice) || !Number.isFinite(exitPrice)) return null;
  const sign = side === 'LONG' ? 1 : -1;
  const gross = sign * leverage * (exitPrice / entryPrice - 1);
  const cost = costBpsRoundTrip / 10_000;
  const net = gross - cost;
  return { signalDay, entryDay, exitDay, entryMinuteEt, exitMinuteEt, side, leverage, entryPrice, exitPrice, grossReturn: gross, cost, netReturn: net, isWin: net > 0 };
}

// --------------------------- Stats + reporting ----------------------------

function summarizeTrades(trades) {
  if (trades.length === 0) {
    return { trade_count: 0, hit_rate: 0, total_gross_pct: 0, total_net_pct: 0, avg_net_bps: 0, sharpe_per_trade: 0, max_drawdown_pct: 0 };
  }
  const n = trades.length;
  const wins = trades.filter((t) => t.netReturn > 0).length;
  const sumGross = trades.reduce((a, t) => a + t.grossReturn, 0);
  const sumNet = trades.reduce((a, t) => a + t.netReturn, 0);
  const meanNet = sumNet / n;
  const sd = Math.sqrt(trades.reduce((a, t) => a + ((t.netReturn - meanNet) ** 2), 0) / n);
  let cum = 0; let peak = 0; let dd = 0;
  for (const t of trades) { cum += t.netReturn; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum; }
  return {
    trade_count: n,
    hit_rate: wins / n,
    total_gross_pct: sumGross * 100,
    total_net_pct: sumNet * 100,
    avg_net_bps: meanNet * 10_000,
    sharpe_per_trade: sd > 0 ? (meanNet / sd) * Math.sqrt(252) : 0,
    max_drawdown_pct: dd * 100,
  };
}

// SPY buy-and-hold baseline for the same date range — closes-only equity curve.
// Uses the PYM daily bars JSONL when available (much faster than reading
// minute bars per day).
async function spyBuyHoldBaseline(startDate, endDate) {
  const spyBars = await loadDailyBars('SPY', startDate, endDate);
  if (spyBars.size === 0) {
    // Fallback to minute-bar path.
    const days = stockTradingDaysInRange(startDate, endDate);
    let first = null; let last = null;
    for (const day of days) {
      // eslint-disable-next-line no-await-in-loop
      const rows = await loadSpyMinuteBars(day);
      if (!rows || rows.length === 0) continue;
      const close = rows[rows.length - 1].close;
      if (Number.isFinite(close)) { if (first === null) first = close; last = close; }
    }
    if (first === null || last === null) return { totalReturnPct: 0 };
    return { totalReturnPct: (last / first - 1) * 100, startClose: first, endClose: last };
  }
  const sortedDates = [...spyBars.keys()].sort();
  const first = spyBars.get(sortedDates[0]).close;
  const last = spyBars.get(sortedDates[sortedDates.length - 1]).close;
  if (!Number.isFinite(first) || !Number.isFinite(last)) return { totalReturnPct: 0 };
  return { totalReturnPct: (last / first - 1) * 100, startClose: first, endClose: last };
}

// ----- Convenience: walk-forward across the protocol windows ---------------

const OFFICIAL_WINDOWS = {
  train: { name: 'train_2026_01_official', startDate: '2026-01-02', endDate: '2026-01-30' },
  tests: [
    { name: 'test_2026_02_official', startDate: '2026-02-02', endDate: '2026-02-27' },
    { name: 'test_2026_03_official', startDate: '2026-03-02', endDate: '2026-03-31' },
    { name: 'test_2026_04_official', startDate: '2026-04-01', endDate: '2026-04-27' },
  ],
};

const SENSITIVITY_WINDOWS = {
  train: { name: 'train_2025_sensitivity', startDate: '2025-01-02', endDate: '2025-12-31' },
  test: { name: 'test_2026_sensitivity', startDate: '2026-01-02', endDate: '2026-05-12' },
  full: { name: 'full_16mo_sensitivity', startDate: '2025-01-02', endDate: '2026-05-12' },
};

function printResultTable(label, sectionResults) {
  const header = ['window', 'trades', 'net %', 'gross %', 'hit %', 'Sharpe', 'maxDD %'];
  process.stdout.write(`\n${label}\n`);
  process.stdout.write('  ' + header.map((h) => h.padEnd(13)).join(' ') + '\n');
  for (const [winName, s] of Object.entries(sectionResults)) {
    const row = [
      winName,
      String(s.trade_count || 0),
      (s.total_net_pct || 0).toFixed(2),
      (s.total_gross_pct || 0).toFixed(2),
      ((s.hit_rate || 0) * 100).toFixed(1),
      (s.sharpe_per_trade || 0).toFixed(2),
      (s.max_drawdown_pct || 0).toFixed(2),
    ];
    process.stdout.write('  ' + row.map((c) => String(c).padEnd(13)).join(' ') + '\n');
  }
}

module.exports = {
  PROJECT_ROOT,
  isWeekend,
  stockTradingDaysInRange,
  loadOccZScoreSeries,
  loadOccZScoreSeriesWide,
  loadVixTenorsForDay,
  loadVixTermZSeries,
  loadVixTermZSeriesWide,
  filterMapByRange,
  loadAllDailyBars,
  loadDailyBars,
  prefetchSpyMinuteBarsBulk,
  loadPymBiasByDay,
  loadSpyMinuteBars,
  getSpyBarAtMinute,
  rollingZ,
  executeTrade,
  summarizeTrades,
  spyBuyHoldBaseline,
  printResultTable,
  OFFICIAL_WINDOWS,
  SENSITIVITY_WINDOWS,
};
