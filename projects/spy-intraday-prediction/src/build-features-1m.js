const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');
const { spawn } = require('node:child_process');

const { datasetCsvPath, datasetParquetPath, resolveDatasetSource, PROJECT_ROOT } = require('./config');
const { readGzipCsv, toNumber } = require('./csv');
const { nsToMinuteMs, getEtParts } = require('./time');

const REGULAR_OPEN_ET = 570;
const REGULAR_CLOSE_ET = 960;

// DuckDB-based parquet streamer — same approach pym-v5-replication/src/market-data.js uses
// so today's live parquet ({date}.live.parquet under config.roots.liveParquet) can be read.
function duckdbString(value) { return `'${String(value).replace(/'/g, "''")}'`; }

function buildParquetSql(filePath, columns) {
  const cols = columns.join(', ');
  return `COPY (SELECT ${cols} FROM read_parquet(${duckdbString(filePath)})) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);`;
}

async function streamParquetRows(filePath, columns, onRow) {
  const sql = buildParquetSql(filePath, columns);
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
  const code = await new Promise((resolve, reject) => {
    child.on('error', reject);
    child.on('close', resolve);
  });
  if (code !== 0) {
    throw new Error(`duckdb_parquet_read_failed:${filePath}:${stderr.join('').trim() || code}`);
  }
}

async function streamRows(source, columns, onRow) {
  if (source.format === 'csv.gz') {
    await readGzipCsv(source.filePath, onRow);
  } else if (source.format === 'parquet') {
    await streamParquetRows(source.filePath, columns, onRow);
  }
}

async function loadStockBars(config, dayIso, symbol) {
  const source = resolveDatasetSource(config, config.datasets.stockBars, dayIso);
  if (source.format === 'missing') return new Map();
  const byMinute = new Map();
  await streamRows(source, ['ticker', 'volume', 'open', 'close', 'high', 'low', 'window_start'], async (row) => {
    if (row.ticker !== symbol) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null) return;
    byMinute.set(minuteMs, {
      open: toNumber(row.open),
      close: toNumber(row.close),
      high: toNumber(row.high),
      low: toNumber(row.low),
      volume: toNumber(row.volume),
    });
  });
  return byMinute;
}

async function loadIndexBars(config, dayIso, symbol) {
  const source = resolveDatasetSource(config, config.datasets.indexBars, dayIso);
  if (source.format === 'missing') return new Map();
  const byMinute = new Map();
  await streamRows(source, ['ticker', 'open', 'close', 'high', 'low', 'window_start'], async (row) => {
    if (row.ticker !== symbol) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null) return;
    byMinute.set(minuteMs, {
      open: toNumber(row.open),
      close: toNumber(row.close),
    });
  });
  return byMinute;
}

async function loadFlowRows(flowPath) {
  if (!fs.existsSync(flowPath)) return [];
  const out = [];
  const stream = fs.createReadStream(flowPath).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    out.push(JSON.parse(line));
  }
  return out;
}

function loadOccDaily(occRoot, dayIso) {
  const file = path.join(occRoot, `date=${dayIso}`, `${dayIso}.csv`);
  if (!fs.existsSync(file)) return null;
  const lines = fs.readFileSync(file, 'utf8').trim().split('\n');
  if (lines.length < 2) return null;
  const header = lines[0].split(',');
  const cols = lines[1].split(',');
  const row = {};
  header.forEach((h, i) => { row[h] = cols[i]; });
  return {
    date: row.date,
    equity_calls: Number(row.equity_calls),
    equity_puts: Number(row.equity_puts),
    equity_total: Number(row.equity_total),
    index_other_calls: Number(row.index_other_calls),
    index_other_puts: Number(row.index_other_puts),
    index_other_total: Number(row.index_other_total),
    occ_total: Number(row.occ_total),
  };
}

function rollingPrev(map, key) {
  return map.get(key) || null;
}

// Z-score helper using last N daily rows
function zScore(values, current) {
  if (!values || values.length < 3) return 0;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((a, b) => a + ((b - mean) ** 2), 0) / values.length;
  const sd = Math.sqrt(variance);
  if (sd === 0) return 0;
  return (current - mean) / sd;
}

async function buildOccOverlay({ occRoot, dates, lookbackDays = 20 }) {
  // Pre-load OCC for all dates plus lookback before
  const rows = new Map(); // dayIso → row
  for (const d of dates) {
    const r = loadOccDaily(occRoot, d);
    if (r) rows.set(d, r);
  }
  // Also load earlier dates for lookback z-scores: scan filesystem for last N days before start
  const startDate = dates[0];
  const allDirs = fs.readdirSync(occRoot)
    .filter((d) => d.startsWith('date='))
    .map((d) => d.slice('date='.length))
    .filter((d) => d < startDate)
    .sort();
  const lookbackDates = allDirs.slice(-lookbackDays * 2); // get some buffer
  for (const d of lookbackDates) {
    const r = loadOccDaily(occRoot, d);
    if (r) rows.set(d, r);
  }
  const sorted = Array.from(rows.keys()).sort();
  const pcRatioHistory = [];
  const totalHistory = [];
  const overlay = new Map();
  for (const day of sorted) {
    const r = rows.get(day);
    const pc = r.equity_total > 0 ? r.equity_puts / Math.max(1, r.equity_calls) : 0;
    const pcZ = zScore(pcRatioHistory.slice(-lookbackDays), pc);
    const prevTotal = totalHistory.length > 0 ? totalHistory[totalHistory.length - 1] : null;
    const totalChange = prevTotal ? (r.equity_total - prevTotal) / prevTotal : 0;
    const totalChangeHistory = totalHistory.length >= 2
      ? totalHistory.slice(1).map((v, i) => (v - totalHistory[i]) / totalHistory[i])
      : [];
    const totalChangeZ = zScore(totalChangeHistory.slice(-lookbackDays), totalChange);
    overlay.set(day, {
      equity_pc_ratio: pc,
      equity_pc_ratio_z: pcZ,
      equity_total: r.equity_total,
      equity_total_change: totalChange,
      equity_total_change_z: totalChangeZ,
      index_pc_ratio: r.index_other_calls > 0 ? r.index_other_puts / r.index_other_calls : 0,
    });
    pcRatioHistory.push(pc);
    totalHistory.push(r.equity_total);
  }
  return overlay;
}

async function buildFeaturesForDay({
  config,
  dayIso,
  occOverlayDay,
  flowPath,
  outputPath,
  prevDayClose,
  underlyingSymbol = 'SPY',
}) {
  // Auto-route: I:* symbols come from indices_1m, others from stock_quotes_1m.
  const spyBars = underlyingSymbol.startsWith('I:')
    ? await loadIndexBars(config, dayIso, underlyingSymbol)
    : await loadStockBars(config, dayIso, underlyingSymbol);
  const vixBars = await loadIndexBars(config, dayIso, 'I:VIX');
  const spxBars = await loadIndexBars(config, dayIso, 'I:SPX');
  const flowRows = await loadFlowRows(flowPath);
  const flowByMinute = new Map();
  for (const r of flowRows) flowByMinute.set(r.minute_ms, r);

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  const gzip = zlib.createGzip();
  const file = fs.createWriteStream(outputPath);
  gzip.pipe(file);
  const done = new Promise((resolve, reject) => {
    file.on('finish', resolve);
    file.on('error', reject);
    gzip.on('error', reject);
  });

  // Build sorted minute list within regular session
  const minutes = Array.from(spyBars.keys())
    .filter((m) => {
      const et = getEtParts(m);
      return et.dateEt === dayIso && et.minuteOfDayEt >= REGULAR_OPEN_ET && et.minuteOfDayEt < REGULAR_CLOSE_ET;
    })
    .sort((a, b) => a - b);

  let cumDealerDelta = 0;
  let cumDealerGamma = 0;
  let cumDealerVega = 0;
  let cumDealerVanna = 0;
  let cumDealerCharm = 0;
  let cumNetPremium = 0;
  let cumSweepBuy = 0;
  let cumSweepSell = 0;
  let cumBlockBuy = 0;
  let cumBlockSell = 0;
  let cumCallBuy = 0;
  let cumCallSell = 0;
  let cumPutBuy = 0;
  let cumPutSell = 0;
  let cumPremium0dteCallBuy = 0;
  let cumPremium0dteCallSell = 0;
  let cumPremium0dtePutBuy = 0;
  let cumPremium0dtePutSell = 0;

  const sessionOpen = spyBars.get(minutes[0])?.open;
  let firstClose = null;

  for (let i = 0; i < minutes.length; i += 1) {
    const minuteMs = minutes[i];
    const spy = spyBars.get(minuteMs);
    if (!spy) continue;
    if (firstClose === null) firstClose = spy.close;
    const et = getEtParts(minuteMs);
    const vix = vixBars.get(minuteMs)?.close ?? null;
    const spx = spxBars.get(minuteMs)?.close ?? null;
    const flow = flowByMinute.get(minuteMs);

    // accumulate flow
    if (flow) {
      cumDealerDelta += flow.dealer_delta_flow || 0;
      cumDealerGamma += flow.dealer_gamma_flow || 0;
      cumDealerVega += flow.dealer_vega_flow || 0;
      cumDealerVanna += flow.dealer_vanna_flow || 0;
      cumDealerCharm += flow.dealer_charm_flow || 0;
      cumSweepBuy += flow.sweep_buy_premium || 0;
      cumSweepSell += flow.sweep_sell_premium || 0;
      cumBlockBuy += flow.block_buy_premium || 0;
      cumBlockSell += flow.block_sell_premium || 0;
      cumCallBuy += flow.call_buy_premium || 0;
      cumCallSell += flow.call_sell_premium || 0;
      cumPutBuy += flow.put_buy_premium || 0;
      cumPutSell += flow.put_sell_premium || 0;
      cumNetPremium += (flow.call_buy_premium || 0) + (flow.put_sell_premium || 0)
        - (flow.call_sell_premium || 0) - (flow.put_buy_premium || 0);
      cumPremium0dteCallBuy += flow.premium_0dte_call_buy || 0;
      cumPremium0dteCallSell += flow.premium_0dte_call_sell || 0;
      cumPremium0dtePutBuy += flow.premium_0dte_put_buy || 0;
      cumPremium0dtePutSell += flow.premium_0dte_put_sell || 0;
    }

    // Returns
    const fiveBack = i - 5 >= 0 ? spyBars.get(minutes[i - 5])?.close : null;
    const sixtyBack = i - 60 >= 0 ? spyBars.get(minutes[i - 60])?.close : null;
    // overnight_return = today's session open vs yesterday's close (constant within day)
    const overnightOpenClose = (prevDayClose && sessionOpen)
      ? (sessionOpen / prevDayClose - 1) : null;
    const intradayReturn = sessionOpen ? (spy.close / sessionOpen - 1) : null;
    const ret5m = fiveBack ? (spy.close / fiveBack - 1) : null;
    const ret60m = sixtyBack ? (spy.close / sixtyBack - 1) : null;

    const out = {
      minute_ms: minuteMs,
      date_et: dayIso,
      minute_of_day_et: et.minuteOfDayEt,
      // Underlying
      spy_open: spy.open,
      spy_close: spy.close,
      spy_high: spy.high,
      spy_low: spy.low,
      spy_volume: spy.volume,
      vix_close: vix,
      spx_close: spx,
      session_open: sessionOpen ?? null,
      first_close: firstClose,
      intraday_return: intradayReturn,
      overnight_return: overnightOpenClose,
      ret_5m: ret5m,
      ret_60m: ret60m,
      // Flow per-minute (raw)
      flow_buy_premium: flow?.buy_premium || 0,
      flow_sell_premium: flow?.sell_premium || 0,
      flow_call_buy_premium: flow?.call_buy_premium || 0,
      flow_call_sell_premium: flow?.call_sell_premium || 0,
      flow_put_buy_premium: flow?.put_buy_premium || 0,
      flow_put_sell_premium: flow?.put_sell_premium || 0,
      flow_sweep_buy_premium: flow?.sweep_buy_premium || 0,
      flow_sweep_sell_premium: flow?.sweep_sell_premium || 0,
      flow_sweep_call_buy_premium: flow?.sweep_call_buy_premium || 0,
      flow_sweep_call_sell_premium: flow?.sweep_call_sell_premium || 0,
      flow_sweep_put_buy_premium: flow?.sweep_put_buy_premium || 0,
      flow_sweep_put_sell_premium: flow?.sweep_put_sell_premium || 0,
      flow_block_buy_premium: flow?.block_buy_premium || 0,
      flow_block_sell_premium: flow?.block_sell_premium || 0,
      flow_block_call_buy_premium: flow?.block_call_buy_premium || 0,
      flow_block_call_sell_premium: flow?.block_call_sell_premium || 0,
      flow_block_put_buy_premium: flow?.block_put_buy_premium || 0,
      flow_block_put_sell_premium: flow?.block_put_sell_premium || 0,
      flow_premium_0dte_call_buy: flow?.premium_0dte_call_buy || 0,
      flow_premium_0dte_call_sell: flow?.premium_0dte_call_sell || 0,
      flow_premium_0dte_put_buy: flow?.premium_0dte_put_buy || 0,
      flow_premium_0dte_put_sell: flow?.premium_0dte_put_sell || 0,
      flow_dealer_delta_flow: flow?.dealer_delta_flow || 0,
      flow_dealer_gamma_flow: flow?.dealer_gamma_flow || 0,
      flow_dealer_vanna_flow: flow?.dealer_vanna_flow || 0,
      flow_dealer_charm_flow: flow?.dealer_charm_flow || 0,
      // Cumulative session-to-now (great for "regime" signals)
      cum_dealer_delta: cumDealerDelta,
      cum_dealer_gamma: cumDealerGamma,
      cum_dealer_vega: cumDealerVega,
      cum_dealer_vanna: cumDealerVanna,
      cum_dealer_charm: cumDealerCharm,
      cum_net_premium: cumNetPremium,
      cum_sweep_buy_premium: cumSweepBuy,
      cum_sweep_sell_premium: cumSweepSell,
      cum_block_buy_premium: cumBlockBuy,
      cum_block_sell_premium: cumBlockSell,
      cum_call_buy_premium: cumCallBuy,
      cum_call_sell_premium: cumCallSell,
      cum_put_buy_premium: cumPutBuy,
      cum_put_sell_premium: cumPutSell,
      cum_premium_0dte_call_buy: cumPremium0dteCallBuy,
      cum_premium_0dte_call_sell: cumPremium0dteCallSell,
      cum_premium_0dte_put_buy: cumPremium0dtePutBuy,
      cum_premium_0dte_put_sell: cumPremium0dtePutSell,
      // OCC daily overlay (constant within day)
      occ_equity_pc_ratio: occOverlayDay?.equity_pc_ratio ?? null,
      occ_equity_pc_ratio_z: occOverlayDay?.equity_pc_ratio_z ?? null,
      occ_equity_total: occOverlayDay?.equity_total ?? null,
      occ_equity_total_change: occOverlayDay?.equity_total_change ?? null,
      occ_equity_total_change_z: occOverlayDay?.equity_total_change_z ?? null,
      occ_index_pc_ratio: occOverlayDay?.index_pc_ratio ?? null,
    };
    if (!gzip.write(`${JSON.stringify(out)}\n`)) {
      // eslint-disable-next-line no-await-in-loop
      await new Promise((resolve) => gzip.once('drain', resolve));
    }
  }

  gzip.end();
  await done;
  return { rows: minutes.length };
}

function defaultFeaturesPath(projectRoot, root, dayIso) {
  return path.join(projectRoot, 'runtime', 'features-1m', root, `date=${dayIso}`, `${dayIso}.jsonl.gz`);
}

module.exports = {
  buildFeaturesForDay,
  defaultFeaturesPath,
  buildOccOverlay,
  loadStockBars,
  loadIndexBars,
};
