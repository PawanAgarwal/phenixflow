const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const readline = require('node:readline');

const { datasetCsvPath } = require('./config');
const { readGzipCsv, toNumber } = require('./csv');
const { parseOpraTicker } = require('./opra');
const { nsToMinuteMs, getEtParts } = require('./time');

const REGULAR_OPEN_ET = 570;
const REGULAR_CLOSE_ET = 960;
const SWEEP_WINDOW_NS = 100_000_000n; // 100 ms
const MIN_DISTINCT_EXCHANGES_FOR_SWEEP = 3;
const BLOCK_SIZE_THRESHOLD = 100;
const BLOCK_PREMIUM_THRESHOLD = 50_000;
const NOTABLE_PREMIUM_THRESHOLD = 5_000; // minimum premium to keep in "notable" per-trade output
const BAR_MID_EPSILON = 0.005; // dollars from mid before we declare side; under this falls back to tick rule

// DTE buckets for downstream aggregation
function dteBucket(dte) {
  if (dte === 0) return '0dte';
  if (dte === 1) return '1dte';
  if (dte >= 2 && dte <= 7) return '2_7dte';
  if (dte >= 8 && dte <= 30) return '8_30dte';
  if (dte > 30) return 'gt_30dte';
  return null;
}

// Pre-load per-minute bar high/low/close map from option_quotes_1m for the target roots
async function loadBarMap(config, dayIso, rootSet) {
  const csvPath = datasetCsvPath({ ...config, datasets: config.datasets }, config.datasets.optionBars, dayIso);
  if (!fs.existsSync(csvPath)) throw new Error(`Missing option bars ${csvPath}`);
  // key = `${ticker}|${minuteMs}` -> { high, low, close, mid }
  const byKey = new Map();
  await readGzipCsv(csvPath, async (row) => {
    const parsed = parseOpraTicker(row.ticker);
    if (!parsed || !rootSet.has(parsed.root)) return;
    const minuteMs = nsToMinuteMs(row.window_start);
    if (minuteMs === null) return;
    const high = toNumber(row.high);
    const low = toNumber(row.low);
    const close = toNumber(row.close);
    if (high === null || low === null || close === null) return;
    byKey.set(`${row.ticker}|${minuteMs}`, {
      high,
      low,
      close,
      mid: (high + low) / 2,
    });
  });
  return byKey;
}

// Pre-load per-(ticker, minute) greeks from the Phase-1 output (jsonl.gz).
async function loadGreeksMap(greeksPath) {
  if (!fs.existsSync(greeksPath)) return new Map();
  const byKey = new Map();
  const stream = fs.createReadStream(greeksPath).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  for await (const line of rl) {
    if (!line) continue;
    const obj = JSON.parse(line);
    byKey.set(`${obj.ticker}|${obj.minute_ms}`, {
      iv: obj.iv,
      delta: obj.delta,
      gamma: obj.gamma,
      vega_per_1pct: obj.vega_per_1pct,
      theta_per_day: obj.theta_per_day,
      vanna: obj.vanna,
      charm_per_day: obj.charm_per_day,
      vomma: obj.vomma,
      spot: obj.spot,
      T: obj.T,
      dte: obj.dte,
    });
  }
  return byKey;
}

// Per-side, per-DTE keys for 1m flow aggregation.
function emptyMinuteAgg(root) {
  return {
    root,
    minute_ms: 0,
    date_et: '',
    minute_of_day_et: 0,
    trade_count: 0,
    buy_size: 0,
    sell_size: 0,
    unk_size: 0,
    buy_premium: 0,
    sell_premium: 0,
    unk_premium: 0,
    call_buy_size: 0,
    call_sell_size: 0,
    put_buy_size: 0,
    put_sell_size: 0,
    call_buy_premium: 0,
    call_sell_premium: 0,
    put_buy_premium: 0,
    put_sell_premium: 0,
    sweep_count: 0,
    sweep_buy_premium: 0,
    sweep_sell_premium: 0,
    sweep_call_buy_premium: 0,
    sweep_call_sell_premium: 0,
    sweep_put_buy_premium: 0,
    sweep_put_sell_premium: 0,
    block_count: 0,
    block_buy_premium: 0,
    block_sell_premium: 0,
    block_call_buy_premium: 0,
    block_call_sell_premium: 0,
    block_put_buy_premium: 0,
    block_put_sell_premium: 0,
    // DTE × right × side premium (the BullFlow-style breakdown)
    premium_0dte_call_buy: 0,
    premium_0dte_call_sell: 0,
    premium_0dte_put_buy: 0,
    premium_0dte_put_sell: 0,
    premium_1dte_call_buy: 0,
    premium_1dte_call_sell: 0,
    premium_1dte_put_buy: 0,
    premium_1dte_put_sell: 0,
    premium_2_7dte_call_buy: 0,
    premium_2_7dte_call_sell: 0,
    premium_2_7dte_put_buy: 0,
    premium_2_7dte_put_sell: 0,
    premium_8_30dte_call_buy: 0,
    premium_8_30dte_call_sell: 0,
    premium_8_30dte_put_buy: 0,
    premium_8_30dte_put_sell: 0,
    // Dealer-perspective signed greek flow.
    // Convention: positive dealer_delta_flow = dealers got longer delta this minute.
    // BUY (customer) → dealer is short the option (negative delta sensitivity to customer's side).
    // For a CALL: customer BUY → dealer Δ decreases (sells calls → -Δ_call); customer SELL → dealer Δ increases.
    // For a PUT:  customer BUY → dealer Δ increases (sells puts → +Δ_put, but Δ_put<0 so this DECREASES dealer Δ).
    //   Equivalently: dealer takes -1 × Δ_option per BUY contract and +1 × Δ_option per SELL contract.
    // For Gamma: BUY → dealer Γ decreases (always), SELL → dealer Γ increases.
    dealer_delta_flow: 0,
    dealer_gamma_flow: 0,
    dealer_vega_flow: 0,
    dealer_vanna_flow: 0,
    dealer_charm_flow: 0,
  };
}

function dteBucketColumnKey(prefix, bucket, right, side) {
  if (!bucket || !right || !side) return null;
  if (side === 'UNK') return null;
  const rightKey = right === 'CALL' ? 'call' : 'put';
  const sideKey = side.toLowerCase();
  return `${prefix}_${bucket}_${rightKey}_${sideKey}`;
}

async function buildTradeFlowForDay({
  config,
  dayIso,
  root = 'SPY',
  greeksPath,
  outputFlowPath,
  outputNotablePath,
  regularSessionOnly = true,
}) {
  const tradesPath = datasetCsvPath({ ...config, datasets: config.datasets }, config.datasets.optionTrades, dayIso);
  if (!fs.existsSync(tradesPath)) throw new Error(`Missing option trades ${tradesPath}`);

  const rootSet = new Set([root]);
  const barMap = await loadBarMap(config, dayIso, rootSet);
  const greeksMap = await loadGreeksMap(greeksPath);

  // Group all trades by ticker first (in memory). For SPY ~1.1M trades/day = manageable.
  const tradesByTicker = new Map();
  await readGzipCsv(tradesPath, async (row) => {
    const parsed = parseOpraTicker(row.ticker);
    if (!parsed || !rootSet.has(parsed.root)) return;
    const correction = Number(row.correction);
    if (Number.isFinite(correction) && correction > 0) return; // skip corrected trades
    const size = toNumber(row.size);
    const price = toNumber(row.price);
    const ns = String(row.sip_timestamp || '').trim();
    if (!size || !price || !ns) return;
    if (!tradesByTicker.has(row.ticker)) tradesByTicker.set(row.ticker, []);
    tradesByTicker.get(row.ticker).push({
      ns_str: ns,
      size,
      price,
      exchange: Number(row.exchange) || 0,
      conditions: Number(row.conditions) || 0,
      parsed,
    });
  });

  // Open output streams
  const outDirFlow = path.dirname(outputFlowPath);
  fs.mkdirSync(outDirFlow, { recursive: true });
  const outDirNotable = path.dirname(outputNotablePath);
  fs.mkdirSync(outDirNotable, { recursive: true });

  const flowGzip = zlib.createGzip();
  const flowFile = fs.createWriteStream(outputFlowPath);
  flowGzip.pipe(flowFile);
  const flowDone = new Promise((resolve, reject) => {
    flowFile.on('finish', resolve);
    flowFile.on('error', reject);
    flowGzip.on('error', reject);
  });
  const notableGzip = zlib.createGzip();
  const notableFile = fs.createWriteStream(outputNotablePath);
  notableGzip.pipe(notableFile);
  const notableDone = new Promise((resolve, reject) => {
    notableFile.on('finish', resolve);
    notableFile.on('error', reject);
    notableGzip.on('error', reject);
  });

  const minuteAggs = new Map(); // minute_ms → agg

  function getMinuteAgg(minuteMs) {
    let agg = minuteAggs.get(minuteMs);
    if (!agg) {
      agg = emptyMinuteAgg(root);
      agg.minute_ms = minuteMs;
      const et = getEtParts(minuteMs);
      agg.date_et = et.dateEt;
      agg.minute_of_day_et = et.minuteOfDayEt;
      minuteAggs.set(minuteMs, agg);
    }
    return agg;
  }

  const stats = {
    rowsSeen: 0,
    rowsKept: 0,
    rowsSession: 0,
    sideBar: 0,
    sideTick: 0,
    sideUnk: 0,
    sweepClusters: 0,
    sweepTrades: 0,
    blocks: 0,
    notableEmitted: 0,
  };

  for (const [ticker, trades] of tradesByTicker.entries()) {
    if (trades.length === 0) continue;
    // Sort by sip_timestamp ascending. BigInt sort.
    trades.sort((a, b) => {
      const an = BigInt(a.ns_str);
      const bn = BigInt(b.ns_str);
      if (an < bn) return -1;
      if (an > bn) return 1;
      return 0;
    });

    // First pass: side assignment via bar-mid + tick rule
    let prevPrice = null;
    let prevSide = null;
    for (let i = 0; i < trades.length; i += 1) {
      const t = trades[i];
      const minuteMs = nsToMinuteMs(t.ns_str);
      const barKey = `${ticker}|${minuteMs}`;
      const bar = barMap.get(barKey);
      let side = 'UNK';
      let method = 'unknown';
      if (bar) {
        if (t.price > bar.mid + BAR_MID_EPSILON) { side = 'BUY'; method = 'bar'; }
        else if (t.price < bar.mid - BAR_MID_EPSILON) { side = 'SELL'; method = 'bar'; }
      }
      if (side === 'UNK') {
        if (prevPrice !== null) {
          if (t.price > prevPrice) { side = 'BUY'; method = 'tick'; }
          else if (t.price < prevPrice) { side = 'SELL'; method = 'tick'; }
          else if (prevSide) { side = prevSide; method = 'tick'; }
        }
      }
      t.side = side;
      t.method = method;
      t.minute_ms = minuteMs;
      prevPrice = t.price;
      if (side !== 'UNK') prevSide = side;
    }

    // Second pass: sweep clustering
    // A cluster = trades on this ticker within 100ms with same side and ≥3 distinct exchanges.
    let clusterStart = 0;
    let clusterStartNs = BigInt(trades[0].ns_str);
    let nextClusterId = 1;
    function tagCluster(startIdx, endIdx) {
      // [startIdx, endIdx)
      // Group within this span by side and check exchange diversity
      const bySide = new Map();
      for (let i = startIdx; i < endIdx; i += 1) {
        const t = trades[i];
        if (t.side === 'UNK') continue;
        if (!bySide.has(t.side)) bySide.set(t.side, []);
        bySide.get(t.side).push(i);
      }
      for (const [, indices] of bySide.entries()) {
        const exch = new Set();
        let totalSize = 0;
        for (const i of indices) {
          exch.add(trades[i].exchange);
          totalSize += trades[i].size;
        }
        if (exch.size >= MIN_DISTINCT_EXCHANGES_FOR_SWEEP && totalSize >= 5) {
          const cid = nextClusterId++;
          for (const i of indices) {
            trades[i].sweep_id = `${ticker}:${cid}`;
          }
          stats.sweepClusters += 1;
          stats.sweepTrades += indices.length;
        }
      }
    }
    for (let i = 1; i < trades.length; i += 1) {
      const ns = BigInt(trades[i].ns_str);
      if (ns - clusterStartNs > SWEEP_WINDOW_NS) {
        tagCluster(clusterStart, i);
        clusterStart = i;
        clusterStartNs = ns;
      }
    }
    tagCluster(clusterStart, trades.length);

    // Third pass: aggregate + emit notables
    for (const t of trades) {
      stats.rowsSeen += 1;
      const minuteMs = t.minute_ms;
      if (minuteMs === null) continue;
      const et = getEtParts(minuteMs);
      if (regularSessionOnly
        && (et.minuteOfDayEt < REGULAR_OPEN_ET || et.minuteOfDayEt >= REGULAR_CLOSE_ET)) continue;
      stats.rowsSession += 1;

      const premium = t.size * t.price * 100;
      const isBlock = t.size >= BLOCK_SIZE_THRESHOLD || premium >= BLOCK_PREMIUM_THRESHOLD;
      const isSweep = Boolean(t.sweep_id);
      if (isBlock) stats.blocks += 1;
      if (t.method === 'bar') stats.sideBar += 1;
      else if (t.method === 'tick') stats.sideTick += 1;
      else stats.sideUnk += 1;
      stats.rowsKept += 1;

      // Dealer greek flow signs: BUY (customer) → dealer is short option → contribution = -1 * option_greek * size * 100
      //                          SELL (customer) → dealer is long option → contribution = +1 * option_greek * size * 100
      // For UNK trades: do not contribute to signed dealer flow (avoid noise).
      const greeks = greeksMap.get(`${t.parsed.ticker}|${minuteMs}`);
      const dealerSign = t.side === 'BUY' ? -1 : t.side === 'SELL' ? +1 : 0;
      const sizeContracts = t.size;
      const agg = getMinuteAgg(minuteMs);
      agg.trade_count += 1;
      if (t.side === 'BUY') { agg.buy_size += t.size; agg.buy_premium += premium; }
      else if (t.side === 'SELL') { agg.sell_size += t.size; agg.sell_premium += premium; }
      else { agg.unk_size += t.size; agg.unk_premium += premium; }
      if (t.parsed.right === 'CALL') {
        if (t.side === 'BUY') { agg.call_buy_size += t.size; agg.call_buy_premium += premium; }
        else if (t.side === 'SELL') { agg.call_sell_size += t.size; agg.call_sell_premium += premium; }
      } else {
        if (t.side === 'BUY') { agg.put_buy_size += t.size; agg.put_buy_premium += premium; }
        else if (t.side === 'SELL') { agg.put_sell_size += t.size; agg.put_sell_premium += premium; }
      }
      if (isSweep) {
        agg.sweep_count += 1;
        if (t.side === 'BUY') {
          agg.sweep_buy_premium += premium;
          if (t.parsed.right === 'CALL') agg.sweep_call_buy_premium += premium;
          else agg.sweep_put_buy_premium += premium;
        } else if (t.side === 'SELL') {
          agg.sweep_sell_premium += premium;
          if (t.parsed.right === 'CALL') agg.sweep_call_sell_premium += premium;
          else agg.sweep_put_sell_premium += premium;
        }
      }
      if (isBlock) {
        agg.block_count += 1;
        if (t.side === 'BUY') {
          agg.block_buy_premium += premium;
          if (t.parsed.right === 'CALL') agg.block_call_buy_premium += premium;
          else agg.block_put_buy_premium += premium;
        } else if (t.side === 'SELL') {
          agg.block_sell_premium += premium;
          if (t.parsed.right === 'CALL') agg.block_call_sell_premium += premium;
          else agg.block_put_sell_premium += premium;
        }
      }
      const dteDays = greeks?.dte;
      const bucket = dteBucket(dteDays);
      const dteColumn = dteBucketColumnKey('premium', bucket, t.parsed.right, t.side);
      if (dteColumn && agg[dteColumn] !== undefined) agg[dteColumn] += premium;

      if (greeks && Number.isFinite(greeks.delta) && dealerSign !== 0) {
        agg.dealer_delta_flow += dealerSign * greeks.delta * sizeContracts * 100;
      }
      if (greeks && Number.isFinite(greeks.gamma) && dealerSign !== 0) {
        agg.dealer_gamma_flow += dealerSign * greeks.gamma * sizeContracts * 100;
      }
      if (greeks && Number.isFinite(greeks.vega_per_1pct) && dealerSign !== 0) {
        agg.dealer_vega_flow += dealerSign * greeks.vega_per_1pct * sizeContracts * 100;
      }
      if (greeks && Number.isFinite(greeks.vanna) && dealerSign !== 0) {
        agg.dealer_vanna_flow += dealerSign * greeks.vanna * sizeContracts * 100;
      }
      if (greeks && Number.isFinite(greeks.charm_per_day) && dealerSign !== 0) {
        agg.dealer_charm_flow += dealerSign * greeks.charm_per_day * sizeContracts * 100;
      }

      // Notable per-trade output: keep sweeps + blocks + any premium >= NOTABLE_PREMIUM_THRESHOLD
      if (isSweep || isBlock || premium >= NOTABLE_PREMIUM_THRESHOLD) {
        const bar = barMap.get(`${t.parsed.ticker}|${minuteMs}`);
        const notable = {
          sip_timestamp: t.ns_str,
          minute_ms: minuteMs,
          date_et: et.dateEt,
          minute_of_day_et: et.minuteOfDayEt,
          ticker: t.parsed.ticker,
          root: t.parsed.root,
          expiration: t.parsed.expiration,
          right: t.parsed.right,
          strike: t.parsed.strike,
          dte: dteDays ?? null,
          exchange: t.exchange,
          conditions: t.conditions,
          size: t.size,
          price: t.price,
          premium,
          bar_high: bar?.high ?? null,
          bar_low: bar?.low ?? null,
          bar_mid: bar?.mid ?? null,
          side: t.side,
          side_method: t.method,
          sweep_id: t.sweep_id ?? null,
          is_block: isBlock,
          spot: greeks?.spot ?? null,
          iv: greeks?.iv ?? null,
          delta: greeks?.delta ?? null,
          gamma: greeks?.gamma ?? null,
          vega_per_1pct: greeks?.vega_per_1pct ?? null,
          theta_per_day: greeks?.theta_per_day ?? null,
          vanna: greeks?.vanna ?? null,
          charm_per_day: greeks?.charm_per_day ?? null,
          dealer_delta_flow: greeks?.delta ? dealerSign * greeks.delta * sizeContracts * 100 : null,
          dealer_gamma_flow: greeks?.gamma ? dealerSign * greeks.gamma * sizeContracts * 100 : null,
        };
        if (!notableGzip.write(`${JSON.stringify(notable)}\n`)) {
          // backpressure: wait
          // eslint-disable-next-line no-await-in-loop
          await new Promise((resolve) => notableGzip.once('drain', resolve));
        }
        stats.notableEmitted += 1;
      }
    }
  }

  // Write 1m flow file (sorted by minute_ms)
  const minutes = Array.from(minuteAggs.keys()).sort((a, b) => a - b);
  for (const m of minutes) {
    const agg = minuteAggs.get(m);
    if (!flowGzip.write(`${JSON.stringify(agg)}\n`)) {
      // eslint-disable-next-line no-await-in-loop
      await new Promise((resolve) => flowGzip.once('drain', resolve));
    }
  }

  flowGzip.end();
  notableGzip.end();
  await Promise.all([flowDone, notableDone]);
  return stats;
}

function defaultFlowPaths(projectRoot, root, dayIso) {
  return {
    flow: path.join(projectRoot, 'runtime', 'trade-flow-1m', root, `date=${dayIso}`, `${dayIso}.flow.jsonl.gz`),
    notable: path.join(projectRoot, 'runtime', 'trade-flow-1m', root, `date=${dayIso}`, `${dayIso}.notable.jsonl.gz`),
  };
}

module.exports = {
  buildTradeFlowForDay,
  defaultFlowPaths,
  dteBucket,
};
