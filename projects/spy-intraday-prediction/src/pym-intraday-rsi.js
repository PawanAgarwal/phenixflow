// Phase 20 — PYM bias × intraday SPY RSI confirmation.
//
// Idea: PYM's daily bias gives the direction. Compute SPY RSI on resampled 2H bars within the same day
// to confirm or veto. If PYM bias is long AND SPY 2H RSI is also above mid-range, enter LONG; etc.
// Then we hold to close.
//
// 2H bars within a session (9:30–16:00 ET, 390 minutes):
//   bar 1: 9:30–11:30  (minutes 570–689)
//   bar 2: 11:30–13:30 (minutes 690–809)
//   bar 3: 13:30–15:30 (minutes 810–929)
//   bar 4: 15:30–16:00 (minutes 930–959, partial)
//
// We compute RSI(N) across the trailing N 2H bars, including prior days.

const fs = require('node:fs');
const zlib = require('node:zlib');
const readline = require('node:readline');
const path = require('node:path');

const { defaultFeaturesPath } = require('./build-features-1m');

// 2H buckets: start minute (inclusive) → end minute (exclusive) ET
const BUCKETS_2H = [
  { startEt: 570, endEt: 690 },
  { startEt: 690, endEt: 810 },
  { startEt: 810, endEt: 930 },
  { startEt: 930, endEt: 960 },
];

function bucketIndexForMinute(minuteOfDayEt) {
  for (let i = 0; i < BUCKETS_2H.length; i += 1) {
    const b = BUCKETS_2H[i];
    if (minuteOfDayEt >= b.startEt && minuteOfDayEt < b.endEt) return i;
  }
  return -1;
}

async function loadFeaturesForDay(projectRoot, root, day) {
  const p = defaultFeaturesPath(projectRoot, root, day);
  if (!fs.existsSync(p)) return [];
  const stream = fs.createReadStream(p).pipe(zlib.createGunzip());
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  const out = [];
  for await (const line of rl) {
    if (!line) continue;
    out.push(JSON.parse(line));
  }
  return out;
}

// Build 2H bars from per-minute SPY rows.
function build2hBars(minuteRows) {
  const bars = [];
  let cur = null;
  for (const r of minuteRows) {
    const bIdx = bucketIndexForMinute(r.minute_of_day_et);
    if (bIdx === -1) continue;
    if (!cur || cur.bucket !== bIdx || cur.date !== r.date_et) {
      if (cur) bars.push(cur);
      cur = {
        date: r.date_et,
        bucket: bIdx,
        startMinuteEt: BUCKETS_2H[bIdx].startEt,
        open: r.spy_open,
        high: r.spy_high,
        low: r.spy_low,
        close: r.spy_close,
      };
    } else {
      cur.high = Math.max(cur.high, r.spy_high);
      cur.low = Math.min(cur.low, r.spy_low);
      cur.close = r.spy_close;
    }
  }
  if (cur) bars.push(cur);
  return bars;
}

// Wilder RSI on a series of closes.
function wilderRsi(closes, period) {
  if (closes.length < period + 1) return null;
  let avgGain = 0; let avgLoss = 0;
  for (let i = 1; i <= period; i += 1) {
    const ch = closes[i] - closes[i - 1];
    if (ch > 0) avgGain += ch; else avgLoss -= ch;
  }
  avgGain /= period;
  avgLoss /= period;
  for (let i = period + 1; i < closes.length; i += 1) {
    const ch = closes[i] - closes[i - 1];
    const gain = ch > 0 ? ch : 0;
    const loss = ch < 0 ? -ch : 0;
    avgGain = (avgGain * (period - 1) + gain) / period;
    avgLoss = (avgLoss * (period - 1) + loss) / period;
  }
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function isWeekend(d) {
  const x = new Date(`${d}T00:00:00.000Z`).getUTCDay();
  return x === 0 || x === 6;
}

async function buildSpy2hSeries(projectRoot, startDate, endDate) {
  // Returns array of bar objects across all days.
  const allBars = [];
  const cur = new Date(`${startDate}T00:00:00.000Z`);
  const stop = new Date(`${endDate}T00:00:00.000Z`);
  while (cur <= stop) {
    const d = cur.toISOString().slice(0, 10);
    if (!isWeekend(d)) {
      // eslint-disable-next-line no-await-in-loop
      const rows = await loadFeaturesForDay(projectRoot, 'SPY', d);
      if (rows.length > 0) {
        const bars = build2hBars(rows);
        bars.forEach((b) => allBars.push(b));
      }
    }
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return allBars;
}

async function backtestPymWithIntradayRsi({
  projectRoot,
  pymByDate,
  startDate,
  endDate,
  params = {},
}) {
  const p = {
    biasLong: 0.20,
    biasShort: -0.20,
    rsiPeriod: 14,
    rsiBullThreshold: 50, // when PYM is long, require RSI > this to enter
    rsiBearThreshold: 50, // when PYM is short, require RSI < this to enter
    rsiSampleBucket: 0, // 0 = enter at end of bar 0 (11:30 ET)
    exitMinuteEt: 955,
    costBpsRoundTrip: 2,
    ...params,
  };

  // Build 2H bar series across the whole window (so RSI has lookback across days)
  const bars2h = await buildSpy2hSeries(projectRoot, startDate, endDate);

  // For each trading day, find the bar at rsiSampleBucket index, compute RSI using prior bars, decide trade
  const tradesByDay = new Map();
  for (let i = 0; i < bars2h.length; i += 1) {
    const b = bars2h[i];
    if (b.bucket !== p.rsiSampleBucket) continue;
    if (!pymByDate.has(b.date)) continue;
    const pymEntry = pymByDate.get(b.date);
    if (!Number.isFinite(pymEntry.bias)) continue;
    // Determine direction from PYM
    let side = null;
    if (pymEntry.bias >= p.biasLong) side = 'LONG';
    else if (pymEntry.bias <= p.biasShort) side = 'SHORT';
    if (!side) continue;
    // RSI using all prior bar closes (up to i, inclusive)
    const priorCloses = [];
    for (let j = 0; j <= i; j += 1) priorCloses.push(bars2h[j].close);
    const rsi = wilderRsi(priorCloses, p.rsiPeriod);
    if (rsi === null) continue;
    // Apply confirmation filter
    if (side === 'LONG' && rsi < p.rsiBullThreshold) continue;
    if (side === 'SHORT' && rsi > p.rsiBearThreshold) continue;
    // Entry price = close of current 2H bar; exit price = close at exitMinuteEt (or last bar of day)
    const entryPrice = b.close;
    // Find exit row from features file
    // eslint-disable-next-line no-await-in-loop
    const rows = await loadFeaturesForDay(projectRoot, 'SPY', b.date);
    const exitRow = rows.find((r) => r.minute_of_day_et === p.exitMinuteEt) || rows[rows.length - 1];
    if (!exitRow || !Number.isFinite(exitRow.spy_close)) continue;
    const sign = side === 'LONG' ? +1 : -1;
    const gross = sign * (exitRow.spy_close / entryPrice - 1);
    const cost = p.costBpsRoundTrip / 10_000;
    const net = gross - cost;
    tradesByDay.set(b.date, {
      date: b.date,
      bias: pymEntry.bias,
      rsi,
      side,
      entry_price: entryPrice,
      exit_price: exitRow.spy_close,
      gross_return: gross,
      cost,
      net_return: net,
    });
  }
  const trades = Array.from(tradesByDay.values()).sort((a, b) => a.date.localeCompare(b.date));
  // Stats
  if (trades.length === 0) return { params: p, trade_count: 0, total_net_pct: 0, total_gross_pct: 0, hit_rate: 0, sharpe_per_trade: 0, max_drawdown_pct: 0, trades };
  const n = trades.length;
  const wins = trades.filter((t) => t.net_return > 0).length;
  const sumGross = trades.reduce((a, t) => a + t.gross_return, 0);
  const sumNet = trades.reduce((a, t) => a + t.net_return, 0);
  const meanNet = sumNet / n;
  const sd = Math.sqrt(trades.reduce((a, t) => a + ((t.net_return - meanNet) ** 2), 0) / n);
  let cum = 0; let peak = 0; let dd = 0;
  for (const t of trades) { cum += t.net_return; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum; }
  return {
    params: p,
    trade_count: n,
    hit_rate: wins / n,
    total_gross_pct: sumGross * 100,
    total_net_pct: sumNet * 100,
    avg_net_bps: meanNet * 10_000,
    sharpe_per_trade: sd > 0 ? (meanNet / sd) * Math.sqrt(252) : 0,
    max_drawdown_pct: dd * 100,
    trades,
  };
}

module.exports = {
  build2hBars,
  buildSpy2hSeries,
  wilderRsi,
  backtestPymWithIntradayRsi,
};
