// Phase 19 — PYM bias as intraday SPY trade gate.
//
// Strategy idea: PYM has a strong daily regime signal (+82% / 2.4 Sharpe over Jan-2025 → Apr-2026).
// Its sleeve weights tell us whether the market is in a risk-on or risk-off regime each day.
// Use that as the direction for intraday SPY trades, then (optionally) use our option flow
// features as a confirmation filter.

const fs = require('node:fs');
const zlib = require('node:zlib');
const readline = require('node:readline');
const path = require('node:path');

const { defaultFeaturesPath } = require('./build-features-1m');

// Ticker → category buckets used to derive the daily PYM bias.
const RISK_ON_TICKERS = new Set([
  'TQQQ', 'UPRO', 'QQQ', 'SPY', 'EEM', 'EDC', 'SOXL', 'TECL', 'IWM', 'DIA', 'QLD', 'SPXL', 'FAS', 'TNA',
]);
const DEFENSIVE_TICKERS = new Set([
  'BIL', 'SHV', 'ZROZ', 'TLT', 'TMF', 'SHY', 'BSV', 'IEF', 'VBF', 'AGG', 'TLH',
]);
const INVERSE_TICKERS = new Set([
  'SQQQ', 'SPXU', 'SOXS', 'EDZ', 'PSQ', 'TECS', 'TZA', 'SH', 'SDS', 'TMV', 'QID', 'FAZ',
]);
const VOL_TICKERS = new Set(['VIXY', 'UVXY']); // long vol = bearish bet
const SVOL_TICKERS = new Set(['SVXY', 'SVIX']); // short vol = bullish bet
const SECTOR_DEFENSIVE = new Set(['XLP', 'XLU', 'XLV']);
const SECTOR_CYCLICAL = new Set(['XLK', 'XLY', 'XLF', 'XLB', 'XLE', 'XLI', 'XLC', 'XLRE']);

function pymBias(holdings) {
  // Returns a directional score in roughly [-1, +1].
  // > 0 means PYM is bullish on equities; < 0 means bearish/defensive.
  let bullish = 0;
  let bearish = 0;
  let assigned = 0;
  for (const [ticker, weight] of Object.entries(holdings)) {
    if (!Number.isFinite(weight)) continue;
    if (RISK_ON_TICKERS.has(ticker) || SVOL_TICKERS.has(ticker)) {
      bullish += weight;
      assigned += weight;
    } else if (DEFENSIVE_TICKERS.has(ticker) || INVERSE_TICKERS.has(ticker) || VOL_TICKERS.has(ticker)) {
      bearish += weight;
      assigned += weight;
    } else if (SECTOR_CYCLICAL.has(ticker)) {
      bullish += 0.5 * weight;
      assigned += weight;
    } else if (SECTOR_DEFENSIVE.has(ticker)) {
      bearish += 0.5 * weight;
      assigned += weight;
    } else {
      assigned += weight; // count weight but no directional opinion
    }
  }
  if (assigned <= 0) return 0;
  return (bullish - bearish) / assigned;
}

function loadPymHoldings(pymArtifactPath) {
  const obj = JSON.parse(fs.readFileSync(pymArtifactPath, 'utf8'));
  const byDate = new Map();
  for (const ec of obj.equityCurve || []) {
    // The signal that *informs* a given trade day is the holdings assigned on that day's date row.
    // PYM uses signalDate (prior trading day) -> holdings -> apply on date with next_close timing.
    // For our intraday gating: at day T open, we can use the bias for day T (signal known by prior close).
    byDate.set(ec.date, {
      signalDate: ec.signalDate,
      holdings: ec.holdings || {},
      bias: pymBias(ec.holdings || {}),
      grossReturn: ec.grossReturn,
      netReturn: ec.netReturn,
    });
  }
  return byDate;
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

// Execute one intraday trade per day based on the bias score.
// Several variants — see backtest-pym-gated.js for the full grid.
async function backtestPymGated({
  projectRoot,
  root = 'SPY',
  pymByDate,
  startDate,
  endDate,
  params = {},
}) {
  const p = {
    biasLong: 0.10,
    biasShort: -0.10,
    entryMinuteEt: 575,
    exitMinuteEt: 955,
    costBpsRoundTrip: 2,
    flowFilter: false,
    flowMinCumCallNet: 50_000_000,
    flowMaxCumCallNet: -50_000_000,
    flowMinuteEt: 600,
    // Optional flow-as-position-size-modifier (not gate). When flowSizing=true:
    //   sizeMultiplier = 1 if flow disagrees, 2 if flow agrees with PYM direction
    flowSizing: false,
    flowAgreeMultiplier: 1.5,
    flowDisagreeMultiplier: 1.0,
    flowAgreementThreshold: 5_000_000, // |cum net call premium| > this to count as agreement
    // VIX filter — skip if VIX above this (high-vol days are volatile)
    maxVixClose: null,
    // Position size proportional to |bias| (with cap)
    biasProportionalSize: false,
    biasSizeMultiplier: 5, // size = min(|bias| * mult, 1.0)
    ...params,
  };
  const trades = [];
  const days = Array.from(pymByDate.keys()).sort();
  for (const day of days) {
    if (day < startDate || day > endDate) continue;
    const pymEntry = pymByDate.get(day);
    if (!pymEntry || !Number.isFinite(pymEntry.bias)) continue;
    // eslint-disable-next-line no-await-in-loop
    const rows = await loadFeaturesForDay(projectRoot, root, day);
    if (rows.length === 0) continue;
    // If we're going to use flow (either as filter or sizing), entry must be at or after the flow observation
    // minute, otherwise we'd be peeking into the future. Auto-advance entry to one minute after flow observation.
    let effectiveEntryMinuteEt = p.entryMinuteEt;
    if ((p.flowFilter || p.flowSizing) && p.flowMinuteEt >= p.entryMinuteEt) {
      effectiveEntryMinuteEt = p.flowMinuteEt + 1;
    }
    const entryRow = rows.find((r) => r.minute_of_day_et === effectiveEntryMinuteEt) || rows[0];
    const exitRow = rows.find((r) => r.minute_of_day_et === p.exitMinuteEt) || rows[rows.length - 1];
    if (!Number.isFinite(entryRow.spy_open) || !Number.isFinite(exitRow.spy_close)) continue;
    let side = null;
    if (pymEntry.bias >= p.biasLong) side = 'LONG';
    else if (pymEntry.bias <= p.biasShort) side = 'SHORT';
    else continue;
    // VIX filter
    if (Number.isFinite(p.maxVixClose) && Number.isFinite(entryRow.vix_close) && entryRow.vix_close > p.maxVixClose) continue;
    // Flow filter
    if (p.flowFilter) {
      const flowRow = rows.find((r) => r.minute_of_day_et === p.flowMinuteEt);
      if (!flowRow) continue;
      const cumNet = (flowRow.cum_call_buy_premium || 0) - (flowRow.cum_call_sell_premium || 0)
        - (flowRow.cum_put_buy_premium || 0) + (flowRow.cum_put_sell_premium || 0);
      if (side === 'LONG' && cumNet < p.flowMinCumCallNet) continue;
      if (side === 'SHORT' && cumNet > p.flowMaxCumCallNet) continue;
    }
    // Position sizing
    let size = 1.0;
    if (p.biasProportionalSize) {
      size = Math.min(Math.abs(pymEntry.bias) * p.biasSizeMultiplier, 1.0);
    }
    if (p.flowSizing) {
      const flowRow = rows.find((r) => r.minute_of_day_et === p.flowMinuteEt);
      if (flowRow) {
        const cumNet = (flowRow.cum_call_buy_premium || 0) - (flowRow.cum_call_sell_premium || 0)
          - (flowRow.cum_put_buy_premium || 0) + (flowRow.cum_put_sell_premium || 0);
        const flowAgrees = (side === 'LONG' && cumNet > p.flowAgreementThreshold)
          || (side === 'SHORT' && cumNet < -p.flowAgreementThreshold);
        size *= flowAgrees ? p.flowAgreeMultiplier : p.flowDisagreeMultiplier;
      }
    }
    const sign = side === 'LONG' ? +1 : -1;
    const gross = sign * size * (exitRow.spy_close / entryRow.spy_open - 1);
    const cost = (p.costBpsRoundTrip / 10_000) * size;
    const net = gross - cost;
    trades.push({
      date: day,
      bias: pymEntry.bias,
      side,
      size,
      entry_price: entryRow.spy_open,
      exit_price: exitRow.spy_close,
      gross_return: gross,
      cost,
      net_return: net,
    });
  }
  return aggregateStats(trades, p);
}

function aggregateStats(trades, params) {
  if (trades.length === 0) {
    return {
      params, trade_count: 0, total_net_pct: 0, total_gross_pct: 0, hit_rate: 0,
      sharpe_per_trade: 0, max_drawdown_pct: 0, avg_net_bps: 0, trades,
    };
  }
  const n = trades.length;
  const wins = trades.filter((t) => t.net_return > 0).length;
  const sumGross = trades.reduce((a, t) => a + t.gross_return, 0);
  const sumNet = trades.reduce((a, t) => a + t.net_return, 0);
  const meanNet = sumNet / n;
  const sd = Math.sqrt(trades.reduce((a, t) => a + ((t.net_return - meanNet) ** 2), 0) / n);
  let cum = 0; let peak = 0; let dd = 0;
  for (const t of trades) {
    cum += t.net_return; if (cum > peak) peak = cum; if (peak - cum > dd) dd = peak - cum;
  }
  return {
    params,
    trade_count: n,
    hit_rate: wins / n,
    total_gross_pct: sumGross * 100,
    total_net_pct: sumNet * 100,
    avg_net_bps: meanNet * 10_000,
    std_net_bps: sd * 10_000,
    sharpe_per_trade: sd > 0 ? (meanNet / sd) * Math.sqrt(252) : 0,
    max_drawdown_pct: dd * 100,
    trades,
  };
}

module.exports = {
  loadPymHoldings,
  pymBias,
  backtestPymGated,
  RISK_ON_TICKERS,
  DEFENSIVE_TICKERS,
  INVERSE_TICKERS,
};
