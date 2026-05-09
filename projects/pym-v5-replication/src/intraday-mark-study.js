const { openCalendarDays } = require('./calendar');
const { loadConfig } = require('./config');
const { readDailyBarsJsonl } = require('./backtest');
const { readMinuteBarsForDay } = require('./intraday-minute-data');
const { buildTargetReport } = require('./intraday-suite');

const DEFAULT_MARKS = Object.freeze([
  { id: 'close', label: 'Prior EOD close' },
  { id: '0935', label: '09:35 ET', minute: 575 },
  { id: '1030', label: '10:30 ET', minute: 630 },
  { id: '1130', label: '11:30 ET', minute: 690 },
  { id: '1230', label: '12:30 ET', minute: 750 },
  { id: '1330', label: '13:30 ET', minute: 810 },
  { id: '1430', label: '14:30 ET', minute: 870 },
  { id: '1555', label: '15:55 ET', minute: 955 },
]);

const DEFAULT_SEGMENTS = Object.freeze([
  { id: 'close_to_0935', from: 'close', to: '0935' },
  { id: 'close_to_1030', from: 'close', to: '1030' },
  { id: 'close_to_1130', from: 'close', to: '1130' },
  { id: 'close_to_1230', from: 'close', to: '1230' },
  { id: 'close_to_1330', from: 'close', to: '1330' },
  { id: 'close_to_1430', from: 'close', to: '1430' },
  { id: '0935_to_1030', from: '0935', to: '1030' },
  { id: '1030_to_1130', from: '1030', to: '1130' },
  { id: '1130_to_1230', from: '1130', to: '1230' },
  { id: '1230_to_1330', from: '1230', to: '1330' },
  { id: '1330_to_1430', from: '1330', to: '1430' },
  { id: '1430_to_1555', from: '1430', to: '1555' },
  { id: '0935_to_1430', from: '0935', to: '1430' },
  { id: '0935_to_1555', from: '0935', to: '1555' },
  { id: 'close_to_1555', from: 'close', to: '1555' },
]);

const DEFAULT_UNIVERSES = Object.freeze([
  { id: 'all', label: 'All PYM target holdings', limit: null },
  { id: 'top3', label: 'Top 3 PYM target weights', limit: 3 },
  { id: 'top5', label: 'Top 5 PYM target weights', limit: 5 },
  { id: 'top8', label: 'Top 8 PYM target weights', limit: 8 },
]);

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  return Math.sqrt(mean(values.map((value) => (value - avg) ** 2)));
}

function maxDrawdownFromReturns(returns) {
  let equity = 1;
  let peak = 1;
  let maxDrawdown = 0;
  returns.forEach((value) => {
    equity *= (1 + value);
    if (equity > peak) peak = equity;
    maxDrawdown = Math.min(maxDrawdown, (equity / peak) - 1);
  });
  return maxDrawdown;
}

function cleanAndNormalize(entries) {
  const out = new Map();
  let total = 0;
  entries.forEach(([ticker, weight]) => {
    if (Number.isFinite(weight) && weight > 1e-10) {
      out.set(ticker, weight);
      total += weight;
    }
  });
  if (total <= 0) return new Map();
  out.forEach((weight, ticker) => out.set(ticker, weight / total));
  return out;
}

function weightsForUniverse(snapshot, universe) {
  const entries = (snapshot?.holdings || [])
    .map((holding) => [holding.ticker, holding.weight])
    .sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
  return cleanAndNormalize(universe.limit ? entries.slice(0, universe.limit) : entries);
}

function targetTickers(snapshot) {
  return new Set((snapshot?.holdings || []).map((holding) => holding.ticker));
}

function dailyClose(market, dateIndex, date, ticker) {
  const index = dateIndex.get(date);
  if (index === undefined) return null;
  const close = market.closes.get(ticker)?.[index];
  return Number.isFinite(close) && close > 0 ? close : null;
}

function rawCloseAtOrBefore(dayBars, ticker, minute) {
  const bars = dayBars.barsByTicker.get(ticker);
  if (!bars) return null;
  let selectedMinute = null;
  let selectedClose = null;
  bars.forEach((bar, barMinute) => {
    if (barMinute > minute) return;
    if (selectedMinute !== null && barMinute < selectedMinute) return;
    if (!Number.isFinite(bar.close) || bar.close <= 0) return;
    selectedMinute = barMinute;
    selectedClose = bar.close;
  });
  return selectedClose;
}

function lastRawClose(dayBars, ticker) {
  const bars = dayBars.barsByTicker.get(ticker);
  if (!bars) return null;
  let selectedMinute = null;
  let selectedClose = null;
  bars.forEach((bar, minute) => {
    if (selectedMinute !== null && minute < selectedMinute) return;
    if (!Number.isFinite(bar.close) || bar.close <= 0) return;
    selectedMinute = minute;
    selectedClose = bar.close;
  });
  return selectedClose;
}

function adjustedIntradayPrice({ market, dateIndex, dayBars, date, ticker, minute }) {
  const raw = rawCloseAtOrBefore(dayBars, ticker, minute);
  const rawEnd = lastRawClose(dayBars, ticker);
  const adjustedEnd = dailyClose(market, dateIndex, date, ticker);
  if (!Number.isFinite(raw) || !Number.isFinite(rawEnd) || !Number.isFinite(adjustedEnd) || rawEnd <= 0) return null;
  return raw * (adjustedEnd / rawEnd);
}

function markPrice({ market, dateIndex, dayBars, signalDate, date, ticker, mark }) {
  if (mark.id === 'close') return dailyClose(market, dateIndex, signalDate, ticker);
  return adjustedIntradayPrice({ market, dateIndex, dayBars, date, ticker, minute: mark.minute });
}

function portfolioSegmentReturn({ market, dateIndex, dayBars, signalDate, date, weights, marksById, segment }) {
  let grossReturn = 0;
  const missing = [];
  weights.forEach((weight, ticker) => {
    const start = markPrice({
      market,
      dateIndex,
      dayBars,
      signalDate,
      date,
      ticker,
      mark: marksById.get(segment.from),
    });
    const end = markPrice({
      market,
      dateIndex,
      dayBars,
      signalDate,
      date,
      ticker,
      mark: marksById.get(segment.to),
    });
    if (!Number.isFinite(start) || !Number.isFinite(end) || start <= 0) {
      missing.push({ ticker, weight });
      return;
    }
    grossReturn += weight * ((end / start) - 1);
  });
  return { grossReturn, missing };
}

function emptySeries(universe, segment) {
  return {
    universe,
    segment,
    grossReturns: [],
    netReturns: [],
    missingTickerEvents: 0,
    skippedDays: 0,
    rows: [],
  };
}

function summarizeSeries(series) {
  const grossTotal = series.grossReturns.reduce((equity, value) => equity * (1 + value), 1) - 1;
  const netTotal = series.netReturns.reduce((equity, value) => equity * (1 + value), 1) - 1;
  const netVol = standardDeviation(series.netReturns) * Math.sqrt(252);
  const netAvg = mean(series.netReturns);
  return {
    universeId: series.universe.id,
    universeLabel: series.universe.label,
    segmentId: series.segment.id,
    from: series.segment.from,
    to: series.segment.to,
    observations: series.netReturns.length,
    skippedDays: series.skippedDays,
    missingTickerEvents: series.missingTickerEvents,
    grossTotalReturn: grossTotal,
    grossTotalReturnPct: grossTotal * 100,
    netTotalReturn: netTotal,
    netTotalReturnPct: netTotal * 100,
    meanGrossBps: mean(series.grossReturns) * 10000,
    meanNetBps: mean(series.netReturns) * 10000,
    netSharpe: netVol > 0 ? (netAvg * 252) / netVol : 0,
    netMaxDrawdown: maxDrawdownFromReturns(series.netReturns),
    netMaxDrawdownPct: maxDrawdownFromReturns(series.netReturns) * 100,
    netWinRate: series.netReturns.length ? series.netReturns.filter((value) => value > 0).length / series.netReturns.length : 0,
  };
}

async function runIntradayMarkStudy(settings = {}) {
  const config = settings.config || loadConfig();
  const startDate = settings.startDate || '2025-01-02';
  const endDate = settings.endDate || '2026-05-06';
  const costBps = Number.isFinite(settings.costBps) ? settings.costBps : 4;
  const marks = settings.marks || DEFAULT_MARKS;
  const marksById = new Map(marks.map((mark) => [mark.id, mark]));
  const segments = settings.segments || DEFAULT_SEGMENTS;
  const universes = settings.universes || DEFAULT_UNIVERSES;
  const targetReport = settings.targetReport || buildTargetReport({
    config,
    dailyBarsPath: settings.dailyBarsPath,
    scorePath: settings.scorePath,
    startDate,
    rsiMode: settings.rsiMode || 'wilder',
  });
  const market = readDailyBarsJsonl(targetReport.source.barsPath);
  const dateIndex = new Map(market.dates.map((date, index) => [date, index]));
  const targetByDate = new Map(targetReport.snapshots.map((snapshot) => [snapshot.date, snapshot]));
  const days = openCalendarDays(config.roots.calendar, startDate, endDate);
  const series = new Map();
  universes.forEach((universe) => {
    segments.forEach((segment) => {
      series.set(`${universe.id}:${segment.id}`, emptySeries(universe, segment));
    });
  });

  const skippedDays = [];
  let previousSnapshot = null;
  let processedDays = 0;
  for (const day of days) {
    const todaysSnapshot = targetByDate.get(day.date);
    if (!previousSnapshot) {
      previousSnapshot = todaysSnapshot || previousSnapshot;
      skippedDays.push({ date: day.date, reason: 'no_prior_eod_target' });
      continue;
    }
    const tickers = targetTickers(previousSnapshot);
    const dayBars = await readMinuteBarsForDay(config, day, tickers);
    if (!dayBars.minutes.length) {
      skippedDays.push({ date: day.date, reason: 'missing_minute_bars' });
      previousSnapshot = todaysSnapshot || previousSnapshot;
      continue;
    }

    universes.forEach((universe) => {
      const weights = weightsForUniverse(previousSnapshot, universe);
      segments.forEach((segment) => {
        const result = portfolioSegmentReturn({
          market,
          dateIndex,
          dayBars,
          signalDate: previousSnapshot.date,
          date: day.date,
          weights,
          marksById,
          segment,
        });
        const selectedSeries = series.get(`${universe.id}:${segment.id}`);
        if (!weights.size || result.missing.length === weights.size) {
          selectedSeries.skippedDays += 1;
          return;
        }
        const netReturn = result.grossReturn - ((2 * costBps) / 10000);
        selectedSeries.grossReturns.push(result.grossReturn);
        selectedSeries.netReturns.push(netReturn);
        selectedSeries.missingTickerEvents += result.missing.length;
        selectedSeries.rows.push({
          date: day.date,
          signalDate: previousSnapshot.date,
          grossReturn: result.grossReturn,
          netReturn,
          missingTickerCount: result.missing.length,
        });
      });
    });

    processedDays += 1;
    previousSnapshot = todaysSnapshot || previousSnapshot;
    if (settings.onProgress) settings.onProgress({ day, processedDays, skippedDays: skippedDays.length });
  }

  const summaries = [...series.values()].map(summarizeSeries)
    .sort((left, right) => (
      left.universeId.localeCompare(right.universeId)
      || left.segmentId.localeCompare(right.segmentId)
    ));

  return {
    generatedAt: new Date().toISOString(),
    settings: {
      startDate,
      endDate,
      costBps,
      rsiMode: settings.rsiMode || 'wilder',
      costModel: 'independent segment round trip; cash->weights plus weights->cash at costBps each side',
      timing: 'previous_eod_pym_target_to_intraday_marks',
    },
    targetSource: targetReport.source,
    skippedDays,
    marks,
    segments,
    universes,
    summaries,
    series: [...series.values()].map((value) => ({
      universeId: value.universe.id,
      segmentId: value.segment.id,
      rows: value.rows,
    })),
  };
}

module.exports = {
  DEFAULT_MARKS,
  DEFAULT_SEGMENTS,
  DEFAULT_UNIVERSES,
  runIntradayMarkStudy,
  weightsForUniverse,
};
