#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const ARTIFACT_PATH = path.resolve(
  process.env.ARTIFACT_PATH
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports', 'vixregime-backtest-2025-01-02-2026-03-21.json'),
);
const OUTPUT_DIR = path.resolve(
  process.env.OUTPUT_DIR
    || path.join(process.cwd(), 'vixregime', 'artifacts', 'reports'),
);
const DAILY_TOLERANCE_PCT = Number(process.env.DAILY_TOLERANCE_PCT || 0.25);

const REGIME_COLORS = Object.freeze({
  Calm: '#16a34a',
  Normal: '#64748b',
  Stress: '#f59e0b',
  Crash: '#dc2626',
});

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function escapeXml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&apos;');
}

function buildLinePath(points = [], xScale, yScale) {
  const usable = points.filter((point) => point && point.y !== null && point.y !== undefined);
  if (!usable.length) return '';
  return usable.map((point, index) => {
    const x = xScale(point.x);
    const y = yScale(point.y);
    return `${index === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(' ');
}

function buildRegimeBands(features = [], xScale, top, bottom) {
  if (!features.length) return '';
  const dayMs = 24 * 60 * 60 * 1000;
  return features.map((row) => {
    const x = xScale(row.timestampMs - (dayMs / 2));
    const nextX = xScale(row.timestampMs + (dayMs / 2));
    const color = REGIME_COLORS[row.regime] || '#cbd5e1';
    return `<rect x="${x.toFixed(2)}" y="${top}" width="${Math.max(1, nextX - x).toFixed(2)}" height="${bottom - top}" fill="${color}" opacity="0.07" />`;
  }).join('\n');
}

function buildRegimeMarkers(features = [], values = [], xScale, yScale) {
  return features.map((row, index) => {
    const y = values[index];
    if (y === null || y === undefined) return '';
    const xPos = xScale(row.timestampMs);
    const yPos = yScale(y);
    const color = REGIME_COLORS[row.regime] || '#cbd5e1';
    return `<circle cx="${xPos.toFixed(2)}" cy="${yPos.toFixed(2)}" r="2.4" fill="${color}" stroke="#ffffff" stroke-width="0.7" />`;
  }).join('\n');
}

function buildRegimeLegend(width) {
  const entries = ['Calm', 'Normal', 'Stress', 'Crash'];
  return entries.map((label, idx) => {
    const x = width - 410 + (idx * 95);
    const color = REGIME_COLORS[label];
    return [
      `<rect x="${x}" y="18" width="14" height="14" fill="${color}" opacity="0.8" rx="2" />`,
      `<text x="${x + 20}" y="30" font-size="12" fill="#334155">${escapeXml(label)}</text>`,
    ].join('');
  }).join('');
}

function classifyDayOutcome(regime, benchmarkReturn, tolerancePct) {
  const tol = tolerancePct / 100;
  const bench = toNumber(benchmarkReturn);
  if (bench === null) return 'no_signal';
  if (bench > tol) {
    return (regime === 'Calm' || regime === 'Normal') ? 'right' : 'wrong';
  }
  if (bench < -tol) {
    return (regime === 'Stress' || regime === 'Crash') ? 'right' : 'wrong';
  }
  if (regime === 'Normal' || regime === 'Stress') return 'neutral';
  return 'neutral';
}

function evaluateRegimes(observations = [], tolerancePct = DAILY_TOLERANCE_PCT) {
  const totals = {
    tolerancePct,
    overall: { right: 0, wrong: 0, neutral: 0, no_signal: 0 },
    byRegime: {},
    wrongExamples: [],
    rightExamples: [],
  };
  observations.forEach((row) => {
    const label = classifyDayOutcome(row.regime, row.benchmarkReturn, tolerancePct);
    totals.overall[label] += 1;
    if (!totals.byRegime[row.regime]) {
      totals.byRegime[row.regime] = { right: 0, wrong: 0, neutral: 0, no_signal: 0 };
    }
    totals.byRegime[row.regime][label] += 1;
    const payload = {
      tradeDateUtc: row.tradeDateUtc,
      regime: row.regime,
      benchmarkReturn: row.benchmarkReturn,
      portfolioReturn: row.portfolioReturn,
      outcome: label,
      reasons: row.reasons,
    };
    if (label === 'wrong' && totals.wrongExamples.length < 12) totals.wrongExamples.push(payload);
    if (label === 'right' && totals.rightExamples.length < 12) totals.rightExamples.push(payload);
  });
  return totals;
}

function makeSvgChart({
  title,
  subtitle,
  series,
  features = [],
  markerSeries = [],
  width = 1200,
  height = 620,
  yLabel,
}) {
  const margin = { top: 70, right: 30, bottom: 70, left: 70 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;

  const allPoints = series.flatMap((s) => s.points).filter((point) => point && point.y !== null && point.y !== undefined);
  const xMin = Math.min(...allPoints.map((point) => point.x));
  const xMax = Math.max(...allPoints.map((point) => point.x));
  let yMin = Math.min(...allPoints.map((point) => point.y));
  let yMax = Math.max(...allPoints.map((point) => point.y));
  if (yMin === yMax) {
    yMin -= 1;
    yMax += 1;
  }
  const yPad = (yMax - yMin) * 0.08;
  yMin -= yPad;
  yMax += yPad;

  const xScale = (x) => margin.left + (((x - xMin) / Math.max(1, xMax - xMin)) * plotWidth);
  const yScale = (y) => margin.top + (plotHeight - (((y - yMin) / Math.max(1e-9, yMax - yMin)) * plotHeight));

  const yTicks = 5;
  const xTicks = 6;
  const yTickValues = Array.from({ length: yTicks + 1 }, (_, idx) => yMin + (((yMax - yMin) / yTicks) * idx));
  const xTickValues = Array.from({ length: xTicks + 1 }, (_, idx) => xMin + (((xMax - xMin) / xTicks) * idx));

  const gridLines = [];
  yTickValues.forEach((value) => {
    const y = yScale(value);
    gridLines.push(`<line x1="${margin.left}" y1="${y.toFixed(2)}" x2="${(width - margin.right).toFixed(2)}" y2="${y.toFixed(2)}" stroke="#e5e7eb" stroke-width="1" />`);
  });
  xTickValues.forEach((value) => {
    const x = xScale(value);
    gridLines.push(`<line x1="${x.toFixed(2)}" y1="${margin.top}" x2="${x.toFixed(2)}" y2="${(height - margin.bottom).toFixed(2)}" stroke="#f1f5f9" stroke-width="1" />`);
  });

  const xLabels = xTickValues.map((value) => {
    const date = new Date(value);
    const label = `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, '0')}`;
    return `<text x="${xScale(value).toFixed(2)}" y="${height - margin.bottom + 24}" text-anchor="middle" font-size="12" fill="#475569">${escapeXml(label)}</text>`;
  }).join('');
  const yLabels = yTickValues.map((value) => {
    const y = yScale(value);
    return `<text x="${margin.left - 10}" y="${(y + 4).toFixed(2)}" text-anchor="end" font-size="12" fill="#475569">${escapeXml(value.toFixed(2))}</text>`;
  }).join('');

  const linePaths = series.map((entry) => (
    `<path d="${buildLinePath(entry.points, xScale, yScale)}" fill="none" stroke="${entry.color}" stroke-width="${entry.strokeWidth || 2}" stroke-linejoin="round" stroke-linecap="round" />`
  )).join('\n');
  const regimeBands = buildRegimeBands(features, xScale, margin.top, height - margin.bottom);
  const regimeMarkers = markerSeries.map((entry) => buildRegimeMarkers(features, entry.values, xScale, yScale)).join('\n');

  const legend = series.map((entry, idx) => {
    const x = margin.left + (idx * 180);
    return [
      `<line x1="${x}" y1="32" x2="${x + 24}" y2="32" stroke="${entry.color}" stroke-width="3" />`,
      `<text x="${x + 32}" y="36" font-size="13" fill="#0f172a">${escapeXml(entry.label)}</text>`,
    ].join('');
  }).join('');

  return `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}">
  <rect width="100%" height="100%" fill="#ffffff" />
  <text x="${margin.left}" y="26" font-size="24" font-weight="700" fill="#0f172a">${escapeXml(title)}</text>
  <text x="${margin.left}" y="52" font-size="13" fill="#475569">${escapeXml(subtitle)}</text>
  ${legend}
  ${buildRegimeLegend(width, margin.left)}
  ${regimeBands}
  ${gridLines.join('\n')}
  <line x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}" stroke="#94a3b8" stroke-width="1.5" />
  <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${height - margin.bottom}" stroke="#94a3b8" stroke-width="1.5" />
  ${xLabels}
  ${yLabels}
  <text x="24" y="${height / 2}" transform="rotate(-90 24 ${height / 2})" font-size="13" fill="#475569">${escapeXml(yLabel)}</text>
  ${linePaths}
  ${regimeMarkers}
</svg>
`;
}

function cumulativeReturn(rows, key) {
  let curve = 1;
  rows.forEach((row) => {
    const ret = toNumber(row[key]);
    if (ret === null) return;
    curve *= (1 + ret);
  });
  return curve - 1;
}

function findWorstRelativeWindow(observations = []) {
  const rows = observations.filter((row) => (
    toNumber(row.portfolioReturn) !== null
    && toNumber(row.benchmarkReturn) !== null
    && (1 + row.portfolioReturn) > 0
    && (1 + row.benchmarkReturn) > 0
  ));

  const prefix = [0];
  rows.forEach((row) => {
    const relLog = Math.log(1 + row.portfolioReturn) - Math.log(1 + row.benchmarkReturn);
    prefix.push(prefix[prefix.length - 1] + relLog);
  });

  let best = null;
  for (let i = 0; i < rows.length; i += 1) {
    for (let j = i; j < rows.length; j += 1) {
      const relLogSum = prefix[j + 1] - prefix[i];
      if (best && relLogSum >= best.relLogSum) continue;
      const windowRows = rows.slice(i, j + 1);
      const regimeCounts = {};
      windowRows.forEach((row) => {
        regimeCounts[row.regime] = (regimeCounts[row.regime] || 0) + 1;
      });
      const strategyReturn = cumulativeReturn(windowRows, 'portfolioReturn');
      const benchmarkReturn = cumulativeReturn(windowRows, 'benchmarkReturn');
      best = {
        startDate: windowRows[0].tradeDateUtc,
        endDate: windowRows[windowRows.length - 1].tradeDateUtc,
        tradingDays: windowRows.length,
        strategyReturn,
        benchmarkReturn,
        relativeReturn: ((1 + strategyReturn) / (1 + benchmarkReturn)) - 1,
        relLogSum,
        regimeCounts,
      };
    }
  }
  return best;
}

function run() {
  const raw = JSON.parse(fs.readFileSync(ARTIFACT_PATH, 'utf8'));
  const features = raw.daily.features || [];
  const observations = raw.daily.observations || [];
  const obsByDate = new Map(observations.map((row) => [row.tradeDateUtc, row]));
  const datedFeatures = features.map((row) => ({
    ...row,
    timestampMs: Date.parse(`${row.tradeDateUtc}T00:00:00.000Z`),
  }));

  const equitySvg = makeSvgChart({
    title: 'Daily Equity Curves: VIXRegime vs SPY',
    subtitle: `${raw.startDate} to ${raw.endDate} | benchmark = SPY buy-and-hold`,
    yLabel: 'Equity (start = 1.0)',
    features: datedFeatures,
    series: [
      {
        label: 'VIXRegime Strategy',
        color: '#0b6e4f',
        points: datedFeatures.map((row) => ({ x: row.timestampMs, y: toNumber(obsByDate.get(row.tradeDateUtc)?.equityCurve) })),
        strokeWidth: 2.4,
      },
      {
        label: 'SPY Buy & Hold',
        color: '#2563eb',
        points: datedFeatures.map((row) => ({ x: row.timestampMs, y: toNumber(obsByDate.get(row.tradeDateUtc)?.benchmarkCurve) })),
        strokeWidth: 2.1,
      },
    ],
    markerSeries: [
      {
        values: datedFeatures.map((row) => toNumber(obsByDate.get(row.tradeDateUtc)?.equityCurve)),
      },
    ],
  });

  const vixSvg = makeSvgChart({
    title: 'Daily VIX Family Levels',
    subtitle: `${raw.startDate} to ${raw.endDate}`,
    yLabel: 'Index Level',
    features: datedFeatures,
    series: [
      {
        label: 'VIX1D',
        color: '#dc2626',
        points: datedFeatures.map((row) => ({ x: row.timestampMs, y: toNumber(row.vix1d) })),
        strokeWidth: 1.6,
      },
      {
        label: 'VIX',
        color: '#111827',
        points: datedFeatures.map((row) => ({ x: row.timestampMs, y: toNumber(row.vix) })),
        strokeWidth: 1.9,
      },
      {
        label: 'VIX9D',
        color: '#f59e0b',
        points: datedFeatures.map((row) => ({ x: row.timestampMs, y: toNumber(row.vix9d) })),
        strokeWidth: 1.6,
      },
      {
        label: 'VIX3M',
        color: '#7c3aed',
        points: datedFeatures.map((row) => ({ x: row.timestampMs, y: toNumber(row.vix3m) })),
        strokeWidth: 1.6,
      },
    ],
    markerSeries: [
      { values: datedFeatures.map((row) => toNumber(row.vix)) },
    ],
  });

  fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  const equityPath = path.join(OUTPUT_DIR, 'vixregime-vs-spy-daily-equity.svg');
  const vixPath = path.join(OUTPUT_DIR, 'vix-family-daily-levels.svg');
  fs.writeFileSync(equityPath, equitySvg, 'utf8');
  fs.writeFileSync(vixPath, vixSvg, 'utf8');

  const regimeEvaluation = evaluateRegimes(observations, DAILY_TOLERANCE_PCT);
  const summary = {
    startDate: raw.startDate,
    endDate: raw.endDate,
    dailyObservationStart: observations[0]?.tradeDateUtc || null,
    dailyObservationEnd: observations[observations.length - 1]?.tradeDateUtc || null,
    benchmark: 'SPY',
    regimeEvaluation,
    worstRelativeWindow: findWorstRelativeWindow(observations),
    plotPaths: {
      equity: equityPath,
      vixFamily: vixPath,
    },
  };
  const summaryPath = path.join(OUTPUT_DIR, 'vixregime-relative-window-summary.json');
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, 'utf8');
  console.log(JSON.stringify({ ...summary, summaryPath }, null, 2));
}

run();
