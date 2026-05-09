#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { artifactPath, ensureDir } = require('../src/config');

function parseArgs(argv) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--suite') out.suitePath = argv[++index];
    else if (arg === '--strategy') out.strategyId = argv[++index];
    else if (arg === '--benchmark') out.benchmarkId = argv[++index];
    else if (arg === '--output') out.outputPath = argv[++index];
  }
  return out;
}

function escapeXml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

function pct(value) {
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
}

function money(value) {
  return `$${Math.round(value).toLocaleString('en-US')}`;
}

function timestamp(date) {
  return Date.parse(`${date}T00:00:00.000Z`);
}

function strategyById(suite, id) {
  const strategy = suite.strategies.find((item) => item.summary.id === id);
  if (!strategy) throw new Error(`missing_strategy:${id}`);
  return strategy;
}

function buildSeries(strategy) {
  const startDate = strategy.summary.firstSignalDate;
  const initialEquity = 10000;
  return [
    { date: startDate, equity: initialEquity },
    ...strategy.equityCurve.map((point) => ({ date: point.date, equity: point.equity })),
  ];
}

function pathForSeries(series, xScale, yScale) {
  return series.map((point, index) => {
    const command = index === 0 ? 'M' : 'L';
    return `${command}${xScale(timestamp(point.date)).toFixed(1)},${yScale(point.equity).toFixed(1)}`;
  }).join(' ');
}

function nearestPoint(series, date) {
  let selected = series[0];
  for (const point of series) {
    if (point.date <= date) selected = point;
    else break;
  }
  return selected;
}

function yTicks(minY, maxY, count = 6) {
  const span = maxY - minY;
  const rough = span / Math.max(1, count - 1);
  const pow = 10 ** Math.floor(Math.log10(rough));
  const step = [1, 2, 2.5, 5, 10].map((factor) => factor * pow)
    .find((candidate) => candidate >= rough) || (10 * pow);
  const start = Math.floor(minY / step) * step;
  const ticks = [];
  for (let value = start; value <= maxY + step; value += step) {
    if (value >= minY - step * 0.1 && value <= maxY + step * 0.1) ticks.push(value);
  }
  return ticks;
}

function monthTicks(startDate, endDate) {
  const ticks = [];
  const start = new Date(`${startDate.slice(0, 7)}-01T00:00:00.000Z`);
  const end = new Date(`${endDate.slice(0, 7)}-01T00:00:00.000Z`);
  for (let current = start; current <= end; current.setUTCMonth(current.getUTCMonth() + 2)) {
    const iso = current.toISOString().slice(0, 10);
    ticks.push({ date: iso, label: iso.slice(0, 7) });
  }
  return ticks;
}

function metricLine(summary) {
  return `${pct(summary.totalReturnPct)} return, ${pct(summary.maxDrawdownPct)} max DD, Sharpe ${summary.sharpe.toFixed(3)}`;
}

function drawChart({ suitePath, strategyId, benchmarkId, outputPath }) {
  const suite = JSON.parse(fs.readFileSync(suitePath, 'utf8'));
  const strategy = strategyById(suite, strategyId);
  const benchmark = strategyById(suite, benchmarkId);
  const strategySeries = buildSeries(strategy);
  const benchmarkSeries = buildSeries(benchmark);
  const allPoints = [...strategySeries, ...benchmarkSeries];
  const startDate = strategy.summary.firstSignalDate;
  const endDate = strategy.summary.lastRealizedDate;
  const minX = timestamp(startDate);
  const maxX = timestamp(endDate);
  const minEquity = Math.min(...allPoints.map((point) => point.equity), 10000) * 0.96;
  const maxEquity = Math.max(...allPoints.map((point) => point.equity), 10000) * 1.05;
  const width = 1280;
  const height = 760;
  const margin = { top: 80, right: 280, bottom: 88, left: 92 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const xScale = (value) => margin.left + ((value - minX) / (maxX - minX)) * plotWidth;
  const yScale = (value) => margin.top + (1 - ((value - minEquity) / (maxEquity - minEquity))) * plotHeight;
  const months = monthTicks(startDate, endDate);
  const ticks = yTicks(minEquity, maxEquity);
  const strategyPath = pathForSeries(strategySeries, xScale, yScale);
  const benchmarkPath = pathForSeries(benchmarkSeries, xScale, yScale);
  const markerDates = ['2025-01-02', '2025-04-01', '2025-07-01', '2025-10-01', '2026-01-02', '2026-04-01', endDate];
  const markers = markerDates.map((date) => ({
    date,
    strategy: nearestPoint(strategySeries, date),
    benchmark: nearestPoint(benchmarkSeries, date),
  }));
  const displayStrategyName = strategy.summary.name.replace(/^Grid /, '');

  const svg = `<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-labelledby="title desc">
  <title id="title">${escapeXml(displayStrategyName)} versus ${escapeXml(benchmark.summary.name)}</title>
  <desc id="desc">Equity curve from January 2025 onward comparing ${escapeXml(displayStrategyName)} to ${escapeXml(benchmark.summary.name)}.</desc>
  <rect width="${width}" height="${height}" fill="#f7f4ee"/>
  <text x="${margin.left}" y="38" font-family="Inter, Arial, sans-serif" font-size="24" font-weight="700" fill="#17202a">${escapeXml(displayStrategyName)} vs ${escapeXml(benchmark.summary.name)}</text>
  <text x="${margin.left}" y="64" font-family="Inter, Arial, sans-serif" font-size="13" fill="#53616f">Initial equity $10,000. First trading day ${startDate}; last realized date ${endDate}. Signal uses same-day 1m option-flow features, then EOD ETF rebalance.</text>

  <rect x="${margin.left}" y="${margin.top}" width="${plotWidth}" height="${plotHeight}" fill="#ffffff" stroke="#d7d3c8"/>
  ${ticks.map((tick) => `
  <line x1="${margin.left}" x2="${margin.left + plotWidth}" y1="${yScale(tick).toFixed(1)}" y2="${yScale(tick).toFixed(1)}" stroke="#ebe6dc"/>
  <text x="${margin.left - 12}" y="${(yScale(tick) + 4).toFixed(1)}" text-anchor="end" font-family="Inter, Arial, sans-serif" font-size="12" fill="#5b6670">${money(tick)}</text>`).join('')}
  ${months.map((tick) => `
  <line x1="${xScale(timestamp(tick.date)).toFixed(1)}" x2="${xScale(timestamp(tick.date)).toFixed(1)}" y1="${margin.top}" y2="${margin.top + plotHeight}" stroke="#f0ece4"/>
  <text x="${xScale(timestamp(tick.date)).toFixed(1)}" y="${height - 48}" text-anchor="middle" font-family="Inter, Arial, sans-serif" font-size="11" fill="#5b6670">${escapeXml(tick.label)}</text>`).join('')}

  <path d="${benchmarkPath}" fill="none" stroke="#6b7280" stroke-width="3"/>
  <path d="${strategyPath}" fill="none" stroke="#167f72" stroke-width="4"/>

  ${markers.map((marker) => `
  <circle cx="${xScale(timestamp(marker.strategy.date)).toFixed(1)}" cy="${yScale(marker.strategy.equity).toFixed(1)}" r="3.5" fill="#167f72"/>
  <circle cx="${xScale(timestamp(marker.benchmark.date)).toFixed(1)}" cy="${yScale(marker.benchmark.equity).toFixed(1)}" r="3" fill="#6b7280"/>`).join('')}

  <line x1="${margin.left}" x2="${margin.left + plotWidth}" y1="${yScale(10000).toFixed(1)}" y2="${yScale(10000).toFixed(1)}" stroke="#c6bfb2" stroke-dasharray="6 6"/>
  <text x="${margin.left + plotWidth - 6}" y="${(yScale(10000) - 8).toFixed(1)}" text-anchor="end" font-family="Inter, Arial, sans-serif" font-size="12" fill="#746b5c">Initial $10,000</text>

  <g transform="translate(${margin.left + plotWidth + 32}, ${margin.top + 8})">
    <text x="0" y="0" font-family="Inter, Arial, sans-serif" font-size="15" font-weight="700" fill="#17202a">Legend</text>
    <line x1="0" x2="34" y1="28" y2="28" stroke="#167f72" stroke-width="4"/>
    <text x="44" y="33" font-family="Inter, Arial, sans-serif" font-size="13" font-weight="700" fill="#17202a">${escapeXml(displayStrategyName)}</text>
    <text x="44" y="53" font-family="Inter, Arial, sans-serif" font-size="12" fill="#53616f">${escapeXml(metricLine(strategy.summary))}</text>
    <text x="44" y="73" font-family="Inter, Arial, sans-serif" font-size="12" fill="#53616f">Final equity ${money(strategy.summary.finalEquity)}</text>

    <line x1="0" x2="34" y1="112" y2="112" stroke="#6b7280" stroke-width="3"/>
    <text x="44" y="117" font-family="Inter, Arial, sans-serif" font-size="13" font-weight="700" fill="#17202a">${escapeXml(benchmark.summary.name)}</text>
    <text x="44" y="137" font-family="Inter, Arial, sans-serif" font-size="12" fill="#53616f">${escapeXml(metricLine(benchmark.summary))}</text>
    <text x="44" y="157" font-family="Inter, Arial, sans-serif" font-size="12" fill="#53616f">Final equity ${money(benchmark.summary.finalEquity)}</text>

    <text x="0" y="212" font-family="Inter, Arial, sans-serif" font-size="13" font-weight="700" fill="#17202a">Checkpoint Values</text>
    ${markers.map((marker, index) => `
    <text x="0" y="${238 + (index * 22)}" font-family="Inter, Arial, sans-serif" font-size="11" fill="#53616f">${escapeXml(marker.date)}</text>
    <text x="86" y="${238 + (index * 22)}" font-family="Inter, Arial, sans-serif" font-size="11" fill="#167f72">${money(marker.strategy.equity)}</text>
    <text x="162" y="${238 + (index * 22)}" font-family="Inter, Arial, sans-serif" font-size="11" fill="#6b7280">${money(marker.benchmark.equity)}</text>`).join('')}
  </g>

  <text x="${margin.left}" y="${height - 18}" font-family="Inter, Arial, sans-serif" font-size="11" fill="#6d7680">Source: Massive adjusted EOD ETF bars plus Massive 1m SPY option aggregate bars. Option data is a signal only; portfolio trades ETFs.</text>
</svg>
`;

  ensureDir(path.dirname(outputPath));
  fs.writeFileSync(outputPath, svg, 'utf8');
  const htmlPath = outputPath.replace(/\.svg$/, '.html');
  fs.writeFileSync(htmlPath, `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>${escapeXml(displayStrategyName)} vs ${escapeXml(benchmark.summary.name)}</title>
  <style>
    body { margin: 0; background: #f7f4ee; }
    .wrap { width: min(1280px, 100vw); margin: 0 auto; }
    svg { display: block; width: 100%; height: auto; }
  </style>
</head>
<body>
  <div class="wrap">
${svg.replace(/^<\?xml[^>]+>\n/, '')}
  </div>
</body>
</html>
`, 'utf8');
  return { outputPath, htmlPath, strategy: strategy.summary, benchmark: benchmark.summary };
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const suitePath = path.resolve(args.suitePath || artifactPath('pym-v5-option-overlay-suite-grid-full-2025-01-02-2026-05-06.json'));
  const strategyId = args.strategyId || 'grid_pym_spy_put_z2p5_to_bil';
  const benchmarkId = args.benchmarkId || 'spy_buy_hold';
  const outputPath = path.resolve(args.outputPath || artifactPath('pym-v5-spy-put-z2p5-vs-spy-2025-01-02-2026-05-07.svg'));
  const result = drawChart({ suitePath, strategyId, benchmarkId, outputPath });
  console.log(JSON.stringify({
    outputPath: result.outputPath,
    htmlPath: result.htmlPath,
    strategyReturnPct: result.strategy.totalReturnPct,
    benchmarkReturnPct: result.benchmark.totalReturnPct,
  }, null, 2));
}

main();
