const state = {
  summary: null,
  days: [],
  equity: [],
  selectedDate: null,
  selectedSnapshot: null,
};

const els = {
  dataStatus: document.getElementById('dataStatus'),
  recomputeButton: document.getElementById('recomputeButton'),
  refreshButton: document.getElementById('refreshButton'),
  latestDate: document.getElementById('latestDate'),
  strategyReturn: document.getElementById('strategyReturn'),
  spyReturn: document.getElementById('spyReturn'),
  qqqReturn: document.getElementById('qqqReturn'),
  maxDrawdown: document.getElementById('maxDrawdown'),
  dateFilter: document.getElementById('dateFilter'),
  dayRows: document.getElementById('dayRows'),
  selectedTitle: document.getElementById('selectedTitle'),
  selectedEquity: document.getElementById('selectedEquity'),
  selectedTurnover: document.getElementById('selectedTurnover'),
  selectedNextReturn: document.getElementById('selectedNextReturn'),
  equityChart: document.getElementById('equityChart'),
  holdingCount: document.getElementById('holdingCount'),
  holdingRows: document.getElementById('holdingRows'),
  sessionDate: document.getElementById('sessionDate'),
  grossExposure: document.getElementById('grossExposure'),
  estimatedCost: document.getElementById('estimatedCost'),
  nextClose: document.getElementById('nextClose'),
  realizedNet: document.getElementById('realizedNet'),
  endEquity: document.getElementById('endEquity'),
};

function formatPct(value, digits = 2) {
  if (!Number.isFinite(value)) return '-';
  return `${(value * 100).toFixed(digits)}%`;
}

function formatPctRaw(value, digits = 2) {
  if (!Number.isFinite(value)) return '-';
  return `${value.toFixed(digits)}%`;
}

function formatMoney(value) {
  if (!Number.isFinite(value)) return '-';
  return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(value);
}

function cssClassFor(value) {
  if (!Number.isFinite(value) || Math.abs(value) < 0.000001) return '';
  return value > 0 ? 'positive' : 'negative';
}

async function fetchJson(url, options) {
  const response = await fetch(url, options);
  const body = await response.json();
  if (!response.ok) throw new Error(body.error?.message || `request_failed:${response.status}`);
  return body;
}

async function loadAll({ keepSelection = false } = {}) {
  els.dataStatus.textContent = 'Loading';
  const [summary, days, equity] = await Promise.all([
    fetchJson('/api/rebalance/summary'),
    fetchJson('/api/rebalance/days?limit=5000'),
    fetchJson('/api/rebalance/equity'),
  ]);
  state.summary = summary;
  state.days = days.data;
  state.equity = equity.data;
  const nextDate = keepSelection && state.selectedDate ? state.selectedDate : summary.summary.latestRebalanceDate;
  renderSummary();
  renderDays();
  await selectDate(nextDate);
  renderChart();
  els.dataStatus.textContent = `Loaded ${summary.summary.latestRebalanceDate}`;
}

function renderSummary() {
  const summary = state.summary.summary;
  els.latestDate.textContent = summary.latestRebalanceDate || '-';
  els.strategyReturn.textContent = formatPct(summary.totalReturn);
  els.strategyReturn.className = cssClassFor(summary.totalReturn);
  els.spyReturn.textContent = formatPct(summary.spyReturn);
  els.spyReturn.className = cssClassFor(summary.spyReturn);
  els.qqqReturn.textContent = formatPct(summary.qqqReturn);
  els.qqqReturn.className = cssClassFor(summary.qqqReturn);
  els.maxDrawdown.textContent = formatPct(summary.maxDrawdown);
  els.maxDrawdown.className = cssClassFor(summary.maxDrawdown);
  els.dateFilter.max = summary.latestRebalanceDate || '';
}

function dayRow(day) {
  const row = document.createElement('tr');
  row.dataset.date = day.date;
  if (day.date === state.selectedDate) row.classList.add('selected');
  const netClass = cssClassFor(day.netReturn);
  row.innerHTML = `
    <td>
      <strong>${day.date}</strong>
      <div class="muted">${day.nextDate || 'latest'}</div>
    </td>
    <td class="num ${netClass}">${formatPct(day.netReturn)}</td>
    <td>${day.topHoldings || '-'}</td>
  `;
  row.addEventListener('click', () => selectDate(day.date));
  return row;
}

function renderDays() {
  const filter = els.dateFilter.value;
  const rows = filter ? state.days.filter((day) => day.date >= filter) : state.days;
  els.dayRows.replaceChildren(...rows.map(dayRow));
}

async function selectDate(date) {
  if (!date) return;
  const body = await fetchJson(`/api/rebalance/days/${date}`);
  state.selectedDate = date;
  state.selectedSnapshot = body.data;
  renderDays();
  renderSelected();
  renderChart();
}

function renderSelected() {
  const snapshot = state.selectedSnapshot;
  if (!snapshot) return;
  const realized = snapshot.realized;
  els.selectedTitle.textContent = snapshot.date;
  els.selectedEquity.textContent = formatMoney(snapshot.equityBeforeNextSession);
  els.selectedTurnover.textContent = `Turnover ${formatPct(snapshot.turnover)}`;
  els.selectedNextReturn.textContent = realized ? `Net ${formatPct(realized.netReturn)}` : 'Open';
  els.selectedNextReturn.className = realized ? cssClassFor(realized.netReturn) : '';
  els.holdingCount.textContent = `${snapshot.holdings.length} holdings`;
  els.sessionDate.textContent = snapshot.nextDate || 'latest';
  els.grossExposure.textContent = formatPct(snapshot.grossExposure);
  els.estimatedCost.textContent = formatMoney(snapshot.estimatedRebalanceCost);
  els.nextClose.textContent = snapshot.nextDate || '-';
  els.realizedNet.textContent = realized ? formatPct(realized.netReturn) : '-';
  els.realizedNet.className = realized ? cssClassFor(realized.netReturn) : '';
  els.endEquity.textContent = realized ? formatMoney(realized.endEquity) : '-';
  renderHoldings(snapshot.holdings);
}

function renderHoldings(holdings) {
  const maxWeight = Math.max(...holdings.map((holding) => holding.weight), 0.01);
  const rows = holdings.map((holding) => {
    const tr = document.createElement('tr');
    const changeClass = cssClassFor(holding.weightChange);
    const width = Math.max(4, (holding.weight / maxWeight) * 100);
    tr.innerHTML = `
      <td><strong>${holding.ticker}</strong></td>
      <td>
        <div class="holding-row-bar" style="--bar-width: ${width}%">
          <span></span>
          <strong>${formatPctRaw(holding.weightPct, 4)}</strong>
        </div>
      </td>
      <td class="num ${changeClass}">${formatPctRaw(holding.weightChangePct, 4)}</td>
      <td class="num">${formatMoney(holding.dollars)}</td>
    `;
    return tr;
  });
  els.holdingRows.replaceChildren(...rows);
}

function chartSeries() {
  return state.equity.map((point) => ({
    date: point.date,
    strategy: point.totalReturn,
    spy: point.spyReturn,
    qqq: point.qqqReturn,
  })).filter((point) => Number.isFinite(point.strategy));
}

function drawLine(ctx, points, xScale, yScale, color) {
  ctx.beginPath();
  points.forEach((point, index) => {
    const x = xScale(index);
    const y = yScale(point);
    if (index === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = color;
  ctx.lineWidth = 2;
  ctx.stroke();
}

function renderChart() {
  const canvas = els.equityChart;
  const rect = canvas.getBoundingClientRect();
  const pixelRatio = window.devicePixelRatio || 1;
  canvas.width = Math.floor(rect.width * pixelRatio);
  canvas.height = Math.floor(rect.height * pixelRatio);
  const ctx = canvas.getContext('2d');
  ctx.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
  const width = rect.width;
  const height = rect.height;
  ctx.clearRect(0, 0, width, height);
  const series = chartSeries();
  if (series.length < 2) return;
  const margin = { top: 22, right: 22, bottom: 34, left: 54 };
  const values = series.flatMap((point) => [point.strategy, point.spy, point.qqq]).filter(Number.isFinite);
  const min = Math.min(...values, 0);
  const max = Math.max(...values, 0.01);
  const span = Math.max(0.01, max - min);
  const yMin = min - (span * 0.12);
  const yMax = max + (span * 0.12);
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const xScale = (index) => margin.left + ((index / (series.length - 1)) * plotWidth);
  const yScale = (value) => margin.top + ((yMax - value) / (yMax - yMin)) * plotHeight;

  ctx.strokeStyle = '#d8ddd7';
  ctx.lineWidth = 1;
  ctx.font = '12px Inter, sans-serif';
  ctx.fillStyle = '#65716d';
  for (let i = 0; i <= 4; i += 1) {
    const value = yMin + ((yMax - yMin) * i / 4);
    const y = yScale(value);
    ctx.beginPath();
    ctx.moveTo(margin.left, y);
    ctx.lineTo(width - margin.right, y);
    ctx.stroke();
    ctx.fillText(formatPct(value, 0), 8, y + 4);
  }
  drawLine(ctx, series.map((point) => point.strategy), xScale, yScale, '#267a5b');
  drawLine(ctx, series.map((point) => point.spy), xScale, yScale, '#2f62b3');
  drawLine(ctx, series.map((point) => point.qqq), xScale, yScale, '#a66a15');

  const selectedIndex = series.findIndex((point) => point.date === state.selectedSnapshot?.realized?.date);
  if (selectedIndex >= 0) {
    const x = xScale(selectedIndex);
    ctx.strokeStyle = '#1f2728';
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    ctx.moveTo(x, margin.top);
    ctx.lineTo(x, height - margin.bottom);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  const legend = [
    ['Strategy', '#267a5b'],
    ['SPY', '#2f62b3'],
    ['QQQ', '#a66a15'],
  ];
  legend.forEach(([label, color], index) => {
    const x = margin.left + (index * 100);
    ctx.fillStyle = color;
    ctx.fillRect(x, height - 22, 16, 3);
    ctx.fillStyle = '#1f2728';
    ctx.fillText(label, x + 22, height - 17);
  });
}

async function recompute() {
  els.recomputeButton.disabled = true;
  els.dataStatus.textContent = 'Recomputing';
  try {
    await fetchJson('/api/rebalance/recompute', { method: 'POST' });
    await loadAll({ keepSelection: true });
  } finally {
    els.recomputeButton.disabled = false;
  }
}

async function refreshEod() {
  els.refreshButton.disabled = true;
  els.dataStatus.textContent = 'Refreshing EOD';
  try {
    await fetchJson('/api/rebalance/refresh-eod', { method: 'POST' });
    await pollRefresh();
  } finally {
    els.refreshButton.disabled = false;
  }
}

async function pollRefresh() {
  for (let index = 0; index < 120; index += 1) {
    await new Promise((resolve) => setTimeout(resolve, 1500));
    const summary = await fetchJson('/api/rebalance/summary');
    if (!summary.refresh.running) {
      await loadAll();
      return;
    }
    els.dataStatus.textContent = 'Refreshing EOD';
  }
}

els.dateFilter.addEventListener('change', renderDays);
els.recomputeButton.addEventListener('click', () => recompute().catch((error) => {
  els.dataStatus.textContent = error.message;
}));
els.refreshButton.addEventListener('click', () => refreshEod().catch((error) => {
  els.dataStatus.textContent = error.message;
  els.refreshButton.disabled = false;
}));
window.addEventListener('resize', () => renderChart());

loadAll().catch((error) => {
  els.dataStatus.textContent = error.message;
});
