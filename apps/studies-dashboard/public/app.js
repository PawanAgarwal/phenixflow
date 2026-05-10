const state = {
  config: null,
  strategies: [],
  strategyId: null,
  summary: null,
  chart: null,
  values: [],
  latestPortfolio: null,
  weekPortfolios: [],
  loadToken: 0,
};

const els = {
  statusText: document.getElementById('statusText'),
  strategyTabs: document.getElementById('strategyTabs'),
  strategySelect: document.getElementById('strategySelect'),
  reloadButton: document.getElementById('reloadButton'),
  latestDate: document.getElementById('latestDate'),
  studyReturn: document.getElementById('studyReturn'),
  spyReturn: document.getElementById('spyReturn'),
  edgeReturn: document.getElementById('edgeReturn'),
  portfolioValue: document.getElementById('portfolioValue'),
  latestTurnover: document.getElementById('latestTurnover'),
  lastPortfolioReturn: document.getElementById('lastPortfolioReturn'),
  lastSpyReturn: document.getElementById('lastSpyReturn'),
  chartTitle: document.getElementById('chartTitle'),
  performanceChart: document.getElementById('performanceChart'),
  portfolioTitle: document.getElementById('portfolioTitle'),
  holdingCount: document.getElementById('holdingCount'),
  portfolioRows: document.getElementById('portfolioRows'),
  weekRange: document.getElementById('weekRange'),
  weekRows: document.getElementById('weekRows'),
};

function isFiniteNumber(value) {
  if (value === null || value === undefined || value === '') return false;
  return Number.isFinite(Number(value));
}

function formatPct(value, digits = 2) {
  if (!isFiniteNumber(value)) return '-';
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

function formatPctPoints(value, digits = 2) {
  if (!isFiniteNumber(value)) return '-';
  return `${Number(value).toFixed(digits)}%`;
}

function formatMoney(value) {
  if (!isFiniteNumber(value)) return '-';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(Number(value));
}

function valueClass(value) {
  if (!isFiniteNumber(value) || Math.abs(Number(value)) < 0.000001) return '';
  return Number(value) > 0 ? 'positive' : 'negative';
}

function clearDashboard() {
  [
    els.latestDate,
    els.studyReturn,
    els.spyReturn,
    els.edgeReturn,
    els.portfolioValue,
    els.latestTurnover,
    els.lastPortfolioReturn,
    els.lastSpyReturn,
    els.portfolioTitle,
    els.holdingCount,
    els.weekRange,
    els.chartTitle,
  ].forEach((element) => {
    element.textContent = '-';
    element.className = '';
  });
  els.portfolioRows.replaceChildren();
  els.weekRows.replaceChildren();
  const canvas = els.performanceChart;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

async function fetchJson(path, options = {}) {
  const { timeoutMs = 12000, ...fetchOptions } = options;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(path, { ...fetchOptions, signal: controller.signal });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error?.message || `request_failed:${response.status}`);
    return body;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error(`request_timeout:${path}`);
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

function selectedStrategy() {
  return state.strategies.find((strategy) => strategy.id === state.strategyId) || state.strategies[0] || null;
}

function completedWeekValues() {
  return state.values.slice(-7);
}

function latestCompletedValue() {
  return [...state.values].reverse().find((value) => isFiniteNumber(value.netReturn)) || null;
}

function latestSpySessionReturn(completedValue) {
  if (!completedValue) return null;
  const points = state.chart?.data || [];
  const currentIndex = points.findIndex((point) => point.signalDate === completedValue.date);
  if (currentIndex <= 0) return null;
  const previousSpy = points[currentIndex - 1]?.benchmarks?.spy;
  const currentSpy = points[currentIndex]?.benchmarks?.spy;
  if (!isFiniteNumber(previousSpy) || !isFiniteNumber(currentSpy)) return null;
  return ((1 + Number(currentSpy)) / (1 + Number(previousSpy))) - 1;
}

function renderStrategySelect() {
  const options = state.strategies.map((strategy) => {
    const option = document.createElement('option');
    option.value = strategy.id;
    option.textContent = strategy.displayName || strategy.name || strategy.id;
    option.selected = strategy.id === state.strategyId;
    return option;
  });
  els.strategySelect.replaceChildren(...options);
}

function switchStrategy(strategyId) {
  if (!strategyId || strategyId === state.strategyId) return;
  state.strategyId = strategyId;
  const nextUrl = new URL(window.location.href);
  nextUrl.searchParams.set('strategy', state.strategyId);
  window.history.replaceState(null, '', nextUrl);
  renderStrategySelect();
  renderStrategyTabs();
  loadStudy().catch((error) => {
    els.statusText.textContent = error.message;
  });
}

function renderStrategyTabs() {
  const tabs = state.strategies.map((strategy) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = strategy.id === state.strategyId ? 'study-tab active' : 'study-tab';
    button.textContent = strategy.displayName || strategy.name || strategy.id;
    button.setAttribute('aria-pressed', strategy.id === state.strategyId ? 'true' : 'false');
    button.addEventListener('click', () => switchStrategy(strategy.id));
    return button;
  });
  els.strategyTabs.replaceChildren(...tabs);
}

async function loadShell() {
  const [config, strategies] = await Promise.all([
    fetchJson('/api/dashboard/config'),
    fetchJson('/api/strategies'),
  ]);
  state.config = config;
  state.strategies = strategies.data || [];
  const params = new URLSearchParams(window.location.search);
  const requested = params.get('strategy');
  const defaultId = requested || config.defaultStrategyId || state.strategies[0]?.id;
  state.strategyId = state.strategies.some((strategy) => strategy.id === defaultId)
    ? defaultId
    : state.strategies[0]?.id || null;
  renderStrategySelect();
  renderStrategyTabs();
}

async function loadStudy() {
  if (!state.strategyId) throw new Error('No strategies available from strategy service');
  const token = state.loadToken + 1;
  state.loadToken = token;
  const strategyId = state.strategyId;
  const strategy = selectedStrategy();
  els.statusText.textContent = `Loading ${strategy?.name || strategyId}`;
  state.summary = null;
  state.chart = null;
  state.values = [];
  state.latestPortfolio = null;
  state.weekPortfolios = [];
  clearDashboard();
  let summary;
  let chart;
  let values;
  let latestPortfolio;
  try {
    [summary, chart, values, latestPortfolio] = await Promise.all([
      fetchJson(`/api/strategies/${strategyId}`),
      fetchJson(`/api/strategies/${strategyId}/chart`),
      fetchJson(`/api/strategies/${strategyId}/values?limit=12`),
      fetchJson(`/api/strategies/${strategyId}/portfolio/latest`),
    ]);
  } catch (error) {
    if (token !== state.loadToken || strategyId !== state.strategyId) return;
    throw error;
  }
  if (token !== state.loadToken || strategyId !== state.strategyId) return;
  state.summary = summary;
  state.chart = chart;
  state.values = values.data || [];
  state.latestPortfolio = latestPortfolio.data;
  state.weekPortfolios = [];
  renderDashboard();
  els.statusText.textContent = `Loaded ${summary.summary.latestRebalanceDate || 'latest'}`;

  const weekDates = completedWeekValues().map((value) => value.date);
  const weekPortfolios = await Promise.all(weekDates.map((date) => (
    fetchJson(`/api/strategies/${strategyId}/portfolio/${date}`, { timeoutMs: 6000 })
      .then((payload) => payload.data)
      .catch(() => null)
  )));
  if (token !== state.loadToken || strategyId !== state.strategyId) return;
  state.weekPortfolios = weekPortfolios.filter(Boolean);
  renderWeekChanges();
}

function renderDashboard() {
  const strategy = selectedStrategy();
  renderSummary(strategy);
  renderPortfolio();
  renderWeekChanges();
  renderChart();
}

function renderSummary(strategy) {
  const summary = state.summary.summary;
  const latest = state.latestPortfolio.snapshot;
  const latestChange = state.latestPortfolio.changeFromPrevious;
  const latestCompleted = latestCompletedValue();
  const spySessionReturn = latestSpySessionReturn(latestCompleted);
  const edge = isFiniteNumber(summary.totalReturn) && isFiniteNumber(summary.spyReturn)
    ? summary.totalReturn - summary.spyReturn
    : null;
  els.latestDate.textContent = summary.latestRebalanceDate || '-';
  els.studyReturn.textContent = formatPct(summary.totalReturn);
  els.studyReturn.className = valueClass(summary.totalReturn);
  els.spyReturn.textContent = formatPct(summary.spyReturn);
  els.spyReturn.className = valueClass(summary.spyReturn);
  els.edgeReturn.textContent = formatPct(edge);
  els.edgeReturn.className = valueClass(edge);
  els.portfolioValue.textContent = formatMoney(latest.equityBeforeNextSession);
  els.latestTurnover.textContent = formatPctPoints(latestChange.turnoverPct);
  els.lastPortfolioReturn.textContent = latestCompleted ? formatPct(latestCompleted.netReturn) : '-';
  els.lastPortfolioReturn.className = valueClass(latestCompleted?.netReturn);
  els.lastSpyReturn.textContent = formatPct(spySessionReturn);
  els.lastSpyReturn.className = valueClass(spySessionReturn);
  els.chartTitle.textContent = `${strategy?.name || 'Study'} vs SPY`;
}

function weightBar(holding) {
  const value = Math.max(0, Number(holding.weightPct || 0));
  return `
    <div class="weight-bar" style="--weight-width: ${Math.min(100, value)}%">
      <span></span>
      <strong>${formatPctPoints(holding.weightPct, 2)}</strong>
    </div>
  `;
}

function latestPortfolioRows(snapshot, changeFromPrevious) {
  const currentRows = (snapshot.holdings || []).map((holding) => ({
    ...holding,
    status: 'current',
  }));
  const currentTickers = new Set(currentRows.map((holding) => holding.ticker));
  const removedRows = (changeFromPrevious?.removed || [])
    .filter((change) => !currentTickers.has(change.ticker))
    .map((change) => ({
      ticker: change.ticker,
      weight: 0,
      weightPct: 0,
      previousWeight: change.previousWeight,
      weightChange: change.weightChange,
      weightChangePct: change.weightChangePct,
      dollars: 0,
      status: 'removed',
    }));
  return [...currentRows, ...removedRows];
}

function holdingCountLabel(currentCount, removedCount) {
  const holdings = currentCount === 1 ? '1 holding' : `${currentCount} holdings`;
  if (!removedCount) return holdings;
  const exits = removedCount === 1 ? '1 exit' : `${removedCount} exits`;
  return `${holdings} + ${exits}`;
}

function renderPortfolio() {
  const { snapshot, changeFromPrevious } = state.latestPortfolio;
  const displayHoldings = latestPortfolioRows(snapshot, changeFromPrevious);
  const removedCount = displayHoldings.filter((holding) => holding.status === 'removed').length;
  els.portfolioTitle.textContent = snapshot.date || '-';
  els.holdingCount.textContent = holdingCountLabel(snapshot.holdings.length, removedCount);
  const rows = displayHoldings.map((holding) => {
    const tr = document.createElement('tr');
    if (holding.status === 'removed') tr.className = 'removed-holding';
    const change = Number(holding.weightChangePct || 0);
    tr.innerHTML = `
      <td>
        <strong>${holding.ticker}</strong>
        ${holding.status === 'removed' ? '<span>closed</span>' : ''}
      </td>
      <td>${weightBar(holding)}</td>
      <td class="num ${valueClass(change)}">${formatPctPoints(change, 2)}</td>
      <td class="num">${formatMoney(holding.dollars)}</td>
    `;
    return tr;
  });
  els.portfolioRows.replaceChildren(...rows);
}

function topMovesFor(portfolio, limit = 3) {
  return (portfolio?.changeFromPrevious?.changes || [])
    .filter((change) => Math.abs(change.weightChangePct || 0) > 0.0001)
    .slice(0, limit)
    .map((change) => `${change.ticker} ${formatPctPoints(change.weightChangePct, 1)}`);
}

function portfolioByDate(date) {
  return state.weekPortfolios.find((portfolio) => portfolio.snapshot.date === date) || null;
}

function renderWeekChanges() {
  const week = completedWeekValues();
  const first = week[0]?.date || null;
  const last = week.at(-1)?.date || null;
  els.weekRange.textContent = first && last ? `${first} to ${last}` : '-';
  const rows = week.map((value) => {
    const portfolio = portfolioByDate(value.date);
    const moves = topMovesFor(portfolio);
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>
        <strong>${value.date}</strong>
        <span>${value.nextDate || 'open target'}</span>
      </td>
      <td class="num ${valueClass(value.netReturn)}">${isFiniteNumber(value.netReturn) ? formatPct(value.netReturn) : 'Open'}</td>
      <td class="num">${formatPctPoints(portfolio?.changeFromPrevious?.turnoverPct ?? value.turnoverPct)}</td>
      <td>${moves.length ? moves.join(', ') : 'No target change'}</td>
    `;
    return tr;
  });
  els.weekRows.replaceChildren(...rows);
}

function chartPoints() {
  return (state.chart?.data || [])
    .filter((point) => isFiniteNumber(point.totalReturn) && isFiniteNumber(point.benchmarks?.spy))
    .map((point) => ({
      date: point.date,
      strategy: point.totalReturn,
      spy: point.benchmarks.spy,
    }));
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
  ctx.lineWidth = 2.5;
  ctx.stroke();
}

function renderChart() {
  const canvas = els.performanceChart;
  const rect = canvas.getBoundingClientRect();
  const pixelRatio = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.floor(rect.width * pixelRatio));
  canvas.height = Math.max(1, Math.floor(rect.height * pixelRatio));
  const ctx = canvas.getContext('2d');
  ctx.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);
  const width = rect.width;
  const height = rect.height;
  ctx.clearRect(0, 0, width, height);

  const points = chartPoints();
  if (points.length < 2) return;

  const margin = { top: 18, right: 24, bottom: 30, left: 58 };
  const values = points.flatMap((point) => [point.strategy, point.spy]);
  const min = Math.min(0, ...values);
  const max = Math.max(0.01, ...values);
  const span = Math.max(0.01, max - min);
  const yMin = min - span * 0.12;
  const yMax = max + span * 0.12;
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const xScale = (index) => margin.left + (index / (points.length - 1)) * plotWidth;
  const yScale = (value) => margin.top + ((yMax - value) / (yMax - yMin)) * plotHeight;

  ctx.strokeStyle = '#d9dfd7';
  ctx.fillStyle = '#66736f';
  ctx.font = '12px Inter, ui-sans-serif, system-ui';
  ctx.lineWidth = 1;
  for (let index = 0; index <= 4; index += 1) {
    const value = yMin + ((yMax - yMin) * index / 4);
    const y = yScale(value);
    ctx.beginPath();
    ctx.moveTo(margin.left, y);
    ctx.lineTo(width - margin.right, y);
    ctx.stroke();
    ctx.fillText(formatPct(value, 0), 10, y + 4);
  }

  drawLine(ctx, points.map((point) => point.strategy), xScale, yScale, '#1d7d62');
  drawLine(ctx, points.map((point) => point.spy), xScale, yScale, '#305fb8');

  const latest = points.at(-1);
  ctx.fillStyle = '#1c2527';
  ctx.fillText(points[0].date, margin.left, height - 9);
  ctx.textAlign = 'right';
  ctx.fillText(latest.date, width - margin.right, height - 9);
  ctx.textAlign = 'left';
}

async function boot() {
  await loadShell();
  await loadStudy();
}

els.reloadButton.addEventListener('click', () => {
  els.reloadButton.disabled = true;
  loadStudy()
    .catch((error) => {
      els.statusText.textContent = error.message;
    })
    .finally(() => {
      els.reloadButton.disabled = false;
    });
});

els.strategySelect.addEventListener('change', () => {
  switchStrategy(els.strategySelect.value);
});

window.addEventListener('resize', () => renderChart());

boot().catch((error) => {
  els.statusText.textContent = error.message;
});
