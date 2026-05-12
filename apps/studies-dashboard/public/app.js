const state = {
  config: null,
  strategies: [],
  strategyId: null,
  summary: null,
  chart: null,
  values: [],
  latestPortfolio: null,
  weekPortfolios: [],
  trades: [],
  loadToken: 0,
};

const els = {
  statusText: document.getElementById('statusText'),
  strategyTabs: document.getElementById('strategyTabs'),
  strategySelect: document.getElementById('strategySelect'),
  reloadButton: document.getElementById('reloadButton'),
  strategyInfoTitle: document.getElementById('strategyInfoTitle'),
  strategyInfoDescription: document.getElementById('strategyInfoDescription'),
  strategyRule: document.getElementById('strategyRule'),
  strategyLinks: document.getElementById('strategyLinks'),
  latestDate: document.getElementById('latestDate'),
  studyReturn: document.getElementById('studyReturn'),
  spyReturn: document.getElementById('spyReturn'),
  edgeReturn: document.getElementById('edgeReturn'),
  sharpeMetric: document.getElementById('sharpeMetric'),
  maxDrawdown: document.getElementById('maxDrawdown'),
  hitRate: document.getElementById('hitRate'),
  tradeCount: document.getElementById('tradeCount'),
  portfolioValue: document.getElementById('portfolioValue'),
  latestTurnover: document.getElementById('latestTurnover'),
  lastPortfolioReturn: document.getElementById('lastPortfolioReturn'),
  lastSpyReturn: document.getElementById('lastSpyReturn'),
  tradesPanel: document.getElementById('tradesPanel'),
  tradesBadge: document.getElementById('tradesBadge'),
  tradesRows: document.getElementById('tradesRows'),
  openPositionsPanel: document.getElementById('openPositionsPanel'),
  openPositionsBadge: document.getElementById('openPositionsBadge'),
  openPositionsRows: document.getElementById('openPositionsRows'),
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
    els.sharpeMetric,
    els.maxDrawdown,
    els.hitRate,
    els.tradeCount,
    els.portfolioValue,
    els.latestTurnover,
    els.lastPortfolioReturn,
    els.lastSpyReturn,
    els.portfolioTitle,
    els.holdingCount,
    els.weekRange,
    els.chartTitle,
    els.tradesBadge,
  ].forEach((element) => {
    if (!element) return;
    element.textContent = '-';
    element.className = '';
  });
  els.portfolioRows.replaceChildren();
  els.weekRows.replaceChildren();
  if (els.tradesRows) els.tradesRows.replaceChildren();
  if (els.tradesPanel) els.tradesPanel.hidden = true;
  if (els.openPositionsRows) els.openPositionsRows.replaceChildren();
  if (els.openPositionsPanel) els.openPositionsPanel.hidden = true;
  const canvas = els.performanceChart;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, canvas.width, canvas.height);
}

function activeStrategyMetadata(strategy) {
  return state.summary?.metadata || strategy || selectedStrategy();
}

function renderStrategyInfo(strategy) {
  const metadata = activeStrategyMetadata(strategy);
  els.strategyInfoTitle.textContent = metadata?.displayName || metadata?.name || metadata?.id || '-';
  els.strategyInfoDescription.textContent = metadata?.description || metadata?.strategySource || '-';

  const ruleLines = Array.isArray(metadata?.ruleSummary) ? metadata.ruleSummary.filter(Boolean) : [];
  const ruleNodes = ruleLines.map((line) => {
    const item = document.createElement('code');
    item.textContent = line;
    return item;
  });
  els.strategyRule.replaceChildren(...ruleNodes);

  const links = Array.isArray(metadata?.sourceLinks) ? metadata.sourceLinks.filter((link) => link?.href) : [];
  const linkNodes = links.map((link) => {
    const anchor = document.createElement('a');
    anchor.href = link.href;
    anchor.target = '_blank';
    anchor.rel = 'noreferrer';
    anchor.textContent = link.label || 'Source';
    return anchor;
  });
  els.strategyLinks.replaceChildren(...linkNodes);
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
  renderStrategyInfo(strategy);
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
  // Optional: fetch trades if strategy advertises support. Don't block dashboard render on it.
  state.trades = [];
  state.openPositions = [];
  const supports = Array.isArray(summary?.metadata?.supports) ? summary.metadata.supports : [];
  const supportsTrades = supports.includes('trade_log');
  const supportsOpen = supports.includes('open_positions');
  if (supportsTrades) {
    fetchJson(`/api/strategies/${strategyId}/trades?limit=30`, { timeoutMs: 6000 })
      .then((payload) => {
        if (token !== state.loadToken || strategyId !== state.strategyId) return;
        state.trades = payload.data || [];
        renderTrades();
      })
      .catch(() => { /* trades are optional */ });
  }
  if (supportsOpen) {
    fetchJson(`/api/strategies/${strategyId}/open-positions`, { timeoutMs: 6000 })
      .then((payload) => {
        if (token !== state.loadToken || strategyId !== state.strategyId) return;
        state.openPositions = payload.data || [];
        renderOpenPositions();
      })
      .catch(() => { /* open positions are optional */ });
  }
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
  renderStrategyInfo(strategy);
  renderSummary(strategy);
  renderPortfolio();
  renderWeekChanges();
  renderOpenPositions();
  renderTrades();
  renderChart();
}

function renderTrades() {
  if (!els.tradesPanel) return;
  const trades = Array.isArray(state.trades) ? state.trades : [];
  if (trades.length === 0) {
    els.tradesPanel.hidden = true;
    return;
  }
  els.tradesPanel.hidden = false;
  els.tradesBadge.textContent = `${trades.length} most recent`;
  // Most recent first
  const rows = trades.slice().reverse().map((trade) => {
    const tr = document.createElement('tr');
    const sideClass = trade.side === 'LONG' ? 'positive' : trade.side === 'SHORT' ? 'negative' : '';
    const netClass = valueClass(trade.netReturn);
    const grossClass = valueClass(trade.grossReturn);
    // Carry-over: entry was on a different (prior) date — flagged explicitly by the strategy.
    const carryOver = trade.carryOver === true
      || (trade.entryDate && trade.date && trade.entryDate !== trade.date)
      || trade.entryMode === 'overnight';
    const carryCell = carryOver
      ? `<span class="carry-flag" title="Position carried over from ${trade.entryDate || 'prior session'}">↻ carry</span>`
      : '<span class="intraday-flag">same-day</span>';
    const exitDate = trade.exitDate || trade.date || '-';
    const entryDate = trade.entryDate || trade.entryDay || trade.date || '-';
    const entryToExit = (isFiniteNumber(trade.entryPrice) && isFiniteNumber(trade.exitPrice))
      ? `$${Number(trade.entryPrice).toFixed(2)} <span class="muted">${entryDate !== exitDate ? `(${entryDate})` : ''}</span> → $${Number(trade.exitPrice).toFixed(2)}`
      : '-';
    const cells = [
      exitDate,
      carryCell,
      `<span class="${sideClass}">${trade.side || '-'}</span>`,
      trade.ticker || trade.symbol || '-',
      trade.entryMode || trade.type || '-',
      isFiniteNumber(trade.bias) ? Number(trade.bias).toFixed(2) : (isFiniteNumber(trade.strike) ? `K=${trade.strike}` : '-'),
      entryToExit,
      `<span class="${grossClass}">${formatPct(trade.grossReturn)}</span>`,
      `<span class="${netClass}">${formatPct(trade.netReturn)}</span>`,
    ];
    tr.innerHTML = cells.map((c) => `<td>${c}</td>`).join('');
    return tr;
  });
  els.tradesRows.replaceChildren(...rows);
}

function renderOpenPositions() {
  if (!els.openPositionsPanel) return;
  const positions = Array.isArray(state.openPositions) ? state.openPositions : [];
  if (positions.length === 0) {
    els.openPositionsPanel.hidden = true;
    return;
  }
  els.openPositionsPanel.hidden = false;
  els.openPositionsBadge.textContent = `${positions.length} held`;
  const rows = positions.map((pos) => {
    const tr = document.createElement('tr');
    const sideClass = pos.side === 'LONG' ? 'positive' : pos.side === 'SHORT' ? 'negative' : '';
    const mode = pos.entryMode || pos.type || (pos.carryOver ? 'overnight' : 'intraday');
    const conv = (isFiniteNumber(pos.bias)
      ? `bias ${Number(pos.bias).toFixed(2)}`
      : (isFiniteNumber(pos.strike)
        ? `K=${pos.strike} ${pos.right || ''}`.trim()
        : '-'));
    const sizeCell = isFiniteNumber(pos.contracts)
      ? `${pos.contracts} contracts`
      : (isFiniteNumber(pos.leverage) ? `${pos.leverage}× ${pos.ticker || ''}` : '-');
    const entryPxCell = isFiniteNumber(pos.entryPrice)
      ? `$${Number(pos.entryPrice).toFixed(2)}`
      : (isFiniteNumber(pos.grossPremium) ? `$${Number(pos.grossPremium).toFixed(2)} premium` : '-');
    const expectedExit = pos.expectedExitDate || pos.expiration || '-';
    const cells = [
      pos.entryDate || '-',
      `<span class="${sideClass}">${pos.side || 'SHORT'}</span>`,
      pos.ticker || pos.symbol || '-',
      mode,
      conv,
      sizeCell,
      entryPxCell,
      expectedExit,
    ];
    tr.innerHTML = cells.map((c) => `<td>${c}</td>`).join('');
    return tr;
  });
  els.openPositionsRows.replaceChildren(...rows);
}

function formatNumber(value, digits = 2) {
  if (!isFiniteNumber(value)) return '-';
  return Number(value).toFixed(digits);
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
  els.latestDate.textContent = summary.latestRebalanceDate || summary.endDate || summary.todayDate || '-';
  els.studyReturn.textContent = formatPct(summary.totalReturn);
  els.studyReturn.className = valueClass(summary.totalReturn);
  els.spyReturn.textContent = formatPct(summary.spyReturn);
  els.spyReturn.className = valueClass(summary.spyReturn);
  els.edgeReturn.textContent = formatPct(edge);
  els.edgeReturn.className = valueClass(edge);

  // Universal metrics — preferred field names with fallbacks for legacy summary shapes
  const sharpe = summary.sharpe ?? summary.sharpePerDay ?? summary.sharpePerTrade ?? null;
  els.sharpeMetric.textContent = formatNumber(sharpe);
  els.sharpeMetric.className = valueClass(sharpe);

  const maxDd = summary.maxDrawdown ?? null;
  els.maxDrawdown.textContent = isFiniteNumber(maxDd) ? formatPct(maxDd) : '-';
  els.maxDrawdown.className = isFiniteNumber(maxDd) && Number(maxDd) < 0 ? 'negative' : '';

  const hr = summary.hitRate ?? null;
  els.hitRate.textContent = isFiniteNumber(hr) ? formatPct(hr) : '-';

  const activeDays = summary.activeDays ?? summary.tradeCount ?? null;
  const tradingDays = summary.tradingDays ?? null;
  if (isFiniteNumber(activeDays) && isFiniteNumber(tradingDays)) {
    els.tradeCount.textContent = `${activeDays} / ${tradingDays}`;
  } else if (isFiniteNumber(activeDays)) {
    els.tradeCount.textContent = `${activeDays}`;
  } else {
    els.tradeCount.textContent = '-';
  }

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
