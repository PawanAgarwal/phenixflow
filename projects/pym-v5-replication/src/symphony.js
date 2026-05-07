const { indicatorValue, windowParam } = require('./indicators');

function mergeWeights(parts) {
  const out = new Map();
  parts.forEach(({ weights, scale }) => {
    weights.forEach((weight, ticker) => {
      out.set(ticker, (out.get(ticker) || 0) + (weight * scale));
    });
  });
  return out;
}

function normalizeWeights(weights) {
  let total = 0;
  weights.forEach((weight) => {
    if (weight > 0) total += weight;
  });
  if (total <= 0) return new Map();
  const out = new Map();
  weights.forEach((weight, ticker) => {
    if (weight > 0) out.set(ticker, weight / total);
  });
  return out;
}

function nodeWeight(node) {
  if (!node.weight) return null;
  const numerator = Number(node.weight.num);
  const denominator = Number(node.weight.den);
  if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || denominator === 0) return null;
  return numerator / denominator;
}

function equalChildScales(ctx, children) {
  const evaluatedChildren = children.map((child) => ({ child, weights: evalNode(ctx, child), explicitWeight: nodeWeight(child) }))
    .filter((item) => item.weights.size > 0);
  if (!evaluatedChildren.length) return [];
  if (ctx.equalWeightMode === 'specified_residual') {
    const explicit = evaluatedChildren.filter((item) => Number.isFinite(item.explicitWeight));
    if (explicit.length) {
      const explicitSum = explicit.reduce((sum, item) => sum + Math.max(0, item.explicitWeight), 0);
      const unweighted = evaluatedChildren.filter((item) => !Number.isFinite(item.explicitWeight));
      if (explicitSum >= 1 || !unweighted.length) {
        const denominator = explicitSum || explicit.length;
        return explicit.map((item) => ({ weights: item.weights, scale: Math.max(0, item.explicitWeight) / denominator }));
      }
      const residualScale = (1 - explicitSum) / unweighted.length;
      return evaluatedChildren.map((item) => ({
        weights: item.weights,
        scale: Number.isFinite(item.explicitWeight) ? Math.max(0, item.explicitWeight) : residualScale,
      }));
    }
  }
  if (ctx.equalWeightMode === 'relative_child_weight') {
    const hasExplicit = evaluatedChildren.some((item) => Number.isFinite(item.explicitWeight));
    if (hasExplicit) {
      const raw = evaluatedChildren.map((item) => ({
        item,
        weight: Number.isFinite(item.explicitWeight) ? Math.max(0, item.explicitWeight) : 1,
      }));
      const total = raw.reduce((sum, item) => sum + item.weight, 0);
      return raw.map(({ item, weight }) => ({ weights: item.weights, scale: total > 0 ? weight / total : 0 }));
    }
  }
  return evaluatedChildren.map((item) => ({ weights: item.weights, scale: 1 / evaluatedChildren.length }));
}

function collectTickers(node, out = new Set()) {
  if (!node || typeof node !== 'object') return out;
  if (node.step === 'asset' && node.ticker) out.add(String(node.ticker).toUpperCase());
  for (const key of ['lhs-val', 'rhs-val']) {
    const value = node[key];
    const fixed = key === 'rhs-val' && node['rhs-fixed-value?'];
    if (!fixed && typeof value === 'string' && /[A-Z]/.test(value)) out.add(value.toUpperCase());
  }
  (node.children || []).forEach((child) => collectTickers(child, out));
  return out;
}

function compareValues(left, comparator, right) {
  if (!Number.isFinite(left) || !Number.isFinite(right)) return false;
  switch (comparator) {
    case 'gt': return left > right;
    case 'gte': return left >= right;
    case 'lt': return left < right;
    case 'lte': return left <= right;
    default:
      throw new Error(`Unsupported Composer comparator: ${comparator}`);
  }
}

function conditionValue(ctx, child, side) {
  const fnName = child[`${side}-fn`];
  const rawValue = child[`${side}-val`];
  if (side === 'rhs' && child['rhs-fixed-value?']) return Number(rawValue);
  if (!fnName) return Number(rawValue);
  return indicatorValue(ctx, fnName, rawValue, ctx.signalIndex, windowParam(child, side));
}

function childConditionIsTrue(ctx, child) {
  if (child['is-else-condition?']) return true;
  return compareValues(
    conditionValue(ctx, child, 'lhs'),
    child.comparator,
    conditionValue(ctx, child, 'rhs'),
  );
}

function evalChildrenEqual(ctx, children) {
  return mergeWeights(equalChildScales(ctx, children));
}

function evalChildrenSpecified(ctx, children) {
  const parts = [];
  children.forEach((child) => {
    const scale = nodeWeight(child);
    if (!Number.isFinite(scale) || scale <= 0) return;
    parts.push({ weights: evalNode(ctx, child), scale });
  });
  return mergeWeights(parts);
}

function evalInverseVol(ctx, node) {
  const window = Number(node['window-days'] || 10);
  const scored = (node.children || [])
    .map((child) => {
      const ticker = child.ticker;
      const vol = indicatorValue(ctx, 'standard-deviation-return', ticker, ctx.signalIndex, window);
      return { child, vol };
    })
    .filter(({ vol }) => Number.isFinite(vol) && vol > 0);
  if (!scored.length) return new Map();
  const raw = new Map();
  scored.forEach(({ child, vol }) => raw.set(child.ticker.toUpperCase(), 1 / vol));
  return normalizeWeights(raw);
}

function evalFilter(ctx, node) {
  const sortFn = node['sort-by-fn'];
  const selectFn = node['select-fn'];
  const selectN = Number(node['select-n'] || 1);
  const scored = (node.children || [])
    .filter((child) => child.step === 'asset' && child.ticker)
    .map((child) => ({
      child,
      score: indicatorValue(ctx, sortFn, child.ticker, ctx.signalIndex, windowParam(node, 'sort-by')),
    }))
    .filter(({ score }) => Number.isFinite(score));
  const sorted = scored.sort((left, right) => {
    if (selectFn === 'bottom') return left.score - right.score;
    return right.score - left.score;
  });
  const selected = sorted.slice(0, selectN).map(({ child }) => evalNode(ctx, child));
  if (!selected.length) return new Map();
  return mergeWeights(selected.map((weights) => ({ weights, scale: 1 / selected.length })));
}

function evalIf(ctx, node) {
  const selected = (node.children || []).find((child) => child.step === 'if-child' && childConditionIsTrue(ctx, child));
  if (!selected) return new Map();
  return evalChildrenEqual(ctx, selected.children || []);
}

function evalNode(ctx, node) {
  if (!node || typeof node !== 'object') return new Map();
  switch (node.step) {
    case 'root':
    case 'group':
      return evalChildrenEqual(ctx, node.children || []);
    case 'asset':
      return node.ticker ? new Map([[String(node.ticker).toUpperCase(), 1]]) : new Map();
    case 'wt-cash-equal':
      return evalChildrenEqual(ctx, node.children || []);
    case 'wt-cash-specified':
      return evalChildrenSpecified(ctx, node.children || []);
    case 'wt-inverse-vol':
      return evalInverseVol(ctx, node);
    case 'filter':
      return evalFilter(ctx, node);
    case 'if':
      return evalIf(ctx, node);
    case 'if-child':
      return evalChildrenEqual(ctx, node.children || []);
    default:
      throw new Error(`Unsupported Composer node step: ${node.step}`);
  }
}

function evaluateSymphony(score, ctx, signalIndex, options = {}) {
  const weights = evalNode({ ...ctx, ...options, signalIndex }, score);
  return normalizeWeights(weights);
}

module.exports = {
  collectTickers,
  evaluateSymphony,
  compareValues,
};
