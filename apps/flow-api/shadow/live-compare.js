function uniqueIds(rows = []) {
  const ids = [];
  const seen = new Set();

  rows.forEach((row) => {
    if (!row || typeof row.id !== 'string') return;
    if (seen.has(row.id)) return;
    seen.add(row.id);
    ids.push(row.id);
  });

  return ids;
}

function buildShadowComparison({
  activeVersion,
  shadowVersion,
  activeRows = [],
  shadowRows = [],
  diffLimit = 100,
} = {}) {
  const activeIds = uniqueIds(activeRows);
  const shadowIds = uniqueIds(shadowRows);
  const activeSet = new Set(activeIds);
  const shadowSet = new Set(shadowIds);

  const addedByShadow = shadowIds.filter((id) => !activeSet.has(id)).slice(0, diffLimit);
  const removedByShadow = activeIds.filter((id) => !shadowSet.has(id)).slice(0, diffLimit);

  return {
    activeVersion,
    shadowVersion,
    activeCount: activeIds.length,
    shadowCount: shadowIds.length,
    addedByShadow,
    removedByShadow,
    truncated: {
      addedByShadow: shadowIds.length - addedByShadow.length,
      removedByShadow: activeIds.length - removedByShadow.length,
    },
  };
}

module.exports = {
  buildShadowComparison,
  uniqueIds,
};
