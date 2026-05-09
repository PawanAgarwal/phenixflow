function holdingMap(snapshot) {
  return new Map((snapshot?.holdings || []).map((holding) => [holding.ticker, holding]));
}

function compareSnapshots(current, previous) {
  const currentMap = holdingMap(current);
  const previousMap = holdingMap(previous);
  const tickers = [...new Set([...currentMap.keys(), ...previousMap.keys()])].sort();
  const equity = current?.equityBeforeNextSession || 0;
  const changes = tickers.map((ticker) => {
    const currentHolding = currentMap.get(ticker);
    const previousHolding = previousMap.get(ticker);
    const currentWeight = currentHolding?.weight || 0;
    const previousWeight = previousHolding?.weight || 0;
    const weightChange = currentWeight - previousWeight;
    return {
      ticker,
      previousWeight,
      currentWeight,
      weightChange,
      previousWeightPct: previousWeight * 100,
      currentWeightPct: currentWeight * 100,
      weightChangePct: weightChange * 100,
      currentDollars: equity * currentWeight,
      tradeDollarsVsPreviousTarget: equity * weightChange,
      status: previousWeight === 0 && currentWeight > 0
        ? 'added'
        : currentWeight === 0 && previousWeight > 0
          ? 'removed'
          : weightChange > 0
            ? 'increased'
            : weightChange < 0
              ? 'decreased'
              : 'unchanged',
    };
  }).sort((left, right) => Math.abs(right.weightChange) - Math.abs(left.weightChange) || right.currentWeight - left.currentWeight);

  return {
    currentDate: current?.date || null,
    previousDate: previous?.date || null,
    turnover: changes.reduce((sum, item) => sum + Math.abs(item.weightChange), 0),
    turnoverPct: changes.reduce((sum, item) => sum + Math.abs(item.weightChange), 0) * 100,
    changes,
    added: changes.filter((item) => item.status === 'added'),
    removed: changes.filter((item) => item.status === 'removed'),
    increased: changes.filter((item) => item.status === 'increased'),
    decreased: changes.filter((item) => item.status === 'decreased'),
  };
}

function snapshotResponse(snapshot, previous) {
  return {
    snapshot,
    changeFromPrevious: compareSnapshots(snapshot, previous),
  };
}

module.exports = {
  compareSnapshots,
  snapshotResponse,
};
