function emptyConfusion() {
  return { tp: 0, tn: 0, fp: 0, fn: 0 };
}

function bucketName(confidence) {
  if (confidence < 0.525) return '0.500-0.525';
  if (confidence < 0.550) return '0.525-0.550';
  if (confidence < 0.600) return '0.550-0.600';
  if (confidence < 0.700) return '0.600-0.700';
  return '0.700-1.000';
}

function computePredictionMetrics(predictions) {
  const confusion = emptyConfusion();
  let absoluteErrorSum = 0;
  let squaredErrorSum = 0;
  let brierSum = 0;
  let logLossSum = 0;
  const buckets = new Map();

  predictions.forEach((prediction) => {
    const actual = prediction.actualDirection;
    const predicted = prediction.predictedDirection;
    if (predicted === 1 && actual === 1) confusion.tp += 1;
    if (predicted === 0 && actual === 0) confusion.tn += 1;
    if (predicted === 1 && actual === 0) confusion.fp += 1;
    if (predicted === 0 && actual === 1) confusion.fn += 1;

    const error = prediction.predictedReturn - prediction.actualReturn;
    absoluteErrorSum += Math.abs(error);
    squaredErrorSum += error * error;
    const probability = Math.min(1 - 1e-9, Math.max(1e-9, prediction.directionProbability));
    brierSum += (probability - actual) ** 2;
    logLossSum += actual === 1 ? -Math.log(probability) : -Math.log(1 - probability);

    const bucket = bucketName(prediction.confidence);
    const item = buckets.get(bucket) || { bucket, count: 0, correct: 0, accuracy: null };
    item.count += 1;
    if (predicted === actual) item.correct += 1;
    buckets.set(bucket, item);
  });

  const total = predictions.length;
  const positiveTotal = confusion.tp + confusion.fn;
  const negativeTotal = confusion.tn + confusion.fp;
  const positiveAccuracy = positiveTotal ? confusion.tp / positiveTotal : null;
  const negativeAccuracy = negativeTotal ? confusion.tn / negativeTotal : null;
  const directionalAccuracy = total ? (confusion.tp + confusion.tn) / total : null;
  const balancedAccuracy = positiveAccuracy === null || negativeAccuracy === null
    ? directionalAccuracy
    : (positiveAccuracy + negativeAccuracy) / 2;
  const confidenceBuckets = [...buckets.values()]
    .sort((left, right) => left.bucket.localeCompare(right.bucket))
    .map((item) => ({
      ...item,
      accuracy: item.count ? item.correct / item.count : null,
    }));

  return {
    count: total,
    directionalAccuracy,
    balancedAccuracy,
    confusion,
    confidenceBuckets,
    returnMae: total ? absoluteErrorSum / total : null,
    returnRmse: total ? Math.sqrt(squaredErrorSum / total) : null,
    brierScore: total ? brierSum / total : null,
    logLoss: total ? logLossSum / total : null,
  };
}

function roundMetric(value, digits = 6) {
  return typeof value === 'number' && Number.isFinite(value) ? Number(value.toFixed(digits)) : value;
}

function compactMetrics(metrics) {
  return {
    count: metrics.count,
    directionalAccuracy: roundMetric(metrics.directionalAccuracy),
    balancedAccuracy: roundMetric(metrics.balancedAccuracy),
    confusion: metrics.confusion,
    confidenceBuckets: metrics.confidenceBuckets.map((bucket) => ({
      ...bucket,
      accuracy: roundMetric(bucket.accuracy),
    })),
    returnMae: roundMetric(metrics.returnMae),
    returnRmse: roundMetric(metrics.returnRmse),
    brierScore: roundMetric(metrics.brierScore),
    logLoss: roundMetric(metrics.logLoss),
  };
}

module.exports = {
  computePredictionMetrics,
  compactMetrics,
};
