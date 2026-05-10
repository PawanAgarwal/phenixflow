#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const { ensureDir } = require('../../pym-v5-replication/src/config');
const {
  CORE_TICKERS,
  DEFAULT_LOOKBACK,
  DEFAULT_START_DATE,
  OPTION_FIELDS,
  OPTION_ROOTS,
  artifactPath,
  buildFeatureVector,
  buildSamples,
  findLatestOptionFeaturesPath,
  loadInputs,
  readOptionFeatureMap,
  safeTickerForMarket,
} = require('../src/experiment');

function parseArgs(argv) {
  const out = {
    startDate: DEFAULT_START_DATE,
    lookback: DEFAULT_LOOKBACK,
    useOptions: true,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === '--daily-bars') out.dailyBarsPath = argv[++index];
    else if (arg === '--score') out.scorePath = argv[++index];
    else if (arg === '--option-features') out.optionFeaturesPath = argv[++index];
    else if (arg === '--no-options') out.useOptions = false;
    else if (arg === '--lookback') out.lookback = Number(argv[++index]);
    else if (arg === '--start') out.startDate = argv[++index];
    else if (arg === '--end') out.endDate = argv[++index];
    else if (arg === '--out') out.outPath = argv[++index];
  }
  return out;
}

function featureGroupsForSample(sample, context, includeNames = false) {
  const groups = {
    price: buildFeatureVector(sample, context, ['price'], includeNames),
    liquidity: buildFeatureVector(sample, context, ['liquidity'], includeNames),
    micro: buildFeatureVector(sample, context, ['micro'], includeNames),
    pym: buildFeatureVector(sample, context, ['pym'], includeNames),
    attention: buildFeatureVector(sample, context, ['attention'], includeNames),
  };
  if (context.optionByDate.size) {
    groups.options = buildFeatureVector(sample, context, ['options'], includeNames);
  }
  return groups;
}

function runCli(argv) {
  const options = parseArgs(argv);
  const inputs = loadInputs(options);
  const optionFeaturesPath = options.useOptions === false ? null : (options.optionFeaturesPath || findLatestOptionFeaturesPath());
  const optionByDate = optionFeaturesPath ? readOptionFeatureMap(optionFeaturesPath) : new Map();
  const { samples, outputTickers } = buildSamples({
    market: inputs.market,
    score: inputs.score,
    lookback: options.lookback,
    startDate: options.startDate,
    endDate: options.endDate || null,
  });
  if (!samples.length) throw new Error('No samples available for export');
  const coreTickers = CORE_TICKERS.filter((ticker) => inputs.market.closes.has(ticker));
  const context = {
    market: inputs.market,
    lookback: options.lookback,
    coreTickers,
    outputTickers,
    optionByDate,
    optionRoots: OPTION_ROOTS,
    optionFields: OPTION_FIELDS,
    safeTicker: safeTickerForMarket(inputs.market),
  };
  const featureNames = Object.fromEntries(
    Object.entries(featureGroupsForSample(samples[0], context, true))
      .map(([name, payload]) => [name, payload.names]),
  );
  const outPath = options.outPath || artifactPath(`pym-v5-walkforward-dataset-${options.startDate}-${inputs.market.dates.at(-1)}.jsonl`);
  ensureDir(path.dirname(outPath));
  const stream = fs.createWriteStream(outPath);
  stream.write(`${JSON.stringify({
    type: 'metadata',
    generatedAt: new Date().toISOString(),
    source: {
      dailyBarsPath: inputs.dailyBarsPath,
      scorePath: inputs.scorePath,
      optionFeaturesPath,
      provider: 'Massive adjusted EOD plus Massive OPRA option aggregates when present',
    },
    settings: {
      startDate: options.startDate,
      endDate: options.endDate || null,
      lookback: options.lookback,
      timing: 'signal_eod_close_then_next_close',
    },
    marketStartDate: inputs.market.dates[0],
    marketEndDate: inputs.market.dates.at(-1),
    safeTicker: context.safeTicker,
    coreTickers,
    outputTickers,
    featureNames,
  })}\n`);
  samples.forEach((sample) => {
    const groups = Object.fromEntries(
      Object.entries(featureGroupsForSample(sample, context, false))
        .map(([name, values]) => [name, values]),
    );
    stream.write(`${JSON.stringify({
      type: 'sample',
      date: sample.date,
      nextDate: sample.nextDate,
      index: sample.index,
      teacherWeights: sample.teacherWeights,
      teacherReturn: sample.teacherReturn,
      nextReturns: sample.nextReturns,
      featureGroups: groups,
    })}\n`);
  });
  stream.end(() => {
    console.log(JSON.stringify({
      outputPath: outPath,
      samples: samples.length,
      firstSampleDate: samples[0].date,
      lastSampleDate: samples.at(-1).date,
      optionFeatureDates: optionByDate.size,
      outputTickers: outputTickers.length,
      featureGroups: Object.fromEntries(Object.entries(featureNames).map(([name, values]) => [name, values.length])),
    }, null, 2));
  });
}

runCli(process.argv.slice(2));
