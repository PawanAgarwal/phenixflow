#!/usr/bin/env node
const fs = require('node:fs');
const https = require('node:https');
const path = require('node:path');
const zlib = require('node:zlib');

const { ensureDir, loadConfig, runtimePath } = require('../src/config');
const { parseCsvLine, readCsvStream } = require('../src/csv');
const {
  buildAuthorizationHeaders,
  buildObjectKey,
  buildObjectUrl,
  loadMassiveEnv,
  resolveCredentials,
} = require('../src/massive-s3');

function parseArgs(argv = process.argv.slice(2)) {
  const out = {};
  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      out[key] = true;
      continue;
    }
    out[key] = next;
    index += 1;
  }
  return out;
}

function asBool(value) {
  return value === true || value === 'true' || value === '1' || value === 'yes';
}

function datesInRange(startDate, endDate) {
  const out = [];
  const cursor = new Date(`${startDate}T00:00:00.000Z`);
  const end = new Date(`${endDate}T00:00:00.000Z`);
  while (cursor <= end) {
    const day = cursor.getUTCDay();
    if (day !== 0 && day !== 6) out.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return out;
}

function outputPathForDay(symbol, dayIso) {
  return runtimePath('tick-quotes', `massive-stock-quotes-${String(symbol).toUpperCase()}-${dayIso}.csv.gz`);
}

function manifestPathForDay(symbol, dayIso) {
  return runtimePath('tick-quotes', `massive-stock-quotes-${String(symbol).toUpperCase()}-${dayIso}.manifest.json`);
}

function findTickerColumn(headers) {
  const lower = headers.map((header) => String(header || '').toLowerCase());
  return lower.indexOf('ticker') >= 0 ? lower.indexOf('ticker') : lower.indexOf('symbol');
}

async function filterQuoteFileForSymbol({
  config,
  dayIso,
  symbol,
  outputPath,
  overwrite = false,
  assumeSorted = false,
  dataset = null,
}) {
  if (!overwrite && fs.existsSync(outputPath)) {
    return {
      dayIso,
      symbol,
      outputPath,
      skipped: true,
      bytesWritten: fs.statSync(outputPath).size,
    };
  }

  loadMassiveEnv();
  const credentials = resolveCredentials();
  if (!credentials.ready) {
    throw new Error('massive_s3_credentials_missing');
  }

  const datasetId = dataset || config.research?.massiveStockTickQuoteDataset || 'us_stocks_sip/quotes_v1';
  const objectKey = buildObjectKey(datasetId, dayIso);
  const url = buildObjectUrl(objectKey);
  const targetSymbol = String(symbol || config.target || 'TSLL').toUpperCase();
  ensureDir(path.dirname(outputPath));
  const tempPath = `${outputPath}.tmp`;
  fs.rmSync(tempPath, { force: true });

  return new Promise((resolve, reject) => {
    const request = https.request(url, {
      method: 'GET',
      timeout: 180000,
      headers: buildAuthorizationHeaders({
        method: 'GET',
        objectKey,
        credentials,
      }),
    }, async (response) => {
      const statusCode = Number(response.statusCode || 0);
      if (statusCode !== 200) {
        let body = '';
        response.setEncoding('utf8');
        response.on('data', (chunk) => {
          body += chunk;
        });
        response.on('end', () => {
          reject(new Error(`massive_quote_download_failed:${statusCode}:${dayIso}:${body.trim().slice(0, 240)}`));
        });
        return;
      }

      const startedAt = Date.now();
      let inputRows = 0;
      let matchedRows = 0;
      let tickerColumn = -1;
      let seenTarget = false;
      let bytesDownloaded = 0;
      let stoppedEarly = false;
      response.on('data', (chunk) => {
        bytesDownloaded += chunk.length;
      });

      const gunzip = zlib.createGunzip();
      const gzip = zlib.createGzip({ level: 6 });
      const output = fs.createWriteStream(tempPath);
      gzip.pipe(output);

      let settled = false;
      function fail(error) {
        if (settled) return;
        settled = true;
        fs.rmSync(tempPath, { force: true });
        reject(error);
      }

      response.on('error', fail);
      gunzip.on('error', fail);
      gzip.on('error', fail);
      output.on('error', fail);

      output.on('finish', () => {
        if (settled) return;
        settled = true;
        fs.renameSync(tempPath, outputPath);
        const result = {
          dayIso,
          symbol: targetSymbol,
          dataset: datasetId,
          objectKey,
          outputPath,
          skipped: false,
          assumeSorted,
          stoppedEarly,
          inputRows,
          matchedRows,
          bytesDownloaded,
          bytesWritten: fs.statSync(outputPath).size,
          elapsedMs: Date.now() - startedAt,
        };
        fs.writeFileSync(manifestPathForDay(targetSymbol, dayIso), `${JSON.stringify({
          ...result,
          generatedAt: new Date().toISOString(),
        }, null, 2)}\n`);
        resolve(result);
      });

      response.pipe(gunzip);
      try {
        await readCsvStream(gunzip, (row, rowCount, headers, rawLine) => {
          if (!row) {
            tickerColumn = findTickerColumn(headers);
            if (tickerColumn < 0) throw new Error(`ticker_column_missing:${headers.join('|')}`);
            gzip.write(`${rawLine}\n`);
            return undefined;
          }
          inputRows = rowCount;
          const fields = parseCsvLine(rawLine);
          const ticker = String(fields[tickerColumn] || '').toUpperCase();
          if (ticker !== targetSymbol) {
            if (assumeSorted && seenTarget && ticker > targetSymbol) {
              stoppedEarly = true;
              response.destroy();
              gunzip.destroy();
              return false;
            }
            return undefined;
          }
          seenTarget = true;
          matchedRows += 1;
          gzip.write(`${rawLine}\n`);
          return undefined;
        });
        gzip.end();
      } catch (error) {
        fail(error);
      }
    });
    request.on('timeout', () => {
      request.destroy(Object.assign(new Error(`massive_quote_download_timeout:${dayIso}`), { code: 'ETIMEDOUT' }));
    });
    request.on('error', reject);
    request.end();
  });
}

async function probeObject({
  config,
  dayIso,
  dataset = null,
}) {
  loadMassiveEnv();
  const credentials = resolveCredentials();
  if (!credentials.ready) {
    throw new Error('massive_s3_credentials_missing');
  }

  const datasetId = dataset || config.research?.massiveStockTickQuoteDataset || 'us_stocks_sip/quotes_v1';
  const objectKey = buildObjectKey(datasetId, dayIso);
  const url = buildObjectUrl(objectKey);
  return new Promise((resolve, reject) => {
    const request = https.request(url, {
      method: 'HEAD',
      timeout: 30000,
      headers: buildAuthorizationHeaders({
        method: 'HEAD',
        objectKey,
        credentials,
      }),
    }, (response) => {
      response.resume();
      response.on('end', () => resolve({
        dayIso,
        dataset: datasetId,
        objectKey,
        statusCode: response.statusCode,
        contentLength: response.headers['content-length'],
        lastModified: response.headers['last-modified'],
      }));
    });
    request.on('timeout', () => {
      request.destroy(Object.assign(new Error(`massive_quote_probe_timeout:${dayIso}`), { code: 'ETIMEDOUT' }));
    });
    request.on('error', reject);
    request.end();
  });
}

async function main() {
  const args = parseArgs();
  const config = loadConfig(args.config);
  const symbol = String(args.symbol || config.target || 'TSLL').toUpperCase();
  const startDate = args['start-date'] || args.date || '2026-01-08';
  const endDate = args['end-date'] || startDate;
  const dataset = args.dataset || null;
  const dates = datesInRange(startDate, endDate);
  if (!dates.length) throw new Error(`no_weekdays:${startDate}:${endDate}`);

  if (asBool(args['probe-only'])) {
    const results = [];
    for (const dayIso of dates) {
      console.error(`[tsll-quotes] probing dataset=${dataset || config.research?.massiveStockTickQuoteDataset || 'us_stocks_sip/quotes_v1'} day=${dayIso}`);
      results.push(await probeObject({ config, dayIso, dataset }));
    }
    console.log(JSON.stringify({
      symbol,
      startDate,
      endDate,
      probeOnly: true,
      results,
    }, null, 2));
    return;
  }

  const results = [];
  for (const dayIso of dates) {
    if (args.output && dates.length > 1) {
      throw new Error('single_output_path_with_multiple_dates');
    }
    const outputPath = path.resolve(args.output || outputPathForDay(symbol, dayIso));
    console.error(`[tsll-quotes] filtering dataset=${dataset || config.research?.massiveStockTickQuoteDataset || 'us_stocks_sip/quotes_v1'} symbol=${symbol} ${dayIso} -> ${outputPath}`);
    const result = await filterQuoteFileForSymbol({
      config,
      dayIso,
      symbol,
      outputPath,
      overwrite: asBool(args.overwrite),
      assumeSorted: asBool(args['assume-sorted']),
      dataset,
    });
    results.push(result);
    console.error(`[tsll-quotes] ${dayIso} matched=${result.matchedRows ?? 'cache'} bytesWritten=${result.bytesWritten} elapsed=${(((result.elapsedMs || 0) / 1000)).toFixed(1)}s skipped=${result.skipped ? 'yes' : 'no'}`);
  }
  console.log(JSON.stringify({
    symbol,
    startDate,
    endDate,
    results,
  }, null, 2));
}

main().catch((error) => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
