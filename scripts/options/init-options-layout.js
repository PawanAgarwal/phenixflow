#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

function resolveRoot(envKey, fallbackRelative) {
  const configured = (process.env[envKey] || '').trim();
  if (configured) return path.resolve(configured);
  return path.resolve(process.cwd(), fallbackRelative);
}

function ensureDir(target) {
  fs.mkdirSync(target, { recursive: true });
  const testFile = path.join(target, '.write-check.tmp');
  fs.writeFileSync(testFile, 'ok\n', 'utf8');
  fs.unlinkSync(testFile);
}

function createLayout(rawRoot, curatedRoot) {
  const rawDirs = [
    path.join(rawRoot, 'raw', 'thetadata', 'trade_quote', 'by_day'),
    path.join(rawRoot, 'raw', 'thetadata', 'trade_quote', 'by_symbol'),
    path.join(rawRoot, 'manifests', 'download_runs'),
    path.join(rawRoot, 'logs'),
    path.join(rawRoot, 'tmp'),
  ];

  const curatedDirs = [
    path.join(curatedRoot, 'curated', 'sqlite'),
    path.join(curatedRoot, 'curated', 'parquet'),
    path.join(curatedRoot, 'curated', 'catalog'),
    path.join(curatedRoot, 'curated', 'reports'),
    path.join(curatedRoot, 'derived', 'features'),
    path.join(curatedRoot, 'derived', 'signals'),
    path.join(curatedRoot, 'logs'),
    path.join(curatedRoot, 'tmp'),
  ];

  rawDirs.forEach(ensureDir);
  curatedDirs.forEach(ensureDir);

  const manifest = {
    createdAt: new Date().toISOString(),
    rawRoot,
    curatedRoot,
    paths: {
      dayRaw: path.join(rawRoot, 'raw', 'thetadata', 'trade_quote', 'by_day'),
      symbolRaw: path.join(rawRoot, 'raw', 'thetadata', 'trade_quote', 'by_symbol'),
      sqliteDb: path.join(curatedRoot, 'curated', 'sqlite', 'options_trade_quote.sqlite'),
      reports: path.join(curatedRoot, 'curated', 'reports'),
      catalog: path.join(curatedRoot, 'curated', 'catalog'),
    },
  };

  const layoutFile = path.resolve(process.cwd(), 'data', 'options_storage', 'layout.json');
  fs.mkdirSync(path.dirname(layoutFile), { recursive: true });
  fs.writeFileSync(layoutFile, `${JSON.stringify(manifest, null, 2)}\n`, 'utf8');

  return { rawDirs, curatedDirs, layoutFile, manifest };
}

function main() {
  const rawRoot = resolveRoot('OPTIONS_RAW_ROOT', path.join('data', 'options_storage', 'raw'));
  const curatedRoot = resolveRoot('OPTIONS_CURATED_ROOT', path.join('data', 'options_storage', 'curated'));

  const { rawDirs, curatedDirs, layoutFile, manifest } = createLayout(rawRoot, curatedRoot);

  console.log('Options storage layout initialized');
  console.log(JSON.stringify({
    rawRoot,
    curatedRoot,
    rawDirCount: rawDirs.length,
    curatedDirCount: curatedDirs.length,
    layoutFile,
  }, null, 2));
  console.log('\nExport these for downloader runs:');
  console.log(`export OPTIONS_RAW_ROOT="${manifest.rawRoot}"`);
  console.log(`export OPTIONS_CURATED_ROOT="${manifest.curatedRoot}"`);
}

main();
