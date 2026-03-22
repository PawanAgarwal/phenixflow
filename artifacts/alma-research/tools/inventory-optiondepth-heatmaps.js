#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.resolve(__dirname, '..');
const POSTS_DIR = path.join(ROOT, 'posts');
const OUT_DIR = path.join(ROOT, 'analysis', 'backtests', 'optiondepth-image-backtest');
const OUT_JSON = path.join(OUT_DIR, 'inventory.json');
const OUT_MD = path.join(OUT_DIR, 'inventory.md');

function readText(filePath) {
  try {
    return fs.readFileSync(filePath, 'utf8');
  } catch {
    return '';
  }
}

function listPostDirs() {
  return fs
    .readdirSync(POSTS_DIR, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => path.join(POSTS_DIR, entry.name))
    .sort();
}

function extractTitle(content) {
  const firstLine = content.split(/\r?\n/, 1)[0] || '';
  return firstLine.trim();
}

function extractOdImageUuid(sourceHtml) {
  const marker = 'OptionsDepth Heatmap';
  const idx = sourceHtml.indexOf(marker);
  if (idx === -1) {
    return null;
  }
  const nearby = sourceHtml.slice(idx, idx + 40000);
  const encoded = nearby.match(/public%2Fimages%2F([0-9a-f-]+)_[^"']+/i);
  if (encoded) {
    return encoded[1];
  }
  const plain = nearby.match(/public\/images\/([0-9a-f-]+)_[^"']+/i);
  return plain ? plain[1] : null;
}

function findLocalImage(postDir, uuid) {
  if (!uuid) {
    return null;
  }
  const imagesDir = path.join(postDir, 'images');
  if (!fs.existsSync(imagesDir)) {
    return null;
  }
  const candidates = fs
    .readdirSync(imagesDir)
    .filter((name) => name.startsWith(`${uuid}_`))
    .sort();
  return candidates.length ? path.join(imagesDir, candidates[0]) : null;
}

function extractSection(content, startMarker, endMarker) {
  const startIdx = content.indexOf(startMarker);
  if (startIdx === -1) {
    return '';
  }
  const fromStart = content.slice(startIdx + startMarker.length);
  if (!endMarker) {
    return fromStart.trim();
  }
  const endIdx = fromStart.indexOf(endMarker);
  return (endIdx === -1 ? fromStart : fromStart.slice(0, endIdx)).trim();
}

function normalizeWhitespace(value) {
  return value.replace(/\s+/g, ' ').trim();
}

function isDailyPost(title, dirName) {
  return /intraday post/i.test(title) || /intraday-post/i.test(dirName);
}

function buildRecord(postDir) {
  const dirName = path.basename(postDir);
  const contentPath = path.join(postDir, 'content.txt');
  const sourceHtmlPath = path.join(postDir, 'source.html');
  const content = readText(contentPath);
  const sourceHtml = readText(sourceHtmlPath);
  if (!content || !sourceHtml) {
    return null;
  }
  if (!content.includes('OptionsDepth Heatmap') && !sourceHtml.includes('OptionsDepth Heatmap')) {
    return null;
  }

  const title = extractTitle(content);
  const odImageUuid = extractOdImageUuid(sourceHtml);
  const localImagePath = findLocalImage(postDir, odImageUuid);
  const commentaryBeforeOd = extractSection(
    content,
    'INTRADAY POST Coding today’s positioning:',
    'OptionsDepth Heatmap'
  );
  const scriptSection = extractSection(content, 'SCRIPT INPUTS', null);

  return {
    date: dirName.slice(0, 10),
    dirName,
    title,
    dailyPost: isDailyPost(title, dirName),
    postDir,
    contentPath,
    sourceHtmlPath,
    odImageUuid,
    localImagePath,
    commentaryBeforeOd: normalizeWhitespace(commentaryBeforeOd),
    hasScriptInputs: scriptSection.includes('=== SPX closed at'),
  };
}

function renderMarkdown(records) {
  const lines = [];
  lines.push('# OptionDepth Heatmap Inventory');
  lines.push('');
  lines.push(`Generated ${new Date().toISOString()}.`);
  lines.push('');
  lines.push(`Posts with an archived \`OptionsDepth Heatmap\`: ${records.length}`);
  lines.push(`Daily/intraday posts: ${records.filter((record) => record.dailyPost).length}`);
  lines.push('');
  lines.push('| Date | Daily | Title | OD image | Script inputs | Post |');
  lines.push('| --- | --- | --- | --- | --- | --- |');
  for (const record of records) {
    const imageLabel = record.localImagePath ? 'yes' : 'missing';
    lines.push(
      `| ${record.date} | ${record.dailyPost ? 'yes' : 'no'} | ${record.title.replace(/\|/g, '\\|')} | ${imageLabel} | ${record.hasScriptInputs ? 'yes' : 'no'} | [${record.dirName}](${record.contentPath}) |`
    );
  }
  lines.push('');
  return lines.join('\n');
}

function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  const records = listPostDirs()
    .map(buildRecord)
    .filter(Boolean)
    .sort((a, b) => a.date.localeCompare(b.date) || a.dirName.localeCompare(b.dirName));

  const output = {
    generatedAt: new Date().toISOString(),
    totalPostsWithOdHeatmap: records.length,
    dailyPostsWithOdHeatmap: records.filter((record) => record.dailyPost).length,
    records,
  };

  fs.writeFileSync(OUT_JSON, JSON.stringify(output, null, 2));
  fs.writeFileSync(OUT_MD, renderMarkdown(records));
  console.log(`Wrote ${records.length} OptionDepth records to ${OUT_JSON}`);
}

main();
