#!/usr/bin/env node
const fs = require('node:fs');
const path = require('node:path');

const { loadConfig, ensureDir, runtimePath } = require('../src/config');

function text(prop) {
  if (!Array.isArray(prop)) return '';
  return prop.map((part) => (Array.isArray(part) ? part[0] : '')).join('');
}

function blockValue(blockMap, id) {
  const wrapper = blockMap[id];
  return wrapper?.value?.value || wrapper?.value || null;
}

async function loadNotionPage(pageId) {
  const block = {};
  let cursor = { stack: [] };
  for (let chunkNumber = 0; chunkNumber < 60; chunkNumber += 1) {
    const response = await fetch('https://www.notion.so/api/v3/loadPageChunk', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ pageId, limit: 100, cursor, chunkNumber, verticalColumns: false }),
    });
    if (!response.ok) throw new Error(`Notion loadPageChunk failed: ${response.status}`);
    const json = await response.json();
    Object.assign(block, json.recordMap?.block || {});
    cursor = json.cursor;
    if (!cursor?.stack?.length) break;
  }
  return { block };
}

function renderBlocksMarkdown(blockMap, rootId, depth = 0, seen = new Set()) {
  if (seen.has(rootId)) return [];
  seen.add(rootId);
  const value = blockValue(blockMap, rootId);
  if (!value) return [];
  const title = text(value.properties?.title);
  const lines = [];
  if (value.type === 'page') lines.push(`${'#'.repeat(Math.max(1, Math.min(6, depth + 1)))} ${title}`);
  else if (value.type === 'header') lines.push(`## ${title}`);
  else if (value.type === 'sub_header') lines.push(`### ${title}`);
  else if (value.type === 'sub_sub_header') lines.push(`#### ${title}`);
  else if (value.type === 'bulleted_list') lines.push(`${'  '.repeat(Math.max(0, depth - 1))}- ${title}`);
  else if (value.type === 'numbered_list') lines.push(`${'  '.repeat(Math.max(0, depth - 1))}1. ${title}`);
  else if (value.type === 'quote') lines.push(`> ${title}`);
  else if (value.type === 'code') lines.push(`\n\`\`\`\n${title}\n\`\`\``);
  else if (value.type === 'divider') lines.push('---');
  else if (title) lines.push(title);
  (value.content || []).forEach((childId) => {
    lines.push(...renderBlocksMarkdown(blockMap, childId, depth + 1, seen));
  });
  return lines;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Fetch failed for ${url}: ${response.status}`);
  return response.json();
}

async function main() {
  const config = loadConfig();
  const sourceDir = runtimePath('source');
  ensureDir(sourceDir);

  const notionIds = [config.source.notionPageId, ...(config.source.notionLinkedPageIds || [])];
  for (const pageId of notionIds) {
    const page = await loadNotionPage(pageId);
    fs.writeFileSync(path.join(sourceDir, `notion-${pageId}.json`), JSON.stringify(page, null, 2));
    fs.writeFileSync(
      path.join(sourceDir, `notion-${pageId}.md`),
      `${renderBlocksMarkdown(page.block, pageId).join('\n\n')}\n`,
    );
    console.log(`wrote Notion snapshot ${pageId}`);
  }

  const symphonyId = config.source.composerSymphonyId;
  const apiBase = config.source.composerApiBase.replace(/\/$/, '');
  const details = await fetchJson(`${apiBase}/api/v1/public/symphonies/${symphonyId}`);
  const score = await fetchJson(`${apiBase}/api/v1/public/symphonies/${symphonyId}/score`);
  fs.writeFileSync(path.join(sourceDir, `composer-${symphonyId}-details.json`), JSON.stringify(details, null, 2));
  fs.writeFileSync(path.join(sourceDir, `composer-${symphonyId}-score.json`), JSON.stringify(score, null, 2));
  console.log(`wrote Composer public symphony ${symphonyId}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
