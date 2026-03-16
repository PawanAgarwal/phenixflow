const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

const OPENAI_RESPONSES_URL = 'https://api.openai.com/v1/responses';
const DEFAULT_MODEL = 'gpt-5-mini';
const COMMENTARY_CACHE_VERSION = 'commentary-v1';
const CHAT_CACHE_VERSION = 'chat-v1';
const HEATMAP_CACHE_VERSION = 'heatmap-v1';
const STANDARD_INSTRUMENTS = [
  'SPX',
  'ES',
  'SPY',
  'VIX',
  'NDX',
  'NQ',
  'QQQ',
  'IWM',
  'RUT',
  'NVDA',
  'AMZN',
  'AAPL',
  'GOOG',
  'GOOGL',
  'META',
  'TSLA',
  'TSM',
  'AMD',
  'GC',
  'SI',
  'TLT',
  'ZB',
  'CL',
];

const TEXT_PREDICTION_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    predictions: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          instrument: { type: 'string' },
          prediction_text: { type: 'string' },
          condition: { type: 'string' },
          expected_result: { type: 'string' },
        },
        required: ['instrument', 'prediction_text', 'condition', 'expected_result'],
      },
    },
  },
  required: ['predictions'],
};

const CHAT_PREDICTION_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    predictions: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          made_at: { type: 'string' },
          instrument: { type: 'string' },
          prediction_text: { type: 'string' },
          condition: { type: 'string' },
          expected_result: { type: 'string' },
        },
        required: ['made_at', 'instrument', 'prediction_text', 'condition', 'expected_result'],
      },
    },
  },
  required: ['predictions'],
};

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function normalizeWhitespace(value = '') {
  return String(value || '')
    .replace(/\u00a0/g, ' ')
    .replace(/\r/g, '')
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function compactText(text = '', maxLength = 320) {
  const clean = normalizeWhitespace(text);
  if (clean.length <= maxLength) return clean;
  return `${clean.slice(0, maxLength - 1).trimEnd()}…`;
}

function parseEnvLine(rawLine = '') {
  const line = String(rawLine || '').trim();
  if (!line || line.startsWith('#')) return null;
  const match = line.match(/^([A-Z0-9_]+)\s*=\s*(.*)$/);
  if (!match) return null;
  let value = match[2].trim();
  if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith('\'') && value.endsWith('\''))) {
    value = value.slice(1, -1);
  }
  return { key: match[1], value };
}

function loadEnvFile(filePath, baseEnv = process.env) {
  const env = { ...baseEnv };
  if (!filePath || !fs.existsSync(filePath)) return env;
  const lines = fs.readFileSync(filePath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const parsed = parseEnvLine(line);
    if (!parsed) continue;
    if (!env[parsed.key]) env[parsed.key] = parsed.value;
  }
  return env;
}

function stableHash(value) {
  return crypto.createHash('sha1').update(String(value || '')).digest('hex');
}

function extractResponseText(payload) {
  if (!payload || typeof payload !== 'object') return '';
  const pieces = [];
  for (const outputItem of payload.output || []) {
    if (outputItem.type !== 'message') continue;
    for (const contentItem of outputItem.content || []) {
      if (contentItem.type === 'output_text' && contentItem.text) {
        pieces.push(String(contentItem.text));
      }
    }
  }
  return pieces.join('\n').trim();
}

function readCache(cachePath, inputHash) {
  if (!cachePath || !fs.existsSync(cachePath)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(cachePath, 'utf8'));
    if (parsed.inputHash !== inputHash) return null;
    return parsed.payload || null;
  } catch {
    return null;
  }
}

function writeCache(cachePath, inputHash, payload) {
  ensureDir(path.dirname(cachePath));
  fs.writeFileSync(cachePath, JSON.stringify({
    cachedAt: new Date().toISOString(),
    inputHash,
    payload,
  }, null, 2));
}

function normalizeInstrument(value = '') {
  const raw = normalizeWhitespace(value).toUpperCase();
  if (!raw) return '';
  if (STANDARD_INSTRUMENTS.includes(raw)) return raw;

  const aliases = new Map([
    ['S&P 500', 'SPX'],
    ['S&P500', 'SPX'],
    ['SPX CASH', 'SPX'],
    ['SP500', 'SPX'],
    ['SP 500', 'SPX'],
    ['ES FUTURES', 'ES'],
    ['E-MINI S&P', 'ES'],
    ['NASDAQ 100', 'NDX'],
    ['NASDAQ-100', 'NDX'],
    ['NQ FUTURES', 'NQ'],
    ['VIX INDEX', 'VIX'],
    ['RUSSELL 2000', 'RUT'],
    ['GOLD', 'GC'],
    ['SILVER', 'SI'],
    ['CRUDE', 'CL'],
    ['CRUDE OIL', 'CL'],
    ['30Y BOND', 'ZB'],
    ['US 30Y BOND', 'ZB'],
  ]);
  return aliases.get(raw) || '';
}

function normalizePredictionItem(item = {}) {
  return {
    instrument: normalizeInstrument(item.instrument || ''),
    predictionText: compactText(item.prediction_text || item.predictionText || '', 340),
    condition: compactText(item.condition || '', 220),
    expected: compactText(item.expected_result || item.expected || '', 260),
  };
}

function hasTradeableMarketSignal(text = '') {
  const value = String(text || '');
  if (/\b(market|tape|spot|vol|iv|risk[- ]on|risk[- ]off|gamma|vanna|charm|speed|pivot|target|centroid|pin|reversion|breakout|breakdown|downside|upside|bull|bear|futures|index|0dte|dte)\b/i.test(value)) {
    return true;
  }
  if (/\b(support|resistance)\b/i.test(value) && /\b\d{3,5}(?:\.\d+)?(?:\/\d+(?:\.\d+)?)?\b/.test(value)) {
    return true;
  }
  return false;
}

function isMeaningfulPrediction(item = {}, extraContext = '') {
  if (!item.predictionText || !item.expected) return false;
  if (item.instrument) return true;
  return hasTradeableMarketSignal(item.predictionText);
}

function toInputText(text = '') {
  return { type: 'input_text', text: String(text || '') };
}

function toInputImage(imagePath) {
  const ext = path.extname(imagePath).toLowerCase();
  const mimeType = ext === '.png'
    ? 'image/png'
    : ext === '.webp'
      ? 'image/webp'
      : 'image/jpeg';
  const base64 = fs.readFileSync(imagePath, 'base64');
  return {
    type: 'input_image',
    image_url: `data:${mimeType};base64,${base64}`,
  };
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableError(error, statusCode = 0) {
  if (statusCode === 408 || statusCode === 409 || statusCode === 429 || statusCode >= 500) return true;
  const message = String(error?.message || error || '').toLowerCase();
  return /fetch failed|timed out|timeout|econnreset|enotfound|socket|network|503|502|500/.test(message);
}

async function callOpenAiJson({ apiKey, model, cachePath, cacheKey, schemaName, schema, content }) {
  if (!apiKey) return null;
  const inputHash = stableHash(JSON.stringify({
    model,
    schemaName,
    schema,
    content,
    cacheKey,
  }));
  const cached = readCache(cachePath, inputHash);
  if (cached) return cached;

  let lastError;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 120000);
    try {
      const response = await fetch(OPENAI_RESPONSES_URL, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model,
          reasoning: {
            effort: 'minimal',
          },
          input: content,
          text: {
            format: {
              type: 'json_schema',
              name: schemaName,
              schema,
              strict: true,
            },
          },
        }),
        signal: controller.signal,
      });
      const payload = JSON.parse(await response.text());
      if (!response.ok) {
        const error = new Error(`OpenAI ${response.status}: ${payload?.error?.message || 'request failed'}`);
        if (!isRetryableError(error, response.status) || attempt === 3) throw error;
        lastError = error;
        await sleep(1500 * (attempt + 1));
        continue;
      }
      const outputText = extractResponseText(payload);
      const parsed = JSON.parse(outputText);
      const result = {
        responseId: payload.id || '',
        model: payload.model || model,
        parsed,
      };
      writeCache(cachePath, inputHash, result);
      return result;
    } catch (error) {
      lastError = error;
      if (!isRetryableError(error) || attempt === 3) throw error;
      await sleep(1500 * (attempt + 1));
    } finally {
      clearTimeout(timer);
    }
  }
  throw lastError;
}

function buildCommentaryPrompt({ source, paragraphs }) {
  const renderedParagraphs = paragraphs
    .map((paragraph, index) => `[Paragraph ${index + 1}]\n${normalizeWhitespace(paragraph)}`)
    .join('\n\n');
  return [
    `Source title: ${source.title}`,
    `Source timestamp: ${source.timestamp}`,
    'Extract all forward-looking market predictions from Alma commentary.',
    'Include centroid, pivot, target, support, resistance, pinning, reversion, stabilization, rejection, breakout, whipsaw, vol-compression, and vol-expansion calls when they imply expected behavior.',
    'Ignore pure macro or geopolitical background unless it directly implies an actionable tradable market expectation.',
    'Use one instrument per prediction. If Alma explicitly names a market, keep that exact symbol. If no defensible instrument is present, leave instrument blank.',
    'prediction_text should preserve the substantive claim in a complete short sentence. condition should contain only the trigger if one exists. expected_result must be actionable and testable, not a restatement.',
    '',
    renderedParagraphs,
  ].join('\n');
}

function buildChatPrompt({ source, messages }) {
  const renderedMessages = messages
    .map((message) => `TIMESTAMP: ${message.timestamp}\nTEXT: ${normalizeWhitespace(message.text)}`)
    .join('\n\n---\n\n');
  return [
    `Chat title: ${source.title}`,
    'Extract only forward-looking market predictions from Alma chat messages.',
    'Return one row per distinct prediction. Use the exact TIMESTAMP value from the source message in made_at.',
    'Ignore admin chatter, support or subscription issues, social replies, references to reading another post, and retrospective comments unless the same message also contains a fresh forward-looking market call.',
    'Include centroid, pivot, target, support, resistance, pinning, reversion, stabilization, rejection, breakout, whipsaw, vol-compression, and vol-expansion calls when they imply expected behavior.',
    'Use one instrument per prediction. If no defensible instrument is present, leave instrument blank.',
    'prediction_text should preserve the substantive claim in a complete short sentence. condition should contain only the trigger if one exists. expected_result must be actionable and testable.',
    '',
    renderedMessages,
  ].join('\n');
}

function buildHeatmapPrompt({ source, contextText, descriptionText, imageCount }) {
  return [
    `Source title: ${source.title}`,
    `Source timestamp: ${source.timestamp}`,
    `Attached images: ${imageCount}`,
    'These are OptionsDepth heatmap images from Alma posts.',
    'Use the image plus nearby post context to extract only high-confidence market predictions visible in the heatmap.',
    'Focus on support/brake zones, pinning magnets, likely reversion levels, rejection zones, sticky zones, or likely price travel corridors.',
    'Prefer SPX for level ranges in the 5000-8000 area unless the surrounding text explicitly indicates ES instead.',
    'If a level or instrument is unreadable, skip it rather than guessing.',
    'prediction_text should explain what the heatmap is signaling. condition should contain only the trigger if one exists. expected_result must be actionable and testable.',
    '',
    'Nearby text context:',
    contextText || '(none)',
    '',
    'Linked heatmap description text:',
    descriptionText || '(none)',
  ].join('\n');
}

function readNearbyHeatmapContext(sourceText = '') {
  const marker = 'OptionsDepth Heatmap';
  const source = String(sourceText || '');
  const index = source.indexOf(marker);
  if (index < 0) return '';
  const before = source.slice(Math.max(0, index - 1600), index);
  const after = source.slice(index, Math.min(source.length, index + 400));
  return normalizeWhitespace(`${before}\n\n${after}`);
}

function findHeatmapImages(postDir) {
  const htmlPath = path.join(postDir, 'source.html');
  if (!fs.existsSync(htmlPath)) return [];
  const html = fs.readFileSync(htmlPath, 'utf8');
  const markerIndex = html.indexOf('OptionsDepth Heatmap');
  if (markerIndex < 0) return [];
  const remainder = html.slice(markerIndex);
  const segment = remainder.split('SCRIPT INPUTS')[0];
  const filenames = Array.from(new Set(
    Array.from(segment.matchAll(/\/public\/images\/([^"'?&\\]+(?:png|jpg|jpeg|webp|gif))/gi))
      .map((match) => decodeURIComponent(match[1] || ''))
      .filter(Boolean),
  ));
  const candidates = [];
  for (const filename of filenames) {
    const exactImage = path.join(postDir, 'images', filename);
    const exactLinked = path.join(postDir, 'linked-content', filename);
    if (fs.existsSync(exactImage)) candidates.push(exactImage);
    if (fs.existsSync(exactLinked)) candidates.push(exactLinked);

    const stem = path.basename(filename, path.extname(filename));
    for (const dirName of ['images', 'linked-content']) {
      const dirPath = path.join(postDir, dirName);
      if (!fs.existsSync(dirPath)) continue;
      for (const localName of fs.readdirSync(dirPath)) {
        if (path.basename(localName, path.extname(localName)) === stem) {
          candidates.push(path.join(dirPath, localName));
        }
      }
    }
  }
  return Array.from(new Set(candidates)).slice(0, 3);
}

function readHeatmapDescription(postDir) {
  const linkedDir = path.join(postDir, 'linked-content');
  if (!fs.existsSync(linkedDir)) return '';
  const files = fs.readdirSync(linkedDir)
    .filter((name) => name.toLowerCase().endsWith('.txt'))
    .filter((name) => /description|heatmap/i.test(name))
    .sort();
  const chunks = [];
  for (const name of files.slice(0, 3)) {
    chunks.push(fs.readFileSync(path.join(linkedDir, name), 'utf8'));
  }
  return normalizeWhitespace(chunks.join('\n\n'));
}

function createUnit(base = {}) {
  return {
    predictionText: base.predictionText || '',
    rawPredictionText: base.predictionText || '',
    contextText: base.contextText || '',
    condition: base.condition || '',
    expected: base.expected || '',
    basis: base.basis || 'commentary',
    sectionKind: base.sectionKind || 'commentary',
    origin: base.origin || '',
    madeAt: base.madeAt || '',
    sourceType: base.sourceType || '',
    sourceTitle: base.sourceTitle || '',
    sourcePath: base.sourcePath || '',
    instrument: base.instrument || '',
    instrumentFamily: base.instrumentFamily || '',
  };
}

function chunkMessages(messages = [], targetChars = 12000) {
  const chunks = [];
  let current = [];
  let currentSize = 0;
  for (const message of messages) {
    const size = (message.text || '').length + 64;
    if (current.length > 0 && currentSize + size > targetChars) {
      chunks.push(current);
      current = [];
      currentSize = 0;
    }
    current.push(message);
    currentSize += size;
  }
  if (current.length > 0) chunks.push(current);
  return chunks;
}

function createAlmaLlmExtractor({ workspaceRoot, outputRoot, cacheRoot: providedCacheRoot }) {
  const env = loadEnvFile(path.join(workspaceRoot, '.env'), process.env);
  const apiKey = env.OPENAI_API_KEY || process.env.OPENAI_API_KEY || '';
  const model = env.ALMA_OPENAI_MODEL || env.OPENAI_MODEL || DEFAULT_MODEL;
  const cacheRoot = providedCacheRoot || path.join(outputRoot, 'llm-cache');

  async function extractPostCommentaryPredictions(source, paragraphs = []) {
    const usable = paragraphs.map((value) => normalizeWhitespace(value)).filter(Boolean);
    if (!apiKey || usable.length === 0) return [];
    const cachePath = path.join(cacheRoot, 'commentary', `${path.basename(path.dirname(source.sourcePath))}.json`);
    const content = [
      {
        role: 'system',
        content: [toInputText('Return only the requested JSON. Do not add prose.')],
      },
      {
        role: 'user',
        content: [toInputText(buildCommentaryPrompt({ source, paragraphs: usable }))],
      },
    ];
    const result = await callOpenAiJson({
      apiKey,
      model,
      cachePath,
      cacheKey: COMMENTARY_CACHE_VERSION,
      schemaName: 'alma_post_commentary_predictions',
      schema: TEXT_PREDICTION_SCHEMA,
      content,
    });
    return (result?.parsed?.predictions || [])
      .map(normalizePredictionItem)
      .filter((item) => isMeaningfulPrediction(item, usable.join('\n\n')))
      .map((item) => createUnit({
        ...item,
        contextText: usable.join('\n\n'),
        basis: 'commentary',
        sectionKind: 'commentary',
        madeAt: source.timestamp,
        sourceType: source.sourceType,
        sourceTitle: source.title,
        sourcePath: source.sourcePath,
      }));
  }

  async function extractTextBlockCommentaryPredictions(source, textBlock, madeAt, origin) {
    const usable = normalizeWhitespace(textBlock);
    if (!apiKey || !usable) return [];
    const cachePath = path.join(
      cacheRoot,
      'commentary-blocks',
      `${path.basename(path.dirname(source.sourcePath))}-${stableHash(`${origin}::${madeAt}::${usable}`)}.json`,
    );
    const content = [
      {
        role: 'system',
        content: [toInputText('Return only the requested JSON. Do not add prose.')],
      },
      {
        role: 'user',
        content: [toInputText(buildCommentaryPrompt({ source: { ...source, timestamp: madeAt }, paragraphs: [usable] }))],
      },
    ];
    const result = await callOpenAiJson({
      apiKey,
      model,
      cachePath,
      cacheKey: COMMENTARY_CACHE_VERSION,
      schemaName: 'alma_text_block_predictions',
      schema: TEXT_PREDICTION_SCHEMA,
      content,
    });
    return (result?.parsed?.predictions || [])
      .map(normalizePredictionItem)
      .filter((item) => isMeaningfulPrediction(item, usable))
      .map((item) => createUnit({
        ...item,
        contextText: usable,
        basis: 'commentary',
        sectionKind: 'commentary',
        origin,
        madeAt,
        sourceType: source.sourceType,
        sourceTitle: source.title,
        sourcePath: source.sourcePath,
      }));
  }

  async function extractChatPredictions(source, messages = []) {
    if (!apiKey || messages.length === 0) return [];
    const messageMap = new Map(messages.map((message) => [message.timestamp, message]));
    const chunks = chunkMessages(messages);
    const units = [];

    for (let index = 0; index < chunks.length; index += 1) {
      const chunk = chunks[index];
      const cachePath = path.join(cacheRoot, 'chat', `${path.basename(path.dirname(source.sourcePath))}-chunk-${String(index + 1).padStart(3, '0')}.json`);
      const content = [
        {
          role: 'system',
          content: [toInputText('Return only the requested JSON. Do not add prose.')],
        },
        {
          role: 'user',
          content: [toInputText(buildChatPrompt({ source, messages: chunk }))],
        },
      ];
      const result = await callOpenAiJson({
        apiKey,
        model,
        cachePath,
        cacheKey: `${CHAT_CACHE_VERSION}-${index}`,
        schemaName: 'alma_chat_predictions',
        schema: CHAT_PREDICTION_SCHEMA,
        content,
      });
      for (const rawItem of result?.parsed?.predictions || []) {
        const madeAt = String(rawItem.made_at || '').trim();
        if (!messageMap.has(madeAt)) continue;
        const item = normalizePredictionItem(rawItem);
        const sourceMessage = messageMap.get(madeAt);
        if (!isMeaningfulPrediction(item, sourceMessage?.text || '')) continue;
        units.push(createUnit({
          ...item,
          contextText: sourceMessage.text,
          basis: 'commentary',
          sectionKind: 'commentary',
          origin: 'chat_prediction',
          madeAt,
          sourceType: source.sourceType,
          sourceTitle: source.title,
          sourcePath: source.sourcePath,
        }));
      }
    }

    return units;
  }

  async function extractHeatmapPredictions(source) {
    if (!apiKey) return [];
    const postDir = path.dirname(path.join(path.dirname(path.dirname(outputRoot)), source.sourcePath));
    const heatmapImages = findHeatmapImages(postDir);
    if (heatmapImages.length === 0) return [];
    const cachePath = path.join(cacheRoot, 'heatmap', `${path.basename(postDir)}.json`);
    const contextText = readNearbyHeatmapContext(fs.readFileSync(path.join(postDir, 'content.txt'), 'utf8'));
    const descriptionText = readHeatmapDescription(postDir);
    const content = [
      {
        role: 'system',
        content: [toInputText('Return only the requested JSON. Do not add prose.')],
      },
      {
        role: 'user',
        content: [
          toInputText(buildHeatmapPrompt({
            source,
            contextText,
            descriptionText,
            imageCount: heatmapImages.length,
          })),
          ...heatmapImages.map(toInputImage),
        ],
      },
    ];
    const result = await callOpenAiJson({
      apiKey,
      model,
      cachePath,
      cacheKey: HEATMAP_CACHE_VERSION,
      schemaName: 'alma_heatmap_predictions',
      schema: TEXT_PREDICTION_SCHEMA,
      content,
    });
    return (result?.parsed?.predictions || [])
      .map(normalizePredictionItem)
      .filter((item) => isMeaningfulPrediction(item, `${contextText}\n${descriptionText}`))
      .map((item) => createUnit({
        ...item,
        contextText,
        basis: 'optiondepth_heatmap',
        sectionKind: 'optiondepth_heatmap',
        origin: 'post_optiondepth_heatmap',
        madeAt: source.timestamp,
        sourceType: source.sourceType,
        sourceTitle: source.title,
        sourcePath: source.sourcePath,
      }));
  }

  return {
    enabled: Boolean(apiKey),
    model,
    extractChatPredictions,
    extractHeatmapPredictions,
    extractPostCommentaryPredictions,
    extractTextBlockCommentaryPredictions,
  };
}

module.exports = {
  createAlmaLlmExtractor,
};
