#!/usr/bin/env node

const fs = require('node:fs');
const path = require('node:path');

const DEFAULT_CDP_HTTP_URL = process.env.PLAYWRIGHT_CDP_URL || 'http://127.0.0.1:58210';
const DEFAULT_OUTPUT_DIR = path.resolve(process.cwd(), 'artifacts/alma-research');
const PUBLICATION_ID = 2679129;
const PUBLICATION_BASE_URL = 'https://stochvoltrader.substack.com';
const SUBSTACK_BASE_URL = 'https://substack.com';
const ARCHIVE_PAGE_LIMIT = 25;
const COMMUNITY_PAGE_LIMIT = 25;
const ALMA_USER_ID = 204864402;
const ALMA_HANDLE = 'alma271828';
const SCRIPT_VERSION = 4;
const APPROVED_SKIPS_FILENAME = 'approved-skips.json';
const DEFAULT_APPROVED_SKIPS = [
  {
    pattern: 'drive.google.com/',
    reason: 'Approved by user on 2026-03-15: skip Google Drive links we do not have access to.',
  },
  {
    pattern: 'drive.usercontent.google.com/',
    reason: 'Approved by user on 2026-03-15: skip Google Drive download endpoints we do not have access to.',
  },
  {
    pattern: 'drive.google.com/file/d/1fHY4pxnVLCicnOSzS1PARdwQG5K4EKwS',
    reason: 'Approved by user on 2026-03-15: inaccessible Google Drive script bundle from "Reversion scripts".',
  },
];
const INTERNAL_SUBSTACK_ALIASES = [
  {
    brokenUrl: `${PUBLICATION_BASE_URL}/p/us-china-war-trump-putin-xi-and-the`,
    canonicalUrl: `${PUBLICATION_BASE_URL}/p/board-of-peace-greenland-hidden-left`,
    title: 'Board of Peace, Greenland, Hidden Left-tail and Kurtosis | Geopolitical Developments [FREE]',
    archivePath: 'posts/2026-01-20_board-of-peace-greenland-hidden-left',
    reason: 'Approved by user on 2026-03-15: broken internal Alma slug should resolve to the archived January 20, 2026 post.',
  },
];

class ReviewRequiredError extends Error {
  constructor(message, details = {}) {
    super(message);
    this.name = 'ReviewRequiredError';
    this.details = details;
  }
}

function parseArgs(argv) {
  const out = {};
  for (let i = 2; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith('--')) continue;
    const key = token.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      out[key] = '1';
      continue;
    }
    out[key] = next;
    i += 1;
  }
  return out;
}

function toBool(value) {
  if (value === undefined || value === null) return false;
  const normalized = String(value).trim().toLowerCase();
  return normalized === '1' || normalized === 'true' || normalized === 'yes';
}

function nowIso() {
  return new Date().toISOString();
}

function ensureDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function readJsonIfExists(filePath, fallback = null) {
  try {
    return JSON.parse(fs.readFileSync(filePath, 'utf8'));
  } catch {
    return fallback;
  }
}

function writeJson(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function writeText(filePath, value) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, value, 'utf8');
}

function writeBinary(filePath, buffer) {
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, buffer);
}

function slugify(value, fallback = 'item') {
  const normalized = String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 120);
  return normalized || fallback;
}

function isoDatePart(value, fallback = 'unknown-date') {
  const match = String(value || '').match(/^\d{4}-\d{2}-\d{2}/);
  return match ? match[0] : fallback;
}

function formatFolderName(isoValue, slug, fallbackSlug) {
  const datePart = isoDatePart(isoValue);
  const safeSlug = slugify(slug, fallbackSlug);
  return `${datePart}_${safeSlug}`;
}

function uniqueBy(items, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of items) {
    const key = keyFn(item);
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

function summarizeText(value) {
  return String(value || '')
    .replace(/\r/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function stripHtmlToText(html) {
  return summarizeText(
    String(html || '')
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/&nbsp;/gi, ' ')
      .replace(/&amp;/gi, '&')
      .replace(/&quot;/gi, '"')
      .replace(/&#39;/gi, "'"),
  );
}

function sortDescByIso(items, key) {
  return items.slice().sort((a, b) => {
    const aMs = Date.parse(a?.[key] || '') || 0;
    const bMs = Date.parse(b?.[key] || '') || 0;
    return bMs - aMs;
  });
}

function safeFilename(value, fallback = 'file') {
  const cleaned = String(value || '')
    .replace(/[?#].*$/, '')
    .replace(/^https?:\/\//, '')
    .replace(/[^A-Za-z0-9._-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 140);
  return cleaned || fallback;
}

function normalizeUrl(url, baseUrl) {
  try {
    return new URL(String(url || ''), baseUrl).toString();
  } catch {
    return null;
  }
}

function isSsrnUrl(url) {
  try {
    const hostname = new URL(String(url || '')).hostname;
    return hostname === 'download.ssrn.com' || /(^|\.)ssrn\.com$/i.test(hostname);
  } catch {
    return false;
  }
}

function deriveSsrnAbstractUrl(url) {
  const normalized = normalizeUrl(url, 'https://papers.ssrn.com');
  if (!normalized) return null;
  try {
    const parsed = new URL(normalized);
    if (!/ssrn\.com$/i.test(parsed.hostname) && !/download\.ssrn\.com$/i.test(parsed.hostname)) {
      return null;
    }
    const abstractId = parsed.searchParams.get('abstract_id')
      || parsed.searchParams.get('abstractid')
      || parsed.searchParams.get('abstractId');
    if (abstractId) {
      return `https://papers.ssrn.com/sol3/papers.cfm?abstract_id=${encodeURIComponent(abstractId)}`;
    }
    return parsed.hostname.includes('papers.ssrn.com') && parsed.pathname.includes('/papers.cfm')
      ? parsed.toString()
      : null;
  } catch {
    return null;
  }
}

function withHashMarker(url, marker) {
  const parsed = new URL(url);
  parsed.hash = marker;
  return parsed.toString();
}

function canonicalizeComparableUrl(url, baseUrl = PUBLICATION_BASE_URL) {
  const normalized = normalizeUrl(url, baseUrl);
  if (!normalized) return null;
  try {
    const parsed = new URL(normalized);
    parsed.hash = '';
    parsed.search = '';
    return parsed.toString().replace(/\/+$/, '');
  } catch {
    return null;
  }
}

function findInternalSubstackAlias(url) {
  const comparable = canonicalizeComparableUrl(url);
  if (!comparable) return null;
  return INTERNAL_SUBSTACK_ALIASES.find((alias) => canonicalizeComparableUrl(alias.brokenUrl) === comparable) || null;
}

function getBinaryUrlFallback(url) {
  const normalized = normalizeUrl(url, 'https://example.com');
  if (!normalized) return null;
  try {
    const parsed = new URL(normalized);
    if (parsed.protocol === 'https:' && parsed.hostname === 'docs.finance.free.fr') {
      parsed.protocol = 'http:';
      return parsed.toString();
    }
    return null;
  } catch {
    return null;
  }
}

function isSubstackUrl(url) {
  return /^https:\/\/([^.]+\.)?substack\.com\//i.test(String(url || ''))
    || /^https:\/\/stochvoltrader\.substack\.com\//i.test(String(url || ''));
}

function isAlmaThread(thread) {
  const communityPost = thread?.communityPost || null;
  const user = thread?.user || communityPost?.user || null;
  return communityPost?.user_id === ALMA_USER_ID
    || user?.id === ALMA_USER_ID
    || user?.handle === ALMA_HANDLE
    || communityPost?.user?.handle === ALMA_HANDLE;
}

function removePath(targetPath) {
  fs.rmSync(targetPath, { recursive: true, force: true });
}

function ensureApprovedSkipsFile(outputDir) {
  const skipsPath = path.join(outputDir, APPROVED_SKIPS_FILENAME);
  const existing = readJsonIfExists(skipsPath, []);
  const merged = dedupeBy(
    [...(Array.isArray(existing) ? existing : []), ...DEFAULT_APPROVED_SKIPS],
    (item) => String(item?.pattern || ''),
  );
  writeJson(skipsPath, merged);
  return merged;
}

function findApprovedSkip(urls, approvedSkips) {
  for (const skip of approvedSkips) {
    for (const url of urls.filter(Boolean)) {
      if (String(url).includes(skip.pattern)) return skip;
    }
  }
  return null;
}

function isAutoSkippableLinkedContext(context) {
  const type = String(context?.type || '');
  return type.includes('linked');
}

function buildSkippedResourceResult({
  url,
  finalUrl,
  reason,
  status = null,
  contentType = null,
  error = null,
}) {
  return {
    kind: 'skipped',
    url,
    finalUrl: finalUrl || null,
    reason,
    status,
    contentType,
    error,
  };
}

function parseContentDispositionFilename(contentDisposition) {
  const raw = String(contentDisposition || '');
  const utf8Match = raw.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match) return decodeURIComponent(utf8Match[1]);
  const basicMatch = raw.match(/filename="?([^";]+)"?/i);
  return basicMatch ? basicMatch[1] : null;
}

function extensionFromContentType(contentType) {
  const normalized = String(contentType || '').toLowerCase().split(';')[0].trim();
  if (!normalized) return '';
  const map = {
    'application/pdf': '.pdf',
    'application/json': '.json',
    'application/zip': '.zip',
    'application/x-zip-compressed': '.zip',
    'text/plain': '.txt',
    'text/csv': '.csv',
    'text/html': '.html',
    'image/png': '.png',
    'image/jpeg': '.jpg',
    'image/jpg': '.jpg',
    'image/webp': '.webp',
    'image/gif': '.gif',
    'video/mp4': '.mp4',
    'audio/mpeg': '.mp3',
  };
  return map[normalized] || '';
}

function isHtmlContentType(contentType) {
  return String(contentType || '').toLowerCase().includes('text/html');
}

function looksLikeAuthGate(url, finalUrl, contentType, bodyBuffer) {
  const urls = [url, finalUrl].filter(Boolean).map((value) => String(value).toLowerCase());
  if (urls.some((value) => value.includes('accounts.google.com/'))) return true;
  if (urls.some((value) => value.includes('/signin') || value.includes('/login'))) return true;
  if (!isHtmlContentType(contentType)) return false;
  const head = bodyBuffer.toString('utf8', 0, Math.min(bodyBuffer.length, 5000)).toLowerCase();
  return head.includes('servicelogin')
    || head.includes('interactive login')
    || head.includes('sign in')
    || head.includes('identifier');
}

function unwrapSubstackMediaUrl(url) {
  const raw = String(url || '').replace(/&quot;.*$/, '').trim();
  if (!raw) return raw;
  const embedded = raw.match(/https?:\/\/[^"'\\\s<>)]+/g);
  if (embedded && embedded.length > 0) {
    return embedded[embedded.length - 1];
  }
  const encoded = raw.match(/https?%3A%2F%2F[^"'\\\s<>)]+/i);
  if (encoded) {
    return decodeURIComponent(encoded[0]);
  }
  return raw;
}

function dedupeBy(items, keyFn) {
  return uniqueBy(items.filter(Boolean), keyFn);
}

function looksLikeDownloadLink(link, canonicalUrl) {
  const href = String(link?.href || '');
  const text = String(link?.text || '');
  const lower = `${href} ${text}`.toLowerCase();
  if (!href || href === canonicalUrl) return false;
  if (lower.includes('/comment/') || lower.includes('/comments') || lower.includes('/subscribe')) return false;
  if (lower.includes('enable-javascript.com')) return false;
  if (/^https:\/\/open\.substack\.com\//i.test(href)) return true;
  if (/^https:\/\/substack\.com\/chat\//i.test(href)) return true;
  if (/^https:\/\/[^/]+\.substack\.com\/p\//i.test(href)) return true;
  if (/\.(png|jpe?g|gif|webp|pdf|txt|csv|json|zip|xlsx?|docx?|pptx?|mp4|mp3)([?#].*)?$/i.test(href)) return true;
  if (/(download|attached|attachment|script|source code|pine|pdf|file bundle|zip|text file)/i.test(text)) return true;
  if (/(drive\.google\.com|docs\.google\.com|dropbox\.com|box\.com|raw\.githubusercontent\.com|gist\.github\.com|github\.com|tradingview\.com|s3\.amazonaws\.com|onedrive\.live\.com|1drv\.ms)/i.test(href)) return true;
  return false;
}

function extractUrlsFromText(value) {
  const matches = String(value || '').match(/https?:\/\/[^\s<>)"']+/g) || [];
  return dedupeBy(matches.map((url) => unwrapSubstackMediaUrl(url)), (url) => url);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientHttpStatus(status) {
  const code = Number(status);
  return code === 429 || code === 502 || code === 503 || code === 504;
}

function formatRetryBackoff(attempt) {
  return Math.min(1_000 * (2 ** (attempt - 1)), 8_000);
}

function formatRetryContext(result, error) {
  if (result) {
    return `status=${result.status || 'unknown'} url=${result.url || 'unknown'}`;
  }
  if (error) {
    return `error=${error.message || String(error)}`;
  }
  return 'unknown';
}

async function runWithRetry({
  label,
  maxAttempts = 4,
  run,
  shouldRetryResult = () => false,
}) {
  let lastResult = null;
  let lastError = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const result = await run(attempt);
      lastResult = result;
      lastError = null;
      if (!shouldRetryResult(result)) return result;
      if (attempt === maxAttempts) return result;
      const backoffMs = formatRetryBackoff(attempt);
      console.warn(`[alma-export] retry ${label} attempt=${attempt}/${maxAttempts} ${formatRetryContext(result)} waitMs=${backoffMs}`);
      await sleep(backoffMs);
      continue;
    } catch (error) {
      lastError = error;
      if (attempt === maxAttempts) break;
      const backoffMs = formatRetryBackoff(attempt);
      console.warn(`[alma-export] retry ${label} attempt=${attempt}/${maxAttempts} ${formatRetryContext(null, error)} waitMs=${backoffMs}`);
      await sleep(backoffMs);
    }
  }

  if (lastError) {
    throw new Error(`Failed ${label} after ${maxAttempts} attempts: ${lastError.message || lastError}`);
  }
  return lastResult;
}

async function fetchJson(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  return response.json();
}

async function fetchTargets(cdpHttpUrl) {
  return fetchJson(`${cdpHttpUrl}/json/list`);
}

function selectTarget(targets, predicate, description) {
  const target = targets.find(predicate);
  if (!target || !target.webSocketDebuggerUrl) {
    const available = targets
      .map((item) => `${item.type || 'unknown'} ${item.url || ''}`.trim())
      .join('\n');
    throw new Error(`Could not find ${description} CDP target.\nAvailable targets:\n${available}`);
  }
  return target;
}

class CdpPage {
  constructor(target) {
    this.target = target;
    this.ws = null;
    this.nextId = 0;
    this.pending = new Map();
  }

  async connect() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;
    await new Promise((resolve, reject) => {
      const ws = new WebSocket(this.target.webSocketDebuggerUrl);
      this.ws = ws;
      const timeout = setTimeout(() => {
        reject(new Error(`Timed out connecting to ${this.target.url}`));
      }, 10_000);
      ws.onopen = () => {
        clearTimeout(timeout);
        resolve();
      };
      ws.onerror = (error) => {
        clearTimeout(timeout);
        reject(error);
      };
      ws.onmessage = (event) => {
        const message = JSON.parse(event.data.toString());
        if (!message.id) return;
        const pending = this.pending.get(message.id);
        if (!pending) return;
        this.pending.delete(message.id);
        if (message.error) {
          pending.reject(new Error(message.error.message || 'CDP error'));
          return;
        }
        pending.resolve(message.result);
      };
      ws.onclose = () => {
        for (const [, pending] of this.pending) {
          pending.reject(new Error(`CDP socket closed for ${this.target.url}`));
        }
        this.pending.clear();
      };
    });
    await this.send('Runtime.enable');
  }

  async send(method, params = {}) {
    await this.connect();
    return new Promise((resolve, reject) => {
      const id = ++this.nextId;
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async evaluate(expression, { awaitPromise = true } = {}) {
    const result = await this.send('Runtime.evaluate', {
      expression,
      awaitPromise,
      returnByValue: true,
    });
    return result.result?.value;
  }

  async call(fnSource, ...args) {
    const expression = `(${fnSource})(...${JSON.stringify(args)})`;
    return this.evaluate(expression, { awaitPromise: true });
  }

  async close() {
    if (!this.ws) return;
    const socket = this.ws;
    this.ws = null;
    this.pending.clear();
    await new Promise((resolve) => {
      socket.onclose = () => resolve();
      socket.close();
    }).catch(() => {});
  }
}

async function createCdpPages(cdpHttpUrl) {
  const targets = await fetchTargets(cdpHttpUrl);
  const publicationTarget = selectTarget(
    targets,
    (target) => typeof target.url === 'string' && target.url.startsWith(PUBLICATION_BASE_URL),
    'Alma publication page',
  );
  const chatTarget = selectTarget(
    targets,
    (target) => typeof target.url === 'string' && target.url.startsWith(`${SUBSTACK_BASE_URL}/chat`),
    'Substack chat page',
  );
  const inboxTarget = targets.find(
    (target) => typeof target.url === 'string' && target.url.startsWith(`${SUBSTACK_BASE_URL}/inbox`),
  ) || chatTarget;

  const publicationPage = new CdpPage(publicationTarget);
  const chatPage = new CdpPage(chatTarget);
  const inboxPage = inboxTarget.id === chatTarget.id ? chatPage : new CdpPage(inboxTarget);
  const pages = [publicationPage, chatPage];
  if (inboxPage !== chatPage) pages.push(inboxPage);
  await Promise.all(pages.map((page) => page.connect()));
  return { publicationPage, chatPage, inboxPage };
}

const FETCH_JSON_FN = async function fetchJsonInPage(targetUrl) {
  const response = await fetch(targetUrl, { credentials: 'include' });
  const text = await response.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    json = null;
  }
  return {
    ok: response.ok,
    status: response.status,
    url: response.url,
    text,
    json,
  };
};

const FETCH_HTML_BUNDLE_FN = async function fetchHtmlBundleInPage(targetUrl) {
  const response = await fetch(targetUrl, { credentials: 'include' });
  const html = await response.text();
  const doc = new DOMParser().parseFromString(html, 'text/html');
  const root = doc.querySelector('article') || doc.querySelector('main') || doc.body;
  const blocks = [];
  const selector = 'h1, h2, h3, h4, p, li, blockquote, figcaption, pre';
  const nodes = root ? Array.from(root.querySelectorAll(selector)) : [];
  if (nodes.length > 0) {
    for (const node of nodes) {
      const text = (node.textContent || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
      if (text) blocks.push(text);
    }
  } else if (doc.body) {
    const fallbackText = (doc.body.innerText || '')
      .replace(/\u00a0/g, ' ')
      .replace(/[ \t]+\n/g, '\n')
      .replace(/\n{3,}/g, '\n\n')
      .trim();
    if (fallbackText) blocks.push(fallbackText);
  }
  const links = root
    ? Array.from(root.querySelectorAll('a[href]')).map((anchor, index) => ({
      index,
      href: anchor.href,
      text: (anchor.textContent || '').replace(/\s+/g, ' ').trim(),
    }))
    : [];
  const images = root
    ? Array.from(root.querySelectorAll('img[src]')).map((image, index) => ({
      index,
      src: image.getAttribute('src'),
      alt: image.getAttribute('alt') || null,
      title: image.getAttribute('title') || null,
    }))
    : [];
  return {
    ok: response.ok,
    status: response.status,
    url: response.url,
    html,
    title: (doc.querySelector('h1')?.textContent || doc.title || '').trim() || null,
    metaDescription: doc.querySelector('meta[name="description"]')?.getAttribute('content') || null,
    text: blocks.join('\n\n'),
    links,
    images,
  };
};

const CAPTURE_CURRENT_PAGE_BUNDLE_FN = async function captureCurrentPageBundleInPage() {
  const doc = document;
  const root = doc.querySelector('article') || doc.querySelector('main') || doc.body;
  const blocks = [];
  const selector = 'h1, h2, h3, h4, p, li, blockquote, figcaption, pre';
  const nodes = root ? Array.from(root.querySelectorAll(selector)) : [];
  if (nodes.length > 0) {
    for (const node of nodes) {
      const text = (node.textContent || '')
        .replace(/\u00a0/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
      if (text) blocks.push(text);
    }
  } else if (doc.body) {
    const fallbackText = (doc.body.innerText || '')
      .replace(/\u00a0/g, ' ')
      .replace(/[ \t]+\n/g, '\n')
      .replace(/\n{3,}/g, '\n\n')
      .trim();
    if (fallbackText) blocks.push(fallbackText);
  }
  const links = root
    ? Array.from(root.querySelectorAll('a[href]')).map((anchor, index) => ({
      index,
      href: anchor.href,
      text: (anchor.textContent || '').replace(/\s+/g, ' ').trim(),
    }))
    : [];
  const images = root
    ? Array.from(root.querySelectorAll('img[src]')).map((image, index) => ({
      index,
      src: image.getAttribute('src'),
      alt: image.getAttribute('alt') || null,
      title: image.getAttribute('title') || null,
    }))
    : [];
  return {
    ok: true,
    status: 200,
    url: location.href,
    html: doc.documentElement ? doc.documentElement.outerHTML : '',
    title: (doc.querySelector('h1')?.textContent || doc.title || '').trim() || null,
    metaDescription: doc.querySelector('meta[name="description"]')?.getAttribute('content') || null,
    text: blocks.join('\n\n'),
    links,
    images,
  };
};

const FETCH_BINARY_BASE64_IN_PAGE_FN = async function fetchBinaryBase64InPage(targetUrl) {
  const response = await fetch(targetUrl, { credentials: 'include' });
  const buffer = await response.arrayBuffer();
  const bytes = new Uint8Array(buffer);
  let binary = '';
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }
  return {
    ok: response.ok,
    status: response.status,
    url: response.url,
    contentType: response.headers.get('content-type') || null,
    contentDisposition: response.headers.get('content-disposition') || null,
    base64: btoa(binary),
    size: bytes.length,
  };
};

async function fetchJsonPayloadWithRetry(page, url, label, maxAttempts = 5) {
  return runWithRetry({
    label,
    maxAttempts,
    run: async () => page.call(FETCH_JSON_FN.toString(), url),
    shouldRetryResult: (payload) => !payload || (!payload.ok && isTransientHttpStatus(payload.status)),
  });
}

async function fetchHtmlBundleWithRetry(page, url, label, maxAttempts = 5) {
  return runWithRetry({
    label,
    maxAttempts,
    run: async () => page.call(FETCH_HTML_BUNDLE_FN.toString(), url),
    shouldRetryResult: (bundle) => !bundle || (!bundle.ok && isTransientHttpStatus(bundle.status)),
  });
}

async function waitForPageReady(page, label, maxAttempts = 20) {
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const readyState = await page.evaluate('document.readyState', { awaitPromise: false }).catch(() => null);
    if (readyState === 'complete' || readyState === 'interactive') return;
    await sleep(500);
  }
  console.warn(`[alma-export] temp-tab not ready after wait label=${label}`);
}

async function openTemporaryTab(openerPage, url, label) {
  const marker = `codex-alma-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const markedUrl = withHashMarker(url, marker);
  await openerPage.evaluate(`window.open(${JSON.stringify(markedUrl)}, '_blank'); true;`, { awaitPromise: false });

  let target = null;
  for (let attempt = 1; attempt <= 20; attempt += 1) {
    const targets = await fetchTargets(DEFAULT_CDP_HTTP_URL);
    target = targets.find((item) => typeof item.url === 'string' && item.url.includes(marker));
    if (target?.webSocketDebuggerUrl) break;
    await sleep(500);
  }

  if (!target?.webSocketDebuggerUrl) {
    throw new ReviewRequiredError('Temporary browser tab could not be opened for linked resource capture.', {
      type: 'temp-tab-open',
      label,
      url,
    });
  }

  const tempPage = new CdpPage(target);
  await tempPage.connect();
  await waitForPageReady(tempPage, label);
  return { tempPage, marker, markedUrl };
}

async function closeTemporaryTab(tempPage) {
  if (!tempPage) return;
  await tempPage.send('Page.close').catch(() => {});
  await tempPage.close().catch(() => {});
}

async function withTemporaryTab(openerPage, url, label, run) {
  const { tempPage } = await openTemporaryTab(openerPage, url, label);
  try {
    return await run(tempPage);
  } finally {
    await closeTemporaryTab(tempPage);
  }
}

async function fetchPublicationSections(publicationPage) {
  const sections = await publicationPage.evaluate('JSON.stringify(window._preloads?.pub?.sections || [])', {
    awaitPromise: false,
  });
  try {
    return JSON.parse(sections);
  } catch {
    return [];
  }
}

async function fetchPublicationSessionState(publicationPage) {
  const raw = await publicationPage.evaluate(
    'JSON.stringify({ confirmedLogin: window._preloads?.confirmedLogin ?? null, isSubscribed: window._analyticsConfig?.properties?.is_subscribed ?? null, currentUrl: location.href, currentUser: window._preloads?.user?.name ?? null, publication: window._preloads?.pub?.name ?? null })',
    { awaitPromise: false },
  );
  try {
    return JSON.parse(raw);
  } catch {
    return {
      confirmedLogin: null,
      isSubscribed: null,
      currentUrl: null,
      currentUser: null,
      publication: null,
    };
  }
}

async function ensurePublicationAccess(publicationPage) {
  const sessionState = await fetchPublicationSessionState(publicationPage);
  if (!sessionState.confirmedLogin || !sessionState.isSubscribed) {
    throw new ReviewRequiredError('Publication browser session is not logged in with active subscriber access.', {
      type: 'publication-auth',
      sessionState,
    });
  }
  return sessionState;
}

async function fetchArchivePagesForScope(publicationPage, scope) {
  const pages = [];
  const posts = [];
  let offset = 0;
  while (true) {
    const params = new URLSearchParams({
      sort: 'new',
      search: '',
      offset: String(offset),
      limit: String(ARCHIVE_PAGE_LIMIT),
    });
    if (scope.sectionId) {
      params.set('section_id', String(scope.sectionId));
    }
    const archiveUrl = `${PUBLICATION_BASE_URL}/api/v1/archive?${params.toString()}`;
    const payload = await fetchJsonPayloadWithRetry(
      publicationPage,
      archiveUrl,
      `archive:${scope.label}:offset=${offset}`,
    );
    if (!payload.ok || !Array.isArray(payload.json)) {
      throw new Error(`Archive fetch failed for ${scope.label} offset=${offset}: ${payload.status}`);
    }
    const items = payload.json;
    if (items.length === 0) break;
    pages.push({
      scope,
      offset,
      limit: ARCHIVE_PAGE_LIMIT,
      count: items.length,
      archiveUrl,
      items,
    });
    posts.push(...items);
    console.log(`[alma-export] archive ${scope.label} offset=${offset} count=${items.length}`);
    if (items.length < ARCHIVE_PAGE_LIMIT) break;
    offset += ARCHIVE_PAGE_LIMIT;
  }
  return { pages, posts };
}

async function fetchArchivePosts(publicationPage) {
  const sections = await fetchPublicationSections(publicationPage);
  const scopes = [
    { label: 'root', sectionId: null, sectionSlug: null, sectionName: 'Root archive' },
    ...sections.map((section) => ({
      label: `section-${section.slug || section.id}`,
      sectionId: section.id,
      sectionSlug: section.slug || null,
      sectionName: section.name || null,
    })),
  ];

  const allPages = [];
  const allPosts = [];
  for (const scope of scopes) {
    const result = await fetchArchivePagesForScope(publicationPage, scope);
    allPages.push(...result.pages);
    allPosts.push(...result.posts);
  }
  return {
    sections,
    pages: allPages,
    posts: uniqueBy(allPosts, (item) => String(item.id)),
  };
}

async function fetchCommunityFeed(chatPage) {
  const pages = [];
  const threads = [];
  let before = null;
  const seenCursors = new Set();
  while (true) {
    const params = new URLSearchParams({ limit: String(COMMUNITY_PAGE_LIMIT) });
    if (before) params.set('before', before);
    const query = `?${params.toString()}`;
    const feedUrl = `${SUBSTACK_BASE_URL}/api/v1/community/publications/${PUBLICATION_ID}/posts${query}`;
    const payload = await fetchJsonPayloadWithRetry(
      chatPage,
      feedUrl,
      `community-feed:${before || 'latest'}`,
    );
    if (!payload.ok || !payload.json || !Array.isArray(payload.json.threads)) {
      throw new Error(`Community feed fetch failed before=${before || 'latest'} status=${payload.status}`);
    }
    const pageThreads = payload.json.threads;
    if (pageThreads.length === 0) break;
    pages.push({
      before,
      count: pageThreads.length,
      more: payload.json.more || false,
      moreAfter: payload.json.moreAfter || false,
      moreBefore: payload.json.moreBefore || false,
      threads: pageThreads,
    });
    threads.push(...pageThreads);
    console.log(`[alma-export] chat feed page before=${before || 'latest'} count=${pageThreads.length}`);
    const lastCreatedAt = pageThreads[pageThreads.length - 1]?.communityPost?.created_at || null;
    if (!payload.json.moreBefore || !lastCreatedAt || seenCursors.has(lastCreatedAt)) break;
    seenCursors.add(lastCreatedAt);
    before = lastCreatedAt;
  }
  return {
    pages,
    threads: uniqueBy(threads.filter(isAlmaThread), (item) => String(item?.communityPost?.id || '')),
  };
}

async function fetchInboxSnapshot(inboxPage) {
  const inboxUrl = `${SUBSTACK_BASE_URL}/api/v1/messages/inbox?tab=all`;
  const payload = await fetchJsonPayloadWithRetry(inboxPage, inboxUrl, 'inbox-snapshot');
  if (!payload.ok || !payload.json) {
    return {
      error: 'inbox_snapshot_unavailable',
      status: payload.status,
      ok: payload.ok,
      url: payload.url,
      rawText: payload.text,
    };
  }
  return payload.json;
}

async function fetchBinaryResource(url) {
  const response = await fetch(url, { redirect: 'follow' });
  const buffer = Buffer.from(await response.arrayBuffer());
  return {
    ok: response.ok,
    status: response.status,
    url,
    finalUrl: response.url,
    contentType: response.headers.get('content-type') || null,
    contentDisposition: response.headers.get('content-disposition') || null,
    buffer,
  };
}

async function fetchBinaryResourceWithRetry(url, label, maxAttempts = 4) {
  return runWithRetry({
    label,
    maxAttempts,
    run: async () => fetchBinaryResource(url),
    shouldRetryResult: (resource) => !resource || (!resource.ok && isTransientHttpStatus(resource.status)),
  });
}

async function fetchHtmlBundleNode(url) {
  const response = await fetch(url, { redirect: 'follow' });
  const html = await response.text();
  const titleMatch = html.match(/<title[^>]*>([^<]+)</i);
  const descriptionMatch = html.match(/<meta[^>]+name=["']description["'][^>]+content=["']([^"']*)/i);
  return {
    ok: response.ok,
    status: response.status,
    url: response.url,
    html,
    title: titleMatch ? titleMatch[1].trim() : null,
    metaDescription: descriptionMatch ? descriptionMatch[1].trim() : null,
    text: stripHtmlToText(html),
    links: [],
    images: [],
  };
}

async function fetchHtmlBundleNodeWithRetry(url, label, maxAttempts = 4) {
  return runWithRetry({
    label,
    maxAttempts,
    run: async () => fetchHtmlBundleNode(url),
    shouldRetryResult: (bundle) => !bundle || (!bundle.ok && isTransientHttpStatus(bundle.status)),
  });
}

async function saveSsrnResource({
  page,
  url,
  fileDir,
  filenameHint,
  context,
  archiveRoot,
}) {
  const abstractUrl = deriveSsrnAbstractUrl(url);
  if (!abstractUrl) {
    throw new ReviewRequiredError('Could not derive an SSRN abstract page for the linked resource.', {
      context,
      url,
    });
  }

  const existingArchive = findExistingSsrnArchive(archiveRoot, abstractUrl);
  if (existingArchive) {
    return saveExistingResourceReference({
      url,
      fileDir,
      filenameStem: filenameHint,
      context,
      finalUrl: abstractUrl,
      title: `SSRN ${abstractUrl}`,
      archiveRoot,
      archivedFolderRelative: existingArchive.folderRelative,
      reason: 'Reused an already archived SSRN resource copy to avoid duplicate challenge/rate-limited downloads.',
    });
  }

  return withTemporaryTab(page, abstractUrl, `ssrn:${filenameHint || 'resource'}`, async (tempPage) => {
    const bundle = await tempPage.call(CAPTURE_CURRENT_PAGE_BUNDLE_FN.toString());
    const pageText = `${bundle?.title || ''}\n${bundle?.text || ''}`.toLowerCase();
    if (pageText.includes('just a moment') || pageText.includes('attention required')) {
      throw new ReviewRequiredError('SSRN page is blocked by a browser challenge and could not be archived automatically.', {
        context,
        url,
        abstractUrl,
      });
    }

    const stem = safeFilename(filenameHint || bundle.title || 'ssrn-resource', 'ssrn-resource');
    const htmlPath = path.join(fileDir, `${stem}.html`);
    const textPath = path.join(fileDir, `${stem}.txt`);
    writeText(htmlPath, bundle.html || '');
    writeText(textPath, `${summarizeText(bundle.text)}\n`);

    const pdfLinks = dedupeBy(
      (bundle.links || []).filter((link) => /delivery\.cfm|download this paper|open pdf in browser/i.test(`${link.href || ''} ${link.text || ''}`)),
      (link) => link.href,
    );

    let pdfFile = null;
    for (const pdfLink of pdfLinks) {
      const pdfPayload = await runWithRetry({
        label: `ssrn-pdf:${filenameHint || stem}`,
        maxAttempts: 3,
        run: async () => tempPage.call(FETCH_BINARY_BASE64_IN_PAGE_FN.toString(), pdfLink.href),
        shouldRetryResult: (result) => !result || (!result.ok && isTransientHttpStatus(result.status)),
      });
      if (!pdfPayload?.ok || !/application\/pdf/i.test(pdfPayload.contentType || '')) {
        continue;
      }

      const dispositionName = parseContentDispositionFilename(pdfPayload.contentDisposition);
      let finalName = safeFilename(dispositionName || `${stem}.pdf`, `${stem}.pdf`);
      if (!path.extname(finalName)) finalName = `${finalName}.pdf`;
      const pdfPath = path.join(fileDir, finalName);
      writeBinary(pdfPath, Buffer.from(pdfPayload.base64 || '', 'base64'));
      pdfFile = {
        path: pdfPath,
        finalUrl: pdfPayload.url,
        contentType: pdfPayload.contentType,
        size: pdfPayload.size,
      };
      break;
    }

    return {
      kind: 'saved',
      url,
      finalUrl: bundle.url,
      title: bundle.title,
      htmlPath,
      textPath,
      pdf: pdfFile,
    };
  });
}

function saveInternalAliasReference({
  url,
  fileDir,
  filenameStem,
  context,
  archiveRoot,
  alias,
}) {
  const stem = safeFilename(filenameStem || alias.title || 'linked-alias', 'linked-alias');
  const htmlPath = path.join(fileDir, `${stem}.html`);
  const textPath = path.join(fileDir, `${stem}.txt`);
  const aliasPath = path.join(fileDir, `${stem}.alias.json`);
  const archiveTargetPath = archiveRoot ? path.join(archiveRoot, alias.archivePath) : null;

  if (!archiveTargetPath || !fs.existsSync(archiveTargetPath)) {
    throw new ReviewRequiredError('Internal Substack alias target is not present in the archive.', {
      context,
      url,
      alias,
      archiveTargetPath,
    });
  }

  const textBody = summarizeText([
    'Broken Internal Substack Link Alias',
    `Requested URL: ${url}`,
    `Canonical URL: ${alias.canonicalUrl}`,
    `Archived Folder: ${alias.archivePath}`,
    `Reason: ${alias.reason}`,
  ].join('\n'));
  const htmlBody = `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>${alias.title}</title>
  </head>
  <body>
    <h1>Broken Internal Substack Link Alias</h1>
    <p>Requested URL: <a href="${url}">${url}</a></p>
    <p>Canonical URL: <a href="${alias.canonicalUrl}">${alias.canonicalUrl}</a></p>
    <p>Archived Folder: ${alias.archivePath}</p>
    <p>Reason: ${alias.reason}</p>
  </body>
</html>
`;

  writeText(htmlPath, htmlBody);
  writeText(textPath, `${textBody}\n`);
  writeJson(aliasPath, {
    type: 'internal-substack-alias',
    createdAt: nowIso(),
    requestedUrl: url,
    canonicalUrl: alias.canonicalUrl,
    archivedFolder: alias.archivePath,
    title: alias.title,
    reason: alias.reason,
    context,
  });

  return {
    kind: 'aliased',
    url,
    finalUrl: alias.canonicalUrl,
    title: alias.title,
    htmlPath,
    textPath,
    aliasPath,
    archivedFolder: archiveTargetPath,
  };
}

function findExistingSsrnArchive(archiveRoot, abstractUrl) {
  const abstractId = (() => {
    try {
      const parsed = new URL(abstractUrl);
      return parsed.searchParams.get('abstract_id')
        || parsed.searchParams.get('abstractid')
        || parsed.searchParams.get('abstractId');
    } catch {
      return null;
    }
  })();
  if (!archiveRoot || !abstractId) return null;

  const postsRoot = path.join(archiveRoot, 'posts');
  if (!fs.existsSync(postsRoot)) return null;

  const stack = [postsRoot];
  while (stack.length > 0) {
    const currentDir = stack.pop();
    for (const entry of fs.readdirSync(currentDir, { withFileTypes: true })) {
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (!entry.isFile() || !entry.name.endsWith('.html')) continue;
      const html = fs.readFileSync(fullPath, 'utf8');
      if (!html.includes(`abstract_id=${abstractId}`) && !html.includes(`abstractid=${abstractId}`)) {
        continue;
      }
      const folderPath = path.dirname(fullPath);
      const pdfPaths = fs.readdirSync(folderPath)
        .filter((name) => name.toLowerCase().endsWith('.pdf'))
        .map((name) => path.join(folderPath, name));
      return {
        folderPath,
        folderRelative: path.relative(archiveRoot, folderPath),
        htmlPath: fullPath,
        textPath: fs.existsSync(fullPath.replace(/\.html$/, '.txt')) ? fullPath.replace(/\.html$/, '.txt') : null,
        pdfPaths,
      };
    }
  }

  return null;
}

function saveExistingResourceReference({
  url,
  fileDir,
  filenameStem,
  context,
  finalUrl,
  title,
  archiveRoot,
  archivedFolderRelative,
  reason,
}) {
  const stem = safeFilename(filenameStem || title || 'linked-resource', 'linked-resource');
  const htmlPath = path.join(fileDir, `${stem}.html`);
  const textPath = path.join(fileDir, `${stem}.txt`);
  const aliasPath = path.join(fileDir, `${stem}.alias.json`);

  const textBody = summarizeText([
    'Existing Archived Resource Reference',
    `Requested URL: ${url}`,
    `Resolved URL: ${finalUrl}`,
    `Archived Folder: ${archivedFolderRelative}`,
    `Reason: ${reason}`,
  ].join('\n'));
  const htmlBody = `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>${title || 'Existing Archived Resource Reference'}</title>
  </head>
  <body>
    <h1>Existing Archived Resource Reference</h1>
    <p>Requested URL: <a href="${url}">${url}</a></p>
    <p>Resolved URL: <a href="${finalUrl}">${finalUrl}</a></p>
    <p>Archived Folder: ${archivedFolderRelative}</p>
    <p>Reason: ${reason}</p>
  </body>
</html>
`;

  writeText(htmlPath, htmlBody);
  writeText(textPath, `${textBody}\n`);
  writeJson(aliasPath, {
    type: 'existing-archive-reference',
    createdAt: nowIso(),
    requestedUrl: url,
    finalUrl,
    archivedFolder: archivedFolderRelative,
    reason,
    context,
    archiveRoot,
  });

  return {
    kind: 'aliased',
    url,
    finalUrl,
    title,
    htmlPath,
    textPath,
    aliasPath,
    archivedFolder: archivedFolderRelative,
  };
}

async function saveBinaryResource({
  url,
  fileDir,
  filenameHint,
  approvedSkips,
  context,
  page,
  archiveRoot,
}) {
  let resource = null;
  const autoSkipReason = 'User-approved policy on 2026-03-15: inaccessible linked resource skipped automatically.';
  try {
    resource = await fetchBinaryResourceWithRetry(
      url,
      `binary:${context?.type || 'asset'}:${filenameHint || 'asset'}`,
    );
  } catch (error) {
    const fallbackUrl = getBinaryUrlFallback(url);
    if (fallbackUrl) {
      try {
        resource = await fetchBinaryResourceWithRetry(
          fallbackUrl,
          `binary-fallback:${context?.type || 'asset'}:${filenameHint || 'asset'}`,
        );
      } catch (fallbackError) {
        const skip = findApprovedSkip([url, fallbackUrl], approvedSkips);
        if (skip || isAutoSkippableLinkedContext(context)) {
          return buildSkippedResourceResult({
            url,
            finalUrl: fallbackUrl,
            reason: skip?.reason || autoSkipReason,
            error: fallbackError.message || String(fallbackError),
          });
        }
        throw new ReviewRequiredError('Linked resource could not be downloaded after retries.', {
          context,
          url,
          fallbackUrl,
          error: fallbackError.message || String(fallbackError),
        });
      }
    } else {
      const skip = findApprovedSkip([url], approvedSkips);
      if (skip || isAutoSkippableLinkedContext(context)) {
        return buildSkippedResourceResult({
          url,
          finalUrl: url,
          reason: skip?.reason || autoSkipReason,
          error: error.message || String(error),
        });
      }
      throw new ReviewRequiredError('Linked resource could not be downloaded after retries.', {
        context,
        url,
        error: error.message || String(error),
      });
    }
  }
  const ssrnCandidate = [url, resource.finalUrl].filter(Boolean).some((value) => {
    try {
      return isSsrnUrl(value);
    } catch {
      return false;
    }
  });
  if (page && ssrnCandidate && (!resource.ok || looksLikeAuthGate(resource.url, resource.finalUrl, resource.contentType, resource.buffer))) {
    return saveSsrnResource({
      page,
      url: resource.finalUrl || url,
      fileDir,
      filenameHint,
      context,
      archiveRoot,
    });
  }
  const skip = findApprovedSkip([url, resource.finalUrl], approvedSkips);
  if (looksLikeAuthGate(resource.url, resource.finalUrl, resource.contentType, resource.buffer)) {
    if (skip || isAutoSkippableLinkedContext(context)) {
      return buildSkippedResourceResult({
        url,
        finalUrl: resource.finalUrl,
        reason: skip?.reason || autoSkipReason,
        status: resource.status,
        contentType: resource.contentType,
      });
    }
    throw new ReviewRequiredError('Encountered an inaccessible linked resource that requires review.', {
      context,
      url,
      finalUrl: resource.finalUrl,
      status: resource.status,
      contentType: resource.contentType,
    });
  }
  if (!resource.ok) {
    if (skip || isAutoSkippableLinkedContext(context)) {
      return buildSkippedResourceResult({
        url,
        finalUrl: resource.finalUrl,
        reason: skip?.reason || autoSkipReason,
        status: resource.status,
        contentType: resource.contentType,
      });
    }
    throw new ReviewRequiredError('Linked resource returned a non-success status.', {
      context,
      url,
      finalUrl: resource.finalUrl,
      status: resource.status,
      contentType: resource.contentType,
    });
  }

  const dispositionName = parseContentDispositionFilename(resource.contentDisposition);
  const urlFilename = dispositionName
    || filenameHint
    || path.basename(new URL(resource.finalUrl).pathname)
    || 'asset';
  let finalName = safeFilename(urlFilename, filenameHint || 'asset');
  const inferredExt = path.extname(finalName) || extensionFromContentType(resource.contentType);
  if (!path.extname(finalName) && inferredExt) {
    finalName = `${finalName}${inferredExt}`;
  }
  const filePath = path.join(fileDir, finalName);
  writeBinary(filePath, resource.buffer);
  return {
    kind: 'saved',
    url,
    finalUrl: resource.finalUrl,
    contentType: resource.contentType,
    size: resource.buffer.length,
    path: filePath,
  };
}

async function saveHtmlBundle({
  page,
  url,
  fileDir,
  filenameStem,
  context,
  approvedSkips,
  archiveRoot,
}) {
  let bundle = null;
  const autoSkipReason = 'User-approved policy on 2026-03-15: inaccessible linked resource skipped automatically.';
  try {
    bundle = await fetchHtmlBundleWithRetry(
      page,
      url,
      `html:${context?.type || 'page'}:${filenameStem || url}`,
    );
  } catch {
    bundle = null;
  }
  if (!bundle?.ok) {
    bundle = await fetchHtmlBundleNodeWithRetry(
      url,
      `html-node:${context?.type || 'page'}:${filenameStem || url}`,
    ).catch(() => null);
  }
  if (!bundle?.ok) {
    const alias = findInternalSubstackAlias(url);
    if (alias && Number(bundle?.status) === 404) {
      return saveInternalAliasReference({
        url,
        fileDir,
        filenameStem,
        context,
        archiveRoot,
        alias,
      });
    }
    const skip = findApprovedSkip([url, bundle?.url], approvedSkips);
    if (skip || isAutoSkippableLinkedContext(context)) {
      return buildSkippedResourceResult({
        url,
        finalUrl: bundle?.url || url,
        reason: skip?.reason || autoSkipReason,
        status: bundle?.status || null,
      });
    }
    throw new ReviewRequiredError('Linked page could not be fetched from the authenticated browser session.', {
      context,
      url,
      status: bundle?.status || null,
    });
  }
  const stem = safeFilename(filenameStem || bundle.title || 'linked-page', 'linked-page');
  const htmlPath = path.join(fileDir, `${stem}.html`);
  const textPath = path.join(fileDir, `${stem}.txt`);
  writeText(htmlPath, bundle.html || '');
  writeText(textPath, `${summarizeText(bundle.text)}\n`);
  const embeddedDir = path.join(fileDir, `${stem}-assets`);
  const embeddedImages = [];
  const embeddedLinked = [];

  const embeddedImageUrls = dedupeBy(
    (bundle.images || [])
      .map((image) => normalizeUrl(unwrapSubstackMediaUrl(image.src), bundle.url))
      .filter(Boolean),
    (imageUrl) => imageUrl,
  );

  for (let index = 0; index < embeddedImageUrls.length; index += 1) {
    const imageUrl = embeddedImageUrls[index];
    const saved = await saveBinaryResource({
      url: imageUrl,
      fileDir: embeddedDir,
      filenameHint: `embedded-image-${String(index + 1).padStart(3, '0')}`,
      approvedSkips,
      context: {
        type: 'linked-page-image',
        parentUrl: url,
        ...context,
      },
      page,
      archiveRoot,
    });
    embeddedImages.push(saved);
  }

  const embeddedLinks = dedupeBy(
    [
      ...(bundle.links || []).map((link) => ({
        href: normalizeUrl(link.href, bundle.url),
        text: link.text,
      })),
      ...extractUrlsFromText(bundle.text).map((href) => ({
        href: normalizeUrl(href, bundle.url),
        text: href,
      })),
    ].filter((link) => link.href && looksLikeDownloadLink(link, bundle.url)),
    (link) => link.href,
  );

  for (let index = 0; index < embeddedLinks.length; index += 1) {
    const link = embeddedLinks[index];
    const normalizedUrl = unwrapSubstackMediaUrl(link.href);
    if (!normalizedUrl) continue;
    if (isSubstackUrl(normalizedUrl) && !/\.(png|jpe?g|gif|webp|pdf|txt|csv|json|zip|xlsx?|docx?|pptx?|mp4|mp3)([?#].*)?$/i.test(normalizedUrl)) {
      continue;
    }
    const saved = await saveBinaryResource({
      url: normalizedUrl,
      fileDir: embeddedDir,
      filenameHint: `embedded-linked-${String(index + 1).padStart(3, '0')}-${safeFilename(link.text || 'resource', 'resource')}`,
      approvedSkips,
      context: {
        type: 'linked-page-resource',
        parentUrl: url,
        href: normalizedUrl,
        ...context,
      },
      page,
      archiveRoot,
    });
    embeddedLinked.push(saved);
  }

  return {
    kind: 'saved',
    url,
    finalUrl: bundle.url,
    title: bundle.title,
    htmlPath,
    textPath,
    embedded: {
      images: embeddedImages,
      linked: embeddedLinked,
    },
  };
}

async function captureLinkedResource({
  page,
  link,
  fileDir,
  approvedSkips,
  context,
  filenameHint,
  archiveRoot,
}) {
  const normalizedUrl = unwrapSubstackMediaUrl(link.href || link.url || '');
  if (!normalizedUrl) return null;
  if (isSubstackUrl(normalizedUrl) && !/\.(png|jpe?g|gif|webp|pdf|txt|csv|json|zip|xlsx?|docx?|pptx?|mp4|mp3)([?#].*)?$/i.test(normalizedUrl)) {
    return saveHtmlBundle({
      page,
      url: normalizedUrl,
      fileDir,
      filenameStem: filenameHint || link.text || 'linked-page',
      context,
      approvedSkips,
      archiveRoot,
    });
  }
  return saveBinaryResource({
    url: normalizedUrl,
    fileDir,
    filenameHint: filenameHint || link.text || 'asset',
    approvedSkips,
    context,
    page,
    archiveRoot,
  });
}

async function downloadPostRelatedFiles({
  post,
  postDir,
  bundle,
  publicationPage,
  approvedSkips,
  archiveRoot,
}) {
  const imagesDir = path.join(postDir, 'images');
  const linkedDir = path.join(postDir, 'linked-content');
  const imageDownloads = [];
  const linkedDownloads = [];

  const imageUrls = dedupeBy(
    (bundle.images || [])
      .map((image) => normalizeUrl(unwrapSubstackMediaUrl(image.src), bundle.url))
      .filter(Boolean),
    (url) => url,
  );

  for (let index = 0; index < imageUrls.length; index += 1) {
    const imageUrl = imageUrls[index];
    const saved = await saveBinaryResource({
      url: imageUrl,
      fileDir: imagesDir,
      filenameHint: `image-${String(index + 1).padStart(3, '0')}`,
      approvedSkips,
      context: {
        type: 'post-image',
        postId: post.id,
        slug: post.slug,
      },
      page: publicationPage,
      archiveRoot,
    });
    imageDownloads.push(saved);
  }

  const downloadLinks = dedupeBy(
    [
      ...(bundle.links || []).map((link) => ({
        href: normalizeUrl(link.href, bundle.url),
        text: link.text,
      })),
      ...extractUrlsFromText(bundle.text).map((href) => ({
        href: normalizeUrl(href, bundle.url),
        text: href,
      })),
    ].filter((link) => looksLikeDownloadLink(link, post.canonical_url)),
    (link) => link.href,
  );

  for (let index = 0; index < downloadLinks.length; index += 1) {
    const link = downloadLinks[index];
    const captured = await captureLinkedResource({
      page: publicationPage,
      link,
      fileDir: linkedDir,
      approvedSkips,
      context: {
        type: 'post-linked-resource',
        postId: post.id,
        slug: post.slug,
        href: link.href,
      },
      filenameHint: `linked-${String(index + 1).padStart(3, '0')}-${safeFilename(link.text || 'resource', 'resource')}`,
      archiveRoot,
    });
    if (captured) linkedDownloads.push(captured);
  }

  return {
    images: imageDownloads,
    linked: linkedDownloads,
  };
}

async function fetchChatReplyPages(chatPage, threadId, approvedSkips = []) {
  const pages = [];
  const replies = [];
  let after = null;
  const seenCursor = new Set();

  while (true) {
    const params = new URLSearchParams({ order: 'asc' });
    if (after) params.set('after', after);
    const commentsUrl = `${SUBSTACK_BASE_URL}/api/v1/community/posts/${threadId}/comments?${params.toString()}`;
    const payload = await fetchJsonPayloadWithRetry(
      chatPage,
      commentsUrl,
      `chat-replies:${threadId}:${after || 'start'}`,
    );
    if (!payload.ok || !payload.json || !Array.isArray(payload.json.replies)) {
      const skip = findApprovedSkip([
        commentsUrl,
        `${SUBSTACK_BASE_URL}/api/v1/community/posts/${threadId}/comments`,
      ], approvedSkips);
      if (skip) {
        pages.push({
          after,
          status: payload?.status || null,
          more: false,
          moreAfter: false,
          moreBefore: false,
          lastViewedAt: payload?.json?.lastViewedAt || null,
          replies: [],
          post: payload?.json?.post || null,
          skipped: true,
          skipReason: skip.reason,
        });
        return {
          pages,
          replies: dedupeBy(replies, (reply) => String(reply?.comment?.id || '')),
          skipped: true,
          skipReason: skip.reason,
          status: payload?.status || null,
        };
      }
      throw new ReviewRequiredError('Chat replies endpoint did not return the expected reply payload.', {
        type: 'chat-replies',
        threadId,
        url: commentsUrl,
        status: payload.status,
      });
    }
    const pageReplies = payload.json.replies;
    pages.push({
      after,
      status: payload.status,
      more: payload.json.more || false,
      moreAfter: payload.json.moreAfter || false,
      moreBefore: payload.json.moreBefore || false,
      lastViewedAt: payload.json.lastViewedAt || null,
      replies: pageReplies,
      post: payload.json.post || null,
    });
    replies.push(...pageReplies);
    if (!payload.json.moreAfter || pageReplies.length === 0) break;
    const lastCreatedAt = pageReplies[pageReplies.length - 1]?.comment?.created_at || null;
    if (!lastCreatedAt || seenCursor.has(lastCreatedAt)) {
      const skip = findApprovedSkip([
        commentsUrl,
        `${SUBSTACK_BASE_URL}/api/v1/community/posts/${threadId}/comments`,
      ], approvedSkips);
      if (skip) {
        return {
          pages,
          replies: dedupeBy(replies, (reply) => String(reply?.comment?.id || '')),
          skipped: true,
          skipReason: skip.reason,
          status: payload.status,
        };
      }
      throw new ReviewRequiredError('Chat reply pagination stalled before completion.', {
        type: 'chat-replies',
        threadId,
        after,
      });
    }
    seenCursor.add(lastCreatedAt);
    after = lastCreatedAt;
  }

  return {
    pages,
    replies: dedupeBy(replies, (reply) => String(reply?.comment?.id || '')),
    skipped: false,
    skipReason: null,
    status: pages[0]?.status || null,
  };
}

async function downloadChatAssets({
  chatDir,
  threadId,
  postSummary,
  replies,
  chatPage,
  approvedSkips,
  archiveRoot,
}) {
  const assetsDir = path.join(chatDir, 'assets');
  const items = [];

  const topLevelMedia = [
    ...(postSummary?.mediaAttachments || []),
    ...(postSummary?.media_assets || []),
  ].map((asset) => ({
    url: asset.url || asset.thumb_url || null,
    label: 'post-media',
  }));

  const replyMedia = replies.flatMap((reply, index) => (
    (reply?.comment?.mediaAttachments || []).map((asset, mediaIndex) => ({
      url: asset.url || asset.thumb_url || null,
      label: `reply-${index + 1}-${mediaIndex + 1}`,
    }))
  ));

  const mediaItems = dedupeBy([...topLevelMedia, ...replyMedia].filter((asset) => asset.url), (asset) => asset.url);
  for (let index = 0; index < mediaItems.length; index += 1) {
    const media = mediaItems[index];
    const saved = await saveBinaryResource({
      url: unwrapSubstackMediaUrl(media.url),
      fileDir: assetsDir,
      filenameHint: `media-${String(index + 1).padStart(3, '0')}-${safeFilename(media.label, 'media')}`,
      approvedSkips,
      context: {
        type: 'chat-media',
        threadId,
      },
      page: chatPage,
      archiveRoot,
    });
    items.push(saved);
  }

  const linkCandidates = dedupeBy(
    [
      ...extractUrlsFromText(postSummary?.body || '').map((href) => ({ href, text: href })),
      ...extractUrlsFromText(postSummary?.raw_body || '').map((href) => ({ href, text: href })),
      ...extractUrlsFromText(postSummary?.paywallInfo?.body || '').map((href) => ({ href, text: href })),
      ...replies.flatMap((reply) => {
        const comment = reply?.comment || {};
        return [
          ...extractUrlsFromText(comment.body || '').map((href) => ({ href, text: href })),
          ...extractUrlsFromText(comment.raw_body || '').map((href) => ({ href, text: href })),
        ];
      }),
    ]
      .map((link) => ({
        href: normalizeUrl(link.href, SUBSTACK_BASE_URL),
        text: link.text,
      }))
      .filter((link) => link.href && looksLikeDownloadLink(link, null)),
    (link) => link.href,
  );

  for (let index = 0; index < linkCandidates.length; index += 1) {
    const link = linkCandidates[index];
    const saved = await captureLinkedResource({
      page: chatPage,
      link,
      fileDir: assetsDir,
      approvedSkips,
      context: {
        type: 'chat-linked-resource',
        threadId,
        href: link.href,
      },
      filenameHint: `linked-${String(index + 1).padStart(3, '0')}-${safeFilename(link.text || 'resource', 'resource')}`,
      archiveRoot,
    });
    if (saved) items.push(saved);
  }

  return items;
}

function buildChatTranscript(postSummary, replies) {
  const lines = [];
  const postUser = postSummary?.user?.name || 'Unknown';
  lines.push('Chat Post');
  lines.push(`${postSummary?.created_at || ''} | ${postUser}`);
  if (postSummary?.body) lines.push(postSummary.body);
  if (!postSummary?.body && postSummary?.paywallInfo?.body) lines.push(`[paywall-info] ${postSummary.paywallInfo.body}`);
  lines.push('');
  lines.push('Replies');
  for (const reply of replies) {
    const comment = reply?.comment || {};
    const user = reply?.user?.name || 'Unknown';
    lines.push(`${comment.created_at || ''} | ${user}`);
    lines.push(comment.body || comment.raw_body || '');
    lines.push('');
  }
  return summarizeText(lines.join('\n'));
}

function writeArchivePageSnapshots(baseDir, pages, label = '') {
  const pagesDir = label ? path.join(baseDir, 'pages', label) : path.join(baseDir, 'pages');
  ensureDir(pagesDir);
  pages.forEach((pageItem, index) => {
    writeJson(path.join(pagesDir, `page-${String(index + 1).padStart(3, '0')}.json`), pageItem);
  });
}

async function exportPosts({
  outputDir,
  refresh,
  existingManifest,
  publicationPage,
  approvedSkips,
}) {
  const postsBaseDir = path.join(outputDir, 'posts');
  ensureDir(postsBaseDir);
  const archive = await fetchArchivePosts(publicationPage);
  writeJson(path.join(postsBaseDir, 'sections.json'), archive.sections);
  const pagesByScope = new Map();
  for (const pageItem of archive.pages) {
    const key = pageItem.scope?.label || 'root';
    if (!pagesByScope.has(key)) pagesByScope.set(key, []);
    pagesByScope.get(key).push(pageItem);
  }
  for (const [label, pages] of pagesByScope.entries()) {
    writeArchivePageSnapshots(postsBaseDir, pages, label);
  }

  const existingById = new Map((existingManifest?.posts || []).map((item) => [String(item.id), item]));
  const postEntries = [];
  let fetchedCount = 0;
  let skippedCount = 0;

  for (const post of sortDescByIso(archive.posts, 'post_date')) {
    const folderName = formatFolderName(post.post_date, post.slug || post.title, `post-${post.id}`);
    const postDir = path.join(postsBaseDir, folderName);
    const metadataPath = path.join(postDir, 'metadata.json');
    const htmlPath = path.join(postDir, 'source.html');
    const textPath = path.join(postDir, 'content.txt');
    const archivedAt = nowIso();
    const existingItem = existingById.get(String(post.id));
    const existingFolderMetadata = readJsonIfExists(metadataPath, null);
    const imagesDir = path.join(postDir, 'images');
    const linkedDir = path.join(postDir, 'linked-content');
    const needsMetadataRepair = Boolean(
      fs.existsSync(htmlPath)
      && fs.existsSync(textPath)
      && (!existingFolderMetadata?.extracted || !existingFolderMetadata?.fetchHttpStatus),
    );
    const needsAssetRepair = Boolean(
      existingFolderMetadata && (!existingFolderMetadata.assets || !existingFolderMetadata.assets.images),
    );
    const needsStatusRepair = Boolean(
      existingFolderMetadata && (
        existingFolderMetadata.fetchStatus !== 'fetched'
        || Number(existingFolderMetadata.fetchHttpStatus) !== 200
      ),
    );
    const shouldFetch = refresh
      || !fs.existsSync(metadataPath)
      || !fs.existsSync(htmlPath)
      || !fs.existsSync(textPath)
      || needsMetadataRepair
      || needsAssetRepair
      || needsStatusRepair;

    let fetchStatus = existingItem?.fetchStatus || existingFolderMetadata?.fetchStatus || 'skipped';
    let fetchHttpStatus = existingItem?.fetchHttpStatus || existingFolderMetadata?.fetchHttpStatus || null;
    let bundle = null;
    let assets = existingFolderMetadata?.assets || { images: [], linked: [] };

    if (shouldFetch) {
      bundle = await fetchHtmlBundleWithRetry(
        publicationPage,
        post.canonical_url,
        `post:${post.slug || post.id}`,
      );
      if (!bundle?.ok) {
        throw new ReviewRequiredError('Publication post page could not be fully downloaded.', {
          type: 'post-page',
          postId: post.id,
          slug: post.slug,
          url: post.canonical_url,
          status: bundle?.status || null,
        });
      }
      ensureDir(postDir);
      removePath(imagesDir);
      removePath(linkedDir);
      writeText(htmlPath, bundle.html || '');
      writeText(textPath, `${summarizeText(bundle.text)}\n`);
      assets = await downloadPostRelatedFiles({
        post,
        postDir,
        bundle,
        publicationPage,
        approvedSkips,
        archiveRoot: outputDir,
      });
      fetchedCount += 1;
      fetchStatus = bundle.ok ? 'fetched' : 'non_200';
      fetchHttpStatus = bundle.status;
      console.log(`[alma-export] post ${fetchedCount}/${archive.posts.length} fetched ${post.post_date} ${post.slug}`);
    } else {
      skippedCount += 1;
    }

    const metadata = {
      archivedAt,
      fetchStatus,
      fetchHttpStatus,
      folderName,
      post,
      extracted: bundle ? {
        title: bundle.title,
        metaDescription: bundle.metaDescription,
        textLength: summarizeText(bundle.text).length,
        sourceUrl: bundle.url,
      } : (existingItem?.extracted || existingFolderMetadata?.extracted || null),
      assets,
      files: {
        metadata: path.relative(outputDir, metadataPath),
        html: path.relative(outputDir, htmlPath),
        text: path.relative(outputDir, textPath),
      },
    };

    writeJson(metadataPath, metadata);
    postEntries.push({
      id: post.id,
      slug: post.slug,
      title: post.title,
      canonicalUrl: post.canonical_url,
      postDate: post.post_date,
      audience: post.audience,
      sectionId: post.section_id,
      sectionSlug: post.section_slug,
      sectionName: post.section_name,
      folderName,
      fetchStatus,
      fetchHttpStatus,
      extracted: metadata.extracted,
      assets: {
        images: metadata.assets?.images?.length || 0,
        linked: metadata.assets?.linked?.length || 0,
      },
      files: metadata.files,
    });
  }

  return {
    entries: sortDescByIso(postEntries, 'postDate'),
    fetchedCount,
    skippedCount,
    totalCount: postEntries.length,
  };
}

function filterAlmaInboxThreads(inboxSnapshot) {
  return (inboxSnapshot?.threads || []).filter((thread) => {
    const publicationId = thread?.publication?.id || null;
    const userHandle = thread?.user?.handle || thread?.communityPost?.user?.handle || null;
    return publicationId === PUBLICATION_ID || userHandle === 'alma271828';
  });
}

async function exportChats({
  outputDir,
  refresh,
  existingManifest,
  chatPage,
  inboxPage,
  approvedSkips,
}) {
  const chatsBaseDir = path.join(outputDir, 'chats');
  ensureDir(chatsBaseDir);

  const inboxSnapshot = await fetchInboxSnapshot(inboxPage);
  writeJson(path.join(chatsBaseDir, 'inbox-snapshot.json'), inboxSnapshot);

  const feed = await fetchCommunityFeed(chatPage);
  writeArchivePageSnapshots(chatsBaseDir, feed.pages, 'feed');

  const existingById = new Map((existingManifest?.chats || []).map((item) => [String(item.id), item]));
  const chatEntries = [];
  let fetchedCount = 0;
  let skippedCount = 0;

  for (const thread of sortDescByIso(feed.threads.map((item) => item.communityPost).filter(Boolean), 'created_at')) {
    const folderName = formatFolderName(thread.created_at, thread.body || `chat-${thread.id}`, `chat-${thread.id}`);
    const chatDir = path.join(chatsBaseDir, folderName);
    const metadataPath = path.join(chatDir, 'metadata.json');
    const postJsonPath = path.join(chatDir, 'post.json');
    const repliesJsonPath = path.join(chatDir, 'replies.json');
    const replyPagesDir = path.join(chatDir, 'reply-pages');
    const htmlPath = path.join(chatDir, 'source.html');
    const textPath = path.join(chatDir, 'content.txt');
    const chatUrl = `${SUBSTACK_BASE_URL}/chat/${PUBLICATION_ID}/post/${thread.id}`;
    const existingItem = existingById.get(String(thread.id));
    const existingFolderMetadata = readJsonIfExists(metadataPath, null);
    const shouldFetch = refresh || !fs.existsSync(metadataPath) || !fs.existsSync(postJsonPath)
      || !fs.existsSync(repliesJsonPath) || !fs.existsSync(htmlPath) || !fs.existsSync(textPath);
    const approvedReplySkipExists = Boolean(
      existingFolderMetadata?.replyFetchSkipped && existingFolderMetadata?.replySkipReason,
    );
    const deletedFallbackExists = Boolean(
      existingFolderMetadata?.deletedFallback && existingFolderMetadata?.deletedReason,
    );
    const needsRepair = Boolean(
      existingFolderMetadata && (
        !['fetched', 'approved_skip', 'deleted_fallback'].includes(String(existingFolderMetadata.fetchStatus || ''))
        || (!deletedFallbackExists && Number(existingFolderMetadata.detailsStatus) !== 200)
        || (!approvedReplySkipExists && !deletedFallbackExists && Number(existingFolderMetadata.commentsStatus) !== 200)
        || Number(existingFolderMetadata.pageStatus) !== 200
        || !Array.isArray(existingFolderMetadata.assets)
      ),
    );
    const shouldRefetch = shouldFetch || needsRepair;

    let detailsPayload = null;
    let replyPages = [];
    let replies = [];
    let pageBundle = null;
    let assets = existingFolderMetadata?.assets || [];
    let fetchStatus = existingItem?.fetchStatus || existingFolderMetadata?.fetchStatus || 'skipped';
    let commentsStatus = existingItem?.commentsStatus || existingFolderMetadata?.commentsStatus || null;
    let pageStatus = existingItem?.pageStatus || existingFolderMetadata?.pageStatus || null;
    let replyFetchSkipped = Boolean(existingFolderMetadata?.replyFetchSkipped);
    let replySkipReason = existingFolderMetadata?.replySkipReason || null;
    let deletedFallback = Boolean(existingFolderMetadata?.deletedFallback);
    let deletedReason = existingFolderMetadata?.deletedReason || null;

    if (shouldRefetch) {
      const detailsUrl = `${SUBSTACK_BASE_URL}/api/v1/community/posts/${thread.id}`;
      detailsPayload = await fetchJsonPayloadWithRetry(
        chatPage,
        detailsUrl,
        `chat-details:${thread.id}`,
      );
      const canUseDeletedFallback = Boolean(
        Number(detailsPayload?.status) === 404
        && thread.status === 'deleted',
      );
      if (canUseDeletedFallback) {
        deletedFallback = true;
        deletedReason = 'Feed shows this Alma chat as deleted; archived from feed metadata and page HTML because details/comments endpoints now return 404.';
        detailsPayload = {
          ...detailsPayload,
          json: {
            communityPost: thread,
            deletedFallback: true,
          },
        };
      }
      if (!detailsPayload?.ok && !deletedFallback || !detailsPayload?.json) {
        throw new ReviewRequiredError('Chat details could not be fully downloaded.', {
          type: 'chat-details',
          threadId: thread.id,
          url: detailsUrl,
          status: detailsPayload?.status || null,
        });
      }
      if (deletedFallback) {
        commentsStatus = 404;
        replyFetchSkipped = true;
        replySkipReason = deletedReason;
        replyPages = [
          {
            after: null,
            status: 404,
            more: false,
            moreAfter: false,
            moreBefore: false,
            lastViewedAt: null,
            replies: [],
            post: {
              communityPost: thread,
            },
            skipped: true,
            skipReason: deletedReason,
          },
        ];
        replies = [];
      } else {
        const replyPayload = await fetchChatReplyPages(chatPage, thread.id, approvedSkips);
        replyPages = replyPayload.pages;
        replies = replyPayload.replies;
        replyFetchSkipped = Boolean(replyPayload.skipped);
        replySkipReason = replyPayload.skipReason || null;
        commentsStatus = replyPayload.status || replyPages[0]?.status || null;
      }
      pageBundle = await fetchHtmlBundleWithRetry(
        chatPage,
        chatUrl,
        `chat-page:${thread.id}`,
      );
      if (!pageBundle?.ok) {
        throw new ReviewRequiredError('Chat page could not be fully downloaded.', {
          type: 'chat-page',
          threadId: thread.id,
          url: chatUrl,
          status: pageBundle?.status || null,
        });
      }

      ensureDir(chatDir);
      removePath(replyPagesDir);
      removePath(path.join(chatDir, 'assets'));
      writeJson(postJsonPath, detailsPayload.json || null);
      ensureDir(replyPagesDir);
      replyPages.forEach((pageItem, index) => {
        writeJson(path.join(replyPagesDir, `page-${String(index + 1).padStart(3, '0')}.json`), pageItem);
      });
      writeJson(repliesJsonPath, replies);
      writeText(htmlPath, pageBundle.html || '');
      const postSummaryForTranscript = replyPages[0]?.post?.communityPost
        || detailsPayload.json?.communityPost
        || thread;
      writeText(textPath, `${buildChatTranscript(postSummaryForTranscript, replies)}\n`);
      assets = await downloadChatAssets({
        chatDir,
        threadId: thread.id,
        postSummary: postSummaryForTranscript,
        replies,
        chatPage,
        approvedSkips,
        archiveRoot: outputDir,
      });

      fetchedCount += 1;
      fetchStatus = deletedFallback
        ? 'deleted_fallback'
        : replyFetchSkipped
        ? 'approved_skip'
        : (detailsPayload.ok || replyPages.length > 0 ? 'fetched' : 'non_200');
      pageStatus = pageBundle.status;
      if (deletedFallback) {
        console.log(`[alma-export] chat ${fetchedCount}/${feed.threads.length} deleted-fallback ${thread.created_at} ${thread.id}`);
      } else if (replyFetchSkipped) {
        console.log(`[alma-export] chat ${fetchedCount}/${feed.threads.length} approved-skip replies ${thread.created_at} ${thread.id}`);
      } else {
        console.log(`[alma-export] chat ${fetchedCount}/${feed.threads.length} fetched ${thread.created_at} ${thread.id}`);
      }
    } else {
      skippedCount += 1;
    }

    const summarySource = replyPages[0]?.post?.communityPost
      || detailsPayload?.json?.communityPost
      || existingFolderMetadata?.postSummary
      || thread;
    const metadata = {
      archivedAt: nowIso(),
      fetchStatus,
      detailsStatus: detailsPayload?.status || existingItem?.detailsStatus || existingFolderMetadata?.detailsStatus || null,
      commentsStatus,
      pageStatus,
      replyFetchSkipped,
      replySkipReason,
      deletedFallback,
      deletedReason,
      folderName,
      thread,
      postSummary: summarySource,
      replyCount: replies.length || existingFolderMetadata?.replyCount || 0,
      assets,
      inboxThreads: filterAlmaInboxThreads(inboxSnapshot).filter((item) => {
        return item?.communityPost?.id === thread.id || item?.publication?.id === PUBLICATION_ID;
      }),
      files: {
        metadata: path.relative(outputDir, metadataPath),
        postJson: path.relative(outputDir, postJsonPath),
        repliesJson: path.relative(outputDir, repliesJsonPath),
        replyPagesDir: path.relative(outputDir, replyPagesDir),
        html: path.relative(outputDir, htmlPath),
        text: path.relative(outputDir, textPath),
      },
    };

    writeJson(metadataPath, metadata);
    chatEntries.push({
      id: thread.id,
      body: summarySource?.body || thread.body || null,
      createdAt: thread.created_at,
      updatedAt: summarySource?.updated_at || thread.updated_at || null,
      audience: summarySource?.audience || thread.audience || null,
      commentCount: summarySource?.comment_count || thread.comment_count || 0,
      reactionCount: summarySource?.reaction_count || thread.reaction_count || 0,
      isLocked: Boolean(summarySource?.is_locked),
      folderName,
      fetchStatus,
      detailsStatus: metadata.detailsStatus,
      commentsStatus,
      pageStatus,
      replyFetchSkipped,
      deletedFallback,
      replyCount: metadata.replyCount,
      assets: metadata.assets?.length || 0,
      files: metadata.files,
    });
  }

  return {
    entries: sortDescByIso(chatEntries, 'createdAt'),
    inboxSnapshot,
    inboxThreads: filterAlmaInboxThreads(inboxSnapshot),
    fetchedCount,
    skippedCount,
    totalCount: chatEntries.length,
  };
}

function buildManifest({
  outputDir,
  cdpHttpUrl,
  postExport,
  chatExport,
}) {
  return {
    scriptVersion: SCRIPT_VERSION,
    exportedAt: nowIso(),
    cdpHttpUrl,
    publication: {
      id: PUBLICATION_ID,
      baseUrl: PUBLICATION_BASE_URL,
      substackChatBaseUrl: `${SUBSTACK_BASE_URL}/chat/${PUBLICATION_ID}`,
    },
    sync: {
      latestPostDate: postExport.entries[0]?.postDate || null,
      oldestPostDate: postExport.entries[postExport.entries.length - 1]?.postDate || null,
      latestChatDate: chatExport.entries[0]?.createdAt || null,
      oldestChatDate: chatExport.entries[chatExport.entries.length - 1]?.createdAt || null,
      postCount: postExport.totalCount,
      chatCount: chatExport.totalCount,
    },
    stats: {
      postsFetchedThisRun: postExport.fetchedCount,
      postsSkippedThisRun: postExport.skippedCount,
      chatsFetchedThisRun: chatExport.fetchedCount,
      chatsSkippedThisRun: chatExport.skippedCount,
      inboxThreadCount: (chatExport.inboxSnapshot?.threads || []).length,
      almaInboxThreadCount: chatExport.inboxThreads.length,
    },
    posts: postExport.entries,
    chats: chatExport.entries,
  };
}

async function main() {
  const args = parseArgs(process.argv);
  if (toBool(args.help)) {
    console.log('Usage: node artifacts/alma-research/tools/export-alma-substack-archive.js [--out artifacts/alma-research] [--refresh 1] [--wipe 1]');
    return;
  }

  const outputDir = path.resolve(args.out || DEFAULT_OUTPUT_DIR);
  const refresh = toBool(args.refresh);
  const wipe = toBool(args.wipe);
  const manifestPath = path.join(outputDir, 'manifest.json');

  ensureDir(outputDir);
  const approvedSkips = ensureApprovedSkipsFile(outputDir);

  if (wipe) {
    removePath(path.join(outputDir, 'posts'));
    removePath(path.join(outputDir, 'chats'));
    removePath(manifestPath);
  }

  const existingManifest = readJsonIfExists(manifestPath, null);

  const { publicationPage, chatPage, inboxPage } = await createCdpPages(DEFAULT_CDP_HTTP_URL);
  try {
    const publicationSession = await ensurePublicationAccess(publicationPage);
    console.log(`[alma-export] authenticated session user=${publicationSession.currentUser || 'unknown'} publication=${publicationSession.publication || 'unknown'}`);
    const postExport = await exportPosts({
      outputDir,
      refresh,
      existingManifest,
      publicationPage,
      approvedSkips,
    });
    const chatExport = await exportChats({
      outputDir,
      refresh,
      existingManifest,
      chatPage,
      inboxPage,
      approvedSkips,
    });
    const manifest = buildManifest({
      outputDir,
      cdpHttpUrl: DEFAULT_CDP_HTTP_URL,
      postExport,
      chatExport,
    });
    writeJson(manifestPath, manifest);

    console.log('[alma-export] complete');
    console.log(JSON.stringify({
      outputDir,
      manifestPath,
      postCount: postExport.totalCount,
      chatCount: chatExport.totalCount,
      postsFetchedThisRun: postExport.fetchedCount,
      chatsFetchedThisRun: chatExport.fetchedCount,
      latestPostDate: manifest.sync.latestPostDate,
      latestChatDate: manifest.sync.latestChatDate,
    }, null, 2));
  } finally {
    const pages = [publicationPage, chatPage];
    if (inboxPage !== chatPage) pages.push(inboxPage);
    await Promise.allSettled(pages.map((page) => page.close()));
  }
}

main().catch((error) => {
  if (error instanceof ReviewRequiredError) {
    console.error('[alma-export] review-required');
    console.error(JSON.stringify(error.details, null, 2));
    process.exitCode = 2;
    return;
  }
  console.error(`[alma-export] failed: ${error.stack || error.message}`);
  process.exitCode = 1;
});
