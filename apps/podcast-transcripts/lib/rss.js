'use strict';

const https = require('https');
const http = require('http');

/**
 * Fetch a URL, following redirects, returning the body as a string.
 */
function fetchUrl(url, redirects = 5) {
  return new Promise((resolve, reject) => {
    if (redirects === 0) return reject(new Error('Too many redirects'));
    const lib = url.startsWith('https') ? https : http;
    lib
      .get(url, { headers: { 'User-Agent': 'Mozilla/5.0 (compatible; podcast-transcripts/1.0)' } }, (res) => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return resolve(fetchUrl(res.headers.location, redirects - 1));
        }
        if (res.statusCode !== 200) {
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
        res.on('error', reject);
      })
      .on('error', reject);
  });
}

/**
 * Extract content between XML tags (first match, non-greedy, handles CDATA).
 */
function extractTag(xml, tag) {
  const re = new RegExp(`<${tag}[^>]*><!\\[CDATA\\[([\\s\\S]*?)\\]\\]></${tag}>|<${tag}[^>]*>([\\s\\S]*?)</${tag}>`, 'i');
  const m = re.exec(xml);
  if (!m) return '';
  return (m[1] !== undefined ? m[1] : m[2] || '').trim();
}

/**
 * Parse an RSS/Atom feed XML string into an array of episode objects.
 * Returns: [{ title, link, pubDate, description, guid }]
 */
function parseRss(xml) {
  const episodes = [];

  // Split by <item> tags
  const itemRe = /<item[\s>]([\s\S]*?)<\/item>/gi;
  let match;

  while ((match = itemRe.exec(xml)) !== null) {
    const item = match[1];

    const title = extractTag(item, 'title');
    const pubDate = extractTag(item, 'pubDate');
    const guid = extractTag(item, 'guid');
    const description = extractTag(item, 'description');

    // <link> is sometimes self-closing or has href attribute in Atom
    let link = extractTag(item, 'link');
    if (!link) {
      const hrefMatch = /<link[^>]+href="([^"]+)"/i.exec(item);
      if (hrefMatch) link = hrefMatch[1];
    }

    if (!title && !link) continue;

    // content:encoded (full HTML body — often contains transcript)
    const contentEncodedRaw = item.match(/<content:encoded>([\s\S]*?)<\/content:encoded>/i)?.[1] || '';
    const rawContentEncoded = contentEncodedRaw.replace(/^<!\[CDATA\[/, '').replace(/\]\]>$/, '');

    const date = pubDate ? new Date(pubDate) : null;

    episodes.push({
      title: title.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&#39;/g, "'").replace(/&quot;/g, '"'),
      link,
      pubDate,
      date: date && !isNaN(date.getTime()) ? date : null,
      description,
      rawContentEncoded,
      guid,
    });
  }

  return episodes;
}

/**
 * Fetch and parse an RSS feed URL.
 * Returns sorted episodes (newest first).
 */
async function fetchRss(url) {
  const xml = await fetchUrl(url);
  return parseRss(xml);
}

module.exports = { fetchRss, fetchUrl };
