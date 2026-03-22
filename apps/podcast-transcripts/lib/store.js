'use strict';

const fs = require('fs');
const path = require('path');

const DATA_DIR = path.join(__dirname, '..', 'data');

function podcastDir(podcastId) {
  const dir = path.join(DATA_DIR, podcastId);
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function manifestPath(podcastId) {
  return path.join(podcastDir(podcastId), 'manifest.json');
}

function loadManifest(podcastId) {
  const p = manifestPath(podcastId);
  if (!fs.existsSync(p)) return {};
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function saveManifest(podcastId, manifest) {
  fs.writeFileSync(manifestPath(podcastId), JSON.stringify(manifest, null, 2));
}

function isDownloaded(podcastId, episodeId) {
  const manifest = loadManifest(podcastId);
  return !!manifest[episodeId];
}

/**
 * Save an episode transcript.
 * @param {string} podcastId
 * @param {object} episode - { id, title, date, url, transcript }
 */
function saveEpisode(podcastId, episode) {
  const dir = podcastDir(podcastId);
  const filename = `${episode.date}-${episode.id}.json`;
  const filepath = path.join(dir, filename);

  fs.writeFileSync(filepath, JSON.stringify({ podcastId, ...episode }, null, 2));

  // Update manifest
  const manifest = loadManifest(podcastId);
  manifest[episode.id] = { date: episode.date, title: episode.title, file: filename };
  saveManifest(podcastId, manifest);
}

function getStats(podcastId) {
  const manifest = loadManifest(podcastId);
  const ids = Object.keys(manifest);
  return { total: ids.length, episodes: manifest };
}

module.exports = { isDownloaded, saveEpisode, getStats, loadManifest };
