'use strict';

/**
 * Extract key claims with timeframes from podcast transcripts using Claude.
 *
 * Uses the Batches API: 50% cost reduction, no rate limit concerns, fire-and-poll.
 * Estimated cost: ~$8-10 for all 964 episodes using claude-opus-4-6.
 *
 * Usage:
 *   node scripts/extract-claims.js                          # all podcasts
 *   node scripts/extract-claims.js --podcast morgan-stanley # one podcast
 *   node scripts/extract-claims.js --resume <batch-id>      # resume a batch
 *
 * Output: claims/<podcast-id>/<date>-<episode-id>.txt
 * Idempotent: already-completed claim files are skipped on re-run.
 */

const Anthropic = require('@anthropic-ai/sdk');
const fs = require('fs');
const path = require('path');

const client = new Anthropic(); // reads ANTHROPIC_API_KEY from env

const ROOT = path.join(__dirname, '..');
const DATA_DIR = path.join(ROOT, 'data');
const CLAIMS_DIR = path.join(ROOT, 'claims');
const BATCH_STATE_FILE = path.join(CLAIMS_DIR, '.batch-state.json');

// ---------------------------------------------------------------------------
// Prompt
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT = `You are a senior financial analyst. Given a podcast transcript from a major financial institution, extract every key claim, prediction, or forward-looking view that has an explicit or clearly implied timeframe.

Output one claim per line in this format:
[TIMEFRAME] Claim text — Speaker name (if identified)

Guidelines:
- TIMEFRAME must be specific: a quarter (Q2 2024), a year (2025), a range (2025-2026), a duration ("next 6-12 months"), or a milestone ("by year-end", "before the Fed's next meeting").
- Convert relative references using the episode date provided. For example, "next year" in a March 2023 episode → [2024]; "this quarter" → [Q1 2023].
- Focus on: market/asset price forecasts, economic outlooks (GDP, inflation, rates), sector/industry views, policy expectations (Fed, fiscal, geopolitical), earnings/revenue guidance, commodity/currency targets.
- Omit: purely historical observations, opinions without any forward-looking element, vague statements with no timeframe even after interpretation.
- Target 5-15 claims per episode. Quality over quantity — each claim must be specific and actionable.
- If genuinely no timeframe-anchored claims exist, write exactly: NO_CLAIMS_WITH_TIMEFRAMES`;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseArgs() {
  const args = process.argv.slice(2);
  const opts = { podcast: null, resume: null };
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--podcast' && args[i + 1]) opts.podcast = args[++i];
    if (args[i] === '--resume' && args[i + 1]) opts.resume = args[++i];
  }
  return opts;
}

function getEpisodeFiles(podcastId) {
  const dir = path.join(DATA_DIR, podcastId);
  if (!fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .filter((f) => f.endsWith('.json') && f !== 'manifest.json')
    .map((f) => ({
      podcastId,
      file: f,
      filePath: path.join(dir, f),
      customId: `${podcastId}||${f}`,
    }));
}

function claimsPath(podcastId, episodeFile) {
  return path.join(CLAIMS_DIR, podcastId, episodeFile.replace('.json', '.txt'));
}

function loadBatchState() {
  if (!fs.existsSync(BATCH_STATE_FILE)) return null;
  try {
    return JSON.parse(fs.readFileSync(BATCH_STATE_FILE, 'utf8'));
  } catch (_) {
    return null;
  }
}

function saveBatchState(state) {
  fs.mkdirSync(path.dirname(BATCH_STATE_FILE), { recursive: true });
  fs.writeFileSync(BATCH_STATE_FILE, JSON.stringify(state, null, 2));
}

function buildUserMessage(episode) {
  return `Episode: ${episode.title || 'Unknown title'}
Date recorded: ${episode.date || 'Unknown'}
Podcast: ${episode.podcastId || ''}

Transcript:
${episode.transcript || ''}`.trim();
}

function writeClaimsFile(podcastId, episodeFile, episode, claimsText) {
  const outDir = path.join(CLAIMS_DIR, podcastId);
  fs.mkdirSync(outDir, { recursive: true });

  const lines = [
    `Title  : ${episode.title || 'Unknown'}`,
    `Date   : ${episode.date || 'Unknown'}`,
    `Podcast: ${episode.podcastId || podcastId}`,
    `Source : ${episode.url || ''}`,
    '',
    'KEY CLAIMS WITH TIMEFRAMES',
    '==========================',
    '',
  ];

  if (claimsText.trim() === 'NO_CLAIMS_WITH_TIMEFRAMES') {
    lines.push('(No claims with explicit timeframes found in this episode.)');
  } else {
    lines.push(claimsText.trim());
  }

  lines.push('');
  fs.writeFileSync(claimsPath(podcastId, episodeFile), lines.join('\n'), 'utf8');
}

// ---------------------------------------------------------------------------
// Batch submission
// ---------------------------------------------------------------------------

async function submitBatch(pendingEpisodes) {
  console.log(`Building ${pendingEpisodes.length} batch requests...`);

  const requests = pendingEpisodes.map(({ customId, filePath }) => {
    const episode = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return {
      custom_id: customId,
      params: {
        model: 'claude-opus-4-6',
        max_tokens: 1024,
        system: SYSTEM_PROMPT,
        messages: [{ role: 'user', content: buildUserMessage(episode) }],
      },
    };
  });

  const batch = await client.messages.batches.create({ requests });
  console.log(`Batch submitted — ID: ${batch.id}`);
  console.log(`Status: ${batch.processing_status}`);
  return batch;
}

// ---------------------------------------------------------------------------
// Poll
// ---------------------------------------------------------------------------

async function pollBatch(batchId) {
  console.log('\nPolling for completion (checking every 30s)...');
  const startTime = Date.now();

  while (true) {
    const batch = await client.messages.batches.retrieve(batchId);
    const { processing, succeeded, errored, expired } = batch.request_counts;
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const total = processing + succeeded + errored + expired;

    process.stdout.write(
      `\r[${new Date().toISOString().slice(11, 19)}] +${elapsed}s | ` +
      `done: ${succeeded + errored + expired}/${total} | ` +
      `succeeded: ${succeeded} | errored: ${errored} | processing: ${processing}   `
    );

    if (batch.processing_status === 'ended') {
      console.log('\nBatch complete.');
      return batch;
    }

    await new Promise((r) => setTimeout(r, 30000));
  }
}

// ---------------------------------------------------------------------------
// Write results
// ---------------------------------------------------------------------------

async function writeResults(batchId, episodeMap) {
  console.log('\nWriting claim files...');
  let written = 0;
  let errors = 0;
  let noClaimsCount = 0;

  for await (const result of await client.messages.batches.results(batchId)) {
    const [podcastId, file] = result.custom_id.split('||');
    const episode = episodeMap.get(result.custom_id);

    if (result.result.type === 'succeeded') {
      const textBlock = result.result.message.content.find((b) => b.type === 'text');
      if (textBlock && episode) {
        writeClaimsFile(podcastId, file, episode, textBlock.text);
        if (textBlock.text.trim() === 'NO_CLAIMS_WITH_TIMEFRAMES') noClaimsCount++;
        written++;
        if (written % 50 === 0) console.log(`  ... ${written} written`);
      }
    } else {
      console.error(`\n  ✗ Error for ${result.custom_id}: ${result.result.error?.type}`);
      errors++;
    }
  }

  return { written, errors, noClaimsCount };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  const opts = parseArgs();

  const podcastIds = opts.podcast ? [opts.podcast] : ['morgan-stanley', 'goldman-sachs'];

  // Collect episodes needing processing
  const pending = [];
  const episodeMap = new Map();
  let alreadyDone = 0;

  for (const podcastId of podcastIds) {
    for (const ep of getEpisodeFiles(podcastId)) {
      if (fs.existsSync(claimsPath(ep.podcastId, ep.file))) {
        alreadyDone++;
        continue;
      }
      pending.push(ep);
      const episode = JSON.parse(fs.readFileSync(ep.filePath, 'utf8'));
      episodeMap.set(ep.customId, episode);
    }
  }

  console.log(`\nClaim Extractor`);
  console.log(`  Podcasts  : ${podcastIds.join(', ')}`);
  console.log(`  Already done: ${alreadyDone}`);
  console.log(`  To process  : ${pending.length}`);

  if (pending.length === 0) {
    console.log('\nAll claims already extracted!');
    return;
  }

  // Resume or submit
  let batchId = opts.resume;

  if (!batchId) {
    const state = loadBatchState();
    if (state?.batchId) {
      console.log(`\nFound existing batch: ${state.batchId} (submitted ${state.submittedAt})`);
      console.log('Resuming... (pass --resume <id> to resume a different batch)');
      batchId = state.batchId;
    } else {
      const batch = await submitBatch(pending);
      batchId = batch.id;
      saveBatchState({
        batchId,
        count: pending.length,
        submittedAt: new Date().toISOString(),
        podcasts: podcastIds,
      });
    }
  }

  // Poll until done
  await pollBatch(batchId);

  // Write claim files
  const { written, errors, noClaimsCount } = await writeResults(batchId, episodeMap);

  // Clean up state file
  if (fs.existsSync(BATCH_STATE_FILE)) fs.unlinkSync(BATCH_STATE_FILE);

  console.log('\n=== Summary ===');
  console.log(`Written : ${written} claim files`);
  console.log(`Errors  : ${errors}`);
  console.log(`No claims found: ${noClaimsCount} episodes`);
  console.log(`Output  : apps/podcast-transcripts/claims/`);
}

main().catch((err) => {
  console.error('\nFatal error:', err.message);
  process.exit(1);
});
