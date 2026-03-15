# Alma Research Archive

This folder is the single home for Alma-specific materials in this repo: the archive, the working knowledge docs, the approved skip list, and the export tool.

Layout:
- `manifest.json`: sync index with latest/oldest dates and per-item file paths
- `approved-skips.json`: explicitly approved blocked-resource patterns to skip without stopping the export
- `knowledge/`: Alma methodology and reconstruction notes
- `tools/export-alma-substack-archive.js`: exporter that attaches to the logged-in Playwright browser session
- `posts/pages/`: raw archive API pages used to enumerate publication posts
- `posts/YYYY-MM-DD_slug/`: one folder per Substack post with `metadata.json`, `source.html`, `content.txt`, downloaded images, and accessible linked assets
- `chats/pages/`: raw community feed pages used to enumerate Alma chat posts
- `chats/inbox-snapshot.json`: current Substack inbox snapshot from the logged-in session
- `chats/YYYY-MM-DD_slug/`: one folder per Alma-authored chat post with `metadata.json`, `post.json`, paginated reply JSON, `source.html`, transcript text, and downloaded media

Refresh command:

```bash
node artifacts/alma-research/tools/export-alma-substack-archive.js
```

Force refetch of already archived items:

```bash
node artifacts/alma-research/tools/export-alma-substack-archive.js --refresh 1
```

Wipe and rebuild from scratch:

```bash
node artifacts/alma-research/tools/export-alma-substack-archive.js --wipe 1
```

Notes:
- The exporter attaches to the live Playwright browser via CDP at `http://127.0.0.1:58210`.
- Paid publication posts are archived from the logged-in `stochvoltrader.substack.com` session.
- Alma chat replies are paginated and archived when the current session exposes them.
- Inaccessible Google Drive links are skipped if they match the approved skip list in `approved-skips.json`.
