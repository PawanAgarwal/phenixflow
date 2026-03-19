# Podcast Prediction

This folder contains the podcast market-claim extraction work for Morgan Stanley and Goldman Sachs.

Contents:
- `build_full_podcast_market_claim_table.py`: full pipeline, including direct transcript retrieval and the older fallback path.
- `build_web_only_podcast_market_claim_table.py`: web-only pipeline used for the final accepted table.
- `outputs/podcast_claims/`: final reviewable deliverables, including the Goldman and Morgan review Markdown files sorted by date.
- `outputs/model_comparison/`: small benchmark comparing local `gemma3` with OpenAI on a few transcripts.

Intentionally not committed:
- `runtime/`: regenerated local metadata and bulky scrape, transcript, and audio caches produced during collection and analysis.

Useful entry points:
- `python podcast_prediction/build_web_only_podcast_market_claim_table.py`
- `python podcast_prediction/build_full_podcast_market_claim_table.py`
