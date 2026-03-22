# Podcast Prediction

This folder contains the podcast market-claim extraction work for Morgan Stanley and Goldman Sachs.

Contents:
- `build_full_podcast_market_claim_table.py`: full pipeline, including direct transcript retrieval and the older fallback path.
- `build_web_only_podcast_market_claim_table.py`: web-only pipeline used for the final accepted table.
- `build_web_only_podcast_predictions.py`: Codex-based prediction extraction pipeline that works from direct web transcripts when available and otherwise falls back to the firm page summary.
- `prediction_support.py`: shared metadata collection, transcript extraction, prompt assembly, and Codex execution helpers for the prediction pipeline.
- `outputs/podcast_claims/`: final reviewable deliverables, including the Goldman and Morgan review Markdown files sorted by date.
- `outputs/podcast_predictions/`: generated prediction review files written by the Codex-based pipeline.
- `outputs/model_comparison/`: small benchmark comparing local `gemma3` with OpenAI on a few transcripts.

Intentionally not committed:
- `runtime/`: regenerated local metadata and bulky scrape, transcript, and audio caches produced during collection and analysis.

Useful entry points:
- `python podcast_prediction/build_web_only_podcast_market_claim_table.py`
- `python podcast_prediction/build_full_podcast_market_claim_table.py`
- `python podcast_prediction/build_web_only_podcast_predictions.py`
