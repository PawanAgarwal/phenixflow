# Alma Prediction Capture

This pass is capture-first: one row per prediction, with timestamps and basis, and no evaluation filled in yet.
Commentary and chats are extracted with cached OpenAI structured output first, script tables stay rule-based, and OptionsDepth heatmaps are captured as a separate image-derived basis.

## What is here

- [weekly-predictions.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/weekly-predictions.md) is the weekly prediction capture view.
- [methodology.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/methodology.md) explains the capture rules.
- [build-state.json](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/build-state.json) records the latest incremental checkpoint and source hashes.
- [daily/2025-01.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-01.md) is the daily prediction table for 2025-01.
- [daily/2025-02.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-02.md) is the daily prediction table for 2025-02.
- [daily/2025-03.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-03.md) is the daily prediction table for 2025-03.
- [daily/2025-04.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-04.md) is the daily prediction table for 2025-04.
- [daily/2025-05.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-05.md) is the daily prediction table for 2025-05.
- [daily/2025-06.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-06.md) is the daily prediction table for 2025-06.
- [daily/2025-07.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-07.md) is the daily prediction table for 2025-07.
- [daily/2025-08.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-08.md) is the daily prediction table for 2025-08.
- [daily/2025-09.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-09.md) is the daily prediction table for 2025-09.
- [daily/2025-10.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-10.md) is the daily prediction table for 2025-10.
- [daily/2025-11.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-11.md) is the daily prediction table for 2025-11.
- [daily/2025-12.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2025-12.md) is the daily prediction table for 2025-12.
- [daily/2026-01.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2026-01.md) is the daily prediction table for 2026-01.
- [daily/2026-02.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2026-02.md) is the daily prediction table for 2026-02.
- [daily/2026-03.md](/Users/pawanagarwal/github/phenixflow/artifacts/alma-research/analysis/predictions/daily/2026-03.md) is the daily prediction table for 2026-03.

## Snapshot

- Daily prediction rows: 4665
- Weekly prediction rows: 457
- Commentary rows: 3071
- Script-level rows: 822
- Heatmap rows: 772

## Notes

- Multiple predictions on the same target date are kept as separate rows.
- Rows are ordered by target date, then prediction-made time, earliest first.
- Displayed prediction timestamps are shown in PT.
- Instrument tagging now tries to keep one market per row when the text supports it.
- When an instrument is tagged, the ledger also captures the closest available ThetaData or Yahoo reference value and the actual symbol used for that lookup.
- Script-table rows are intentionally narrowed to SPX only; Alma commentary rows and Alma chat rows still keep all markets she explicitly talks about.
- Weekly rows now include weekly-post predictions and week-duration predictions coming from posts or chats.
- Weekly and vague forward comments are expanded into daily rows for the relevant future days.
- Incremental builds reuse source hashes and only rebuild new or changed Alma sources plus the recent overlap window.
- `Proxy actual value` and `Aligned?` are left blank on purpose for the next evaluation pass.

