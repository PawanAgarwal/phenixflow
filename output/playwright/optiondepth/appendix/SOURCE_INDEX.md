# OptionDepth Methodology Source Index

Captured on 2026-03-15.

Purpose: keep a durable local provenance trail for the external papers and research PDFs used while building the OptionDepth reconstruction methodology.

## Source List

| Source | Local file | Type | Notes | Used in |
|---|---|---|---|---|
| Does 0DTE Options Trading Increase Volatility? | `does-0dte-options-trading-increase-volatility-source-brief.pdf` | Local provenance brief | Original SSRN abstract/PDF blocked by Cloudflare from this API environment | `OPTIONDEPTH_EOD_RECONSTRUCTION_METHOD.md` Section 19 |
| Retail Traders Love 0DTE Options... But Should They? | `retail-traders-love-0dte-options-but-should-they-working-paper.pdf` | Full working-paper PDF | Lancaster FoFI workshop mirror | `OPTIONDEPTH_EOD_RECONSTRUCTION_METHOD.md` Section 19 |
| Options market makers' hedging and informed trading: Theory and evidence | `options-market-makers-hedging-and-informed-trading-source-brief.pdf` | Local provenance brief | Canonical DOI preserved; publisher PDF gated in this environment | `OPTIONDEPTH_EOD_RECONSTRUCTION_METHOD.md` Sections 7 and 19 |
| Vibrato and Automatic Differentiation for High Order Derivatives and Sensitivities of Financial Options | `vibrato-and-automatic-differentiation-for-high-order-derivatives-and-sensitivities-of-financial-options-arxiv-1606.06143.pdf` | Full arXiv PDF | Supplemental formula/computation reference for higher-order Greeks and numerical differentiation | `OPTIONDEPTH_EOD_RECONSTRUCTION_METHOD.md` Sections 4, 5, and 11 |
| 0DTE Index Options and Market Volatility: How Large Is Their Impact? | `0dte-index-options-and-market-volatility-how-large-is-their-impact-cboe.pdf` | Full PDF | Supplemental 0DTE market-impact reference | `OPTIONDEPTH_EOD_RECONSTRUCTION_METHOD.md` Section 19 |

## Canonical URLs

- `https://ssrn.com/abstract=4426358`
- `https://wp.lancs.ac.uk/fofi2024/files/2024/04/FoFI-2024-146-Leander-Gayda.pdf`
- `https://doi.org/10.1016/j.finmar.2015.01.001`
- `https://arxiv.org/abs/1606.06143`
- `https://cdn.cboe.com/resources/education/research_publications/gammasqueezes.pdf`

## Notes

- The higher-order Greek formulas in the methodology doc are standard BSM identities and implementation choices, not claims that OptionDepth itself published those exact formulas.
- The Alma-style `speed` and `color` formulas remain educated reconstructions. The papers above help constrain what is plausible, but they do not reveal Alma's private production code.
