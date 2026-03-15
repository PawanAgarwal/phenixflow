# Stochastic Volatility Substack Research

This folder captures Alma's published method from the `Stochastic Volatility Trader - Quant Insights` Substack and maps it to what PhenixFlow can build with ThetaData.

Primary conclusion:

- Alma's edge is not a lagging put/call-ratio workflow.
- She uses end-of-day option positioning to infer the next session's expected structure, hidden spread pattern, pivot zones, and implied distribution.
- The intraday job is then to validate or reject that expected structure by watching realized volatility, spot/volatility behavior, and liquidity migration.

Docs in this folder:

- `alma-framework.md`: condensed method notes from the key posts reviewed in the logged-in browser session.
- `thetadata-reconstruction.md`: how to recreate an OptionDepth-like structural view with the data already available in this repo.

Method-bearing sources reviewed:

- `A Guide to Reading My Daily Posts` (2025-11-12)
- `Liquidity structure | Let's put speed profile into context` (2026-01-08)
- `What is volatility?` (2025-01-25)
- `January OpEx week quant breakdown | Weekly post (12-16/Jan) - FREE` (2026-01-11)
- `Window of Risk Pt. 2 - Flows update` (2025-10-29)
- `OpEx, VIXpery and PCE week | Weekly post (17-20/Feb)` (2026-02-17)

Chat/discussion note:

- The active publication chat thread exposed in the current browser session was `WEEKEND CHAT (13/March)` with current pub-chat metadata and comment activity.
- The browser session did not expose a clean historical web archive of full prior chat bodies, so the durable methodology here is driven mainly by the posts above plus the active chat snapshot.

Practical takeaway for PhenixFlow:

- We do not need the proprietary OptionDepth product to start.
- We do need a synthetic end-of-day structural surface built from chain data, open interest, spot, IV, and local greek reconstruction.
- The main build target is a forward-looking liquidity/realized-volatility map, not a raw order-book clone.
