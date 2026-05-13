#!/usr/bin/env node
// Final summary table across all 9 research items.

const fs = require('node:fs');
const path = require('node:path');

const RESEARCH_DIR = path.resolve(__dirname, '..', '..', 'artifacts', 'research');

const ITEMS = [
  { id: 'S1', file: 's1-fear-2of2-confirmation.json',  label: 'S1 — 2-of-2 fear-extreme confirmation',          getStats: (j) => ({ sharpe: j.results.full_16mo_sensitivity.sharpe_per_trade, hit: j.results.full_16mo_sensitivity.hit_rate, trades: j.results.full_16mo_sensitivity.trade_count, net: j.results.full_16mo_sensitivity.total_net_pct, verdict: j.verdict }) },
  { id: 'S2', file: 's2-fear-portfolio-combiner.json',  label: 'S2 — Equal-weight 4-variant fear portfolio',    getStats: (j) => ({ sharpe: j.portfolio_stats.full_16mo_sensitivity['equal-weight 4-variant'].sharpe_per_trade, hit: j.portfolio_stats.full_16mo_sensitivity['equal-weight 4-variant'].hit_rate, trades: j.portfolio_stats.full_16mo_sensitivity['equal-weight 4-variant'].trade_count, net: j.portfolio_stats.full_16mo_sensitivity['equal-weight 4-variant'].total_net_pct, verdict: j.verdict }) },
  { id: 'S3', file: 's3-3of3-stack.json',  label: 'S3 — 3-of-3 stack (PYM + OCC + VIX)',                       getStats: (j) => ({ sharpe: j.results.full_16mo_sensitivity.sharpe_per_trade, hit: j.results.full_16mo_sensitivity.hit_rate, trades: j.results.full_16mo_sensitivity.trade_count, net: j.results.full_16mo_sensitivity.total_net_pct, verdict: j.verdict }) },
  { id: 'A2', file: 'a2-multiday-drawdown-buythedip.json', label: 'A2 — Multi-day SPY drawdown buy-the-dip',     getStats: (j) => {
    const intra = j.results.intraday.full_16mo_sensitivity;
    const over = j.results.overnight.full_16mo_sensitivity;
    const best = (over.total_net_pct || 0) >= (intra.total_net_pct || 0) ? over : intra;
    const bestMode = best === over ? 'overnight' : 'intraday';
    return { sharpe: best.sharpe_per_trade, hit: best.hit_rate, trades: best.trade_count, net: best.total_net_pct, verdict: `intraday=${j.verdicts.intraday}; overnight=${j.verdicts.overnight}`, mode: bestMode };
  } },
  { id: 'A3', file: 'a3-vvix-spike-contrarian.json', label: 'A3 — VVIX spike contrarian',                       getStats: (j) => ({ sharpe: j.results.full_16mo_sensitivity.sharpe_per_trade, hit: j.results.full_16mo_sensitivity.hit_rate, trades: j.results.full_16mo_sensitivity.trade_count, net: j.results.full_16mo_sensitivity.total_net_pct, verdict: j.verdict }) },
  { id: 'B1', file: 'b1-premarket-gap-fade.json', label: 'B1 — Pre-market gap fade',                            getStats: (j) => {
    // Report the best (gap-down-long was the survivor).
    const variants = Object.keys(j.results);
    let best = null; let bestKey = null;
    for (const k of variants) {
      const s = j.results[k].full_16mo_sensitivity;
      if (best === null || s.total_net_pct > best.total_net_pct) { best = s; bestKey = k; }
    }
    return { sharpe: best.sharpe_per_trade, hit: best.hit_rate, trades: best.trade_count, net: best.total_net_pct, verdict: `down-long=${j.verdicts['gap-down-long-3x']}; up-short=${j.verdicts['gap-up-short-3x']}; both=${j.verdicts['both-fade-3x']}`, mode: bestKey };
  } },
  { id: 'B2', file: 'b2-first-hour-range-break.json', label: 'B2 — First-hour range break',                     getStats: (j) => {
    const m30 = j.results['30m'].full_16mo_sensitivity;
    const m60 = j.results['60m'].full_16mo_sensitivity;
    const best = m30.total_net_pct >= m60.total_net_pct ? m30 : m60;
    return { sharpe: best.sharpe_per_trade, hit: best.hit_rate, trades: best.trade_count, net: best.total_net_pct, verdict: `30m=${j.verdicts['30m']}; 60m=${j.verdicts['60m']}`, mode: best === m30 ? '30m' : '60m' };
  } },
  { id: 'A1', file: 'a1-credit-stress-contrarian.json', label: 'A1 — HYG/LQD credit-stress contrarian',         getStats: (j) => {
    const v1 = j.results['lev-1x'].full_16mo_sensitivity;
    const v3 = j.results['lev-3x'].full_16mo_sensitivity;
    const best = v1.total_net_pct >= v3.total_net_pct ? v1 : v3;
    return { sharpe: best.sharpe_per_trade, hit: best.hit_rate, trades: best.trade_count, net: best.total_net_pct, verdict: `1x=${j.verdicts['lev-1x']}; 3x=${j.verdicts['lev-3x']}`, mode: best === v1 ? '1x' : '3x' };
  } },
  { id: 'B3', file: 'b3-seasonality-overlay.json', label: 'B3 — Calendar seasonality overlay',                  getStats: (j) => {
    const all = j.results.full_16mo.all;
    return { sharpe: all.sharpe, hit: all.hit, trades: all.n, net: (all.mean * all.n) * 100, verdict: j.verdict, mode: 'overlay-multipliers-only' };
  } },
];

function readyToRegister(verdict) {
  return /\bstrong\b/i.test(verdict) || /\bmarginal\b/i.test(verdict);
}

function main() {
  const rows = [];
  for (const item of ITEMS) {
    const fp = path.join(RESEARCH_DIR, item.file);
    if (!fs.existsSync(fp)) {
      rows.push({ id: item.id, label: item.label, missing: true });
      continue;
    }
    const j = JSON.parse(fs.readFileSync(fp, 'utf8'));
    const s = item.getStats(j);
    rows.push({ id: item.id, label: item.label, ...s, ready: readyToRegister(s.verdict) });
  }

  // Print final table
  process.stdout.write('\n=== Final summary across all 9 research items ===\n');
  process.stdout.write('Item  Description                                              Sharpe  Hit%    Trades  Net%      Ready?  Verdict\n');
  process.stdout.write('----  -------------------------------------------------------  ------  ------  ------  --------  ------  -------\n');
  for (const r of rows) {
    if (r.missing) {
      process.stdout.write(`${r.id.padEnd(6)}${r.label.padEnd(57)}  (artifact missing)\n`);
      continue;
    }
    const row = [
      r.id.padEnd(6),
      r.label.padEnd(57),
      r.sharpe.toFixed(2).padStart(6),
      (r.hit * 100).toFixed(1).padStart(6),
      String(r.trades).padStart(6),
      r.net.toFixed(2).padStart(8),
      (r.ready ? 'Y' : 'N').padStart(6),
      r.verdict,
    ];
    process.stdout.write(row.join('  ') + '\n');
  }

  // Quick "ready to register" set
  const ready = rows.filter((r) => r.ready);
  process.stdout.write(`\nReady-to-register items: ${ready.map((r) => r.id).join(', ') || '(none)'}\n`);
  process.stdout.write('Reminder: per backlog rules these are NOT auto-registered.  User must decide which to wire into the strategy registry.\n');

  const outPath = path.join(RESEARCH_DIR, '_summary.json');
  fs.writeFileSync(outPath, JSON.stringify({ generated_at: new Date().toISOString(), rows }, null, 2));
  process.stdout.write(`\nWritten ${outPath}\n`);
}

main();
