# Boxing Closing-Odds Source Research

**Date:** 2026-04-24
**Goal:** Find a public historical boxing closing-odds dataset analogous to tennis-data.co.uk for tennis. Need at least moneyline closing odds covering 2018-2025 for ~2,000+ fights to backtest a model with credible ROI/edge measurement.

## Probe Results

| Source | Accessible | Coverage | Format | Cost | Verdict |
|---|---|---|---|---|---|
| **proboxingodds.com** | YES (HTTP 200, no Cloudflare wall on event pages) | 2016-2026, 1,733 events in sitemap | Server-rendered HTML, multi-book moneyline + props per event | Free | **PRIMARY** |
| **oddsportal.com/boxing/results/** | YES (HTTP 200, 530KB) | Multi-year (depth unverified, dynamic JS list) | HTML, JS-heavy listing pages, odds in detail pages | Free | **SECONDARY (cross-check)** |
| **bestfightodds.com/archive** | YES (HTTP 200) | UFC/MMA only — boxing redirects to ProBoxingOdds | HTML | Free | Same operator as PBO; use PBO directly |
| **betmma.tips** | NO — Cloudflare JS challenge (HTTP 403) | Unknown | HTML | Free | Skip; would require browser automation |
| **sportsbookreview.com/betting-odds/boxing/** | Partial (HTTP 200 but page is editorial picks, not a structured archive) | Recent only, no per-fight closing-odds table | HTML, blog-like | Free | Insufficient; skip |
| **the-odds-api.com** | API responds (HTTP 401 without key) | Boxing endpoint exists; historical only on paid plans | JSON | Paid (~$30+/mo for historical, free tier is current-only) | Skip unless PBO+OP fail |
| **Action Network** | Not probed | Unknown | API/HTML | Paid | Skip |
| **DataGolf-style boxing** | None found | — | — | — | Does not exist |
| **Pinnacle (curl_cffi)** | Not probed in detail | Live only — no completed-event archive endpoint exposed | JSON | Free | No historical archive available |
| **Wayback Machine (PBO snapshots)** | YES (CDX API works, snapshots from 2019+) | Backup for any pages that break | HTML | Free | Tertiary fallback only |
| **Kaggle / GitHub** | Datasets exist but none with per-fight betting odds (only fight outcomes / fighter stats) | n/a | CSV | Free | Insufficient for odds backtest |

## Recommended Primary: proboxingodds.com

Same operator as BestFightOdds (Pacific Tide Media Inc.), which we already use for MMA. Boxing is on a separate domain.

### Enumeration

The sitemap at `https://www.proboxingodds.com/sitemap-teams.xml` (misleading filename — it actually contains event URLs) lists every event. Fetched 2026-04-24:

| Year | Event count |
|---|---|
| 2016 | 162 |
| 2017 | 151 |
| 2018 | 129 |
| 2019 | 135 |
| 2020 | 143 |
| 2021 | 172 |
| 2022 | 182 |
| 2023 | 198 |
| 2024 | 209 |
| 2025 | 179 |
| 2026 | 72 (in progress) |
| **2018-2025 total** | **1,347 events** |

URL pattern: `https://www.proboxingodds.com/events/{YYYY-MM-DD}-{event_id}` (no auth required).

### Per-event payload

Verified on `https://www.proboxingodds.com/events/2023-05-20-1750` (Devin Haney vs. Vasyl Lomachenko, 1.5 MB HTML):

- **Moneyline** per fighter, per bookmaker. Books observed in modern events: Bet365, BetWay, FanDuel, DraftKings, BetMGM, Caesars, BetRivers, PointsBet (8 books). Older events (2018-2020) likely had Bookmaker.eu, 5Dimes, Bovada, William Hill — same as BFO archive.
- **Opening line** is embedded in inline JSON (`opening`, `data`: [[fighter_name, opening_odds]]). Current displayed cell == latest available (effectively closing for completed events).
- **Method props**: "wins by decision", "wins by TKO/KO or DQ", "wins in round 1", ..., "wins in round 12", "fight goes to decision", "fight is a draw", "both fighters knocked down", "fighter is knocked down". This matches BFO MMA prop structure exactly.
- **Round group props**: "wins in round 1-2", "3-4", "7-8" etc.
- **Totals**: Over/Under N rounds.

### Estimated scrape effort

- 1,347 events x ~13 fights/event x 8 bookmakers = ~140k moneyline rows; ~17,500 fight-rows after collapsing books.
- At 1.5s/request (polite): 1,347 events ≈ 34 minutes wall-clock. Comfortable single-session run.
- Implementation: ~150 LOC in Python — fetch sitemap, regex out event URLs, fetch each, parse `<table class="odds-table">` rows + inline `opening` JSON. Dedup by `(event_id, fighter_pair)`.
- Output schema (suggested): `event_date, event_id, fighter_a, fighter_b, book, ml_open_a, ml_open_b, ml_close_a, ml_close_b, prop_type, prop_selection, prop_odds`.

## Recommended Secondary: oddsportal.com/boxing/results/

Use to cross-check disputed closing prices and to fill any gaps. Drawback: dynamic JS pagination and Cloudflare-friendly headers required; per-event pages need browser-like fetching (curl_cffi or playwright). Effort is ~3-5x higher than PBO. Recommend only spinning this up if PBO coverage gaps are found in 2018-2019 data.

## Tertiary: Wayback Machine

CDX API works on `proboxingodds.com/events/*` with snapshots back to 2019. Use only to recover events PBO has dropped from current site (none observed yet) or to validate that a "current" cell really was the closing price near fight time.

## Cost

- **Money:** $0. All sources free.
- **Time:** ~1 hour to write & validate scraper, ~35 min to run, ~30 min QA = 2-2.5 hours total to first usable dataset.

## Backtest Sufficiency Verdict

**YES — credible backtesting is feasible without paid services.**

- **Moneyline backtest:** Strongly viable. ~17.5k fight-rows across 8 sharp/recreational books for 2016-2025, with both opening and closing prices per book. Plenty for proper edge/CLV measurement.
- **Method-of-victory props:** Viable. Decision/KO/TKO splits available on most events with ~5+ books typically pricing them.
- **Round-by-round props:** Viable but sparser — round-N props mostly available for marquee/title fights (estimate 30-40% of events). Sufficient to backtest a round-distribution model on the high-profile subset (~400 fights).
- **Caveats:**
  - PBO does not include Pinnacle (Pinnacle blocks US so PBO doesn't list them). For Pinnacle closing as the "true" line, would need OddsPortal cross-reference.
  - Pre-2018 method-prop coverage is thinner; safest to scope round-prop work to 2020+.
  - "Closing" here means last displayed value before the page froze — usually within minutes of fight time, but not guaranteed to be the literal final-second tick.

## Next Steps (out of scope, recommended)

1. Implement `scripts/scrape_proboxingodds.py` — sitemap-driven, polite (1.5s+), resumable.
2. Land `data/proboxingodds_raw/{event_id}.html` cache so re-parses are free.
3. Parse into `data/boxing_odds.parquet` with the schema above.
4. Join to BoxRec scraper output (parallel agent's `boxrec_*.csv`) on fighter name + date for outcome/method/rounds ground truth.
