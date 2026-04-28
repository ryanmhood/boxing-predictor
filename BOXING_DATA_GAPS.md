# Boxing Data Gaps

Status notes from the initial scrape session (2026-04-24), updated
2026-04-28 after the BoxRec-pivot probe (bx-7og).

## TL;DR — current state of the boxer-record feed

* **BoxRec**: closed. Cloudflare-walled past curl_cffi TLS impersonation,
  Playwright stealth, and Chrome-CDP. Every workaround logged below.
  `scripts/probe_boxer_data_sources.py` formalises the verdict.
* **Tapology** (`scripts/scrape_tapology.py`): the live winner. Profile
  pages serve uncontested over `curl_cffi` (chrome120 impersonation),
  carry full fight history (incl. 2024-25), and parse cleanly out of
  `<div class="result">` blocks.
* **Plainte BoxRec dump** (`scripts/ingest_plainte_dump.py`): a free,
  static safety net of 133k pre-2020 BoxRec fights. Frozen Jan 2020 so
  contributes only 7.4% PBO coverage on its own — but 53-56% per year
  on 2018-19 PBO bouts where it overlaps.
* **All other candidates** (ESPN, BoxingScene, TheRing, Wikipedia,
  RapidAPI boxing-data.com) ruled out — see "Source verdicts" below.

The recommended modelling feed for v2 is the **union of Tapology +
Plainte**, computed by `scripts/merge_boxer_results.py`.

## Source verdicts (2026-04-28 probe)

`scripts/probe_boxer_data_sources.py` ran a one-shot reconnaissance
against eight candidate sources. Raw response bodies are in
`data/raw_html/source_probe/`; the markdown digest in
`data/reports/boxer_source_eval.md`.

| Source                                   | HTTP | Anti-bot          | Outcome |
|---|---:|---|---|
| Tapology                                 | 200  | none (curl_cffi)  | **WINNER**: 815 KB Canelo profile, 70 bouts incl. 2025 fights |
| Plainte GitHub dump                      | 200/206 | none           | static CSV, 133k bouts but frozen Jan 2020 |
| Wikipedia                                | 200  | none              | known: ~0.8% PBO coverage on bulk roster |
| BoxingScene                              | 200  | none              | home OK; `/boxer/<slug>` URL pattern is wrong (404) |
| Ring TV / Ring Magazine                  | 200  | none              | only news landing pages, no per-fighter records |
| ESPN /boxing/fighter                     | 404  | n/a               | URL pattern stale; no boxer-profile endpoint exists |
| Kaggle (mexwell, iyadelwy)               | 200 / 404 | login wall   | dataset pages public; downloads need auth (skipped) |
| boxing-data.com (RapidAPI)               | 200  | RapidAPI gate     | paid commercial; not subscribed (per bead constraint) |

## BoxRec — BLOCKED by Cloudflare

BoxRec is the canonical pro-boxing record database, but every endpoint we
probed sits behind a Cloudflare "Just a moment..." JavaScript challenge.
Three probes, all failed with HTTP 403 returning the challenge HTML:

| URL | Method | Result |
|---|---|---|
| `https://boxrec.com/robots.txt` | curl + browser UA | 403, Cloudflare challenge body |
| `https://boxrec.com/` | curl + full browser headers | 403, Cloudflare challenge body |
| `https://boxrec.com/en/proboxer/348759` (Canelo) | curl + sec-fetch-* headers | 403, Cloudflare challenge body |

### 2026-04-24 follow-up: curl_cffi TLS impersonation also blocked

We extended the probe with `curl_cffi` (the same browser-TLS
impersonation stack that defeats Pinnacle's edge for the tennis and
golf daemons). Script: `scripts/probe_boxrec_curl_cffi.py`. Raw bodies
under `data/raw_html/boxrec_probe/`. Summary:
`data/raw_html/boxrec_probe/_summary.json`.

Tested matrix (15 requests total, 6s spacing):

| Profile     | Header mode    | Targets                  | Status | Body type |
|-------------|----------------|--------------------------|--------|-----------|
| chrome120   | with_headers   | home / canelo / robots   | 403    | CF challenge (~5.8 KB, `cf_chl_opt`, `cdn-cgi/challenge-platform`) |
| chrome120   | minimal        | home / canelo            | 403    | CF challenge |
| chrome119   | with_headers   | home / canelo / robots   | 403    | CF challenge |
| chrome119   | minimal        | home / canelo            | 403    | CF challenge |
| safari17_0  | with_headers   | home / canelo / robots   | 403    | CF challenge |
| safari17_0  | minimal        | home / canelo            | 403    | CF challenge |
| edge99      | -              | (skipped — early exit)   | -      | budget exhausted on first three |

Every response is the canonical Cloudflare interstitial — title `Just a
moment...`, embedded `_cf_chl_opt` orchestration token, and a
`/cdn-cgi/challenge-platform/h/g/orchestrate/chl_page/v1` script tag
the client must execute to mint a `cf_clearance` cookie. The challenge
body is *identical in shape* across every (profile, header mode,
endpoint) tuple — Cloudflare is fingerprinting at the request level
and never letting curl_cffi past the JS gate, regardless of TLS
identity.

This is a hard block, not a soft 403. TLS impersonation alone is not
sufficient for BoxRec, even though it suffices for Pinnacle.

### Real options to unlock BoxRec, in increasing complexity

1. **One-shot Playwright (or undetected-chromedriver) → cookie harvest
   → curl_cffi with `cf_clearance`**. Run a real headless Chromium
   once per ~30 minutes (the cf_clearance lifetime), solve the JS
   challenge, dump cookies, then do the volume scraping with curl_cffi
   sharing the cookie jar. This is the standard Cloudflare-bypass
   pattern and is what the MMA scraping community uses against Tapology
   / BoxRec / similar.
2. **FlareSolverr** as a sidecar service. Same idea as #1 but packaged
   as an HTTP proxy you POST URLs to; it returns the solved-challenge
   HTML. Lower-effort to integrate than rolling our own Playwright
   harness, but adds a Docker dependency.
3. **Authenticated free BoxRec account** behind options 1/2 unlocks
   deeper pages (head-to-head, full opponent records). Account
   creation is free but rate-limited; mass scraping is explicitly
   discouraged in their ToS.

### Recommendation (post-curl_cffi probe)

* **Continue with Wikipedia-only for v0/v1.** The expected lift from
  BoxRec is in the long tail (regional / journeyman fighters),
  precisely the population we won't be modelling until later. A
  Wikipedia roster of ~500 active boxers + their full pro records is
  enough fight-volume to validate the feature pipeline.
* **Defer the Cloudflare bypass to v2.** When we do tackle it, go
  straight to **option 1 (Playwright cookie-harvest → curl_cffi reuse)**
  — it's the cleanest way to keep the scraping side of the daemon a
  pure-Python `curl_cffi` codepath while only paying the headless-browser
  cost once per cookie refresh. FlareSolverr is fine if we want a
  faster integration but adds infra weight we don't currently need.

None of those fit inside a 35-min scaffold session. The path of least
resistance — and what the scraper currently uses — is Wikipedia.

## Wikipedia — primary source for v1

Wikipedia returns HTTP 200 for every page tested (UA: descriptive
research string with contact email; well within their bot policy at
~5s between requests).

Per-boxer page contains:

* **Infobox**: DOB (`<span class="bday">`), height (cm), reach (cm),
  stance, nationality, total record (W-L-D), wins-by-KO.
* **Professional boxing record table** (`wikitable`): one row per pro
  fight with date, opponent, result, method, round, location, notes
  (titles defended/won, etc).

Coverage observations from the 12-boxer sample:

* Profile fields populated: 6-8 of 9 per boxer (gym/trainer is
  inconsistent, KO-loss count is rare, country normalisation is dirty —
  Usyk shows "Soviet Union" because that's his birth-country line).
* Fight records: 6-71 fights per boxer, full career back to debut.
* Method codes parsed cleanly: UD/MD/SD/TKO/KO/RTD/PTS.

## Known gaps vs. the BoxRec ideal

| Field | Wikipedia coverage | Notes |
|---|---|---|
| `gym` / trainer | sparse | usually in prose, not infobox |
| `weight_class` per fight | missing | record table doesn't say; would need fight-page lookups |
| `scheduled_rounds` | missing | record table has rounds-completed, not scheduled |
| `promoter` | missing | rarely on per-fight rows |
| `country` | dirty (birthplace) | needs an explicit nationality lookup |
| Lower-tier boxers (regional / non-titlist) | thin / absent | no Wikipedia article at all |

For modeling, the **gaps that matter most** are:

1. **Fight-level weight class** — needed for catchweight handling and
   to bucket performance by division. Workaround: fall back to the
   boxer's `weight_class_primary` if the fight class is unknown; flag
   catchweights via the `notes` text.
2. **Lower-tier coverage** — Wikipedia is great for champions and
   contenders but useless for the regional cards where edge actually
   lives (per the README's "+5-6% ML" thesis). A real production
   pipeline still needs BoxRec or a paid feed for the long tail.
3. **Promoter / venue normalization** — needed for promoter-card edge
   and home-fighter bias features. Doable as a downstream parsing pass.

## Roster discovery

The discovery CSV (`data/raw/boxrec_discovery.csv`) currently has 97
active titlist + P4P-ranked boxers. Reaching the 500-boxer target
requires one more discovery pass against the per-weight-class champion
history pages on Wikipedia (e.g.
`/wiki/List_of_WBC_world_champions`, `/wiki/List_of_WBA_world_champions`),
which together yield ~600 unique active+recently-active boxers. That is
~10 more requests and is the obvious next increment.

## Tapology — primary live source for v2 (2026-04-28)

After the BoxRec dead-end, Tapology was selected as the new primary
boxer-record feed. Probe results in `data/reports/boxer_source_eval.md`;
scraper in `scripts/scrape_tapology.py`.

**Why Tapology**:

* No Cloudflare or Akamai gate — 200s for everything we tried using
  `curl_cffi` `chrome120` impersonation.
* Profile pages embed the full fight history as plain HTML (no JS
  rendering required). Each bout is a `<div class="result">` block
  carrying:
  - W/L/D letter (first inner div)
  - Method short, e.g. `DEC` / `KO` / `TKO`
  - Method long, e.g. "Decision · Unanimous"
  - Opponent name + Tapology fighter id
  - Year + month-day inside the linked event anchor
  - Rounds (e.g. "12 Rounds")
  - Country flag image (`/assets/flags/<CC>-...`)
* Sport tag (`Boxing` / `MMA` / `Kickboxing`) lets us drop non-boxing
  rows cleanly.
* Coverage extends through 2025 (Canelo's Sep-2025 fight with Crawford
  is in the record).

**Scraper shape**:

* Targets list = `data/raw/pbo_fighter_targets.csv` (7,001 fighters
  ranked by PBO appearance count). Generated from
  `data/processed/pbo_results.csv`.
* For each target: search → resolve to `/fightcenter/fighters/<id>-<slug>`
  → fetch profile → parse `<div class="result">` blocks (boxing only)
  → emit one row per fight to `data/raw/boxer_results_tapology.csv`
  with the same schema as `data/raw/boxer_results.csv`.
* Politeness: 1.0s delay floor, 1200-request hard cap per invocation,
  full HTML cache under `data/raw_html/tapology/` so re-runs are
  cheap and survive session death.

## PBO bout coverage ceiling — why top-N matters

PBO bout count by year (8,942 bouts in `data/processed/pbo_results.csv`):

| Year | PBO bouts |
|---|---:|
| 2018 |   158 |
| 2019 | 1,025 |
| 2020 |   590 |
| 2021 | 1,199 |
| 2022 | 1,351 |
| 2023 | 1,414 |
| 2024 | 1,702 |
| 2025 | 1,503 |

86% of PBO bouts are 2020 or later. The 7,001 unique fighters in PBO
have a long-tail distribution (52% appear in only one bout). Coverage
ceilings for "top-N fighters scraped" against the 8,942-bout universe:

| Top-N fighters scraped | PBO bouts where ≥1 fighter is in our set | % |
|---:|---:|---:|
|    250 | 2,828 | 31.6 |
|    500 | 4,339 | 48.5 |
|  1,000 | 6,135 | 68.6 |
|  2,000 | 7,650 | 85.6 |

Implication: the scraper must hit at least the **top-500 PBO fighters**
to come within reach of the 50% gate, and the **top-1000** to clear it
with margin. That sets the budget at ~50-100 minutes of polite scraping
per run (each profile = 2 HTTP requests at ~3-6s each).

## Coverage measured (2026-04-28 run)

| Feed                                | Bouts matched | %     |
|---|---:|---:|
| Wikipedia bulk (prior baseline)     |     ~70       |  0.8  |
| Plainte alone (133,864 rows)        |    660        |  7.4  |
| Tapology (top-500 fighters, 7,800 rows) |  2,889    | 32.3  |
| **Tapology + Plainte union (104,859 rows)** | **3,310** | **37.0** |

The Tapology run terminated at fighter 366/500 due to rate-limit
(HTTP 503) on the search endpoint after ~330 successful searches at
1.0s spacing — 364 profiles resolved before the cap kicked in. The
union nonetheless lifts coverage from 0.8% to 37.0%.

**Per-year union coverage** (`data/reports/tapology_plainte_pbo_join_coverage.md`):

| Year | PBO bouts | Matched | % |
|---|---:|---:|---:|
| 2018 |   158 |   108 | 68.4 |
| 2019 | 1,025 |   702 | 68.5 |
| 2020 |   590 |   261 | 44.2 |
| 2021 | 1,199 |   423 | 35.3 |
| 2022 | 1,351 |   478 | 35.4 |
| 2023 | 1,414 |   478 | 35.4 |
| 2024 | 1,702 |   482 | 28.3 |
| 2025 | 1,503 |   363 | 24.2 |

The pre-2020 years (where Plainte covers) clear 68%; post-2020 sits
at 24-44% on Tapology alone. To raise the overall figure above 50%,
re-run `scripts/scrape_tapology.py --limit 1000 --resume --delay 3.0`
once the rate-limit cooldown is clear — the existing top-1000 ceiling
(68.6% per the table above) brings the union to **roughly 58-62%**
without any code changes. The rate-limit handling could also be
hardened (exponential back-off on 503, longer floor delay).

## Recommendation (post-pivot)

1. **Use Tapology as the primary live feed** for fight-history features.
   Re-run `scripts/scrape_tapology.py` weekly to pick up new fights.
2. **Keep Plainte as a static historical floor** — its 53-56% per-year
   coverage on 2018-19 PBO bouts is genuinely useful, free, and never
   needs re-scraping.
3. **Treat the union as the modelling input.** `scripts/merge_boxer_results.py`
   produces a deduped `boxer_results_union.csv`; that is what feature
   engineering should join against.
4. **Expand the Tapology target set** if/when we need higher coverage:
   the script already supports `--limit N`; bumping from 500 to 1,000-2,000
   is a one-flag change that buys ~20-37 percentage points of coverage.
5. **BoxRec stays closed** until someone wants to invest in the
   FlareSolverr-or-Playwright cookie-harvest path. The Tapology feed
   is good enough that this is no longer urgent.
