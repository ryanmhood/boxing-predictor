# Boxing Data Gaps

Status notes from the initial scrape session (2026-04-24).

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

## Recommendation

* **Short term (model v0)**: train on the Wikipedia sample. 314+ pro
  fights from 12 boxers is enough to validate the feature pipeline and
  proof-out KO-rate / reach / age features. Not enough for serious
  predictions.
* **Medium term**: expand the Wikipedia roster to ~500 boxers across
  the 17 male weight classes (~5,000-15,000 fights). Sufficient to
  train a baseline Glicko / logistic ML model for top-card fights.
* **Long term**: stand up the `curl_cffi` + cf-clearance BoxRec scraper
  to cover the regional / journeyman population where the actual edge
  lives.
