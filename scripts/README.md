# Scripts

Boxing data scrapers + helpers. Most of these are polite, single-process
network jobs that cache HTML to disk so re-runs are cheap.

## BoxRec stack (Cloudflare-walled, human-assisted)

BoxRec is hard-blocked behind a Cloudflare JS challenge — `requests` and
`curl_cffi` alone both fail (see `BOXING_DATA_GAPS.md` and
`probe_boxrec_curl_cffi.py` for the post-mortem). The workaround is a
two-stage pipeline:

1. **Solve the challenge once in a real Chromium** via Playwright
   (human-in-the-loop), dump the resulting `cf_clearance` + `__cf_bm`
   cookies and the matching User-Agent.
2. **Bulk-scrape with `curl_cffi`** sharing those cookies. Cookies
   typically last ~30 minutes, which is plenty for a few hundred profiles
   at 1.5 s / request.

If a request mid-scrape comes back as the CF interstitial, the bulk
scraper aborts cleanly and tells you to re-harvest.

### Files

| File | Purpose |
|---|---|
| `scrape_boxrec_playwright.py` | Cookie harvester. `verify` mode is a no-human launch smoke-test; `harvest` mode opens a visible Chromium for you to solve the challenge. |
| `scrape_boxrec_bulk.py`       | Bulk profile + fight-record scraper. Reads cookies, walks `data/raw/boxer_overlap_targets.csv`, writes `data/raw/boxer_results_boxrec.csv`. |
| `check_boxrec_pbo_join.py`    | Coverage validator. Joins the BoxRec scrape to the PBO bout archive and writes `data/reports/pbo_boxrec_join_coverage.md`. |

### How to run end-to-end

> Run from the repo root (`/Users/Ryan/gt/boxing_predictor`), not from a
> polecat worktree — the scripts assume `data/raw/` and
> `data/processed/` are populated by upstream PBO and overlap scrapers.

**Step 0 — one-time setup (already installed in this env):**

```bash
pip install playwright curl_cffi beautifulsoup4 lxml
python3 -m playwright install chromium
```

**Step 1 — Playwright launch smoke-test (no human needed):**

```bash
python3 scripts/scrape_boxrec_playwright.py verify
```

This launches a visible Chromium, navigates to BoxRec, takes a
screenshot at `data/cache/boxrec_initial_load.png`, then exits. A
`cf_challenge=True` flag in the output is **expected** here — `verify`
is just a sanity check that the Playwright stack itself is wired up.

**Step 2 — harvest cookies (human-in-the-loop, ~10–30 s):**

```bash
python3 scripts/scrape_boxrec_playwright.py harvest
```

A Chromium window will open. Wait for the Cloudflare "Just a moment…"
page to finish, solve any CAPTCHA / "I am human" checkbox if shown, and
make sure the BoxRec front page actually renders. Then come back to the
terminal and press **ENTER**. The script will:

- dump `data/cache/boxrec_cookies.json` (cookies + UA + harvested_at + expires_at),
- validate by hitting Canelo's profile (`/en/proboxer/348759`) via
  `curl_cffi`,
- print `SUCCESS` if the validation page looks like a real BoxRec page
  (not the CF interstitial).

If validation fails, just re-run — usually the issue is pressing ENTER
before the BoxRec page actually loaded.

**Step 3 — bulk scrape (5 min for ~250 boxers @ 1.5 s spacing):**

```bash
# Smoke test first:
python3 scripts/scrape_boxrec_bulk.py --limit 5

# Full run (uses the targets CSV from prior PBO overlap work):
python3 scripts/scrape_boxrec_bulk.py

# If cookies expire mid-run, re-harvest then:
python3 scripts/scrape_boxrec_bulk.py --resume
```

This writes one row per fight to
`data/raw/boxer_results_boxrec.csv` with columns:
`fight_date, boxer_id, boxer_name, opp_id, opp_name, result, method,
round, weight_class, location`.

HTML is cached under `data/raw_html/boxrec/{boxer_id}.html` so re-parses
are free.

**Step 4 — check coverage gate:**

```bash
python3 scripts/check_boxrec_pbo_join.py
```

Prints a per-year coverage table and writes
`data/reports/pbo_boxrec_join_coverage.md`. Exit code is `0` if
coverage is ≥50% (the gate to justify continuing the boxing model) and
`1` otherwise.

### Recovering from common failures

| Symptom | Fix |
|---|---|
| `cookies.json not found` | Run `scrape_boxrec_playwright.py harvest` first. |
| `cf_challenge=True` after harvest validation | Re-harvest; the BoxRec front page hadn't actually rendered when you pressed ENTER. |
| Bulk scrape aborts with `CF block on profile` | Cookies expired (~30 min lifetime). Re-harvest, then `--resume`. |
| `0%` coverage in the join report | Either the bulk scraper hasn't run yet or normalisation is dropping rows — spot-check `boxer_results_boxrec.csv`. |

## Other scrapers in this directory

- `scrape_boxrec.py` — original Wikipedia-fallback scraper (kept as
  reference; do **not** modify).
- `probe_boxrec_curl_cffi.py` — original `curl_cffi`-only probe that
  proved BoxRec needs Playwright (kept as evidence).
- `scrape_pbo_archive.py` — sitemap-driven PBO odds archive scraper.
- `scrape_pbo_overlap.py` — produces the `boxer_overlap_targets.csv`
  consumed by the BoxRec bulk scraper.
- `capture_bfo_boxing.py`, `expand_boxer_scrape.py`,
  `refresh_features_daily.py`, `score_live.py` — model / live pipeline.
