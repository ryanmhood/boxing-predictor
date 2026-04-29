# Boxing G4 — Phase 2 Backtest Results

**Date**: 2026-04-28
**Issue**: bx-zz7
**Author**: polecat nux
**Predecessor**: G3 (`BOXING_BACKTEST_PHASE1_RESULTS.md`, bx-g5o)

## TL;DR — Verdict

**FAIL** on the pre-registered PASS criteria across all three strategies.
**Do not activate paper betting.**

The model improved measurably vs G3 — the global-strategy Brier moved from
−0.77pp to +0.69pp on the same holdout (a +1.46pp swing that vindicates the
feature expansion + per-tier Platt) — but no strategy clears every bar at
once on the 2024-25 holdout. The bead's explicit rule applies: "Do NOT
activate paper betting if all three strategies fail."

| Pre-registered criterion (must clear ALL on ≥1 strategy) | Global | Mid-tier | Heavy-fav |
|---|---:|---:|---:|
| ROI ≥ 4% on 2024-25 holdout | −0.23% ❌ | +20.80% ✅ | +4.61% ✅ |
| ROI positive after 5% vig haircut | −2.12% ❌ | +17.81% ✅ | +4.30% ✅ |
| Brier improvement ≥ 0.5pp on bets | +0.69pp ✅ | +0.38pp ❌ | +0.27pp ❌ |
| Sample size ≥ 50 bets in holdout | 158 ✅ | 36 ❌ | 133 ✅ |
| **PASS** | **❌** | **❌** | **❌** |

Mid-tier comes closest: it crushes ROI (+20.80% raw, +17.81% post-vig), but
falls short on both the Brier gate and the sample-size gate. Two more years
of fresh PBO bouts at the current cadence would lift its holdout sample
above 50 — that is the natural next pre-registered checkpoint.

## What changed vs G3 (bead Parts A-C)

### Part A — Refresh the union

A live `scrape_tapology.py --resume --delay 4.0` rate-limited at fighter
166/7001 (Tapology 503 on the search endpoint). The script now falls
through to a **cache-only mode** instead of aborting (see `scripts/scrape_tapology.py`
loop change), and a one-shot harvester (`scripts/harvest_tapology_cache.py`)
parses every cached profile via the search-cache → profile-cache chain so
the resulting CSV uses the same boxer-name strings as the live scraper.

| Metric | G3 baseline | G4 |
|---|---:|---:|
| Tapology rows           | 7,801   | 8,907   |
| Union rows              | 104,860 | 105,653 |
| PBO coverage (overall)  | 37.0%   | 40.1%   |
| PBO scoring rows (joined both sides) | 922 | 844 |
| 2024-25 holdout bouts   | 298     | 281     |

The PBO scoring set shrank slightly because the new harvest path uses the
same name-resolution logic as the live scraper (no slug-derived names),
trading a small coverage drop for cleaner dedupe.

### Part B — Feature expansion (`scripts/build_boxer_features.py`)

Added eleven new per-fighter features alongside the G3 set:

* **Stylistic** (from `data/processed/tapology_attrs.csv`, scraped from
  cached Tapology profile HTML — 389/417 fighters with height, 262/417
  with reach):
  - `height_cm`, `reach_cm`, `reach_to_height_ratio`
* **Method-distribution** (rolling 10 decisive fights):
  - `ko_win_rate_10`, `tko_loss_rate_10`, `dec_rate_10`
* **Round-distribution** (rolling 10):
  - `avg_scheduled_rounds_10`
* **Inactivity penalty**:
  - `inactive_180d_flag`, `inactive_365d_flag`
* **Schedule strength** (over `opp_glicko_last5`):
  - `opp_glicko_min_last5`, `opp_glicko_std_last5`

**Skipped** features and why:
* **Stance** (orthodox/southpaw/switch): zero of 80 sampled cached
  Tapology profiles surface a stance field. Plainte's BoxRec dump also
  lacks per-fighter stance. Keeping the feature would have meant
  shipping NaN for every row — dropped.
* **`p_finish_round_1_3`**: requires the *finish round* per bout, not
  the *scheduled* round. Tapology's profile-page row only carries the
  scheduled rounds (`12 Rounds`, `10 Rounds`, …); the finish round
  lives on the bout-detail page, which we don't scrape. Dropped to
  avoid building a feature on data we don't have.
* **`height_minus_avg_for_weight_class`**: requires per-fight weight
  class. The current union has empty `weight_class` (Plainte didn't
  carry it; Tapology row HTML doesn't either). Replaced with
  `reach_to_height_ratio` which is weight-class-agnostic.

The Glicko-2 time-decay clause from the bead (item 5) was already in the
script (`inflate_phi(...)` at the top of every `get_glicko` call). Verified
in code review; no change required.

### Part C — Per-tier calibration (`scripts/train_boxing_model.py`)

After fitting the global Platt scaler on the 20% calibration holdout, the
script now also fits **three tier-specific Platts** based on the raw
LightGBM prob:

* `heavy`: raw_p ≥ 0.85 OR raw_p ≤ 0.15
* `mid`:   0.35 ≤ raw_p ≤ 0.65
* `light`: 0.15 < raw_p < 0.35 OR 0.65 < raw_p < 0.85

Each is saved as `data/models/boxing/year={Y}/platt_{heavy,mid,light}.json`.
Tier-specific scaler falls back to the global Platt when its training-set
size in the calibration window drops below 50 (in practice this never
triggers — heavy gets 4-8k rows, mid 2-3k, light 3-5k per year). At
score-time, `backtest_boxing.py` routes each prediction to the right
scaler based on raw model_p.

Within-distribution Brier (union eval, not PBO bouts) is essentially flat
between the global and per-tier Platts — the eval set is large and well-
balanced enough that one global scaler and three per-tier scalers
calibrate to the same curve. The per-tier value shows up at PBO eval
time, where the bouts are skewed heavily to one tier.

## Walk-forward training metrics (within-distribution union eval)

```
YEAR  train_n  eval_n  base   brier_base  brier_cal  brier_tier  Δbase     auc    n_heavy/mid/light
2018  51,308   13,775  0.434  0.2433      0.1303     0.1303      +0.1130   0.892   4698/2250/3314
2019  65,083   15,085  0.430  0.2429      0.1287     0.1288      +0.1141   0.894   6118/2813/4086
2020  80,168     445   0.427  0.2660      0.0715     0.0718      +0.1945   0.949   7661/3434/4939
2021  80,613     723   0.428  0.2633      0.0893     0.0896      +0.1740   0.930   7726/3382/5015
2022  81,336     759   0.429  0.2550      0.0817     0.0818      +0.1733   0.942   7684/3416/5168
2023  82,095     747   0.430  0.2492      0.1026     0.1028      +0.1466   0.918   7913/3426/5080
2024  82,842     686   0.430  0.2492      0.1310     0.1313      +0.1182   0.895   7943/3490/5136
2025  83,528     566   0.430  0.2537      0.1328     0.1332      +0.1208   0.884   8038/3512/5156
```

AUC is on par with G3 (0.884–0.949 vs 0.880–0.957) — the new features
don't materially shift in-distribution discrimination, but as the
backtest shows, they shift PBO-bout calibration.

## PBO backtest (Part D — three strategies)

Headline 2024-25 holdout numbers and naive baseline reference:

```
Strategy   bouts  bets   ROI%      ROIh5%   BrierΔpp   PASS-bar
GLOBAL      281   158   -0.23%    -2.12%   +0.69      ROI<4% / vig<0% / brier<0.5
MID         281    36   +20.80%  +17.81%   +0.38      bets<50 / brier<0.5
HEAVY       281   133    +4.61%   +4.30%   +0.27      brier<0.5

Naive always-favorite (this exact 844-bout subset): hit=89.1% ROI=+3.41%
```

### Per-year — GLOBAL strategy

```
year  bouts  bets   ROI%       ±CI95   ROIh5%    BrierΔpp
2018    36    22   +37.14%   ±76.70%   +33.46%   +0.97
2019   197   112   -13.14%   ±24.24%   -15.12%   -3.93
2020    20    14   +94.29%  ±128.69%   +88.50%   +8.90
2021    91    45   +33.70%   ±28.37%   +31.13%   +9.68
2022    83    33   +25.73%   ±42.46%   +23.08%   +3.72
2023   136    59   +37.11%   ±31.26%   +34.33%   +4.60
2024   162    81   -10.43%   ±18.51%   -11.88%   -1.79
2025   119    77   +10.49%   ±29.64%    +8.15%   +3.31
ALL    844   443   +11.70%   ±12.03%    +9.33%   +1.46
2024-25 holdout (281 bouts, 158 bets): ROI=-0.23% ±17.30pp BrierΔ=+0.69pp
```

The full-window aggregate is +11.70% ROI / +9.33% post-vig — both *much
better than G3's* +3.37% / +1.04% on the same gate. But the holdout sub-
window (which is what pre-registration cares about) is essentially flat.
The model's good years (2020-2023) are doing the lifting; 2024 punctures
the aggregate. This is the same pattern G3 found and is what the formal
pre-reg is supposed to control for — it does, and the verdict is FAIL.

### Per-year — MID-TIER strategy (formally pre-registered)

```
year  bouts  bets   ROI%       ±CI95    ROIh5%     BrierΔpp
2018    36     2  +143.50%   ±12.74%  +136.32%   +26.66
2019   197    19    -0.25%   ±40.83%    -2.34%    -1.47
2020    20     0      --                   --        --
2021    91    11   +88.22%   ±19.78%   +83.81%   +20.90
2022    83    11   +69.39%   ±55.19%   +65.01%   +10.09
2023   136    10   +87.58%   ±43.53%   +82.70%   +17.84
2024   162    19   +22.18%   ±50.74%   +18.96%    -1.86
2025   119    17   +19.26%   ±44.36%   +16.53%    +2.88
ALL    844    89   +40.91%   ±19.60%   +37.46%    +6.27
2024-25 holdout (281 bouts, 36 bets): ROI=+20.80% ±33.52pp BrierΔ=+0.38pp
```

The mid-tier strategy is the most promising of the three. ROI on the
holdout is +20.80% raw, +17.81% post-vig — both well above 4%. The
strategy fails on **two** pre-reg gates:

1. **Holdout sample = 36** (need ≥ 50). At the 2024-25 cadence (≈18
   mid-tier bets/year), an additional ~1.5 years of fresh PBO bouts
   would lift the count above 50.
2. **Brier improvement = +0.38pp** (need ≥ 0.5pp). Close, but the
   market-implied prob on close fights is sharp enough that the model
   only edges past it.

The G3 mid-tier sub-finding (post-hoc) was +31.14% on 42 bets. G4 (now
formally pre-registered) is +20.80% on 36 bets — the pattern persists,
and the per-tier Platt narrows the variance, but the bar isn't cleared.

### Per-year — HEAVY-FAV strategy (sanity check vs naive)

```
year  bouts  bets   ROI%       ±CI95    ROIh5%   BrierΔpp
2018    36    14    -3.64%   ±14.61%    -3.82%   -0.70
2019   197    52    -1.67%   ± 6.72%    -1.88%   +0.06
2020    20     6    +2.00%   ± 0.72%    +1.90%   +0.25
2021    91    45    +5.49%   ± 0.93%    +5.21%   +0.40
2022    83    43    +2.14%   ± 4.82%    +1.91%   +0.08
2023   136    72    +4.12%   ± 2.96%    +3.85%   +0.41
2024   162    85    +5.91%   ± 0.70%    +5.61%   +0.36
2025   119    48    +2.31%   ± 6.23%    +1.98%   +0.13
ALL    844   365    +3.08%   ± 1.62%    +2.81%   +0.23
2024-25 holdout (281 bouts, 133 bets): ROI=+4.61% ±2.30pp BrierΔ=+0.27pp
```

The heavy-fav strategy clears ROI and the haircut gate handily, with the
2024-25 holdout returning +4.61% on 133 bets. But the Brier improvement
is just +0.27pp — the market is already very good at pricing heavy
favorites, so there is barely any room for the model to add information.
This is the predicted result from G3's "Brier 4 (heavy-favorite
calibration drift)" hypothesis, now confirmed at the strategy level.

The naive always-favorite line on this same 844-bout subset returns
+3.41% — so the heavy-fav-with-model-agreement strategy does extract a
modest +1.2pp lift over the naive baseline. That is real, but not enough
to justify activating paper betting on its own when Brier fails the gate.

## Why no strategy passes — three hypotheses (vs G3's four)

The G3 root-cause analysis listed four hypotheses; G4 substantially
addressed three of them, but two new patterns emerge:

1. **Survivorship bias in scraped fighters** (still present). The
   Tapology cache only resolves 417 of 7,001 PBO fighter targets —
   essentially the top-ranked ~6% — so half of every PBO bout has at
   least one fighter that would be feature-NaN. The model still trains
   primarily on champions / contenders; bottom-of-card mid-tier fights
   have less signal.
2. **Heavy-favorite calibration drift** (improved but not closed). The
   per-tier Platt narrowed the gap (heavy 2024-25 BrierΔ went from G3's
   −0.6pp to +0.27pp, a real swing) but didn't beat the bar. The
   market-implied prob on these is hard to beat.
3. **Plainte freeze + post-2020 thin training data** (unchanged). 2020-
   2025 training windows still have ~600/year decisive bouts where both
   sides have feature rows. This is the largest leverage point for a
   future iteration.

The G3 "adversarial features" hypothesis (camp/promoter/injury news) is
also unchanged — those signals still aren't in the pipeline.

## What this PR delivers

* ✅ Part A: union refresh + cache-only fallback in `scrape_tapology.py`
  + new `harvest_tapology_cache.py` + new
  `extract_tapology_attrs.py`.
* ✅ Part B: 11 new features in `build_boxer_features.py`,
  `build_match_features.py`, `train_boxing_model.py`. Per-row schema
  expanded; backwards-compat-clean (older models with the old
  `feature_list.json` would fail to score the new parquet, which is
  why we re-trained the entire walk-forward).
* ✅ Part C: per-tier Platt calibration in `train_boxing_model.py`,
  consumed by `backtest_boxing.py` via `apply_platt_per_tier`.
* ✅ Part D: walk-forward retrain on 84,224-row training window;
  three-strategy backtest with ROI / vig-haircut / Brier per
  strategy per year.
* ✅ Part E: this report.
* ❌ Part F: NOT activated. Verdict is FAIL on the 2024-25 holdout for
  every strategy, so `score_live.py`, `paper_bets/append.py`, and the
  launchd plist are unchanged.

## What a successful Phase 3 would look like (deferred — NOT activated)

The mid-tier strategy is the most credible path. A pre-registered
Phase 3 should:

1. Wait for ≈1.5 years of fresh PBO bouts at current cadence to get the
   mid-tier holdout sample above 50.
2. Re-run the same three strategies on the new holdout (no other
   changes) and check whether the +20.80% ROI / +0.38pp BrierΔ pattern
   holds. If both hit the bar, then activate.
3. The G3 Phase 2D bankroll plan still applies if that test passes:
   $1k bankroll, $5 max per pick (0.5%), 30% drawdown halt at $700,
   100-bet rolling −5% ROI halt with 95% CI excluding 0, daily 06:00
   ET cadence via launchd. None of that is wired up by this PR.

The work is honest about what the data does and doesn't show. The G4
expansion **measurably** shifted Brier in the right direction (+1.46pp
swing on the global strategy) but didn't move it far enough on any one
strategy to clear the gate.
