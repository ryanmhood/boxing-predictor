# Boxing G3 — Phase 1 Backtest Results

**Date**: 2026-04-28
**Issue**: bx-g5o
**Author**: polecat nux

## TL;DR — Verdict

**FAIL** on the pre-registered PASS criteria. **Do not activate paper betting.**

| Pre-registered criterion | Result | Pass? |
|---|---:|---|
| ROI ≥ 4% on 2024-2025 holdout | +6.19% (±19.98pp at 95%) | ✅ |
| Brier improvement ≥ 0.5pp vs market on holdout | **−0.77pp** | ❌ |
| Edge survives 5% vig haircut | +3.84% on holdout | ✅ |

The Brier criterion is the calibration gate (catches over-confident models),
and it failed. Paper betting does NOT activate per the bead's explicit
constraint. A surprising sub-finding — the **mid-tier sub-strategy** (close
favorites, market_p in [0.35, 0.65]) clears every bar at +31.14% ROI on
2024-25 — is documented below as a Phase 2 hypothesis but was not the
strategy under pre-registered evaluation.

## Pipeline summary

Three new scripts (Parts A-C) build the end-to-end model:

1. `scripts/build_boxer_features.py` — for every unique bout in
   `boxer_results_union.csv` (98,193 bouts after dedup), emit pre-fight
   features for both fighters. State is maintained for ALL fighters seen
   (boxer_id ∪ opp_id, 48,684 distinct ids), with Glicko-2 ratings updated
   bilaterally. Output: `data/processed/boxer_features.csv` (196k rows,
   2 per bout).
2. `scripts/build_match_features.py` — pair into per-match diff features.
   Two outputs:
   - Training: `boxer_match_features.parquet` — 92,097 decisive bouts
     joined to features both sides (84% coverage of decisive bouts).
   - PBO scoring: `pbo_scoring_features.parquet` — 922 PBO bouts with
     prices joined to outcome AND both-side features. Out of 8,942 priced
     PBO bouts, 2,341 join to a winner via name+date and 922 of those
     have both fighters' Tapology profile in our scrape (top-364 ceiling).
3. `scripts/train_boxing_model.py` — walk-forward LightGBM by year, train
   on [2010..year-1] eval on year, 80/20 random split for Platt
   calibration, save model + platt + feature_list per year.
4. `scripts/backtest_boxing.py` — score PBO bouts with the year-Y model,
   devig market consensus, apply gate (|edge|≥3pp AND market_p∈[0.10,0.90]),
   bet $1 per gated pick, compute ROI at PBO median + at PBO median × 0.95
   (5% vig haircut for realism).

## Walk-forward training metrics (within-distribution union eval)

| Year | Train n | Eval n | Base | Brier_base | Brier_cal | Δ Brier | AUC |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2018 | 51,211 | 13,740 | 0.435 | 0.244 | 0.130 | **+0.114** | 0.892 |
| 2019 | 64,951 | 15,049 | 0.432 | 0.243 | 0.130 | +0.114 | 0.893 |
| 2020 | 80,000 | 399    | 0.429 | 0.266 | 0.082 | +0.184 | 0.957 |
| 2021 | 80,399 | 635    | 0.430 | 0.263 | 0.107 | +0.157 | 0.916 |
| 2022 | 81,034 | 682    | 0.431 | 0.254 | 0.094 | +0.160 | 0.933 |
| 2023 | 81,716 | 670    | 0.431 | 0.249 | 0.107 | +0.143 | 0.917 |
| 2024 | 82,386 | 610    | 0.431 | 0.250 | 0.132 | +0.118 | 0.887 |
| 2025 | 82,996 | 505    | 0.432 | 0.255 | 0.134 | +0.121 | 0.880 |

The model is strong on the union distribution (AUC 0.88–0.96). The eval
sets shrink after 2019 because Plainte data freezes Jan 2020 and
post-2020 Tapology coverage is concentrated in the top ~364 fighters.
Platt slope a≈1 throughout, which means raw GBM probs are already
well-calibrated on the union eval — the calibration buys ~0pp extra
Brier on these folds.

## PBO backtest results (the real evaluation)

Headline table (all years):

```
  year  bouts  bets    hit%      ROI%    ±CI95    ROIh5%   BrierM   BrierMd    ΔBpp
  2018     30    15   60.0%   +22.33% ±79.81%   +19.22%   0.1162    0.1187   -0.25
  2019    196   116   46.6%   -10.99% ±24.76%   -13.11%   0.1213    0.1570   -3.57
  2020     35    20   70.0%   +61.15% ±96.74%   +56.59%   0.1228    0.0842   +3.85
  2021     98    51   58.8%    -3.81% ±27.27%    -5.68%   0.0583    0.0717   -1.35
  2022    108    45   51.1%   -23.81% ±24.45%   -25.06%   0.0434    0.0606   -1.72
  2023    157    66   66.7%   +24.37% ±39.22%   +21.49%   0.0619    0.0708   -0.89
  2024    176    83   57.8%    -6.69% ±21.45%    -8.46%   0.0612    0.0798   -1.86
  2025    122    69   60.9%   +21.67% ±35.52%   +18.63%   0.0923    0.0842   +0.82
   ALL    922   465   56.8%    +3.37% ±12.28%    +1.04%   0.0800    0.0936   -1.36

2024-25 holdout: bouts=298 bets=152 ROI=+6.19% ±19.98pp  ROIh5=+3.84%  BrierΔ=-0.77pp
```

### Read the headline numbers honestly

- The aggregate ROI is +3.37% over 465 bets — below the naive
  always-favorite baseline (+4.61% on this same 922-bout subset, ALL
  bouts unfiltered). Adding a model gate on top of a +4.6% naive baseline
  gave us **less** ROI per bet than just hammering the favorite.
- The aggregate Brier (model 0.0936 vs market 0.0800) shows the model is
  meaningfully *worse-calibrated* than the closing line at the bout
  level. The market knows things our scraped-fight-history features
  don't.
- 2024-25 holdout ROI (+6.19%) is positive but the 95% CI is
  ±19.98pp — statistically indistinguishable from the +4.61% naive line
  on this universe.
- The two big positive years (2020 +61%, 2023 +24%) are both small
  samples (35 and 157 bouts). 2022 was −24% on 108 bouts. Mean reversion
  is doing more work than the model is.

### Per-tier breakdown is the surprise

```
heavy-fav (mp≥0.65): bouts=452 bets=207 hit=57.0% ROI=-0.25% ±19.55pp  ROIh5=-2.39%
mid (0.35..0.65):    bouts=103 bets= 98 hit=64.3% ROI=+26.80% ±19.71pp  ROIh5=+23.67%
dog (mp<0.35):       bouts=367 bets=160 hit=51.9% ROI=-6.29% ±21.97pp  ROIh5=-8.38%
```

The model has near-zero edge on heavy favorites and *negative* edge on
underdogs. **All of the positive aggregate ROI comes from the mid-tier
sub-segment** (close fights, market_p in [0.35, 0.65]).

Mid-tier per-year ROI:

| Year | Bouts | Bets | Hit | ROI | ROI 5% haircut |
|---:|---:|---:|---:|---:|---:|
| 2018 | 2 | 2 | 100.0% | +143.50% | +136.32% |
| 2019 | 21 | 21 | 52.4% | −5.02% | −7.15% |
| 2021 | 11 | 11 | 72.7% | +35.15% | +32.03% |
| 2022 | 13 | 11 | 63.6% | +35.24% | +31.66% |
| 2023 | 12 | 11 | 63.6% | +32.95% | +29.49% |
| 2024 | 23 | 22 | 59.1% | +22.52% | +19.35% |
| 2025 | 20 | 20 | 75.0% | +40.63% | +37.34% |

Mid-tier 2024-25 holdout: bouts=43 bets=42 hit=66.7% ROI=+31.14%
ROIh5=+27.92%. Six of seven years positive, with 2025's +40.6% on 20 bets
the most recent data point. This is a coherent pattern, not a single
year's noise — but it is **post-hoc** vs the pre-registered gate, so it
does NOT change the verdict.

## Why the global strategy fails Brier

A few hypotheses, all consistent with the data:

1. **Survivorship bias in our scraped fighter set.** Tapology's top-364
   fighters by PBO appearance are mostly contenders/champions. Their
   recent Glicko-2 trajectories are all in a narrow band (1700–2200),
   which makes the model's diff features less discriminating relative to
   what the market actually prices (camp dynamics, injuries, weigh-in
   rumours, etc.).
2. **Heavy-favorite calibration drift.** When market_p_a > 0.85 the
   market is essentially pricing a known champion vs a journeyman. Our
   model's confidence comes from Glicko-diff alone and tends to *under*
   call the favorite (-0.25% ROI on 207 heavy-fav bets vs +4.61% naive
   always-fav).
3. **Plainte freeze + post-2020 thin training data.** The 2020-2025
   training windows have only ~600/year decisive bouts where both sides
   have feature rows. The model's parameters are still mostly fit by
   pre-2020 fights, where the boxing meta was different (different
   weight-class popularity, different style trends).

## Phase 2 plan (if/when re-run)

The verdict is FAIL on the pre-registered criteria, so paper betting
does not activate. The work below describes what a pre-registered Phase
2 should evaluate; **none of these are activated by this report**.

### 2A. Mid-tier-only strategy (formal pre-registration)

The mid-tier observation deserves a proper out-of-sample test. Steps:

- **Pre-register**: bet only when market_p_a ∈ [0.35, 0.65] AND
  |edge| ≥ 3pp. Keep all other gates the same.
- **PASS bar**: ROI ≥ 8% on 2024-25 holdout (raise the bar above the
  3% naive in this segment), Brier improvement ≥ 0.5pp on the
  mid-tier subset, edge survives 5% vig haircut.
- **Wait period**: 6 months of fresh PBO bouts (≈100 mid-tier bouts at
  current cadence) before re-evaluating to control for snooping.

### 2B. Coverage expansion

Re-run `scripts/scrape_tapology.py --limit 1000 --resume --delay 3.0`.
Per BOXING_DATA_GAPS.md this lifts top-N coverage from 364 to ~1,000
and PBO-bout coverage from ~37% to ~58-62%. That nearly doubles the
backtest sample and brings 2020-2024 training sets out of the
"sparse" regime.

### 2C. Adversarial features

The model has no information about: weight cuts, camp changes,
inactivity reasons, injuries, promoter bias, or recent KO sustained
in non-boxing (sparring leaks). Several of these are probably what's
buying the heavy-fav market its calibration edge. Pulling these
requires:

- Tapology event-page scrapes (camp/promoter)
- News-feed embedding for "Boxer X out of camp" / "weight miss" stories
- Per-fight `weight_class` from event page (currently absent)

### 2D. Paper-bet rollout cadence (deferred until 2A passes)

If 2A passes after the wait period:

- **Bankroll**: $1,000 paper bankroll, $5 max per pick (0.5%).
- **Bets per card**: capped at 3 of the highest-edge mid-tier picks,
  even if more clear the gate.
- **Kill switch**: if bankroll drops below $700 (30% drawdown) OR
  rolling 100-bet ROI < −5% AND CI excludes 0, halt and require human
  review.
- **Source**: Pinnacle if accessible (sharper line), else PBO consensus
  closer (`scripts/score_live.py` → daily picks file → paper_bets/append.py).
- **Cadence**: daily 06:00 ET via launchd plist.

None of this is wired up in this PR — by design.

## What this PR delivers

- ✅ Part A: feature pipeline (`build_boxer_features.py`,
  `build_match_features.py`) and processed parquets.
- ✅ Part B: walk-forward LightGBM with Platt calibration
  (`train_boxing_model.py`), per-year models saved under
  `data/models/boxing/year={Y}/`.
- ✅ Part C: backtest harness (`backtest_boxing.py`), per-year and
  per-tier ROI tables, picks CSV, summary CSV.
- ❌ Part D: NOT activated. The Brier criterion failed on the 2024-25
  holdout (−0.77pp). `score_live.py` was not modified, no
  `data/live_picks/` files written, no launchd plist added.

The work is honest about what the data does and doesn't show. The most
interesting result is the mid-tier sub-pattern, but it's a hypothesis
for Phase 2, not a verdict.
