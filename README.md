# boxing-odds-daemon

**Status: scaffold only.** Pipeline structure is operational (capture → parse →
score → append → resolve → summarize) but no model is trained yet, and BFO
turns out to be a poor data source for boxing (it's overwhelmingly MMA-focused).

## What works
- `scripts/paper_bets/` ledger module (cloned from regional MMA daemon —
  same schema works for boxing).
- `scripts/capture_bfo_boxing.py` — discovers BFO events using boxing-specific
  include keywords, filters out MMA URL patterns. Currently finds very few
  events because BFO's boxing coverage is thin.
- `scripts/score_live.py` — placeholder; emits empty picks file. Replace with
  real scoring logic when a model is trained.
- `scripts/refresh_features_daily.py` — placeholder; calls the regional MMA
  parser on cached HTML.
- `scripts/update.sh` orchestrator.
- `boxing-update` zsh alias points to `scripts/update.sh`.
- Registered in central app at `/Users/Ryan/mlb-odds-daemon/app_config.py`.

## What's NOT done
- **No model.** Boxing-specific features (boxer Elo / Glicko, KO rate,
  TKO rate, decision rate, height/reach, age, southpaw/orthodox, gym, last-
  fight rest, weight class, ratings from BoxRec/ESPN) need to be built.
- **No proper data source.** BFO covers boxing sporadically; better options:
  - **BoxRec** — canonical boxing record DB. Heavily rate-limited; scraping
    requires care. Has every fighter's full history.
  - **Pinnacle** — sport ID for boxing (separate from MMA). Has live moneyline,
    method, total rounds for any major fight. Use `curl_cffi` pattern from
    tennis/golf Pinnacle captures.
  - **DraftKings / Bovada** — softer books for boxing; capture pattern same
    as tennis.

## Realistic build effort
A working boxing model is **2-4 weeks of focused work** — see the design
session's scope discussion (paraphrased):
- Day 1-2: BoxRec scraping (rate-limited)
- Day 3-5: Feature engineering
- Day 6-8: Model training + walk-forward backtest
- Day 9-10: Pinnacle + soft-book boxing capture
- Day 11+: Methods/round props, app integration, paper-bet validation

## Realistic edge expectation
Similar to regional MMA: **+5-6% ML, +3-5% method props at sharp closing.**
Public-data ceiling against Pinnacle moneyline is approximately zero. Edges
live in:
- Smaller cards (regional/UK/Mexico/Japan boxing)
- Method props (KO/TKO vs decision)
- Total rounds o/u
- Cross-book line shopping

## Today (2026-04-26)
Scaffold dropped. Pipeline runs end-to-end with placeholder model. `boxing-
update` will execute clean and produce empty picks files until the model is
built. App shows "Boxing" with a "Scaffold only" disclaimer.
