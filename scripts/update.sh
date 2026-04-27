#!/bin/bash
# Boxing paper-bet pipeline — run anytime. Idempotent.
# Usage: boxing-update
#
# CURRENT STATE: scaffold only. Capture + parse work; scoring is a placeholder
# that emits an empty picks file because the boxing model isn't trained yet.
# Once a model lands, score_live.py will produce real picks and the existing
# append/resolve/summarize chain will fill the ledger.
set -uo pipefail
cd /Users/Ryan/gt/boxing_predictor

echo "🥊 Boxing Pipeline — $(date '+%Y-%m-%d %H:%M %Z')"
echo

echo "[1/5] resolve"
/usr/bin/python3 -m scripts.paper_bets.resolve 2>&1 | grep -E "checked|resolved|still_open" || true

echo "[2/5] capture BFO boxing upcoming"
/usr/bin/python3 scripts/capture_bfo_boxing.py 2>&1 | tail -3 || true

echo "[3/5] refresh features"
/usr/bin/python3 scripts/refresh_features_daily.py 2>&1 | tail -1 || true

echo "[4/5] score + append (PLACEHOLDER — no model yet)"
/usr/bin/python3 scripts/score_live.py 2>&1 | tail -1 || true
/usr/bin/python3 -m scripts.paper_bets.append 2>&1 | grep -E "flagged|appended" || true

echo "[5/5] summarize"
/usr/bin/python3 -m scripts.paper_bets.summarize 2>&1 | tail -1 || true

echo
echo "✅ Done. Boxing scaffold operational; awaiting model."
