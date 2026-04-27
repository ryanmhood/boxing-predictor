#!/usr/bin/env python3
"""
BoxRec Cloudflare cookie harvester via Playwright (human-in-the-loop).

WHY THIS EXISTS:
  curl_cffi alone cannot pass BoxRec's Cloudflare JS challenge -- every
  TLS-impersonation profile we tested came back with the "Just a moment..."
  interstitial (see scripts/probe_boxrec_curl_cffi.py and the post-mortem
  in BOXING_DATA_GAPS.md). The standard workaround is to solve the
  challenge once in a real Chromium, dump `cf_clearance` + `cf_bm` cookies,
  and hand them to curl_cffi for fast bulk fetching while the cookies are
  fresh (typical lifetime ~30 min per challenge solve).

WHAT THIS SCRIPT DOES:
  * `harvest` (default): launches a *visible* Chromium window, navigates
    to https://boxrec.com, blocks on input() while the user solves the
    challenge interactively, then dumps cookies + User-Agent to
    data/cache/boxrec_cookies.json. Validates by fetching Canelo Alvarez's
    profile via curl_cffi -- if the response looks like a real boxer page
    rather than the CF interstitial, we declare the cookies valid.
  * `verify`: launches Chromium, navigates to BoxRec, snapshots the first
    page to data/cache/boxrec_initial_load.png, then exits. No human
    interaction required. This is the smoke-test you run from CI / the
    polecat worktree to confirm the Playwright stack itself is wired up.

USAGE:
    # Smoke-test (no human in the loop):
    python3 scripts/scrape_boxrec_playwright.py verify

    # Real harvest (human solves the challenge):
    python3 scripts/scrape_boxrec_playwright.py harvest

OUTPUTS:
    data/cache/boxrec_cookies.json     -- cookies + UA + harvested_at + expires_at
    data/cache/boxrec_initial_load.png -- screenshot from `verify` mode
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = ROOT / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

COOKIES_PATH = CACHE_DIR / "boxrec_cookies.json"
SCREENSHOT_PATH = CACHE_DIR / "boxrec_initial_load.png"

BOXREC_HOME = "https://boxrec.com/"
BOXREC_VALIDATE_URL = "https://boxrec.com/en/proboxer/348759"  # Canelo
CHALLENGE_MARKERS = (
    "Just a moment",
    "cf-browser-verification",
    "cf_chl_opt",
    "challenge-platform",
    "Enable JavaScript and cookies to continue",
)


def _is_challenge(body: str) -> bool:
    return bool(body) and any(m in body for m in CHALLENGE_MARKERS)


def _looks_real(body: str) -> bool:
    if not body or len(body) < 8000:
        return False
    needles = ("BoxRec", "proboxer", "Pro Boxer", "boxer")
    return any(n in body for n in needles) and not _is_challenge(body)


# --------------------------------------------------------------------------
# verify-mode: launch -> snapshot -> exit (no human required)
# --------------------------------------------------------------------------
def verify_launch(headless: bool = False, wait_s: float = 6.0) -> int:
    """Launch Playwright, hit BoxRec home, screenshot, exit. No input() block.

    `headless=False` is the realistic configuration (matches harvest mode)
    but we accept --headless for environments without a display.
    """
    from playwright.sync_api import sync_playwright

    print(f"[verify] launching Chromium headless={headless}")
    t0 = time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        try:
            page.goto(BOXREC_HOME, wait_until="domcontentloaded", timeout=30_000)
        except Exception as e:
            print(f"[verify] navigation error (often benign during CF check): {e!r}")
        # Give Cloudflare a few seconds to draw the challenge UI so the
        # screenshot is informative either way.
        page.wait_for_timeout(int(wait_s * 1000))
        try:
            page.screenshot(path=str(SCREENSHOT_PATH), full_page=False)
            print(f"[verify] screenshot -> {SCREENSHOT_PATH}")
        except Exception as e:
            print(f"[verify] screenshot failed: {e!r}")
        ua = page.evaluate("navigator.userAgent")
        body = page.content()
        looks_blocked = _is_challenge(body)
        looks_through = _looks_real(body)
        cookie_count = len(context.cookies())
        browser.close()
    elapsed = time.time() - t0
    print(f"[verify] elapsed={elapsed:.1f}s ua={ua!r}")
    print(
        f"[verify] page bytes={len(body)} cookies_in_context={cookie_count} "
        f"cf_challenge={looks_blocked} looks_real_page={looks_through}"
    )
    print(
        "[verify] (challenge=True is expected here -- this mode is just a "
        "Playwright launch smoke-test, not a full bypass.)"
    )
    return 0


# --------------------------------------------------------------------------
# harvest-mode: launch -> human solves challenge -> dump cookies -> validate
# --------------------------------------------------------------------------
def harvest_cookies(no_validate: bool = False) -> int:
    from playwright.sync_api import sync_playwright

    print("[harvest] launching VISIBLE Chromium (headless=False)")
    print("[harvest] a Chromium window will open. In that window:")
    print("  1) wait for BoxRec.com to finish the Cloudflare challenge")
    print("  2) if a CAPTCHA / 'I am human' checkbox appears, solve it")
    print("  3) you should land on the BoxRec front page")
    print("  4) come back here and press ENTER")
    print()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()
        try:
            page.goto(BOXREC_HOME, wait_until="domcontentloaded", timeout=60_000)
        except Exception as e:
            print(f"[harvest] navigation note: {e!r} (often benign during CF check)")

        try:
            input("[harvest] press ENTER when the BoxRec page is fully loaded > ")
        except (EOFError, KeyboardInterrupt):
            print("[harvest] aborted by user before cookie capture")
            browser.close()
            return 2

        cookies = context.cookies()
        ua = page.evaluate("navigator.userAgent")
        # Snapshot the page so the user has visual confirmation we captured
        # the right state.
        try:
            page.screenshot(path=str(SCREENSHOT_PATH), full_page=False)
        except Exception:
            pass
        browser.close()

    cf_clearance = next(
        (c for c in cookies if c.get("name") == "cf_clearance"), None
    )
    cf_bm = next(
        (c for c in cookies if c.get("name") == "__cf_bm" or c.get("name") == "cf_bm"),
        None,
    )

    if not cf_clearance:
        print(
            "[harvest] WARNING: no cf_clearance cookie found -- challenge "
            "may not have been solved. Other cookies still saved for "
            "inspection."
        )
    else:
        print(
            f"[harvest] cf_clearance OK (len={len(cf_clearance.get('value', ''))} "
            f"expires={cf_clearance.get('expires')})"
        )
    if cf_bm:
        print(
            f"[harvest] cf_bm OK (len={len(cf_bm.get('value', ''))} "
            f"expires={cf_bm.get('expires')})"
        )

    payload = {
        "harvested_at": datetime.now(timezone.utc).isoformat(),
        "user_agent": ua,
        "cookies": cookies,
        "cf_clearance_present": bool(cf_clearance),
        "cf_bm_present": bool(cf_bm),
    }
    # Best-effort expires_at = min of cf_clearance / cf_bm expiry if available
    expirations = [
        c.get("expires") for c in (cf_clearance, cf_bm)
        if c and isinstance(c.get("expires"), (int, float)) and c["expires"] > 0
    ]
    if expirations:
        try:
            payload["expires_at"] = datetime.fromtimestamp(
                min(expirations), tz=timezone.utc
            ).isoformat()
        except (OverflowError, OSError, ValueError):
            payload["expires_at"] = None
    else:
        payload["expires_at"] = None

    COOKIES_PATH.write_text(json.dumps(payload, indent=2))
    print(f"[harvest] wrote {len(cookies)} cookies -> {COOKIES_PATH}")
    if payload.get("expires_at"):
        print(f"[harvest] earliest CF cookie expiry: {payload['expires_at']}")
    print(f"[harvest] ua: {ua}")

    if no_validate:
        print("[harvest] --no-validate set; skipping curl_cffi validation")
        return 0 if cf_clearance else 3

    # ----------------------------------------------------------------------
    # Validate the harvested cookies against the Canelo profile via curl_cffi
    # ----------------------------------------------------------------------
    print(
        f"[harvest] validating cookies via curl_cffi against "
        f"{BOXREC_VALIDATE_URL}"
    )
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError as e:
        print(f"[harvest] curl_cffi not available, skipping validation: {e!r}")
        return 0 if cf_clearance else 3

    cookie_jar = {
        c["name"]: c["value"] for c in cookies if c.get("name") and c.get("value")
    }
    headers = {
        "User-Agent": ua,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
            "image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    session = cffi_requests.Session(impersonate="chrome120")
    try:
        resp = session.get(
            BOXREC_VALIDATE_URL,
            headers=headers,
            cookies=cookie_jar,
            timeout=30,
            allow_redirects=True,
        )
    except Exception as e:
        print(f"[harvest] validation request failed: {e!r}")
        return 4

    body = resp.text or ""
    blocked = _is_challenge(body)
    real = _looks_real(body)
    print(
        f"[harvest] validation: status={resp.status_code} bytes={len(body)} "
        f"cf_challenge={blocked} real_page={real}"
    )
    if real:
        print("[harvest] SUCCESS -- cookies are usable for bulk scraping.")
        print("[harvest] now run: python3 scripts/scrape_boxrec_bulk.py")
        return 0
    print(
        "[harvest] FAIL -- response still looks like a Cloudflare challenge. "
        "Re-run harvest mode and ensure the BoxRec page actually loads "
        "before pressing ENTER."
    )
    return 5


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    ap.add_argument(
        "mode",
        nargs="?",
        choices=("harvest", "verify"),
        default="harvest",
        help="harvest = solve challenge + dump cookies (default); "
             "verify = launch Playwright + screenshot + exit",
    )
    ap.add_argument(
        "--headless",
        action="store_true",
        help="(verify mode only) run Chromium headless. "
             "Harvest mode always launches a visible window.",
    )
    ap.add_argument(
        "--no-validate",
        action="store_true",
        help="(harvest mode) skip the post-harvest curl_cffi validation hit",
    )
    args = ap.parse_args(argv)

    if args.mode == "verify":
        return verify_launch(headless=args.headless)
    return harvest_cookies(no_validate=args.no_validate)


if __name__ == "__main__":
    sys.exit(main())
