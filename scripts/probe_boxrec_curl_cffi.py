#!/usr/bin/env python3
"""Probe BoxRec with curl_cffi browser TLS impersonation.

Tests whether curl_cffi (same TLS-impersonation stack used by the tennis
and golf daemons against Pinnacle) can bypass the Cloudflare "Just a
moment..." challenge that blocks plain `requests` against BoxRec.

We try several impersonation profiles against three BoxRec endpoints,
saving each raw response so a human can eyeball whether it's a real page
or the Cloudflare interstitial.

Constraints:
  * 5-10s spacing between requests
  * Hard cap: 15 total requests
  * Single process

Output:
  data/raw_html/boxrec_probe/{profile}_{path-key}.html
  data/raw_html/boxrec_probe/_summary.json
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT_DIR = ROOT / "data" / "raw_html" / "boxrec_probe"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Pinnacle daemon uses chrome120 successfully; try a spread of profiles.
PROFILES = ["chrome120", "chrome119", "safari17_0", "edge99"]

TARGETS = [
    ("home", "https://boxrec.com/"),
    ("canelo", "https://boxrec.com/en/proboxer/348759"),
    ("robots", "https://boxrec.com/robots.txt"),
]

OPTIONAL_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

REQ_SPACING_S = 6.0  # within 5-10s window
HARD_CAP = 15


def is_cloudflare_challenge(body: str) -> bool:
    if not body:
        return False
    markers = (
        "Just a moment",
        "cf-browser-verification",
        "cf_chl_opt",
        "challenge-platform",
        "cdn-cgi/challenge-platform",
        "Enable JavaScript and cookies to continue",
    )
    return any(m in body for m in markers)


def looks_like_real_page(body: str) -> bool:
    if not body:
        return False
    needles = (
        "BoxRec",
        "boxer",
        "Pro Boxer",
        "/en/proboxer/",
    )
    # Real BoxRec pages are large + reference internal slugs
    return len(body) > 8000 and any(n in body for n in needles) and not is_cloudflare_challenge(body)


def probe() -> dict:
    from curl_cffi import requests as cffi_requests

    summary = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "results": [],
        "request_count": 0,
        "any_success": False,
    }

    # Strategy: for each profile, hit homepage first (small) — if that's
    # a real 200 with BoxRec content, escalate to the deep page. We also
    # alternate "with optional headers" vs without to see if either matters.
    for profile in PROFILES:
        if summary["request_count"] >= HARD_CAP:
            break

        for hdr_mode in ("with_headers", "minimal"):
            if summary["request_count"] >= HARD_CAP:
                break
            session = cffi_requests.Session(impersonate=profile)
            for path_key, url in TARGETS:
                if summary["request_count"] >= HARD_CAP:
                    break

                # Skip robots in minimal mode to save budget for deep pages
                if hdr_mode == "minimal" and path_key == "robots":
                    continue

                if summary["request_count"] > 0:
                    time.sleep(REQ_SPACING_S)

                headers = dict(OPTIONAL_HEADERS) if hdr_mode == "with_headers" else {}

                t0 = time.time()
                status = None
                err = None
                body = ""
                try:
                    resp = session.get(url, headers=headers, timeout=30, allow_redirects=True)
                    status = resp.status_code
                    body = resp.text or ""
                except Exception as e:
                    err = repr(e)

                summary["request_count"] += 1
                elapsed_ms = int((time.time() - t0) * 1000)

                fname = f"{profile}__{hdr_mode}__{path_key}.html"
                out_path = OUT_DIR / fname
                if body:
                    try:
                        out_path.write_text(body, encoding="utf-8", errors="replace")
                    except Exception as e:
                        err = (err or "") + f" | write_fail={e!r}"

                cf_challenge = is_cloudflare_challenge(body)
                real = looks_like_real_page(body)
                if real:
                    summary["any_success"] = True

                print(
                    f"[probe] {profile:10s} {hdr_mode:13s} {path_key:7s} "
                    f"status={status} bytes={len(body):>7d} "
                    f"cf_challenge={cf_challenge} real={real} "
                    f"({elapsed_ms}ms) err={err}"
                )

                summary["results"].append({
                    "profile": profile,
                    "hdr_mode": hdr_mode,
                    "path_key": path_key,
                    "url": url,
                    "status": status,
                    "bytes": len(body),
                    "cf_challenge": cf_challenge,
                    "real_page": real,
                    "elapsed_ms": elapsed_ms,
                    "error": err,
                    "saved_as": fname if body else None,
                })

            # Early exit: if this (profile, hdr_mode) already worked on home,
            # we don't need to retry the same profile in a different header mode.
            success_for_pair = any(
                r["profile"] == profile and r["hdr_mode"] == hdr_mode and r["real_page"]
                for r in summary["results"]
            )
            if success_for_pair:
                break

        # Profile-level early exit: if any header mode worked on home + canelo, stop.
        deep_hits = [
            r for r in summary["results"]
            if r["profile"] == profile and r["real_page"] and r["path_key"] == "canelo"
        ]
        if deep_hits:
            print(f"[probe] {profile} cleared deep page; stopping further profiles")
            break

    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    return summary


def main() -> int:
    summary = probe()
    sum_path = OUT_DIR / "_summary.json"
    sum_path.write_text(json.dumps(summary, indent=2))
    print(f"\n[probe] wrote summary -> {sum_path}")
    print(f"[probe] requests issued: {summary['request_count']}")
    print(f"[probe] any successful real-page fetch: {summary['any_success']}")
    return 0 if summary["any_success"] else 2


if __name__ == "__main__":
    sys.exit(main())
