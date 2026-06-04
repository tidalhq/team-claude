"""
LinkedIn DMA → Git cache bridge.

Pulls Tidalwave's LinkedIn organization posts from the Pages Data Portability
(DMA) API, joins with social metadata + repost counts, and merges into
data/tidalwave_posts_cache.json at the repo root.

The DMA endpoints we use:
  1. /rest/dmaFeedContentsExternal?q=postsByAuthor — list post URNs
  2. /rest/dmaPosts (BATCH_GET)                    — post content + timestamps
  3. /rest/dmaSocialMetadata (BATCH_GET)           — likes (by reaction type) + comments
  4. /rest/dmaFeedContentsExternal?q=repostsFromEntity   — reposts (no commentary)
  5. /rest/dmaFeedContentsExternal?q=resharesFromEntity  — reshares (with commentary)

Rate limit: dmaFeedContentsExternal is 1 request per 60 seconds, hard.
Posts BATCH_GET and SocialMetadata are normal-rate.

Modes:
  default — weekly run. Fetches content for everything, refreshes engagement
            on posts published within REFRESH_WINDOW_DAYS (default 7) days.
  --initial — first run. Fetches content + engagement + reposts for every
            PDP post regardless of age. Use once, to bridge from the
            Supermetrics era. Adds ~50 min of repost calls for ~25 posts.

Secrets (env, never hardcoded):
  LINKEDIN_CLIENT_ID
  LINKEDIN_CLIENT_SECRET
  LINKEDIN_REFRESH_TOKEN
  LINKEDIN_ORG_URN          e.g. urn:li:organization:94119582
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH = REPO_ROOT / "linkedin-bridge" / "data" / "tidalwave_posts_cache.json"
SNAPSHOT_DIR = REPO_ROOT / "linkedin-bridge" / "data" / "snapshots"

LINKEDIN_API_BASE = "https://api.linkedin.com"
LINKEDIN_OAUTH_URL = "https://www.linkedin.com/oauth/v2/accessToken"
LINKEDIN_API_VERSION = "202604"
REQUEST_TIMEOUT = 60
FEED_RATE_LIMIT_SECONDS = 60  # dmaFeedContentsExternal: 1 req / 60s
POSTS_PAGE_SIZE = 100         # max 1000 per docs, 100 is plenty for weekly pulls
BATCH_CHUNK_SIZE = 50         # posts/socialMetadata batch get chunk size
REFRESH_WINDOW_DAYS = 7       # rolling repost-refresh window

# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

@dataclass
class Post:
    """Mirrors the existing cache schema, plus created_at_ms."""

    post_id: str
    date: str
    created_at_ms: int | None
    channel: str = "linkedin"
    source: str = "update"
    author_voice: str = "brand"
    content: str = ""
    url: str = ""
    likes: int = 0
    comments: int = 0
    reposts: int | None = None
    clicks: int | None = None
    impressions: int | None = None
    engagement_rate: float | None = None
    engagement_score: int | None = None
    performance_tier: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "post_id": self.post_id,
            "date": self.date,
            "created_at_ms": self.created_at_ms,
            "channel": self.channel,
            "source": self.source,
            "author_voice": self.author_voice,
            "content": self.content,
            "url": self.url,
            "likes": self.likes,
            "comments": self.comments,
            "reposts": self.reposts,
            "clicks": self.clicks,
            "impressions": self.impressions,
            "engagement_rate": self.engagement_rate,
            "engagement_score": self.engagement_score,
            "performance_tier": self.performance_tier,
        }


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_access_token(client_id: str, client_secret: str, refresh_token: str) -> str:
    """Exchange a long-lived refresh token for a short-lived access token."""
    resp = requests.post(
        LINKEDIN_OAUTH_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        timeout=REQUEST_TIMEOUT,
    )
    if not resp.ok:
        print(
            f"LinkedIn token endpoint returned {resp.status_code}: {resp.text}",
            file=sys.stderr,
        )
        resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("LinkedIn OAuth response missing access_token")
    return token


def auth_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "LinkedIn-Version": LINKEDIN_API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
    }


# ---------------------------------------------------------------------------
# URN helpers
# ---------------------------------------------------------------------------

def url_for_post(urn: str) -> str:
    """Produce a viewable feed URL for a post URN."""
    return f"https://www.linkedin.com/feed/update/{urn}"


def encode_urn_for_list(urn: str) -> str:
    """URL-encode a URN for use inside Rest.li's List(...) syntax."""
    return urllib.parse.quote(urn, safe="")


def build_ids_param(urns: list[str]) -> str:
    """Build the ids=List(...) parameter value for a BATCH_GET."""
    encoded = ",".join(encode_urn_for_list(u) for u in urns)
    return f"List({encoded})"


# ---------------------------------------------------------------------------
# Feed Content Finder (rate-limited: 1 req per 60s)
# ---------------------------------------------------------------------------

class FeedFinderClient:
    """
    Thin wrapper that enforces the 60-second-between-calls rate limit on
    every call to dmaFeedContentsExternal, regardless of which finder.
    """

    def __init__(self, access_token: str):
        self.headers = auth_headers(access_token)
        self._last_call_ts: float = 0.0

    def _wait_for_rate_limit(self) -> None:
        now = time.time()
        elapsed = now - self._last_call_ts
        if elapsed < FEED_RATE_LIMIT_SECONDS:
            wait = FEED_RATE_LIMIT_SECONDS - elapsed + 1  # +1s safety buffer
            print(f"  rate-limit: sleeping {wait:.0f}s", flush=True)
            time.sleep(wait)

    def call(self, params: dict[str, Any]) -> dict[str, Any]:
        self._wait_for_rate_limit()
        # urlencode with safe='' so URNs get encoded properly
        url = f"{LINKEDIN_API_BASE}/rest/dmaFeedContentsExternal"
        resp = requests.get(
            url,
            headers=self.headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        self._last_call_ts = time.time()
        if not resp.ok:
            print(
                f"dmaFeedContentsExternal returned {resp.status_code}: "
                f"{resp.text[:500]}",
                file=sys.stderr,
            )
            resp.raise_for_status()
        return resp.json()

    def list_posts_by_author(self, org_urn: str) -> list[str]:
        """Paginate through all posts authored by the org. Returns post URNs."""
        out: list[str] = []
        cursor: str | None = None
        page = 0
        while True:
            page += 1
            params: dict[str, Any] = {
                "q": "postsByAuthor",
                "author": org_urn,
                "maxPaginationCount": POSTS_PAGE_SIZE,
            }
            if cursor:
                params["paginationCursor"] = cursor
            print(f"  postsByAuthor page {page} (cursor={cursor or 'start'})",
                  flush=True)
            data = self.call(params)
            elements = data.get("elements", [])
            for e in elements:
                urn = e.get("id")
                if urn:
                    out.append(urn)
            meta = data.get("metadata", {}).get("paginationCursorMetdata", {})
            # Yes, the API really spells it "Metdata" — docs confirm.
            cursor = meta.get("nextPaginationCursor")
            if not cursor or not elements:
                break
        return out

    def count_reposts_from_entity(self, entity_urn: str) -> int:
        """Total instantReposts for a post (reposts without commentary)."""
        return self._count_paginated_finder(
            q="repostsFromEntity",
            param_name="entity",
            param_value=entity_urn,
        )

    def count_reshares_from_entity(self, entity_urn: str) -> int:
        """Total reshare posts for a post (reposts with commentary)."""
        return self._count_paginated_finder(
            q="resharesFromEntity",
            param_name="entity",
            param_value=entity_urn,
        )

    def _count_paginated_finder(
        self, *, q: str, param_name: str, param_value: str
    ) -> int:
        """
        Walk a paginated finder and return total element count.
        Note each page costs 60s of rate limit, so for posts with very high
        repost counts this can be slow. In practice the response includes
        a 'total' in paging metadata which we use to short-circuit.
        """
        count = 0
        cursor: str | None = None
        page = 0
        while True:
            page += 1
            params: dict[str, Any] = {
                "q": q,
                param_name: param_value,
                "maxPaginationCount": POSTS_PAGE_SIZE,
            }
            if cursor:
                params["paginationCursor"] = cursor
            data = self.call(params)
            elements = data.get("elements", [])
            count += len(elements)
            # If the paging block gives us a total, trust it and bail
            paging = data.get("paging", {})
            total = paging.get("total")
            if isinstance(total, int) and page == 1 and total == count:
                return count
            meta = data.get("metadata", {}).get("paginationCursorMetdata", {})
            cursor = meta.get("nextPaginationCursor")
            if not cursor or not elements:
                break
        return count


# ---------------------------------------------------------------------------
# Posts BATCH_GET — full content for a list of URNs
# ---------------------------------------------------------------------------

def fetch_post_content(
    access_token: str, post_urns: list[str]
) -> dict[str, dict[str, Any]]:
    """
    BATCH_GET full post data for the given URNs.
    Returns a dict keyed by post URN.
    """
    headers = auth_headers(access_token)
    out: dict[str, dict[str, Any]] = {}

    for i in range(0, len(post_urns), BATCH_CHUNK_SIZE):
        chunk = post_urns[i : i + BATCH_CHUNK_SIZE]
        ids_param = build_ids_param(chunk)
        url = (
            f"{LINKEDIN_API_BASE}/rest/dmaPosts"
            f"?ids={ids_param}&viewContext=READER"
        )
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            print(
                f"dmaPosts batch {i}–{i+len(chunk)} returned {resp.status_code}:"
                f" {resp.text[:500]}",
                file=sys.stderr,
            )
            resp.raise_for_status()
        results = resp.json().get("results", {})
        out.update(results)

    return out


# ---------------------------------------------------------------------------
# Social Metadata BATCH_GET — reactions + comment counts
# ---------------------------------------------------------------------------

def fetch_social_metadata(
    access_token: str, post_urns: list[str]
) -> dict[str, dict[str, Any]]:
    """
    BATCH_GET social metadata. Accepts share/ugcPost URNs directly.
    Returns a dict keyed by URN with reactionSummaries and commentSummary.
    """
    headers = auth_headers(access_token)
    out: dict[str, dict[str, Any]] = {}

    for i in range(0, len(post_urns), BATCH_CHUNK_SIZE):
        chunk = post_urns[i : i + BATCH_CHUNK_SIZE]
        ids_param = build_ids_param(chunk)
        url = f"{LINKEDIN_API_BASE}/rest/dmaSocialMetadata?ids={ids_param}"
        resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        if not resp.ok:
            print(
                f"dmaSocialMetadata batch {i}–{i+len(chunk)} returned "
                f"{resp.status_code}: {resp.text[:500]}",
                file=sys.stderr,
            )
            # Don't fail the whole run on social metadata failure — log and continue
            continue
        results = resp.json().get("results", {})
        out.update(results)

    return out


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def extract_commentary(raw_post: dict[str, Any]) -> str:
    """
    Pull the post body text from a dmaPosts response object.
    `commentary` is the documented field; LinkedIn may obfuscate it if
    the author hasn't opted into DMA data sharing.
    """
    commentary = raw_post.get("commentary")
    if isinstance(commentary, dict):
        text = commentary.get("text")
        if isinstance(text, str):
            return text
    if isinstance(commentary, str):
        return commentary
    return ""


def extract_created_ms(raw_post: dict[str, Any]) -> int | None:
    """publishedAt is preferred; fall back to created.time."""
    if isinstance(raw_post.get("publishedAt"), int):
        return raw_post["publishedAt"]
    created = raw_post.get("created", {})
    if isinstance(created, dict) and isinstance(created.get("time"), int):
        return created["time"]
    return None


def sum_reactions(social: dict[str, Any] | None) -> int:
    """Sum all reaction types (LIKE + PRAISE + EMPATHY + …) into a single likes count."""
    if not social:
        return 0
    summaries = social.get("reactionSummaries") or {}
    total = 0
    for entry in summaries.values():
        c = entry.get("count")
        if isinstance(c, int):
            total += c
    return total


def comment_count(social: dict[str, Any] | None) -> int:
    if not social:
        return 0
    summary = social.get("commentSummary") or {}
    c = summary.get("count")
    return c if isinstance(c, int) else 0


def derive_performance_tier(score: int | None) -> str | None:
    if score is None:
        return None
    if score >= 40:
        return "top"
    if score >= 15:
        return "mid"
    return "low"


def compute_engagement_score(
    likes: int, comments: int, reposts: int | None
) -> int:
    """Match the score shape already in the cache. Reposts weighted heavily."""
    base = likes * 0.5 + comments * 2
    if reposts is not None:
        base += reposts * 3
    return min(100, int(base))


def build_post(
    urn: str,
    raw_post: dict[str, Any] | None,
    social: dict[str, Any] | None,
    repost_total: int | None,
) -> Post | None:
    """Combine the three sources into a single Post record."""
    if raw_post is None:
        return None

    pid = urn.rsplit(":", 1)[-1]
    created_ms = extract_created_ms(raw_post)
    if created_ms is None:
        return None  # can't place it in time, skip

    dt = datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc)
    likes = sum_reactions(social)
    comments = comment_count(social)

    score = compute_engagement_score(likes, comments, repost_total)
    # Engagement rate is unrecoverable without impressions; null is correct.
    return Post(
        post_id=pid,
        date=dt.strftime("%Y-%m-%d"),
        created_at_ms=created_ms,
        content=extract_commentary(raw_post),
        url=url_for_post(urn),
        likes=likes,
        comments=comments,
        reposts=repost_total,  # may be None if we skipped repost fetch
        clicks=None,
        impressions=None,
        engagement_rate=None,
        engagement_score=score if (likes + comments + (repost_total or 0)) else None,
        performance_tier=derive_performance_tier(
            score if (likes + comments + (repost_total or 0)) else None
        ),
    )


# ---------------------------------------------------------------------------
# Merge + write
# ---------------------------------------------------------------------------

def load_existing_cache() -> list[dict[str, Any]]:
    if not CACHE_PATH.exists():
        return []
    with CACHE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def merge_posts(
    existing: list[dict[str, Any]],
    new_posts: list[Post],
) -> list[dict[str, Any]]:
    """
    Upsert by post_id. Existing posts not in the new fetch are preserved
    (backfilled posts survive forever).

    For posts present in both:
      - engagement fields from the new fetch win
      - but: never null-out a previously-good value with None
              (e.g. if reposts wasn't refreshed this run, keep old reposts)
      - author_voice set to something non-default is preserved
    """
    by_id: dict[str, dict[str, Any]] = {p["post_id"]: dict(p) for p in existing}

    for new in new_posts:
        new_dict = new.to_dict()
        prior = by_id.get(new.post_id)
        if prior is None:
            by_id[new.post_id] = new_dict
            continue
        merged = dict(prior)
        for k, v in new_dict.items():
            if v is None and prior.get(k) is not None:
                continue  # preserve old non-null value
            merged[k] = v
        if (
            prior.get("author_voice")
            and prior["author_voice"] != "brand"
            and new.author_voice == "brand"
        ):
            merged["author_voice"] = prior["author_voice"]
        by_id[new.post_id] = merged

    def sort_key(p: dict[str, Any]) -> tuple[int, str]:
        ms = p.get("created_at_ms")
        if isinstance(ms, int):
            return (ms, p["post_id"])
        d = datetime.strptime(p["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (int(d.timestamp() * 1000), p["post_id"])

    return sorted(by_id.values(), key=sort_key, reverse=True)


def write_atomic(path: Path, data: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def require_env(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        print(f"Missing required env var: {key}", file=sys.stderr)
        sys.exit(2)
    return val


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Pull LinkedIn DMA posts into cache.")
    p.add_argument(
        "--initial",
        action="store_true",
        help=(
            "First-run mode: fetch reposts for ALL PDP posts regardless of "
            "age. Use once at bridge initialization. Adds ~50min to the run."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()

    client_id = require_env("LINKEDIN_CLIENT_ID")
    client_secret = require_env("LINKEDIN_CLIENT_SECRET")
    refresh_token = require_env("LINKEDIN_REFRESH_TOKEN")
    org_urn = require_env("LINKEDIN_ORG_URN")

    print("→ Refreshing access token", flush=True)
    access_token = get_access_token(client_id, client_secret, refresh_token)

    feed = FeedFinderClient(access_token)

    print("→ Listing posts by author (rate-limited, 60s between pages)",
          flush=True)
    post_urns = feed.list_posts_by_author(org_urn)
    print(f"  found {len(post_urns)} post URNs from PDP", flush=True)

    if not post_urns:
        print("No PDP posts returned. Cache untouched.")
        return 0

    print("→ Fetching post content (batch)", flush=True)
    raw_posts = fetch_post_content(access_token, post_urns)
    print(f"  hydrated {len(raw_posts)} posts", flush=True)

    print("→ Fetching social metadata (batch)", flush=True)
    social = fetch_social_metadata(access_token, post_urns)
    print(f"  got social metadata for {len(social)} posts", flush=True)

    # Decide which posts get repost counts this run.
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    window_ms = REFRESH_WINDOW_DAYS * 24 * 60 * 60 * 1000
    refresh_cutoff_ms = now_ms - window_ms

    repost_targets: list[str] = []
    for urn in post_urns:
        raw = raw_posts.get(urn)
        if raw is None:
            continue
        created_ms = extract_created_ms(raw)
        if created_ms is None:
            continue
        if args.initial:
            repost_targets.append(urn)
        elif created_ms >= refresh_cutoff_ms:
            repost_targets.append(urn)

    if args.initial:
        mode_label = "INITIAL (all posts)"
    else:
        mode_label = f"rolling {REFRESH_WINDOW_DAYS}-day window"
    print(
        f"→ Fetching reposts for {len(repost_targets)} posts "
        f"({mode_label}). "
        f"Each post = 2 calls × 60s ≈ {len(repost_targets) * 2} min.",
        flush=True,
    )

    repost_counts: dict[str, int] = {}
    for idx, urn in enumerate(repost_targets, 1):
        print(f"  [{idx}/{len(repost_targets)}] reposts for {urn}", flush=True)
        try:
            r1 = feed.count_reposts_from_entity(urn)
            r2 = feed.count_reshares_from_entity(urn)
            repost_counts[urn] = r1 + r2
            print(f"    instantReposts={r1} reshares={r2} total={r1+r2}",
                  flush=True)
        except requests.HTTPError as e:
            # Log and continue — one bad post shouldn't kill the whole run.
            print(f"    failed: {e}", file=sys.stderr)
            continue

    print("→ Transforming", flush=True)
    new_posts: list[Post] = []
    for urn in post_urns:
        raw = raw_posts.get(urn)
        post = build_post(
            urn=urn,
            raw_post=raw,
            social=social.get(urn),
            repost_total=repost_counts.get(urn),
        )
        if post is not None:
            new_posts.append(post)
    print(f"  built {len(new_posts)} post records", flush=True)

    print("→ Merging with existing cache", flush=True)
    existing = load_existing_cache()
    print(f"  existing cache: {len(existing)} posts", flush=True)
    merged = merge_posts(existing, new_posts)
    print(f"  merged cache:   {len(merged)} posts", flush=True)

    print(f"→ Writing {CACHE_PATH.relative_to(REPO_ROOT)}", flush=True)
    write_atomic(CACHE_PATH, merged)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snapshot_path = SNAPSHOT_DIR / f"{today}.json"
    print(f"→ Writing {snapshot_path.relative_to(REPO_ROOT)}", flush=True)
    write_atomic(snapshot_path, merged)

    print("Done.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
