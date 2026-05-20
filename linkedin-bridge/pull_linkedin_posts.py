"""
LinkedIn PDP → Git cache bridge.

Pulls Tidalwave's LinkedIn organization posts from the Pages Data Portability
(PDP / DMA) API, joins with engagement analytics, and merges into the
data/tidalwave_posts_cache.json file at the repo root.

Design notes:
- Backfilled posts (pre-app-creation, outside PDP's window) are preserved
  forever — the API doesn't return them, but we never drop posts from the
  cache that we can't see. New posts get `created_at_ms` (Unix ms); old
  backfilled posts keep `date` only with `created_at_ms: null`.
- Atomic writes: temp file → fsync → rename. The repo never ends up with
  a half-cache if the run fails partway through.
- A dated snapshot is also written to data/snapshots/YYYY-MM-DD.json for
  week-over-week diffs.

Secrets (all from environment, never hardcoded):
  LINKEDIN_CLIENT_ID
  LINKEDIN_CLIENT_SECRET
  LINKEDIN_REFRESH_TOKEN
  LINKEDIN_ORG_URN          e.g. urn:li:organization:94119582
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent
CACHE_PATH = REPO_ROOT / "data" / "tidalwave_posts_cache.json"
SNAPSHOT_DIR = REPO_ROOT / "data" / "snapshots"

LINKEDIN_API_BASE = "https://api.linkedin.com"
LINKEDIN_OAUTH_URL = "https://www.linkedin.com/oauth/v2/accessToken"
LINKEDIN_API_VERSION = "202401"  # bump as LinkedIn rolls forward
REQUEST_TIMEOUT = 30
PAGE_SIZE = 50


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

@dataclass
class Post:
    """Mirrors the existing tidalwave_posts_cache.json schema, plus created_at_ms."""

    post_id: str
    date: str                       # YYYY-MM-DD (kept for backward compat)
    created_at_ms: int | None       # Unix ms, full timestamp; null for backfilled posts
    channel: str
    source: str
    author_voice: str
    content: str
    url: str
    likes: int
    comments: int
    reposts: int
    clicks: int
    impressions: int
    engagement_rate: float
    engagement_score: int
    performance_tier: str

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
# Fetch
# ---------------------------------------------------------------------------

def fetch_dma_posts(access_token: str, org_urn: str) -> list[dict[str, Any]]:
    """
    Pull all posts for the organization from /dmaPosts.

    Paginates until exhausted. Returns the raw post objects; we extract
    fields downstream so the parsing is easy to revisit.
    """
    headers = auth_headers(access_token)
    start = 0
    out: list[dict[str, Any]] = []

    while True:
        params = {
            "q": "memberAndOrganization",
            "author": org_urn,
            "count": PAGE_SIZE,
            "start": start,
        }
        resp = requests.get(
            f"{LINKEDIN_API_BASE}/rest/dmaPosts",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        elements = data.get("elements", [])
        if not elements:
            break
        out.extend(elements)
        if len(elements) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(0.5)  # be polite

    return out


def fetch_post_analytics(
    access_token: str,
    org_urn: str,
    post_ids: list[str],
) -> dict[str, dict[str, Any]]:
    """
    Pull engagement metrics for the given posts.

    Returns a dict keyed by post_id. Posts the API doesn't return analytics
    for end up missing from the dict; callers should treat that as
    "leave existing metrics alone."
    """
    if not post_ids:
        return {}

    headers = auth_headers(access_token)
    out: dict[str, dict[str, Any]] = {}

    # LinkedIn's analytics endpoints typically accept batched URN lists.
    # Chunk to keep URLs reasonable.
    CHUNK = 20
    for i in range(0, len(post_ids), CHUNK):
        chunk = post_ids[i : i + CHUNK]
        share_urns = [f"urn:li:share:{pid}" for pid in chunk]
        params = {
            "q": "organizationalEntity",
            "organizationalEntity": org_urn,
            "shares": "List(" + ",".join(share_urns) + ")",
        }
        resp = requests.get(
            f"{LINKEDIN_API_BASE}/rest/organizationalPageContentAnalytics",
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        # Don't fail the whole run if analytics is flaky — log and continue.
        if not resp.ok:
            print(
                f"  analytics chunk {i}-{i+len(chunk)} failed: "
                f"{resp.status_code} {resp.text[:200]}",
                file=sys.stderr,
            )
            continue
        for elem in resp.json().get("elements", []):
            share_urn = elem.get("share") or elem.get("entity") or ""
            pid = share_urn.rsplit(":", 1)[-1]
            if pid:
                out[pid] = elem
        time.sleep(0.5)

    return out


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def derive_performance_tier(engagement_score: int) -> str:
    """Match the tiering already present in the cache."""
    if engagement_score >= 40:
        return "top"
    if engagement_score >= 15:
        return "mid"
    return "low"


def post_id_from_urn(urn: str) -> str:
    """urn:li:ugcPost:1234 → 1234"""
    return urn.rsplit(":", 1)[-1]


def url_from_urn(urn: str) -> str:
    return f"https://www.linkedin.com/feed/update/{urn}"


def extract_content(raw: dict[str, Any]) -> str:
    """
    PDP post objects nest the body text under commentary.text or
    specificContent depending on post type. Try the common paths.
    """
    commentary = raw.get("commentary")
    if isinstance(commentary, dict):
        text = commentary.get("text")
        if isinstance(text, str):
            return text
    if isinstance(commentary, str):
        return commentary
    specific = raw.get("specificContent", {})
    share = specific.get("com.linkedin.ugc.ShareContent", {})
    text = share.get("shareCommentary", {}).get("text")
    if isinstance(text, str):
        return text
    return ""


def build_post_from_pdp(
    raw: dict[str, Any],
    analytics: dict[str, Any] | None,
) -> Post | None:
    """Turn a raw PDP post + analytics blob into our Post model."""
    urn = raw.get("id") or raw.get("urn") or ""
    if not urn:
        return None
    pid = post_id_from_urn(urn)

    created = raw.get("createdAt") or raw.get("created", {}).get("time")
    if not isinstance(created, int):
        return None  # no timestamp = useless for our purposes

    dt = datetime.fromtimestamp(created / 1000, tz=timezone.utc)

    metrics = analytics or {}
    likes = int(metrics.get("likeCount", 0) or 0)
    comments = int(metrics.get("commentCount", 0) or 0)
    reposts = int(metrics.get("shareCount", 0) or 0)
    clicks = int(metrics.get("clickCount", 0) or 0)
    impressions = int(metrics.get("impressionCount", 0) or 0)

    engagement_rate = 0.0
    if impressions > 0:
        engagement_rate = round(
            (likes + comments + reposts + clicks) / impressions * 100, 4
        )

    # Score formula mirrors the existing cache's apparent shape:
    # weighted sum normalized to a 0-100ish band. Reasonable starting point;
    # tune in the brand-voice skill if needed.
    engagement_score = min(
        100,
        int(likes * 0.5 + comments * 2 + reposts * 3 + clicks * 0.05),
    )

    return Post(
        post_id=pid,
        date=dt.strftime("%Y-%m-%d"),
        created_at_ms=created,
        channel="linkedin",
        source="update",
        author_voice="brand",
        content=extract_content(raw),
        url=url_from_urn(urn),
        likes=likes,
        comments=comments,
        reposts=reposts,
        clicks=clicks,
        impressions=impressions,
        engagement_rate=engagement_rate,
        engagement_score=engagement_score,
        performance_tier=derive_performance_tier(engagement_score),
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
    (this is how backfilled history survives forever). For posts present
    in both, the new fetch wins for engagement fields; we add created_at_ms
    if it was previously null.
    """
    by_id: dict[str, dict[str, Any]] = {
        p["post_id"]: dict(p) for p in existing
    }

    for new in new_posts:
        new_dict = new.to_dict()
        prior = by_id.get(new.post_id)
        if prior is None:
            by_id[new.post_id] = new_dict
            continue
        # Preserve any prior fields the new fetch couldn't determine
        # (e.g. author_voice if it was manually tagged something other
        # than "brand"). Then overlay the freshly-known fields.
        merged = dict(prior)
        merged.update(new_dict)
        # Don't downgrade created_at_ms from a real value to null
        if merged.get("created_at_ms") is None and prior.get("created_at_ms"):
            merged["created_at_ms"] = prior["created_at_ms"]
        # Don't overwrite manually-curated author_voice with default "brand"
        if prior.get("author_voice") and new.author_voice == "brand":
            merged["author_voice"] = prior["author_voice"]
        by_id[new.post_id] = merged

    # Sort newest first. Posts without created_at_ms (backfilled) sort by
    # date string, which is still chronologically correct day-resolution.
    def sort_key(p: dict[str, Any]) -> tuple[int, str]:
        ms = p.get("created_at_ms")
        if isinstance(ms, int):
            return (ms, p["post_id"])
        # Convert date to a comparable ms-ish value
        d = datetime.strptime(p["date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return (int(d.timestamp() * 1000), p["post_id"])

    return sorted(by_id.values(), key=sort_key, reverse=True)


def write_atomic(path: Path, data: list[dict[str, Any]]) -> None:
    """Write to a temp file, fsync, rename — never leave a half-cache."""
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


def main() -> int:
    client_id = require_env("LINKEDIN_CLIENT_ID")
    client_secret = require_env("LINKEDIN_CLIENT_SECRET")
    refresh_token = require_env("LINKEDIN_REFRESH_TOKEN")
    org_urn = require_env("LINKEDIN_ORG_URN")

    print("→ Refreshing access token")
    access_token = get_access_token(client_id, client_secret, refresh_token)

    print("→ Fetching posts from /dmaPosts")
    raw_posts = fetch_dma_posts(access_token, org_urn)
    print(f"  got {len(raw_posts)} posts")

    if not raw_posts:
        print("No posts returned — keeping existing cache untouched.")
        return 0

    post_ids = [post_id_from_urn(p.get("id") or p.get("urn") or "") for p in raw_posts]
    post_ids = [pid for pid in post_ids if pid]

    print("→ Fetching analytics")
    analytics = fetch_post_analytics(access_token, org_urn, post_ids)
    print(f"  got analytics for {len(analytics)} posts")

    print("→ Transforming")
    new_posts: list[Post] = []
    for raw in raw_posts:
        urn = raw.get("id") or raw.get("urn") or ""
        pid = post_id_from_urn(urn)
        post = build_post_from_pdp(raw, analytics.get(pid))
        if post is not None:
            new_posts.append(post)
    print(f"  transformed {len(new_posts)} posts")

    print("→ Merging with existing cache")
    existing = load_existing_cache()
    print(f"  existing cache has {len(existing)} posts")
    merged = merge_posts(existing, new_posts)
    print(f"  merged cache has {len(merged)} posts")

    print(f"→ Writing {CACHE_PATH.relative_to(REPO_ROOT)}")
    write_atomic(CACHE_PATH, merged)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    snapshot_path = SNAPSHOT_DIR / f"{today}.json"
    print(f"→ Writing snapshot {snapshot_path.relative_to(REPO_ROOT)}")
    write_atomic(snapshot_path, merged)

    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
