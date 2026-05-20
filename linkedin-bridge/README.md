# LinkedIn → cache bridge

Scheduled pipeline that pulls Tidalwave's LinkedIn organization posts from the
Pages Data Portability (PDP / DMA) API and commits a fresh JSON cache to this
repo each week. The cache is the data source for the `brand-voice` skill.

## How it runs

`.github/workflows/linkedin-weekly.yml` runs `pull_linkedin_posts.py` every
Monday at 09:15 UTC. Manual runs are available from the Actions tab.

Output files (committed back to `main`):

- `data/tidalwave_posts_cache.json` — live cache, overwritten each run
- `data/snapshots/YYYY-MM-DD.json` — dated snapshot, append-only history

## Required secrets

Set these in GitHub → Settings → Secrets and variables → Actions:

| Secret | Description |
|---|---|
| `LINKEDIN_CLIENT_ID` | LinkedIn Developer app client ID |
| `LINKEDIN_CLIENT_SECRET` | LinkedIn Developer app client secret |
| `LINKEDIN_REFRESH_TOKEN` | Long-lived refresh token (one-time OAuth dance) |
| `LINKEDIN_ORG_URN` | e.g. `urn:li:organization:94119582` |

The script reads these from environment variables only. **Never commit
secret values to this repo.** If a secret is exposed (chat, screenshot, log),
rotate it in the LinkedIn Developer console immediately.

## Schema

The cache extends the original `tidalwave_posts_cache.json` schema with one
new field, `created_at_ms`. Everything else is unchanged for backward
compatibility with the `brand-voice` skill.

```json
{
  "post_id": "7455272048932696065",
  "date": "2026-04-29",
  "created_at_ms": 1745942400000,
  "channel": "linkedin",
  "source": "update",
  "author_voice": "brand",
  "content": "...",
  "url": "https://www.linkedin.com/feed/update/urn:li:ugcPost:...",
  "likes": 35,
  "comments": 4,
  "reposts": 11,
  "clicks": 149,
  "impressions": 1204,
  "engagement_rate": 16.5282,
  "engagement_score": 50,
  "performance_tier": "top"
}
```

`created_at_ms` is Unix milliseconds (UTC). Posts backfilled from before the
LinkedIn app's creation date have `created_at_ms: null` — PDP doesn't expose
them, so they stay date-only forever. The brand-voice skill should treat
`null` as "no time-of-day info available."

## Merge behavior

On each run, the script reads the existing cache, fetches fresh data from
PDP, and merges by `post_id`:

- **New posts** from the API are added.
- **Existing posts** the API still returns get their engagement metrics
  refreshed.
- **Backfilled posts** (in the cache but not returned by PDP) are preserved
  untouched. The pipeline never drops a post it can't see.
- `author_voice` set to something other than `"brand"` is preserved on
  re-fetch — manual curation isn't clobbered.

Writes are atomic (temp file → fsync → rename). If a run fails partway, the
cache stays as it was.

## Running locally

```bash
cd linkedin-bridge
pip install -r requirements.txt
export LINKEDIN_CLIENT_ID=...
export LINKEDIN_CLIENT_SECRET=...
export LINKEDIN_REFRESH_TOKEN=...
export LINKEDIN_ORG_URN=urn:li:organization:94119582
python pull_linkedin_posts.py
```

This writes to `../data/`, just like the workflow.

## Troubleshooting

**`401 Unauthorized` from LinkedIn.** The refresh token has likely expired
or been revoked. LinkedIn refresh tokens last ~12 months. Re-do the OAuth
dance and update `LINKEDIN_REFRESH_TOKEN` in Actions secrets.

**`403 Forbidden` from `/dmaPosts`.** The app may have lost the
`r_dma_admin_pages_content` scope, or the org URN doesn't match an org the
app is authorized for. Check the LinkedIn Developer console.

**Empty `elements` array.** Either the org has no posts in the PDP window, or
the API version (`LINKEDIN_API_VERSION` constant in the script) has rolled
forward and needs bumping.

**Analytics chunk failures.** The script continues past per-chunk analytics
failures and writes a stderr line. The cache will be written with whatever
analytics did succeed; the next run picks up the rest.

## Bumping the API version

LinkedIn rolls API versions roughly monthly. If you see `426 Upgrade Required`,
update `LINKEDIN_API_VERSION` in `pull_linkedin_posts.py` to the current
recommended version from the LinkedIn Developer console.
