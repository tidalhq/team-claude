# team-claude

Shared knowledge and data pipelines that Claude instances at Tidalwave can
read. Acts as an external persistence layer that bypasses the silos between
Claude.ai accounts, Projects, and connectors.

The pattern: a scheduled job pulls data from a source system (LinkedIn, in
the first case) and commits structured output into `data/`. Any Claude with
read access to this repo — via the GitHub connector, Zapier MCP, or a direct
clone — gets the same picture.

## Pipelines

| Path | Purpose | Schedule |
|---|---|---|
| [`linkedin-bridge/`](./linkedin-bridge/) | Tidalwave LinkedIn org posts + engagement, fed to the `brand-voice` skill | Weekly, Mon 09:15 UTC |

## Data layout

```
data/
├── tidalwave_posts_cache.json    # live cache, overwritten each run
└── snapshots/
    └── YYYY-MM-DD.json           # weekly snapshots, append-only history
```

Pipelines write to `data/`. Schemas are documented in each pipeline's README.

## Adding a pipeline

1. Create a subdirectory under the repo root: `your-pipeline/`.
2. Write the script and a `requirements.txt`.
3. Add a workflow at `.github/workflows/your-pipeline-*.yml`.
4. Document the schema and secrets in `your-pipeline/README.md`.
5. List it in the table above.

## Secrets

All credentials live in GitHub Actions secrets — never in code, never in
this repo. If a secret is exposed, rotate it at the source immediately.
