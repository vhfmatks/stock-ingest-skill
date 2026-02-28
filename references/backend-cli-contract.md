# Standalone runtime contract

Entry wrapper:

```bash
scripts/run_stock_ingest.sh <command> [options]
```

Engine:

```bash
scripts/standalone_stock_ingest.py
```

Legacy alias:

```bash
scripts/run_backend_ingest.sh <command> [options]   # deprecated
```

## Commands

- `run` (default ingest execution)
- `status <run_id>`
- `db-check <run_id>`
- `help`

## Global options

- `--json`
- `--sqlite-path <path>`
- `--timeout <sec>`
- `--kis-base-url <url>`
- `--dart-base-url <url>`
- `--kis-max-price-pages <int>`
- `--env-file <path>` (optional, default: skill root `.env`)

## `run` options

- `--run-type symbols|prices|fundamental|financials|events|margins|all`
- `--scope single|all`
- `--symbol <code>` (repeatable)
- `--symbols <csv>`
- `--source-profile all|kis|dart`
- `--timeframes D W M Y`
- `--as-of <iso-date>`
- `--as-of-from <iso-date>`
- `--as-of-to <iso-date>`
- `--prices-window fast|normal|full` (default: fast)
- `--prices-lookback-days <int>`
- `--prices-backfill`
- `--limit-symbols <int>` (scope=all guardrail)
- `--dry-run`

## Env preflight policy

Non-dry-run `run` checks exported shell env:

- prices/financials/margins 포함 + source_profile all|kis:
  - `KIS_APP_KEY`
  - `KIS_APP_SECRET`
- margins 포함 + source_profile all|kis:
  - `KIS_ACCOUNT_NO`
- scope=all 또는 events 포함:
  - `DART_API_KEY`

Missing keys → stop and print export guidance.

Recommended: keep env in skill-local `.env` and run via `scripts/run_stock_ingest.sh`
(wrapper auto-loads `.env`).

## SQLite policy

SQLite is the runtime DB (source of truth for this standalone skill).

Default file:
- `~/.openclaw/workspace/data/stockfinder_standalone.db`
