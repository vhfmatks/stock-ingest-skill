---
name: stock-ingest
description: "Standalone stock ingest skill (no backend app import). Use when users want single/all symbol ingest with run_type all, sqlite persistence, and shell-export env preflight checks for KIS/DART credentials."
---

# Stock Ingest (Standalone + SQLite)

This skill runs a standalone runtime from the skill folder only.
It does **not** use `app/cli/ingest.py`.

Runtime entry:

```bash
scripts/run_stock_ingest.sh
```

Underlying engine:

```bash
scripts/standalone_stock_ingest.py
```

## Workflow

1. Detect command (`run`, `status`, `db-check`, `help`).
2. For `run` (non-dry-run), check required **exported shell env vars**.
3. If env is missing, stop and show export guidance.
4. Execute standalone ingest and persist to sqlite.
5. Return JSON summary including `run_id`, status, row counts, notes.

`margins` 단계는 KIS `inquire-psbl-order` + `intgr-margin` 응답 기반으로 실데이터를 저장합니다.

## Simplified `run_type=all` mode

Preferred flows:

1. 특정 종목 all
2. 전체 종목 all

Prices window:

- `fast` (7d, default)
- `normal` (30d, explicit)
- `full` (backfill, explicit)

## Quick Start

### 1) Dry-run (no env required)

```bash
scripts/run_stock_ingest.sh run \
  --run-type all \
  --scope single \
  --symbol 005930 \
  --dry-run \
  --json
```

### 2) Real run (env required)

```bash
scripts/run_stock_ingest.sh run \
  --run-type all \
  --scope single \
  --symbol 005930 \
  --prices-window fast \
  --json
```

### 3) Status / DB check

```bash
scripts/run_stock_ingest.sh status <run_id> --json
scripts/run_stock_ingest.sh db-check <run_id> --json
```

## Required exported env vars (non-dry-run)

Use skill-local `.env` file (preferred):

```bash
cp .env.example .env
```

`scripts/run_stock_ingest.sh` auto-loads `.env` by default.
If needed, use `--env-file /path/to/file.env` to override.

Required keys (non-dry-run):

```bash
export KIS_APP_KEY=...
export KIS_APP_SECRET=...
export DART_API_KEY=...   # scope=all 또는 events 포함 시 필요
export KIS_ACCOUNT_NO=... # margins 포함 시 필요 (예: 12345678+01)
```

Optional:

```bash
export KIS_BASE_URL=https://openapi.koreainvestment.com:9443
```

## Data storage

Default sqlite path:

`~/.openclaw/workspace/data/stockfinder_standalone.db`

Override:

```bash
scripts/run_stock_ingest.sh --sqlite-path /path/to/file.db run ...
```

Legacy alias (deprecated):

```bash
scripts/run_backend_ingest.sh ...
```

## Safety rules

- Do not use backend `app/cli/ingest.py`.
- Keep `run_type=all` default prices window as `fast`.
- Use `full/backfill` only when user explicitly asks.
- Never print secret values.

## References

- Runtime contract: `references/backend-cli-contract.md`
- Input checklist: `references/input-template.md`
