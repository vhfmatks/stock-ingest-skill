# Input checklist template (ask user before execute)

Use this concise checklist when required inputs are missing.

## 1) Command

- command: `run` / `status` / `db-check` / `help`

## 2) If command is `run`

- run_type: (default `symbols`)
- source_profile: (default `all`)
- symbol scope:
  - `--symbol` repeated OR
  - `--symbols` csv OR
  - explicit "전체 대상"
- persist mode:
  - normal execute OR
  - `--dry-run`

### Simplified mode for `run_type=all` (preferred)

Ask only:

1. scope:
   - `single` (특정 종목)
   - `all` (전체 종목)
2. symbols (single일 때만)
3. prices window:
   - `fast` (최근 7일, 기본)
   - `normal` (최근 30일)
   - `full` (backfill)
4. execute or dry-run

## 3) Optional filters

- timeframes: D/W/M/Y
- as_of / as_of_from / as_of_to
- execution_date
- force_refresh
- table_targets
- provider_routing (e.g. `prices=backup`)
- fixture path
- timeout sec

## 4) For `status` / `db-check`

- run_id (required)

## 5) Full window rule

`full`은 비용이 크므로, 사용자가 명시적으로 요청한 경우에만 사용한다.
