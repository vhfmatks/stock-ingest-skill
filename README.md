# stock-ingest skill (standalone)

이 저장소는 `stock-ingest` 스킬만 독립 배포하기 위한 패키지입니다.

## 구성

- `SKILL.md`
- `agents/openai.yaml`
- `references/*`
- `scripts/run_stock_ingest.sh`
- `scripts/standalone_stock_ingest.py`
- `scripts/run_backend_ingest.sh` (deprecated alias)

## 설치

```bash
mkdir -p ~/.codex/skills/stock-ingest
cp -R . ~/.codex/skills/stock-ingest
chmod +x ~/.codex/skills/stock-ingest/scripts/*.sh
```

## 실행 예시 (dry-run)

```bash
scripts/run_stock_ingest.sh run \
  --run-type all \
  --scope single \
  --symbol 005930 \
  --dry-run \
  --json
```

## 필수 환경변수 (실행 시)

```bash
export KIS_APP_KEY=...
export KIS_APP_SECRET=...
export DART_API_KEY=...
```
