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

## Skill 전용 env 파일 사용

쉘 전역(`~/.bashrc`, `~/.zshrc`) 대신, skill 루트의 `.env`를 사용합니다.

```bash
cp .env.example .env
# .env 파일에 실제 값 입력
```

`scripts/run_stock_ingest.sh` 실행 시 기본으로 `.env`를 자동 로드합니다.

다른 파일을 쓰고 싶다면:

```bash
scripts/run_stock_ingest.sh --env-file /path/to/stock-ingest.env run ...
```

또는:

```bash
STOCK_INGEST_ENV_FILE=/path/to/stock-ingest.env scripts/run_stock_ingest.sh run ...
```

## 필수 환경변수 (실행 시, .env 기준)

```bash
export KIS_APP_KEY=...
export KIS_APP_SECRET=...
export DART_API_KEY=...
export KIS_ACCOUNT_NO=...   # margins 포함 시 필요
```
