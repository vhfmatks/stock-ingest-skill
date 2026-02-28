#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/standalone_stock_ingest.py"
DEFAULT_ENV_FILE="${SKILL_DIR}/.env"
ENV_FILE="${STOCK_INGEST_ENV_FILE:-${DEFAULT_ENV_FILE}}"
ENV_FILE_EXPLICIT=false

if [[ ! -f "${PY_SCRIPT}" ]]; then
  echo "[ERROR] standalone runtime not found: ${PY_SCRIPT}" >&2
  exit 2
fi

# Compatibility alias:
# previous runtime supported `config` via backend CLI wizard.
if [[ "${1:-}" == "config" ]]; then
  cat >&2 <<'EOF'
[INFO] standalone mode에서는 config wizard를 사용하지 않습니다.
skill 전용 env 파일(.env)을 사용하세요:
  cp .env.example .env
  # .env 수정 후 실행
필요 시 --env-file /path/to/file.env 로 다른 파일도 지정할 수 있습니다.
EOF
  exit 0
fi

# Pre-parse env file option (do not forward this option to python).
args=("$@")
i=0
while [[ $i -lt ${#args[@]} ]]; do
  arg="${args[$i]}"
  case "${arg}" in
    --env-file)
      ENV_FILE_EXPLICIT=true
      if [[ $((i + 1)) -lt ${#args[@]} ]]; then
        i=$((i + 1))
        ENV_FILE="${args[$i]}"
      else
        echo "[ERROR] --env-file requires a value" >&2
        exit 2
      fi
      ;;
    --env-file=*)
      ENV_FILE_EXPLICIT=true
      ENV_FILE="${arg#*=}"
      ;;
  esac
  i=$((i + 1))
done

if [[ -n "${ENV_FILE}" && -f "${ENV_FILE}" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
elif [[ "${ENV_FILE_EXPLICIT}" == "true" ]]; then
  echo "[ERROR] env file not found: ${ENV_FILE}" >&2
  exit 2
fi

# Allow global options anywhere:
# standalone_stock_ingest.py [GLOBAL] <command> [command options]
globals=()
rest=()
i=0
while [[ $i -lt ${#args[@]} ]]; do
  arg="${args[$i]}"
  case "${arg}" in
    --json|--help|-h)
      globals+=("${arg}")
      ;;
    --sqlite-path|--timeout|--kis-base-url|--dart-base-url|--kis-max-price-pages)
      globals+=("${arg}")
      if [[ $((i + 1)) -lt ${#args[@]} ]]; then
        i=$((i + 1))
        globals+=("${args[$i]}")
      fi
      ;;
    --sqlite-path=*|--timeout=*|--kis-base-url=*|--dart-base-url=*|--kis-max-price-pages=*)
      globals+=("${arg}")
      ;;
    --env-file)
      if [[ $((i + 1)) -lt ${#args[@]} ]]; then
        i=$((i + 1))
      fi
      ;;
    --env-file=*)
      ;;
    *)
      rest+=("${arg}")
      ;;
  esac
  i=$((i + 1))
done

if command -v python3 >/dev/null 2>&1; then
  exec python3 "${PY_SCRIPT}" "${globals[@]}" "${rest[@]}"
fi

exec python "${PY_SCRIPT}" "${globals[@]}" "${rest[@]}"
