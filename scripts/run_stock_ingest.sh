#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY_SCRIPT="${SCRIPT_DIR}/standalone_stock_ingest.py"

if [[ ! -f "${PY_SCRIPT}" ]]; then
  echo "[ERROR] standalone runtime not found: ${PY_SCRIPT}" >&2
  exit 2
fi

# Compatibility alias:
# previous runtime supported `config` via backend CLI wizard.
if [[ "${1:-}" == "config" ]]; then
  cat >&2 <<'EOF'
[INFO] standalone mode에서는 config wizard를 사용하지 않습니다.
아래 환경변수를 shell에 export 하세요:
  export KIS_APP_KEY=...
  export KIS_APP_SECRET=...
  export DART_API_KEY=...   # scope=all 또는 events 시 필요
  export KIS_ACCOUNT_NO=... # margins 시 필요
필요 시 ~/.bashrc 또는 ~/.profile에 저장 후 source 하세요.
EOF
  exit 0
fi

# Allow global options anywhere:
# standalone_stock_ingest.py [GLOBAL] <command> [command options]
globals=()
rest=()
args=("$@")
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
