#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
NEW_ENTRY="${SCRIPT_DIR}/run_stock_ingest.sh"

if [[ ! -f "${NEW_ENTRY}" ]]; then
  echo "[ERROR] runtime wrapper not found: ${NEW_ENTRY}" >&2
  exit 2
fi

if [[ "${STOCK_INGEST_SILENCE_DEPRECATION:-0}" != "1" ]]; then
  echo "[WARN] run_backend_ingest.sh 는 deprecated 입니다. run_stock_ingest.sh 를 사용하세요." >&2
fi

exec "${NEW_ENTRY}" "$@"
