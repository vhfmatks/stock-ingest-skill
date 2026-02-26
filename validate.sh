#!/usr/bin/env bash
set -euo pipefail

required=(
  "SKILL.md"
  "agents/openai.yaml"
  "references/backend-cli-contract.md"
  "references/input-template.md"
  "scripts/run_stock_ingest.sh"
  "scripts/standalone_stock_ingest.py"
)

for f in "${required[@]}"; do
  [[ -f "$f" ]] || { echo "missing: $f"; exit 1; }
done

[[ -x scripts/run_stock_ingest.sh ]] || { echo "not executable: scripts/run_stock_ingest.sh"; exit 1; }

echo "ok: stock-ingest package is deployable"
