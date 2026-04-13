#!/usr/bin/env bash
# Leftover to Makeover — create venv, install API deps, start the web UI + REST API.
# Usage: bash run_ui.sh
# Then open http://127.0.0.1:8000/

set -euo pipefail
cd "$(dirname "$0")"
PY="${PY:-python3}"

if ! command -v "$PY" &>/dev/null; then
  echo "Need $PY on PATH." >&2
  exit 1
fi

if [[ ! -d .venv ]]; then
  echo "Creating .venv ..."
  "$PY" -m venv .venv
fi

# shellcheck source=/dev/null
source .venv/bin/activate

echo "Installing dependencies (python -m pip) ..."
python -m pip install -U pip setuptools wheel -q
python -m pip install -r requirements.txt

echo "Starting server at http://127.0.0.1:8000/ (Ctrl+C to stop)"
exec python -m uvicorn recommendation_api.main:app --host 0.0.0.0 --port 8000 --reload
