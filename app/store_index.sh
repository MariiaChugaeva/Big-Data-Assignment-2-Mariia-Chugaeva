#!/usr/bin/env bash
set -eu

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

export PATH="$ROOT/.venv/bin:$PATH"
"$ROOT/.venv/bin/python3" "$ROOT/app.py"
