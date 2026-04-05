#!/usr/bin/env bash
set -eu

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

IN="${1:-/input/data}"
bash "$ROOT/create_index.sh" "${IN}"
bash "$ROOT/store_index.sh"
