#!/usr/bin/env bash
set -eu

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <query words...>" >&2
  exit 1
fi

export PATH="$ROOT/.venv/bin:$PATH"
export PYSPARK_DRIVER_PYTHON="$ROOT/.venv/bin/python3"

if [[ -f /app/.venv.tar.gz ]]; then
  ARCHIVE_ARGS=(--archives /app/.venv.tar.gz#.venv)
  export PYSPARK_PYTHON=.venv/bin/python
else
  ARCHIVE_ARGS=()
  export PYSPARK_PYTHON="${PYSPARK_DRIVER_PYTHON}"
fi

spark-submit \
  --master yarn \
  --deploy-mode client \
  --name assignment2-search \
  --py-files "$ROOT/scylla_connect.py" \
  "${ARCHIVE_ARGS[@]}" \
  --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}" \
  --conf "spark.executorEnv.PYSPARK_PYTHON=${PYSPARK_PYTHON}" \
  "$ROOT/query.py" "$@"
