#!/usr/bin/env bash
set -eu
#
# Optional (+10): add one local plain-text document to HDFS /data, rebuild /input/data,
# then rerun the full MapReduce index + Scylla load.
#

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 /path/to/<doc_id>_<Title_With_Underscores>.txt" >&2
  exit 1
fi

FILE="$1"
if [[ ! -f "${FILE}" ]]; then
  echo "Not a file: ${FILE}" >&2
  exit 1
fi

base="$(basename "${FILE}")"
if [[ "${base}" != *.txt ]]; then
  echo "Expected a .txt file, got ${base}" >&2
  exit 1
fi

hdfs dfs -mkdir -p /data
hdfs dfs -put -f "${FILE}" "/data/${base}"

export PATH="$ROOT/.venv/bin:$PATH"
export PYSPARK_DRIVER_PYTHON="$ROOT/.venv/bin/python3"

spark-submit \
  --master yarn \
  --deploy-mode client \
  --name add-to-index-rebuild-input \
  "$ROOT/rebuild_input.py"

bash "$ROOT/index.sh" /input/data
