#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

if [[ ! -f a.parquet ]]; then
  echo "Missing app/a.parquet — place a Wikipedia parquet shard here (see assignment dataset)." >&2
  exit 1
fi

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON PYSPARK_PYTHON
PYSPARK_DRIVER_PYTHON="$(command -v python3)"
PYSPARK_PYTHON="$(command -v python3)"

hdfs dfs -mkdir -p /user/root
echo "prepare_data: uploading a.parquet to HDFS /a.parquet ..."
hdfs dfs -put -f a.parquet /a.parquet

# prepare_data.py uses .master("local"); no YARN executors needed here.
echo "prepare_data: running spark-submit prepare_data.py ..."
spark-submit --name prepare-data --driver-memory 2g prepare_data.py

echo "prepare_data: done — local data/ should list .txt files; HDFS has /data and /input/data."
