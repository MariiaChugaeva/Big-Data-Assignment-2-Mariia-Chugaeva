#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

INPUT_PATH="${1:-/input/data}"

STREAMING_JAR="$(find "${HADOOP_HOME}/share/hadoop/tools/lib" -name 'hadoop-streaming-*.jar' 2>/dev/null | head -n 1 || true)"
if [[ -z "${STREAMING_JAR}" ]]; then
  echo "Could not locate hadoop-streaming jar under HADOOP_HOME=${HADOOP_HOME:-unset}" >&2
  exit 1
fi

hdfs dfs -rm -r -f /tmp/indexer/mr1
hdfs dfs -mkdir -p /tmp/indexer

hadoop jar "${STREAMING_JAR}" \
  -D mapreduce.job.name=search-mr1-inverted-index \
  -D mapreduce.job.reduces=1 \
  -files mapreduce/mapper1.py,mapreduce/reducer1.py \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "${INPUT_PATH}" \
  -output /tmp/indexer/mr1

hdfs dfs -rm -r -f /indexer/pipeline1
hdfs dfs -mkdir -p /indexer/pipeline1
echo "Contents of /tmp/indexer/mr1:"
hdfs dfs -ls /tmp/indexer/mr1 2>&1 || true
PART="$(hdfs dfs -ls /tmp/indexer/mr1 2>/dev/null | awk '/part-/ {print $NF; exit}')"
if [[ -z "${PART}" ]]; then
  echo "No reducer part file under /tmp/indexer/mr1" >&2
  exit 1
fi
hdfs dfs -rm -f "/indexer/pipeline1/part-00000"
hdfs dfs -cp "${PART}" "/indexer/pipeline1/part-00000"

echo "Indexer output: /indexer/pipeline1/part-00000"
