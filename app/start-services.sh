#!/usr/bin/env bash
set -eu

echo "[start-services] HDFS + YARN + MR history"

# Course compose has only cluster-slave-1; image defaults list slaves 2–5 → fix HDFS capacity 0.
if [[ -n "${HADOOP_HOME:-}" ]]; then
  for wf in "$HADOOP_HOME/etc/hadoop/workers" "$HADOOP_HOME/etc/hadoop/slaves"; do
    if [[ -f "$wf" ]]; then
      printf '%s\n' "cluster-slave-1" >"$wf"
      echo "[start-services] wrote cluster-slave-1 -> $wf"
    fi
  done
fi

"${HADOOP_HOME}/sbin/start-dfs.sh"
"${HADOOP_HOME}/sbin/start-yarn.sh"
mapred --daemon start historyserver || true

jps -lm || true
hdfs dfsadmin -report || true
hdfs dfsadmin -safemode leave || true

hdfs dfs -mkdir -p /apps/spark/jars
hdfs dfs -chmod 744 /apps/spark/jars || true
hdfs dfs -put -f /usr/local/spark/jars/* /apps/spark/jars/ 2>/dev/null || true
hdfs dfs -chmod -R +rx /apps/spark/jars/ 2>/dev/null || true

hdfs dfs -mkdir -p /user/root

jps -lm || true
