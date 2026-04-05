"""Rebuild /input/data from every *.txt under HDFS /data (single output partition)."""
from __future__ import annotations

import os
import subprocess
import sys

from pyspark.sql import SparkSession


def line_from_kv(kv: tuple[str, str]) -> str | None:
    path, content = kv
    base = os.path.basename(path)
    if not base.endswith(".txt"):
        return None
    stem = base[:-4]
    if "_" not in stem:
        return None
    doc_id, title_u = stem.split("_", 1)
    body = content.strip()
    if not body:
        return None
    safe = body.replace("\t", " ").replace("\r", "\n")
    return f"{doc_id}\t{title_u}\t{safe}"


def main() -> None:
    spark = SparkSession.builder.appName("rebuild-input-from-hdfs-data").getOrCreate()
    sc = spark.sparkContext
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=False)
    whole = sc.wholeTextFiles("/data")
    lines = whole.map(line_from_kv).filter(lambda x: x is not None)
    lines.coalesce(1).saveAsTextFile("/input/data")
    spark.stop()


if __name__ == "__main__":
    main()
    sys.exit(0)
