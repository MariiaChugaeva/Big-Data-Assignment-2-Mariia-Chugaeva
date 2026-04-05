import os
import subprocess

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("data preparation")
    .master("local")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .getOrCreate()
)

n = int(os.environ.get("N_DOCS", "100"))
parquet_path = os.environ.get("PARQUET_PATH", "/a.parquet")

df = spark.read.parquet(parquet_path).select("id", "title", "text")
df = df.filter(df.text.isNotNull() & (df.text != ""))
cnt = df.count()
if cnt == 0:
    raise SystemExit("No non-empty documents in parquet.")

frac = min(1.0, max(100 * n / cnt, 1e-9))
df = df.sample(withReplacement=False, fraction=frac, seed=0).limit(min(n, cnt))

data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(data_dir, exist_ok=True)


def create_doc(row):
    fn = os.path.join(
        data_dir,
        sanitize_filename(str(row["id"]) + "_" + str(row["title"])).replace(" ", "_")
        + ".txt",
    )
    with open(fn, "w", encoding="utf-8") as f:
        f.write(row["text"] or "")


df.foreach(create_doc)

subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/data"], check=False)
subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/data"], check=True)
subprocess.run(
    ["bash", "-c", f"hdfs dfs -put -f '{data_dir}'/*.txt /data/"],
    check=True,
)

sc = spark.sparkContext


def line_from(kv):
    path, text = kv
    base = os.path.basename(path)
    if not base.endswith(".txt") or "_" not in base[:-4]:
        return None
    stem = base[:-4]
    doc_id, title = stem.split("_", 1)
    body = (text or "").strip().replace("\t", " ")
    if not body:
        return None
    return f"{doc_id}\t{title}\t{body}"


subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "/input/data"], check=False)
(
    sc.wholeTextFiles("/data")
    .map(line_from)
    .filter(lambda x: x is not None)
    .coalesce(1)
    .saveAsTextFile("/input/data")
)

spark.stop()
