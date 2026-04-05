"""
BM25 retrieval: reads index rows from ScyllaDB and ranks documents with PySpark RDD ops.
Query from CLI args (search.sh) or stdin.
"""
from __future__ import annotations

import math
import os
import re
import sys

from pyspark.sql import SparkSession

import scylla_connect

K1 = 1.0
B = 0.75
TOP_K = 10
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "search_eng")

TOKEN = re.compile(r"[a-z0-9]+")


def tokenize(q: str) -> list[str]:
    return TOKEN.findall(q.lower())


def read_query() -> str:
    if len(sys.argv) > 1:
        return " ".join(sys.argv[1:]).strip()
    data = sys.stdin.read()
    return data.strip()


def load_corpus_row(session):
    row = session.execute(
        "SELECT doc_count, total_dl FROM corpus_stats WHERE singleton=%s", ("global",)
    ).one()
    if row is None:
        raise RuntimeError("corpus_stats is empty — run the indexer first.")
    n_docs = int(row.doc_count)
    total_dl = int(row.total_dl)
    avgdl = total_dl / n_docs if n_docs else 0.0
    return n_docs, avgdl


def idf(n_docs: int, df: int) -> float:
    return math.log((n_docs - df + 0.5) / (df + 0.5) + 1.0)


def term_contributions(term: str, n_docs: int, avgdl: float, session):
    row = session.execute(
        "SELECT df, postings FROM inverted_index WHERE term=%s", (term,)
    ).one()
    if row is None:
        return []
    df = int(row.df)
    weight = idf(n_docs, df)
    out: list[tuple[str, float]] = []
    for chunk in row.postings.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        doc_id, tf_s, dl_s = chunk.split("|", 2)
        tf = int(tf_s)
        dl = int(dl_s)
        denom = tf + K1 * (1.0 - B + B * (dl / avgdl if avgdl else 0.0))
        num = tf * (K1 + 1.0)
        if denom == 0:
            continue
        out.append((doc_id, weight * (num / denom)))
    return out


def make_remote_shard(n_docs: int, avgdl: float):
    ks = KEYSPACE

    def shard(term: str):
        cl = scylla_connect.make_cluster(connect_timeout=10)
        sess = cl.connect(ks)
        try:
            return term_contributions(term, n_docs, avgdl, sess)
        finally:
            cl.shutdown()

    return shard


def main() -> None:
    query = read_query()
    if not query:
        print("Empty query.", file=sys.stderr)
        sys.exit(2)

    terms = tokenize(query)
    if not terms:
        print("No usable tokens after normalization.", file=sys.stderr)
        sys.exit(2)

    cluster = scylla_connect.make_cluster(connect_timeout=15)
    session = cluster.connect(KEYSPACE)
    spark = SparkSession.builder.appName("assignment2-query").getOrCreate()
    try:
        n_docs, avgdl = load_corpus_row(session)
        sc = spark.sparkContext
        shard_fn = make_remote_shard(n_docs, avgdl)
        pairs = sc.parallelize(terms).flatMap(shard_fn)
        ranked = pairs.reduceByKey(lambda a, b: a + b).takeOrdered(
            TOP_K, key=lambda kv: -kv[1]
        )
        stmt = session.prepare("SELECT title FROM documents WHERE doc_id=?")
        for doc_id, score in ranked:
            drow = session.execute(stmt, (doc_id,)).one()
            title = drow.title if drow else "?"
            print(f"{doc_id}\t{title}\t{score:.6f}")
    finally:
        cluster.shutdown()
        spark.stop()


if __name__ == "__main__":
    main()
