"""
ScyllaDB schema + bulk load of indexer output and document titles.

Invoked by store_index.sh after MapReduce finishes.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time

from cassandra.cluster import Cluster, NoHostAvailable

import scylla_connect

KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "search_eng")
INDEX_HDFS = os.environ.get("INDEX_HDFS_PATH", "/indexer/pipeline1/part-00000")

TOKEN = re.compile(r"[a-z0-9]+")


def wait_for_scylla(max_wait_s: int = 360) -> Cluster:
    deadline = time.time() + max_wait_s
    last_exc: Exception | None = None
    while time.time() < deadline:
        try:
            cluster = scylla_connect.make_cluster(connect_timeout=10)
            cluster.connect()
            return cluster
        except NoHostAvailable as exc:
            last_exc = exc
            time.sleep(5)
    points = ",".join(scylla_connect.contact_points())
    raise RuntimeError(f"ScyllaDB not reachable at [{points}]: {last_exc}")


def hdfs_text(path: str) -> str:
    proc = subprocess.run(
        ["hdfs", "dfs", "-cat", path],
        check=True,
        capture_output=True,
        text=True,
        errors="replace",
    )
    return proc.stdout


def hdfs_merge_parts(dir_path: str) -> str:
    ls = subprocess.run(
        ["hdfs", "dfs", "-ls", dir_path],
        check=False,
        capture_output=True,
        text=True,
    )
    parts: list[str] = []
    for line in ls.stdout.splitlines():
        line = line.strip()
        if not line or line.startswith("Found "):
            continue
        cols = line.split()
        if not cols:
            continue
        pth = cols[-1]
        if "/part-" in pth:
            parts.append(pth)
    chunks = [hdfs_text(p) for p in sorted(parts)]
    return "".join(chunks)


def ensure_schema(session) -> None:
    dc = scylla_connect.local_dc()
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'NetworkTopologyStrategy', '{dc}': 1}};
        """
    )
    session.set_keyspace(KEYSPACE)
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            singleton text PRIMARY KEY,
            doc_count bigint,
            total_dl bigint
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term text PRIMARY KEY,
            df int
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS inverted_index (
            term text PRIMARY KEY,
            df int,
            postings text
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id text PRIMARY KEY,
            title text,
            dl int
        );
        """
    )


def reset_tables(session) -> None:
    for tbl in ("documents", "inverted_index", "vocabulary", "corpus_stats"):
        session.execute(f"TRUNCATE {tbl};")


def doc_token_len(title_underscored: str, text: str) -> int:
    blob = f"{title_underscored.replace('_', ' ')} {text}".lower()
    return len(TOKEN.findall(blob))


def load_index(session, raw: str) -> None:
    ins_vocab = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    ins_inv = session.prepare(
        "INSERT INTO inverted_index (term, df, postings) VALUES (?, ?, ?)"
    )
    ins_corpus = session.prepare(
        "INSERT INTO corpus_stats (singleton, doc_count, total_dl) VALUES (?, ?, ?)"
    )

    corpus_written = False
    for line in raw.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if parts[0] == "CORPUS" and len(parts) >= 3:
            session.execute(
                ins_corpus, ("global", int(parts[1]), int(parts[2]))
            )
            corpus_written = True
            continue
        if parts[0] == "TERM" and len(parts) >= 4:
            term = parts[1]
            df = int(parts[2])
            postings = parts[3]
            session.execute(ins_vocab, (term, df))
            session.execute(ins_inv, (term, df, postings))

    if not corpus_written:
        print("Warning: no CORPUS row in indexer output.", file=sys.stderr)


def load_documents(session, raw: str) -> None:
    ins = session.prepare("INSERT INTO documents (doc_id, title, dl) VALUES (?, ?, ?)")
    for line in raw.splitlines():
        if not line.strip():
            continue
        doc_id, title, text = line.split("\t", 2)
        dl = doc_token_len(title, text)
        session.execute(ins, (doc_id, title.replace("_", " "), dl))


def main() -> None:
    docs_dir = os.environ.get("DOCS_HDFS_DIR", "/input/data")

    cluster = wait_for_scylla()
    session = cluster.connect()
    ensure_schema(session)
    reset_tables(session)

    load_index(session, hdfs_text(INDEX_HDFS))
    load_documents(session, hdfs_merge_parts(docs_dir))

    cluster.shutdown()
    print("ScyllaDB load finished.")


if __name__ == "__main__":
    main()
