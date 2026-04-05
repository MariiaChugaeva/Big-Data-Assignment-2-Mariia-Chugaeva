#!/usr/bin/env python3
"""
Streaming reducer: groups on term (or __CORPUS__) and writes BM25-oriented rows.
"""
from __future__ import annotations

import sys

CORPUS_KEY = "__CORPUS__"


def flush_corpus(buf: list[str]) -> None:
    total_docs = 0
    sum_dl = 0
    for item in buf:
        try:
            _, dl_s = item.split("\t", 1)
            sum_dl += int(dl_s)
            total_docs += 1
        except ValueError:
            continue
    sys.stdout.write(f"CORPUS\t{total_docs}\t{sum_dl}\n")


def flush_term(term: str, buf: list[str]) -> None:
    postings: dict[str, tuple[int, int]] = {}
    for item in buf:
        try:
            doc_id, tf_s, dl_s = item.split("\t", 2)
            tf = int(tf_s)
            dl = int(dl_s)
        except ValueError:
            continue
        prev = postings.get(doc_id)
        if prev is None or tf > prev[0]:
            postings[doc_id] = (tf, dl)
    if not postings:
        return
    df = len(postings)
    chunks = sorted(f"{d}|{tf}|{dl}" for d, (tf, dl) in postings.items())
    post = ",".join(chunks)
    sys.stdout.write(f"TERM\t{term}\t{df}\t{post}\n")


def main() -> None:
    cur: str | None = None
    bucket: list[str] = []

    def spill() -> None:
        nonlocal cur, bucket
        if cur is None:
            return
        if cur == CORPUS_KEY:
            flush_corpus(bucket)
        else:
            flush_term(cur, bucket)
        bucket = []

    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line:
            continue
        key, val = line.split("\t", 1)
        if key != cur:
            spill()
            cur = key
        bucket.append(val)
    spill()


if __name__ == "__main__":
    main()
