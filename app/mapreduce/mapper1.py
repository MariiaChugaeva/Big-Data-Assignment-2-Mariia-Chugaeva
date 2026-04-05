#!/usr/bin/env python3
"""
Streaming mapper: one input line = doc_id \\t title \\t text
Emits corpus metadata and per-term posting fragments for reducer1.
"""
from __future__ import annotations

import re
import sys
from collections import Counter

TOKEN = re.compile(r"[a-z0-9]+")


def emit_corpus(doc_id: str, dl: int) -> None:
    sys.stdout.write(f"__CORPUS__\t{doc_id}\t{dl}\n")


def emit_term(term: str, doc_id: str, tf: int, dl: int) -> None:
    sys.stdout.write(f"{term}\t{doc_id}\t{tf}\t{dl}\n")


def main() -> None:
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        doc_id, title, text = parts[0], parts[1], parts[2]
        blob = f"{title.replace('_', ' ')} {text}".lower()
        toks = TOKEN.findall(blob)
        dl = len(toks)
        if dl == 0:
            emit_corpus(doc_id, 0)
            continue
        emit_corpus(doc_id, dl)
        for term, tf in Counter(toks).items():
            emit_term(term, doc_id, tf, dl)


if __name__ == "__main__":
    main()
