## app

Runnable search-engine code for Assignment 2: PySpark data prep, Hadoop Streaming indexers, **ScyllaDB** storage (via the `cassandra-driver` CQL client), and a YARN `spark-submit` ranker.

### Layout

| Path | Role |
|------|------|
| `data/` | Local scratch for `.txt` shards before `hdfs dfs -put` |
| `mapreduce/` | `mapper1.py` / `reducer1.py` (inverted index + corpus stats) |
| `prepare_data.py` | Parquet → `/data` on HDFS, then `/input/data` (one RDD partition) |
| `app.py` | ScyllaDB keyspace/tables + loader (invoked from `store_index.sh`) |
| `scylla_connect.py` | Shared CQL cluster settings (DC-aware LB, `LOCAL_ONE`) |
| `query.py` | BM25 retrieval (RDD path), reads stdin or CLI args |
| `*.sh` | Service startup, indexing, search |

Read `requirements.txt` for Python dependencies; `app.sh` installs them into `.venv` and packs the env for YARN executors.
