# Big Data Assignment 2 — Simple search engine (MapReduce + ScyllaDB + Spark)

Based on the course template: [firas-jolha/big-data-assignment2](https://github.com/firas-jolha/big-data-assignment2).

## Prerequisites

- Docker and Docker Compose

### Windows hosts

Shell scripts **should use LF line endings** (not CRLF). On Windows, Cursor/VS Code: open `app/*.sh` → status bar **CRLF** → **LF** → save.

**Automatic fix:** `docker-compose` mounts **`entrypoint-master.sh`** (repo root). It runs first and runs `sed` on every `/app/*.sh` to remove carriage returns, then starts `app.sh`. Keep **`entrypoint-master.sh`** as **LF** if you edit it (same as other shell files). `.gitattributes` requests LF for `*.sh` and this file when using Git.

## Run

From this directory (the folder that contains **`docker-compose.yml`**, not only `app/`):

```bash
docker compose up
```

**If it looks like “nothing happens”:**

1. First run can sit on **“Pulling …”** for a long time (large images). Wait, or run `docker compose pull` once.
2. Use **`docker compose up`** without **`-d`** so logs stream in the terminal. With **`-d`**, run **`docker compose logs -f cluster-master`** to follow the master.
3. Check containers: **`docker compose ps`**. If **`cluster-master`** is **Exited**, see **`docker compose logs cluster-master`**.
4. Ensure **Docker Desktop** is running (Windows).

**If you see `$'\r': command not found`:** scripts still had CRLF; the entrypoint now fixes them with **Python** (more reliable than `sed` on Windows mounts).

**If HDFS shows `Configured Capacity: 0`:** the worker container must stay running (**`stdin_open` + `tty`** on `cluster-slave-1`), and **`start-services.sh`** overwrites Hadoop’s **`workers`** file so only **`cluster-slave-1`** is used (the image default lists non-existent slaves 2–5).

This starts the Hadoop master/slave stack and **ScyllaDB**, mounts `./app`, and runs **`entrypoint-master.sh`** then **`app/app.sh`** on the master (HDFS/YARN, Python venv, data prep, index, sample searches).

**Configuration:** the DB host defaults to `scylla-server` (see `docker-compose.yml`). Override with `SCYLLA_HOSTS` or `CASSANDRA_HOSTS` if needed; set `SCYLLA_LOCAL_DC` if your cluster uses a datacenter name other than `datacenter1`.

## Data

Place a Wikipedia parquet shard (e.g. `a.parquet` from the Kaggle dataset linked in the brief) in `app/` before building. The compose entrypoint expects `app/a.parquet` (see `prepare_data.sh`). By default, 100 non-empty articles are sampled for indexing (override with `N_DOCS` if you need more).

## Reports

Submit `report.pdf` separately per course instructions; this repository holds the runnable code and scripts only.
