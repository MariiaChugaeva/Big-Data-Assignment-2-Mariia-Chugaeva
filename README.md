# Big Data Assignment 2

Simple search engine with Hadoop MapReduce, ScyllaDB, and Spark, based on the course template: [firas-jolha/big-data-assignment2](https://github.com/firas-jolha/big-data-assignment2).

## Prerequisites

- Docker
- Docker Compose

## Data

Put the parquet shard in:

`app/a.parquet`

The scripts expect that exact path and filename.

## Run

From the repository root (the folder with `docker-compose.yml`):

```bash
docker compose up
```

This starts the Hadoop master/worker containers and ScyllaDB, then runs `app/app.sh` on the master. That script:

1. starts HDFS and YARN
2. creates the Python environment
3. runs data preparation
4. builds the index
5. loads ScyllaDB
6. runs sample searches

## Configuration

- Database host defaults to `scylla-server`
- You can override hosts with `SCYLLA_HOSTS` or `CASSANDRA_HOSTS`
- If needed, override the datacenter name with `SCYLLA_LOCAL_DC`

## Report

Compile `report/main.tex` to produce `report.pdf` for submission.
