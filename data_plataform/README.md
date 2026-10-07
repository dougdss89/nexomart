# Data Platform — IcebergLake

Open-source data platform built from scratch for the **IcebergLake** case study — a
multi-channel Brazilian marketplace (NexoMart). The goal is to separate the transactional
layer (OLTP) from the analytical layer (OLAP) without depending on any proprietary cloud.

> **Note:** this README is intentionally short while the project is released in stages. A
> fuller version will land as more of the platform is published.

## Project status — what is live in this repository

This stage covers the **transactional and ingestion** side only:

- **PostgreSQL** — OLTP database (natively partitioned on the five largest tables, with a
  retention routine). Tuning and partition migrations in `configs/postgresql/`.
- **MinIO** — S3-compatible object storage; the `landing` zone for raw Parquet.
- **dbapi** — async FastAPI ingestion API (SQLAlchemy 2.0 + asyncpg + Pydantic), in a strict
  Router -> Service -> Repository layering, with the domain seeders and chaos engineering.
- **Airflow (NexoMart DAGs)** — `nexomart_seeder_dag`, `nexomart_mutator_dag`,
  `nexomart_ingestion_dag`, `nexomart_streaming_dag`: they simulate a live OLTP workload and
  land it incrementally (watermark-based) into MinIO.

The analytical layer (Spark, Delta Lake, Apache Iceberg, Hive Metastore, Trino, dbt) exists
in development but is **not part of this repository yet**.

## Business domains

`product`, `person`, `sales`, `marketing`, `humanresources`, `purchasing`, `warehouse` — a
realistic marketplace schema seeded with dynamic volumes and 1-10% injected data-quality
anomalies (chaos engineering) for testing cleaning, fraud-detection, and governance pipelines.

## Repository layout (this stage)

```
data_plataform/
├── dbapi/          # FastAPI ingestion API (models, schemas, repositories,
│                   #   services, routers, seeders, samples)
├── postgres/       # PostgreSQL + init scripts
├── minio/          # S3-compatible object storage
├── airflow/dags/nexomart/   # seeder / mutator / ingestion / streaming DAGs
├── configs/        # Postgres migrations & retention, seeder ranges, connection loader
└── scripts/        # local ops (WSL2/Windows disk mount, maintenance)
```

## Quick start

```bash
# 1. credentials (never committed)
cp configs/secrets.env.example configs/secrets.env   # then edit with real values

# 2. bring up the transactional + ingestion stack
make up-postgres
make up-minio
make up-airflow
make up-dbapi
```

```bash
# run a seeder directly
uv run python dbapi/api/seeder/<domain>/<file>.py --min-count 1000 --max-count 50000
```

## Prerequisites

- Docker Desktop (WSL2 on Windows) + Docker Compose v2 + Make
- Python 3.12+ with [uv](https://github.com/astral-sh/uv)
