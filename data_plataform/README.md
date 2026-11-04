# NexoMart

Open-source data platform built from scratch around **NexoMart**, a fictional multi-channel
Brazilian marketplace. Every analytical table is **Delta Lake**, cataloged by the Hive
Metastore. The goal is to separate the transactional
layer (OLTP) from the analytical layer (OLAP) without depending on any proprietary cloud.

> **Note:** this README is intentionally short while the project is released in stages. A
> fuller version will land as more of the platform is published.

## Project status — what is live in this repository

This stage covers the **transactional and ingestion** side, plus the **Spark foundation**:

- **PostgreSQL** — OLTP database (natively partitioned on the five largest tables, with a
  retention routine). Tuning and partition migrations in `configs/postgresql/`.
- **MinIO** — S3-compatible object storage; the `landing` zone for raw Parquet.
- **dbapi** — async FastAPI ingestion API (SQLAlchemy 2.0 + asyncpg + Pydantic), in a strict
  Router -> Service -> Repository layering, with the domain seeders and chaos engineering.
- **Airflow (NexoMart DAGs)** — `nexomart_seeder_dag`, `nexomart_mutator_dag`,
  `nexomart_ingestion_dag`, `nexomart_streaming_dag`: they simulate a live OLTP workload and
  land it incrementally (watermark-based) into MinIO.
- **Spark foundation (Delta Lake)** — `spark/src/`: reusable reader/writer classes, Delta
  helpers (partitioned and MERGE writes), column/table validators, and composable
  transformers (`parseJson`, `flatten`, `explode`, `email`, `splitname`, `extractsize`,
  `fillnullvalues`, `normalizegender`, `normalizemaritalstatus`, `tofloat`, `totimestamp`,
  `tolower`, `audit`, `metadata`). Bronze and Silver jobs that use them are not published yet.

The rest of the analytical layer (Spark jobs and cluster, Hive Metastore, Trino, dbt) exists in
development but is **not part of this repository yet**.

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
├── spark/src/      # Spark foundation: common/ (reader, writer, delta, validators),
│                   #   transformers/, domains/ (per-layer, per-domain logic)
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

## Roadmap / release checklist

Release numbers are a working plan, not git tags. Checked means built in development; the
Spark foundation (v0.6) is the latest stage published here.

### Shipped

- [x] **v0.1 — Platform foundation:** Docker stack on a shared network, `make` targets, secrets-driven config with no hardcoded endpoints
- [x] **v0.2 — Synthetic OLTP:** 7 domains, 31 tables, 3-layer dbapi, seeders with dynamic volume ranges and 5-10% chaos engineering
- [x] **v0.3 — Orchestration and landing zone:** seeder, mutator, ingestion and streaming DAGs; watermark-based incremental ingestion to `s3a://landing/`; dimension history spread 2020 to now
- [x] **v0.4 — Data safety and tuning:** Postgres tuning, partitioning, 45-day retention, nightly VHD compaction, disk-mount guards
- [x] **v0.5 — Contracts and CI:** YAML data contracts, contract drift gate in the seeder DAG, GitHub Actions
- [x] **v0.6 — Spark foundation (Delta):** reader/writer/validators, partitioned and MERGE writers, composable transformers

### Next

- [ ] **v0.7 — Bronze layer:** one job per table, landing to Delta with audit columns, all 7 schemas, Airflow DAG after ingestion
- [ ] **v0.8 — Silver and data quality:** Great Expectations suites from the documented chaos rules, quarantine tables, `auto_fix` vs `flag_only` corrections with MERGE on natural key; prototype on `marketing`, then the other 6 domains
- [ ] **v0.9 — Gold and dbt:** dimensions and facts from the bus matrix, dbt models and tests, serving layer in Postgres for BI
- [ ] **v0.10 — Cleanup and hardening:** remove legacy Iceberg and AdventureWorks leftovers, apply Postgres partitioning durably, automated tests for the transformers
- [ ] **v1.0 — End-to-end release:** one command from seed to Gold, every layer under contract and orchestrated

### Later

- [ ] **v1.1 — Marketplace:** PJ sellers as a new dimension across sales and product
- [ ] **v1.2 — NexoBank base:** KYC, digital account, ledger, PIX, cards
- [ ] **v1.3 — Credit:** consumer credit and seller working capital, scores computed in Spark and written back to Postgres
- [ ] **v1.4 — Investments:** CDB/LCI/Treasury/funds, daily yield in Spark, CDI/Selic/IPCA from the Banco Central API
- [ ] **v1.5 — AI/ML:** demand and inventory forecasting, then semantic search over the product catalog
- [ ] **Backlog:** CDC/Kafka with schema registry, OpenTelemetry, OpenLineage, Alembic migrations
