# NexoMart

Open-source data platform built from scratch around **NexoMart**, a fictional multi-channel Brazilian marketplace. The goal is to separate the transactional layer (OLTP) from the analytical layer (OLAP) without depending on any proprietary cloud, and to practice the full data engineering lifecycle: synthetic data with controlled chaos, orchestration, incremental ingestion, a Delta Lake medallion, SQL federation, and data contracts.

Everything runs locally in Docker. Every analytical table is **Delta Lake**; the catalog is the **Hive Metastore**.

> **Status:** Landing zone is live and validated (~40M rows across 7 schemas). Bronze and Silver for NexoMart are under construction; Gold and dbt marts are planned. See [Roadmap](#roadmap--release-checklist).

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      INGESTION & API                         │
│    FastAPI (async) + SQLAlchemy 2.0 + asyncpg + Pydantic     │
├──────────────────────────────────────────────────────────────┤
│                       OLTP DATABASE                          │
│     PostgreSQL 16 + pgvector + JSONB + Partitioning          │
├──────────────────────────────────────────────────────────────┤
│                      ORCHESTRATION                           │
│           Apache Airflow 2.9 (CeleryExecutor + Redis)        │
├──────────────────────────────────────────────────────────────┤
│              DISTRIBUTED PROCESSING (Spark 3.5.8)            │
│          Landing (Parquet) → Bronze → Silver → Gold          │
├──────────────────────────────────────────────────────────────┤
│                   ANALYTICAL STORAGE                         │
│              Delta Lake 3.2.1 on MinIO (S3-compatible)       │
├──────────────────────────────────────────────────────────────┤
│                  CATALOG & METASTORE                         │
│           Hive Metastore 4.0 (PostgreSQL backend)            │
├──────────────────────────────────────────────────────────────┤
│                   QUERY FEDERATION                           │
│          Apache Trino (delta_lake + postgres catalogs)       │
├──────────────────────────────────────────────────────────────┤
│                 ANALYTICAL TRANSFORMATIONS                   │
│                 dbt-trino 1.10 / dbt-core 1.11               │
└──────────────────────────────────────────────────────────────┘
```

### Data Pipeline (Medallion Architecture)

```
PostgreSQL (OLTP, nexomart_db)
      │  nexomart_ingestion_dag (incremental, watermark-based)
      ▼
 Landing  (raw Parquet, s3a://landing/nexomart/<schema>/<table>/)
      │  Spark jobs (reader → validators → transformers → writer)
      ▼
 Bronze   (Delta Lake, as-landed + audit/metadata columns)
      ▼
 Silver   (Delta Lake, cleaned, typed, deduplicated; rejected rows → quarantine)
      ▼
 Gold     (Delta Lake, dimensional model: dims + facts)
      ▼
 Trino ──► dbt (marts) ──► Postgres serving DB (golddw) ──► BI
```

**Format per layer:**

| Layer | Format | Spark location | Trino catalog |
|-------|--------|----------------|---------------|
| Landing | Raw Parquet | `s3a://landing/` | — |
| Quarantine | Parquet (`payload` column) | `s3a://quarantine/` | — |
| Bronze | Delta Lake | `spark_catalog.nexomart_bronze.<schema>_<table>` | `nexomart` |
| Silver | Delta Lake | `spark_catalog.nexomart_silver.<schema>_<table>` | `nexomart` |
| Gold | Delta Lake | `spark_catalog.nexomart_gold.<schema>_<table>` | `nexomart` |
| Serving (BI) | Postgres | `golddw` database | `postgres` |

Layer status: Landing done, Bronze/Silver in progress, Gold planned (see roadmap).

---

## Catalogs

### Spark

A single catalog, `spark_catalog`, backed by `DeltaCatalog` and the Hive Metastore. Delta's catalog only works when registered under that exact name, so there is no per-project Spark catalog. Instead each project/layer pair gets its own Hive database, `<project>_<layer>` (for example `nexomart_silver`), with tables named `<schema>_<table>`.

The helpers in `spark/src/common/delta.py` hide this naming:

```python
from spark.src.common.delta import qualified_table_name, resolved_table_name

# compact name used by write_delta(): "<project>.<layer>.<schema>_<table>"
qualified_table_name("nexomart", "silver", "person", "customer")
# literal name Spark can resolve: spark_catalog.nexomart_silver.person_customer
resolved_table_name("nexomart", "silver", "person", "customer")
```

### Trino

| Catalog | Connector | Use |
|---------|-----------|-----|
| `nexomart` | `delta_lake` + HMS | NexoMart Bronze / Silver / Gold (the project's read path) |
| `nexo` | `delta_lake` + HMS | Platform-level catalog, default target of dbt |
| `postgres` | `postgresql` | Serving layer for BI (`golddw`) |

```sql
SHOW SCHEMAS IN nexomart;
SELECT * FROM nexomart.nexomart_silver.person_customer LIMIT 10;
```

Schema names are the Hive databases, so Trino sees `nexomart_<layer>` as the schema.

> **Legacy:** `iceberglake` and `minio` Trino catalogs, the `iceberglake` Spark catalog and the Iceberg runtime JAR still exist from the old AdventureWorks (`advworks`) pipeline. They are **not used** by NexoMart and are scheduled for removal (see roadmap).

---

## Business Domains

| Schema | Domain | Description |
|--------|--------|-------------|
| `product` | Product | Catalog, models, categories, semantic search via pgvector |
| `person` | Person | Customers, addresses, contacts |
| `sales` | Sales | Orders, line items (JSONB), returns, payments |
| `marketing` | Marketing | Campaigns, ads, per-channel performance (4 levels) |
| `humanresources` | HR | Employees, departments, job history (SCD Type 2) |
| `purchasing` | Purchasing | Purchase orders, vendors, receiving |
| `warehouse` | Warehouse | Inventory, transfers, movement history |

31 tables across the 7 schemas, all with `AuditMixin` (`created_at`, `updated_at`, `is_active`, `row_guid`).

---

## Repository Structure

```
nexomart/
├── dbapi/                  # Ingestion API (FastAPI) + seeders
│   └── api/
│       ├── models/         # SQLAlchemy models per domain
│       ├── schemas/        # Pydantic schemas
│       ├── repositories/   # Data access layer (bulk insert)
│       ├── services/       # Business logic
│       ├── routers/        # FastAPI endpoints
│       ├── seeder/         # Data generators with chaos engineering
│       │   └── ingestions/ # Postgres → landing (incremental)
│       ├── samples/        # Reference data per domain
│       └── utils/          # Shared modules (cli_common, mutator_common, quarantine)
├── airflow/
│   ├── dags/
│   │   ├── nexomart/       # seeder, mutator, ingestion, streaming DAGs
│   │   └── advworks/       # legacy AdventureWorks DAGs (to be removed)
│   └── dockerfile          # Astro Runtime 11.4 with Spark 3.5.8 embedded
├── spark/
│   ├── src/                # Reusable Spark framework
│   │   ├── common/         # reader, writer, delta helpers, validators, logger
│   │   ├── transformers/   # composable DataFrame transformers
│   │   └── domains/        # per-layer, per-domain logic (bronze/, silver/)
│   ├── jobs/               # Spark entrypoints (advworks/ is legacy)
│   └── conf/
│       └── spark-defaults.conf.template  # generated via envsubst
├── nexomart_contract/      # Data contracts (api / raw / silver / gold) in YAML
├── tests/
│   ├── contracts/          # Contract vs Pydantic vs SQLAlchemy vs live DB drift tests
│   └── seeder/             # Seeder and quarantine tests
├── dbt/                    # dbt-trino project (Gold marts)
├── postgres/               # PostgreSQL 16 + init scripts
├── minio/                  # S3-compatible object storage + bucket provisioning
├── hive/                   # Hive Metastore 4.0 (PostgreSQL backend)
├── trino/
│   └── catalog/            # nexomart, nexo, postgres (+ legacy iceberglake, minio)
├── configs/
│   ├── secrets.env         # Credentials (not committed; mirrored in secrets.env.example)
│   ├── seeders/            # Per-run volume and chaos ranges (single source of truth)
│   ├── postgresql/         # GUC tuning notes + partitioning migrations
│   └── profiles.yml        # dbt profile (Trino)
├── scripts/                # lint, bind-mount staging, Windows disk-mount / maintenance tasks
├── docs/                   # Design notes, postmortems, guides
└── Makefile                # Stack operation commands
```

---

## Prerequisites

- Docker Desktop (WSL2 on Windows)
- Docker Compose v2
- Make
- Python 3.12+ with [uv](https://github.com/astral-sh/uv)

---

## Quick Start

### First time

```bash
# 1. Clone
git clone git@github.com:dougdss89/nexomart.git
cd nexomart

# 2. Set up credentials (never committed)
cp configs/secrets.env.example configs/secrets.env
# edit secrets.env with real values

# 3. Create network, volumes, and build images
make setup

# 4. Bring up the whole stack (generates configs and starts services in order)
make up
```

### Day to day

```bash
make up          # brings everything up (creates containers)
make down        # stops AND REMOVES all containers in the stack
make stop        # just STOPS containers (no removal) — faster to resume
make start       # restarts containers stopped with `make stop` (no recreation)
make start-dev   # only the essentials: postgres, minio, hive, airflow (no spark/trino/dbapi)
make start WORKERS=3         # same as start, but scales spark-worker to 3 replicas
make scale-spark WORKERS=3   # scale spark-worker up/down without touching anything else
make ps          # lists running containers with status and ports
make logs s=trino-coordinator   # tail logs for one container
make restart s=spark-master     # restart one container
make clean       # ⚠️  removes containers AND volumes (destroys data)
```

`make down` removes containers, but it **does not** delete data. Postgres, `dockervol` and the Spark caches live in Docker volumes, and MinIO data lives on a dedicated disk bind-mounted at `MINIO_DATA_DIR` (see Known Issues #7 and #8). These are only removed with an explicit `-v`/`--volumes` flag, which only `clean` uses (intentionally destructive). Use `make down`/`make up` after editing a `docker-compose.yml`; use `make stop`/`make start` for day-to-day pauses.

`spark-worker` is sized at 4 cores / 8GB per replica (`SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY`/`SPARK_WORKER_MEM_LIMIT` in `configs/secrets.env`) so it can scale to 3 replicas (12 cores / 24GB) without starving the rest of the stack. `WORKERS` defaults to `1`; scale out for heavy jobs, then back down (`make scale-spark WORKERS=1`). Both scale targets use `up -d --scale`, so they are safe to call at any time.

### Individual services

```bash
make up-postgres
make up-minio
make up-hive
make up-spark
make up-trino
make up-airflow
make up-dbapi
```

### Quality gates

```bash
make lint        # ruff, black, isort, flake8, sqlfluff
make test        # pytest
make ci          # lint + test + security, same as the GitHub Actions pipeline
```

---

## MinIO Buckets

Buckets are provisioned automatically by the `minio-init` one-shot container (`minio/docker-compose.yml`) every time MinIO comes up. It runs `mc mb --ignore-existing`, so it is idempotent.

- **`spark-events`** is hardcoded in `minio-init`: the Spark History Server reads `spark.eventLog.dir=s3a://spark-events/apps` and fails on boot without it.
- **Everything else comes from `MINIO_EXTRA_BUCKETS`** in `configs/secrets.env` (comma-separated, no spaces):

  ```bash
  MINIO_EXTRA_BUCKETS=landing,bronze,silver,iceberg,deltadw,quarantine
  ```

  `quarantine` holds the rows that seeders reject at validation time, as Parquet with a `payload` column (see [`docs/MINIO.md`](docs/MINIO.md)). `iceberg` is a leftover from the legacy pipeline.

  To add a bucket, append its name to this variable in `secrets.env` **and** `secrets.env.example`, then run `make up-minio`. No code or compose change is needed.

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | per `secrets.env` |
| Spark Master UI | http://localhost:9090 | — |
| Spark History Server | http://localhost:18080 | — |
| DataFlint (live, during a job) | http://localhost:4040/dataflint | — |
| Trino UI | http://localhost:8070 | — |
| Airflow UI | http://localhost:8181 | per `secrets.env` |
| dbapi (FastAPI) | http://localhost:8001 | — |
| dbapi Docs | http://localhost:8001/docs | — |

---

## NexoMart Pipeline (Airflow)

Four DAGs under `airflow/dags/nexomart/` simulate a live OLTP workload against `nexomart_db` and land it in the lake:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `nexomart_seeder_dag` | every 15 minutes (`*/15 * * * *`) | Inserts new rows across all 7 domains, with chaos engineering (see [Chaos Engineering](#chaos-engineering)) |
| `nexomart_mutator_dag` | hourly (`0 * * * *`) | Applies UPDATE and, where safe, DELETE to rows the seeder already inserted, so a future CDC pipeline gets realistic update/delete events |
| `nexomart_ingestion_dag` | none (triggered by the seeder, or by hand) | Reads from `nexomart_db` and lands raw Parquet into MinIO's `landing` bucket, one file per table per run. Supports a backfill mode |
| `nexomart_streaming_dag` | `@continuous`, created paused | Low-volume, back-to-back variant of the seeder for streaming studies. Pause `nexomart_seeder_dag` while it runs |

**Concurrency.** The seeder and mutator use `max_active_runs=1`, so a slow run never overlaps the next tick. The seeder also caps `max_active_tasks=8` to avoid disk I/O contention.

**Contract gate.** The seeder starts with a `contract_drift_check` task that runs `tests/contracts` (data contract YAML vs Pydantic schema vs SQLAlchemy model vs live Postgres). Any drift blocks the run before data is written.

**Seeder → ingestion coupling.** The seeder ends with a `trigger_ingestion` task using `wait_for_completion=True`, so the next seeder tick cannot start until ingestion for the current one has finished.

**Incremental ingestion.** Ingestion reads only rows that are new or changed since the last successful run, with a `COALESCE(updated_at, created_at) > watermark` filter per table. Watermarks live in `ingestion_control.watermarks` inside `nexomart_db`. Details and the bugs found building it: [`docs/INGESTION_INCREMENTAL.md`](docs/INGESTION_INCREMENTAL.md).

**Historical depth.** Dimension seeders backdate `created_at` to a random point in `[2020-01-01, now]` (and set `updated_at = now()` so the watermark still picks the row up). Fact tables partition by their own event date and are never backdated. This gives the lake deep history to join facts against.

**Mutator safety.** The mutator never hard-deletes a row that is still a foreign-key target, nor the two fan-out parents (`person.customer`, `purchasing.purchaseorderheader`). Those get a soft delete (`is_active = false`). Postmortem: [`docs/MUTATOR_DAG_FIXES.md`](docs/MUTATOR_DAG_FIXES.md).

**Volumes.** Per-run volumes are sized for a 10-15 minute cadence (not a daily cron) and live in `configs/seeders/config.py`. Analysis: [`docs/SEEDER_MICROBATCHING.md`](docs/SEEDER_MICROBATCHING.md).

`SEEDER_ROOT` and `UV_PROJECT` (paths each DAG uses to shell out to a seeder via `uv run`) come from `configs/secrets.env`, not from the DAG files.

```bash
# Trigger by hand
docker exec airflow_scheduler airflow dags trigger nexomart_ingestion_dag
```

---

## Spark Framework

`spark/src/` is a small reusable framework so each Bronze/Silver job is mostly composition:

| Module | Role |
|--------|------|
| `common/reader.py` | Reads landing/Bronze/Silver sources |
| `common/writer.py` | `DeltaTableWriter` (append) and `DeltaMergeWriter` (MERGE on natural key), with partition support |
| `common/delta.py` | Table naming helpers and Delta write patterns |
| `common/column_validator.py`, `table_validator.py` | Fail early when expected columns are missing |
| `common/transformer.py` | Base class for composable transformers |
| `transformers/` | `parseJson`, `flatten`, `explode`, `email` flag, `splitname`, `extractsize`, `fillnullvalues`, `normalizegender`, `normalizemaritalstatus`, `tofloat`, `totimestamp`, `tolower`, `audit`, `metadata` |
| `domains/<layer>/<schema>/` | Per-domain logic (started: `bronze/person`, `silver/sales`) |

### Spark Query Monitoring (DataFlint)

DataFlint (JAR baked into the Spark images) adds visual query-plan analysis.

- **Live** (while a job runs): `http://localhost:4040/dataflint`
- **History** (completed jobs): `http://localhost:18080` → click an app → "DataFlint" tab

```bash
# Interactive PySpark (a standing driver for exploration)
docker exec -it airflow_worker /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=172.18.0.12 \
  --conf spark.driver.bindAddress=0.0.0.0
```

---

## Critical Stack Configuration

### spark-defaults.conf

Generated by `make up` via `envsubst` from the template. **Never edit the `.conf` directly**: edit `spark/conf/spark-defaults.conf.template` and regenerate.

```bash
make generate-confs
```

Key settings:

```properties
# External metastore (without this, Spark falls back to local Derby)
spark.sql.catalogImplementation      hive
spark.sql.warehouse.dir              s3a://deltadw/warehouse/
spark.hadoop.hive.metastore.uris     thrift://hive-metastore:9083

# Delta Lake as the default catalog
spark.sql.extensions                 io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog      org.apache.spark.sql.delta.catalog.DeltaCatalog

# MinIO
spark.hadoop.fs.s3a.endpoint         http://minio:9000
```

Credentials and endpoints come from `configs/secrets.env`. Nothing is hardcoded in config files (see "Zero hardcode" in `CLAUDE.md`).

### dbt

The dbt project lives in `dbt/` and uses the profile in `configs/profiles.yml` (Trino, catalog `nexo`, schema `gold`). Models are not written yet. Marts that serve BI override `database: postgres` to materialize in the `golddw` serving database via cross-catalog CTAS.

### PostgreSQL tuning, partitioning and retention

`pgprojeto` runs with non-default GUCs (memory, WAL and I/O tuned for the container limits and the WSL2 virtual disk), set through `postgres/docker-compose.yml` and `configs/secrets.env` so they survive container recreation. Five of the largest tables (`person.customer`, `sales.salesorder`, `warehouse.transactionhistory`, `purchasing.purchaseorderheader`, `marketing.dailyperformance`) are natively partitioned. Partition migrations live in `configs/postgresql/migrations/`. A 45-day retention policy plus a nightly offline VHD compaction keep the data disk bounded. Full detail: [`configs/postgresql/README.md`](configs/postgresql/README.md).

---

## Known Issues and Fixes

### 1. Hive Metastore schema incomplete (`HIVE_LOCKS` does not exist)

**Symptom:** a lock call against the metastore fails with `TApplicationException: Internal error processing lock`, and the metastore log shows `relation "HIVE_LOCKS" does not exist`.

**Cause:** `hive/docker-compose.yml` had `IS_RESUME: "true"`, which makes the entrypoint skip `schematool` on every boot. With `datanucleus.schema.autoCreateAll=true`, the metastore self-provisioned only ORM-mapped tables and never the raw-SQL ones (`TXNS`, `HIVE_LOCKS`, `WM_*`). The `VERSION` table stayed empty.

**Fix:** `IS_RESUME` is now `"false"`, so `schematool -initOrUpgradeSchema` runs on every boot (idempotent). On an already-broken environment, recreate the `metastore` database once:

```bash
docker stop hive-metastore
docker exec pgprojeto psql -U admin -d postgres -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='metastore' AND pid <> pg_backend_pid();"
docker exec pgprojeto psql -U admin -d postgres -c "DROP DATABASE metastore;"
docker exec pgprojeto psql -U admin -d postgres -c "CREATE DATABASE metastore;"
docker compose --env-file configs/secrets.env -f hive/docker-compose.yml up -d --force-recreate
```

Check: `SELECT * FROM "VERSION"` returns one row and `\dt` lists `TXNS`/`HIVE_LOCKS`/`WM_*`.

---

### 2. Trino: `HIVE_CANNOT_OPEN_SPLIT` after recreating a Delta table

**Symptom:** `SELECT *` fails with `Error opening Hive split`; `SELECT count(*)` works.

**Cause:** Trino caches `_delta_log/00000000000000000000.json`. If a table is dropped and recreated at the same path, the Delta log restarts at version 0 but Trino serves the old cached content.

**Fix:**

```bash
# Without restart:
docker exec trino-coordinator trino \
  --execute "CALL nexomart.system.flush_metadata_cache(schema_name => 'nexomart_silver', table_name => '<table>')" \
  --server http://localhost:8080 --user admin

# Or restart Trino:
docker restart trino-coordinator trino-trino-worker-1
```

---

### 3. Hive Metastore 4.0: Thrift timeout on `createTable`

**Symptom:** `RetryingMetaStoreClient: MetaStoreClient lost connection. createTable` with `TTransportException: Read timed out`.

**Cause:** PostgreSQL transactions stuck `idle in transaction` block DDL on the metastore.

**Fix:**
1. `hive/conf/hive-site.xml`: `hive.metastore.server.max.threads=200`, `hive.metastore.client.socket.timeout=120`.
2. Auto-kill stuck sessions:

```sql
ALTER DATABASE metastore SET idle_in_transaction_session_timeout = '2min';
```

**Diagnosis:**

```bash
docker exec pgprojeto psql -U admin -d metastore -c "
SELECT pid, state, now() - query_start AS duration, query
FROM pg_stat_activity
WHERE datname = 'metastore' AND state = 'idle in transaction'
ORDER BY duration DESC;"
```

---

### 4. WSL2: stale bind mount after `docker restart`

**Symptom:** `docker restart <container>` fails with `OCI runtime create failed ... error mounting "/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/..."`. It can also show up as `flags: 0x1000: no such file or directory` on a fresh `up` after a Docker Desktop/WSL2 restart.

**Cause:** Docker Desktop on WSL2 uses hashes for bind mounts, and a `restart` reuses the old (now stale) hash.

**Fix:** never `docker restart` containers with WSL2 bind mounts. Remove and recreate instead:

```bash
docker rm -f <container>
docker compose --env-file configs/secrets.env -f <service>/docker-compose.yml up -d <service>
```

If the stale mount is on a named volume, remove the containers using it, `docker volume rm <volume-name>`, and let Compose recreate it.

---

### 5. Airflow worker with an empty `SQL_ALCHEMY_CONN`

**Symptom:** `airflow_worker` crashes with `FATAL: role "astro" does not exist`; `${POSTGRES_USER}`-style variables are not expanded.

**Fix:** always start through the Makefile, which passes `--env-file configs/secrets.env`:

```bash
make up-airflow
```

---

### 6. MinIO unreachable on `localhost:9000`/`:9001` while the container is healthy

**Symptom:** `docker ps` shows MinIO `Up (healthy)` but `curl localhost:9000` times out. `docker inspect` shows empty `NetworkSettings.Ports`.

**Cause:** either a Docker Desktop/WSL2 network hiccup that left port forwarding unreconciled, or a host process already owning the port. Check the second first: `ss -tlnp | grep -E ':9000|:9001'`.

**Fix:**

```bash
docker compose --env-file configs/secrets.env -f minio/docker-compose.yml up -d --force-recreate minio
```

---

### 7. MinIO starts empty because the data disk was not mounted

**Symptom:** MinIO is healthy but buckets are empty or recreated by `minio-init`, and jobs find no data.

**Cause:** MinIO data lives on a dedicated disk mounted in WSL2 at `/mnt/wsl/minio-f` (`scripts/windows/mount-minio-f.ps1`). `wsl --mount` is lost whenever the WSL VM restarts. With no disk there, Docker creates an empty directory and MinIO starts on empty storage. The real data is untouched on the disk, and Docker Desktop can keep reusing a stale bind mount to the empty directory.

**Fix:**

```bash
# 1. Mount the disk from Windows (administrator PowerShell), see scripts/windows/README.md
# 2. Remove the container and the stale bind mount, then recreate it
docker rm -f minio
awk '$4=="/minio-f/data" {print $5}' /proc/self/mountinfo | while read m; do
  wsl.exe -d Ubuntu-22.04 -u root -- umount -l "$m"
done
make up-minio
# 3. Check: should list the real buckets with data
docker exec minio ls /data
```

`make up`, `make up-minio` and `make start` now stop with an error while the disk is not mounted (`make check-minio-disk`). The same guard exists for the Postgres disk (`make check-postgres-disk`), added after an incident where Postgres initialized an empty database over the unmounted disk path.

---

### 8. Late-attached disks fail to mount: `bind-mounts/<distro>/<hash> ... no such file or directory`

**Symptom:** the disk *is* mounted, but starting any container that uses a volume on `minio-f` or `postgres-ssd` fails with `failed to mount local volume ... flags: 0x1000: no such file or directory`.

**Cause:** Docker Desktop first re-exports a WSL bind source into `/mnt/wsl/docker-desktop-bind-mounts/<distro>/<sha256(source_path)>`, then bind-mounts that onto the volume. It only creates those re-exports for disks present when its WSL integration initialized. Disks attached late (`wsl --mount`) lose them after a WSL or Docker Desktop restart. Restarting Docker Desktop does not fix it.

**Fix:** the hash is deterministic, so the staging can be rebuilt:

```bash
make stage        # idempotent: stage-minio-f + stage-postgres
```

This runs automatically as a prerequisite of `make init` (every `make up` / `make up-*`) and of `make start`. Implementation: `scripts/stage-minio-f.sh`.

---

### 9. dbt: `__dbt_tmp` / `__dbt_backup` leftovers in MinIO

**Cause:** `materialized='table'` with dbt-trino uses an atomic rename; intermediate failures leave orphaned paths in S3.

**Cleanup:**

```bash
docker exec trino-coordinator trino --server http://localhost:8080 --user admin \
  --execute "DROP TABLE IF EXISTS nexo.gold.<model>__dbt_backup"
```

**Prevention:** use `materialized='view'` for marts that are direct projections.

---

## Operational Notes

### `make clean`: what's destroyed and what survives

| Resource | `make down` | `make clean` |
|----------|-------------|--------------|
| Containers | removed | removed |
| `dockervol` volume | preserved | **destroyed** |
| `spark-logs` volume | preserved | **destroyed** |
| `spark-ivy-cache` volume | preserved | **destroyed** |
| MinIO data (landing + Delta) | preserved | preserved |
| PostgreSQL data | preserved | preserved |
| `hadoopcluster` network | preserved | **destroyed** |

### Service dependency order

```
postgres → minio → hive-metastore → spark → trino → airflow → dbapi
```

### Checking Hive Metastore health

```bash
# Metastore's PostgreSQL sessions
docker exec pgprojeto psql -U admin -d metastore -c \
  "SELECT state, count(*) FROM pg_stat_activity WHERE datname='metastore' GROUP BY state;"

# Tables registered in Hive
docker exec -i pgprojeto psql -U admin -d metastore -c \
  'SELECT "TBL_NAME", "TBL_TYPE" FROM "TBLS" ORDER BY "TBL_NAME";'

# Schema sanity check (see Known Issue #1)
docker exec pgprojeto psql -U admin -d metastore -c 'SELECT * FROM "VERSION";'
```

### Checking tables in Trino

```bash
docker exec trino-coordinator trino \
  --execute "SHOW SCHEMAS IN nexomart" \
  --server http://localhost:8080 --user admin

docker exec trino-coordinator trino \
  --execute "SHOW TABLES IN nexomart.nexomart_silver" \
  --server http://localhost:8080 --user admin
```

---

## Ingestion API (dbapi)

Mandatory 3-layer architecture for every domain:

```
Router (FastAPI) → Service (business rules) → Repository (SQLAlchemy bulk insert)
```

In practice the runtime is batch/CLI: Airflow invokes seeders through `BashOperator` with `uv run`. The API layer exists in code and demonstrates the pattern, but it is not run as a persistent online service.

```bash
# Run locally
cd dbapi && uv run python api/app/main.py

# Run a seeder
uv run python dbapi/api/seeder/<domain>/<file>.py \
  --min-count 1000 --max-count 50000 \
  --min-chaos 0.05  --max-chaos 0.10

# Run tests
cd dbapi && uv run python api/tests/test_product_services.py
```

---

## Data Contracts

`nexomart_contract/` holds one YAML contract per table per stage (`api_contract/`, `raw_contract/`, `silver_contract/`, `gold_contract/`). Contracts record what the physical schema cannot: owner, PII, quality rules, refresh cadence, breaking-change policy. `person.customer` is filled in at all four stages as the reference example. `tests/contracts/` keeps them honest by checking contract vs Pydantic vs SQLAlchemy vs live Postgres. Details: [`nexomart_contract/README.md`](nexomart_contract/README.md).

---

## Chaos Engineering

Seeders inject controlled anomalies on every run, with a rate drawn per run between **5% and 10%** (per-schema maximum in `configs/seeders/config.py`):

| Type | Examples |
|------|----------|
| Duplicates | Same record with formatting variations |
| Dirty data | Invalid CPF, email without TLD, `0000000` phone numbers |
| Fraud | Geographically impossible purchases, fake CPF with a real address |
| Incomplete processes | Orders paid but never shipped, open chargeback disputes |
| Inventory anomalies | Negative quantities, expired products in active stock |

The anomalies are intentional and documented so the Silver layer can detect and fix them. Full catalog per table: [`docs/CHAOS_DOCUMENTATION.md`](docs/CHAOS_DOCUMENTATION.md).

---

## Dimensional Model (planned Gold)

### Bus Matrix

| Fact | date | customer | product | seller | channel | warehouse | employee | vendor | special_offer |
|------|:----:|:--------:|:-------:|:------:|:-------:|:---------:|:--------:|:------:|:-------------:|
| `fact_sales_order_item` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — | ✓ |
| `fact_inventory_snapshot` | ✓ | — | ✓ | — | — | ✓ | — | — | — |
| `fact_marketing_performance` | ✓ | — | — | — | ✓ | — | — | — | — |
| `fact_purchase_order_line` | ✓ | — | ✓ | — | — | — | ✓ | ✓ | — |
| `fact_order_lifecycle` | ✓ | ✓ | — | ✓ | ✓ | — | — | — | — |

---

## Transactional ID Prefixes

| Domain | Header | Line |
|--------|--------|------|
| Sales | `SO-XXXXXXXXX` | `SO-XXXXXXXXX-XX` |
| Purchasing | `PO-XXXXXXXXX` | `PO-XXXXXXXXX-XX` |
| Warehouse | `TR-XXXXXXXXX` | `TR-XXXXXXXXX-XX` |
| Returns | `RO-XXXXXXXXX` | — |

---

## Full Stack

| Component | Technology | Version |
|-----------|------------|---------|
| Ingestion API | FastAPI + asyncpg | — |
| ORM | SQLAlchemy | 2.0 |
| Validation | Pydantic | 2.x |
| OLTP Database | PostgreSQL | 16 |
| Semantic Search | pgvector | — |
| Object Storage | MinIO | latest |
| Orchestration | Apache Airflow (Astro Runtime) | 2.9 |
| Processing | Apache Spark | 3.5.8 |
| Table Format (all layers) | Delta Lake | 3.2.1 |
| Metastore | Hive Metastore | 4.0.0 |
| SQL Federation | Apache Trino | 482 |
| Transformations | dbt-core / dbt-trino | 1.11 / 1.10 |
| Spark Observability | DataFlint | 0.9.9 |
| Package / env manager | uv | — |
| CI | GitHub Actions | — |

---

## Roadmap / Release Checklist

Release numbers are a working plan, not git tags. Check items off as they land.

### Shipped

- [x] **v0.1 — Platform foundation:** Docker stack (Postgres, MinIO, Hive Metastore, Spark, Trino, Airflow, dbapi) on a shared network, `make` targets, secrets-driven config with zero hardcoded endpoints
- [x] **v0.2 — Synthetic OLTP:** 7 domains, 31 tables, 3-layer dbapi, seeders with dynamic volume ranges and 5-10% chaos engineering, documented anomaly catalog
- [x] **v0.3 — Orchestration and landing zone:** seeder, mutator, ingestion and streaming DAGs; watermark-based incremental ingestion to `s3a://landing/`; dimension history spread 2020→now; quarantine bucket
- [x] **v0.4 — Data safety and tuning:** Postgres GUC tuning, partitioning, 45-day retention, nightly VHD compaction, disk-mount guards, bind-mount staging
- [x] **v0.5 — Contracts and CI:** YAML data contracts, contract drift gate in the seeder DAG, GitHub Actions CI/CD
- [x] **v0.6 — Spark framework (Delta):** reader/writer/validators, Delta partitioned and MERGE writers, composable transformers

### Next

- [ ] **v0.7 — Bronze layer (NexoMart)**
  - [ ] Bronze job per table, landing → Delta, with audit/metadata columns (`person.customer` started)
  - [ ] All 7 schemas covered
  - [ ] Airflow DAG `nexomart_bronze_dag` triggered after ingestion
  - [ ] Bronze contracts validated against landing
- [ ] **v0.8 — Silver layer and data quality**
  - [ ] Great Expectations suites built from the rules in `docs/CHAOS_DOCUMENTATION.md`
  - [ ] Quarantine tables (`dq_rule_id`, `dq_failure_reason`, `quarantined_at`)
  - [ ] Correction job: `auto_fix` vs `flag_only` policy, MERGE on natural key, rolling-window backfill
  - [ ] Prototype end to end on `marketing`, then generalize to the other 6 domains
  - [ ] Silver contracts promoted from intent to enforced
- [ ] **v0.9 — Gold and dbt**
  - [ ] Dimensions and facts from the bus matrix
  - [ ] dbt models and tests on `nexomart` / `nexo` catalogs
  - [ ] Serving layer in Postgres `golddw` for BI
  - [ ] Gold contracts enforced
  - [ ] Decide the post-Gold path: Trino → Postgres → Power BI, or Power BI direct on Gold
- [ ] **v0.10 — Cleanup and hardening**
  - [ ] Remove legacy Iceberg: `iceberglake`/`minio` Trino catalogs, Iceberg JAR and catalog in `spark-defaults.conf.template`, `iceberg` bucket, `common/iceberg.py`
  - [ ] Remove or archive the `advworks` DAGs and Spark jobs
  - [ ] Apply Postgres partitioning durably (re-check `pg_class.relkind` after any `create_tables.py` run)
  - [ ] Point the git remote at the renamed `nexomart` repo and update CI paths (`cd data_plataform` in workflows)
  - [ ] Automated tests for Spark transformers
- [ ] **v1.0 — End-to-end release:** one command from seed to Gold, every layer under contract and DAG-orchestrated, runbook and architecture diagrams refreshed

### Later

- [ ] **v1.1 — Marketplace:** PJ sellers as a new dimension across sales and product
- [ ] **v1.2 — NexoBank base:** KYC, digital account, ledger, PIX, cards
- [ ] **v1.3 — Credit:** consumer credit and seller working capital, with scores computed in Spark and written back to Postgres (reverse ETL)
- [ ] **v1.4 — Investments:** CDB/LCI/Treasury/funds, daily yield computed in Spark, CDI/Selic/IPCA from the Banco Central public API
- [ ] **v1.5 — AI/ML:** demand and inventory forecasting first, then semantic search/RAG over the product catalog ([`docs/AI_ML_ROADMAP.md`](docs/AI_ML_ROADMAP.md))
- [ ] **Backlog:** CDC/Kafka with schema registry and outbox pattern, OpenTelemetry observability, OpenLineage in DAGs, Alembic migrations ([`docs/ENGINEERING_DEBATE_AND_ROADMAP.md`](docs/ENGINEERING_DEBATE_AND_ROADMAP.md))

---

## Documentation

| File | Description |
|------|-------------|
| [`docs/nexomart_requisitos.md`](docs/nexomart_requisitos.md) | NexoMart functional and non-functional requirements |
| [`docs/AIRFLOW.md`](docs/AIRFLOW.md) | Airflow DAGs, schedules, and operational notes |
| [`docs/MINIO.md`](docs/MINIO.md) | MinIO buckets, provisioning, and client configuration |
| [`docs/INGESTION_INCREMENTAL.md`](docs/INGESTION_INCREMENTAL.md) | Watermark-based incremental ingestion, and the bugs found building it |
| [`docs/SEEDER_MICROBATCHING.md`](docs/SEEDER_MICROBATCHING.md) | Rescaling the seeders and their DAG for a 10-15 minute cadence |
| [`docs/MUTATOR_DAG_FIXES.md`](docs/MUTATOR_DAG_FIXES.md) | Bugs found and fixed in the UPDATE/DELETE mutation DAG |
| [`docs/CHAOS_DOCUMENTATION.md`](docs/CHAOS_DOCUMENTATION.md) | Every anomaly type injected by the seeders, per table |
| [`docs/MIGRATION_GUIDE.md`](docs/MIGRATION_GUIDE.md) | Migrating a seeder to the chaos engineering + volume limits pattern |
| [`docs/DYNAMIC_RANDOMIZATION_GUIDE.md`](docs/DYNAMIC_RANDOMIZATION_GUIDE.md) | Using dynamic volume and chaos-rate ranges in a seeder |
| [`docs/INCIDENTS.md`](docs/INCIDENTS.md) | Postmortems for bugs found in production-ish operation |
| [`docs/CICD_SETUP.md`](docs/CICD_SETUP.md) | GitHub Actions CI/CD pipeline setup |
| [`docs/ENGINEERING_DEBATE_AND_ROADMAP.md`](docs/ENGINEERING_DEBATE_AND_ROADMAP.md) | Engineering decisions and technical roadmap |
| [`docs/AI_ML_ROADMAP.md`](docs/AI_ML_ROADMAP.md) | AI/ML use-case roadmap (demand forecasting, semantic search) |
| [`docs/walkthrough.md`](docs/walkthrough.md) | Implementation walkthrough for the product domain |
| [`configs/postgresql/README.md`](configs/postgresql/README.md) | PostgreSQL GUC tuning and table partitioning notes |
| [`nexomart_contract/README.md`](nexomart_contract/README.md) | Data contract layout and drift tests |
