# Data Platform — IcebergLake

Open-source data platform built from scratch for the **IcebergLake** case study — a multi-channel Brazilian marketplace processing between 100 thousand and 50 million orders per cycle. The goal is to separate the transactional layer (OLTP) from the analytical layer (OLAP) without depending on any proprietary cloud.

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
│  Landing → Bronze → Silver (Delta Lake) → Gold (Iceberg)     │
├──────────────────────────────────────────────────────────────┤
│                   ANALYTICAL STORAGE                         │
│       Delta Lake + Apache Iceberg + MinIO (S3-compatible)    │
├──────────────────────────────────────────────────────────────┤
│                  CATALOG & METASTORE                         │
│    Hive Metastore 4.0 (PostgreSQL) + Unity Catalog OSS       │
├──────────────────────────────────────────────────────────────┤
│                   QUERY FEDERATION                           │
│    Apache Trino 482 (delta_lake + iceberg + nexo catalogs)   │
├──────────────────────────────────────────────────────────────┤
│                 ANALYTICAL TRANSFORMATIONS                   │
│                  dbt-trino 1.10 / dbt-core 1.11               │
└──────────────────────────────────────────────────────────────┘
```

### Data Pipeline (Medallion Architecture)

```
PostgreSQL (OLTP)
      │
      ▼
 Landing  (raw Parquet,   s3a://landing/)
      │  spark/jobs/landing_layer_scripts/
      ▼
 Bronze   (Delta Lake,     s3a://bronze/)
      │  spark/jobs/raw_layer_extract_scripts/
      ▼
 Silver   (Delta Lake + surrogate keys, s3a://silver/)
      │  spark/jobs/silver_layer_advworks_scripts/
      ▼
 Gold     (Apache Iceberg, s3a://deltadw/iceberglake/)
      │  spark/jobs/gold_layer_scripts/
      │  catalog: iceberglake (HiveCatalog → HMS)
      ▼
 Trino ──► dbt (analytical views/marts in the nexo.gold catalog)
```

**Format per layer:**

| Layer | Format | Spark Catalog | Trino Catalog |
|-------|--------|----------------|----------------|
| Landing | Raw Parquet | — | `minio` |
| Bronze | Delta Lake | `spark_catalog` | `delta` |
| Silver | Delta Lake | `spark_catalog` | `delta` |
| Gold | Apache Iceberg | `iceberglake` | `nexo` |
| dbt marts | Trino views | — | `nexo` |

---

## Catalogs

### Spark — 3 simultaneous catalogs

| Catalog | Implementation | Use |
|---------|-----------------|-----|
| `spark_catalog` | DeltaCatalog (default) | Landing, Bronze, Silver in Delta Lake |
| `iceberglake` | IcebergHiveCatalog (HMS) | Gold writes in Iceberg |
| `nexo` | IcebergRESTCatalog (UC OSS) | Reads via Unity Catalog |

```python
# Gold — Iceberg write via HiveCatalog
df.writeTo("iceberglake.gold.gold_dim_productcategory").createOrReplace()

# Read via Unity Catalog REST
spark.sql("SELECT * FROM nexo.gold.gold_dim_productcategory")
```

### Trino — 4 catalogs

| Catalog | Connector | Use |
|---------|-----------|-----|
| `delta` | `delta_lake` + HMS | Bronze and Silver |
| `iceberglake` | `iceberg` + HMS | Gold direct access (ops/debug) |
| `nexo` | `iceberg` + UC REST | Gold (reads and dbt) — official read path |
| `minio` | `delta_lake` + HMS | Landing / raw exploration |

```sql
-- Query Gold via Trino
SELECT * FROM nexo.gold.gold_dim_productcategory LIMIT 10;
SHOW TABLES IN nexo.gold;
```

### Unity Catalog OSS — via REST API

The UC OSS v0.4.0 GUI requires OAuth (Google/OIDC) and doesn't work without a configured provider. Use the REST API or the `nexo` catalog in Spark/Trino instead:

```bash
# Catalogs
curl -s http://localhost:8182/api/2.1/unity-catalog/catalogs | python3 -m json.tool

# Schemas
curl -s "http://localhost:8182/api/2.1/unity-catalog/schemas?catalog_name=unity" | python3 -m json.tool

# Tables
curl -s "http://localhost:8182/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=gold" | python3 -m json.tool
```

---

## Business Domains

| Schema | Domain | Description |
|--------|--------|-------------|
| `product` | Product | Catalog, models, categories, semantic search via pgvector |
| `person` | Person | Customers, sellers, addresses, contacts |
| `sales` | Sales | Orders, line items (multi-seller JSONB), returns, payments |
| `marketing` | Marketing | Campaigns, ads, per-channel performance (4 levels) |
| `humanresources` | HR | Employees, departments, job history (SCD Type 2) |
| `purchasing` | Purchasing | Purchase orders, vendors, receiving |
| `warehouse` | Warehouse | Inventory, transfers, movement history |

---

## Repository Structure

```
data_plataform/
├── dbapi/                  # Ingestion API (FastAPI)
│   └── api/
│       ├── models/         # SQLAlchemy models per domain
│       ├── schemas/        # Pydantic schemas
│       ├── repositories/   # Data access layer (bulk insert)
│       ├── services/       # Business logic
│       ├── routers/        # FastAPI endpoints
│       ├── seeder/         # Data generators with chaos engineering
│       └── samples/        # Reference data per domain
├── postgres/               # PostgreSQL 16 + init scripts
├── minio/                  # S3-compatible object storage
├── airflow/
│   ├── dags/               # Airflow DAGs
│   └── dockerfile          # Astro Runtime 11.4 with Spark 3.5.8 embedded
├── spark/
│   ├── jobs/               # PySpark scripts per layer
│   │   ├── landing_layer_scripts/
│   │   ├── raw_layer_extract_scripts/    # bronze
│   │   ├── silver_layer_advworks_scripts/
│   │   └── gold_layer_scripts/           # Iceberg via iceberglake
│   └── conf/
│       └── spark-defaults.conf.template  # generated via envsubst
├── hive/                   # Hive Metastore 4.0 (PostgreSQL backend)
├── trino/
│   └── catalog/             # delta.properties, iceberglake.properties, nexomart.properties, nexo.properties, minio.properties
├── unitycatalog/           # Unity Catalog OSS (REST API + Iceberg REST catalog)
├── configurations/
│   ├── .env                # Job paths, MinIO buckets, connections
│   ├── secrets.env         # Credentials (not committed)
│   ├── profiles.yml        # dbt target: nexo.gold via Trino
│   ├── dbt/                # dbt-trino project (gold models)
│   └── configconnections_loader.py
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
# 1. Set up credentials (never committed)
cp configurations/secrets.env.example configurations/secrets.env
# edit secrets.env with real values

# 2. Create network, volumes, and build images
make setup

# 3. Bring up the whole stack (generates configs and starts services in order)
make up
```

### Day to day

```bash
make up          # brings everything up (creates containers)
make down        # stops AND REMOVES all containers in the stack
make stop        # just STOPS containers (no removal) — faster to resume
make start       # restarts containers stopped with `make stop` (no recreation)
make start WORKERS=3   # same, but scales spark-worker to 3 replicas as it restarts
make scale-spark WORKERS=3   # scale spark-worker up/down without touching anything else
make ps          # lists running containers with status and ports
make logs s=trino-coordinator   # tail logs for one container
make restart s=spark-master     # restart one container
make clean       # ⚠️  removes containers AND volumes (destroys data)
```

`make down` removes containers, but it **does not** delete data — everything that matters (Postgres, MinIO, `dockervol`, Spark caches) lives in named Docker volumes (`postgres_pgprojeto_data`, `minio_minio_data`, etc.), which are only removed with an explicit `-v`/`--volumes` flag — something no target other than `clean` (⚠️ intentionally destructive) does. Use `make down`/`make up` when you edit a `docker-compose.yml` (the container needs to be recreated to pick up the change); use `make stop`/`make start` for day-to-day work (e.g. pausing before shutting down your machine) — faster to resume, same data-preservation guarantee.

`spark-worker` (`spark/docker-compose.yml`) is sized at 4 cores / 8GB per replica (`SPARK_WORKER_CORES`/`SPARK_WORKER_MEMORY`/`SPARK_WORKER_MEM_LIMIT` in `configurations/secrets.env`) specifically so it can scale to 3 replicas — 12 cores / 24GB total — without starving the rest of the stack. `WORKERS` defaults to `1` for normal day-to-day use; pass `WORKERS=N` to `make start` or `make scale-spark` to scale out for heavier jobs, then scale back down (`make scale-spark WORKERS=1`) when done. `docker compose start` can't create new replicas (it only restarts containers that already exist), so both targets use `up -d --scale` under the hood instead — which also means they're safe to call any time, not just after a `make stop`.

### Individual services

```bash
make up-postgres
make up-minio
make up-hive
make up-unitycatalog
make up-spark
make up-trino
make up-airflow
make up-dbapi
```

---

## MinIO Buckets

Buckets are provisioned automatically by the `minio-init` service (`minio/docker-compose.yml`) every time MinIO comes up (`make up` or `make up-minio`) — there's no need to create them manually. It's a one-shot container that runs `mc mb --ignore-existing` for each bucket and exits (same pattern as `airflow-init`); idempotent, so running it again against buckets that already exist doesn't error.

- **`spark-events`** is mandatory and hardcoded in `minio-init` (it does not live in the list below): the Spark History Server reads `spark.eventLog.dir=s3a://spark-events/apps` (`spark/conf/spark-defaults.conf.template`) and fails on boot if the bucket/prefix doesn't exist.
- **Everything else comes from `MINIO_EXTRA_BUCKETS`** in `configurations/secrets.env` — a comma-separated list, no spaces. Currently:

  ```bash
  MINIO_EXTRA_BUCKETS=landing,bronze,silver,iceberg,deltadw
  ```

  When adding a new pipeline or layer that writes to a bucket that isn't provisioned yet (e.g. `s3a://a-new-bucket/...` in a new Spark job), just append the name to this variable in `secrets.env` **and** in `secrets.env.example` — no code or compose file needs to change. The bucket is created automatically on the next `make up`/`make up-minio`.

```bash
# Re-provision buckets without touching the rest of the stack (e.g. after editing MINIO_EXTRA_BUCKETS)
make up-minio
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|--------------|
| MinIO Console | http://localhost:9001 | per `secrets.env` |
| Spark Master UI | http://localhost:9090 | — |
| Spark History Server | http://localhost:18080 | — |
| DataFlint (live, during a job) | http://localhost:4040/dataflint | — |
| Trino UI | http://localhost:8070 | — |
| Airflow UI | http://localhost:8181 | per `secrets.env` |
| Unity Catalog REST API | http://localhost:8182/api/2.1/unity-catalog | — |
| dbapi (FastAPI) | http://localhost:8001 | — |
| dbapi Docs | http://localhost:8001/docs | — |

---

## End-to-End Integration DAG

`integration_e2e_test` — full pipeline sequence:

```
start → seed_postgres → pg_landing → pg_bronze → pg_silver
      → pg_gold → trino_validate → dbt_run → dbt_test → end
```

`pg_gold` uses `SparkSubmitOperator` in **client mode** (driver on `airflow_worker`). The job writes to `iceberglake.gold.gold_dim_productcategory` via Iceberg + HiveCatalog.

```bash
# Trigger via CLI
docker exec airflow_scheduler airflow dags trigger integration_e2e_test

# Monitor
docker exec airflow_scheduler airflow tasks states-for-dag-run \
  integration_e2e_test <run_id>
```

---

## Spark Query Monitoring

### DataFlint — enriched execution plans

DataFlint (JAR baked into the Spark images) adds visual query-plan analysis.

**Live view** (while a job is running):
```
http://localhost:4040/dataflint
```
Port 4040 stays active while the job's driver is running on `airflow_worker`.

**History view** (completed jobs):
```
http://localhost:18080  → click an app → "DataFlint" tab
                        → "SQL / DataFrame" tab (Spark's default view)
```

**Interactive PySpark** (a standing driver for exploration):
```bash
docker exec -it airflow_worker /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=172.18.0.12 \
  --conf spark.driver.bindAddress=0.0.0.0
```
While the shell is open, `localhost:4040/dataflint` stays reachable.

---

## Critical Stack Configuration

### spark-defaults.conf

Generated by `make up` via `envsubst` from the template. **Never edit the `.conf` directly** — edit the `.template` and regenerate:

```bash
export $(grep -v '^#' configurations/secrets.env | xargs)
envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf
```

Required keys:

```properties
# External metastore (without this, Spark falls back to local Derby)
spark.sql.catalogImplementation      hive
spark.sql.warehouse.dir              s3a://deltadw/warehouse/
spark.hadoop.hive.metastore.uris     thrift://hive-metastore:9083

# Iceberg catalog via HMS (Gold writes)
spark.sql.catalog.iceberglake                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberglake.catalog-impl      org.apache.iceberg.hive.HiveCatalog
spark.sql.catalog.iceberglake.uri               thrift://hive-metastore:9083
spark.sql.catalog.iceberglake.hive.lock-enabled false

# Iceberg catalog via Unity Catalog REST (nexo reads)
spark.sql.catalog.nexo                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nexo.catalog-impl      org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.nexo.uri               http://unitycatalog:8080/api/2.1/unity-catalog/iceberg

# MinIO credentials
spark.hadoop.fs.s3a.access.key       minioadmin
spark.hadoop.fs.s3a.secret.key       minioadmin
spark.hadoop.fs.s3a.endpoint         http://minio:9000
```

### dbt — Gold Models

Target: `nexo.gold` via Trino (`configurations/profiles.yml`).

```yaml
data_platform:
  target: dev
  outputs:
    dev:
      type: trino
      database: nexo
      schema: gold
```

Use `materialized='view'` for marts that are direct projections — no S3 writes, no `__dbt_tmp` / `__dbt_backup` leftovers.

Sources declared in `configurations/dbt/models/gold/sources.yml`:
- `gold` → `nexo.gold` (Iceberg tables written by Spark via iceberglake, read by Trino via nexo/UC REST)
- `gold_iceberg` → alias for future domains (buyer)

---

## Known Issues and Fixes

### 1. Hive Metastore schema incomplete: Iceberg commit locking fails with `Internal error processing lock`

**Symptom:** Spark writes to Iceberg tables (`iceberglake.*`) commit successfully but then hang or fail during the post-commit lock check, with `org.apache.thrift.TApplicationException: Internal error processing lock` on the Airflow/Spark side, and `relation "HIVE_LOCKS" does not exist` (`BadSqlGrammarException`) in the Hive Metastore's own log (`/tmp/hive/hive.log` inside the `hive-metastore` container).

**Root cause:** `hive/docker-compose.yml` had `IS_RESUME: "true"` hardcoded, which makes the base image's entrypoint skip `schematool` entirely on every boot — for Hive 4.x this should instead run `schematool -initOrUpgradeSchema`, which is idempotent (version-checks and only initializes/upgrades what's missing) and safe to run on every start. Combined with `datanucleus.schema.autoCreateAll=true` in `hive/conf/hive-site.xml`, the Metastore silently self-provisioned only the tables mapped through its ORM layer on first use, but never the raw-SQL-only tables (`TXNS`, `HIVE_LOCKS`, `WM_*`, etc.) that Iceberg's `HiveCatalog`/`MetastoreLock` needs for commit locking. The `VERSION` table stayed empty — confirming schema init had, in fact, never properly run, even though the Metastore appeared to work fine for plain catalog reads/writes.

Note that `spark.sql.catalog.iceberglake.hive.lock-enabled=false` is already set in `spark-defaults.conf.template` — but it wasn't sufficient on its own to avoid the failure; Iceberg's lock manager still attempted a `showLocks` call that failed against the incomplete schema.

**Fix:**
1. `hive/docker-compose.yml` — `IS_RESUME` set to `"false"` so `schematool -initOrUpgradeSchema` runs on every boot.
2. One-time: drop and recreate the `metastore` Postgres database, then let the container run a full `schematool -initSchema` against the clean database, producing a complete schema (confirm with `SELECT * FROM "VERSION"` returning one row, and `\dt` showing `TXNS`/`HIVE_LOCKS`/`WM_*`).

```bash
docker stop hive-metastore
docker exec pgprojeto psql -U admin -d postgres -c \
  "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='metastore' AND pid <> pg_backend_pid();"
docker exec pgprojeto psql -U admin -d postgres -c "DROP DATABASE metastore;"
docker exec pgprojeto psql -U admin -d postgres -c "CREATE DATABASE metastore;"
docker compose --env-file configurations/secrets.env -f hive/docker-compose.yml up -d --force-recreate
```

With this fix in place, a fresh environment (new `metastore` DB) will self-initialize correctly on first `make up` — this was previously a silent trap that only surfaced the first time an Iceberg write actually attempted a commit lock.

---

### 2. Trino: `HIVE_CANNOT_OPEN_SPLIT` after recreating a Gold table (Delta)

**Symptom:** `SELECT *` on the Gold Delta table fails with `Error opening Hive split`. `SELECT count(*)` succeeds.

**Cause:** Trino caches `_delta_log/00000000000000000000.json` in memory. If the table is dropped and recreated at the same path, the Delta Log restarts at version 0 (same file name) but Trino still serves the old cached content.

**Note:** The Gold layer has since migrated to Iceberg (`iceberglake`), eliminating this problem for Gold tables. Still relevant for Bronze/Silver.

**Emergency fix:**
```bash
docker restart trino-coordinator trino-trino-worker-1

# Or via procedure (no restart):
docker exec trino-coordinator trino \
  --execute "CALL delta.system.flush_metadata_cache(schema_name => 'gold', table_name => '<table>')" \
  --server http://localhost:8080 --user admin
```

---

### 3. Hive Metastore 4.0: Thrift timeout on `createTable`

**Symptom:** `RetryingMetaStoreClient: MetaStoreClient lost connection. createTable` with `TTransportException: Read timed out`. Gold-layer jobs fail after ~1 min.

**Cause:** PostgreSQL transactions stuck `idle in transaction` block DDL on the metastore.

**Fix:**
1. `hive/conf/hive-site.xml` — widened thread pool:
```xml
<property><name>hive.metastore.server.max.threads</name><value>200</value></property>
<property><name>hive.metastore.client.socket.timeout</name><value>120</value></property>
```
2. PostgreSQL — auto-kill stuck sessions:
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

**Symptom:** `docker restart <container>` fails with `OCI runtime create failed: unable to start container process: error mounting "/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/..."`.

**Cause:** Docker Desktop on WSL2 uses hashes for bind mounts; a `restart` tries to reuse the old (now-stale) hash. This isn't limited to `docker restart` — it can also surface as `error mounting ... flags: 0x1000: no such file or directory` on a fresh `up` after a Docker Desktop/WSL2 restart, for any volume using a `local` driver with `type: none, o: bind` (e.g. `spark-jobs`, `spark-configurations`).

**Fix:**
```bash
# Never use `docker restart` on containers with WSL2 bind mounts
docker rm -f <container>
docker compose --env-file configurations/secrets.env -f <service>/docker-compose.yml up -d <service>

# Example for airflow_worker (critical: loads spark-defaults.conf via bind mount)
docker rm -f airflow_worker
docker compose --env-file configurations/secrets.env -f airflow/docker-compose.yaml up -d --force-recreate airflow-worker
```

If the stale-mount error happens on a bind-mounted named volume (not a live container) instead, remove and let Compose recreate the volume itself:
```bash
docker rm -f <containers using the volume>
docker volume rm <volume-name>
docker compose --env-file configurations/secrets.env -f <service>/docker-compose.yml up -d
```

---

### 5. dbt: `Model depends on source which was not found`

**Symptom:** `dbt run` fails with `Model 'gold_mart_buyer' depends on source 'gold_iceberg.gold_dim_buyer' which was not found`.

**Cause:** dbt compiles all models before running any of them — any source not declared in `sources.yml` breaks the run even when `--select` points at a different model.

**Fix:** `gold_iceberg` declared as a source in `configurations/dbt/models/gold/sources.yml`.

---

### 6. dbt: `__dbt_tmp` and `__dbt_backup` leftovers in MinIO

**Cause:** `materialized='table'` with dbt-trino uses an atomic rename; intermediate failures leave orphaned paths in S3.

**Cleanup:**
```bash
docker exec trino-coordinator trino --server http://localhost:8080 --user admin \
  --execute "DROP TABLE IF EXISTS nexo.gold.gold_mart_productcategory__dbt_backup"
```

**Prevention:** use `materialized='view'` for marts that are direct projections.

---

### 7. Airflow worker with an empty `SQL_ALCHEMY_CONN`

**Symptom:** `airflow_worker` crashes with `FATAL: role "astro" does not exist`. `${POSTGRES_USER}`-style variables not expanded.

**Fix:** always use the Makefile:
```bash
make up-airflow   # uses: docker compose --env-file configurations/secrets.env
```

---

### 8. MinIO unreachable on `localhost:9000`/`:9001` despite the container being healthy

**Symptom:** `docker ps` shows `minio-minio-1` as `Up (healthy)`, but `curl localhost:9000`/`:9001` times out or refuses. `docker inspect minio-minio-1 --format '{{json .NetworkSettings.Ports}}'` shows empty arrays even though `HostConfig.PortBindings` correctly lists the ports.

**Cause:** this can be a genuine Docker port-publish failure after a Docker Desktop/WSL2 network hiccup (container has been running since before the hiccup and its port forwarding never got reconciled) — but check for a mundane host-level port collision first: `ss -tlnp | grep -E ':9000|:9001'` to see if something else on the host (a local dev server, another container, a Jupyter kernel, etc.) already owns those ports, which silently blocks Docker's publish without raising an error at container start.

**Fix:**
```bash
# If something else on the host owns the port, free it, then:
docker compose --env-file configurations/secrets.env -f minio/docker-compose.yml up -d --force-recreate minio
```

---

## Operational Notes

### `make clean` — what's destroyed and what survives

| Resource | `make down` | `make clean` |
|----------|-------------|--------------|
| Containers | ✓ removed | ✓ removed |
| `dockervol` volume | preserved | **destroyed** |
| `spark-logs` volume | preserved | **destroyed** |
| `spark-ivy-cache` volume | preserved | **destroyed** |
| MinIO data (Delta + Iceberg) | preserved | preserved |
| PostgreSQL data | preserved | preserved |
| `hadoopcluster` network | preserved | **destroyed** |

### Service dependency order

```
postgres → minio → hive-metastore → unitycatalog → spark → trino → airflow → dbapi
```

### Checking Hive Metastore health

```bash
# Metastore's PostgreSQL sessions
docker exec pgprojeto psql -U admin -d metastore -c \
  "SELECT state, count(*) FROM pg_stat_activity WHERE datname='metastore' GROUP BY state;"

# Tables registered in Hive
docker exec -i pgprojeto psql -U admin -d metastore -c \
  'SELECT "TBL_NAME", "TBL_TYPE" FROM "TBLS" ORDER BY "TBL_NAME";'

# Full schema sanity check — should return one row and list TXNS/HIVE_LOCKS (see Known Issue #1)
docker exec pgprojeto psql -U admin -d metastore -c 'SELECT * FROM "VERSION";'
docker exec pgprojeto psql -U admin -d metastore -c '\dt' | grep -iE "lock|txn"
```

### Checking tables in Trino

```bash
# Gold (Iceberg)
docker exec trino-coordinator trino \
  --execute "SHOW TABLES IN nexo.gold" \
  --server http://localhost:8080 --user admin

docker exec trino-coordinator trino \
  --execute "SELECT count(*) FROM nexo.gold.gold_dim_productcategory" \
  --server http://localhost:8080 --user admin

# Bronze / Silver (Delta)
docker exec trino-coordinator trino \
  --execute "SHOW TABLES IN delta.silver" \
  --server http://localhost:8080 --user admin
```

### Regenerating spark-defaults.conf

```bash
export $(grep -v '^#' configurations/secrets.env | xargs)
envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf
```

`make up` calls `generate-confs` automatically.

---

## Ingestion API (dbapi)

Mandatory 3-layer architecture for every domain:

```
Router (FastAPI) → Service (business rules) → Repository (SQLAlchemy bulk insert)
```

```bash
# Run locally
cd dbapi && uv run python api/app/main.py

# Run a seeder
uv run python dbapi/api/seeder/<domain>/<file>.py \
  --min-count 100000 --max-count 500000 \
  --min-chaos 0.01   --max-chaos 0.10

# Run tests
cd dbapi && uv run python api/tests/test_product_services.py
```

---

## Chaos Engineering

Seeders inject between **1% and 10%** of controlled anomalies per run:

| Type | Examples |
|------|----------|
| Duplicates | Same record with formatting variations |
| Dirty data | Invalid CPF, email without TLD, `0000000` phone numbers |
| Fraud | Geographically impossible purchases, fake CPF with a real address |
| Incomplete processes | Orders paid but never shipped, open chargeback disputes |
| Inventory anomalies | Negative quantities, expired products in active stock |

Full documentation: [`dbapi/api/seeder/CHAOS_DOCUMENTATION.md`](dbapi/api/seeder/CHAOS_DOCUMENTATION.md)

---

## Dimensional Model

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
| Landing/Bronze/Silver Format | Delta Lake | 3.2.1 |
| Gold Format | Apache Iceberg | 1.7.1 |
| Metastore | Hive Metastore | 4.0.0 |
| SQL Federation | Apache Trino | 482 |
| Transformations | dbt-core / dbt-trino | 1.11 / 1.10 |
| Governance | Unity Catalog OSS | 0.4.0 |
| Spark Observability | DataFlint | 0.9.9 |

---

## Documentation

| File | Description |
|------|--------------|
| [`docs/nexomart_requisitos.md`](docs/nexomart_requisitos.md) | NexoMart functional and non-functional requirements |
| [`docs/CICLO_01_CRONOGRAMA.md`](docs/CICLO_01_CRONOGRAMA.md) | Cycle 1 video schedule and scripts |
| [`docs/ENGINEERING_DEBATE_AND_ROADMAP.md`](docs/ENGINEERING_DEBATE_AND_ROADMAP.md) | Engineering decisions and technical roadmap |
| [`docs/AI_ML_ROADMAP.md`](docs/AI_ML_ROADMAP.md) | AI/ML use-case roadmap (demand forecasting, semantic search) |
| [`docs/INCIDENTS.md`](docs/INCIDENTS.md) | Postmortems for bugs found in production-ish operation |
| [`dbapi/walkthrough.md`](dbapi/walkthrough.md) | Implementation walkthrough for the product domain |
