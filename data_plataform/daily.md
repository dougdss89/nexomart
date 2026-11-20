# Daily log: NexoMart data platform

Engineering journal for the project, grouped by day. Where a day has no single
story, entries are grouped by PR or branch instead.

Sources: `git log`, `docs/INCIDENTS.md`, and
the working notes kept across sessions. Dates before 2026-01 come from commit
dates and messages only, so they carry less detail.

Last updated: 2026-11-18. Branch: `feat/person_silver`.

Legend: **Done** = shipped or verified live. **Mistake** = something that went
wrong. **Lesson** = what to do differently. **Open** = not finished.

---

## 1. Architecture at a glance

```
dbapi (FastAPI, 3 layers)  ->  Postgres 'nexomarket_db' (7 schemas, partitioned, 45d retention)
        ^                              |
        | seeders (argparse CLI)       | incremental ingestion (watermark, named cursor)
        |                              v
Airflow (Celery) DAGs  ------>  MinIO 'landing' (Parquet, partitioned by date)
 - nexomart_seeder_dag                 |
 - nexomart_mutator_dag                | Spark 4.1.1 + Delta 4.1.0
 - ingestion / streaming DAGs          v
                                Bronze -> Silver -> Gold (Delta, Hive Metastore)
                                               |
                                             Trino 483 (SQL federation)
```

- **Network:** every container sits on the shared Docker network `hadoopcluster`.
- **Catalog:** Hive Metastore only. Unity Catalog OSS was removed on 2026-10-03.
- **Config:** one source of truth, `configs/secrets.env` (mirrored in
  `secrets.env.example`). No connection string is hardcoded in compose files.
- **Storage split:** Postgres lives on its own dedicated disk (L:), MinIO on
  another (F:), so one disk failure does not take down both.

---

## 2. Design patterns in use

| Area | Pattern | Where |
|---|---|---|
| API | Repository -> Service -> Router (3 layers), bulk insert, no for-loops | `dbapi/api/{repositories,services,routers}` |
| Models | `AuditMixin` on every table (`created_at`, `updated_at`, `is_active`, `row_guid`) | `dbapi/api/config/base.py` |
| Schemas | Pydantic mirrors the SQL model 1:1; `str` enums; `@model_validator(mode='after')` | `dbapi/api/schemas` |
| Seeders | argparse CLI with ranges; FKs read from the DB; chaos injection 5-10% | `dbapi/api/seeder` |
| Shared seeder code | `cli_common`, `mutator_common`, `quarantine` | `dbapi/api/utils` |
| Spark | Decorator on Reader / Writer / Transformer (`Logging*`, `Incremental*`) | `spark/src/common` |
| Spark | Composite + Strategy: ordered list of small transformers | `spark/src/transformers` |
| Spark | Domain-rule modules (segment, type) kept apart from generic transformers | `spark/src/domains/<domain>/rules` |
| Delta writes | `DeltaMergeWriter` with merge keys + duplicate-key validator, liquid clustering | `spark/src/common/writer.py` |
| Ingestion | Watermark per table: `COALESCE(updated_at, created_at) > watermark` | `dbapi/api/seeder/ingestions` |
| Airflow | One `TaskGroup` per layer, thin `>>` edges that mirror real FKs | `airflow/dags/nexomat` |
| Guard rails | Sentinel file `.nexomart_real_disk` checked in container entrypoints | `postgres/`, `minio/` compose |
| Config | `${VAR}` in compose, `${ENV:VAR}` in Trino, `.template` + `envsubst` for Spark/Hive | `Makefile`, `configs/` |

---

## 3. Timeline

### 2023-09 to 2023-12: First iteration (AdventureWorks, Postgres, dbt)

- **Done:** built dimensions and facts on Postgres from AdventureWorks:
  production, purchasing, customer, supplier, transaction, invoices, orders.
- **Lesson:** most commits are single-dimension "checkpoint" commits. Small and
  frequent, but the messages say little. Later commits describe the change.

### 2024-03 to 2024-12: Facts, macros, raw layer, first Spark scripts

- **Done:** purchase fact, dbt templates and macros, a "training" repo, Hadoop
  files, extract scripts from AdventureWorks into a raw layer, dbt models and folders.
- **Lesson:** Hadoop as storage was dropped later in favour of MinIO.

### 2025-01 to 2025-07: Lakehouse on Spark, Airflow, MinIO

- **Done:** Iceberg table scripts, then raw DAG extraction, then migration of the
  job destination to MinIO (2025-02-14), then Dimension/Fact transformations
  (2025-02-15) and a first Silver layer (2025-03).
- **Done:** Silver DAGs with error logging (2025-04-23); first FastAPI skeleton
  (2025-07-12).
- **Mistake:** bounced between Iceberg and Delta for Bronze/Silver
  (2025-03-08/09 to Delta, 2025-04-06 back to Iceberg). The format was chosen by
  feel, not by a written decision.
- **Lesson:** write the format decision down once (it became the Delta-only
  medallion in 2026).

### 2026-01-25 to 2026-03-26: `dbapi` becomes the source of truth

- **Done:** schemas, models, `Base` optimisation, `warehouse` and `production`
  schemas (2026-02-21), `dbapi` service in compose (2026-03-10).
- **Done:** 3-layer stack (model, repository, service, seeder, router) for
  product, marketing, person, purchasing, humanresources, sales, warehouse
  (2026-03-19 to 2026-03-26).
- **Pattern:** every new area follows the same 5 steps in `CLAUDE.md`
  (model, schema, seeder, repo/service/router, CLI + Airflow).

### 2026-04-19: Chaos engineering framework

- **Done:** chaos injection for seeders (duplicates, dirty CPF/CNPJ/phones,
  outliers, fraud, phantom orders, warehouse anomalies) plus resource limits on
  containers. Catalogue in `dbapi/api/seeder/CHAOS_DOCUMENTATION.md`.
- **Lesson:** dirty data is intentional, so it has to be documented per table or
  nobody can tell a bug from a scenario.

### 2026-05-02 to 2026-05-11: Delta Lake and CI

- **Done:** migrated Iceberg to Delta, added README, SQLAlchemy index fixes,
  CI/CD pipelines and linting config.

### 2026-06-28: Stack bring-up day (many fixes, one PR-sized batch)

About 25 commits in one day, mostly startup failures found by running the full stack.

- **Done:** Hive Metastore added for the Trino Delta catalog; secrets and config
  templates; DataFlint on the Spark UI; Celery shared log folder; Redis password;
  Spark 3.5, Trino 482, Airflow 2.9; Gold layer moved Iceberg to Delta; dbt
  project initialised.
- **Mistakes (each one cost a rebuild):**
  - `nc` and `curl` do not exist in the Hive base image. Healthcheck switched to
    bash `/dev/tcp`, downloads to `wget`.
  - Postgres JDBC driver missing from the Hive image.
  - `unitycatalog-spark` and the Unity Catalog `0.4.0` image tag are not on
    Maven Central / Docker Hub.
  - `pynessie` and `pyiceberg` pulled the `hll` C extension, which failed to
    build. Fixed by `gcc` in the dbapi image and removing the packages.
  - Airflow `requirements.txt` hit `ResolutionTooDeep` (200k pip rounds).
    Fixed by cutting to a minimal set and removing the `cncf` extras (grpcio conflict).
- **Lesson:** keep Airflow requirements minimal; run heavy libraries in an
  isolated venv. Check that an image tag exists before pinning it.

### 2026-07-10 to 2026-07-24: Reorganisation

- **Done:** config and docs reorganised, chaos seeders, Spark ETL layers updated
  (2026-07-10); readability refactor (2026-07-24).

### 2026-07-12: Incident, `dim_customer` silently never written

Full postmortem in `docs/INCIDENTS.md`.

- **Symptom:** Airflow green, but `iceberglake.silver.dim_customer` did not exist.
- **Causes (3, stacked):** only 1 Spark worker instead of 3; hardcoded
  `spark.executor.memory=12g` on an 8g worker (could never schedule); Iceberg
  vectorised Parquet reader SIGSEGV, and the script swallowed the exception and
  exited 0.
- **Fix:** disable vectorisation as a table property; `exit(1)` on failure in all
  21 silver scripts; entrypoint checks `main()` result.
- **Lesson:** a green DAG proves nothing. Scripts must fail loudly with a
  non-zero exit code. Never hardcode memory that overrides `spark-defaults.conf`.
- **Open:** the other 20 dimension/fact DAGs were not re-run to check for the same hidden failure.

### 2026-08-09: Repo translated to English, study planner

- **Done:** docs and comments translated; `STUDY_PLANNER.md` and content strategy
  added. Content strategy file renamed to `docs/CONTENT_STRATEGY.md`.
- **Mistake:** an earlier content-strategy file was never saved to disk, so the
  work lived only in notes. Check the file exists before trusting a summary of it.

### 2026-08-08 to 2026-08-09: Seeders standardised + `nexomart_seeder_dag`

- **Done:** all 33 seeders got the same CLI (`--count`, `--min/max-count`,
  `--chaos-rate`); one DAG with 4 TaskGroups (master_data, dimensions, facts,
  derived) and 31 tasks.
- **Mistakes found only at real volume (not with `--count 2`):**
  - Formatted CNPJ (18 chars) overflowed `varchar(15)`.
  - asyncpg limit of 32,767 parameters per statement: batch size lowered
    10k to 1k, and all fan-out seeders moved to `insert_in_batches()`.
  - SCD2 vendor reused the same PK for history and current rows.
  - Transfer source and target location could be the same.
  - Chaos code produced values the database rejects (fake `employee_id` FK,
    `event_date` in the future, 1535-dim and NaN pgvector vectors). One failed row
    killed the whole batch.
- **Lesson:** a chaos anomaly must stay valid for the database. Keep the signal
  inside a value the schema accepts (null plus a flag in metadata, or a real FK).
- **Lesson:** test with realistic volume early. Small `--count` runs hide most bugs.
- **Pattern:** the DAG runs seeders in an isolated venv (`uv run --project`)
  because Airflow pins SQLAlchemy 1.4 and the seeders use 2.x async. `PYTHONPATH`
  must not include `configurations/`, because its `api` package shadows `dbapi/api`.

### 2026-08-17: First real DAG run, database mismatch, lost tuning

- **Done:** `nexomat_seeder_dag` ran end to end and finished `success`.
- **Mistake:** `postgres/init` created `dougstore` but the app expected
  `nexomarket_db`. Fixed the init script; `create_tables.py` now reads
  `settings.DATABASE_URL`.
- **Mistake:** Postgres tuning (GUCs) and table partitioning had been applied live
  and were lost when the container was recreated.
- **Fix:** GUCs moved into the compose `command:` driven by `PG_*` variables in
  `secrets.env`; partitioning saved as migrations `configs/postgresql/migrations/00{1..5}`.
- **Lesson:** anything applied by hand to a running container is gone on the next
  recreate. Put it in versioned config.
- **Risk accepted on purpose:** `fsync=off` and `full_page_writes=off`, because
  data is synthetic. Never copy these settings to real data.

### 2026-08-25: `create_tables.py` undid partitioning

- **Mistake:** re-running `create_tables.py` rebuilt plain tables, so the 5
  partitioned tables went back to unpartitioned. `create_all` cannot express partitioning.
- **Lesson:** after `create_tables.py`, re-run the partition migrations. Check
  `pg_class.relkind` before assuming partitioning is active.
- **Gotcha:** `POSTGREIP=pgprojeto` resolves only inside the Docker network, so the
  script must run via `docker exec dbapi`.

### 2026-08-28: Landing-zone ingestion validated on all 7 schemas

About 40M rows landed in MinIO. Three real bugs found against real data:

1. `uuid.UUID` objects broke pyarrow. Fixed by stringifying.
2. **OOM in `airflow_worker`:** `pandas.read_sql_query(chunksize=...)` does not
   stream with a plain psycopg2 cursor. Fixed with a **named (server-side)
   cursor**; memory stayed flat at 1.2-1.5 GB.
3. Heterogeneous JSONB (real dicts mixed with a malformed string from chaos)
   broke pyarrow. Fixed by normalising per value, not per column sample.

- **Lesson:** a named cursor exposes `.description` only after the first
  `fetchmany()`.

### 2026-09-01 to 2026-10-01: Docker Desktop will not start

- **Symptom:** `docker_data.vhdx` locked, then the engine never answered.
- **Mistake:** `wsl --terminate docker-desktop` on a slow engine wedged the whole
  WSL service. Several Docker Desktop restarts piled up stale processes.
- **Root cause (found only on 2026-10-01):** the USB external disk on F: was
  failing writes (Ntfs Event 141, delayed write failed). It was never a Docker or WSL bug.
- **Lesson:** check the Windows System log and disk health before spending hours
  on process-kill loops. Stop launching Docker against a failing disk.

### 2026-09-02 to 2026-09-14: Mounts, incremental ingestion, mutator DAG

- **Done:** scheduled task that auto-mounts the Postgres disk at logon
  (2026-09-02).
- **Done (2026-09-04):** watermark-based incremental ingestion, Postgres
  partitioning re-applied, MinIO client hang fixed.
- **Mistakes in `nexomart_mutator_dag` (2026-09-04):** missing FK imports, unsafe
  bulk delete, and hard-deletes on fan-out parents (customer, purchaseorderheader)
  that silently orphaned child rows. Details in `docs/MUTATOR_DAG_FIXES.md`.
- **Done (2026-09-05):** removed an unnecessary serialisation edge between three
  DAG tasks; moved `SEEDER_ROOT` and `UV_PROJECT` out of the DAG files into config;
  added Chile, Peru and Italy to the customer country pool.
- **Done (2026-09-14):** removed a hardcoded absolute host path from the
  airflow-worker compose.
- **Lesson:** never hard-delete a parent that has fan-out children.

### 2026-10-02 to 2026-10-03: Disk retention and Unity Catalog removal

- **Done:** 45-day partition retention (`configs/postgresql/retention.sql`) and a
  nightly Windows maintenance task at 21:00 (`pg-maintenance.ps1`): retention,
  `fstrim`, stop Postgres, unmount, `Optimize-VHD`, remount.
- **Result:** 381 GB freed from Postgres tables; the VHD shrank from 601 GB to
  425 GB (176 GB reclaimed).
- **Cause of the bloat:** one marketing table had about 276M rows per month,
  roughly 800x normal, from heavy seeding.
- **Done:** composite PK `(id, created_at)` for `purchaseorderaudit/detail`
  so they can be range-partitioned; ingestion buffers rows per date before flush.
- **Done (2026-10-03):** Unity Catalog OSS removed. Nothing really depended on
  it; the catalog is the Hive Metastore. Databricks' managed Unity Catalog is
  the planned replacement for study.
- **Lesson:** retention is safe only because ingestion never reads backwards
  past the watermark. The two features depend on each other.
- **Lesson:** a dynamic VHDX shrinks only with offline compaction. Retention alone just stops growth.
- **Gotcha:** the maintenance script runs on PowerShell 5.1 and 7. In 5.1,
  `ErrorActionPreference=Stop` turns any stderr from a native command into a fatal error.

### 2026-10-03 to 2026-10-05: Silent `initdb` incident (two times)

- **Symptom:** Postgres started with the data disk unmounted, ran `initdb` on an
  empty directory, and the real disk was mounted over it later. `nexomart` tables
  and Airflow history seemed gone. The real 140 GB of data was intact but hidden.
- **Recovery:** stop and remove the container, confirm the disk is mounted, recreate.
- **Mistake:** the first guard (named bind-volume) failed to stop it the second
  time, because an empty `pgdata` directory was left on the WSL root filesystem.
- **Fix (2026-10-05, commit `45719fd`):** sentinel file `.nexomart_real_disk`
  checked in the container entrypoint. If it is missing, the container refuses to start.
- **Lesson:** guards in `make` targets do not protect `docker compose up`. A guard
  must live where the container starts. Test the failure case, not only the happy path.

### 2026-10-05 to 2026-10-06: Streaming DAG, history spread, translation

- **Done:** streaming DAG and ingestion backfill mode; `start-dev` target;
  Postgres recreated after nightly compaction.
- **Done (2026-10-06):** dimension `created_at` spread uniformly over 2020 to now
  (`stamp_dimension_history`), so the lake has deep history. `updated_at = now()`
  is mandatory, or incremental ingestion would never pick the row up. Facts and
  `purchaseorderdetail/audit` are excluded (`_NO_BACKDATE_TABLES`).
- **Done:** one-time migration re-dated and re-landed 33.8M rows. MinIO memory
  raised 4g to 8g.
- **Mistake / side effect:** the re-land created about 69k small files in MinIO
  (about 2,100 daily partitions per big dimension).
- **Done:** two code-review fixes (`audit_trail` index key, seeder/ingestion
  race); remaining Portuguese comments and docs translated to English.
- **Done:** `KeepAliveWSL` scheduled task stops the recurring console popup.
- **Lesson:** a detached process (`setsid sleep infinity`) does not survive in
  WSL. Only a live foreground `wsl.exe` session keeps the VM awake.

### 2026-10-11 and 2026-10-25: Bind-mount staging for late-attached disks

- **Symptom:** `no such file or directory` on `docker-desktop-bind-mounts/<distro>/<hash>`
  after a disk is remounted. Restarting Docker Desktop does not help.
- **Fix:** `make stage` (`stage-minio-f` + `stage-postgres`); `init`, `start`
  and `start-dev` depend on it. The path hash is `sha256(source_path)`.
- **Mistake:** the 2026-10-11 fix was never committed, so it was lost and the same
  failure came back on 2026-10-25. It was re-applied that day, and it is still not committed.
- **Lesson:** a fix that is not committed does not exist.

### 2026-10-25: Stale disk attach after maintenance

- **Symptom:** remount failed with "Operation not permitted" after the nightly job.
- **Fix:** `wsl --shutdown`. Four guards added to `pg-maintenance.ps1`: abort
  compaction if the disk is still mounted, do not recreate Postgres if remount fails,
  do not log "done OK" after a failure, log the mount task to a file. Not committed.
- **Lesson:** identify the disk by ext4 UUID (`f1977b24...`), never by `sdX`.
  Device names change on each attach; nearly mounted the Docker data disk by mistake.
- **Lesson:** a Windows task run through `conhost --headless` always reports exit 0, so "Last Result 0" proves nothing.

### 2026-10-18 and 2026-10-24: Spark scripts cleanup

- **Done:** better logging and error handling in Spark job scripts (two commits).

### 2026-10-25 to 2026-11-02: Spark `src/` framework (Bronze and Silver building blocks)

- **Done (2026-10-25):** partitioning support in Delta writes, column validation,
  JSON parsing transformer.
- **Done (2026-11-01):** transformers `EmailFlag`, `FillNullValues`, `Flatten`,
  `NormalizeMaritalStatus`, `NormalizeToFloat`, `NormalizeToTimestamp`,
  `NormalizeString`; `ParseJsonTransformer` refactored; an unused string
  transformer deleted.
- **Done (2026-11-02):** new transformers for size extraction, name splitting and
  tax handling; stricter column validation; seeder DAG schedule updated.
- **Pattern:** every step is a small class with one `transform(df)` method.
  `CompositeTransformer` chains them; `LoggingTransformer` wraps each one.

### 2026-11-02: Requirements for marketplace and NexoBank

- **Open (requirements only, nothing coded):** seller marketplace plus a
  financial arm (payments, ledger, credit, investments). Feedback loops close
  through state in the database and reverse ETL, not circular calls. High-volume
  derived facts (daily yield, interest) are computed in Spark, not in seeders.

### 2026-11-03: README and dead-code cleanup

- **Done:** README rewritten for NexoMart (Delta-only medallion, current pipeline
  state, release checklist); dead Iceberg helper removed from `delta.py`.

### PR #41: `feat/person_bronze_transform` (merged 2026-11-04)

- **Done:** `IncrementalReader` plus Bronze ingestion functions for customer,
  customer_email, seller, stateprovince and countryregion; old `raw_customer.py` removed.
- **Pattern:** `IncrementalReader` is a decorator. It reads the target layer's
  max `COALESCE(updated_at, created_at)` and returns only newer rows. First run
  (missing target) returns everything.
- **Lesson:** tables without both timestamp columns are returned whole, so they can duplicate on re-runs.

### PR #43: `chore/spark-4.1-delta` (merged 2026-11-16)

- **Done:** Spark 3.5.8 to **4.1.1**, Delta **4.1.0**, hadoop-aws 3.4.2 with
  AWS SDK v2, Trino pinned to **483** (was `latest`). Verified live: S3A to MinIO,
  Delta append and MERGE through the Hive Metastore, Trino reads the tables, and
  the Bronze jobs for countryregion and stateprovince ran.
- **Decisions that are not obvious from the code:**
  - `spark.sql.ansi.enabled=false`: Spark 4 turns ANSI on by default, and chaos
    data makes casts fail. Moving to `try_cast` is future work.
  - All jars are baked into `$SPARK_HOME/jars` in both images (Spark and Airflow),
    and the versions must match. Through Ivy, the 640 MB SDK bundle was re-sent on
    every app, and Iceberg landed in a different classloader than DataFlint.
  - DataFlint needs the `spark4` artifact; its instrumentation stays off because
    it breaks codegen (Janino error).
  - `spark.jars.ivy` removed: a shared volume mixed uid 1000 and 50000 and caused
    "Permission denied".
  - Airflow image pins `pyspark==4.1.1` and `delta-spark==4.1.0 --no-deps`.
- **Rollback:** image tags `sparkairflow:spark-3.5.8` and `airflow-airflow-*:pre-spark4`.
- **Gotcha:** never run `pkill -f` or `pgrep -f` with a pattern that appears in your
  own shell command line; it matches and kills the shell.

### 2026-11-16 (commit `971b730`): Customer metrics from a latent engagement level

- **Done:** purchase metrics (total purchased, income range) are now derived from
  one hidden engagement level per customer, instead of independent random numbers.
  New `migrate_customer_metrics.py` backfills existing rows.
- **Why:** independent random columns make segmentation meaningless; a shared
  latent variable gives correlated, realistic features.

### 2026-11-16 to 2026-11-18: Branch `feat/person_silver` (no PR yet)

- **Done (`c93c789`, WIP):** Silver `customer` job (`KeepLatestTransformer`,
  `ParseJson`, `Flatten`, `NormalizeGender`, `NormalizeMaritalStatus`,
  `NormalizeToTimestamp`, `NormalizeToDecimal`, `FillNullValues`) with
  `DeltaMergeWriter` on `customer_id`, `cluster_by` and a duplicate-key validator.
  `tofloat.py` renamed to `toDecimal.py`; `taxcolumns.py` moved up a level.
- **Done (`7303c0f`, 2026-11-18):** Gold `customer`; `customersegment` rules
  rewritten with new thresholds and null handling; `customertype` simplified.
- **Open:**
  - `IncrementalReader` still contains a commented-out block of old filter code.
  - `FillNullValues` list in Silver `customer` has a duplicate entry and a column
    name with a trailing space (`"social_buying_contact_name "`).
  - The Silver `customer` docstring is empty.
  - Quarantine and correction of chaos data in Silver is designed but not built
    (see section 5).

---

## 4. Mistakes that repeated

| Pattern | Seen on | Rule going forward |
|---|---|---|
| Fix applied by hand, then lost | 2026-08-17 (GUCs), 2026-10-11 (stage fix) | Put it in versioned config and commit it the same day |
| Green job, silent failure | 2026-07-12, 2026-08-09 (chaos rows) | Fail loudly, exit non-zero, check the real output |
| Works at small volume, breaks at real volume | 2026-08-09, 2026-08-28 | Test at realistic volume early |
| Guard in the wrong place | 2026-10-04, 2026-10-05 | Put the guard where the container starts |
| Hardware blamed on software | 2026-09-01 to 2026-10-01 | Read the OS and disk logs first |
| Bind-mount on a late-attached disk | 2026-10-04, 2026-10-11, 2026-10-25 | `make stage`; identify disks by UUID |

---

## 5. Useful commands

```bash
make setup && make up            # first time / full stack
make stage                       # re-stage bind mounts after a disk remount
make logs s=spark-master         # tail one container

# run one seeder inside the Airflow worker
docker exec -w /dockervol airflow_worker bash -c \
  "PYTHONPATH=/dockervol:/dockervol/dbapi uv run --project /dockervol python /dockervol/dbapi/api/seeder/<domain>/<file>.py --count 1000"

# create tables (must run inside the Docker network)
docker exec dbapi python create_tables.py
```
