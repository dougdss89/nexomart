# Airflow

Orchestration layer for the platform, running on Astro Runtime 11.4 (Airflow 2.9, CeleryExecutor
+ Redis). Two groups of DAGs live under `airflow/dags/`:

- `nexomart/`: the production-like pipeline. Seeds, mutates, and ingests the `nexomart_db`
  OLTP database on a real schedule.
- `advworks/`: dimension and fact DAGs plus `integration_e2e_test`, used for the Landing to
  Bronze to Silver to Gold pipeline and for CI.

## NexoMart DAGs

| DAG | File | Schedule | `max_active_runs` | `max_active_tasks` |
|-----|------|----------|--------------------|---------------------|
| `nexomart_seeder_dag` | `nexomart/seeder/nexomart_seeder_dag.py` | `*/15 * * * *` | 1 | 8 |
| `nexomart_mutator_dag` | `nexomart/mutator/nexomart_mutator_dag.py` | `0 * * * *` | 1 | 8 |
| `nexomart_ingestion_dag` | `nexomart/ingestion/nexomart_ingestion_dag.py` | `None` (triggered by the seeder) | 1 | default |

The seeder and mutator run on their own schedules. The seeder ends by triggering the ingestion DAG and waits for it to finish. The mutator is independent of both.

### `nexomart_seeder_dag`

Inserts new rows across all 7 business domains every 15 minutes, with chaos engineering (see the
main `README.md`'s Chaos Engineering section). `max_active_runs=1` stops a slow run from
overlapping the next tick, since several seeders (the fan-out ones especially) assume the
previous run already finished. `max_active_tasks=8` caps how many tasks run at once, matching
`airflow-worker`'s Celery `worker_concurrency=8`, to avoid disk I/O contention between heavy
bulk-insert tasks.

Per-run volumes are read from `configs/seeders/config.py` (`SCHEMA_RANGES`/`TABLE_RANGES`),
mirrored in the root `CLAUDE.md`. They were rescaled for a 10-15 minute cadence and had three
seeders rewritten because their cost scaled with total accumulated history instead of the work
needed that run. Full analysis: `docs/SEEDER_MICROBATCHING.md`.

### Contract drift gate

`nexomart_seeder_dag` starts with `contract_drift_check`, a task that runs `pytest tests/contracts` in the
same isolated venv as the seeders (`uv run --group dev`). It takes about a second and checks two things:

- Files against files: each api contract (`nexomart_contract/api_contract/`) against its Pydantic schema and
  SQLAlchemy model.
- Files against the real database: every SQLAlchemy model (31 tables) and the contracts against Postgres'
  `information_schema` and `pg_constraint` (column names, types, `varchar(n)` lengths, nullability, primary keys
  and foreign keys; not indexes). This catches an `ALTER TABLE` that was forgotten, or one that was run
  without updating the code.

Every seeder task depends on it, so a drift stops the run before any data is written. `REQUIRE_DB_DRIFT=1`
makes an unreachable database fail the task instead of skipping the live checks. It has `retries=0` because a
drift does not fix itself. The worker mounts `tests/` and `nexomart_contract/` read-only under `/dockervol`.
Details: `nexomart_contract/README.md`.

### `nexomart_mutator_dag`

Runs UPDATE and, for tables where it is safe, DELETE against rows the seeder already inserted,
so a future CDC pipeline has realistic update/delete events, not only inserts. Runs hourly,
intentionally much less often than the seeder, since mutation is meant to be occasional
background noise.

Rules enforced by each `<domain>/<table>_mutate.py` script (shared driver:
`dbapi/api/utils/mutator_common.py`):

- `sales.*` tables and audit-type tables (`purchaseorderaudit`, `ordereventlog`) never get a real
  DELETE, only the `is_active = false` soft-delete flag from `AuditMixin`.
- Tables that are still a foreign-key target of another table never get a hard DELETE either
  (`product`, `humanresources.employee`, `purchasing.vendor`, `marketing.marketingcampaign`,
  `person.seller`).
- `person.customer` and `purchasing.purchaseorderheader` never get a hard DELETE, even though
  the database no longer enforces their FK after partitioning (the FK becomes a composite key
  requirement partitioning cannot satisfy). A hard delete there used to silently orphan child
  rows in `person.customer_email`, `purchasing.purchaseorderdetail`, and
  `purchasing.purchaseorderaudit`.

The DAG was created disabled pending review of these rules and a live debugging pass. Both are
done: full postmortem, including a missing-dependency bug that let the DAG report success
without running anything, in `docs/MUTATOR_DAG_FIXES.md`.

### `nexomart_ingestion_dag`

Reads from `nexomart_db` and lands raw Parquet into MinIO's `landing` bucket
(`nexomart/<schema>/<table>/<YYYY-MM-DD>/part-<run_ts>-NNNNN.parquet`, day-partitioned, additive).
Only reads rows new or changed since the last successful run per table
(`COALESCE(updated_at, created_at) > watermark`), not a full scan every time. Watermarks live in
`ingestion_control.watermarks` inside `nexomart_db` and only advance on a successful run, so a
crashed task never silently skips rows.

The seeder's last task, `trigger_ingestion`, triggers this DAG with `wait_for_completion=True`, and
both DAGs use `max_active_runs=1`, so the next seeder tick cannot start until this run has finished. The
trigger was removed and reinstated on 2026-09-04: it originally fired without waiting while ingestion
re-read every table in full, so triggered runs queued up. Incremental extraction is what made it safe to
bring back. You can still trigger the DAG by hand. Full incident writeup, including a MinIO client that
had no timeout and a psycopg2 JSONB decode/re-encode round trip that was slowing every row down:
`docs/INGESTION_INCREMENTAL.md`.

## `advworks` DAGs

Dimension DAGs (`dimbuyer_dag.py`, `dimproduct_dag.py`, and so on) and fact DAGs
(`fact_sales_dag.py`, `fact_purchase_dag.py`, `fact_production_dag.py`) implement the Landing to
Bronze to Silver to Gold flow for the reference AdventureWorks-style dataset. `integration_e2e_test`
chains a full run end to end:

```
start -> seed_postgres -> pg_landing -> pg_bronze -> pg_silver
      -> pg_gold -> trino_validate -> dbt_run -> dbt_test -> end
```

`pg_gold` runs via `SparkSubmitOperator` in client mode, with the driver on `airflow_worker`, and
writes to `iceberglake.gold.gold_dim_productcategory` through Iceberg + HiveCatalog.

```bash
docker exec airflow_scheduler airflow dags trigger integration_e2e_test
docker exec airflow_scheduler airflow tasks states-for-dag-run integration_e2e_test <run_id>
```

## Configuration

`SEEDER_ROOT` and `UV_PROJECT`, the paths the NexoMart DAGs use to shell out to a seeder script
via `uv run --project <UV_PROJECT> python <SEEDER_ROOT>/<script>`, are read from
`configs/secrets.env`, not hardcoded in the three DAG files.

## Debugging a task that looks stuck

`airflow-worker` has `cap_add: [SYS_PTRACE]` in `airflow/docker-compose.yaml`, which lets `py-spy`
attach to a running task's process (Docker drops `ptrace` by default). `py-spy` is not persisted
across container recreation, reinstall it each time:

```bash
docker exec airflow_worker sh -c "PYTHONPATH=/dockervol:/dockervol/dbapi uv run --project /dockervol pip install py-spy -q"
docker exec airflow_worker ps aux | grep <script>.py   # find the .venv/bin/python3 line, not the uv run wrapper
docker exec -u root airflow_worker /home/astro/.local/bin/py-spy dump --pid <pid>
```

Take 2-3 samples a few seconds apart. The same frame every time means a genuine hang. Varying
frames mean the process is doing real, if slow, work.

## Known issues

See the root `README.md`'s "Known Issues and Fixes" section for:

- Airflow worker crashing with `role "astro" does not exist` (always start it through
  `make up-airflow`, not `docker compose` directly, so `secrets.env` is loaded).
- Stale bind mounts after a WSL2/Docker Desktop restart (never use `docker restart` on
  `airflow_worker`, use `docker rm -f` + `docker compose up -d` instead).
