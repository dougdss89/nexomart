"""
nexomart_ingestion_dag — lands every table of all 7 Postgres schemas into MinIO's nexomart
landing zone (dbapi/api/seeder/ingestions/), one task per schema, run in parallel.

`schedule=None`: this DAG never runs on its own clock. It only exists to be triggered by
`nexomart_seeder_dag` (see the `trigger_ingestion` task there) once a full seeding run has
finished — seeder -> ingestion, never interleaved with seeding itself, so each ingestion run
reads a stable snapshot. Split out of `nexomart_seeder_dag` into its own DAG (was a TaskGroup
there) so ingestion has its own DagRun history/retries independent of the seeder's, and so a
manual re-run of just the landing step doesn't require re-running the seeder too.

Runs in the same isolated venv as the seeders, via `uv run --project /dockervol` — see
`nexomart_seeder_dag.py`'s docstring for why (SQLAlchemy version conflict with Airflow's own).

Backfill mode (manual trigger only)
-----------------------------------
The normal path is incremental: each schema task reads only rows changed since its last
watermark (see ingestion_common.py). A table that lost its landing data while its watermark
stayed advanced (e.g. the landing bucket was wiped) is never re-landed by a normal run — it
reads 0 rows and writes nothing. Static dimension tables are the usual victims; fact tables
self-heal because fresh rows keep landing.

To re-land such tables, trigger this DAG manually ("Trigger DAG w/ config") and set the
`backfill_tables` param to a comma-separated schema.table list, e.g.
`product.product,product.productembedding,person.countryregion`. When `backfill_tables` is set,
the DAG runs a single `backfill` task (dbapi/api/seeder/ingestions/backfill_ingestion.py) that
resets each table's watermark to EPOCH and does a full re-land, then skips the 7 per-schema
tasks. When it's empty (the seeder's automatic trigger passes no conf), the DAG runs the normal
7-schema incremental ingestion — unchanged.
"""

import os
from datetime import datetime, timedelta
from typing import Any

from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from airflow import DAG

# Values come from configs/secrets.env (SEEDER_ROOT/UV_PROJECT), injected via the
# &airflow_env anchor in airflow/docker-compose.yaml — CLAUDE.md's "Zero Hardcode" rule.
SEEDER_ROOT = os.environ["SEEDER_ROOT"]
UV_PROJECT = os.environ["UV_PROJECT"]

SCHEMAS = ["humanresources", "marketing", "person", "product", "purchasing", "sales", "warehouse"]

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def ingestion_task(task_id: str, relative_path: str) -> BashOperator:
    """BashOperator that runs a nexomart ingestion script via uv, same PYTHONPATH convention as
    `seeder_task()` in `nexomart_seeder_dag.py` — see that function's docstring for why
    `/dockervol/configs` is deliberately left out of PYTHONPATH.
    """
    command = (
        f"PYTHONPATH=/dockervol:/dockervol/dbapi "
        f"uv run --project {UV_PROJECT} python {SEEDER_ROOT}/{relative_path}"
    )
    return BashOperator(task_id=task_id, bash_command=command)


def _choose_mode(**context) -> Any:
    """Branch: backfill when the manual trigger set `backfill_tables`, else the normal 7-schema
    incremental ingestion. The seeder's automatic trigger passes no conf, so params fall back to
    their defaults ("" -> normal path) and the seeder -> ingestion flow is untouched."""
    tables = (context["params"].get("backfill_tables") or "").strip()
    if tables:
        return "backfill"
    return [f"ingest_{schema}" for schema in SCHEMAS]


with DAG(
    dag_id="nexomart_ingestion_dag",
    default_args=DEFAULT_ARGS,
    description=(
        "Lands every table of all 7 nexomarket_db schemas into MinIO's nexomart landing zone — "
        "triggered by nexomart_seeder_dag; manual trigger supports a backfill_tables param"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        # Comma-separated schema.table list to force a full re-land of. Empty (the default, and
        # what the seeder's automatic trigger sends) runs the normal incremental ingestion.
        "backfill_tables": Param(
            default="",
            type="string",
            title="Backfill tables",
            description="Comma-separated schema.table list to re-land fully, e.g. product.product,person.countryregion. Leave empty for normal incremental ingestion.",
        ),
        "chunksize": Param(default=100_000, type="integer", title="Rows per Parquet part (backfill only)"),
    },
    tags=["ingestion", "nexomart", "landing"],
) as dag:

    start_ingestion = DummyOperator(task_id="start_ingestion")

    # Branch on the backfill_tables param (see _choose_mode). Only the chosen downstream
    # task(s) run; the rest are skipped, so end_ingestion uses none_failed_min_one_success.
    choose_mode = BranchPythonOperator(task_id="choose_mode", python_callable=_choose_mode)

    ingest_tasks: list[BashOperator] = [
        ingestion_task(f"ingest_{schema}", f"ingestions/{schema}_ingestion.py") for schema in SCHEMAS
    ]

    # Reuses the exact ingest_table path the per-schema tasks use (resets watermark -> EPOCH ->
    # full re-land -> watermark re-advances). --tables/--chunksize come from the manual-trigger
    # params; backfill never runs on the seeder's automatic (conf-less) trigger.
    backfill = BashOperator(
        task_id="backfill",
        bash_command=(
            "PYTHONPATH=/dockervol:/dockervol/dbapi "
            f"uv run --project {UV_PROJECT} python {SEEDER_ROOT}/ingestions/backfill_ingestion.py "
            '--tables "{{ params.backfill_tables }}" --chunksize {{ params.chunksize }}'
        ),
    )

    # all_success would get skipped when the branch skips one side, so require only that nothing
    # upstream failed and at least one upstream succeeded (i.e. whichever branch actually ran).
    end_ingestion = DummyOperator(task_id="end_ingestion", trigger_rule="none_failed_min_one_success")

    start_ingestion >> choose_mode
    choose_mode >> ingest_tasks >> end_ingestion
    choose_mode >> backfill >> end_ingestion
