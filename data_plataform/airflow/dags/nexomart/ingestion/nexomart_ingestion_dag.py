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
"""

from datetime import datetime, timedelta
from typing import Any, cast

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

SEEDER_ROOT = "/dockervol/dbapi/api/seeder"
UV_PROJECT = "/dockervol"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}
def start_ingestions (taks_id: str) -> BaseOperator:
    """DummyOperator that marks the start of the DAG, for visual clarity in the Airflow UI."""
    return cast(BaseOperator, DummyOperator(task_id=taks_id))

def end_ingestions (taks_id: str) -> BaseOperator:
    """DummyOperator that marks the end of the DAG, for visual clarity in the Airflow UI."""
    return cast(BaseOperator, DummyOperator(task_id=taks_id))

def ingestion_task(task_id: str, relative_path: str) -> BaseOperator:
    """BashOperator that runs a nexomart ingestion script via uv, same PYTHONPATH convention as
    `seeder_task()` in `nexomart_seeder_dag.py` — see that function's docstring for why
    `/dockervol/configs` is deliberately left out of PYTHONPATH.
    """
    command = (
        f"PYTHONPATH=/dockervol:/dockervol/dbapi "
        f"uv run --project {UV_PROJECT} python {SEEDER_ROOT}/{relative_path}"
    )
    return cast(BaseOperator, BashOperator(task_id=task_id, bash_command=command))


with DAG(
    dag_id="nexomart_ingestion_dag",
    default_args=DEFAULT_ARGS,
    description=(
        "Lands every table of all 7 nexomarket_db schemas into MinIO's nexomart landing zone — "
        "triggered by nexomart_seeder_dag, never scheduled on its own"
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "nexomart", "landing"]
) as dag:
    start_ingestion = start_ingestions("start_ingestion")
    ingest_humanresources = ingestion_task("ingest_humanresources", "ingestions/humanresources_ingestion.py")
    ingest_marketing = ingestion_task("ingest_marketing", "ingestions/marketing_ingestion.py")
    ingest_person = ingestion_task("ingest_person", "ingestions/person_ingestion.py")
    ingest_product = ingestion_task("ingest_product", "ingestions/product_ingestion.py")
    ingest_purchasing = ingestion_task("ingest_purchasing", "ingestions/purchasing_ingestion.py")
    ingest_sales = ingestion_task("ingest_sales", "ingestions/sales_ingestion.py")
    ingest_warehouse = ingestion_task("ingest_warehouse", "ingestions/warehouse_ingestion.py")
    end_ingestion = end_ingestions  ("end_ingestion")

    # No dependencies were declared at all before this: start_ingestion/end_ingestion ran as
    # disconnected no-ops (both completing instantly, start_date==end_date, regardless of
    # whether the 7 ingest_* tasks had even started yet) and the 7 schemas had no upstream gate
    # on start_ingestion either. end_ingestion's default trigger_rule ("all_success") means it
    # now only succeeds once every schema has landed cleanly — any single schema failing leaves
    # it upstream_failed instead of the DAG run misreporting success.
    ingest_tasks = [
        ingest_humanresources,
        ingest_marketing,
        ingest_person,
        ingest_product,
        ingest_purchasing,
        ingest_sales,
        ingest_warehouse,
    ]
    start_ingestion >> ingest_tasks >> end_ingestion