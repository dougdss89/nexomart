"""
nexomart_mutator_dag — CDC mutation companion to nexomart_seeder_dag: runs UPDATE and
DELETE against rows nexomart_seeder_dag already inserted, so a future Debezium+Kafka
pipeline has non-INSERT events to capture (see dbapi/api/utils/mutator_common.py).

Was created DISABLED (is_paused_upon_creation=True, schedule=None) pending review of the
per-table `apply_updates` column choices in each <domain>/<table>_mutate.py (each file says
up front which columns it touches and why) — that review is done, and so is a full live
debugging pass (see docs/MUTATOR_DAG_FIXES.md: fixed missing FK imports, an unsafe bulk
DELETE, and unsafe hard-deletes on FK-referenced tables). Runs hourly now
(`schedule="0 * * * *"`) — sized well under the seeder's own 15-min cadence since mutation
is meant to be occasional background CDC noise, not a high-frequency stream. Not wired into
nexomart_seeder_dag; the two run independently on their own schedules.

sales.* tables and audit-type tables (purchaseorderaudit, ordereventlog) never get a
real DELETE — only a soft flag (AuditMixin.is_active=false). Every other table may
take a real DELETE, capped by each table's `delete_ratio` in
configs/seeders/config.py's MUTATION_SCHEMA_RANGES/MUTATION_TABLE_RANGES. This DAG
only orchestrates trigger order/grouping; the no-hard-delete rule itself lives in
each mutate_*.py's `HARD_DELETE_ALLOWED` flag.
"""

import os
from datetime import datetime, timedelta
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# Values come from configs/secrets.env (SEEDER_ROOT/UV_PROJECT), injected via the
# &airflow_env anchor in airflow/docker-compose.yaml — CLAUDE.md's "Zero Hardcode" rule.
SEEDER_ROOT = os.environ["SEEDER_ROOT"]
UV_PROJECT = os.environ["UV_PROJECT"]

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def mutator_task(task_id: str, relative_path: str, extra_args: str = "") -> BashOperator:
    """Same isolated-venv pattern as nexomart_seeder_dag.seeder_task() — see that
    docstring for why PYTHONPATH is deliberately narrowed to /dockervol:/dockervol/dbapi."""
    command = (
        f"PYTHONPATH=/dockervol:/dockervol/dbapi "
        f"uv run --project {UV_PROJECT} python {SEEDER_ROOT}/{relative_path} {extra_args}"
    ).strip()
    return BashOperator(task_id=task_id, bash_command=command)


with DAG(
    dag_id="nexomart_mutator_dag",
    default_args=DEFAULT_ARGS,
    description=(
        "CDC mutation companion to nexomart_seeder_dag — UPDATE/DELETE against already-seeded "
        "rows for a future Debezium+Kafka pipeline. Runs hourly, independent of the seeder."
    ),
    schedule="0 * * * *",  # hourly — see module docstring for why this is much less frequent
    # than the seeder's 15-min cadence
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,  # same I/O-contention rationale as nexomart_seeder_dag
    tags=["mutator", "cdc", "nexomart"],
) as dag:

    start: DummyOperator = DummyOperator(task_id="start")
    # ==================== SALES (no hard DELETE — soft flag only) ====================
    with TaskGroup(group_id="sales_no_hard_delete") as sales_no_hard_delete:
        sales_order: BashOperator = mutator_task("sales_order", "sales/sales_order_mutate.py")
        sales_return_order: BashOperator = mutator_task("sales_return_order", "sales/sales_return_order_mutate.py")
        sales_seller: BashOperator = mutator_task("sales_seller", "sales/sales_seller_mutate.py")

    # ==================== AUDIT-TYPE (no hard DELETE — soft flag only) ====================
    with TaskGroup(group_id="audit_no_hard_delete") as audit_no_hard_delete:
        sales_eventlog: BashOperator = mutator_task("sales_eventlog", "sales/sales_eventlog_mutate.py")
        purchase_audit: BashOperator = mutator_task("purchase_audit", "purchasing/purchase_purchaseorderaudit_mutate.py")

    # ==================== EVERYTHING ELSE (real DELETE allowed) ====================
    with TaskGroup(group_id="mutable") as mutable:
        purchase_header: BashOperator = mutator_task("purchase_header", "purchasing/purchase_purchaseorderheader_mutate.py")
        purchase_detail: BashOperator = mutator_task("purchase_detail", "purchasing/purchase_purchaseorderdetail_mutate.py")
        purchase_vendor: BashOperator = mutator_task("purchase_vendor", "purchasing/purchase_purchaseorder_vendor_mutate.py")
        person_customer: BashOperator = mutator_task("person_customer", "person/person_customer_mutate.py")
        person_customer_email: BashOperator = mutator_task("person_customer_email", "person/person_customer_email_mutate.py")
        transaction_history: BashOperator = mutator_task("transaction_history", "warehouse/transaction_history_mutate.py")
        product_inventory: BashOperator = mutator_task("product_inventory", "warehouse/product_inventory_mutate.py")
        product: BashOperator = mutator_task("product", "product/product_mutate.py")
        marketing_campaign: BashOperator = mutator_task("marketing_campaign", "marketing/marketing_campaign_mutate.py")
        marketing_daily_performance: BashOperator = mutator_task(
            "marketing_daily_performance", "marketing/marketing_daily_performance_mutate.py"
        )
        hr_employee: BashOperator = mutator_task("hr_employee", "humanresources/humanresources_employee_mutate.py")

    end: DummyOperator = DummyOperator(task_id="end")

    start >> [sales_no_hard_delete, audit_no_hard_delete, mutable] >> end

    # No cross-table dependencies: unlike nexomart_seeder_dag, these tasks never insert new
    # rows referencing each other's output — each one only samples and mutates rows already
    # in its own table, so there's no dimensional-integrity ordering to preserve here.
