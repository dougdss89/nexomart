"""
nexomart_mutator_dag — CDC mutation companion to nexomart_seeder_dag: runs UPDATE and
DELETE against rows nexomart_seeder_dag already inserted, so a future Debezium+Kafka
pipeline has non-INSERT events to capture (see dbapi/api/seeder/mutator_common.py).

Created DISABLED on purpose (is_paused_upon_creation=True, schedule=None) — this DAG
is not wired into nexomart_seeder_dag and nothing triggers it automatically. Do not
unpause/trigger until the per-table `apply_updates` column choices in each
<domain>/<table>_mutate.py have been reviewed (each file says up front which columns
it touches and why).

sales.* tables and audit-type tables (purchaseorderaudit, ordereventlog) never get a
real DELETE — only a soft flag (AuditMixin.is_active=false). Every other table may
take a real DELETE, capped by each table's `delete_ratio` in
configs/seeders/config.py's MUTATION_SCHEMA_RANGES/MUTATION_TABLE_RANGES. This DAG
only orchestrates trigger order/grouping; the no-hard-delete rule itself lives in
each mutate_*.py's `HARD_DELETE_ALLOWED` flag.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

SEEDER_ROOT = "/dockervol/dbapi/api/seeder"
UV_PROJECT = "/dockervol"

DEFAULT_ARGS = {
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
        "rows for a future Debezium+Kafka pipeline. Created disabled, not auto-triggered."
    ),
    schedule=None,  # manual trigger only — never runs on a cron
    is_paused_upon_creation=True,  # created paused — must be explicitly unpaused in the UI/CLI
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,  # same I/O-contention rationale as nexomart_seeder_dag
    tags=["mutator", "cdc", "nexomart", "disabled-by-default"],
) as dag:

    # ==================== SALES (no hard DELETE — soft flag only) ====================
    with TaskGroup(group_id="sales_no_hard_delete") as sales_no_hard_delete:
        sales_order = mutator_task("sales_order", "sales/sales_order_mutate.py")
        sales_return_order = mutator_task("sales_return_order", "sales/sales_return_order_mutate.py")
        sales_seller = mutator_task("sales_seller", "sales/sales_seller_mutate.py")

    # ==================== AUDIT-TYPE (no hard DELETE — soft flag only) ====================
    with TaskGroup(group_id="audit_no_hard_delete") as audit_no_hard_delete:
        sales_eventlog = mutator_task("sales_eventlog", "sales/sales_eventlog_mutate.py")
        purchase_audit = mutator_task("purchase_audit", "purchasing/purchase_purchaseorderaudit_mutate.py")

    # ==================== EVERYTHING ELSE (real DELETE allowed) ====================
    with TaskGroup(group_id="mutable") as mutable:
        purchase_header = mutator_task("purchase_header", "purchasing/purchase_purchaseorderheader_mutate.py")
        purchase_detail = mutator_task("purchase_detail", "purchasing/purchase_purchaseorderdetail_mutate.py")
        purchase_vendor = mutator_task("purchase_vendor", "purchasing/purchase_purchaseorder_vendor_mutate.py")
        person_customer = mutator_task("person_customer", "person/person_customer_mutate.py")
        person_customer_email = mutator_task("person_customer_email", "person/person_customer_email_mutate.py")
        transaction_history = mutator_task("transaction_history", "warehouse/transaction_history_mutate.py")
        product_inventory = mutator_task("product_inventory", "warehouse/product_inventory_mutate.py")
        product = mutator_task("product", "product/product_mutate.py")
        marketing_campaign = mutator_task("marketing_campaign", "marketing/marketing_campaign_mutate.py")
        marketing_daily_performance = mutator_task(
            "marketing_daily_performance", "marketing/marketing_daily_performance_mutate.py"
        )
        hr_employee = mutator_task("hr_employee", "humanresources/humanresources_employee_mutate.py")

    # No cross-table dependencies: unlike nexomart_seeder_dag, these tasks never insert new
    # rows referencing each other's output — each one only samples and mutates rows already
    # in its own table, so there's no dimensional-integrity ordering to preserve here.
