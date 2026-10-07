"""
nexomart_streaming_dag — a low-volume, continuous variant of nexomart_seeder_dag for streaming
studies. Same seeders, same dimensional-integrity (FK) ordering, but each volume-driven table is
capped at a small fixed count (STREAM_COUNT) and the DAG runs back-to-back instead of on a clock,
producing a steady trickle into Postgres and then into the MinIO landing zone.

Why @continuous
---------------
`schedule="@continuous"` (Airflow 2.6+) with `max_active_runs=1` starts the next run the moment the
previous one finishes — a self-paced, gap-free micro-batch stream, which is what a streaming study
wants. No fixed cadence to overshoot (a `*/1 * * * *` cron would skip or stack when a cycle runs
longer than a minute). It runs forever until paused, so this DAG is created PAUSED
(`is_paused_upon_creation=True`): unpause it to start the stream, pause it to stop.

Volume
------
Only the 11 volume-driven seeders (the `dimensions` and `facts` groups — the ones whose count is a
free input, see dbapi/api/utils/cli_common.py `add_volume_args`) take `--min-count/--max-count`, so
only they get the STREAM_COUNT cap. Master-data (fixed lists) and derived/fan-out seeders are
chaos-only or take a custom `--count`; passing `--min-count` to them would error, so they run with
no flags — same as nexomart_seeder_dag, and already tiny (fan-out counts follow their now-capped
parents). Chaos is left at the configured default; add `--no-chaos` to STREAM_COUNT_ARGS for a clean
stream.

Ingestion
---------
Each cycle triggers nexomart_ingestion_dag (the normal incremental path — no backfill conf) with
`wait_for_completion=True`, so a cycle is seed -> land -> next cycle and the stream paces itself to
landing throughput. Consume the stream from s3://landing/nexomart/<schema>/<table>/.

NOTE: nexomart_ingestion_dag has `max_active_runs=1`. If the production nexomart_seeder_dag (every
15 min) is also triggering ingestion, the two contend on that single ingestion slot. Pause
nexomart_seeder_dag while running this study so the stream isn't throttled by the heavy 15-min runs.

Runs the seeders in the same isolated venv as nexomart_seeder_dag (`uv run --project /dockervol`,
read-only mount in the airflow-worker) — see that DAG's docstring for why.
"""

import os
from datetime import datetime, timedelta
from typing import Any

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from airflow import DAG

# Values come from configs/secrets.env (SEEDER_ROOT/UV_PROJECT), injected via the
# &airflow_env anchor in airflow/docker-compose.yaml — CLAUDE.md's "Zero Hardcode" rule.
SEEDER_ROOT = os.environ["SEEDER_ROOT"]
UV_PROJECT = os.environ["UV_PROJECT"]

# Low, fixed per-table volume for the stream. min == max so each volume-driven seeder draws exactly
# this many rows per cycle instead of a random range.
STREAM_COUNT = 100
STREAM_COUNT_ARGS = f"--min-count {STREAM_COUNT} --max-count {STREAM_COUNT}"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def seeder_task(task_id: str, relative_path: str, extra_args: str = "") -> BashOperator:
    """Runs a dbapi seeder via uv in the Airflow-isolated venv — same PYTHONPATH rule as
    nexomart_seeder_dag.seeder_task (see that function's docstring)."""
    command = (
        f"PYTHONPATH=/dockervol:/dockervol/dbapi "
        f"uv run --project {UV_PROJECT} python {SEEDER_ROOT}/{relative_path} {extra_args}"
    ).strip()
    return BashOperator(task_id=task_id, bash_command=command)


with DAG(
    dag_id="nexomart_streaming_dag",
    default_args=DEFAULT_ARGS,
    description=(
        f"Continuous low-volume ({STREAM_COUNT}/table) seeding + landing for streaming studies — "
        "same seeders/FK order as nexomart_seeder_dag, runs back-to-back"
    ),
    schedule="@continuous",
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["streaming", "nexomart", "study"],
) as dag:

    start_stream: DummyOperator = DummyOperator(task_id="start")

    # ==================== MASTER DATA (fixed lists / chaos-only — no volume flags) ====================
    with TaskGroup(group_id="master_data") as master_data:
        hr_department = seeder_task("hr_department", "humanresources/humanresources_department.py")
        hr_job_role = seeder_task("hr_job_role", "humanresources/humanresources_job_role.py")
        product_category = seeder_task("product_category", "product/product_category.py")
        product_model = seeder_task("product_model", "product/product_model.py")
        person_country = seeder_task("person_country", "person/person_country.py")
        person_state_province = seeder_task("person_state_province", "person/person_state_province.py")
        marketing_channel = seeder_task("marketing_channel", "marketing/marketing_channel.py")
        sales_special_offer = seeder_task("sales_special_offer", "sales/sales_special_offer.py")
        warehouse_transaction_type = seeder_task("warehouse_transaction_type", "warehouse/transaction_type.py")
        sales_channel = seeder_task("sales_channel", "sales/sales_channel.py")

        person_country >> person_state_province

    # ==================== DIMENSIONS (volume-driven — capped at STREAM_COUNT) ====================
    with TaskGroup(group_id="dimensions") as dimensions:
        product = seeder_task("product", "product/product.py", STREAM_COUNT_ARGS)
        person_customer = seeder_task("person_customer", "person/person_customer.py", STREAM_COUNT_ARGS)
        purchase_vendor = seeder_task("purchase_vendor", "purchasing/purchase_purchaseorder_vendor.py", STREAM_COUNT_ARGS)
        hr_employee = seeder_task("hr_employee", "humanresources/humanresources_employee.py", STREAM_COUNT_ARGS)
        warehouse_location = seeder_task("warehouse_location", "warehouse/warehouse_location.py", STREAM_COUNT_ARGS)
        sales_seller = seeder_task("sales_seller", "sales/sales_seller.py", STREAM_COUNT_ARGS)

    # ==================== FACTS (volume-driven — capped at STREAM_COUNT) ====================
    with TaskGroup(group_id="facts") as facts:
        sales_order = seeder_task("sales_order", "sales/sales_order.py", STREAM_COUNT_ARGS)
        sales_return_order = seeder_task("sales_return_order", "sales/sales_return_order.py", STREAM_COUNT_ARGS)
        purchase_header = seeder_task("purchase_header", "purchasing/purchase_purchaseorderheader.py", STREAM_COUNT_ARGS)
        product_inventory = seeder_task("product_inventory", "warehouse/product_inventory.py", STREAM_COUNT_ARGS)
        transaction_history = seeder_task("transaction_history", "warehouse/transaction_history.py", STREAM_COUNT_ARGS)

        sales_order >> sales_return_order

    # ==================== DERIVED (fan-out / chaos-only — no volume flags) ====================
    with TaskGroup(group_id="derived") as derived:
        purchase_detail = seeder_task("purchase_detail", "purchasing/purchase_purchaseorderdetail.py")
        purchase_audit = seeder_task("purchase_audit", "purchasing/purchase_purchaseorderaudit.py")
        marketing_campaign = seeder_task("marketing_campaign", "marketing/marketing_campaign.py")
        marketing_adgroup = seeder_task("marketing_adgroup", "marketing/marketing_adgroup.py")
        marketing_ads = seeder_task("marketing_ads", "marketing/marketing_ads.py")
        marketing_daily_performance = seeder_task("marketing_daily_performance", "marketing/marketing_daily_performance.py")
        sales_special_offer_product = seeder_task("sales_special_offer_product", "sales/sales_special_offer_product.py")
        sales_eventlog = seeder_task("sales_eventlog", "sales/sales_eventlog.py")
        product_embedding = seeder_task("product_embedding", "product/product_embedding.py")
        person_customer_email = seeder_task("person_customer_email", "person/person_customer_email.py")

        marketing_campaign >> marketing_adgroup >> marketing_ads >> marketing_daily_performance

    # Normal incremental ingestion (no backfill conf). wait_for_completion=True paces the stream:
    # seed -> land -> next cycle. See the module docstring's note on ingestion contention.
    trigger_ingestion: Any = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="nexomart_ingestion_dag",
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    end_stream: DummyOperator = DummyOperator(task_id="end")

    # ==================== FK DEPENDENCIES (mirror nexomart_seeder_dag) ====================
    # -> dimensions
    [product_category, product_model] >> product
    person_state_province >> person_customer
    [hr_department, hr_job_role, person_state_province] >> hr_employee
    person_country >> sales_seller

    # -> facts
    [person_customer, sales_channel, sales_special_offer, sales_seller, product] >> sales_order
    [purchase_vendor, hr_employee] >> purchase_header
    [product, warehouse_location] >> product_inventory
    [product, warehouse_location, warehouse_transaction_type, sales_order] >> transaction_history

    # -> derived
    [purchase_header, hr_employee] >> purchase_audit
    purchase_header >> purchase_detail
    marketing_channel >> marketing_campaign
    [sales_special_offer, product] >> sales_special_offer_product
    sales_order >> sales_eventlog
    product >> product_embedding
    person_customer >> person_customer_email

    # seed everything -> land it -> cycle again
    start_stream >> [master_data, dimensions, facts, derived] >> trigger_ingestion >> end_stream
