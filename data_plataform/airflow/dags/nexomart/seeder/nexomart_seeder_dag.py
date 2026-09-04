"""
nexomart_seeder_dag — triggers all the standardized dbapi seeders
(dbapi/api/seeder/) in dimensional integrity order: master data ->
dimensions -> facts -> derived.

Each seeder already rolls its own volume and chaos rate on its own when
called without flags (see dbapi/api/seeder/cli_common.py and
configs/seeders/config.py) — this DAG only orchestrates the TRIGGER
ORDER, respecting the FKs that each seeder queries via a real select()
against the database before generating data. No fact/derived seeder makes up
customer_id, product_id, category_id, etc. — they all fetch from the
database first (see CLAUDE.md and the two categories below).

Once seeding for a run is fully done, the final `trigger_ingestion` task triggers the separate
`nexomart_ingestion_dag` (airflow/dags/nexomart/ingestion/), which lands every table of all 7
schemas into MinIO's nexomart landing zone — seeder -> ingestion, never interleaved with seeding
itself, so each ingestion run reads a stable snapshot.

This auto-trigger was removed 2026-09-04 and reinstated the same day once the actual problem was
fixed. It originally ran `wait_for_completion=False` (fire-and-forget on every 15-min run
regardless of whether the *previous* ingestion run had finished) while ingestion itself re-read
every table in full on every run — a one-time full backfill measured that day took 1-2+ hours
per heavy schema, so triggers stacked up in the queue behind each other, adding to exactly the
I/O/memory contention this whole file's seeder-side fixes were about. The real fix was making
ingestion incremental (watermark-based, see docs/INGESTION_INCREMENTAL.md) plus two real
performance bugs found and fixed in `ingestion_common.py` (a MinIO client with no timeout, and a
psycopg2 JSONB decode/re-encode round-trip) — with those in place, a normal 15-min ingestion run
only reads what changed and finishes in seconds, not hours. `wait_for_completion=True` this time
(the previous version's docstring claimed this but the code actually said `False` — part of how
the stacking went unnoticed): this DAG run's own success/failure now genuinely reflects whether
ingestion landed cleanly, and `max_active_runs=1` on both DAGs means the next seeder tick can't
start seeding again until this run's ingestion has actually finished, so there's no longer a way
for runs to stack up behind each other.

The seeders run in an isolated venv via `uv run --project /dockervol`, mounted read-only in the
airflow-worker container (see airflow/dockerfile and airflow/docker-compose.yaml) — this avoids
conflicting with the SQLAlchemy that Airflow itself uses internally.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator

SEEDER_ROOT = "/dockervol/dbapi/api/seeder"
UV_PROJECT = "/dockervol"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def seeder_task(task_id: str, relative_path: str, extra_args: str = "") -> BashOperator:
    """BashOperator that runs a dbapi seeder via uv, in an Airflow-isolated venv.

    PYTHONPATH here is intentionally ONLY `/dockervol:/dockervol/dbapi` (without
    `/dockervol/configs` from the worker's global environment) —
    `configs/api/` and `dbapi/api/` collide on the `api` package name
    when both are on the path at the same time (the first has `__init__.py`,
    the second doesn't, and Python always resolves the package with
    `__init__.py`, breaking `import api.schemas.*`). `configs.*` still
    resolves normally from `/dockervol` (which is the parent of both `dbapi/`
    and `configs/`, the same sibling structure as the local repo).
    """
    command = (
        f"PYTHONPATH=/dockervol:/dockervol/dbapi "
        f"uv run --project {UV_PROJECT} python {SEEDER_ROOT}/{relative_path} {extra_args}"
    ).strip()
    return BashOperator(task_id=task_id, bash_command=command)


with DAG(
    dag_id="nexomart_seeder_dag",
    default_args=DEFAULT_ARGS,
    description=(
        "Populates Postgres via the standardized dbapi seeders, respecting "
        "dimensional integrity (FKs always read from the database, never made up)"
    ),
    # every 15 min — steady-state timing established 2026-09-02: 9 uncontended runs averaged
    # 873s (14.5min), min 370s, max 1459s (see docs/SEEDER_MICROBATCHING.md and the DAG-run
    # analysis that day). max_active_runs=1 below means a run overrunning 15min just delays the
    # next tick rather than overlapping. Matches the 10-15min target this file's docstring and
    # CLAUDE.md's ranges table were already calibrated for. Was 40min before this, daily before that.
    schedule="*/05 * * * *",
    catchup=False,
    max_active_runs=1,
    # max_active_tasks reduced from the unset default (16) 2026-08-21: docs/SEEDER_MICROBATCHING.md's
    # Problem 6 found several heavy bulk_copy() tasks (purchase_detail, purchase_audit,
    # person_customer) fighting for the same disk when scheduled concurrently, inflating
    # person_customer from an isolated 5m28s to 12m49s in the one full clean run measured so far.
    # 8 is a starting point (half the implicit default) — the ~28 lightweight tasks don't need much
    # concurrency to stay fast, so this mainly caps how many heavy tasks can overlap at once.
    # Re-tune once a few more real runs establish steady-state timing (see that doc's "what's
    # still open").
    max_active_tasks=8,
    # dagrun_timeout removed 2026-08-21: a 15min (then 45min) cap kept killing runs mid-flight
    # before the real bottleneck (I/O contention under concurrent heavy COPY tasks — see
    # docs/SEEDER_MICROBATCHING.md) could even be measured. Worse, the SIGTERM only kills the
    # Airflow-side client process, not the in-flight Postgres backend query, so a timed-out run
    # left orphaned COPY/SELECT statements still burning I/O on the server for minutes after
    # Airflow had already marked the task "skipped" — actively making the contention worse.
    # Letting runs finish naturally until real steady-state timing is known; reintroduce a
    # timeout once that number is established, sized to it rather than to the 10-15min target.
    tags=["seeder", "nexomart", "chaos-engineering"],
) as dag:

    start_seeder = DummyOperator(task_id="start")

    # ==================== MASTER DATA (category 3 — fixed lists, chaos-only) ====================
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

    # ==================== DIMENSIONS (category 1 root — random volume, no flags) ====================
    with TaskGroup(group_id="dimensions") as dimensions:
        product = seeder_task("product", "product/product.py")
        person_customer = seeder_task("person_customer", "person/person_customer.py")
        purchase_vendor = seeder_task("purchase_vendor", "purchasing/purchase_purchaseorder_vendor.py")
        hr_employee = seeder_task("hr_employee", "humanresources/humanresources_employee.py")
        warehouse_location = seeder_task("warehouse_location", "warehouse/warehouse_location.py")
        sales_seller = seeder_task("sales_seller", "sales/sales_seller.py")

    # ==================== FACTS (category 1 fact — random volume, no flags) ====================
    with TaskGroup(group_id="facts") as facts:
        sales_order = seeder_task("sales_order", "sales/sales_order.py")
        sales_return_order = seeder_task("sales_return_order", "sales/sales_return_order.py")
        purchase_header = seeder_task("purchase_header", "purchasing/purchase_purchaseorderheader.py")
        product_inventory = seeder_task("product_inventory", "warehouse/product_inventory.py")
        transaction_history = seeder_task("transaction_history", "warehouse/transaction_history.py")

        sales_order >> sales_return_order

    # ==================== DERIVED (category 2 — fan-out from existing data, chaos-only) ====================
    with TaskGroup(group_id="derived") as derived:
        purchase_detail = seeder_task("purchase_detail", "purchasing/purchase_purchaseorderdetail.py")
        purchase_audit = seeder_task("purchase_audit", "purchasing/purchase_purchaseorderaudit.py")
        marketing_campaign = seeder_task("marketing_campaign", "marketing/marketing_campaign.py")
        marketing_adgroup = seeder_task("marketing_adgroup", "marketing/marketing_adgroup.py")
        marketing_ads = seeder_task("marketing_ads", "marketing/marketing_ads.py")
        marketing_daily_performance = seeder_task(
            "marketing_daily_performance", "marketing/marketing_daily_performance.py"
        )
        sales_special_offer_product = seeder_task(
            "sales_special_offer_product", "sales/sales_special_offer_product.py"
        )
        sales_eventlog = seeder_task("sales_eventlog", "sales/sales_eventlog.py")
        product_embedding = seeder_task("product_embedding", "product/product_embedding.py")
        person_customer_email = seeder_task("person_customer_email", "person/person_customer_email.py")

        marketing_campaign >> marketing_adgroup >> marketing_ads >> marketing_daily_performance

    # ==================== TRIGGER INGESTION (separate DAG: nexomart_ingestion_dag) ====================
    # Fires only after ALL seeding for this DAG run is done (edge added below:
    # [master_data, dimensions, facts, derived] >> trigger_ingestion — all 4 groups listed
    # explicitly because some tasks, e.g. sales_return_order/product_inventory/sales_channel, are
    # leaves of their group with no downstream seeder at all, so a plain
    # `derived >> trigger_ingestion` wouldn't actually wait for them), so nexomart_ingestion_dag
    # reads a stable snapshot instead of racing the seeders. `wait_for_completion=True` so this
    # DAG run's own success/failure genuinely reflects whether ingestion landed cleanly, and so
    # `max_active_runs=1` on this DAG blocks the next seeder tick from starting until ingestion
    # for this one has actually finished — see this file's docstring for why that matters.
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="nexomart_ingestion_dag",
        wait_for_completion=True,
        poke_interval=30,
    )

    end_seeder = DummyOperator(task_id="end")

    # ==================== FINE-GRAINED DEPENDENCIES BETWEEN GROUPS ====================
    # Each edge below mirrors a real select() that the destination seeder performs before
    # generating data — dimensional integrity, not just schema order.

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

    # -> trigger_ingestion (seeder -> ingestion: only once every seeder in this run has finished)
    start_seeder >>[master_data, dimensions, facts, derived] >> trigger_ingestion >> end_seeder

    # ==================== RESOURCE SERIALIZATION (NOT an FK dependency) ====================
    # person_customer, purchase_audit, and purchase_detail are the three heaviest bulk_copy()
    # tasks (docs/SEEDER_MICROBATCHING.md, Problem 6) — nothing in their actual seeder code
    # reads from or depends on either of the other two (verified: neither purchasing seeder
    # references customer data at all). Left alone, Airflow schedules all three as soon as their
    # real FK dependencies are satisfied, which is what caused them to overlap and fight for I/O
    # in the one full clean run measured so far (person_customer: 5m28s isolated vs 12m49s
    # concurrent). This edge forces them to run one at a time instead, purely to avoid that
    # contention — unlike every edge above, it does not mirror a select() the destination seeder
    # performs. Order is arbitrary among the three (no real relationship to break ties by).
    person_customer >> purchase_audit >> purchase_detail
