"""
nexomat_seeder_dag — triggers all the standardized dbapi seeders
(dbapi/api/seeder/) in dimensional integrity order: master data ->
dimensions -> facts -> derived.

Each seeder already rolls its own volume and chaos rate on its own when
called without flags (see dbapi/api/seeder/cli_common.py and
configurations/seeders/config.py) — this DAG only orchestrates the TRIGGER
ORDER, respecting the FKs that each seeder queries via a real select()
against the database before generating data. No fact/derived seeder makes up
customer_id, product_id, category_id, etc. — they all fetch from the
database first (see CLAUDE.md and the two categories below).

The seeders run in an isolated venv via `uv run --project /dockervol`,
mounted read-only in the airflow-worker container (see airflow/dockerfile
and airflow/docker-compose.yaml) — this avoids conflicting with the
SQLAlchemy that Airflow itself uses internally.
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


def seeder_task(task_id: str, relative_path: str, extra_args: str = "") -> BashOperator:
    """BashOperator that runs a dbapi seeder via uv, in an Airflow-isolated venv.

    PYTHONPATH here is intentionally ONLY `/dockervol:/dockervol/dbapi` (without
    `/dockervol/configurations` from the worker's global environment) —
    `configurations/api/` and `dbapi/api/` collide on the `api` package name
    when both are on the path at the same time (the first has `__init__.py`,
    the second doesn't, and Python always resolves the package with
    `__init__.py`, breaking `import api.schemas.*`). `configurations.*` still
    resolves normally from `/dockervol` (which is the parent of both `dbapi/`
    and `configurations/`, the same sibling structure as the local repo).
    """
    command = (
        f"PYTHONPATH=/dockervol:/dockervol/dbapi "
        f"uv run --project {UV_PROJECT} python {SEEDER_ROOT}/{relative_path} {extra_args}"
    ).strip()
    return BashOperator(task_id=task_id, bash_command=command)


with DAG(
    dag_id="nexomat_seeder_dag",
    default_args=DEFAULT_ARGS,
    description=(
        "Populates Postgres via the standardized dbapi seeders, respecting "
        "dimensional integrity (FKs always read from the database, never made up)"
    ),
    schedule="0 2 * * *",
    catchup=False,
    tags=["seeder", "nexomat", "chaos-engineering"],
) as dag:

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
