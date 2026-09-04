import asyncio
import random
from datetime import datetime, timedelta, timezone

from api.database import AsyncSessionLocal
from api.models.humanresources.employee import HumanResourcesEmployeeModel  # noqa: F401
from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel
from api.models.purchasing.vendor import PurchaseVendorModel  # noqa: F401
from api.seeder.cli_common import build_parser
from api.seeder.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# HumanResourcesEmployeeModel/PurchaseVendorModel: unused directly, but purchaseorderheader has
# FKs to both — see humanresources_employee_mutate.py's comment for the full explanation (same
# root cause).

SCHEMA_NAME = "purchasing"
TABLE_NAME = "purchasing.purchaseorderheader"
HARD_DELETE_ALLOWED = True  # not itself an audit table (purchaseorderaudit is the sibling that is)

STATUS_TRANSITIONS = ["Approved", "Shipped", "Completed", "Cancelled"]


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        status = random.choice(STATUS_TRANSITIONS)
        row["status"] = status
        if status in ("Shipped", "Completed"):
            row["ship_date"] = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 10))
        if status == "Completed":
            row["orderfinished_date"] = datetime.now(timezone.utc)
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, PurchaseOrderHeaderModel,
            label=TABLE_NAME,
            count=count,
            delete_ratio=delete_ratio,
            batch_size=batch_size,
            apply_updates=apply_updates,
            hard_delete_allowed=HARD_DELETE_ALLOWED,
        )


if __name__ == "__main__":
    parser = build_parser(f"CDC mutation seeder (UPDATE/DELETE) for {TABLE_NAME}")
    add_mutation_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, delete_ratio, batch_size = resolve_mutation_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)
    asyncio.run(mutate(count, delete_ratio, batch_size))
