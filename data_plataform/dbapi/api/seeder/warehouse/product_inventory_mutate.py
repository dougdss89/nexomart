import asyncio
import random
from datetime import UTC, datetime

from api.database import AsyncSessionLocal
from api.models.product.product import Product  # noqa: F401
from api.models.warehouse.productinventory import WarehouseProductInventoryModel
from api.models.warehouse.warehouselocation import WarehouseLocationModel  # noqa: F401
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# Product/WarehouseLocationModel: unused directly, but productinventory has FKs to both — see
# humanresources_employee_mutate.py's comment for the full explanation (same root cause).

SCHEMA_NAME = "warehouse"
TABLE_NAME = "warehouse.productinventory"
HARD_DELETE_ALLOWED = True


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["quantity"] = random.randint(0, 5_000)
        row["last_restock_date"] = datetime.now(UTC)
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session,
            WarehouseProductInventoryModel,
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
    count, delete_ratio, batch_size = resolve_mutation_args(
        args, schema=SCHEMA_NAME, table=TABLE_NAME
    )
    asyncio.run(mutate(count, delete_ratio, batch_size))
