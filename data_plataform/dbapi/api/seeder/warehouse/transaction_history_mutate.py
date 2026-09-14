import asyncio
import random
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.product.product import Product  # noqa: F401
from api.models.warehouse.transactionhistory import TransactionHistoryModel
from api.models.warehouse.transactiontype import WarehouseTransactionType  # noqa: F401
from api.seeder.cli_common import build_parser
from api.seeder.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# Product/WarehouseTransactionType: unused directly, but transactionhistory has FKs to both —
# see humanresources_employee_mutate.py's comment for the full explanation (same root cause).

SCHEMA_NAME = "warehouse"
TABLE_NAME = "warehouse.transactionhistory"
HARD_DELETE_ALLOWED = True


def apply_updates(rows: list[dict]) -> list[dict]:
    # Correction-style update (e.g. cycle-count adjustment), not a full re-generation.
    for row in rows:
        row["quantity"] = random.randint(1, 200)
        row["actual_cost"] = Decimal(random.uniform(1.0, 500.0)).quantize(Decimal("0.01"))
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, TransactionHistoryModel,
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
