import asyncio
import random
from datetime import datetime, timezone

from api.database import AsyncSessionLocal
from api.models.product.product import Product  # noqa: F401
from api.models.purchasing.purchaseorderdetail import PurchaseOrderDetailModel
from api.seeder.cli_common import build_parser
from api.seeder.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# Product: unused directly, but purchaseorderdetail has an FK to product.product — see
# humanresources_employee_mutate.py's comment for the full explanation (same root cause).

SCHEMA_NAME = "purchasing"
TABLE_NAME = "purchasing.purchaseorderdetail"
HARD_DELETE_ALLOWED = True


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        # NOTE: order_qty isn't fetched here, so received_qty is a plausible-looking
        # random receipt rather than one bounded by the line's actual order_qty —
        # fine as a CDC-event generator, but review before trusting it for reconciliation.
        received = random.randint(1, 500)
        row["received_qty"] = received
        row["stocked_qty"] = received
        row["is_fully_received"] = True
        row["last_receipt_date"] = datetime.now(timezone.utc)
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, PurchaseOrderDetailModel,
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
