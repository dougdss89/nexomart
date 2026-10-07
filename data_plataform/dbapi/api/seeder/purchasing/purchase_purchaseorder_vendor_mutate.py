import asyncio
import random
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.purchasing.vendor import PurchaseVendorModel
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

SCHEMA_NAME = "purchasing"
TABLE_NAME = "purchasing.vendor"
HARD_DELETE_ALLOWED = False  # referenced by purchaseorderheader — see mutator_common.py's
# bulk_hard_delete() comment; random sampling doesn't check for inbound FKs, so a hard delete
# here fails ~1-in-N with IntegrityError whenever it samples a still-referenced vendor
# (confirmed live 2026-09-04). Soft-delete (is_active=false) is also the more realistic chaos
# scenario for a vendor relationship ending anyway.


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["credit_rating"] = Decimal(random.uniform(1.0, 5.0)).quantize(Decimal("0.01"))
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, PurchaseVendorModel,
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
