import asyncio
import random
from datetime import datetime, timezone

from api.database import AsyncSessionLocal
from api.models.product.product import Product
from api.models.product.productcategory import ProductCategoryModel  # noqa: F401
from api.models.product.productmodel import ProductModel  # noqa: F401
from api.seeder.cli_common import build_parser
from api.seeder.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# ProductCategoryModel/ProductModel: unused directly, but product has FKs to both — see
# humanresources_employee_mutate.py's comment for the full explanation (same root cause).

SCHEMA_NAME = "product"
TABLE_NAME = "product.product"
HARD_DELETE_ALLOWED = False  # referenced by specialofferproduct — see mutator_common.py's
# bulk_hard_delete() comment; random sampling doesn't check for inbound FKs, so a hard delete
# here fails ~1-in-N with IntegrityError whenever it samples a still-referenced product
# (confirmed live 2026-09-04). Soft-delete (is_active=false) is also the more realistic chaos
# scenario for a product with purchase history anyway.

# productdetails (color/size/price/brand/sku) is JSONB — deliberately left untouched
# here, a plain column assignment would clobber the whole object instead of merging.
# sell_end_date is the one column this starts with: discontinuing ~half the sampled rows.


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["sell_end_date"] = datetime.now(timezone.utc) if random.random() < 0.5 else None
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, Product,
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
