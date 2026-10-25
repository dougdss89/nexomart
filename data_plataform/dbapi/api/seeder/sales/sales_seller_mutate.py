import asyncio
import random
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.person.country_region import CountryRegionModel  # noqa: F401
from api.models.person.seller import SellerModel
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# CountryRegionModel: unused directly, but seller has an FK to person.countryregion — see
# humanresources_employee_mutate.py's comment for the full explanation (same root cause).

# Model lives in person.seller, but the seeder that inserts it is sales/sales_seller.py
# (marketplace store, referenced by salesorder.store_id) — kept in the same folder for
# discoverability, matching the insert-side seeder.
SCHEMA_NAME = "person"
TABLE_NAME = "person.seller"
HARD_DELETE_ALLOWED = False  # referenced by returnorder.store_id — see mutator_common.py's
# bulk_hard_delete() comment; random sampling doesn't check for inbound FKs, so a hard delete
# here fails ~1-in-N with IntegrityError whenever it samples a still-referenced seller
# (confirmed live 2026-09-04). Soft-delete (is_active=false) is also the more realistic chaos
# scenario for a seller/store that's closed anyway. (Not itself a sales.* table, but ends up
# with the same soft-delete-only treatment for a different reason.)


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["commission_rate"] = Decimal(random.uniform(0.02, 0.20)).quantize(Decimal("0.0001"))
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session,
            SellerModel,
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
