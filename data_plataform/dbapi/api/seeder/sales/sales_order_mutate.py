import asyncio
import random
from datetime import datetime, timezone

from api.database import AsyncSessionLocal
from api.models.person.seller import SellerModel  # noqa: F401
from api.models.sales.saleschannel import SalesChannelModel  # noqa: F401
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.specialoffer import SpecialOfferModel  # noqa: F401
from api.seeder.cli_common import build_parser
from api.seeder.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# SellerModel/SalesChannelModel/SpecialOfferModel: unused directly, but salesorder has FKs to
# all three — see humanresources_employee_mutate.py's comment for the full explanation (same
# root cause).

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.salesorder"
HARD_DELETE_ALLOWED = False  # sales schema — DELETE always resolves to a soft flag

# Starting set of mutable columns — order lifecycle progression only. Review before
# widening this (e.g. order_items/payment_info are JSONB and need their own merge logic,
# not a plain column assignment).
ORDER_STATUS_TRANSITIONS = ["finalized", "partial_shipped", "partial_payment", "canceled"]


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["order_status"] = random.choice(ORDER_STATUS_TRANSITIONS)
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, SalesOrderModel,
            label=TABLE_NAME,
            count=count,
            delete_ratio=delete_ratio,
            batch_size=batch_size,
            apply_updates=apply_updates,
            hard_delete_allowed=HARD_DELETE_ALLOWED,
        )


if __name__ == "__main__":
    parser = build_parser(f"CDC mutation seeder (UPDATE/soft-DELETE) for {TABLE_NAME}")
    add_mutation_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, delete_ratio, batch_size = resolve_mutation_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)
    asyncio.run(mutate(count, delete_ratio, batch_size))
