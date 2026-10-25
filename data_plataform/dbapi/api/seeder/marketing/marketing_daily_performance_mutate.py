import asyncio
import random
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.marketing.marketing_adgroup import MarketingAdGroupModel  # noqa: F401
from api.models.marketing.marketing_advertise import MarketingAdvertiseModel  # noqa: F401
from api.models.marketing.marketing_campaign import MarketingCampaignModel  # noqa: F401
from api.models.marketing.marketing_dailyperformance import MarketingDailyPerformanceModel
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# The 3 unused-looking imports above are required: dailyperformance has FKs to all of them, and
# a missing one raises NoReferencedTableError at runtime, not import time — see
# humanresources_employee_mutate.py's comment for the full explanation (same root cause).

SCHEMA_NAME = "marketing"
TABLE_NAME = "marketing.dailyperformance"
HARD_DELETE_ALLOWED = True


def apply_updates(rows: list[dict]) -> list[dict]:
    # Correction-style update to a day's metrics (late-arriving conversions/cost), not a
    # from-scratch regeneration of the funnel — clicks/conversions kept independent on
    # purpose so this doesn't silently re-derive the CLAUDE.md funnel math.
    for row in rows:
        row["clicks"] = random.randint(0, 10_000)
        row["conversions"] = random.randint(0, row["clicks"]) if row["clicks"] else 0
        row["cost"] = Decimal(random.uniform(1.0, 5_000.0)).quantize(Decimal("0.01"))
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session,
            MarketingDailyPerformanceModel,
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
