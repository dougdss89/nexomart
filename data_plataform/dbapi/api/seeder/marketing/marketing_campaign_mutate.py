import asyncio
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.marketing.marketing_campaign import MarketingCampaignModel
from api.models.marketing.marketing_channel import MarketingChannelModel  # noqa: F401
from api.seeder.cli_common import build_parser
from api.seeder.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# MarketingChannelModel: unused directly, but marketing_campaign has an FK to marketing.channel
# and SQLAlchemy only registers a model's Table once it's been imported somewhere in the
# process — run_mutation()'s bulk UPDATE needs every FK target resolvable (NoReferencedTableError
# otherwise, confirmed live 2026-09-04 on the humanresources.employee mutator; same root cause).

SCHEMA_NAME = "marketing"
TABLE_NAME = "marketing.marketingcampaign"
HARD_DELETE_ALLOWED = False  # referenced by marketingadgroup — see mutator_common.py's
# bulk_hard_delete() comment; random sampling doesn't check for inbound FKs, so a hard delete
# here fails ~1-in-N with IntegrityError whenever it samples a still-referenced campaign
# (confirmed live 2026-09-04). Soft-delete (is_active=false) is also the more realistic chaos
# scenario for a campaign that's simply ended anyway.

STATUS_TRANSITIONS = ["active", "paused", "completed"]


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        status = random.choice(STATUS_TRANSITIONS)
        row["status"] = status
        row["total_budget"] = Decimal(random.uniform(500.0, 50_000.0)).quantize(Decimal("0.01"))
        if status == "completed":
            row["end_date"] = datetime.now(timezone.utc)
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, MarketingCampaignModel,
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
