import asyncio
import random

from api.database import AsyncSessionLocal
from api.models.sales.ordereventlog import SalesOrderEventLogModel
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.ordereventlog"
HARD_DELETE_ALLOWED = False  # audit-type table (event log) — DELETE always resolves to a soft flag

# Event logs are meant to be near-immutable — this models the one legitimate case
# (correcting a misclassified event_type after the fact), not routine mutation.
EVENT_TYPE_CORRECTIONS = ["status_change", "payment_confirmed", "shipment_dispatched", "note_added"]


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["event_type"] = random.choice(EVENT_TYPE_CORRECTIONS)
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session,
            SalesOrderEventLogModel,
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
    count, delete_ratio, batch_size = resolve_mutation_args(
        args, schema=SCHEMA_NAME, table=TABLE_NAME
    )
    asyncio.run(mutate(count, delete_ratio, batch_size))
