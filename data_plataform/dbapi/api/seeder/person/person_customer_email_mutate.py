import asyncio

from api.database import AsyncSessionLocal
from api.models.person.customer_email import CustomerEmailModel
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

SCHEMA_NAME = "person"
TABLE_NAME = "person.customer_email"
HARD_DELETE_ALLOWED = True


def apply_updates(rows: list[dict]) -> list[dict]:
    # NOTE: flips is_primary without checking whether the customer already has another
    # primary email — a real "set primary" op needs to unset the sibling row too. Left
    # simple on purpose; revisit once you've decided the business rule.
    for row in rows:
        row["is_primary"] = 1 if row.get("customer_email_id", 0) % 2 == 0 else 0
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session,
            CustomerEmailModel,
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
