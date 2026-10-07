import asyncio
import random

from api.database import AsyncSessionLocal
from api.models.humanresources.employee import HumanResourcesEmployeeModel  # noqa: F401
from api.models.purchasing.purchaseorderaudit import PurchaseOrderAuditModel
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# HumanResourcesEmployeeModel: unused directly, but purchaseorderaudit has an FK to
# humanresources.employee — see humanresources_employee_mutate.py's comment for the full
# explanation (same root cause).

SCHEMA_NAME = "purchasing"
TABLE_NAME = "purchasing.purchaseorderaudit"
HARD_DELETE_ALLOWED = False  # audit table — DELETE always resolves to a soft flag

OBS_CORRECTIONS = [
    "Corrected after manual review",
    "Duplicate entry note added",
    "Amended per compliance request",
    None,
]


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["obs"] = random.choice(OBS_CORRECTIONS)
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, PurchaseOrderAuditModel,
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
