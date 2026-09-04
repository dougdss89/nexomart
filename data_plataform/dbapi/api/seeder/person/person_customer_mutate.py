import asyncio
import random
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.person.country_region import CountryRegionModel  # noqa: F401
from api.models.person.customer import CustomerModel
from api.models.person.state_province import StateProvinceModel  # noqa: F401
from api.seeder.cli_common import build_parser
from api.seeder.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# CountryRegionModel/StateProvinceModel: unused directly, but customer has FKs to both — see
# humanresources_employee_mutate.py's comment for the full explanation (same root cause).

SCHEMA_NAME = "person"
TABLE_NAME = "person.customer"
HARD_DELETE_ALLOWED = True  # e.g. right-to-be-forgotten style deletions are realistic here

CUSTOMER_TYPES = ["individual", "business", "vip"]


def apply_updates(rows: list[dict]) -> list[dict]:
    for row in rows:
        row["customer_type"] = random.choice(CUSTOMER_TYPES)
        row["credit_limit"] = Decimal(random.uniform(0.0, 20_000.0)).quantize(Decimal("0.01"))
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session, CustomerModel,
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
