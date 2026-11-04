import asyncio
import random
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.humanresources.department import HumanResourcesDepartmentModel  # noqa: F401
from api.models.humanresources.employee import HumanResourcesEmployeeModel
from api.models.humanresources.jobrole import HumanResourcesJobRoleModel  # noqa: F401
from api.models.person.country_region import CountryRegionModel  # noqa: F401
from api.models.person.state_province import StateProvinceModel  # noqa: F401
from api.utils.cli_common import build_parser
from api.utils.mutator_common import add_mutation_args, resolve_mutation_args, run_mutation

# The four imports above are unused directly in this file but required: employee has FKs to
# all of them (jobrole, department, person.countryregion, person.stateprovince), and SQLAlchemy
# only registers a model's Table on Base.metadata once that model class has actually been
# imported somewhere in the process. run_mutation()'s bulk UPDATE needs every FK target
# resolvable to sort tables, so skipping any of these raises `NoReferencedTableError` at
# runtime (confirmed live 2026-09-04) rather than at import time — the regular (non-mutate)
# humanresources_employee.py seeder already imports all four for the same reason.

SCHEMA_NAME = "humanresources"
TABLE_NAME = "humanresources.employee"
HARD_DELETE_ALLOWED = False  # referenced by purchaseorderaudit (approver) — see
# mutator_common.py's bulk_hard_delete() comment; random sampling doesn't check for inbound
# FKs, so a hard delete here fails ~1-in-N with IntegrityError whenever it samples a
# still-referenced employee (confirmed live 2026-09-04). Soft-delete (is_active=false) is
# also the more realistic scenario for an employee who left the company anyway.


def apply_updates(rows: list[dict]) -> list[dict]:
    # Raise-style update only — deliberately not touching department_id/manager_id/jobrole_id
    # here since those need a real FK fetch to stay valid, unlike a scalar salary bump.
    for row in rows:
        row["current_salary"] = Decimal(random.uniform(2_000.0, 25_000.0)).quantize(Decimal("0.01"))
    return rows


async def mutate(count: int, delete_ratio: float, batch_size: int):
    async with AsyncSessionLocal() as session:
        await run_mutation(
            session,
            HumanResourcesEmployeeModel,
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
