import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta, date
from faker import Faker
from typing import Dict, List, Any
from sqlalchemy import select, insert, func
from api.database import AsyncSessionLocal

# Modelos
from api.models.humanresources.jobrole import HumanResourcesJobRoleModel
from api.models.humanresources.department import HumanResourcesDepartmentModel
from api.models.person.country_region import CountryRegionModel
from api.models.person.state_province import StateProvinceModel
from api.models.humanresources.employee import HumanResourcesEmployeeModel
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "humanresources"
TABLE_NAME = "humanresources.employee"

fake = Faker('pt_BR')

# Widened 2026-09-05 — hire_date was hardcoded to a single fixed value (date(2022, 1, 1))
# for every employee; now randomized across a realistic multi-year range.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)

def generate_role_attributes(role: str, is_manager: bool) -> dict:
    """Generates JSONB attributes specific to the role."""
    if role == "SALES":
        return {
            "annual_bonus_target_pct": round(random.uniform(5.0, 25.0), 2),
            "managed_regions": ["SP", "RJ"] if is_manager else ["SP"]
        }
    elif role == "PURCHASING":
        return {"approval_limit": random.randint(10000, 100000)}
    return {}

async def seed_humanresources_employees(count: int = 50, chaos_rate: float = 0.0, batch_size: int = 10_000, apply_chaos: bool = False):
    """Runs the employee load with full dimensional integrity.

    HumanResources is a critical dimension with no chaos engineering by design (CLAUDE.md);
    chaos_rate/apply_chaos are accepted only to keep the CLI uniform with the other
    volume-driven seeders, but have no effect here.
    """
    async with AsyncSessionLocal() as session:
        existing_count = await session.scalar(select(func.count(HumanResourcesEmployeeModel.employee_id)))
        # 1. Fetch base records (Dimensional Integrity)
        jobs = {row[1]: row[0] for row in (await session.execute(select(HumanResourcesJobRoleModel.jobrole_id, HumanResourcesJobRoleModel.job_title))).all()}
        depts = {row[1]: row[0] for row in (await session.execute(select(HumanResourcesDepartmentModel.department_id, HumanResourcesDepartmentModel.department_name))).all()}
        countries = {row[1]: row[0] for row in (await session.execute(select(CountryRegionModel.country_id, CountryRegionModel.country_code))).all()}

        # Map of states by country
        states_result = (await session.execute(select(StateProvinceModel.state_province_id, StateProvinceModel.country_id))).all()
        states_by_country = {}
        for row in states_result:
            states_by_country.setdefault(row[1], []).append(row[0])

        if not jobs or not depts or not countries:
            print("⚠️ Base tables (Jobs, Depts, Geography) are empty! Run the base seeders first.")
            return

        print(f"📣 Generating Corporate Hierarchy | {count:,} employees (existing: {existing_count:,})...")

        def build_employee(title: str, dept: str, country_code: str, is_mgr: bool, manager_id: int = None):
            cid = countries.get(country_code, random.choice(list(countries.values())))
            sid = random.choice(states_by_country.get(cid, [1]))

            first = fake.first_name()
            last = fake.last_name()
            
            return {
                "first_name": first,
                "last_name": last,
                "nationalidnumber": str(random.randint(100000000, 999999999)),
                "loginid": f"{first.lower()}.{last.lower()}",
                "email": f"{first.lower()}.{last.lower()}@empresa.com",
                "birthdate": date(1980, 1, 1),
                "hire_date": (datetime.now() - timedelta(days=random.randint(1, (datetime.now() - EARLIEST_TRANSACTION_DATE).days))).date(),
                "jobrole_id": jobs.get(title, random.choice(list(jobs.values()))),
                "department_id": depts.get(dept, random.choice(list(depts.values()))),
                "manager_id": manager_id,
                "country_id": cid,
                "state_province_id": sid,
                "role_attributes": generate_role_attributes("SALES" if "Vendas" in title else "GENERAL", is_mgr),
                "current_salary": random.randint(5000, 25000),
                "is_current": True,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "is_active": True,
                "row_guid": uuid.uuid4()
            }

        # Hierarchy simulation: the first employee in the batch is always the CEO (only if there
        # are no employees yet), the rest are generic and linked to real IDs.
        batch = []
        if existing_count == 0 and count > 0:
            batch.append(build_employee("Chief Executive Officer (CEO)", "executive", "US", True))

        while len(batch) < count:
            batch.append(build_employee(random.choice(list(jobs.keys())), random.choice(list(depts.keys())), random.choice(list(countries.keys())), False))

        try:
            for i in range(0, len(batch), batch_size):
                stmt = insert(HumanResourcesEmployeeModel).values(batch[i:i + batch_size])
                await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(batch)} Employees seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding employees: {e}")
            raise RuntimeError(f"Employee seeding aborted: {e}") from e

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_humanresources_employees(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
