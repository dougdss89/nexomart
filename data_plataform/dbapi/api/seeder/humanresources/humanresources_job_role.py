import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import insert
from api.database import AsyncSessionLocal

from api.samples.job_roles import JOB_ROLES
from api.models.humanresources.jobrole import HumanResourcesJobRoleModel
from api.seeder.cli_common import build_parser

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_job_role_seeders() -> list[dict]:

    """
    Generates job role seeders with salary base and professional history.
    """

    batch = []

    job_role_id_counter = 1

    today = datetime.now()

    for title, min_salary, max_salary in JOB_ROLES:

        has_raise = random.choice([True, False, False])

        if has_raise:
            #generates an old or inactive record
            #valid for 3 years or 1 year
            #min and max salary with a 20% raise

            old_start = today - timedelta(days=random.randint(700, 1500))
            old_end = old_start + timedelta(days=random.randint(180, 600))

            # Logical adjustment: the old salary was lower than the current one,
            # ranging from a 10 to 25% difference.
            growth_factor = random.uniform(0.75, 0.90)
            old_min_salary = round(min_salary * growth_factor, 2)
            old_max_salary = round(max_salary * growth_factor, 2)

            batch.append({
                "jobrole_id": job_role_id_counter,
                "job_title": title,
                "min_salary": old_min_salary,
                "max_salary": old_max_salary,
                "effective_start_date": old_start,
                "effective_end_date": old_end,
                "is_current": False,

                "created_at": today,
                "updated_at": today,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })

            job_role_id_counter += 1

            current_start = old_end + timedelta(seconds=1)

        else:
            current_start = today - timedelta(days = random.randint(500, 1000))


        #current record
        batch.append({
            "jobrole_id": job_role_id_counter,
            "job_title": title,
            "min_salary": min_salary,
            "max_salary": max_salary,
            "effective_start_date": current_start,
            "effective_end_date": None,
            "is_current": True,

            "created_at": today,
            "updated_at": today,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })

        job_role_id_counter += 1

    return batch


async def seed_job_roles():
    """Runs the job role load with salary history (Master Data — fixed list, no chaos)."""
    async with AsyncSessionLocal() as session:
        roles = get_job_role_seeders()
        print(f"📣 Seeding {len(roles)} job role records (including history)...")
        try:
            stmt = insert(HumanResourcesJobRoleModel).values(roles)
            await session.execute(stmt)
            await session.commit()
            print("✅ Job roles seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding job roles: {e}")


if __name__ == "__main__":
    parser = build_parser("Seeder for humanresources.jobrole (fixed master data, no chaos)")
    parser.parse_args()

    asyncio.run(seed_job_roles())