import uuid
import asyncio
import sys
import os
from datetime import datetime
from sqlalchemy import insert
from api.database import AsyncSessionLocal

# Adjust the imports to your project's correct path
from api.schemas.humanresources.humanresources_department import (
    HRDepartment,
    DEPARTMENT_TO_GROUP_MAP
)

from api.samples.hr_department_organization import ORG_CHART
from api.models.humanresources.department import HumanResourcesDepartmentModel
from api.seeder.cli_common import build_parser

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_department_seeders() -> list[dict]:
    """
    Generates department seeders based on the hierarchical structure.
    """

    batch = []
    department_id_counter = 1

    # Dictionary to map department names to their IDs
    # This is needed to build the relationships (parent_department_id)
    department_map = {}

    today = datetime.now()

    for dept_name, parent_name in ORG_CHART:

        # 1. Get the parent's ID
        parent_id = None
        if parent_name:
            # If the parent hasn't been processed yet (unlikely given ORG_CHART's order, but safe to check)
            if parent_name not in department_map:
                # Fallback: if the parent isn't in the map, we assume it's a higher-level department
                # that should already have been created. For this example, we assume ORG_CHART's
                # order guarantees the parent comes first.
                pass

            parent_id = department_map.get(parent_name)

        # 2. Create the current department record
        # The group is determined automatically by the Pydantic validator

        # Note: to keep the seeder simple, we only create "current" records (is_active=True, effective_end_date=None)
        # If you want history, you'd need more complex logic here too.

        batch.append({
            "department_id": department_id_counter,
            "department_name": dept_name,
            "group_name": DEPARTMENT_TO_GROUP_MAP.get(dept_name),
            "parent_department_id": parent_id,
            "manager_id": None, # You can populate this with employee IDs later, if you want

            "created_at": today,
            "updated_at": today,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })

        # 3. Update the map for future use
        department_map[dept_name] = department_id_counter

        department_id_counter += 1

    return batch


async def seed_departments():
    """Runs the department load (Master Data — fixed org chart, no chaos)."""
    async with AsyncSessionLocal() as session:
        depts = get_department_seeders()
        print(f"📣 Seeding {len(depts)} departments...")
        try:
            stmt = insert(HumanResourcesDepartmentModel).values(depts)
            await session.execute(stmt)
            await session.commit()
            print("✅ Departments seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding departments: {e}")


if __name__ == "__main__":
    parser = build_parser("Seeder for humanresources.department (fixed master data, no chaos)")
    parser.parse_args()

    asyncio.run(seed_departments())
