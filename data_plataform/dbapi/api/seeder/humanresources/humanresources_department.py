import asyncio
import os
import sys
import uuid
from datetime import datetime

from api.database import AsyncSessionLocal
from api.models.humanresources.department import HumanResourcesDepartmentModel
from api.samples.hr_department_organization import ORG_CHART

# Adjust the imports to your project's correct path
from api.schemas.humanresources.humanresources_department import (
    DEPARTMENT_TO_GROUP_MAP,
    HRDepartment,
)
from api.utils.cli_common import build_parser, skip_if_already_seeded, stamp_dimension_history
from api.utils.quarantine import get_quarantine, physical_gate
from sqlalchemy import insert

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))


def get_department_seeders() -> list[dict]:
    """
    Generates department seeders based on the hierarchical structure.
    """

    batch = []
    q = get_quarantine("humanresources.department")
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

        payload = {
            "department_id": department_id_counter,
            "department_name": dept_name,
            "group_name": DEPARTMENT_TO_GROUP_MAP.get(dept_name),
            "parent_department_id": parent_id,
            "manager_id": None,  # You can populate this with employee IDs later, if you want
            "created_at": today,
            "updated_at": today,
            "is_active": True,
            "row_guid": uuid.uuid4(),
        }
        with q.row(payload):
            batch.append(HRDepartment(**payload).model_dump())

        # 3. Update the map for future use
        department_map[dept_name] = department_id_counter

        department_id_counter += 1

    q.finish()
    return batch


async def seed_departments():
    """Runs the department load (Master Data — fixed org chart, no chaos)."""
    async with AsyncSessionLocal() as session:
        depts = get_department_seeders()
        if await skip_if_already_seeded(
            session, HumanResourcesDepartmentModel, label="Departments"
        ):
            return
        print(f"📣 Seeding {len(depts)} departments...")
        depts = physical_gate(HumanResourcesDepartmentModel, depts)
        try:
            stmt = insert(HumanResourcesDepartmentModel).values(stamp_dimension_history(depts))
            await session.execute(stmt)
            await session.commit()
            print("✅ Departments seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding departments: {e}")
            raise RuntimeError(f"Department seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser("Seeder for humanresources.department (fixed master data, no chaos)")
    parser.parse_args()

    asyncio.run(seed_departments())
