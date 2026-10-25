import logging
from typing import Any

from api.repositories.humanresources.humanresources_department_repository import (
    HumanResourcesDepartmentRepository,
)
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class HumanResourcesDepartmentService:
    """Logical Orchestration of Operational Departments"""

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, depts_data: list[dict[str, Any]]) -> bool:
        if not depts_data:
            return False

        return await HumanResourcesDepartmentRepository.bulk_insert(db=db, depts_data=depts_data)
