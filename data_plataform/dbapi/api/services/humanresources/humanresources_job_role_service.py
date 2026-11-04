import logging
from typing import Any

from api.repositories.humanresources.humanresources_job_role_repository import (
    HumanResourcesJobRoleRepository,
)
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class HumanResourcesJobRoleService:
    """Orchestration of Job Roles (Salary History and Titles)"""

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, roles_data: list[dict[str, Any]]) -> bool:
        if not roles_data:
            return False

        return await HumanResourcesJobRoleRepository.bulk_insert(db=db, roles_data=roles_data)
