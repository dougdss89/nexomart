import logging
from typing import Any

from api.models.humanresources.jobrole import HumanResourcesJobRoleModel
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class HumanResourcesJobRoleRepository:
    """Data operator for the polymorphic SCD-2 table of Historical Job Roles and Salaries"""

    @staticmethod
    async def bulk_insert(db: AsyncSession, roles_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(HumanResourcesJobRoleModel).values(roles_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert JobRoles: {e}")
            raise

    @staticmethod
    async def get_by_id(db: AsyncSession, job_role_id: int) -> HumanResourcesJobRoleModel | None:
        try:
            stmt = select(HumanResourcesJobRoleModel).where(
                HumanResourcesJobRoleModel.job_role_id == job_role_id
            )
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching JobRole ID={job_role_id}: {e}")
            raise
