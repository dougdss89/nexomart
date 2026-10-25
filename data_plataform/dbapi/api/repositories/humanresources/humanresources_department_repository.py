import logging
from typing import Any

from api.models.humanresources.department import HumanResourcesDepartmentModel
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class HumanResourcesDepartmentRepository:
    """Exclusively for DB operations on the Department trees"""

    @staticmethod
    async def bulk_insert(db: AsyncSession, depts_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(HumanResourcesDepartmentModel).values(depts_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert Departments: {e}")
            raise

    @staticmethod
    async def get_by_id(
        db: AsyncSession, department_id: int
    ) -> HumanResourcesDepartmentModel | None:
        try:
            stmt = select(HumanResourcesDepartmentModel).where(
                HumanResourcesDepartmentModel.department_id == department_id
            )
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching Dept ID={department_id}: {e}")
            raise
