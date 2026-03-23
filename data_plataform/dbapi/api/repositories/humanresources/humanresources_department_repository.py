import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.humanresources.humanresources_department import HumanResourcesDepartmentModel

logger = logging.getLogger(__name__)

class HumanResourcesDepartmentRepository:
    """ Exclusivo para operações de BD das árvores de Departamentos """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, depts_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(HumanResourcesDepartmentModel).values(depts_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL Bulk Insert Departamentos: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, department_id: int) -> Optional[HumanResourcesDepartmentModel]:
        try:
            stmt = select(HumanResourcesDepartmentModel).where(HumanResourcesDepartmentModel.department_id == department_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Erro ao buscar Dept ID={department_id}: {e}")
            raise
