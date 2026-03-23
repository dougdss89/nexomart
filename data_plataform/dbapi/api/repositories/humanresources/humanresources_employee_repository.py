import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.humanresources.humanresources_employee import HumanResourcesEmployeeModel

logger = logging.getLogger(__name__)

class HumanResourcesEmployeeRepository:
    """ Operações puras do BD (SQL) mapeando os Colaboradores Físicos """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, employees_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(HumanResourcesEmployeeModel).values(employees_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL Bulk Insert Employees: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, employee_id: int) -> Optional[HumanResourcesEmployeeModel]:
        try:
            stmt = select(HumanResourcesEmployeeModel).where(HumanResourcesEmployeeModel.employee_id == employee_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Erro ao buscar Employee ID={employee_id}: {e}")
            raise
