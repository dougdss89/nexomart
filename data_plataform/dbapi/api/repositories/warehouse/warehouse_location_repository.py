import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.warehouse.warehouselocation import WarehouseLocationModel

logger = logging.getLogger(__name__)

class WarehouseLocationRepository:
    """ Exclusively for database operations on Warehouse Locations """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, locations_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(WarehouseLocationModel).values(locations_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert WarehouseLocation: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, warehouse_id: int) -> Optional[WarehouseLocationModel]:
        try:
            stmt = select(WarehouseLocationModel).where(WarehouseLocationModel.warehouse_id == warehouse_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching WarehouseLocation ID={warehouse_id}: {e}")
            raise
            
    @staticmethod
    async def list_all(db: AsyncSession, limit: int = 100) -> List[WarehouseLocationModel]:
        try:
            stmt = select(WarehouseLocationModel).limit(limit)
            result = await db.execute(stmt)
            return result.scalars().all()
        except SQLAlchemyError as e:
            logger.error(f"Error listing WarehouseLocations: {e}")
            raise
