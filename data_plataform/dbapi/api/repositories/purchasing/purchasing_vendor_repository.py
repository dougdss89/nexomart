import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.purchasing.vendor import PurchaseVendorModel as VendorModel

logger = logging.getLogger(__name__)

class PurchasingVendorRepository:
    """ Exclusively for DB operations on Vendor Entities """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, vendors_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(VendorModel).values(vendors_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert Vendors: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, vendor_id: int) -> Optional[VendorModel]:
        try:
            stmt = select(VendorModel).where(VendorModel.vendor_id == vendor_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching Vendor ID={vendor_id}: {e}")
            raise
