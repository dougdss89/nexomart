import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.purchasing.purchaseorder import PurchaseOrderModel

logger = logging.getLogger(__name__)

class PurchasingPurchaseOrderRepository:
    """ Exclusivo para operações Fato (Orders e Details) de B2B """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, orders_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(PurchaseOrderModel).values(orders_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL Bulk Insert Purchase Orders: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, purchase_order_id: int) -> Optional[PurchaseOrderModel]:
        try:
            stmt = select(PurchaseOrderModel).where(PurchaseOrderModel.purchase_order_id == purchase_order_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Erro ao buscar Purchase Order ID={purchase_order_id}: {e}")
            raise
