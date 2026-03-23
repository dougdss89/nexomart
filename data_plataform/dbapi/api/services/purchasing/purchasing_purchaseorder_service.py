import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.purchasing.purchasing_purchaseorder_repository import PurchasingPurchaseOrderRepository

logger = logging.getLogger(__name__)

class PurchasingPurchaseOrderService:
    """ Lógicas Financeiras/Logísticas de Ordem de Compra """
    
    @staticmethod
    async def process_bulk_integration(db: AsyncSession, orders_data: List[Dict[str, Any]]) -> bool:
        if not orders_data:
            return False
            
        return await PurchasingPurchaseOrderRepository.bulk_insert(db=db, orders_data=orders_data)
