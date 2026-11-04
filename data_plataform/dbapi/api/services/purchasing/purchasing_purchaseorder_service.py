import logging
from typing import Any

from api.repositories.purchasing.purchasing_purchaseorder_repository import (
    PurchasingPurchaseOrderRepository,
)
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PurchasingPurchaseOrderService:
    """Financial/Logistical Logic for Purchase Orders"""

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, orders_data: list[dict[str, Any]]) -> bool:
        if not orders_data:
            return False

        return await PurchasingPurchaseOrderRepository.bulk_insert(db=db, orders_data=orders_data)
