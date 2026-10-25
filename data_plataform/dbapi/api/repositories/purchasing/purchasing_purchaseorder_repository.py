import logging
from typing import Any

from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel as PurchaseOrderModel
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PurchasingPurchaseOrderRepository:
    """Exclusively for B2B Fact operations (Orders and Details)"""

    @staticmethod
    async def bulk_insert(db: AsyncSession, orders_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(PurchaseOrderModel).values(orders_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert Purchase Orders: {e}")
            raise

    @staticmethod
    async def get_by_id(db: AsyncSession, purchase_order_id: int) -> PurchaseOrderModel | None:
        try:
            stmt = select(PurchaseOrderModel).where(
                PurchaseOrderModel.purchase_order_id == purchase_order_id
            )
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching Purchase Order ID={purchase_order_id}: {e}")
            raise
