from typing import List, Optional
from sqlalchemy import select, insert, update
from sqlalchemy.ext.asyncio import AsyncSession
from api.models.sales.returnorder import SalesReturnOrderModel

class ReturnOrderRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, return_order: SalesReturnOrderModel) -> SalesReturnOrderModel:
        self.session.add(return_order)
        return return_order

    async def bulk_create(self, return_orders: List[dict]) -> None:
        if not return_orders:
            return
        stmt = insert(SalesReturnOrderModel).values(return_orders)
        await self.session.execute(stmt)

    async def get_by_id(self, return_id: str) -> Optional[SalesReturnOrderModel]:
        stmt = select(SalesReturnOrderModel).where(SalesReturnOrderModel.return_id == return_id)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def get_by_order_number(self, salesordernumber: str) -> List[SalesReturnOrderModel]:
        stmt = select(SalesReturnOrderModel).where(SalesReturnOrderModel.salesordernumber == salesordernumber)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def update_status(self, return_id: str, status: str) -> None:
        stmt = update(SalesReturnOrderModel).where(SalesReturnOrderModel.return_id == return_id).values(return_status=status)
        await self.session.execute(stmt)
