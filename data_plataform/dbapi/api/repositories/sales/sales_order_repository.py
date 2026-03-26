from sqlalchemy import select, insert, update
from sqlalchemy.ext.asyncio import AsyncSession
from api.models.sales.salesorder import SalesOrderModel
from typing import List, Optional

class SalesOrderRepository:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_id(self, salesorder_id: int) -> Optional[SalesOrderModel]:
        query = select(SalesOrderModel).where(SalesOrderModel.salesorder_id == salesorder_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_by_order_number(self, order_number: str) -> Optional[SalesOrderModel]:
        query = select(SalesOrderModel).where(SalesOrderModel.salesordernumber == order_number)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def create(self, data: dict) -> SalesOrderModel:
        obj = SalesOrderModel(**data)
        self.db.add(obj)
        await self.db.flush()
        return obj

    async def bulk_create(self, data_list: List[dict]):
        if not data_list:
            return
        stmt = insert(SalesOrderModel).values(data_list)
        await self.db.execute(stmt)
        await self.db.flush()

    async def update_status(self, salesorder_id: int, new_status: str):
        stmt = (
            update(SalesOrderModel)
            .where(SalesOrderModel.salesorder_id == salesorder_id)
            .values(order_status=new_status)
        )
        await self.db.execute(stmt)
        await self.db.flush()
