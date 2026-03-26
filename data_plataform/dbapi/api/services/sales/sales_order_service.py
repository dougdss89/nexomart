from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.sales.sales_order_repository import SalesOrderRepository
from api.schemas.sales.sales_order import SalesOrderSchema

class SalesOrderService:
    def __init__(self, db: AsyncSession):
        self.repository = SalesOrderRepository(db)

    async def get_order(self, salesorder_id: int):
        return await self.repository.get_by_id(salesorder_id)

    async def get_order_by_number(self, order_number: str):
        return await self.repository.get_by_order_number(order_number)

    async def create_order(self, order_data: dict):
        # Business Logic could go here (e.g. inventory check)
        return await self.repository.create(order_data)

    async def bulk_ingest_orders(self, orders: List[dict]):
        """Ingestão massiva de ordens de venda."""
        await self.repository.bulk_create(orders)

    async def update_order_status(self, salesorder_id: int, status: str):
        await self.repository.update_status(salesorder_id, status)
