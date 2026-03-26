from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.sales.return_order_repository import ReturnOrderRepository
from api.schemas.sales.sales_return_order import SalesReturnOrderSchema
from api.models.sales.returnorder import SalesReturnOrderModel

class ReturnOrderService:
    def __init__(self, session: AsyncSession):
        self.repository = ReturnOrderRepository(session)

    async def create_return(self, schema: SalesReturnOrderSchema) -> SalesReturnOrderModel:
        return_order = SalesReturnOrderModel(**schema.model_dump(exclude={"inspection_data"}))
        return_order.inspection_data = schema.inspection_data.model_dump()
        return await self.repository.create(return_order)

    async def bulk_create_returns(self, schemas: List[SalesReturnOrderSchema]) -> None:
        payloads = []
        for schema in schemas:
            data = schema.model_dump(exclude={"inspection_data"})
            data["inspection_data"] = schema.inspection_data.model_dump()
            payloads.append(data)
        await self.repository.bulk_create(payloads)

    async def get_return(self, return_id: str) -> Optional[SalesReturnOrderModel]:
        return await self.repository.get_by_id(return_id)

    async def get_returns_by_order(self, salesordernumber: str) -> List[SalesReturnOrderModel]:
        return await self.repository.get_by_order_number(salesordernumber)
