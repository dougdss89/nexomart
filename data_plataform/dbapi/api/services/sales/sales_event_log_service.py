from typing import List
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.sales.sales_event_log_repository import SalesEventLogRepository
from api.schemas.sales.sales_eventlog import SalesOrderEventLogSchema
from api.models.sales.ordereventlog import SalesOrderEventLogModel

class SalesEventLogService:
    def __init__(self, session: AsyncSession):
        self.repository = SalesEventLogRepository(session)

    async def get_order_history(self, order_id: int) -> List[SalesOrderEventLogModel]:
        return await self.repository.get_by_order_id(order_id)

    async def process_bulk_events(self, events_data: List[SalesOrderEventLogSchema]):
        """
        Processamento massiva de logs de eventos de pedidos.
        """
        processed_data = [item.model_dump() for item in events_data]
        await self.repository.bulk_insert(processed_data)
