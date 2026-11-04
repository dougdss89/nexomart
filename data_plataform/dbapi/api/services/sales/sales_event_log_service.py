from api.models.sales.ordereventlog import SalesOrderEventLogModel
from api.repositories.sales.sales_event_log_repository import SalesEventLogRepository
from api.schemas.sales.sales_eventlog import SalesOrderEventLogSchema
from sqlalchemy.ext.asyncio import AsyncSession


class SalesEventLogService:
    def __init__(self, session: AsyncSession):
        self.repository = SalesEventLogRepository(session)

    async def get_order_history(self, order_id: int) -> list[SalesOrderEventLogModel]:
        return await self.repository.get_by_order_id(order_id)

    async def process_bulk_events(self, events_data: list[SalesOrderEventLogSchema]):
        """
        Processamento massiva de logs de eventos de pedidos.
        """
        processed_data = []
        for item in events_data:
            data = item.model_dump()
            data["salesorder_id"] = data.pop("order_id")  # nome da coluna real difere do schema
            if data.get("event_id") is None:
                data.pop("event_id", None)  # deixa o autoincrement do banco assumir
            processed_data.append(data)
        await self.repository.bulk_insert(processed_data)
