from typing import List, Optional
from sqlalchemy import select, insert
from sqlalchemy.ext.asyncio import AsyncSession
from api.models.sales.ordereventlog import SalesOrderEventLogModel

class SalesEventLogRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_order_id(self, order_id: int) -> List[SalesOrderEventLogModel]:
        query = select(SalesOrderEventLogModel).filter(
            SalesOrderEventLogModel.salesorder_id == order_id
        ).order_by(SalesOrderEventLogModel.event_date.asc())
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def bulk_insert(self, data_list: List[dict]):
        """
        Inserção massiva de logs de eventos.
        """
        if not data_list:
            return
        
        stmt = insert(SalesOrderEventLogModel).values(data_list)
        await self.session.execute(stmt)
        await self.session.flush()
