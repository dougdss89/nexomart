from api.models.sales.saleschannel import SalesChannelModel
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession


class SalesChannelRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self) -> list[SalesChannelModel]:
        query = select(SalesChannelModel).order_by(SalesChannelModel.saleschannel_name)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_by_id(self, saleschannel_id: int) -> SalesChannelModel | None:
        query = select(SalesChannelModel).filter(
            SalesChannelModel.saleschannel_id == saleschannel_id
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def bulk_upsert(self, data_list: list[dict]):
        """
        Bulk insert/update of sales channels.
        """
        if not data_list:
            return

        stmt = insert(SalesChannelModel).values(data_list)
        await self.session.execute(stmt)
        await self.session.flush()
