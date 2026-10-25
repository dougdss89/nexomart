from api.models.sales.saleschannel import SalesChannelModel
from api.repositories.sales.sales_channel_repository import SalesChannelRepository
from api.schemas.sales.sales_channel import SalesChannelSchema
from sqlalchemy.ext.asyncio import AsyncSession


class SalesChannelService:
    def __init__(self, session: AsyncSession):
        self.repository = SalesChannelRepository(session)

    async def list_channels(self) -> list[SalesChannelModel]:
        return await self.repository.get_all()

    async def seed_initial_channels(self, channels_data: list[SalesChannelSchema]):
        """
        Popula o banco com canais de venda iniciais (Mestre).
        """
        processed_data = [item.model_dump() for item in channels_data]
        await self.repository.bulk_upsert(processed_data)
