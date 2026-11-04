import logging
from typing import Any

from api.repositories.marketing.marketing_channel_repository import MarketingChannelRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class MarketingChannelService:
    """Controls the business logic of Dimensional Channels and Platforms"""

    @staticmethod
    async def process_bulk_integration(
        db: AsyncSession, channels_data: list[dict[str, Any]]
    ) -> bool:
        if not channels_data:
            return False
        return await MarketingChannelRepository.bulk_insert(db=db, channels_data=channels_data)
