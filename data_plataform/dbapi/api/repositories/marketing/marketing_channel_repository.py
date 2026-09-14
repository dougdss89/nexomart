import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.marketing.marketing_channel import MarketingChannelModel

logger = logging.getLogger(__name__)

class MarketingChannelRepository:
    """ Exclusively for database operations on the Wide Dimension (Channel/Platform) """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, channels_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingChannelModel).values(channels_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert Marketing Channel: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, channel_id: int) -> Optional[MarketingChannelModel]:
        try:
            stmt = select(MarketingChannelModel).where(MarketingChannelModel.channel_id == channel_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching MarketingChannel ID={channel_id}: {e}")
            raise
