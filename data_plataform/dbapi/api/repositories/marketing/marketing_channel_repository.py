import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.marketing.marketing_channel import MarketingChannelModel

logger = logging.getLogger(__name__)

class MarketingChannelRepository:
    """ Exclusivo para operações de banco de dados do Wide Dimension (Canal/Platform) """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, channels_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingChannelModel).values(channels_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL Bulk Insert Marketing Channel: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, channel_id: int) -> Optional[MarketingChannelModel]:
        try:
            stmt = select(MarketingChannelModel).where(MarketingChannelModel.channel_id == channel_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Erro ao buscar MarketingChannel ID={channel_id}: {e}")
            raise
