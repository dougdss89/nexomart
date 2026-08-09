import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.marketing.marketing_campaign import MarketingCampaignModel

logger = logging.getLogger(__name__)

class MarketingCampaignRepository:
    """ Exclusively for Marketing Campaign database operations """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, campaigns_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingCampaignModel).values(campaigns_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert Marketing Campaign: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, campaign_id: int) -> Optional[MarketingCampaignModel]:
        try:
            stmt = select(MarketingCampaignModel).where(MarketingCampaignModel.campaign_id == campaign_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching MarketingCampaign ID={campaign_id}: {e}")
            raise
