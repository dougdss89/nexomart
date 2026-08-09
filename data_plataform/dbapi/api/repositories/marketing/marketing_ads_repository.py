import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.marketing.marketing_advertise import MarketingAdvertiseModel

logger = logging.getLogger(__name__)

class MarketingAdsRepository:
    """ Exclusively for database operations on Ads and Creatives """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, ads_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingAdvertiseModel).values(ads_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert Marketing Ads: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, ad_id: int) -> Optional[MarketingAdvertiseModel]:
        try:
            stmt = select(MarketingAdvertiseModel).where(MarketingAdvertiseModel.ad_id == ad_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching MarketingAd ID={ad_id}: {e}")
            raise
