import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.marketing.marketing_adgroup import MarketingAdGroupModel

logger = logging.getLogger(__name__)

class MarketingAdGroupRepository:
    """ Exclusively for Marketing AdGroup database operations """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, adgroups_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingAdGroupModel).values(adgroups_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert Marketing AdGroup: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, ad_group_id: int) -> Optional[MarketingAdGroupModel]:
        try:
            stmt = select(MarketingAdGroupModel).where(MarketingAdGroupModel.ad_group_id == ad_group_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Error fetching MarketingAdGroup ID={ad_group_id}: {e}")
            raise
