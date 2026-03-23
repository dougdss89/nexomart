import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.marketing.marketing_adgroup import MarketingAdGroupModel

logger = logging.getLogger(__name__)

class MarketingAdGroupRepository:
    """ Exclusivo para operações de banco de dados do Marketing AdGroup """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, adgroups_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingAdGroupModel).values(adgroups_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL Bulk Insert Marketing AdGroup: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, adgroup_id: int) -> Optional[MarketingAdGroupModel]:
        try:
            stmt = select(MarketingAdGroupModel).where(MarketingAdGroupModel.adgroup_id == adgroup_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Erro ao buscar MarketingAdGroup ID={adgroup_id}: {e}")
            raise
