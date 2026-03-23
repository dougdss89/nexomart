import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.marketing.marketing_adgroup_repository import MarketingAdGroupRepository

logger = logging.getLogger(__name__)

class MarketingAdGroupService:
    """ Lógica para Conjuntos de Anúncios e Segmentações Ocultas """
    
    @staticmethod
    async def process_bulk_integration(db: AsyncSession, adgroups_data: List[Dict[str, Any]]) -> bool:
        if not adgroups_data:
            return False
        return await MarketingAdGroupRepository.bulk_insert(db=db, adgroups_data=adgroups_data)
