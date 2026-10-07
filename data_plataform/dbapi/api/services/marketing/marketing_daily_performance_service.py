import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.marketing.marketing_daily_performance_repository import MarketingDailyPerformanceRepository

logger = logging.getLogger(__name__)

class MarketingDailyPerformanceService:
    """ Transactional-rule processor for facts (time-series metrics) """
    
    @staticmethod
    async def process_bulk_integration(db: AsyncSession, perf_data: List[Dict[str, Any]]) -> bool:
        if not perf_data:
            return False
        return await MarketingDailyPerformanceRepository.bulk_insert(db=db, perf_data=perf_data)
