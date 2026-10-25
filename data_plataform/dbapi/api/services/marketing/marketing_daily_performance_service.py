import logging
from typing import Any

from api.repositories.marketing.marketing_daily_performance_repository import (
    MarketingDailyPerformanceRepository,
)
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class MarketingDailyPerformanceService:
    """Transactional-rule processor for facts (time-series metrics)"""

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, perf_data: list[dict[str, Any]]) -> bool:
        if not perf_data:
            return False
        return await MarketingDailyPerformanceRepository.bulk_insert(db=db, perf_data=perf_data)
