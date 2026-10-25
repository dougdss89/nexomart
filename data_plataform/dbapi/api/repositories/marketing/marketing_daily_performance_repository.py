import logging
from typing import Any

from api.models.marketing.marketing_dailyperformance import MarketingDailyPerformanceModel
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class MarketingDailyPerformanceRepository:
    """Exclusively for the Daily Analytical Performance Fact Table"""

    @staticmethod
    async def bulk_insert(db: AsyncSession, perf_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingDailyPerformanceModel).values(perf_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error Bulk Insert DailyPerformance Fact: {e}")
            raise
