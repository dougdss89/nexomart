import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.marketing.marketing_dailyperformance import MarketingDailyPerformanceModel

logger = logging.getLogger(__name__)

class MarketingDailyPerformanceRepository:
    """ Exclusivo para Tabela Fato de Performance Analítica Diária """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, perf_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(MarketingDailyPerformanceModel).values(perf_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL Bulk Insert DailyPerformance Fato: {e}")
            raise
