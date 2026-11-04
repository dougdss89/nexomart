import logging
from typing import Any

from api.repositories.marketing.marketing_ads_repository import MarketingAdsRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class MarketingAdsService:
    """Services governing Media Creative Assets"""

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, ads_data: list[dict[str, Any]]) -> bool:
        if not ads_data:
            return False
        return await MarketingAdsRepository.bulk_insert(db=db, ads_data=ads_data)
