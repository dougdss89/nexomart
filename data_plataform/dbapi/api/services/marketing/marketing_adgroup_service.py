import logging
from typing import Any

from api.repositories.marketing.marketing_adgroup_repository import MarketingAdGroupRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class MarketingAdGroupService:
    """Logic for Ad Groups and Hidden Targeting Segments"""

    @staticmethod
    async def process_bulk_integration(
        db: AsyncSession, adgroups_data: list[dict[str, Any]]
    ) -> bool:
        if not adgroups_data:
            return False
        return await MarketingAdGroupRepository.bulk_insert(db=db, adgroups_data=adgroups_data)
