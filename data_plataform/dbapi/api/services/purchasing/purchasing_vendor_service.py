import logging
from typing import Any

from api.repositories.purchasing.purchasing_vendor_repository import PurchasingVendorRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PurchasingVendorService:
    """Analytical Control over B2B Vendors"""

    @staticmethod
    async def process_bulk_integration(
        db: AsyncSession, vendors_data: list[dict[str, Any]]
    ) -> bool:
        if not vendors_data:
            return False

        return await PurchasingVendorRepository.bulk_insert(db=db, vendors_data=vendors_data)
