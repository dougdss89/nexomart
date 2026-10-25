import logging
from typing import Any

from api.repositories.product.product_repository import ProductRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ProductService:
    """Business Rules for Raw Products (Orchestrator)"""

    @staticmethod
    async def process_bulk_insert(db: AsyncSession, products_data: list[dict[str, Any]]) -> bool:
        if not products_data:
            logger.info("No product provided for batch insertion.")
            return False

        return await ProductRepository.bulk_insert(db=db, products_data=products_data)
