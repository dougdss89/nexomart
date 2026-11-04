import logging
from typing import Any

from api.repositories.product.product_embedding_repository import ProductEmbeddingRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ProductEmbeddingService:
    """Business Rules for Vector Embeddings (Orchestrator)"""

    @staticmethod
    async def process_bulk_insert(db: AsyncSession, embeddings_data: list[dict[str, Any]]) -> bool:
        if not embeddings_data:
            logger.info("No vector embedding flow listed.")
            return False

        return await ProductEmbeddingRepository.bulk_insert(db=db, embeddings_data=embeddings_data)
