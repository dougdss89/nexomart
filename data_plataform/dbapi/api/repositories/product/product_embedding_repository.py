import logging
from typing import Any

from api.models.product.product_embedding import ProductEmbeddingModel
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ProductEmbeddingRepository:
    """Data layer for Embeddings"""

    @staticmethod
    async def bulk_insert(db: AsyncSession, embeddings_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(ProductEmbeddingModel).values(embeddings_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL ao inserir Embeddings em lote: {e}")
            raise
