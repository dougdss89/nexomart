import logging
from typing import Any

from api.models.product.productmodel import ProductModel
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ProductModelRepository:
    """Data layer for Product Models"""

    @staticmethod
    async def bulk_insert(db: AsyncSession, models_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(ProductModel).values(models_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL ao inserir Modelos de Produtos: {e}")
            raise
