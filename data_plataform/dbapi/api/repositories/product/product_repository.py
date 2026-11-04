import logging
from typing import Any

from api.models.product.product import Product
from sqlalchemy import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ProductRepository:
    """Data layer for Products"""

    @staticmethod
    async def bulk_insert(db: AsyncSession, products_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(Product).values(products_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL ao inserir Produtos em lote: {e}")
            raise
