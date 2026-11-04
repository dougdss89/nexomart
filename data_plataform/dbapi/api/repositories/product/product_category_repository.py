import logging
from typing import Any

from api.models.product.productcategory import ProductCategoryModel
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ProductCategoryRepository:
    """
    Exclusive Data Access Layer (SQLAlchemy).
    No Business Rules should live here. Only raw CRUD execution.
    """

    @staticmethod
    async def bulk_insert(db: AsyncSession, categories_data: list[dict[str, Any]]) -> bool:
        """Executes an optimized batch insert of Product Categories."""
        try:
            stmt = insert(ProductCategoryModel).values(categories_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error inserting Categories in batch: {e}")
            raise

    @staticmethod
    async def get_by_id(db: AsyncSession, category_id: int) -> ProductCategoryModel | None:
        """Fetches a specific Category from the database."""
        try:
            stmt = select(ProductCategoryModel).where(
                ProductCategoryModel.category_id == category_id
            )
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"SQL Error fetching Category {category_id}: {e}")
            raise
