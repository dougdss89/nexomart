import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from api.repositories.product.product_category_repository import ProductCategoryRepository
from api.models.product.productcategory import ProductCategoryModel

logger = logging.getLogger(__name__)

class ProductCategoryService:
    """
    Business Rules and Orchestration Layer (Abstract).
    Consumes the Repository to access data.
    MUST NEVER import SQL syntax such as 'select' or 'insert' and cannot manipulate the DB directly.
    """

    @staticmethod
    async def process_bulk_insert(db: AsyncSession, categories_data: List[Dict[str, Any]]) -> bool:
        """
        Ensures logical validations and forwards the bulk operation to the DB.
        (Example: Checks permissions, validates anomalies in the logical dictionaries).
        """
        if not categories_data:
            logger.info("No category provided for batch insertion.")
            return False

        # Orchestration rule: call the purely structured DB class
        return await ProductCategoryRepository.bulk_insert(db=db, categories_data=categories_data)

    @staticmethod
    async def verify_category_exists(db: AsyncSession, category_id: int) -> bool:
        """
        Business Rule: Checks whether the hierarchy exists in the database.
        Returns a boolean abstracting the Model — the Router doesn't need to read the SQL Model, just know if it's Valid or not.
        """
        category: Optional[ProductCategoryModel] = await ProductCategoryRepository.get_by_id(db, category_id)
        return category is not None