import logging
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.product.product_model_repository import ProductModelRepository

logger = logging.getLogger(__name__)

class ProductModelService:
    """ Business Rules for Product Models (Orchestrator) """

    @staticmethod
    async def process_bulk_insert(db: AsyncSession, models_data: List[Dict[str, Any]]) -> bool:
        if not models_data:
            logger.info("No model (Product Model) provided for batch insertion.")
            return False
            
        return await ProductModelRepository.bulk_insert(db=db, models_data=models_data)
