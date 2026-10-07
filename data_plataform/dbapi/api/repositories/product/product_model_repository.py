import logging
from typing import List, Dict, Any
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from api.models.product.productmodel import ProductModel

logger = logging.getLogger(__name__)

class ProductModelRepository:
    """ Data layer for Product Models """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, models_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(ProductModel).values(models_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL ao inserir Modelos de Produtos: {e}")
            raise
