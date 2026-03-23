import logging
from typing import List, Dict, Any
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError
from api.models.product.product import Product

logger = logging.getLogger(__name__)

class ProductRepository:
    """ Camada de Dados para Produtos """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, products_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(Product).values(products_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL ao inserir Produtos em lote: {e}")
            raise
