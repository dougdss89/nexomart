import logging
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.product.product_repository import ProductRepository

logger = logging.getLogger(__name__)

class ProductService:
    """ Regras de Negócio para Produtos Brutos (Orquestrador) """
    
    @staticmethod
    async def process_bulk_insert(db: AsyncSession, products_data: List[Dict[str, Any]]) -> bool:
        if not products_data:
            logger.info("Nenhum produto fornecido para inserção lótica.")
            return False
            
        return await ProductRepository.bulk_insert(db=db, products_data=products_data)
