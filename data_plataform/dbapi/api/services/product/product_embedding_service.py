import logging
from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.product.product_embedding_repository import ProductEmbeddingRepository

logger = logging.getLogger(__name__)

class ProductEmbeddingService:
    """ Regras de Negócio para Embeddings Vectoriais (Orquestrador) """
    
    @staticmethod
    async def process_bulk_insert(db: AsyncSession, embeddings_data: List[Dict[str, Any]]) -> bool:
        if not embeddings_data:
            logger.info("Nenhum fluxo de embedding vetorial listado.")
            return False
            
        return await ProductEmbeddingRepository.bulk_insert(db=db, embeddings_data=embeddings_data)