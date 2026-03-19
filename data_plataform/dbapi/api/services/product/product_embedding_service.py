import logging
from typing import List, Dict, Any
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.product.product_embedding import ProductEmbeddingModel

logger = logging.getLogger(__name__)

async def bulk_insert_product_embeddings(db: AsyncSession, embeddings_data: List[Dict[str, Any]]) -> bool:
    """
    Realiza a inserção em lote de vetores de IA (Embeddings).
    Tabela extremamente pesada para I/O devido aos arrays floats gigantes (PGVector).
    """
    if not embeddings_data:
        logger.info("Nenhum embedding fornecido para inserção no lote.")
        return False
        
    try:
        stmt = insert(ProductEmbeddingModel).values(embeddings_data)
        
        await db.execute(stmt)
        await db.commit()
        
        logger.info(f"Sucesso ao inserir {len(embeddings_data)} vetores de embeddings.")
        return True
        
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Erro ao inserir lote de Embeddings. Rollback executado. Detalhes: {e}")
        raise