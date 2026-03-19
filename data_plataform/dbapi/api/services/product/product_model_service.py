import logging
from typing import List, Dict, Any
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.product.productmodel import ProductModel

logger = logging.getLogger(__name__)

async def bulk_insert_product_models(db: AsyncSession, models_data: List[Dict[str, Any]]) -> bool:
    """
    Realiza a inserção em lote (Bulk Insert) de Modelos de Produtos.
    """
    if not models_data:
        logger.info("Nenhum modelo (Product Model) fornecido para inserção no lote.")
        return False
        
    try:
        stmt = insert(ProductModel).values(models_data)
        
        await db.execute(stmt)
        await db.commit()
        
        logger.info(f"Sucesso ao inserir {len(models_data)} modelos.")
        return True
        
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Erro ao inserir lote de Modelos. Rollback executado. Detalhes: {e}")
        raise
