import logging
from typing import List, Dict, Any
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.product.product import Product

logger = logging.getLogger(__name__)

async def bulk_insert_products(db: AsyncSession, products_data: List[Dict[str, Any]]) -> bool:
    """
    Realiza a inserção em lote (Bulk Insert) da principal tabela de Produtos.
    
    A tabela Product recebe grandes volumes (ex: 200k SKUs). Utilizar `insert()` em vez 
    de instanciar os modelos ORM e usar `add_all` melhora o tempo de carregamento 
    exponencialmente (Core query ao invés de ORM overhead).
    """
    if not products_data:
        logger.info("Nenhum produto fornecido para inserção no lote.")
        return False
        
    try:
        stmt = insert(Product).values(products_data)
        
        await db.execute(stmt)
        await db.commit()
        
        logger.info(f"Sucesso ao inserir {len(products_data)} produtos.")
        return True
        
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Erro ao inserir lote de Produtos. Rollback executado. Detalhes: {e}")
        raise
