import logging
from typing import List, Dict, Any
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.product.productcategory import ProductCategoryModel

logger = logging.getLogger(__name__)

async def bulk_insert_product_categories(db: AsyncSession, categories_data: List[Dict[str, Any]]) -> bool:
    """
    Realiza a inserção em lote (Bulk Insert) de Categorias de Produtos.
    
    Abordagem utilizada:
    - **Guard Clauses**: Retorna imediatamente se a lista for vazia, evitando queries nulas.
    - **Bulk Insert**: Uso de `insert().values()` que é altamente otimizado no SQLAlchemy 2.0.
    - **Error Handling**: Captura exceções, executa rollback e emite log de erro crítico para a orquestração (Airflow).
    """
    if not categories_data:
        logger.info("Nenhuma categoria fornecida para inserção no lote.")
        return False
        
    try:
        stmt = insert(ProductCategoryModel).values(categories_data)
        
        await db.execute(stmt)
        await db.commit()
        
        logger.info(f"Sucesso ao inserir {len(categories_data)} categorias.")
        return True
        
    except SQLAlchemyError as e:
        await db.rollback()
        logger.error(f"Erro ao inserir lote de Categorias. Rollback executado. Detalhes: {e}")
        # Re-raise da exceção para que o Airflow/Operator saiba que a task falhou
        raise