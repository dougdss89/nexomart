import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.product.productcategory import ProductCategoryModel

logger = logging.getLogger(__name__)

class ProductCategoryRepository:
    """
    Camada Exclusiva de Acesso a Dados (SQLAlchemy).
    Nenhuma Regra de Negócio deve morar aqui. Apenas execução bruta de CRUD.
    """

    @staticmethod
    async def bulk_insert(db: AsyncSession, categories_data: List[Dict[str, Any]]) -> bool:
        """ Executa insert otimizado em lote de Categorias de Produtos. """
        try:
            stmt = insert(ProductCategoryModel).values(categories_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL ao inserir Categorias em lote: {e}")
            raise
            
    @staticmethod
    async def get_by_id(db: AsyncSession, category_id: int) -> Optional[ProductCategoryModel]:
        """ Busca uma Categoria específica no banco de dados. """
        try:
            stmt = select(ProductCategoryModel).where(ProductCategoryModel.category_id == category_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Erro SQL ao buscar Categoria {category_id}: {e}")
            raise
