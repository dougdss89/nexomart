import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from api.repositories.product.product_category_repository import ProductCategoryRepository
from api.models.product.productcategory import ProductCategoryModel

logger = logging.getLogger(__name__)

class ProductCategoryService:
    """
    Camada de Regras de Negócio e Orquestração (Abstrata).
    Consome o Repository para acessar os dados.
    NUNCA deve importar sintaxes SQL como 'select' ou 'insert' e não pode conter manipulação do BD diretamente.
    """

    @staticmethod
    async def process_bulk_insert(db: AsyncSession, categories_data: List[Dict[str, Any]]) -> bool:
        """
        Garante as validações lógicas e repassa o bulk para o BD.
        (Exemplo: Checa permissões, valida anomalias nos dicionários lógicos).
        """
        if not categories_data:
            logger.info("Nenhuma categoria fornecida para inserção no lote.")
            return False
            
        # Orquestration rule: Chamar a classe puramente estruturada de BD 
        return await ProductCategoryRepository.bulk_insert(db=db, categories_data=categories_data)

    @staticmethod
    async def verify_category_exists(db: AsyncSession, category_id: int) -> bool:
        """
        Regra de Negócio: Verifica se a hierarquia existe no banco de dados.
        Retorna boolean abstraindo o Modelo, o Router não precisa ler o Modelo SQL, apenas saber se Válido ou não.
        """
        category: Optional[ProductCategoryModel] = await ProductCategoryRepository.get_by_id(db, category_id)
        return category is not None