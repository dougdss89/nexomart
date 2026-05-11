import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.person.customer import CustomerModel as PersonCustomerModel

logger = logging.getLogger(__name__)

class PersonCustomerRepository:
    """ 
    Camada de Acesso a Dados (SQLAlchemy). 
    Todas as interações diretas com o PostgreSQL de Clientes ficam restritas aqui. 
    """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, customers_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(PersonCustomerModel).values(customers_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL no bulk_insert de Customers: {e}")
            raise

    @staticmethod
    async def get_by_id(db: AsyncSession, customer_id: int) -> Optional[PersonCustomerModel]:
        try:
            stmt = select(PersonCustomerModel).where(PersonCustomerModel.customer_id == customer_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"Erro SQL ao buscar Customer {customer_id}: {e}")
            raise

    @staticmethod
    async def create_single(db: AsyncSession, customer_data: Dict[str, Any]) -> PersonCustomerModel:
        """ Cria e retorna a própria Entidade originada persistindo no banco (Útil para APIs transacionais). """
        try:
            novo_cliente = PersonCustomerModel(**customer_data)
            db.add(novo_cliente)
            await db.commit()
            await db.refresh(novo_cliente)
            return novo_cliente
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL ao criar cliente único: {e}")
            raise
