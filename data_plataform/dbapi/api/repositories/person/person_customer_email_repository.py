import logging
from typing import List, Dict, Any, Optional
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import SQLAlchemyError

from api.models.person.customeremail import PersonCustomerEmailModel

logger = logging.getLogger(__name__)

class PersonCustomerEmailRepository:
    """ Camada de Dados (SQLAlchemy) para base de Emails relacionais. """
    
    @staticmethod
    async def bulk_insert(db: AsyncSession, emails_data: List[Dict[str, Any]]) -> bool:
        try:
            stmt = insert(PersonCustomerEmailModel).values(emails_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"Erro SQL no bulk_insert de Emails: {e}")
            raise
            
    @staticmethod
    async def get_emails_by_customer_id(db: AsyncSession, customer_id: int) -> List[PersonCustomerEmailModel]:
        try:
            stmt = select(PersonCustomerEmailModel).where(PersonCustomerEmailModel.customer_id == customer_id)
            result = await db.execute(stmt)
            return list(result.scalars().all())
        except SQLAlchemyError as e:
            logger.error(f"Erro SQL buscando emails p/ customer {customer_id}: {e}")
            raise
