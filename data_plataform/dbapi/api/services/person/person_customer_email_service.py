import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from api.repositories.person.person_customer_email_repository import PersonCustomerEmailRepository
from api.models.person.customer_email import CustomerEmailModel as PersonCustomerEmailModel

logger = logging.getLogger(__name__)

class PersonCustomerEmailService:
    """ 
    Regras rigorosas para manutenção de Emails associados a clientes (Validações).
    """

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, emails_data: List[Dict[str, Any]]) -> bool:
        if not emails_data:
            return False
            
        return await PersonCustomerEmailRepository.bulk_insert(db=db, emails_data=emails_data)

    @staticmethod
    async def verify_domain_status(db: AsyncSession, customer_id: int) -> List[PersonCustomerEmailModel]:
        """ Retorna as referências de correios de um cliente logado na base """
        return await PersonCustomerEmailRepository.get_emails_by_customer_id(db=db, customer_id=customer_id)
