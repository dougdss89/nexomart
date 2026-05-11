import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from api.repositories.person.person_customer_repository import PersonCustomerRepository
from api.models.person.customer import CustomerModel as PersonCustomerModel

logger = logging.getLogger(__name__)

class PersonCustomerService:
    """ 
    Camada Lógica Isolada: Responsável pelas Regras de Negócio e Direcionamento.
    NÃO POSSUI IMPORTS DE SELECT, INSERT OU COMMIT DO SQLALCHEMY.
    """
    
    @staticmethod
    async def process_bulk_integration(db: AsyncSession, customers_data: List[Dict[str, Any]]) -> bool:
        """ Checa anomalias de payload e transmite operação lótica ao Banco """
        if not customers_data:
            logger.info("Nenhum dado de PersonCustomer encontrado na matriz de integração.")
            return False
            
        return await PersonCustomerRepository.bulk_insert(db=db, customers_data=customers_data)

    @staticmethod
    async def get_customer_profile(db: AsyncSession, customer_id: int) -> Optional[PersonCustomerModel]:
        """ Traz o Customer encapsulando restrições visuais/lógicas caso tenhamos """
        if customer_id <= 0:
            raise ValueError("ID de cliente deve ser numérico positivo formatado.")
            
        return await PersonCustomerRepository.get_by_id(db=db, customer_id=customer_id)
        
    @staticmethod
    async def register_new_customer(db: AsyncSession, validated_payload: Dict[str, Any]) -> PersonCustomerModel:
        """ Orquestra o ciclo de vida inicial de Onboarding do Cliente Novo """
        # Poderíamos ter testes de email duplicado, análise sistêmica, etc.
        return await PersonCustomerRepository.create_single(db=db, customer_data=validated_payload)
