import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from api.repositories.person.person_customer_repository import PersonCustomerRepository
from api.models.person.customer import CustomerModel as PersonCustomerModel

logger = logging.getLogger(__name__)

class PersonCustomerService:
    """
    Isolated Logic Layer: Responsible for Business Rules and Routing.
    HAS NO IMPORTS OF SELECT, INSERT, OR COMMIT FROM SQLALCHEMY.
    """

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, customers_data: List[Dict[str, Any]]) -> bool:
        """ Checks payload anomalies and forwards the bulk operation to the Database """
        if not customers_data:
            logger.info("No PersonCustomer data found in the integration matrix.")
            return False

        return await PersonCustomerRepository.bulk_insert(db=db, customers_data=customers_data)

    @staticmethod
    async def get_customer_profile(db: AsyncSession, customer_id: int) -> Optional[PersonCustomerModel]:
        """ Fetches the Customer, encapsulating visual/logical constraints if any """
        if customer_id <= 0:
            raise ValueError("Customer ID must be a positive formatted number.")

        return await PersonCustomerRepository.get_by_id(db=db, customer_id=customer_id)

    @staticmethod
    async def register_new_customer(db: AsyncSession, validated_payload: Dict[str, Any]) -> PersonCustomerModel:
        """ Orchestrates the initial Onboarding lifecycle of a New Customer """
        # We could have duplicate email checks, systemic analysis, etc.
        return await PersonCustomerRepository.create_single(db=db, customer_data=validated_payload)
