import logging
from typing import Any

from api.models.person.customer_email import CustomerEmailModel as PersonCustomerEmailModel
from api.repositories.person.person_customer_email_repository import PersonCustomerEmailRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PersonCustomerEmailService:
    """
    Strict rules for maintaining Emails associated with customers (Validations).
    """

    @staticmethod
    async def process_bulk_integration(db: AsyncSession, emails_data: list[dict[str, Any]]) -> bool:
        if not emails_data:
            return False

        return await PersonCustomerEmailRepository.bulk_insert(db=db, emails_data=emails_data)

    @staticmethod
    async def verify_domain_status(
        db: AsyncSession, customer_id: int
    ) -> list[PersonCustomerEmailModel]:
        """Returns the email references of a customer logged in the database"""
        return await PersonCustomerEmailRepository.get_emails_by_customer_id(
            db=db, customer_id=customer_id
        )
