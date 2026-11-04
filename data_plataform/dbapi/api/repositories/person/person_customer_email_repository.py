import logging
from typing import Any

from api.models.person.customer_email import CustomerEmailModel as PersonCustomerEmailModel
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PersonCustomerEmailRepository:
    """Data layer (SQLAlchemy) for the relational email store."""

    @staticmethod
    async def bulk_insert(db: AsyncSession, emails_data: list[dict[str, Any]]) -> bool:
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
    async def get_emails_by_customer_id(
        db: AsyncSession, customer_id: int
    ) -> list[PersonCustomerEmailModel]:
        try:
            stmt = select(PersonCustomerEmailModel).where(
                PersonCustomerEmailModel.customer_id == customer_id
            )
            result = await db.execute(stmt)
            return list(result.scalars().all())
        except SQLAlchemyError as e:
            logger.error(f"Erro SQL buscando emails p/ customer {customer_id}: {e}")
            raise
