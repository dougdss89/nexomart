import logging
from typing import Any

from api.models.person.customer import CustomerModel as PersonCustomerModel
from sqlalchemy import insert, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class PersonCustomerRepository:
    """
    Data Access Layer (SQLAlchemy).
    All direct interactions with the Customers PostgreSQL are restricted to here.
    """

    @staticmethod
    async def bulk_insert(db: AsyncSession, customers_data: list[dict[str, Any]]) -> bool:
        try:
            stmt = insert(PersonCustomerModel).values(customers_data)
            await db.execute(stmt)
            await db.commit()
            return True
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error in bulk_insert of Customers: {e}")
            raise

    @staticmethod
    async def get_by_id(db: AsyncSession, customer_id: int) -> PersonCustomerModel | None:
        try:
            stmt = select(PersonCustomerModel).where(PersonCustomerModel.customer_id == customer_id)
            result = await db.execute(stmt)
            return result.scalar_one_or_none()
        except SQLAlchemyError as e:
            logger.error(f"SQL Error fetching Customer {customer_id}: {e}")
            raise

    @staticmethod
    async def create_single(db: AsyncSession, customer_data: dict[str, Any]) -> PersonCustomerModel:
        """Creates and returns the originated Entity persisted in the database (Useful for transactional APIs)."""
        try:
            new_customer = PersonCustomerModel(**customer_data)
            db.add(new_customer)
            await db.commit()
            await db.refresh(new_customer)
            return new_customer
        except SQLAlchemyError as e:
            await db.rollback()
            logger.error(f"SQL Error creating single customer: {e}")
            raise
