from api.models.warehouse.transactiontype import WarehouseTransactionType
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession


class TransactionTypeRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self) -> list[WarehouseTransactionType]:
        query = select(WarehouseTransactionType).order_by(WarehouseTransactionType.transaction_name)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_by_id(self, transaction_type_id: int) -> WarehouseTransactionType | None:
        query = select(WarehouseTransactionType).filter(
            WarehouseTransactionType.transactiontype_id == transaction_type_id
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_by_code(self, transaction_code: str) -> WarehouseTransactionType | None:
        query = select(WarehouseTransactionType).filter(
            WarehouseTransactionType.transaction_code == transaction_code
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def create(self, data: dict) -> WarehouseTransactionType:
        # Using the from_pydantic utility or just dict mapping depending on the layer
        new_type = WarehouseTransactionType(**data)
        self.session.add(new_type)
        await self.session.flush()
        return new_type

    async def bulk_upsert(self, data_list: list[dict]):
        """
        Inserts multiple records in batch.
        """
        if not data_list:
            return

        stmt = insert(WarehouseTransactionType).values(data_list)
        await self.session.execute(stmt)
