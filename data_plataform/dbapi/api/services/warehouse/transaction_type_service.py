from api.models.warehouse.transactiontype import WarehouseTransactionType
from api.repositories.warehouse.transaction_type_repository import TransactionTypeRepository
from api.schemas.warehouse.warehouse_transaction_type import TransactionTypeSchema
from sqlalchemy.ext.asyncio import AsyncSession


class TransactionTypeService:
    def __init__(self, session: AsyncSession):
        self.repository = TransactionTypeRepository(session)

    async def list_transaction_types(self) -> list[WarehouseTransactionType]:
        return await self.repository.get_all()

    async def get_transaction_type(self, type_id: int) -> WarehouseTransactionType | None:
        return await self.repository.get_by_id(type_id)

    async def create_transaction_type(
        self, schema: TransactionTypeSchema
    ) -> WarehouseTransactionType:
        # We use the enhanced Base's from_pydantic utility for clean conversion
        transaction_type = WarehouseTransactionType.from_pydantic(schema)

        # We check whether the code already exists
        existing = await self.repository.get_by_code(transaction_type.transaction_code)
        if existing:
            raise ValueError(
                f"Transaction code '{transaction_type.transaction_code}' already exists."
            )

        return await self.repository.create(
            transaction_type.to_dict(exclude={"transactiontype_id"})
        )

    async def seed_initial_data(self, data_list: list[TransactionTypeSchema]):
        """
        Initial batch load.
        """
        plain_data = [item.model_dump(exclude={"transactiontype_id"}) for item in data_list]
        await self.repository.bulk_upsert(plain_data)
