from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.warehouse.transaction_type_repository import TransactionTypeRepository
from api.schemas.warehouse.warehouse_transaction_type import TransactionTypeSchema
from api.models.warehouse.transactiontype import WarehouseTransactionType

class TransactionTypeService:
    def __init__(self, session: AsyncSession):
        self.repository = TransactionTypeRepository(session)

    async def list_transaction_types(self) -> List[WarehouseTransactionType]:
        return await self.repository.get_all()

    async def get_transaction_type(self, type_id: int) -> Optional[WarehouseTransactionType]:
        return await self.repository.get_by_id(type_id)

    async def create_transaction_type(self, schema: TransactionTypeSchema) -> WarehouseTransactionType:
        # Utilizamos o utilitário from_pydantic da Base aprimorada para conversão limpa
        transaction_type = WarehouseTransactionType.from_pydantic(schema)
        
        # Verificamos se já existe o código
        existing = await self.repository.get_by_code(transaction_type.transaction_code)
        if existing:
            raise ValueError(f"Transaction code '{transaction_type.transaction_code}' já existe.")
            
        return await self.repository.create(transaction_type.to_dict(exclude={"transactiontype_id"}))

    async def seed_initial_data(self, data_list: List[TransactionTypeSchema]):
        """
        Carga inicial em lote.
        """
        plain_data = [item.model_dump(exclude={"transactiontype_id"}) for item in data_list]
        await self.repository.bulk_upsert(plain_data)
