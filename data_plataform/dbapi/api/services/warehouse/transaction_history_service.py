from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.warehouse.transaction_history_repository import TransactionHistoryRepository
from api.schemas.warehouse.warehouse_transactionhistory import TransactionHistorySchema
from api.models.warehouse.transactionhistory import TransactionHistoryModel

class TransactionHistoryService:
    def __init__(self, session: AsyncSession):
        self.repository = TransactionHistoryRepository(session)

    async def list_transactions(self, limit: int = 100) -> List[TransactionHistoryModel]:
        return await self.repository.get_all(limit=limit)

    async def process_bulk_transactions(self, transactions_data: List[TransactionHistorySchema]):
        """
        Processamento massivo de histórico de transações.
        Converte schemas Pydantic em dicionários prontos para o Repository.
        """
        # Limpezas e preparações antes do bulk insert
        processed_data = []
        for schema in transactions_data:
            # Usamos o model_dump() do Pydantic v2
            data = schema.model_dump()
            
            # Removemos IDs se forem autoincrement no banco (opcional dependendo do seeder)
            # data.pop("transaction_id", None)
            
            # O traceability_info disparará a serialização JSON automática do SQLAlchemy
            processed_data.append(data)
            
        await self.repository.bulk_insert(processed_data)
